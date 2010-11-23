/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Nov 22, 2010
 */
package com.bigdata.htbl;

import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.TestCase2;

/**
 * Test suite for extensible hashing.
 * 
 * @todo Persistence capable hash table for high volume hash joins. The data
 *       will be "rows" in a "relation" modeled using binding sets. We can use
 *       dense encoding of these rows since they have a fixed schema (some
 *       columns may allow nulls). There should also be a relationship to how we
 *       encode these data for network IO.
 * 
 * @todo Extensible hashing:
 *       <p>
 *       - hash(byte[] key) -> IRaba page. Use IRaba for keys/values and key
 *       search.
 *       <p>
 *       - Split if overflows the bucket size (alternative is some versioning
 *       where the computed hash value indexes into a logical address which is
 *       then translated to an IRawStore address - does the RWStore help us out
 *       here?)
 *       <p>
 *       - Ring buffer to wire in hot nodes (but expect random touches).
 *       <p>
 *       - initially, no history (no versioning). just replace the record when
 *       it is evicted from the ring buffer.
 *       <p>
 *       What follows is a summary of an extensible hashing design for bigdata.
 *       This covers most aspects of the hash map design, but does not drill
 *       deeply into the question of scale-out hash maps. The immediate goal is
 *       to develop a hash map which can be used for a variety of tasks,
 *       primarily pertaining to analytic query as described above.
 *       <p>
 *       Extensible hashing is one form of dynamic hashing in which buckets are
 *       split or coalesced as necessary and in which the reorganization is
 *       performed on one bucket at a time.
 *       <p>
 *       Given a hash function h generating, e.g., int32 values where b is the
 *       #of bits in the hash code. At any point, we use 0 LTE i LTE b bits of
 *       that hash code as an index into a table of bucket addresses. The value
 *       of i will change as the #of buckets changes based on the scale of the
 *       data to be addressed.
 *       <p>
 *       Given a key K, the bucket address table is indexed with i bits of the
 *       hash code, h(K). The value at that index is the address of the hash
 *       bucket. However, several consecutive entries in the hash table may
 *       point to the same hash bucket (for example, the hash index may be
 *       created with i=4, which would give 16 index values but only one initial
 *       bucket). The bucket address table entries which map onto the same hash
 *       bucket will have a common bit length, which may be LTE [i]. This bit
 *       length is not stored in the bucket address table, but each bucket knows
 *       its bit length. Given a global bit length of [i] and a bucket bit
 *       length of [j], there will be 2^(i-j) bucket address table entries which
 *       point to the same bucket.
 *       <p>
 *       Hash table versioning can be easily implemented by: (a) a checkpoint
 *       record with the address of the bucket address table (which could be
 *       broken into a two level table comprised of 4k pages in order to make
 *       small updates faster); and (b) a store level policy such that we do not
 *       overwrite the modified records directly (though they may be recycled).
 *       This will give us the same consistent read behind behavior as the
 *       B+Tree.
 *       <p>
 *       The IIndex interface will need to be partitioned appropriately such
 *       that the IRangeScan interface is not part of the hash table indices (an
 *       isBTree() and isHashMap() method might be added).
 *       <p>
 *       While the same read-through views for shards should work with hash maps
 *       as work with B+Tree indices, a different scheme may be necessary to
 *       locate those shards and we might need to use int64 hash codes in
 *       scale-out or increase the page size (at least for the read-only hash
 *       segment files, which would also need a batch build operation). The
 *       AccessPath will also need to be updated to be aware of classes which do
 *       not support key-range scans, but only whole relation scans.
 *       <p>
 *       Locking on hash tables without versioning should be much simpler than
 *       locking on B+Trees since there is no hierarchy and more operations can
 *       proceed without blocking in parallel.
 *       <p>
 *       We can represent tuples (key,value pairs) in an IRaba data structure
 *       and reuse parts of the B+Tree infrastructure relating to compression of
 *       IRaba, key search, etc. In fact, we might use to lazy reordering notion
 *       from Monet DB cracking to only sort the keys in a bucket when it is
 *       persisted. This is also a good opportunity to tackling splitting the
 *       bucket if it overflows the target record size, e.g., 4k. We could throw
 *       out an exception if the sorted, serialized, and optionally compressed
 *       record exceeds the target record size and then split the bucket. All of
 *       this seems reasonable and we might be able to then back port those
 *       concepts into the B+Tree.
 *       <p>
 *       We need to estimate the #of tuples which will fit within the bucket. We
 *       can do this based on: (a) the byte length of the keys and values (key
 *       compression is not going to help out much for a hash index since the
 *       keys will be evenly distributed even if they are ordered within a
 *       bucket); (b) the known per tuple overhead and per bucket overhead; (c)
 *       an estimate of the compression ratio for raba encoding and record
 *       compression. This estimate could be used to proactively split a bucket
 *       before it is evicted. This is most critical before anything is evicted
 *       as we would otherwise have a single very large bucket. So, let's make
 *       this simple and split the bucket if the sum of the key + val bytes
 *       exceeds 120% of the target record size (4k, 8k, etc). The target page
 *       size can be a property of the hash index. [Note: There is an implicit
 *       limit on the size of a tuple with this approach. The alternative is to
 *       fix the #of tuples in the bucket and allow buckets to be of whatever
 *       size they are for the specific data in that bucket.]
 * 
 * @todo RWStore integration notes:
 *       <p>
 *       - RWStore with "temporary" quality. Creates the backing file lazily on
 *       eviction from the write service.
 *       <p>
 *       - RWStore with "RAM" only? (Can not exceed the #of allocated buffers or
 *       can, but then it might force paging out to swap?)
 *       <p>
 *       - RWStore with "RAM" mostly. Converts to disk backed if uses all those
 *       buffers. Possibly just give the WriteCacheService a bunch of write
 *       cache buffers (10-100) and have it evict to disk *lazily* rather than
 *       eagerly (when the #of free buffers is down to 20%).
 *       <p>
 *       - RWStore with memory mapped file? As I recall, the problem is that we
 *       can not guarantee extension or close of the file under Java. But some
 *       people seem to make this work...
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/203
 */
public class TestExtensibleHashing extends TestCase2 {

	public TestExtensibleHashing() {
	}

	public TestExtensibleHashing(String name) {
		super(name);
	}

	/**
	 * Find the first power of two which is GTE the given value. This is used to
	 * compute the size of the address space (in bits) which is required to
	 * address a hash table with that many buckets.
	 */
	private static int getMapSize(final int initialCapacity) {

		if (initialCapacity <= 0)
			throw new IllegalArgumentException();
		
		int i = 1;

		while ((1 << i) < initialCapacity)
			i++;
		
		return i;

	}

	/**
	 * Unit test for {@link #getMapSize(int)}.
	 */
	public void test_getMapSize() {
		
		assertEquals(1/* addressSpaceSize */, getMapSize(1)/* initialCapacity */);
		assertEquals(1/* addressSpaceSize */, getMapSize(2)/* initialCapacity */);
		assertEquals(2/* addressSpaceSize */, getMapSize(3)/* initialCapacity */);
		assertEquals(2/* addressSpaceSize */, getMapSize(4)/* initialCapacity */);
		assertEquals(3/* addressSpaceSize */, getMapSize(5)/* initialCapacity */);
		assertEquals(3/* addressSpaceSize */, getMapSize(6)/* initialCapacity */);
		assertEquals(3/* addressSpaceSize */, getMapSize(7)/* initialCapacity */);
		assertEquals(3/* addressSpaceSize */, getMapSize(8)/* initialCapacity */);
		assertEquals(4/* addressSpaceSize */, getMapSize(9)/* initialCapacity */);

		assertEquals(5/* addressSpaceSize */, getMapSize(32)/* initialCapacity */);

		assertEquals(10/* addressSpaceSize */, getMapSize(1024)/* initialCapacity */);

	}

	/**
	 * Return a bit mask which reveals only the low N bits of an int32 value.
	 * 
	 * @param nbits
	 *            The #of bits to be revealed.
	 * @return The mask.
	 */
	private static int getMaskBits(final int nbits) {

		if (nbits < 0 || nbits > 32)
			throw new IllegalArgumentException();

//		int mask = 1; // mask
//		int pof2 = 1; // power of two.
//		while (pof2 < nbits) {
//			pof2 = pof2 << 1;
//			mask |= pof2;
//		}

		int mask = 0;
		int bit;
		
        for (int i = 0; i < nbits; i++) {

            bit = (1 << i);
            
            mask |= bit;
            
        }

//		System.err.println(nbits +" : "+Integer.toBinaryString(mask));

		return mask;

	}

	/**
	 * Unit test for {@link #getMaskBits(int)}
	 */
	public void test_getMaskBits() {

		assertEquals(0x00000001, getMaskBits(1));
		assertEquals(0x00000003, getMaskBits(2));
		assertEquals(0x00000007, getMaskBits(3));
		assertEquals(0x0000000f, getMaskBits(4));
		assertEquals(0x0000001f, getMaskBits(5));
		assertEquals(0x0000003f, getMaskBits(6));
		assertEquals(0x0000007f, getMaskBits(7));
		assertEquals(0x000000ff, getMaskBits(8));

		assertEquals(0x0000ffff, getMaskBits(16));

		assertEquals(0xffffffff, getMaskBits(32));
		
	}
	
//	private static int[] getMaskArray() {
//
//	}
	
	/**
	 * Extensible hashing data structure.
	 * 
	 * @todo allow duplicate tuples - caller can enforce distinct if they like.
	 * 
	 * @todo automatically promote large tuples into raw record references,
	 *       leaving the hash code of the key and the address of the raw record
	 *       in the hash bucket.
	 * 
	 * @todo initially manage the address table in an int[].
	 * 
	 * @todo use 4k buckets. split buckets when the sum of the data is GT 4k
	 *       (reserve space for a 4byte checksum). use a compact record
	 *       organization. if a tuple is deleted, bit flag it (but immediately
	 *       delete the raw record if one is associated with the tuple). before
	 *       splitting, compact the bucket to remove any deleted tuples.
	 * 
	 * @todo the tuple / raw record promotion logic should be shared with the
	 *       B+Tree. The only catch is that large B+Tree keys will always remain
	 *       a stress factor. For example, TERM2ID will have large B+Tree keys
	 *       if TERM is large and promoting to a blob will not help. In that
	 *       case, we actually need to hash the TERM and store the hash as the
	 *       key (or index only the first N bytes of the term).
	 */
	public static class ExtensibleHashBag {
		
	}

	/**
	 * An implementation of an extensible hash map using a 32 bit hash code and
	 * a fixed length int[] for the bucket. The keys are int32 values. The data
	 * stored in the hash map is just the key. Buckets provide a perfect fit for
	 * N keys. This is used to explore the dynamics of the extensible hashing
	 * algorithm using some well known examples.
	 * <p>
	 * This implementation is not thread-safe. I have not attempted to provide
	 * for visibility guarantees when resizing the map and I have not attempted
	 * to provide for concurrent updates. The implementation exists solely to
	 * explore the extensible hashing algorithm.
	 * <p>
	 * The hash code
	 */
	private static class SimpleExtensibleHashMap {

	    // @todo static logger.
//	    final transient Logger log = SimpleExtensibleHashMap.class
	    
		/**
		 * The #of int32 positions which are available in a {@link SimpleBucket}
		 * .
		 */
		private final int bucketSize;

		/**
		 * The #of hash code bits which are in use by the {@link #addressMap}.
		 * Each hash bucket also as a local #of hash bits. Given <code>i</code>
		 * is the #of global hash bits and <code>j</code> is the number of hash
		 * bits in some bucket, there will be <code>2^(i-j)</code> addresses
		 * which point to the same bucket.
		 */
		private int globalHashBits;

//		/**
//		 * The size of the address space (#of buckets addressable given the #of
//		 * {@link #globalHashBits} in use).
//		 */
//		private int addressSpaceSize;
		
		/**
		 * The address map. You index into this map using
		 * {@link #globalHashBits} out of the hash code for a probe key. The
		 * value of the map is the index into the {@link #buckets} array of the
		 * bucket to which that key is hashed.
		 */
		private int[] addressMap;

		/**
		 * The buckets. The first bucket is pre-allocated when the address table
		 * is setup and all addresses in the table are initialized to point to
		 * that bucket. Thereafter, buckets are allocated when a bucket is
		 * split.
		 */
		private final ArrayList<SimpleBucket> buckets;

		/**
		 * An array of mask values. The index in the array is the #of bits of
		 * the hash code to be considered. The value at that index in the array
		 * is the mask to be applied to mask off to zero the high bits of the
		 * hash code which are to be ignored.
		 */
		private final int[] masks;
		
//		/**
//		 * The current mask for the current {@link #globalHashBits}.
//		 */
//		private int globalMask;
		
		/**
		 * 
		 * @param initialCapacity
		 *            The initial capacity is the #of buckets which may be
		 *            stored in the hash table before it must be resized. It is
		 *            expressed in buckets and not tuples because there is not
		 *            (in general) a fixed relationship between the size of a
		 *            bucket and the #of tuples which can be stored in that
		 *            bucket. This will be rounded up to the nearest power of
		 *            two.
		 * @param bucketSize
		 *            The #of int tuples which may be stored in a bucket.
		 */
		public SimpleExtensibleHashMap(final int initialCapacity, final int bucketSize) {
			
			if (initialCapacity <= 0)
				throw new IllegalArgumentException();

			if (bucketSize <= 0)
				throw new IllegalArgumentException();
			
			this.bucketSize = bucketSize;

			/*
			 * Setup the hash table given the initialCapacity (in buckets). We
			 * need to find the first power of two which is GTE the
			 * initialCapacity.   
			 */
			globalHashBits = getMapSize(initialCapacity);

			if (globalHashBits > 32) {
				/*
				 * The map is restricted to 32-bit hash codes so we can not
				 * address this many buckets.
				 */
				throw new IllegalArgumentException();
			}

			// Populate the array of masking values.
			masks = new int[32];
			
			for (int i = 0; i < 32; i++) {

				masks[i] = getMaskBits(i);
				
			}

//			// save the current masking value for the current #of global bits.
//			globalMask = masks[globalHashBits];

			/*
			 * Now work backwards to determine the size of the address space (in
			 * buckets).
			 */
			final int addressSpaceSize = 1 << globalHashBits;

			/*
			 * Allocate and initialize the address space. All indices are
			 * initially mapped onto the same bucket.
			 */
			addressMap = new int[addressSpaceSize];

			buckets = new ArrayList<SimpleBucket>(addressSpaceSize/* initialCapacity */);
			
			// Note: the local bits of the first bucket is set to ZERO (0).
			buckets.add(new SimpleBucket(0/* localHashBits */, bucketSize));

		}

//		private void toString(StringBuilder sb) {
//			sb.append("addressMap:"+Arrays.toString(addressMap));
//		}
		
		/** The hash of an int key is that int. */
		private int hash(final int key) {
		
		    return key;
		    
		}

        /**
         * The index into the address table given that we use
         * {@link #globalHashBits} of the given hash value.
         * <p>
         * Note: This is identical to maskOff(h,{@link #globalHashBits}).
         */
        private int getIndexOf(final int h) {

            return maskOff(h, globalHashBits);

        }

        /**
         * Mask off all but the lower <i>nbits</i> of the hash value.
         * 
         * @param h
         *            The hash value.
         * @param nbits
         *            The #of bits to consider.
         * @return The hash value considering only the lower <i>nbits</i>.
         */
        private int maskOff(final int h, final int nbits) {

            if (nbits < 0 || nbits > 32)
                throw new IllegalArgumentException();
            
            return h & masks[nbits];
            
        }
        
		/** The bucket address given the hash code of a key. */
		private int addrOf(final int h) {

			final int index = getIndexOf(h);

			return addressMap[index];
			
		}

		/**
		 * Return the pre-allocated bucket having the given address.
		 * 
		 * @param addr
		 *            The address.
		 *            
		 * @return The bucket.
		 */
		private SimpleBucket getBucket(final int addr) {

			return buckets.get(addr);
			
		}

		/**
		 * The #of hash bits which are being used by the address table.
		 */
		public int getGlobalHashBits() {
			
			return globalHashBits;
			
		}

		/**
		 * The size of the address space (the #of positions in the address
		 * table, which is NOT of necessity the same as the #of distinct buckets
		 * since many address positions can point to the same bucket).
		 */
		public int getAddressSpaceSize() {

			return addressMap.length;
			
		}
		
		/**
		 * The #of buckets backing the map.
		 */
		public int getBucketCount() {
			
			return buckets.size();
			
		}
		
		/**
		 * The size of a bucket (the #of int32 values which may be stored
		 * in a bucket).
		 */
		public int getBucketSize() {
			
			return bucketSize;
			
		}

		/**
		 * Return <code>true</code> iff the hash table contains the key.
		 * <p>
		 * Lookup: Compute h(K) and right shift (w/o sign extension) by i bits.
		 * Use this to index into the bucket address table. The address in the
		 * table is the bucket address and may be used to directly read the
		 * bucket.
		 * 
		 * @param key
		 *            The key.
		 * 
		 * @return <code>true</code> iff the key was found.
		 */
		public boolean contains(final int key) {
			
		    final int h = hash(key);
			
		    final int addr = addrOf(h);
			
		    final SimpleBucket b = getBucket(addr);
            
		    return b.contains(h, key);
		    
		}

		/**
		 * Insert the key into the hash table. Duplicates are allowed.
		 * <p>
		 * Insert: Per lookup. On overflow, we need to split the bucket moving
		 * the existing records (and the new record) into new buckets.
		 * 
		 * @see #split(int, int, SimpleBucket)
		 * 
		 * @param key
		 *            The key.
		 * 
		 * @todo define a put() method which returns the old value (no
		 *       duplicates). this could be just sugar over contains(), delete()
		 *       and insert().
		 */
		public void insert(final int key) {
			final int h = hash(key);
			final int addr = addrOf(h);
			final SimpleBucket b = getBucket(addr);
			if (b.insert(h, key)) {
				return;
			}
			// split the bucket and insert the record (recursive?)
			split(key, b);
		}

		/**
		 * Split the bucket, adjusting the address map iff necessary. How this
		 * proceeds depends on whether the hash #of bits used in the bucket is
		 * equal to the #of bits used to index into the bucket address table.
		 * There are two cases:
		 * <p>
		 * Case 1: If {@link #globalHashBits} EQ the
		 * {@link SimpleBucket#localHashBits}, then the bucket address table is
		 * out of space and needs to be resized.
		 * <p>
		 * Case 2: If {@link #globalHashBits} is GT
		 * {@link SimpleBucket#localHashBits}, then there will be at least two
		 * entries in the bucket address table which point to the same bucket.
		 * One of those entries is relabeled. The record is then inserted based
		 * on the new #of hash bits to be considered. If it still does not fit,
		 * then either handle by case (1) or case (2) as appropriate.
		 * <p>
		 * Note that records which are in themselves larger than the bucket size
		 * must eventually be handled by: (A) using an overflow record; (B)
		 * allowing the bucket to become larger than the target page size (using
		 * a larger allocation slot or becoming a blob); or (C) recording the
		 * tuple as a raw record and maintaining only the full hash code of the
		 * tuple and its raw record address in the bucket (this would allow us
		 * to automatically promote long literals out of the hash bucket and a
		 * similar approach might be used for a B+Tree leaf, except that a long
		 * key will still cause a problem [also, this implies that deleting a
		 * bucket or leaf on the unisolated index of the RWStore might require a
		 * scan of the IRaba to identify blob references which must also be
		 * deleted, so it makes sense to track those as part of the bucket/leaf
		 * metadata).
		 * 
		 * @param h
		 *            The key which triggered the split.
		 * @param b
		 *            The bucket lacking sufficient room for the key which
		 *            triggered the split.
		 * 
		 * @todo caller will need an exclusive lock if this is to be thread
		 *       safe.
		 * 
		 * @todo Overflow buckets (or oversize buckets) are required when all
		 *       hash bits considered by the local bucket are the same, when all
		 *       keys in the local bucket are the same, and when the record to
		 *       be inserted is larger than the bucket. In order to handle these
		 *       cases we may need to more closely integrate the insert/split
		 *       logic since detecting some of these cases requires transparency
		 *       into the bucket.
		 */
        private void split(final int key, final SimpleBucket b) {
            if (log.isDebugEnabled())
                log.debug("globalBits=" + globalHashBits + ",localHashBits="
                        + b.localHashBits + ",key=" + key);
            if (globalHashBits < b.localHashBits) {
				// This condition should never arise.
				throw new AssertionError();
			}
			if (globalHashBits == b.localHashBits) {
				/*
				 * The address table is out of space and needs to be resized.
				 * 
				 * Let {@link #globalHashBits} := {@link #globalHashBits} + 1.
				 * This doubles the size of the bucket address table. Each
				 * original entry becomes two entries in the new table. For the
				 * specific bucket which is to be split, a new bucket is
				 * allocated and the 2nd bucket address table for that entry is
				 * set to the address of the new bucket. The tuples are then
				 * assigned to the original bucket and the new bucket by
				 * considering the additional bit of the hash code. Assuming
				 * that all keys are distinct, then one split will always be
				 * sufficient unless all tuples in the original bucket have the
				 * same hash code when their (i+1)th bit is considered (this can
				 * also occur if duplicate keys are allow). In this case, we
				 * resort to an "overflow" bucket (alternatively, the bucket is
				 * allowed to be larger than the target size and gets treated as
				 * a blob).
				 */
			    // the size of the address space before we double it.
			    final int oldAddrSize = getAddressSpaceSize();
                // the hash value of the key.
                final int h = hash(key);
                /*
                 * The index into the address space for the hash key given the
                 * #of bits considered before we double the address space.
                 */
                final int oldIndex = getIndexOf(h);
                // the address of the bucket to be split.
                final int addrOld = addressMap[oldIndex];
                /*
                 * The address into the new address map of the new bucket (once
                 * it gets created).
                 * 
                 * Note: We find that entry by adding the size of the old
                 * address table to the index within the table of the bucket to
                 * be split.
                 */
                final int newIndex = oldIndex + oldAddrSize;
                final int addrNew = addressMap[newIndex];
                // double the address space.
                doubleAddressSpace();
                /*
                 * Create a new bucket and wire it into the 2nd entry for the
                 * hash code for that key.
                 */
                // the original bucket.
                final SimpleBucket bold = getBucket(addrOld);
                bold.localHashBits++;
                // the new bucket.
                final SimpleBucket bnew = new SimpleBucket(bold.localHashBits,
                        bucketSize);
                // The address for the new bucket.
                final int addrBNew = buckets.size();
                // Add to the chain of buckets.
                buckets.add(bnew);
                // Update the address table to point to the new bucket.
                addressMap[addrNew] = addrBNew;
                /*
                 * FIXME Redistribute the keys in the old bucket between the old
                 * and new bucket by considering one more bit in their hash
                 * values.
                 * 
                 * Note: The move has to be handled in a manner which does not
                 * have side-effects which put the visitation of the keys in the
                 * original bucket out of whack. The code below figures out
                 * which keys move and which stay and copies the ones that move
                 * in one step. It then goes back through and deletes all keys
                 * which are found in the new bucket from the original bucket.
                 * 
                 * @todo As a pre-condition to splitting the bucket, we need to
                 * verify that at least one key is not the same as the others in
                 * the bucket. If all keys are the same, then we should have
                 * followed an overflow path instead of a split path.
                 */
                {
                    // // flag for each key says whether it moves or stays.
                    // final boolean[] move = new boolean[bold.size];
                    for (int i = 0; i < bold.size; i++) {
                        // a key from the original bucket.
                        final int k = bold.data[i];
                        // the hash of the key with the #of of local bits.
                        final int h1 = maskOff(k, bold.localHashBits);
                        if (h1 == oldIndex) {
                            // The key does not move.
                            continue;
                            // move[i] = false;
                        } else if (h1 == newIndex) {
                            // The key will move.
                            // move[i] = true;
                            bnew.insert(h/* hash(key) */, key);
                        } else {
                            // Must be hashed to one of these two buckets!!!
                            throw new AssertionError();
                        }
                    }
                    for (int i = 0; i < bnew.size; i++) {
                        // a key from the new bucket.
                        final int k = bnew.data[i];
                        // delete the key from the old bucket.
                        bold.delete(h/* hash(key) */, key);
                    }
                }
                /*
                 * Insert the key into the expanded hash table.
                 */
                {
                    // the address of the bucket for that hash code.
                    final int addr = addrOf(h);
                    // the bucket for that address.
                    final SimpleBucket btmp = getBucket(addr);
                    if (btmp.insert(h, key)) {
                        // insert was successful.
                        return;
                    }
                    /*
                     * FIXME This could be a variety of special conditions which
                     * need to be handled.
                     */
                    throw new UnsupportedOperationException();
                }
			}
			if (globalHashBits > b.localHashBits) {
				/*
				 * There will be at least two entries in the address table which
				 * point to this bucket. One of those entries is relabeled. Both
				 * the original bucket and the new bucket have their {@link
				 * SimpleBucket#localHashBits} incremented by one, but the
				 * {@link #globalHashBits}. Of the entries in the bucket address
				 * table which used to point to the original bucket, the 1st
				 * half are left alone and the 2nd half are updated to point to
				 * the new bucket. (Note that the #of entries depends on the
				 * global #of hash bits in use and the bucket local #of hash
				 * bits in use and will be 2 if there is a difference of one
				 * between those values but can be more than 2 and will always
				 * be an even number). The entries in the original bucket are
				 * rehashed and assigned based on the new #of hash bits to be
				 * considered to either the original bucket or the new bucket.
				 * The record is then inserted based on the new #of hash bits to
				 * be considered. If it still does not fit, then either handle
				 * by case (1) or case (2) as appropriate.
				 */
				throw new UnsupportedOperationException();
			}
		}

        /**
         * Doubles the address space.
         * <p>
         * This allocates a new address table and initializes it with TWO (2)
         * identical copies of the current address table, one right after the
         * other and increases {@link #globalHashBits} by ONE (1).
         * <p>
         * This operation preserves the current mapping of hash values into an
         * address table when we consider one more bit in those hash values. For
         * example, if we used to consider <code>3</code> bits of the hash value
         * then we will now consider <code>4</code> bits. If the fourth bit of
         * the hash value is ZERO (0) then it addresses into the first copy of
         * the address table. If the fourth bit of the hash value is ONE (1)
         * then it addresses into the second copy of the address table. Since
         * the entries point to the same buckets as they did when we only
         * considered <code>3</code> bits of the hash value the mapping of the
         * keys onto the buckets is not changed by this operation.
         */
        private void doubleAddressSpace() {

            if (log.isInfoEnabled())
                log.info("Doubleing the address space: globalBits="
                        + globalHashBits + ", addressSpaceSize="
                        + getAddressSpaceSize());

            final int oldLength = addressMap.length;

            // allocate a new address table which is twice a large.
            final int[] tmp = new int[oldLength << 1];

            /*
             * Copy the current address table into the lower half of the new
             * table.
             */
            System.arraycopy(addressMap/* src */, 0/* srcPos */, tmp/* dest */,
                    0/* destPos */, oldLength);

            /*
             * Copy the current address table into the upper half of the new
             * table.
             */
            System.arraycopy(addressMap/* src */, 0/* srcPos */, tmp/* dest */,
                    oldLength/* destPos */, oldLength);

            // Replace the address table.
            addressMap = tmp;
            
            // Consider one more bit in the hash value of the keys.
            globalHashBits += 1;
            
		}
		
		private void merge(final int h, final SimpleBucket b) {
			throw new UnsupportedOperationException();
		}

		/**
		 * Delete the key from the hash table (in the case of duplicates, a
		 * random entry having that key is deleted).
		 * <p>
		 * Delete: Buckets may be removed no later than when they become empty
		 * and doing this is a local operation with costs similar to splitting a
		 * bucket. Likewise, it is clearly possible to coalesce buckets which
		 * underflow before they become empty by scanning the 2^(i-j) buckets
		 * indexed from the entries in the bucket address table using i bits
		 * from h(K). [I need to research handling deletes a little more,
		 * including under what conditions it is cost effective to reduce the
		 * size of the bucket address table itself.]
		 * 
		 * @param key
		 *            The key.
		 * 
		 * @return <code>true</code> iff a tuple having that key was deleted.
		 * 
		 * @todo return the deleted tuple.
		 * 
		 * @todo merge buckets when they underflow/become empty? (but note that
		 *       we do not delete anything from the hash map for a hash join,
		 *       just insert, insert, insert).
		 */
		public boolean delete(final int key) {
			final int h = hash(key);
			final int addr = addrOf(h);
			final SimpleBucket b = getBucket(addr);
			return b.delete(h,key);
		}
		
		/**
		 * Visit the buckets.
		 * <p>
		 * Note: This is NOT thread-safe!
		 */
		public Iterator<SimpleBucket> buckets() {

		    return buckets.iterator();
		    
		}

        /**
         * Return the #of entries in the hash table having the given key.
         * 
         * @param key
         *            The key.
         *            
         * @return The #of entries having that key.
         */
        public int[] getEntryCount(final int key) {
            throw new UnsupportedOperationException();
        }

        /**
         * Return all entries in the hash table having the given key.
         * 
         * @param key
         *            The key.
         * 
         * @return The entries in the hash table having that key.
         * 
         * @todo this should return an iterator over the tuples for the real
         *       implementation.
         */
        public int[] getEntries(final int key) {
            throw new UnsupportedOperationException();
        }

        /**
         * Return an entry in the hash table having the given key. If there is
         * more than one entry for that key, then any entry having that key may
         * be returned.
         * 
         * @param key
         *            The key.
         * 
         * @return An entry having that key.
         */
        public int getEntry(final int key) {
            throw new UnsupportedOperationException();
        }
        
	}
	
	/**
	 * A (very) simple hash bucket.  The bucket holds N int32 keys.
	 */
	private static class SimpleBucket {

        /**
         * The #of hash code bits which are in use by this {@link SimpleBucket}.
         * 
         * @todo If we need to examine this when we change the size of the
         *       address space then it makes more sense to have this as local
         *       metadata in the address table than as local data in the bucket
         *       (the latter would require us to visit each bucket when
         *       expanding the address space).
         */
		private int localHashBits;

		/**
		 * The #of keys stored in the bucket. The keys are stored in a dense
		 * array. For a given {@link #size}, the only indices of the array which
		 * have any data are [0:{@link #size}-1].
		 */
		private int size;
		
		/**
		 * The user data for the bucket.
		 */
		private final int[] data;
		
        public SimpleBucket(final int localHashBits, final int bucketSize) {

			if (localHashBits < 0 || localHashBits > 32)
				throw new IllegalArgumentException();

			this.localHashBits = localHashBits;
			
			this.data = new int[bucketSize];
			
		}

		/**
		 * Return <code>true</code> if the bucket contains the key.
		 * 
		 * @param h
		 *            The hash code of the key.
		 * @param key
		 *            The key.
		 * 
		 * @return <code>true</code> if the key was found in the bucket.
		 * 
		 * @todo passing in the hash code here makes sense when the bucket
		 *       stores the hash values, e.g., if we always do that or if we
		 *       have an out of bucket reference to a raw record because the
		 *       tuple did not fit in the bucket.
		 */
		public boolean contains(final int h, final int key) {

			for (int i = 0; i < size; i++) {

				if (data[i] == key)
					return true;

			}

			return false;
			
		}

		/**
		 * Insert the key into the bucket (duplicates are allowed). It is an
		 * error if the bucket is full.
		 * 
		 * @param h
		 *            The hash code of the key.
		 * @param key
		 *            The key.
		 * 
		 * @return <code>false</code> iff the bucket must be split.
		 */
		public boolean insert(final int h, final int key) {

			if (size == data.length) {
				/*
				 * The bucket must be split, potentially recursively.
				 * 
				 * Note: Splits need to be triggered based on information which
				 * is only available to the bucket when it considers the insert
				 * of a specific tuple, including whether the tuple is promoted
				 * to a raw record reference, whether the bucket has deleted
				 * tuples which can be compacted, etc.
				 * 
				 * @todo I need to figure out where the control logic goes to
				 * manage the split. If the bucket handles splits, then we need
				 * to pass in the table reference.
				 */
				return false;
			}
			
			data[size++] = key;
	
			return true;
			
		}

		/**
		 * Delete a tuple having the specified key. If there is more than one
		 * such tuple, then a random tuple having the key is deleted.
		 * 
		 * @param h
		 *            The hash code of the key.
		 * @param key
		 *            The key.
		 * 
		 * @todo return the delete tuple.
		 */
		public boolean delete(final int h, final int key) {

			for (int i = 0; i < size; i++) {

				if (data[i] == key) {

					// #of tuples remaining beyond this point.
					final int length = size - i - 1;

					if (length > 0) {

						// Keep the array dense by copying down by one.
						System.arraycopy(data, i + 1/* srcPos */, data/* dest */,
								i/* destPos */, length);

					}

					size--;
					
					return true;

				}

			}

			return false;

		}
		
		/**
		 * The #of entries in the bucket.
		 */
		public int getEntryCount() {

		    return size;
		    
		}

        /**
         * A copy of the entries.
         * <p>
         * Note: This method returns as an array in order to avoid autobox
         * issues which arise with int32 keys and Integers. Visitation of tuples
         * in the bucket will be handled differently in a full implementation.
         */
		public int[] getEntries() {

		    return data.clone();
		    
		}
		
	}

	/**
	 * Map constructor tests.
	 */
	public void test_ctor() {

		final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
				1/* initialCapacity */, 3/* bucketSize */);
	
		assertEquals("globalHashBits", 1, map.getGlobalHashBits());
		
		assertEquals("addressSpaceSize", 2, map.getAddressSpaceSize());

		assertEquals("bucketCount", 1, map.getBucketCount());
		
		assertEquals("bucketSize", 3, map.getBucketSize());
		
	}
	
	/**
	 * Simple CRUD test operating against the initial bucket without triggering
	 * any splits.
	 */
	public void test_crud1() {
	
		final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
				1/* initialCapacity */, 3/* bucketSize */);

		// a bunch of things which are not in the map.
		for (int i : new int[] { 0, 1, -4, 31, -93, 912 }) {
			
			assertFalse(map.contains(i));
			
		}
		
		/*
		 * Insert a record, then delete it, verifying that contains() reports
		 * true or false as appropriate for the pre-/post- conditions.
		 */

		assertFalse(map.contains(83));

		map.insert(83);
		
		assertTrue(map.contains(83));
		
		map.delete(83);
		
		assertFalse(map.contains(83));
		
	}

	/**
	 * CRUD test which inserts some duplicate tuples, but not enough to split
	 * the initial bucket, and the deletes them out again.
	 */
	public void test_crud2() {
	
		final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
				1/* initialCapacity */, 3/* bucketSize */);

		assertEquals("bucketCount", 1, map.getBucketCount());
		
		assertFalse(map.contains(83));

		// insert once.
		map.insert(83);
		
		assertTrue(map.contains(83));
		
		// insert again.
		map.insert(83);
		
		assertTrue(map.contains(83));
		
		// did not split the bucket.
		assertEquals("bucketCount", 1, map.getBucketCount());

		// delete once.
		map.delete(83);

		// still found.
		assertTrue(map.contains(83));

		// delete again.
		map.delete(83);

		// now gone.
		assertFalse(map.contains(83));
		
	}
	
	/**
	 * Test repeated insert of a key until the bucket splits.
	 */
	public void test_split() {

		final int bucketSize = 3;
		
		final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
				1/* initialCapacity */, bucketSize);

		assertEquals("bucketCount", 1, map.getBucketCount());

		map.insert(83);
		map.insert(83);
		map.insert(83);

		// still not split.
		assertEquals("bucketCount", 1, map.getBucketCount());

		// force a split (83 == 0b10000011)
		map.insert(83);

		assertEquals("bucketCount", 2, map.getBucketCount());

	}

    /**
     * Unit test based on example in
     * "External Memory Algorithms and Data Structures" by Vitter, page 239.
     * <p>
     * The assignment of keys to buckets in this example is based on the low
     * bits of the key. Initially, the global depth is <code>3</code> so only
     * the lower three bits are considered.
     * 
     * <pre>
     * 0   0
     * 1   1
     * 2   10
     * 3   11
     * 4   100
     * 5   101
     * 6   110
     * 7   111
     * </pre>
     * 
     * To setup the example, we insert the sequence {4, 23, 18, 10, 44, 32, 9}
     * into an extensible hashing algorithm using buckets with a capacity of
     * <code>3</code>. The keys and the values stored in the buckets are int32
     * values. Inserting this sequence of keys should yield a hash table with a
     * global depth <em>d</em> of <code>8</code> having 8 addresses and four
     * buckets arranged as follows.
     * 
     * <pre>
     * [000] -> (A) [k=2] {4, 44, 32}
     * [001] -> (C) [k=1] {23, 9}
     * [010] -> (B) [k=3] {18}
     * [011] -> (C)
     * [100] -> (A)
     * [101] -> (C)
     * [110] -> (D) [k=3] {10}
     * [111] -> (C)
     * </pre>
     * 
     * Where <em>k</em> is the local depth of a given bucket and the buckets are
     * labeled A, B, C, ... based on the order in which they are written on the
     * page in the example (which presumably is the order in which those buckets
     * were created, but I have not yet verified this).
     * <p>
     * Next, key <code>76</code> is inserted. Considering only the d=3 bits of
     * its hash code, this key would be inserted the address having bits
     * <code>100</code>, which is mapped onto bucket (A). This will cause bucket
     * (A) to be split into (A) and (D). The local depth of (A) is increased by
     * one to <code>k=3</code>. The local depth of (D) is the same as (A). The
     * hash table looks as follows after this split.
     * 
     * <pre>
     * [000] -> (A) [k=3] {32}
     * [001] -> (C) [k=1] {23, 9}
     * [010] -> (B) [k=3] {18}
     * [011] -> (C)
     * [100] -> (E) [k=4] {4, 44, 76}
     * [101] -> (C)
     * [110] -> (D) [k=3] {10}
     * [111] -> (C)
     * </pre>
     * 
     * Finally, key <code>20</code> is inserted, again into the block addressed
     * by address table entry <code>100</code>. This causes block (E) to be
     * split into two blocks (E) and (F) having local bits <code>k=4</code>. The
     * address table is doubled and the #of global bits is increased to
     * <code>d=4</code>. The hash table after this expansion looks as follows:
     * 
     * <pre>
     * [0000] -> (A) [k=3] {32}
     * [0001] -> (C) [k=1] {23, 9}
     * [0010] -> (B) [k=3] {18}
     * [0011] -> (C)
     * [0100] -> (E) [k=4] {4, 20}
     * [0101] -> (C)
     * [0110] -> (D) [k=3] {10}
     * [0111] -> (C)
     *  ---- extension ----
     * [1000] -> (A)
     * [1001] -> (C)
     * [1010] -> (B)
     * [1011] -> (C)
     * [1100] -> (F) [k=4] {44, 76}
     * [1101] -> (C)
     * [1110] -> (D)
     * [1111] -> (C)
     * </pre>
     * 
     * When the address space is extended, the original address table entries
     * remain at their given offsets into the new table. The new address table
     * entries are initialized from the same entry which would have been
     * addressed if we considered one less bit. For example, any hash code
     * ending in <code>000</code> used to index into the first entry in the
     * address table. After the address table is split, one more bit will be
     * considered in the hash code of the key. Therefore, the same key will
     * either be in the address table entry <code>0000</code> or the entry
     * <code>1000</code>. To keep everything consistent, the address table entry
     * for <code>1000</code> is therefore initialized from the address table
     * entry for <code>0000</code>. In practice, all you are doing is writing a
     * second copy of the original address table entries starting immediately
     * after the original address table entries.
     * <p>
     * Personally, I find this representation much easier to interpret. You can
     * see that the address table was simply duplicated and (E) was split into
     * two buckets. One remains (E) and continues to be addressed from the 1st
     * half of the address table. The other is (F) and is the only change in the
     * 2nd half of the address table.
     */
	public void test_simple() {
        final int bucketSize = 3;
        
        final SimpleExtensibleHashMap map = new SimpleExtensibleHashMap(
                1/* initialCapacity */, bucketSize);

        /*
         * Insert the initial key sequence.
         * 
         * FIXME Verify post-condition in detail for each bucket since the next
         * steps require the problem to be setup correctly.
         */
        final int[] keys0 = new int[]{4, 23, 18, 10, 44, 32, 9};
        for(int key : keys0) {
            assertFalse(map.contains(key));
            map.insert(key);
            assertTrue(map.contains(key));
        }

        assertEquals("globalHashBits", 3, map.getGlobalHashBits());
        assertEquals("addressSpace", 8, map.getAddressSpaceSize());
        assertEquals("bucketCount", 3, map.getBucketCount());

        /*
         * Insert key 76. This splits (A), which is the bucket addressed by
         * [000]. This does not cause the address space to double since [100]
         * was also a reference to (A). This increments the local depth of (A)
         * to [3] and also creates a new bucket (E), having the same local depth
         * as (A) [3], and then sets a reference to that bucket at index [100]
         * of the address table. The keys in (A) are redistributed between (A)
         * and (E) by considering the [k=3] bit hash, where [k] is the new local
         * depth of (A) and (E). This gets the hash table into a consistent
         * state. The new key (76) is then inserted into the hash table and
         * winds up in (E).
         * 
         * @todo verify the contents of each bucket in detail.
         */
        map.insert(76);

        // unchanged.
        assertEquals("globalHashBits", 3, map.getGlobalHashBits());
        assertEquals("addressSpace", 8, map.getAddressSpaceSize());

        // one more bucket.
        assertEquals("bucketCount", 4, map.getBucketCount());

        fail("write test");

    }

}
