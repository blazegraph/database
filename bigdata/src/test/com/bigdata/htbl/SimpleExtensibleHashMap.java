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
 * Created on Nov 29, 2010
 */
package com.bigdata.htbl;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.btree.BloomFilter;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * An implementation of an extensible hash map using a 32 bit hash code and a
 * fixed length int[] for the bucket. The keys are int32 values. The data stored
 * in the hash map is just the key. Buckets provide a perfect fit for N keys.
 * This is used to explore the dynamics of the extensible hashing algorithm
 * using some well known examples.
 * <p>
 * This implementation is not thread-safe. I have not attempted to provide for
 * visibility guarantees when resizing the map and I have not attempted to
 * provide for concurrent updates. The implementation exists solely to explore
 * the extensible hashing algorithm.
 * <p>
 * The hash code
 * 
 * @todo We can not directly implement {@link Map} unless the hash table is
 *       configured to NOT permit duplicates.
 */
class SimpleExtensibleHashMap {

	private final transient static Logger log = Logger
			.getLogger(SimpleExtensibleHashMap.class);

	/**
	 * The #of int32 positions which are available in a {@link SimpleBucket} .
	 */
	private final int bucketSize;

	/**
	 * The #of hash code bits which are in use by the {@link #addressMap}. Each
	 * hash bucket also as a local #of hash bits. Given <code>i</code> is the
	 * #of global hash bits and <code>j</code> is the number of hash bits in
	 * some bucket, there will be <code>2^(i-j)</code> addresses which point to
	 * the same bucket.
	 */
	private int globalHashBits;

	// /**
	// * The size of the address space (#of buckets addressable given the #of
	// * {@link #globalHashBits} in use).
	// */
	// private int addressSpaceSize;

	/**
	 * The address map. You index into this map using {@link #globalHashBits}
	 * out of the hash code for a probe key. The value of the map is the index
	 * into the {@link #buckets} array of the bucket to which that key is
	 * hashed.
	 */
	private int[] addressMap;

	/**
	 * The buckets. The first bucket is pre-allocated when the address table is
	 * setup and all addresses in the table are initialized to point to that
	 * bucket. Thereafter, buckets are allocated when a bucket is split.
	 */
	private final ArrayList<SimpleBucket> buckets;

	/**
	 * An array of mask values. The index in the array is the #of bits of the
	 * hash code to be considered. The value at that index in the array is the
	 * mask to be applied to mask off to zero the high bits of the hash code
	 * which are to be ignored.
	 */
	static private final int[] masks;
	static {

		masks = new int[32];
		
		// Populate the array of masking values.
		for (int i = 0; i < 32; i++) {

			masks[i] = getMaskBits(i);

		}
	}
	
	// /**
	// * The current mask for the current {@link #globalHashBits}.
	// */
	// private int globalMask;

	/**
	 * Human friendly representation.
	 */
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(getClass().getName()); // @todo super.toString() with PO.
		sb.append("{bucketSize="+bucketSize);
		sb.append(",globalHashBits=" + globalHashBits);
		sb.append(",addrSpaceSize=" + addressMap.length);
//		sb.append(",addressMap="+Arrays.toString(addressMap));
		sb.append(",buckets="+buckets.size());
		sb.append("}");
//		// used to assign labels to pages.
//		final Map<Integer/*addr*/,String/*label*/> labels = new HashMap<Integer, String>(addressMap.length);
		// used to remember the visited pages.
		final Set<Integer/*addrs*/> visited = new LinkedHashSet<Integer>(addressMap.length);
		final Formatter f = new Formatter(sb);
		for (int i = 0; i < addressMap.length; i++) {
			final int addr = addressMap[i];
			final SimpleBucket b = getBucketAtStoreAddr(addr);
			f.format("\n%2d [%" + globalHashBits + "s] => (% 8d)", i, Integer
					.toBinaryString(maskOff(i, globalHashBits)), addr);
			if (visited.add(addr)) {
				// Show the bucket details the first time we visit it.
				sb.append(" [k=" + b.localHashBits + "] {");
				for (int j = 0; j < b.size; j++) {
					if (j > 0)
						sb.append(", ");
					sb.append(Integer.toString(b.data[j]));
				}
				sb.append("}");
			}
			// Invariant.
			assert b.localHashBits <= globalHashBits;
		}
		sb.append('\n');
		return sb.toString();
	}

	/**
	 * 
	 * @param initialCapacity
	 *            The initial capacity is the #of buckets which may be stored in
	 *            the hash table before it must be resized. It is expressed in
	 *            buckets and not tuples because there is not (in general) a
	 *            fixed relationship between the size of a bucket and the #of
	 *            tuples which can be stored in that bucket. This will be
	 *            rounded up to the nearest power of two.
	 * @param bucketSize
	 *            The #of int tuples which may be stored in a bucket.
	 * 
	 * @todo Options to govern overflow chaining policy so we can test when
	 *       overflows are not created, which is the simplest condition.
	 */
	public SimpleExtensibleHashMap(final int initialCapacity,
			final int bucketSize) {

		if (initialCapacity <= 0)
			throw new IllegalArgumentException();

		if (bucketSize <= 0)
			throw new IllegalArgumentException();

		this.bucketSize = bucketSize;

		/*
		 * Setup the hash table given the initialCapacity (in buckets). We need
		 * to find the first power of two which is GTE the initialCapacity.
		 */
		globalHashBits = getMapSize(initialCapacity);

		if (globalHashBits > 32) {
			/*
			 * The map is restricted to 32-bit hash codes so we can not address
			 * this many buckets.
			 */
			throw new IllegalArgumentException();
		}

		// // save the current masking value for the current #of global bits.
		// globalMask = masks[globalHashBits];

		/*
		 * Now work backwards to determine the size of the address space (in
		 * buckets).
		 */
		final int addressSpaceSize = 1 << globalHashBits;

		/*
		 * Allocate and initialize the address space. All indices are initially
		 * mapped onto the same bucket.
		 */
		addressMap = new int[addressSpaceSize];

		buckets = new ArrayList<SimpleBucket>(addressSpaceSize/* initialCapacity */);

		// Note: the local bits of the first bucket is set to ZERO (0).
		buckets.add(new SimpleBucket(0/* localHashBits */, bucketSize));

	}

	// private void toString(StringBuilder sb) {
	// sb.append("addressMap:"+Arrays.toString(addressMap));
	// }

	/**
	 * The hash of an int key is that int.
	 * 
	 * @todo Consider the {@link BloomFilter} hash code logic. It is based on a
	 *       table of functions. It might be a good fit here.
	 * 
	 * @todo Is is worth while to keep the key and the hash code together in a
	 *       small structure? It is inexpensive to mask off the bits that we do
	 *       not want to consider, but we need to avoid passing the masked off
	 *       hash code into routines which expect the full hash code.
	 */
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
	 * 
	 * @return The hash value considering only the lower <i>nbits</i>.
	 */
	static protected int maskOff(final int h, final int nbits) {

		if (nbits < 0 || nbits > 32)
			throw new IllegalArgumentException();

		final int v = h & masks[nbits];
		
		return v;

	}

	/**
	 * The bucket address given the hash code of a key.
	 * 
	 * @param h
	 *            The hash code of the key.
	 * 
	 * @todo Consider passing in the #of bits to be considered here. That way we
	 *       always pass around the full hash code but specify how many bits are
	 *       used to interpret it when calling this method.
	 */
	private int addrOf(final int h) {

		final int index = getIndexOf(h);

		return getAddressFromEntryIndex(index);

	}

	/**
	 * Return the store address from which the page for the given directory
	 * entry may be read from the backing store.
	 * 
	 * @param indexOf
	 *            The index into the address table.
	 * 
	 * @return The address of the bucket recorded in the address table at that
	 *         index.
	 */
	public int getAddressFromEntryIndex(final int indexOf) {
		
		return addressMap[indexOf];
		
	}

	/**
	 * Return the pre-allocated bucket having the given offset into the address table.
	 * 
	 * @param indexOf
	 *            The index into the address table.
	 * 
	 * @return The bucket.
	 */
	protected SimpleBucket getBucketFromEntryIndex(final int indexOf) {

		final int addr = getAddressFromEntryIndex(indexOf);
		
		return getBucketAtStoreAddr(addr);

	}

	/**
	 * Return the pre-allocated bucket having the given address.
	 * 
	 * @param addr
	 *            The address of the bucket on the backing store.
	 * 
	 * @return The bucket.
	 */
	protected SimpleBucket getBucketAtStoreAddr(final int addr) {

		return buckets.get(addr);

	}
	
	/**
	 * The #of hash bits which are being used by the address table.
	 */
	public int getGlobalHashBits() {

		return globalHashBits;

	}

	/**
	 * The size of the address space is <code>2^{@link #globalHashBits}</code>.
	 */
	public int getAddressSpaceSize() {

		return addressMap.length;

	}

	/**
	 * The #of buckets backing the map. This is never less than one and never
	 * greater than the size of the address space.
	 */
	public int getBucketCount() {

		return buckets.size();

	}

	/**
	 * The size of a bucket (the #of int32 values which may be stored in a
	 * bucket).
	 */
	public int getBucketSize() {

		return bucketSize;

	}

	/**
	 * Return the #of entries in the address map for a page having the given
	 * local depth. This is <code>2^(globalHashBits - localHashBits)</code>. The
	 * following table shows the relationship between the global hash bits (gb),
	 * the local hash bits (lb) for a page, and the #of directory entries for
	 * that page (nentries).
	 * 
	 * <pre>
	 * gb  lb   nentries
	 * 1	0	2
	 * 1	1	1
	 * 2	0	4
	 * 2	1	2
	 * 2	2	1
	 * 3	0	8
	 * 3	1	4
	 * 3	2	2
	 * 3	3	1
	 * 4	0	16
	 * 4	1	8
	 * 4	2	4
	 * 4	3	2
	 * 4	4	1
	 * </pre>
	 * 
	 * @param localHashBits
	 *            The local depth of the page in [0:{@link #globalHashBits}].
	 * 
	 * @return The #of directory entries for that page.
	 * 
	 * @throws IllegalArgumentException
	 *             if either argument is less than ZERO (0).
	 * @throws IllegalArgumentException
	 *             if <i>localHashBits</i> is greater than
	 *             <i>globalHashBits</i>.
	 */
	static protected int getSlotsForPage(final int globalHashBits,
			final int localHashBits) {

		if(localHashBits < 0)
			throw new IllegalArgumentException();

		if(globalHashBits < 0)
			throw new IllegalArgumentException();

		if(localHashBits > globalHashBits)
			throw new IllegalArgumentException();

		// The #of address map entries for this page.
		final int numSlotsForPage = (int) Math.pow(2d,
				(globalHashBits - localHashBits));

		return numSlotsForPage;

	}

	/**
	 * Return <code>true</code> iff the hash table contains the key.
	 * <p>
	 * Lookup: Compute h(K) and right shift (w/o sign extension) by i bits. Use
	 * this to index into the bucket address table. The address in the table is
	 * the bucket address and may be used to directly read the bucket.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return <code>true</code> iff the key was found.
	 */
	public boolean contains(final int key) {

		final int h = hash(key);

		final int addr = addrOf(h);

		final SimpleBucket b = getBucketAtStoreAddr(addr);

		return b.contains(h, key);

	}

	/**
	 * Insert the key into the hash table. Duplicates are allowed.
	 * <p>
	 * Insert: Per lookup. On overflow, we need to split the bucket moving the
	 * existing records (and the new record) into new buckets.
	 * 
	 * @see #split(int, int, SimpleBucket)
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @todo define a put() method which returns the old value (no duplicates).
	 *       this could be just sugar over contains(), delete() and insert().
	 */
	public void insert(final int key) {
		final int h = hash(key);
		final int addr = addrOf(h);
		final SimpleBucket b = getBucketAtStoreAddr(addr);
		if (b.insert(h, key)) {
			return;
		}
		// split the bucket and insert the record (recursive?)
		split(key, b);
	}

	/**
	 * Split the bucket, adjusting the address map iff necessary. How this
	 * proceeds depends on whether the hash #of bits used in the bucket is equal
	 * to the #of bits used to index into the bucket address table. There are
	 * two cases:
	 * <p>
	 * Case 1: If {@link #globalHashBits} EQ the
	 * {@link SimpleBucket#localHashBits}, then the bucket address table is out
	 * of space and needs to be resized.
	 * <p>
	 * Case 2: If {@link #globalHashBits} is GT
	 * {@link SimpleBucket#localHashBits}, then there will be at least two
	 * entries in the bucket address table which point to the same bucket. One
	 * of those entries is relabeled. The record is then inserted based on the
	 * new #of hash bits to be considered. If it still does not fit, then either
	 * handle by case (1) or case (2) as appropriate.
	 * <p>
	 * Note that records which are in themselves larger than the bucket size
	 * must eventually be handled by: (A) using an overflow record; (B) allowing
	 * the bucket to become larger than the target page size (using a larger
	 * allocation slot or becoming a blob); or (C) recording the tuple as a raw
	 * record and maintaining only the full hash code of the tuple and its raw
	 * record address in the bucket (this would allow us to automatically
	 * promote long literals out of the hash bucket and a similar approach might
	 * be used for a B+Tree leaf, except that a long key will still cause a
	 * problem [also, this implies that deleting a bucket or leaf on the
	 * unisolated index of the RWStore might require a scan of the IRaba to
	 * identify blob references which must also be deleted, so it makes sense to
	 * track those as part of the bucket/leaf metadata).
	 * 
	 * @param h
	 *            The key which triggered the split.
	 * @param bold
	 *            The bucket lacking sufficient room for the key which triggered
	 *            the split.
	 * 
	 * @todo caller will need an exclusive lock if this is to be thread safe.
	 * 
	 * @todo Overflow buckets (or oversize buckets) are required when all hash
	 *       bits considered by the local bucket are the same, when all keys in
	 *       the local bucket are the same, and when the record to be inserted
	 *       is larger than the bucket. In order to handle these cases we may
	 *       need to more closely integrate the insert/split logic since
	 *       detecting some of these cases requires transparency into the
	 *       bucket.
	 * 
	 *       FIXME The caller could decide to switch to a larger page size or
	 *       chain overflow pages together in order to increase storage
	 *       utilization or handle buckets having large populations of identical
	 *       keys (or keys with the same int32 hash code). [This decision must
	 *       be made before we decide to split.]
	 * 
	 *       FIXME The caller should handle the promotion of large tuples to raw
	 *       records when they are inserted, so we do not need to handle that
	 *       here either.
	 */
	private void split(final int key, final SimpleBucket bold) {

		if (log.isDebugEnabled())
			log.debug("globalBits=" + globalHashBits + ",localHashBits="
					+ bold.localHashBits + ",key=" + key);

		if (globalHashBits < bold.localHashBits) {
			// This condition should never arise.
			throw new AssertionError();
		}

		if (globalHashBits == bold.localHashBits) {
			/*
			 * The address table is out of space and needs to be resized.
			 */
			doubleAddressSpaceAndSplitBucket(key, bold);
			// fall through
		}

		if (globalHashBits > bold.localHashBits) {
			/*
			 * Split the bucket.
			 */
			splitBucket(key, bold);
			// fall through.
		}

		/*
		 * Insert the key into the expanded hash table (this will insert into
		 * either the old or the new bucket, depending on the hash code for the
		 * key).
		 */
		{
			if (log.isDebugEnabled())
				log.debug("retrying insert: key=" + key);
			// the hash value of the key.
			final int h = hash(key);
			// the address of the bucket for that hash code.
			final int addr = addrOf(h);
			// the bucket for that address.
			final SimpleBucket btmp = getBucketAtStoreAddr(addr);
			if (btmp.insert(h, key)) {
				// insert was successful.
				return;
			}
			/*
			 * FIXME This could be a variety of special conditions which need to
			 * be handled, especially all keys have the same value or the same
			 * int32 hash code or the tuple is too large for the bucket. Those
			 * conditions all need to be handled before requested a split. Since
			 * the caller has to handle all of this, we could just return and
			 * let the caller re-do the insert.
			 */
			log
					.fatal("Split of bucket did not map space available for new key: key="
							+ key + ", table=" + toString());
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * The address table is out of space and needs to be resized.
	 * <p>
	 * Let {@link #globalHashBits} := {@link #globalHashBits} + 1. This doubles
	 * the size of the bucket address table. Each original entry becomes two
	 * entries in the new table. For the specific bucket which is to be split, a
	 * new bucket is allocated and the 2nd bucket address table for that entry
	 * is set to the address of the new bucket. The tuples are then assigned to
	 * the original bucket and the new bucket by considering the additional bit
	 * of the hash code. Assuming that all keys are distinct, then one split
	 * will always be sufficient unless all tuples in the original bucket have
	 * the same hash code when their (i+1)th bit is considered (this can also
	 * occur if duplicate keys are allow). In this case, we resort to an
	 * "overflow" bucket (alternatively, the bucket is allowed to be larger than
	 * the target size and gets treated as a blob).
	 * <p>
	 * Note: The caller must re-do the insert.
	 */
	private void doubleAddressSpaceAndSplitBucket(final int key,
			final SimpleBucket bold) {
		if (log.isDebugEnabled())
			log.debug("key=" + key);
		// the hash value of the key.
		final int h = hash(key);
//		final int oldGlobalHashBits = globalHashBits;
		// The size of the address space before we double it.
		final int oldAddrSize = getAddressSpaceSize();
		/*
		 * The index into the address space for the hash key given the #of bits
		 * considered before we double the address space.
		 */
		final int oldIndex = getIndexOf(h);
		// // The address of the bucket to be split.
		// final int addrOld = addressMap[oldIndex];
		/*
		 * The address into the new address map of the new bucket (once it gets
		 * created).
		 * 
		 * Note: We find that entry by adding the size of the old address table
		 * to the index within the table of the bucket to be split.
		 */
		final int newIndex = oldIndex + oldAddrSize;
		// final int addrNew = addressMap[newIndex];
		// double the address space.
		doubleAddressSpace();
		/*
		 * Create a new bucket and wire it into the 2nd entry for the hash code
		 * for that key.
		 * 
		 * Note: Doubling the address space only gives us TWO (2) address table
		 * entries for a bucket. Therefore, if we wind up repeatedly inserting
		 * into the either of the created buckets then the address space will
		 * have to be double again soon. In order to counter this exponential
		 * expansion of the address space, it may be necessary to have the
		 * primary page either overflow into a chain or expand into a larger
		 * page.
		 */
		// The #of local hash bits _before_ the page is split.
		final int localHashBitsBefore = bold.localHashBits;
		final SimpleBucket bnew;
		{
			// Adjust the #of local bits to be considered.
			bold.localHashBits++;
			// The new bucket.
			bnew = new SimpleBucket(bold.localHashBits, bucketSize);
			// The address for the new bucket.
			final int addrBNew = buckets.size();
			// Add to the chain of buckets.
			buckets.add(bnew);
			// Update the address table to point to the new bucket.
			addressMap[newIndex] = addrBNew;
		}
		// Redistribute the tuples between the old and new buckets.
		redistributeTuples(bold, bnew, /*globalHashBits,*/ localHashBitsBefore);
		/*
		 * Note: The caller must re-do the insert.
		 */
	}

	/**
	 * Split a bucket having more than one reference to the bucket in the
	 * address table.
	 * <p>
	 * There will be at least two entries in the address table which point to
	 * this bucket. The #of entries depends on the global #of hash bits in use
	 * and the bucket local #of hash bits in use. It will be 2 if there is a
	 * difference of one between those values but can be more than 2 and will
	 * always be an even number. More precisely, there will be exactly 2^(
	 * {@link #globalHashBits}- {@link SimpleBucket#localHashBits} such entries.
	 * <p>
	 * Both the original bucket and the new bucket have their
	 * {@link SimpleBucket#localHashBits} incremented by one, but the
	 * {@link #globalHashBits} is unchanged. Of the entries in the bucket
	 * address table which used to point to the original bucket, the 1st half
	 * are left alone and the 2nd half are updated to point to the new bucket.
	 * The entries in the original bucket are rehashed and assigned based on the
	 * new #of hash bits to be considered to either the original bucket or the
	 * new bucket.
	 * <p>
	 * After invoking this method, the record is then inserted (by the caller)
	 * based on the new #of hash bits to be considered. The caller is
	 * responsible for detecting and handling cases which must be handled using
	 * overflow pages, etc.
	 * 
	 * FIXME Implement this next. It handles the simpler split case when we only
	 * need to redistribute the keys but do not need to double the address
	 * space.
	 * 
	 * @todo test when more than two references remain and recheck the logic for
	 *       updating the address table.
	 */
	private void splitBucket(final int key, final SimpleBucket bold) {
		if (log.isDebugEnabled())
			log.debug("key=" + key + ", globalHashBits=" + globalHashBits
					+ ", localHashBits=" + bold.localHashBits);
		assert globalHashBits - bold.localHashBits > 0;
		// The hash value of the key.
		final int h = hash(key);
		/*
		 * @todo add assert to verify that the correct #of address map entries
		 * were updated.
		 * 
		 * FIXME When we evict a dirty primary page, we need to update each of
		 * the entries in the address table which point to that page. This means
		 * that we need to compute how many such entries there are and the
		 * update those entries. [This also implies that the address table must
		 * hold weak/strong references much like the BTree nodes and leaves.]
		 * Likewise, when we evict an overflow page we need to update the
		 * predecessor in the chain. This means that we will wire in buckets
		 * which are chains of pages and evict the entire chain at once,
		 * starting with the last page in the chain so we can get the addresses
		 * serialized into the page. If the serialized page is too large, then
		 * we could wind up splitting the bucket (or increasing the page size)
		 * when writing out the chain. In fact, we could chose to coalesce the
		 * chain into a single large page and then let the RWStore blob it if
		 * necessary.
		 */
		/*
		 * Create a new bucket and wire it into the 2nd entry for the hash code
		 * for that key.
		 */
		// The address for the new bucket.
		final int addrBNew = buckets.size();
		// The new bucket.
		final SimpleBucket bnew;
		final int localHashBitsBefore = bold.localHashBits;
		{
			// the new bucket.
			bnew = new SimpleBucket(bold.localHashBits + 1, bucketSize);
			// Add to the chain of buckets.
			buckets.add(bnew);
		}
		// Hash code with only the local bits showing.
		final int localBitsHash = maskOff(h, bold.localHashBits);
		// The #of address map entries for this page.
		final int numSlotsForPage = (int) Math.pow(2d, globalHashBits
				- bold.localHashBits);
		// Loop over the upper 1/2 of those slots.
		for (int i = (numSlotsForPage / 2); i < numSlotsForPage; i++) {
			// Index into address table of an entry pointing to this page.
			final int entryIndex = (i << bold.localHashBits) + localBitsHash;
			// This entry is updated to point to the new page.
			addressMap[entryIndex] = addrBNew;
		}
		// adjust the #of local bits to be considered on the old bucket.
		bold.localHashBits++;
		// redistribute the tuples between the old and new buckets.
		redistributeTuples(bold, bnew, /*globalHashBits,*/ localHashBitsBefore);
		/*
		 * The caller must re-do the insert.
		 */
	}

	/**
	 * Redistribute the keys in the old bucket between the old and new bucket by
	 * considering one more bit in their hash values.
	 * <p>
	 * Note: The move has to be handled in a manner which does not have
	 * side-effects which put the visitation of the keys in the original bucket
	 * out of whack. The code below figures out which keys move and which stay
	 * and copies the ones that move in one step. It then goes back through and
	 * deletes all keys which are found in the new bucket from the original
	 * bucket.
	 * 
	 * @param bold
	 *            The old bucket.
	 * @param bnew
	 *            The new bucket.
	 * @param localHashBitsBefore
	 *            The #of local hash bits for the old bucket before the split.
	 *            This is used to decide whether the tuple is being moved to the
	 *            new bucket or left behind in the old bucket.
	 * 
	 * @todo As a pre-condition to splitting the bucket, we need to verify that
	 *       at least one key is not the same as the others in the bucket. If
	 *       all keys are the same, then we should have followed an overflow
	 *       path instead of a split path. [This needs to be tested by
	 *       insert().]
	 * 
	 * @todo There can be overflow pages for the bucket. Those pages also need
	 *       to be processed here. The overflow pages then need to be released
	 *       since they no longer have any data.
	 * 
	 * @todo The index of the page (its slot in the directory) should be part of
	 *       the transient metadata for the page, at which point it can be
	 *       removed from this method signature.
	 */
	private void redistributeTuples(final SimpleBucket bold,
			final SimpleBucket bnew, //final int globalHashBits,
			final int localHashBitsBefore) {
		/*
		 * First, run over the entries in the old page. Any entries which will
		 * be hashed to the new page with the modified address table are
		 * inserted into the new page. In order to avoid a requirement for an
		 * entry iterator which handles concurrent modification, we do not
		 * delete the entries from the old page as they are being copied.
		 * Instead, those entries will be deleted in a pass over the new page
		 * below.
		 * 
		 * Note: The decision concerning whether the tuple will remain in the
		 * original page or be moved into the new page depends on whether it
		 * would be inserted into the lower 1/2 of the directory entries (which
		 * will continue to point to the old page) or the upper 1/2 of the
		 * directory entries (which will point to the new page).
		 */
		{
			// The #of address map entries for this page.
			final int numSlotsForPage = getSlotsForPage(globalHashBits,
					localHashBitsBefore);
			final int addrSpaceSize = getAddressSpaceSize();
			// The threshold at which the key will be inserted into the new
			// page.
			final int newPageThreshold = addrSpaceSize>>1;//numSlotsForPage >> 1;
			// Iterator visiting the entries in the old page.
			final Iterator<Integer> eitr = bold.getEntries();
			// Loop over the tuples in the old page.
			while (eitr.hasNext()) {
				// A key from the original bucket.
				final int key = eitr.next();
				// The full hash code for that key.
				final int h = hash(key);
				// Mask off all but the global bits.
				final int h1 = maskOff(h, globalHashBits);
//				// Drop off the lower bits (w/o sign extension).
//				final int h2 = h1 >>> localHashBitsBefore;
				if (h1 >= newPageThreshold) {
					// Move the key to the new bucket.
					bnew.insert(h/* hash(key) */, key);
				}
			}
		}
		/*
		 * Now delete any keys which were moved to the new bucket.
		 */
		{
			// Iterator visiting the entries in the new page.
			final Iterator<Integer> eitr = bnew.getEntries();
			// Loop over the entries in the new page.
			while (eitr.hasNext()) {
				// a key from the new bucket.
				final int k1 = eitr.next();
				// delete the key from the old bucket.
				bold.delete(hash(k1), k1);
			}
		}
	}

	/**
	 * Doubles the address space.
	 * <p>
	 * This allocates a new address table and initializes it with TWO (2)
	 * identical copies of the current address table, one right after the other
	 * and increases {@link #globalHashBits} by ONE (1).
	 * <p>
	 * This operation preserves the current mapping of hash values into an
	 * address table when we consider one more bit in those hash values. For
	 * example, if we used to consider <code>3</code> bits of the hash value
	 * then we will now consider <code>4</code> bits. If the fourth bit of the
	 * hash value is ZERO (0) then it addresses into the first copy of the
	 * address table. If the fourth bit of the hash value is ONE (1) then it
	 * addresses into the second copy of the address table. Since the entries
	 * point to the same buckets as they did when we only considered
	 * <code>3</code> bits of the hash value the mapping of the keys onto the
	 * buckets is not changed by this operation.
	 */
	private void doubleAddressSpace() {

		if (log.isInfoEnabled())
			log.info("Doubling the address space: globalBits=" + globalHashBits
					+ ", addressSpaceSize=" + getAddressSpaceSize());

		final int oldLength = addressMap.length;

		// allocate a new address table which is twice a large.
		final int[] tmp = new int[oldLength << 1];

		/*
		 * Copy the current address table into the lower half of the new table.
		 */
		System.arraycopy(addressMap/* src */, 0/* srcPos */, tmp/* dest */,
				0/* destPos */, oldLength);

		/*
		 * Copy the current address table into the upper half of the new table.
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
	 * Delete the key from the hash table (in the case of duplicates, a random
	 * entry having that key is deleted).
	 * <p>
	 * Delete: Buckets may be removed no later than when they become empty and
	 * doing this is a local operation with costs similar to splitting a bucket.
	 * Likewise, it is clearly possible to coalesce buckets which underflow
	 * before they become empty by scanning the 2^(i-j) buckets indexed from the
	 * entries in the bucket address table using i bits from h(K). [I need to
	 * research handling deletes a little more, including under what conditions
	 * it is cost effective to reduce the size of the bucket address table
	 * itself.]
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return <code>true</code> iff a tuple having that key was deleted.
	 * 
	 * @todo return the deleted tuple.
	 * 
	 * @todo merge buckets when they underflow/become empty? (but note that we
	 *       do not delete anything from the hash map for a hash join, just
	 *       insert, insert, insert).
	 */
	public boolean delete(final int key) {
		final int h = hash(key);
		final int addr = addrOf(h);
		final SimpleBucket b = getBucketAtStoreAddr(addr);
		return b.delete(h, key);
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
	 * Return an iterator which visits all entries in the hash table.
	 */
	@SuppressWarnings("unchecked")
	public Iterator<Integer> getEntries() {
		final IStriterator sitr = new Striterator(buckets.iterator())
				.addFilter(new Expander() {
					private static final long serialVersionUID = 1L;

					@Override
					protected Iterator expand(final Object obj) {
						final SimpleBucket b = (SimpleBucket) obj;
						return b.getEntries();
					}
				});
		return (Iterator<Integer>) sitr;
	}
	
	/**
	 * Return an entry in the hash table having the given key. If there is more
	 * than one entry for that key, then any entry having that key may be
	 * returned.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return An entry having that key.
	 */
	public int getEntry(final int key) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Return a bit mask which reveals only the low N bits of an int32 value.
	 * 
	 * @param nbits
	 *            The #of bits to be revealed.
	 * @return The mask.
	 */
	static int getMaskBits(final int nbits) {

		if (nbits < 0 || nbits > 32)
			throw new IllegalArgumentException();

		// int mask = 1; // mask
		// int pof2 = 1; // power of two.
		// while (pof2 < nbits) {
		// pof2 = pof2 << 1;
		// mask |= pof2;
		// }

		int mask = 0;
		int bit;

		for (int i = 0; i < nbits; i++) {

			bit = (1 << i);

			mask |= bit;

		}

		// System.err.println(nbits +" : "+Integer.toBinaryString(mask));

		return mask;

	}

	/**
	 * Find the first power of two which is GTE the given value. This is used to
	 * compute the size of the address space (in bits) which is required to
	 * address a hash table with that many buckets.
	 */
	static int getMapSize(final int initialCapacity) {

		if (initialCapacity <= 0)
			throw new IllegalArgumentException();

		int i = 1;

		while ((1 << i) < initialCapacity)
			i++;

		return i;

	}

}
