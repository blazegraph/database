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
 * Created on Dec 1, 2010
 */
package com.bigdata.htree;

import java.lang.ref.Reference;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.concurrent.Memoizer;

/**
 * A simple (flat) directory for an extensible hashing.
 */
public class HashDirectory extends AbstractHashPage<HashDirectory> {
	
	private final transient static Logger log = Logger
			.getLogger(HashDirectory.class);

/* FIXME We need a data record (interface and implementations) for the directory
 * page.  The data record for a bucket is more similar to a B+Tree leaf than is
 * the data record for a directory to a B+Tree node.
 */
//    /**
//     * The data record. {@link MutableNodeData} is used for all mutation
//     * operations. {@link ReadOnlyNodeData} is used when the {@link Node} is
//     * made persistent. A read-only data record is automatically converted into
//     * a {@link MutableNodeData} record when a mutation operation is requested.
//     * <p>
//     * Note: This is package private in order to expose it to {@link Leaf}.
//     * 
//     * @todo consider volatile and private for {@link Node#data} and
//     *       {@link Leaf#data} with accessors and settors at package private
//     *       where necessary.
//     */
//    INodeData data;

	/**
	 * The #of hash code bits which are in use by the {@link #addressMap}. Each
	 * hash bucket also as a local #of hash bits. Given <code>i</code> is the
	 * #of global hash bits and <code>j</code> is the number of hash bits in
	 * some bucket, there will be <code>2^(i-j)</code> addresses which point to
	 * the same bucket.
	 */
	private int globalHashBits;

	/**
	 * The maximum number of directories entries which are permitted.
	 * 
	 * @todo This can be ignored for a hash tree (recursive directories). In
	 *       that case we are concerned with when to split the directory because
	 *       the page is full rather than an absolute maximum on the address
	 *       space size.
	 */
	private final int maximumCapacity;

	/**
	 * The address map. You index into this map using {@link #globalHashBits}
	 * out of the hash code for a probe key. The values are storage addresses
	 * for the backing {@link IRawStore}. The address will be {@link #NULL} if
	 * the corresponding child is dirty, in which case {@link #childRefs} will
	 * always have a {@link Reference} to the dirty child. This pattern is used
	 * in combination with either strong references or weak references and a
	 * ring buffer to manage the incremental eviction of dirty pages.
	 * 
	 * @todo make this into a private IDirectoryData record.
	 *       <p>
	 *       It seems likely that we want to also record the local depth for
	 *       each child in the IDataDirectory record and a flag indicating
	 *       whether the child is a bucket or a directory page.
	 */
	private long[] addressMap;

	/**
	 * <p>
	 * Weak references to child pages (may be directories or buckets). The
	 * capacity of this array depends on the #of global bits for the directory.
	 * </p>
	 * <p>
	 * Note: This should not be marked as volatile. Volatile does not make the
	 * elements of the array volatile, only the array reference itself. The
	 * field would be final except that we clear the reference when stealing the
	 * array or deleting the node.
	 * </p>
	 * 
	 * @todo document why package private (AbstractBTree.loadChild uses this but
	 *       maybe that method could be moved to Node).
	 */
    private transient/* volatile */Reference<AbstractHashPage<?>>[] childRefs;

	public String toString() {

		return super.toString();
		
	}

	/**
	 * Dumps the buckets in the directory along with metadata about the
	 * directory.
	 * 
	 * @param sb
	 *            Where to write the dump.
	 */
	protected void dump(final StringBuilder sb) {

		// used to remember the visited pages by their addresses (when non-NULL)
		final Set<Long/* addrs */> visitedAddrs = new LinkedHashSet<Long>();

		// used to remember the visited pages when they are transient.
		final Map<AbstractHashPage/* children */, Integer/* label */> visitedChildren = new LinkedHashMap<AbstractHashPage, Integer>();

		// used to format the address table.
		final Formatter f = new Formatter(sb);

		// scan through the address table.
		for (int index = 0; index < addressMap.length; index++) {

			boolean visited = false;

			long addr = addressMap[index];

			if (addr != NULL && !visitedAddrs.add(addr)) {

				visited = true;

			}

			HashBucket b = (HashBucket) (childRefs[index]).get();

			if (b != null && visitedChildren.containsKey(b)) {

				visited = true;

			} else {
				
				visitedChildren.put(b, index);
				
			}

			if(b == null) {
				
				// materialize the bucket.
				b = getBucketFromEntryIndex(index);
				
				addr = b.getIdentity();
				
			}
			
			/*
			 * The label will be either the storage address followed by "P" (for
			 * Persistent) or the index of the directory entry followed by "T"
			 * (for Transient).
			 */
			final String label = addr == 0L ? (visitedChildren.get(b) + "T")
					: (addr + "P");
			
			f.format("\n%2d [%" + globalHashBits + "s] => (%8s)", index,
					Integer.toBinaryString(HashTree.maskOff(index,
							globalHashBits)), label);

			if (!visited) {
			
				/*
				 * Show the bucket details the first time we visit it.
				 */
				
				// The #of local hash bits for the target page.
				final int localHashBits = b.getLocalHashBits();
				
				// The #of entries in this directory for that target page.
				final int nrefs = HashTree.pow2(globalHashBits
						- localHashBits);
				
				sb.append(" [k=" + b.getLocalHashBits() + ", n=" + nrefs
						+ "] {");

				final Iterator<Integer> eitr = b.getEntries();

				boolean first = true;
				
				while(eitr.hasNext()) {

					if (!first)
						sb.append(", ");
					
					sb.append(eitr.next()/*.getObject()*/);
				
					first = false;
					
				}
				
				sb.append("}");
				
			}
			
		}

		sb.append('\n');

	}
	
	/**
	 * Create a new mutable directory page.
	 * 
	 * @param htbl
	 * @param initialCapacity
	 *            The initial capacity is the #of buckets which may be stored in
	 *            the hash table before it must be resized. It is expressed in
	 *            buckets and not tuples because there is not (in general) a
	 *            fixed relationship between the size of a bucket and the #of
	 *            tuples which can be stored in that bucket. This will be
	 *            rounded up to the nearest power of two.
	 * @param maximumCapacity
	 * @param bucketSize
	 * 
	 * @todo both maximumCapacity and bucketSize will go away. The maximum
	 *       capacity will be replaced by a decision function for splitting the
	 *       directory page. The bucketSize will be replaced by a decision
	 *       function for splitting, overflowing, or growing the bucket page.
	 */
	@SuppressWarnings("unchecked")
	protected HashDirectory(final HashTree htbl,
			final int initialCapacity, final int maximumCapacity,
			final int bucketSize) {

		super(htbl, true /* dirty */);

		if (initialCapacity <= 0)
			throw new IllegalArgumentException();

		if (maximumCapacity < initialCapacity)
			throw new IllegalArgumentException();

		this.maximumCapacity = maximumCapacity;
		
		/*
		 * Setup the hash table given the initialCapacity (in buckets). We need
		 * to find the first power of two which is GTE the initialCapacity.
		 */
		globalHashBits = HashTree.getMapSize(initialCapacity);

		if (globalHashBits > 32) {
			
			/*
			 * The map is restricted to 32-bit hash codes so we can not address
			 * this many buckets.
			 */
			
			throw new IllegalArgumentException();
			
		}

		/*
		 * Determine the size of the address space (in buckets).
		 */
		final int addressSpaceSize = 1 << globalHashBits;

		/*
		 * Allocate and initialize the address space. All indices are initially
		 * mapped onto the same bucket.
		 */
		
		// references to children.
		childRefs = new Reference[addressSpaceSize];
		
		// store addresses for children (0L if dirty or not allocated).
		addressMap = new long[addressSpaceSize];

		// Note: the local bits of the first bucket is set to ZERO (0).
		final HashBucket b = new HashBucket(htbl, 0/* localHashBits */,
				bucketSize);

		for (int i = 0; i < addressSpaceSize; i++) {

			childRefs[i] = ((AbstractHashPage) b).self;
			
		}

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

		return HashTree.pow2(globalHashBits);

	}

	/**
	 * The index into the address table given that we use
	 * {@link #globalHashBits} of the given hash value.
	 * <p>
	 * Note: This is identical to maskOff(h,{@link #globalHashBits}).
	 */
	private int getIndexOf(final int h) {

		return HashTree.maskOff(h, globalHashBits);

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

		final int h = htbl.hash(key);

		return getBucketFromHash(h).contains(h, key);

	}

	/**
	 * Insert the key into the hash table. Duplicates are allowed.
	 * <p>
	 * Insert: Per lookup. On overflow, we need to split the bucket moving the
	 * existing records (and the new record) into new buckets.
	 * 
	 * @see #split(int, int, HashBucket)
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @todo rename as append() method. insert() should retain the semantics of
	 *       replacing the existing tuple for the key. might rename insert to
	 *       put().
	 * 
	 * @todo code paths for insert/append/delete need to use copy-on-write.
	 */
	public void insert(final int key) {

		final int h = htbl.hash(key);
		
		getBucketFromHash(h).insert(h, key);

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

		final int h = htbl.hash(key);
		
		return getBucketFromHash(h).delete(h, key);
		
	}

	/**
	 * Return the bucket which is mapped onto the given hash code.
	 * 
	 * @param h
	 *            The hash code.
	 *            
	 * @return The bucket.
	 * 
	 * @todo Could be bucket or directory when generalizing to hash trees.
	 */
	protected HashBucket getBucketFromHash(final int h) {

		return getBucketFromEntryIndex(getIndexOf(h));

	}

	/**
	 * Return the pre-allocated bucket having the given offset into the address
	 * table.
	 * 
	 * @param index
	 *            The index into the address table.
	 * 
	 * @return The bucket.
	 * 
	 * @todo Could be bucket or directory when generalizing to hash trees.
	 */
	protected HashBucket getBucketFromEntryIndex(final int index) {

		HashBucket b = (HashBucket) childRefs[index].get();
		
		if(b == null) {
		
			b = _getChild(index);

		}
		
		return b;

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
	 * 
	 * @deprecated This should be replaced by a means to verify that the
	 *             children for two indices are the same.
	 */
	protected long getAddressFromEntryIndex(final int indexOf) {
		
		return addressMap[indexOf];
		
	}
	
	/**
	 * 
	 * @param index
	 * @return
	 * 
	 *         FIXME This needs to use a {@link Memoizer} pattern to avoid race
	 *         conditions where a bucket could be loaded concurrently by another
	 *         thread [the code currently synchronizes on (this), but that is
	 *         too expensize as was demonstrated by the B+Tree Node class].
	 *         <p>
	 *         In fact, since there can be multiple references to the same child
	 *         and ALL of those references MUST be set when we read in the child
	 *         we have a different concurrency requirement than the B+Tree
	 *         unless we can use a different representation of the address table
	 *         such that we only represent the address/reference once and
	 *         address it in combination with the local depth of the child in
	 *         order to figure out what child belows at each logical index in
	 *         the address table.
	 */
	private HashBucket _getChild(final int index) {

		synchronized (this) {

			final long addr = addressMap[index];

			final HashBucket b = htbl.getBucketAtStoreAddr(addr);

			// Invariant.
			assert b.getLocalHashBits() <= globalHashBits;
			
			return b;

		}
		
	}

	@Override
	public void delete() throws IllegalStateException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/**
	 * Visit the distinct buckets.
	 * <p>
	 * Note: This is NOT thread-safe!
	 */
	public Iterator<HashBucket> buckets() {
		
		return new DistinctChildIterator();

	}

	/**
	 * Visits the distinct buckets in the directory.
	 * <p>
	 * Note: Buckets do not have assigned storage addresses until they are made
	 * persistent. Until then we have to rely on a {@link Reference} to the
	 * bucket. Thus, if the storage address is non-{@link #NULL}, then we can
	 * use the storage address to impose the distinct filter. Otherwise, we must
	 * use the bucket reference itself to impose distinct.
	 * 
	 * FIXME It would be much easier to write this if we knew the local depth of
	 * each child based on metadata in the directory (and also whether the child
	 * is a bucket or a directory). We could then compute the #of entries in the
	 * directory which will address the same child and use that to know which
	 * directory entries are duplicates [in fact, this suggests that we could
	 * compute the directory from the index of the first directory entry and the
	 * local depth of that entry, where that information was given exactly once
	 * for each distinct child, but note that some extendible hashing scheme
	 * rely on a regular directory structure to compute the offset of the
	 * correct child hash table for a given index in a parent directory.] We
	 * could always compensate for the added metadata in the directory by
	 * increasing its page size on the backing store.
	 */
	private class DistinctChildIterator implements Iterator<HashBucket> {

		private int current = 0;

		/**
		 * Used to impose distinct based on the address of the child (when
		 * non-NULL). @todo allocate on demand (or otherwise provide a fast path when there is a single bucket, which we can know by looking at the local depth of the first bucket).
		 */
		private final Set<Long> addrs = new HashSet<Long>();
		
		/**
		 * Used to impose distinct based on the reference of the child (when
		 * non-null).
		 */
		private final Set<HashBucket> buckets = new HashSet<HashBucket>();

		private DistinctChildIterator() {

		}

		private HashBucket nextBucket = null;

		public boolean hasNext() {

			while (nextBucket == null && current < addressMap.length) {
			
				final int index = current++;
				
				final long addr = addressMap[index];
				
				if (addr != NULL) {
				
					if (!addrs.add(addr)) {
						// Collection was not modified, so already visited.
						continue;
					}
					
					nextBucket = _getChild(index);
					
					break;
				
				}
				
				final HashBucket b = (HashBucket) childRefs[index].get();
				
				if (b != null) {
				
					if (!buckets.add(b)) {
						// Collection was not modified, so already visited.
						continue;
					}
					
					nextBucket = b;
					
					break;
					
				}

			}

			return nextBucket != null;
			
		}

		@Override
		public HashBucket next() {

			if (!hasNext())
				throw new NoSuchElementException();

			final HashBucket tmp = nextBucket;
			
			nextBucket = null;
			
			return tmp;

		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
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
	protected void doubleAddressSpace() {

		if (log.isInfoEnabled())
			log.info("Doubling the address space: globalBits=" + globalHashBits
					+ ", addressSpaceSize=" + getAddressSpaceSize());

		final int oldLength = addressMap.length;

		final int newLength = oldLength << 1;
		
		if (newLength > maximumCapacity) {

			/*
			 * The maximum permitted address space size would be exceeded.
			 */

			throw new UnsupportedOperationException("addressSpaceSize="
					+ oldLength + ", maximumAddressSpaceSize="
					+ maximumCapacity);

		}
		
		// allocate a new address table which is twice a large.
		final long[] tmp = new long[newLength];
		final Reference<AbstractHashPage<?>>[] tmp2 = new Reference[newLength];

		/*
		 * Copy the current address table into the lower half of the new table.
		 */
		System.arraycopy(addressMap/* src */, 0/* srcPos */, tmp/* dest */,
				0/* destPos */, oldLength);

		System.arraycopy(childRefs/* src */, 0/* srcPos */, tmp2/* dest */,
				0/* destPos */, oldLength);

		/*
		 * Copy the current address table into the upper half of the new table.
		 */
		System.arraycopy(addressMap/* src */, 0/* srcPos */, tmp/* dest */,
				oldLength/* destPos */, oldLength);

		System.arraycopy(childRefs/* src */, 0/* srcPos */, tmp2/* dest */,
				oldLength/* destPos */, oldLength);

		// Replace the address table.
		addressMap = tmp;
		childRefs = tmp2;

		// Consider one more bit in the hash value of the keys.
		globalHashBits += 1;

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
	protected void doubleAddressSpaceAndSplitBucket(final int key,
			final HashBucket bold) {

		if (log.isDebugEnabled())
			log.debug("key=" + key);
		
		// the hash value of the key.
		final int h = htbl.hash(key);

		// The size of the address space before we double it.
		final int oldAddrSize = getAddressSpaceSize();

		/*
		 * The index into the address space for the hash key given the #of bits
		 * considered before we double the address space.
		 */
		final int oldIndex = getIndexOf(h);

		/*
		 * The address into the new address map of the new bucket (once it gets
		 * created).
		 * 
		 * Note: We find that entry by adding the size of the old address table
		 * to the index within the table of the bucket to be split.
		 */
		final int newIndex = oldIndex + oldAddrSize;
		
		// double the address space
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
		final int localHashBitsBefore = bold.getLocalHashBits();
		final HashBucket bnew;
		{
			// Adjust the #of local bits to be considered.
			bold.setLocalHashBits(bold.getLocalHashBits() + 1);
			// The new bucket.
			bnew = new HashBucket(htbl, bold.getLocalHashBits(), bold.entries.length/* bucketSize */);
//			// The address for the new bucket.
//			final int addrBNew = htbl.buckets.size();
			// Add to the chain of buckets.
//			htbl.buckets.add(bnew);
			childRefs[newIndex] = ((AbstractHashPage)bnew).self;
//			// Update the address table to point to the new bucket.
//			addressMap[newIndex] = addrBNew;
			addressMap[newIndex] = NULL;
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
	 * {@link #globalHashBits}- {@link HashBucket#localHashBits} such entries.
	 * <p>
	 * Both the original bucket and the new bucket have their
	 * {@link HashBucket#localHashBits} incremented by one, but the
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
	protected void splitBucket(final int key, final HashBucket bold) {
		if (log.isDebugEnabled())
			log.debug("key=" + key + ", globalHashBits=" + globalHashBits
					+ ", localHashBits=" + bold.getLocalHashBits());
		assert globalHashBits - bold.getLocalHashBits() > 0;
		// The hash value of the key.
		final int h = htbl.hash(key);
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
//		// The address for the new bucket.
//		final long addrBNew = htbl.buckets.size();
		// The new bucket.
		final HashBucket bnew;
		final int localHashBitsBefore = bold.getLocalHashBits();
		{
			// the new bucket.
			bnew = new HashBucket(htbl, bold.getLocalHashBits() + 1,
					bold.entries.length/* bucketSize */);
//			// Add to the chain of buckets.
//			htbl.buckets.add(bnew);
		}
		// Hash code with only the local bits showing.
		final int localBitsHash = HashTree.maskOff(h, bold.getLocalHashBits());
		// The #of address map entries for this page.
		final int numSlotsForPage = HashTree.pow2(globalHashBits - bold.getLocalHashBits());
		// Loop over the upper 1/2 of those slots.
		for (int i = (numSlotsForPage / 2); i < numSlotsForPage; i++) {
			// Index into address table of an entry pointing to this page.
			final int entryIndex = (i << bold.getLocalHashBits()) + localBitsHash;
			// This entry is updated to point to the new page.
//			addressMap[entryIndex] = addrBNew;
			childRefs[entryIndex] = ((AbstractHashPage)bnew).self;
			addressMap[entryIndex] = 0L;
		}
		// adjust the #of local bits to be considered on the old bucket.
		bold.setLocalHashBits(bold.getLocalHashBits() + 1);
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
	private void redistributeTuples(final HashBucket bold,
			final HashBucket bnew, //final int globalHashBits,
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
//			final int numSlotsForPage = getSlotsForPage(globalHashBits,
//					localHashBitsBefore);
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
				final int h = htbl.hash(key);
				// Mask off all but the global bits.
				final int h1 = HashTree.maskOff(h, globalHashBits);
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
				bold.delete(htbl.hash(k1), k1);
			}
		}
	}

	private void merge(final int h, final HashBucket b) {
		throw new UnsupportedOperationException();
	}

}
