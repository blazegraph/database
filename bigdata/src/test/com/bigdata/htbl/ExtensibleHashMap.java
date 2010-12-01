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

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.ISimpleBTree;
import com.bigdata.btree.Node;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * An implementation of an extensible hash map using a 32 bit hash code and a
 * fixed length int[] for the bucket. The keys are int32 values. The data stored
 * in the hash map is just the key. Buckets provide a perfect fit for N keys.
 * This is used to explore the dynamics of the extensible hashing algorithm
 * using some well known examples.
 * </p>
 * <h2>Extensible Hashing</h2>
 * <p>
 * Extensible (or extendable) hashing uses a directory and pages (or buckets) to
 * store the data. Directories make it possible to share pages, which can
 * improve the storage efficiency, but directory schemes can suffer from
 * exponential growth when the hash values are not uniformly distributed (a
 * variety of schemes may be used to compensate for that growth, including
 * overflow pages, multi-level directories, better hash functions, etc).
 * Extensible hashing uses a global depth, <i>d</i>, which corresponds to the
 * <i>d</i>-bits of the hash value used to select the directory entry. Each
 * bucket has a local depth which is used to track the #of directory entries
 * which target the same bucket. When an insert would fail due to insufficient
 * space in the target bucket, the bucket must either be split, increased in
 * size, or linked with an overflow page. If the directory has only a single
 * reference to a given page, then the directory must be doubled when the page
 * is split. Overflow pages can be used to defer doubling the size of the
 * directory (and are required when all hash bits are the same, such as when the
 * keys are identical), but overflow pages can degrade point test performance
 * since a miss on the primary bucket will cause a read through to the page(s)
 * of the overflow chain. Increasing the size of the bucket can also be used to
 * defer the doubling of the address space (or handle duplicate keys) and can
 * translate into better IO efficiency. Extensible hash trees use recursive
 * directory structures. Recursive directories essentially allow a hash table to
 * be imposed on the overflow pages which would otherwise be associated with the
 * primary page and thereby increase the efficiency of access to hash values
 * with skewed distributions. Order preserving hash functions may be used to
 * create hash indices which cluster keys (which can improve efficiency at all
 * levels of the memory hierarchy) and support range scans. Order preserving
 * multi-dimensional extensible hashing is a generalization of extensible
 * hashing suitable for spatial data and clustered aggregations. The design of
 * the directory (and handling of split/overflow decision) for both single and
 * multi-dimensional extensible hashing can have an enormous impact on the size
 * of the directory and the storage and retrieval efficiency of the index.
 * </p>
 * <h2>Implementation Design</h2>
 * <p>
 * </p>
 * <p>
 * This implementation is not thread-safe. I have not attempted to provide for
 * visibility guarantees when resizing the map and I have not attempted to
 * provide for concurrent updates. The implementation exists solely to explore
 * the extensible hashing algorithm.
 * </p>
 * 
 * @todo Name htable implies single level directory where htree implies a
 *       multi-level directory. Figure out the directory scheme and lock in the
 *       name for the class.
 * 
 * @todo Backfill in the notes on the implementation design as it gets locked
 *       in.
 * 
 * @todo We can not directly implement {@link Map} unless the hash table is
 *       configured to NOT permit duplicates.
 * 
 * @todo Integrate a ring buffer for retention of frequently accessed pages per
 *       the weak reference policy observed by the BTree with touch()
 */
public class ExtensibleHashMap 
implements ISimpleBTree // @todo rename ISimpleBTree interface
// @todo implement IAutoBoxBTree and rename IAutoBoxBTree interface.
// @todo implements IRangeQuery (iff using order preserving hashing)
, HashFunction
{

	private final transient static Logger log = Logger
			.getLogger(ExtensibleHashMap.class);

//	/**
//	 * The buckets. The first bucket is pre-allocated when the address table is
//	 * setup and all addresses in the table are initialized to point to that
//	 * bucket. Thereafter, buckets are allocated when a bucket is split.
//	 * 
//	 * @todo This needs to become an {@link IRawStore} reference, but first we
//	 *       need to provide {@link Reference}s from the directory to the
//	 *       buckets and then we can allow for persistence of dirty pages.
//	 */
//	protected final ArrayList<SimpleBucket> buckets;

	/**
	 * The backing store.
	 */
	private final IRawStore store;

	/**
	 * The root directory. This is replaced each time copy-on-write triggers a
	 * cascade of updates.
	 * <p>
	 * This hard reference is cleared to <code>null</code> if an index is
	 * {@link #close() closed}. {@link #getRoot()} automatically uses
	 * {@link #reopen()} to reload the root so that closed indices may be
	 * transparently made ready for further use (indices are closed to reduce
	 * their resource burden, not to make their references invalid). The
	 * {@link AbstractHashPage} and derived classes <em>assume</em> that the
	 * root is non-null. This assumption is valid if {@link #close()} is invoked
	 * by the application in a manner consistent with the single-threaded
	 * contract for the index.
	 * <p>
	 * Note: This field MUST be marked as [volatile] in order to guarantee
	 * correct semantics for double-checked locking in {@link #reopen()}.
	 * 
	 * @see http://en.wikipedia.org/wiki/Double-checked_locking
	 */
    protected volatile HashDirectory root;

	/**
	 * The root directory. The root is replaced each time copy-on-write triggers
	 * a cascade of updates.
	 * <p>
	 * The hard reference to the root node is cleared if the index is
	 * {@link #close() closed}. This method automatically {@link #reopen()}s the
	 * index if it is closed, making it available for use.
	 */
    final protected HashDirectory getRoot() {

        // make sure that the root is defined.
        if (root == null)
            reopen();

        return root;

    }
	
    /**
     * This is part of a {@link #close()}/{@link #reopen()} protocol that may
     * be used to reduce the resource burden of an {@link AbstractBTree}. The
     * method delegates to {@link #_reopen()} if double-checked locking
     * demonstrates that the {@link #root} is <code>null</code> (indicating
     * that the index has been closed). This method is automatically invoked by
     * a variety of methods that need to ensure that the index is available for
     * use.
     * 
     * @see #close()
     * @see #isOpen()
     * @see #getRoot()
     * 
     * @todo import the rest of this protocol.
     */
    final protected void reopen() {

        if (root == null) {

            /*
             * reload the root node.
             * 
             * Note: This is synchronized to avoid race conditions when
             * re-opening the index from the backing store.
             * 
             * Note: [root] MUST be marked as [volatile] to guarantee correct
             * semantics.
             * 
             * See http://en.wikipedia.org/wiki/Double-checked_locking
             */

            synchronized(this) {
            
                if (root == null) {

                    // invoke with lock on [this].
                    _reopen();
                    
                }
                
            }

        }

    }

	/**
	 * This method is invoked by {@link #reopen()} once {@link #root} has been
	 * show to be <code>null</code> with double-checked locking. When invoked in
	 * this context, the caller is guaranteed to hold a lock on <i>this</i>.
	 * This is done to ensure that at most one thread gets to re-open the index
	 * from the backing store.
	 */
    /*abstract*/ protected void _reopen() {
    	// @todo ...
    }

	/*
	 * Checkpoint metadata.
	 */

    /**
     * The height of the index. The height is the #of levels minus one.
     */
	final public int getHeight() {
        
        return 1;
        
    }

    /**
     * The #of directory pages in the index.
     */
    final public int getDirectoryCount() {
        
        return 1;
        
    }

    /**
     * The #of buckets in the index.
     */
    final public int getBucketCount() {
        
        return nbuckets;
//    	return buckets.size();
        
    }
    protected int nbuckets; // @todo init from checkpoint

	/**
	 * The #of entries (aka tuples) in the index. When the index supports delete
	 * markers, this value also includes tuples which have been marked as
	 * deleted but not yet purged from the index.
	 */
    final public int getEntryCount() {
        
        return nentries;
        
    }
    protected int nentries; // @todo init from checkpoint

    /**
     * The backing store.
     */
    final public IRawStore getStore() {

        return store;

    }
	
	/*
	 * Static utility methods and data.
	 */

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
	 * Return <code>2^n</code>.
	 * 
	 * @param n
	 *            The exponent.
	 *            
	 * @return The result.
	 */
	static protected int pow2(final int n) {
		
//		return (int) Math.pow(2d, n);
		return 1 << n;
		
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
		final int numSlotsForPage = pow2(globalHashBits - localHashBits);

		return numSlotsForPage;

	}

	/**
	 * Human friendly representation.
	 */
	public String toString() {

		final StringBuilder sb = new StringBuilder();
		
		sb.append(getClass().getName());

		sb.append("{globalHashBits=" + getGlobalHashBits());

		sb.append(",addrSpaceSize=" + getAddressSpaceSize());

		sb.append(",entryCount=" + getEntryCount());

		sb.append(",bucketCount=" + getBucketCount());

		// @todo checkpoint record.
		
		// @todo index metadata record.
		
		sb.append("}");
		
		return sb.toString();

	}

	/**
	 * Dump a representation of the hash index.
	 */
	public String dump() {
		
		final StringBuilder sb = new StringBuilder();
		
		// basic information.
		sb.append(toString());
		
		// Dump the data in the index.
		getRoot().dump(sb);
		
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
	 * @todo Configuration options:
	 *       <p>
	 *       Split, Grow, or Overflow decision function (this subsumes the
	 *       bucketSize parameter since a decision function could consider only
	 *       the #of keys on the page).
	 *       <p>
	 *       Initial and maximum directory size (the latter only for hash tables
	 *       rather than hash trees). [It is perfectly Ok to set the #of global
	 *       bits initially to the #of distinctions which can be persisted in a
	 *       directory page.]
	 *       <p>
	 *       Hash function. Order preserving hashing requires more than just an
	 *       appropriate hashing function. The directory may need to be managed
	 *       differently and pages may need to be chained. Also, hash functions
	 *       with more than 32-bits will require other changes (hash:int to
	 *       hash:Object, some tables must be larger, etc.)
	 *       <p>
	 *       Decision function to inline the key/value of the tuple or to write
	 *       them as a raw record and link to that record using its in-store
	 *       address.
	 *       <p>
	 *       Directory and page encoders.
	 *       <p>
	 *       IRaba implementations for the keys and values (including an option
	 *       for a pure append binary representation with compaction of deleted
	 *       tuples). [Some hash table schemes depend on page transparency for
	 *       the dictionary.] 
	 */
	public ExtensibleHashMap(final int initialCapacity,
			final int bucketSize) {

		// @todo pass in the store reference per AbstractBTree.
		this.store = new SimpleMemoryRawStore();

		root = new HashDirectory(this, initialCapacity,
				Bytes.kilobyte32 * 4/* maximumCapacity */, bucketSize);

//		/*
//		 * Now work backwards to determine the size of the address space (in
//		 * buckets).
//		 */
//		final int addressSpaceSize = SimpleExtensibleHashMap.pow2(root
//				.getGlobalHashBits());
//
//		buckets = new ArrayList<SimpleBucket>(addressSpaceSize/* initialCapacity */);
//
//		// Note: the local bits of the first bucket is set to ZERO (0).
//		buckets.add(new SimpleBucket(this, 0/* localHashBits */, bucketSize));

	}

	/**
	 * @todo Generalize w/ hash function from index metadata (the current
	 *       implementation assumes that the hash of an int key is that int).
	 */
	public int hash(final Object o) {
		return ((Integer)o).intValue();
	}

	/**
	 * @deprecated by {@link #hash(Object)} or maybe hash(byte[]).
	 */
	public int hash(final int key) {

		return key;

	}

	/**
	 * Return the pre-allocated bucket having the given address.
	 * <p>
	 * Note: The caller is responsible for ensuring that duplicate instances of
	 * a given bucket or directory are not loaded from the backing store for the
	 * same hash index object.
	 * 
	 * @param addr
	 *            The address of the bucket on the backing store.
	 * 
	 * @return The bucket.
	 */
	protected HashBucket getBucketAtStoreAddr(final long addr) {

		// @todo various timing and counter stuff.
		
		return (HashBucket) SerializerUtil.deserialize(store.read(addr));

	}

	/**
	 * The #of hash bits which are being used by the address table.
	 */
	public int getGlobalHashBits() {

		return getRoot().getGlobalHashBits();

	}

	/**
	 * The size of the address space is <code>2^{@link #globalHashBits}</code>.
	 */
	public int getAddressSpaceSize() {

		return pow2(getGlobalHashBits());

	}

	/*
	 * IAutoBoxBTree
	 * 
	 * @todo API Alignment with IAutoBoxBTree
	 */
	
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

		return getRoot().contains(key);

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
	 *       replacing the existing tuple for the key.  might rename insert to
	 *       put().
	 */
	public void insert(final int key) {

		getRoot().insert(key);

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

		return getRoot().delete(key);
		
	}

	/*
	 * ISimpleBTree
	 */
	
	@Override
	public boolean contains(byte[] key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte[] insert(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] lookup(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] remove(byte[] key) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * Core CRUD methods.
	 * 
	 * @todo The core B+Tree methods accept and return ITuple objects. The core
	 * hash index methods should do the same thing.
	 */

	/**
	 * Visit the buckets.
	 * <p>
	 * Note: This is NOT thread-safe!
	 */
	public Iterator<HashBucket> buckets() {

		return getRoot().buckets();

	}

	/**
	 * Return an iterator which visits all entries in the hash table.
	 */
	@SuppressWarnings("unchecked")
	public Iterator<Integer> getEntries() {
		final IStriterator sitr = new Striterator(buckets())
				.addFilter(new Expander() {
					private static final long serialVersionUID = 1L;

					@Override
					protected Iterator expand(final Object obj) {
						return ((HashBucket) obj).getEntries();
					}
				});
		return (Iterator<Integer>) sitr;
	}

    /**
     * Create the reference that will be used by a {@link Node} to refer to its
     * children (nodes or leaves).
     * 
     * @param child
     *            A node.
     * 
     * @return A reference to that node.
     * 
     * @see AbstractNode#self
     * @see SoftReference
     * @see WeakReference
     */
    final <T extends AbstractHashPage<T>> Reference<AbstractHashPage<T>> newRef(
            final AbstractHashPage<T> child) {
        
        /*
         * Note: If the parent refers to its children using soft references the
         * the presence of the parent will tend to keep the children wired into
         * memory until the garbage collector is forced to sweep soft references
         * in order to make room on the heap. Such major garbage collections
         * tend to make the application "hesitate".
         * 
         * @todo it may be that frequently used access paths in the btree should
         * be converted dynamically from a weak reference to soft reference in
         * order to bias the garbage collector to leave those paths alone. if we
         * play this game then we should limit the #of soft references and make
         * the node choose among its children for those it will hold with a soft
         * reference so that the notion of frequent access is dynamic and can
         * change as the access patterns on the index change.
         */

        if (store == null) {

            /*
             * Note: Used for transient BTrees.
             */
            
            return new HardReference<AbstractHashPage<T>>(child);
            
        } else {
        
            return new WeakReference<AbstractHashPage<T>>( child );
//        return new SoftReference<AbstractNode>( child ); // causes significant GC "hesitations".
        }
        
        
    }

	/**
	 * A class that provides hard reference semantics for use with transient
	 * indices. While the class extends {@link WeakReference}, it internally
	 * holds a hard reference and thereby prevents the reference from being
	 * cleared. This approach is necessitated on the one hand by the use of
	 * {@link Reference} objects for linking directories and buckets, etc. and
	 * on the other hand by the impossibility of defining your own direct
	 * subclass of {@link Reference} (a runtime security manager exception will
	 * result).
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 *         
	 * @param <T>
	 */
    static class HardReference<T> extends WeakReference<T> {
        
        final private T ref;
        
        HardReference(final T ref) {

            super(null);
            
            this.ref = ref;
            
        }
        
        /**
         * Returns the hard reference.
         */
        public T get() {
            
            return ref;
            
        }
        
        /**
         * Overridden as a NOP.
         */
        public void clear() {

            // NOP
            
        }
        
    }
    
}
