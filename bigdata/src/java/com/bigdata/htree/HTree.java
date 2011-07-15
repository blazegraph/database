/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Apr 19, 2011
 */

package com.bigdata.htree;

import java.io.PrintStream;
import java.lang.ref.Reference;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.AbstractTuple;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.LeafTupleIterator;
import com.bigdata.btree.Node;
import com.bigdata.btree.PO;
import com.bigdata.btree.data.DefaultLeafCoder;
import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.htree.raba.MutableKeyBuffer;
import com.bigdata.htree.raba.MutableValueBuffer;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IRawStore;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * An mutable persistence capable extensible hash tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO It should be possible to define an native int32 hash table in
 *          parallel to the unsigned byte[] hash table simply by having an
 *          alternative descent passing an int32 key all the way down and using
 *          the variant of getBits() method which operates on the int32 values.
 *          We could also optimize the storage and retrieval of the int32 keys,
 *          perhaps with a custom mutable bucket page and mutable bucket data
 *          implementation for int32 keys. This optimization is likely to be
 *          quite worth while as the majority of use cases for the hash tree use
 *          int32 keys.
 * 
 *          TODO It is quite possible to define a range query interface for the
 *          hash tree. You have to use an order preserving hash function, which
 *          is external to the HTree implementation. Internally, the HTree must
 *          either double-link the pages or crawl the directory structure.
 * 
 *          TODO The keys should be declared as a computed key based on the data
 *          fields in the record. The {@link HTree} supports arbitrary bit
 *          length keys, but can be optimized for int32 keys easily enough.
 */
public class HTree extends AbstractHTree 
//	implements 
//	IIndex, 
//  ISimpleBTree//, IAutoboxBTree, ILinearList, IBTreeStatistics, ILocalBTreeView
//  IRangeQuery
{

    private static final transient Logger log = Logger.getLogger(HTree.class);

    /**
     * The #of bits of distinction to be made each time we split a directory
     * page in the {@link HTree}. See
     * {@link #splitDirectoryPage(DirectoryPage, int, AbstractPage)} for a write
     * up on this.
     */
    /*private*/ final int splitBits;
    
    /*
     * metadata about the index.
     * 
     * @todo this data should be rolled into the IndexMetadata object.
     */
    private final boolean versionTimestamps;
    private final boolean deleteMarkers;
    private final boolean rawRecords;

	/**
	 * The #of {@link DirectoryPage} in the {@link HTree}. This is ONE (1) for a
	 * new {@link HTree}.
	 */
	protected long nnodes;

	/**
	 * The #of {@link BucketPage}s in the {@link HTree}. This is one (1) for a
	 * new {@link HTree} (one directory page and one bucket page).
	 */
	protected long nleaves;

	/**
	 * The #of entries in the {@link HTree}. This is ZERO (0) for a new
	 * {@link HTree}.
	 */
	protected long nentries;
    
    final public long getNodeCount() {
        
        return nnodes;
        
    }

    final public long getLeafCount() {
        
        return nleaves;
        
    }

    final public long getEntryCount() {
        
        return nentries;
        
    }

	public boolean isReadOnly() {
		return false; // TODO set by ctor.
	}

	/**
	 * The root of the {@link HTree}. This is always a {@link DirectoryPage}.
	 * <p>
	 * The hard reference to the root node is cleared if the index is
	 * {@link #close() closed}. This method automatically {@link #reopen()}s the
	 * index if it is closed, making it available for use.
	 */
    final protected DirectoryPage getRoot() {

        // make sure that the root is defined.
        if (root == null)
            reopen();

        return root;

    }
    
    // TODO implement close/reopen protocol per AbstractBTree.
    protected void reopen() {
    	
    	throw new UnsupportedOperationException();
    	
    }

    /**
     * 
     * @param store
     *            The backing store.
     * @param addressBits
     *            The #of bits in the address map for a directory bucket. The
     *            #of children for a directory is <code>2^addressBits</code>.
     *            For example, a value of <code>10</code> means a
     *            <code>10</code> bit address space in the directory. Such a
     *            directory would provide direct addressing for
     *            <code>1024</code> child references. Given an overhead of
     *            <code>8</code> bytes per child address, that would result in
     *            an expected page size of 8k before compression.
     * @throws IllegalArgumentException
     *             if addressBits is LT ONE (1).
     * @throws IllegalArgumentException
     *             if addressBits is GT (16) (fan out of 65536).
     */
    public HTree(final IRawStore store, final int addressBits) {

    	this(store, addressBits, true/* rawRecords */);

    }

	/**
	 * 
	 * 
	 * @param store
	 *            The backing store.
	 * @param addressBits
	 *            The #of bits in the address map for a directory bucket. The
	 *            #of children for a directory is <code>2^addressBits</code>.
	 *            For example, a value of <code>10</code> means a
	 *            <code>10</code> bit address space in the directory. Such a
	 *            directory would provide direct addressing for
	 *            <code>1024</code> child references. Given an overhead of
	 *            <code>8</code> bytes per child address, that would result in
	 *            an expected page size of 8k before compression.
	 * @param rawRecords
	 *            <code>true</code> if raw records are in use.
	 * 
	 * @throws IllegalArgumentException
	 *             if addressBits is LT ONE (1).
	 * @throws IllegalArgumentException
	 *             if addressBits is GT (16) (fan out of 65536).
	 */
    public HTree(final IRawStore store, final int addressBits,
            final boolean rawRecords) {

//    	super(store, nodeFactory, readOnly, addressBits, metadata, recordCompressorFactory);
		super(store, false/*readOnly*/, addressBits);

		this.splitBits = 1;// in [1:addressBits];
		
        // @todo from IndexMetadata
        this.versionTimestamps = false;
        this.deleteMarkers = false;
        this.rawRecords = rawRecords;

        /*
         * The initial setup of the hash tree is a root directory page whose
         * global depth is the #of address bits (which is the maximum value that
         * global depth can take on for a given hash tree). There is a single
         * bucket page and all entries in the root directory page point to that
         * bucket. This means that the local depth of the initial bucket page
         * will be zero (you can prove this for yourself by consulting the
         * tables generated by TestHTree). With a depth of zero, the initial
         * bucket page will have buddy hash buckets which can hold only a single
         * distinct hash key (buckets always have to handle duplicates of a hash
         * key).
         * 
         * From this initial configuration, inserts of 2 distinct keys which
         * fall into the same buddy hash tables will cause that buddy hash
         * buckets in the initial bucket page to split, increasing the depth of
         * the resulting bucket page by one. Additional splits driven by inserts
         * of distinct keys will eventually cause the local depth of some bucket
         * page to exceed the global depth of the root and a new level will be
         * introduced below the root. The hash tree can continue to grow in this
         * manner, gradually adding depth where there are bit prefixes for which
         * there exists a lot of variety in the observed keys.
         */
        
		// Initial root.
		final DirectoryPage r = new DirectoryPage(//
				this,// the owning htree instance
				addressBits // the global depth of the root.
				);
		nnodes++;
        assert r.getSlotsPerBuddy() == (1 << addressBits) : "slotsPerBuddy="
                + r.getSlotsPerBuddy();
        assert r.getNumBuddies() == 1 : "numBuddies=" + r.getNumBuddies();

		// Data for the root.
		final MutableDirectoryPageData rdata = (MutableDirectoryPageData) r.data;

        // Initial bucket.
		final BucketPage b = new BucketPage(this, 0/* globalDepth */);
		nleaves++;

		final int nslots = r.getSlotsPerBuddy() * r.getNumBuddies();

		for (int i = 0; i < nslots; i++) {
		
			b.parent = (Reference) r.self;
			
			r.childRefs[i] = (Reference) b.self;

			rdata.childAddr[i] = 0L;
			
		}
		
		this.root = r;
		
    }

	/*
	 * CRUD API.
	 * 
	 * TODO The hash tree intrinsically supports duplicate keys. This means that
	 * some method semantics must be different. E.g., insert() does not update
	 * an existing tuple for the same key. remove() removes an arbitrary tuple
	 * matching the key, lookup() will return an arbitrary match or null if
	 * there is no match, etc. Also, what corresponds to a lookup scenario in a
	 * BTree will normally be a bag traversal in an HTree requiring an iterator
	 * construct.
	 * 
	 * TODO The hash tree will normally be used with int32 keys, and those keys
	 * will typically be obtained from Object#hashCode(). Special methods should
	 * be provided for this purpose. E.g., ISimpleHTree. Custom serialization
	 * and even a custom mutable leaf data record should be used with this case.
	 * 
	 * TODO insert() return for htree is never old value since always adds() a
	 * new tuple never replaces() an existing tuple.  Further, if the key and
	 * val are equals() then we might even define the semantics as a NOP rather
	 * than appending a duplicate item into the bucket (it's worth thinking on
	 * this further as scanning for equals is more work and sometimes we might
	 * want to allow fully duplicated items into the bucket.).
	 */
    
    /**
     * Convert an int32 hash code key into an <code>unsigned byte[4]</code>.
     */
    private byte[] i2k(final int key) {
		final byte[] buf = new byte[4];
		buf[0] = (byte) (key >>> 24);
		buf[1] = (byte) (key >>> 16);
		buf[2] = (byte) (key >>> 8);
		buf[3] = (byte) (key >>> 0);
		return buf;
    }

	// TODO contains with an Object clear needs to test for the Object, not just
	// the key.  Maybe do this as a wrapper similar to BigdataMap?
    public boolean contains(final Object obj) {
        //contains(obj.hashCode());
    	throw new UnsupportedOperationException();
    }

	public boolean contains(final int key) {
		return contains(i2k(key));
	}

	/**
	 * Return <code>true</code> iff there is at least one tuple in the hash tree
	 * having the specified <i>key</i>.
	 * 
	 * @param key
	 *            The key.
	 * @return <code>true</code> iff the hash tree contains at least one tuple
	 *         with that <i>key</i>.
	 * @throws IllegalArgumentException
	 *             if the <i>key</i> is <code>null</code>.
	 * 
	 *             TODO Parallel to insert(), consider a contains() signature
	 *             which permits testing for a specific value as well. Maybe
	 *             in a wrapper class?
	 */
	public boolean contains(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		DirectoryPage current = getRoot(); // start at the root.
		int prefixLength = 0;// prefix length of the root is always zero.
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		while (true) {
			// skip prefixLength bits and then extract globalDepth bits. 
			final int hashBits = current.getLocalHashCode(key, prefixLength);
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
			if (child.isLeaf()) {
				/*
				 * Found the bucket page, update it.
				 */
				final BucketPage bucketPage = (BucketPage) child;
				if(!bucketPage.contains(key, buddyOffset)) {
					return false;
				}
				return true;
			}
			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			prefixLength = prefixLength + current.globalDepth;
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			current = (DirectoryPage) child;
		}
	}

//    public void lookup(final Object obj) {
//        lookup(obj.hashCode());
//    }

	/**
	 * Return the first value for the key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The first value for the key -or- <code>null</code> if there are
	 *         no tuples in the index having that key. Note that the return
	 *         value is not diagnostic if the application allows
	 *         <code>null</code> values into the index.
	 */
    public byte[] lookupFirst(final int key) {
    	return lookupFirst(i2k(key));
    }

	/**
	 * Return the first value for the key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The first value for the key -or- <code>null</code> if there are
	 *         no tuples in the index having that key. Note that the return
	 *         value is not diagnostic if the application allows
	 *         <code>null</code> values into the index.
	 */
	public byte[] lookupFirst(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		DirectoryPage current = getRoot(); // start at the root.
		int prefixLength = 0;// prefix length of the root is always zero.
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		while (true) {
			// skip prefixLength bits and then extract globalDepth bits. 
			final int hashBits = current.getLocalHashCode(key, prefixLength);
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
			if (child.isLeaf()) {
				/*
				 * Found the bucket page, search it for a match.
				 */
				final BucketPage bucketPage = (BucketPage) child;
				return bucketPage.lookupFirst(key, buddyOffset);
			}
			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			prefixLength = prefixLength + current.globalDepth;
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			current = (DirectoryPage) child;
		}
	}

	/**
	 * Return an iterator which will visit each tuple in the index having the
	 * specified key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The iterator and never <code>null</code>.
	 */
	public ITupleIterator lookupAll(final int key) {
		return lookupAll(i2k(key));
	}

	/**
	 * Return an iterator which will visit each tuple in the index having the
	 * specified key.
	 * 
	 * @param key
	 *            The key.
	 * 
	 * @return The iterator and never <code>null</code>.
	 */
	public ITupleIterator lookupAll(final byte[] key) {
		if (key == null)
			throw new IllegalArgumentException();
		DirectoryPage current = getRoot(); // start at the root.
		int prefixLength = 0;// prefix length of the root is always zero.
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		while (true) {
			// skip prefixLength bits and then extract globalDepth bits. 
			final int hashBits = current.getLocalHashCode(key, prefixLength);
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
			if (child.isLeaf()) {
				/*
				 * Found the bucket page, search it for a match.
				 */
				final BucketPage bucketPage = (BucketPage) child;
				return bucketPage.lookupAll(key, buddyOffset);
			}
			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			prefixLength = prefixLength + current.globalDepth;
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			current = (DirectoryPage) child;
		}
	}

    public void insert(final Object obj) {
        insert(obj.hashCode(), SerializerUtil.serialize(obj));
    }

	public void insert(final int key, final byte[] val) {
		insert(i2k(key), val);
	}

	/**
	 * Insert a tuple into the hash tree. Tuples with duplicate keys and even
	 * tuples with duplicate keys and values are allowed and will result in
	 * multiple tuples.
	 * 
	 * @param key
	 *            The key.
	 * @param value
	 *            The value.
	 * @return <code>null</code> (always).
	 * 
	 *         TODO If the application wants to restrict the hash tree to such
	 *         that it does not contain duplicate tuples then it must first
	 *         search in the tree for an exact match (key and value). It is
	 *         easier to do that from within the insert logic so expand the
	 *         method signature to pass an insert enum {ALLDUPS,DUPKEYS,NODUPS}.
	 */
	public byte[] insert(final byte[] key, final byte[] value) {

        if (log.isInfoEnabled())
            log.info("key=" + BytesUtil.toString(key) + ", value="
                    + Arrays.toString(value));

		if (key == null)
			throw new IllegalArgumentException();
		
		// the current directory page.
		DirectoryPage current = getRoot(); // start at the root.
		
		// #of prefix bits already consumed.
		int prefixLength = 0;// prefix length of the root is always zero.
		
		// buddyOffset into [current].
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		
		while (true) {

			// skip prefixLength bits and then extract globalDepth bits. 
		    final int hashBits = current.getLocalHashCode(key, prefixLength);
			
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
			
			if (child.isLeaf()) {

				/*
				 * Found the bucket page, update it.
				 */

			    final BucketPage bucketPage = (BucketPage) child;
				
				// Attempt to insert the tuple into the bucket.
                if (!bucketPage.insert(key, value, current/* parent */,
                        buddyOffset)) {

					// TODO if(parent.isReadOnly()) parent = copyOnWrite();
					
					if (current.globalDepth == child.globalDepth) {

                        /*
                         * There is only one buddy hash bucket on the page.
                         * Either we can split a directory page which is a
                         * parent of that bucket or we have to add a new level
                         * below the root on the path to that bucket.
                         */

                        if (child.globalDepth == addressBits) {

                        	addLevel2(bucketPage);
                        	
                        	return insert(key,value);
//							return innerInsertFullBucket(key, value, bucketPage);

                        } else {

                            splitDirectoryPage(
                                    current.getParentDirectory()/* parent */,
                                    buddyOffset, current/* oldChild */);
                            throw new UnsupportedOperationException();
                        }

					}

					// globalDepth >= localDepth
					// splitBucketsOnPage(current, buddyOffset, bucketPage);
					current.split(bucketPage);
					
					// Try again. The children have changed.
					continue;
		
				}
				
				return null; // TODO should be Void return? or depends on enum controlling dups behavior?
				
			}

			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			
			// increase prefix length by the #of address bits consumed by the
			// buddy hash table in the current directory page as we descend.
			prefixLength = prefixLength + current.globalDepth;
			
			// find the offset of the buddy hash table in the child.
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			
			// update current so we can search in the child.
			current = (DirectoryPage) child;
			
		}

	} // insert()

	/**
	 * Insert a tuple into the hash tree. Tuples with duplicate keys and even
	 * tuples with duplicate keys and values are allowed and will result in
	 * multiple tuples.
	 * 
	 * @param src
	 *            The source {@link BucketPage} src
	 * @param slot
	 *            The slot in the source {@link BucketPage} whose tuple will be
	 *            inserted (really copied).
	 */
	private void insertRawTuple(final BucketPage src, final int slot) {

		if (src == null)
			throw new IllegalArgumentException();

		if (slot < 0 || slot >= (1 << addressBits)) // [0:slotsPerPage].
			throw new IllegalArgumentException();

		// the key to insert
		final byte[] key = src.getKeys().get(slot);
		
		if (key == null)
			throw new IllegalArgumentException();
		
		if (log.isInfoEnabled())
			log.info("key=" + BytesUtil.toString(key));

		// the current directory page.
		DirectoryPage current = getRoot(); // start at the root.
		
		// #of prefix bits already consumed.
		int prefixLength = 0;// prefix length of the root is always zero.
		
		// buddyOffset into [current].
		int buddyOffset = 0; // buddyOffset of the root is always zero.
		
		while (true) {

			// skip prefixLength bits and then extract globalDepth bits. 
		    final int hashBits = current.getLocalHashCode(key, prefixLength);
			
			// find the child directory page or bucket page.
			final AbstractPage child = current.getChild(hashBits, buddyOffset);
			
			if (child.isLeaf()) {

				/*
				 * Found the bucket page, update it.
				 */

			    final BucketPage bucketPage = (BucketPage) child;
				
				// Attempt to insert the tuple into the bucket.
				if (!bucketPage.insertRawTuple(src, slot, key,
						current/* parent */, buddyOffset)) {

					// TODO if(parent.isReadOnly()) parent = copyOnWrite();
					
					if (current.globalDepth == child.globalDepth) {

                        /*
                         * There is only one buddy hash bucket on the page.
                         * Either we can split a directory page which is a
                         * parent of that bucket or we have to add a new level
                         * below the root on the path to that bucket.
                         */

						if (child.globalDepth == addressBits) {

							addLevel2(bucketPage); // add a level.

							insertRawTuple(src, slot); // try again
							return;

                        } else {

                            splitDirectoryPage(
                                    current.getParentDirectory()/* parent */,
                                    buddyOffset, current/* oldChild */);
                            throw new UnsupportedOperationException();

					    }

					}

					// globalDepth >= localDepth
					splitBucketsOnPage(current, buddyOffset, bucketPage);
					
					// Try again. The children have changed.
					continue;
		
				}
				
				return;
				
			}

			/*
			 * Recursive descent into a child directory page. We have to update
			 * the prefixLength and compute the offset of the buddy hash table
			 * within the child before descending into the child.
			 */
			
			// increase prefix length by the #of address bits consumed by the
			// buddy hash table in the current directory page as we descend.
			prefixLength = prefixLength + current.globalDepth;
			
			// find the offset of the buddy hash table in the child.
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			
			// update current so we can search in the child.
			current = (DirectoryPage) child;
			
		}

	} // insertRawTuple()

//    /**
//     * Handle an insert when the target {@link BucketPage} is full (a single
//     * buddy hash bucket) but can be split (not all keys in the buckets are
//     * duplicates).
//     * 
//     * @param key
//     * @param value
//     * @param prefixLength
//     *            The prefix length to <i>current</i>.
//     * @param current
//     *            The parent of <i>bucketPage</i>. The depth is
//     *            <i>addressBits</i>.
//     * @param bucketPage
//     *            A full {@link BucketPage}. The depth is <i>addressBits</i>.
//     *            All slots a full.
//     * @return
//     */
//	private byte[] innerInsertFullBucket(final byte[] key, final byte[] value,
//			final BucketPage bucketPage) {
//
//    	if (bucketPage == null)
//            throw new IllegalArgumentException();
//
//    	if (bucketPage.globalDepth != addressBits)
//            throw new IllegalStateException();
//
//		final DirectoryPage current = bucketPage.getParentDirectory();
//
//		if (current.globalDepth != addressBits)
//			throw new IllegalStateException();
//
//		/*
//		 * Figure out how much we need to extend the prefix length before we can
//		 * split-and-reindex this bucket page.
//		 * 
//		 * FIXME Probably must recompute each time in the loop since addLevel()
//		 * can recurse through insert() and back into this method.
//		 */
//        final int distinctBitsRequired = bucketPage.distinctBitsRequired();
//
//        if (distinctBitsRequired == -1)
//            throw new AssertionError();
//
//        int n = current.getPrefixLength();
//        
//        while (n < distinctBitsRequired) {
//
//            /*
//             * Find the first directory above the bucket which can be split. If
//             * null, then we will have to add a level instead.
//             */
//            DirectoryPage p = bucketPage.parent.get();
//            DirectoryPage pp;
//            while ((pp = p.getParentDirectory()) != null) {
//
//                if (p.globalDepth < pp.globalDepth) {
//
//                    break;
//
//                }
//
//                p = pp;
//                
//            }
//            
//            if (pp == null) {
//
////                /*
////                 * Add a level; increases prefixLength to bucketPage by one.
////                 */
////
//////                final int buddyOffset = getBuddyOffsetOfDirectoryPageOnPathToFullBucket(
//////                        key, bucketPage.parent.get());
//////
//////                addLevel(bucketPage.parent.get()/* oldParentIsAlwaysRoot */,
//////                        buddyOffset, splitBits);
////
////				/*
////				 * To split the page, we have to introduce a new directory page
////				 * above it and then introduce a sibling bucket page and finally
////				 * re-index the tuples in the bucket page such that they are
////				 * either the original bucket page or its new sibling bucket
////				 * page.
////				 * 
////				 * Note: If all keys on the page are duplicates then we can not
////				 * split the page. Instead we must let the page "overflow."
////				 * BucketPage.insert() handles this condition for us by allowing
////				 * the page to overflow rather than telling us to split the
////				 * page.
////				 * 
////				 * TODO Unit test this to make sure that it is introducing the
////				 * new level along the path to the full bucket page (rather than
////				 * some other path).
////				 */
////
////				// hash bits for the root directory.
////				final int hashBitsForRoot = root
////						.getLocalHashCode(key, 0/* prefixLengthForRootIsZero */);
////
////				// find the child of the root directory page.
////				final AbstractPage childOfRoot = root.getChild(
////						hashBitsForRoot, 0/* buddyOffsetOfRootIsZero */);
////
////				final int buddyOffsetInChild = HTreeUtil.getBuddyOffset(
////						hashBitsForRoot, root.globalDepth,
////						childOfRoot.globalDepth/* localDepthOfChild */);
////
////				// Add a level.
////				addLevel(root/* parent */, buddyOffsetInChild, splitBits);
//            	
//				System.err.println("before addLevel(): distinctBitsRequired="
//						+ distinctBitsRequired + ", n=" + n + "\n" + PP());
//
//				addLevel2(bucketPage);
//				
//				System.err.println("after addLevel(): distinctBitsRequired="
//						+ distinctBitsRequired + ", n=" + n + "\n" + PP());
//
//            } else {
//
//                /*
//                 * Split the directory; increases prefixLength to bucketPage
//                 * one.
//                 */
//
////                final int buddyOffset = getBuddyOffsetOfDirectoryPageOnPathToFullBucket(
////                        key, p);
////
////                splitDirectoryPage(p.parent.get(), buddyOffset, p/*oldChild*/);
//
//            	/*
//            	 * Expectation is that we will always have directory pages at
//            	 * depth := addressBits.
//            	 */
//            	throw new UnsupportedOperationException();
//            	
//            }    
//
//            // adding a level increases the bit depth by addressBits each time.
//            n += addressBits;
//            
//        }
//        
//        return insert(key, value); // TODO Move into outer insert().
//
////        /*
////         * Run up the parent references back to the root. If we find a directory
////         * page whose local depth is LT its parent's global depth then we will
////         * split that directory. Otherwise, we will have to add a new level.
////         */
////        if(splitDirectoryOnPathToFullBucket(key)) {
////            
////            /*
////             * Recursion through the top-level insert().
////             */
////
////            return insert(key, value);
////
////        }
////
////        /*
////         * To split the page, we have to introduce a new directory page above it
////         * and then introduce a sibling bucket page and finally re-index the
////         * tuples in the bucket page such that they are either the original
////         * bucket page or its new sibling bucket page.
////         * 
////         * Note: If all keys on the page are duplicates then we can not split
////         * the page. Instead we must let the page "overflow."
////         * BucketPage.insert() handles this condition for us by allowing the
////         * page to overflow rather than telling us to split the page.
////         * 
////         * TODO Unit test this to make sure that it is introducing the new level
////         * along the path to the full bucket page (rather than some other path).
////         */
////
////        // hash bits for the root directory.
////        final int hashBitsForRoot = current
////                .getLocalHashCode(key, 0/* prefixLengthForRootIsZero */);
////
////        // find the child of the root directory page.
////        final AbstractPage childOfRoot = current
////                .getChild(hashBitsForRoot, 0/* buddyOffsetOfRootIsZero */);
////
////        final int buddyOffsetInChild = HTreeUtil
////                .getBuddyOffset(hashBitsForRoot, root.globalDepth,
////                        childOfRoot.globalDepth/* localDepthOfChild */);
////
////        // Add a level.
////        addLevel(root/* parent */, buddyOffsetInChild, splitBits);
////
////        return insertAfterAddLevel(key, value);
//
//	}
//
//    private int getBuddyOffsetOfDirectoryPageOnPathToFullBucket(final byte[] key,
//            final DirectoryPage sought) {
//
//        if (log.isInfoEnabled())
//            log.info("key=" + BytesUtil.toString(key));
//
//        if (key == null)
//            throw new IllegalArgumentException();
//
//        // the current directory page.
//        DirectoryPage current = getRoot(); // start at the root.
//
//        // #of prefix bits already consumed.
//        int prefixLength = 0;// prefix length of the root is always zero.
//
//        // buddyOffset into [current].
//        int buddyOffset = 0; // buddyOffset of the root is always zero.
//
//        while (true) {
//
//            if (current == sought) {
//
//                return buddyOffset;
//                
//            }
//
//            // skip prefixLength bits and then extract globalDepth bits.
//            final int hashBits = current.getLocalHashCode(key, prefixLength);
//
//            // find the child directory page or bucket page.
//            final AbstractPage child = current.getChild(hashBits, buddyOffset);
//
//            if (child.isLeaf()) {
//
//                // No directory page can be split.
//                throw new RuntimeException();
//
//            }
//
//            /*
//             * Recursive descent into a child directory page. We have to update
//             * the prefixLength and compute the offset of the buddy hash table
//             * within the child before descending into the child.
//             */
//
//            // increase prefix length by the #of address bits consumed by the
//            // buddy hash table in the current directory page as we descend.
//            prefixLength = prefixLength + current.globalDepth;
//
//            // find the offset of the buddy hash table in the child.
//            buddyOffset = HTreeUtil
//                    .getBuddyOffset(hashBits, current.globalDepth,
//                            child.globalDepth/* localDepthOfChild */);
//
//            // update current so we can search in the child.
//            current = (DirectoryPage) child;
//
//        }
//
//    }

//    /**
//     * Search the tree along the path to a full {@link BucketPage} (a single
//     * buddy bucket in which at least one key is not a duplicate of the rest).
//     * If we find a {@link DirectoryPage} which can be split (its depth is LT
//     * its parent's depth), then split that {@link DirectoryPage} and return
//     * <code>true</code>. Otherwise return <code>false</code>.
//     * 
//     * @param key
//     *            The original key.
//     * 
//     * @return <code>true</code> if a {@link DirectoryPage} along the path to
//     *         the full {@link BucketPage} was split such that the insert()
//     *         operation can be retried. <code>false</code> if a new level must
//     *         be added instead.
//     */
//    private boolean splitDirectoryOnPathToFullBucket(final byte[] key) {
//
//        if (log.isInfoEnabled())
//            log.info("key=" + BytesUtil.toString(key));
//
//        if (key == null)
//            throw new IllegalArgumentException();
//
//        // the current directory page.
//        DirectoryPage current = getRoot(); // start at the root.
//
//        // #of prefix bits already consumed.
//        int prefixLength = 0;// prefix length of the root is always zero.
//
//        // buddyOffset into [current].
//        int buddyOffset = 0; // buddyOffset of the root is always zero.
//
//        while (true) {
//
//            // skip prefixLength bits and then extract globalDepth bits.
//            final int hashBits = current.getLocalHashCode(key, prefixLength);
//
//            // find the child directory page or bucket page.
//            final AbstractPage child = current.getChild(hashBits, buddyOffset);
//
//            if (child.isLeaf()) {
//
//                // No directory page can be split.
//                return false;
//
//            }
//
//            if (child.globalDepth < current.globalDepth) {
//
//                splitDirectoryPage(current/* parent */, buddyOffset,
//                        (DirectoryPage) child);
//
//                return true;
//
//            }
//
//            /*
//             * Recursive descent into a child directory page. We have to update
//             * the prefixLength and compute the offset of the buddy hash table
//             * within the child before descending into the child.
//             */
//
//            // increase prefix length by the #of address bits consumed by the
//            // buddy hash table in the current directory page as we descend.
//            prefixLength = prefixLength + current.globalDepth;
//
//            // find the offset of the buddy hash table in the child.
//            buddyOffset = HTreeUtil
//                    .getBuddyOffset(hashBits, current.globalDepth,
//                            child.globalDepth/* localDepthOfChild */);
//
//            // update current so we can search in the child.
//            current = (DirectoryPage) child;
//
//        }
//
//    }
//
//    /**
//     * Handle an insert after introducing a new level in the tree because a full
//     * {@link BucketPage} could not otherwise be split. This method is
//     * responsible for re-indexing the tuples in that full {@link BucketPage}.
//     * It will then retry the insert (recursively entering the top level insert
//     * method).
//     * 
//     * @param key
//     * @param value
//     * @return
//     */
//	private byte[] insertAfterAddLevel(final byte[] key, final byte[] value) {
//
//        if (log.isInfoEnabled())
//            log.info("key=" + BytesUtil.toString(key) + ", value="
//                    + Arrays.toString(value));
//
//        if (key == null)
//            throw new IllegalArgumentException();
//        
//        // the current directory page.
//        DirectoryPage current = getRoot(); // start at the root.
//        
//        // #of prefix bits already consumed.
//        int prefixLength = 0;// prefix length of the root is always zero.
//        
//        // buddyOffset into [current].
//        int buddyOffset = 0; // buddyOffset of the root is always zero.
//        
//        while (true) {
//
//            // skip prefixLength bits and then extract globalDepth bits. 
//            final int hashBits = current.getLocalHashCode(key, prefixLength);
//            
//            // find the child directory page or bucket page.
//            final AbstractPage child = current.getChild(hashBits, buddyOffset);
//            
//            if (child.isLeaf()) {
//
//                /*
//                 * Found the bucket page, update it.
//                 */
//
//                final BucketPage bucketPage = (BucketPage) child;
//
//                /*
//                 * FIXME This is adding [addressBits] to the [prefixLength] for
//                 * this call. Verify that this is always the right thing to do.
//                 * This is based on some experience with a unit test working
//                 * through a detailed example (insert 1,2,3,4,5) but I have not
//                 * generalized this to a clear rule yet.
//                 */
//                if(!splitAndReindexFullBucketPage(current/* parent */, buddyOffset,
//                        prefixLength + addressBits, bucketPage/* oldBucket */)) {
//                    
//                    throw new AssertionError("Reindex of full bucket fails");
//                    
//                }
//
//                /*
//                 * Outer insert.
//                 * 
//                 * Note: We MUST NOT recursively enter insert() if the sole
//                 * buddy bucket on the page consists entirely of tuples having
//                 * the same key (all duplicate keys). BucketPage.insert()
//                 * handles this condition for us by allowing the page to
//                 * overflow rather than telling us to split the page.
//                 */
//                return insert(key, value);
//                
//            }
//
//            /*
//             * Recursive descent into a child directory page. We have to update
//             * the prefixLength and compute the offset of the buddy hash table
//             * within the child before descending into the child.
//             */
//            
//            // increase prefix length by the #of address bits consumed by the
//            // buddy hash table in the current directory page as we descend.
//            prefixLength = prefixLength + current.globalDepth;
//            
//            // find the offset of the buddy hash table in the child.
//            buddyOffset = HTreeUtil
//                    .getBuddyOffset(hashBits, current.globalDepth,
//                            child.globalDepth/* localDepthOfChild */);
//            
//            // update current so we can search in the child.
//            current = (DirectoryPage) child;
//            
//        }
//
//    } // insertAfterAddLevel()
	
	public byte[] remove(final byte[] key) {
		// TODO Remove 1st match, returning value.
		throw new UnsupportedOperationException();
	}

    /**
     * Handle split if buddy bucket is full but localDepth LT globalDepth (so
     * there is more than one buddy bucket on the page). This will allocate a
     * new bucket page; update the references in the parent, and then
     * redistribute buddy buckets among the old and new bucket page. The depth
     * of the child will be increased by one. As a post-condition, the depth of
     * the new child will be the same as the then current depth of the original
     * child. Note that this doubles the size of each buddy bucket, thus always
     * creating room for additional tuples.
     * <p>
     * Note: This is really just doubling the address space for the buddy buckets
     * on the page.  Since each buddy bucket now has twice as many slots, we have
     * to move 1/2 of the buddy buckets to a new bucket page.
     * 
     * @param parent
     *            The parent {@link DirectoryPage}.
     * @param buddyOffset
     *            The buddyOffset within the <i>parent</i>. This identifies
     *            which buddy hash table in the parent must be its pointers
     *            updated such that it points to both the original child and new
     *            child.
     * @param oldChild
     *            The child {@link BucketPage}.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the depth of the child is GTE the depth of the parent.
     * @throws IllegalStateException
     *             if the <i>parent<i/> is read-only.
     * @throws IllegalStateException
     *             if the <i>oldChild</i> is read-only.
     * @throws IllegalStateException
     *             if the parent of the <i>oldChild</i> is not the given
     *             <i>parent</i>.
     */
    // Note: package private for unit tests.
    void splitBucketsOnPage(final DirectoryPage parent, final int buddyOffset,
            final BucketPage oldChild) {
    	
		if (parent == null)
			throw new IllegalArgumentException();
		if (oldChild == null)
			throw new IllegalArgumentException();
		if (oldChild.globalDepth >= parent.globalDepth) {
			// In this case we have to introduce a new directory page instead.
			throw new IllegalStateException();
		}
		if (buddyOffset < 0)
			throw new IllegalArgumentException();
		if (buddyOffset >= (1 << addressBits)) {
			/*
			 * Note: This check is against the maximum possible slot index. The
			 * actual max buddyOffset depends on parent.globalBits also since
			 * (1<<parent.globalBits) gives the #of slots per buddy and the
			 * allowable buddyOffset values must fall on an buddy hash table
			 * boundary.
			 */
			throw new IllegalArgumentException();
		}
		if(parent.isReadOnly()) // must be mutable.
			throw new IllegalStateException();
		if(oldChild.isReadOnly()) // must be mutable.
			throw new IllegalStateException();
		if (oldChild.parent != parent.self) // must be same Reference.
			throw new IllegalStateException();
		
		if (log.isInfoEnabled())
			log.info("parent=" + parent.toShortString() + ", buddyOffset="
					+ buddyOffset + ", child=" + oldChild.toShortString());

		final int oldDepth = oldChild.globalDepth;
		final int newDepth = oldDepth + 1;

		// Allocate a new bucket page (globalDepth is increased by one).
		final BucketPage newBucket = new BucketPage(this, newDepth);

		assert newBucket.isDirty();
		
		// Set the parent reference on the new bucket.
		newBucket.parent = (Reference) parent.self;
		
		// Increase global depth on the old page also.
		oldChild.globalDepth = newDepth;

		nleaves++; // One more bucket page in the hash tree. 

		// update the pointers in the parent.
		updatePointersInParent(parent, buddyOffset, oldDepth, oldChild,
				newBucket);
		
		// redistribute buddy buckets between old and new pages.
		redistributeBuddyBuckets(oldDepth, newDepth, oldChild, newBucket);

		// TODO assert invariants?
		
	}

	/**
	 * Update pointers in buddy hash table in the parent in order to link the
	 * new {@link AbstractPage} into the parent {@link DirectoryPage}.
	 * <p>
	 * There will be [npointers] slots in the appropriate buddy hash table in
	 * the parent {@link DirectoryPage} which point to the old
	 * {@link AbstactPage}. The upper 1/2 of those pointers will be modified to
	 * point to the new {@link AbstractPage}. The lower 1/2 of the pointers will
	 * be unchanged.
	 * 
	 * @param parent
	 *            The parent {@link DirectoryPage}.
	 * @param buddyOffset
	 *            The buddyOffset within the <i>parent</i>. This identifies
	 *            which buddy hash table in the parent must have its pointers
	 *            updated such that it points to both the original child and new
	 *            child.
	 * @param oldDepth
	 *            The depth of the oldBucket before the split.
	 * @param oldChild
	 *            The old {@link AbstractPage}.
	 * @param newChild
	 *            The new {@link AbstractPage}.
	 */
	private void updatePointersInParent(final DirectoryPage parent,
			final int buddyOffset, final int oldDepth,
			final AbstractPage oldChild, final AbstractPage newChild) {

		// #of address slots in the parent buddy hash table.
		final int slotsPerBuddy = (1 << parent.globalDepth);

		// #of pointers in the parent buddy hash table to the old child.
		final int npointers = 1 << (parent.globalDepth - oldDepth);
		
		// Must be at least two slots since we will change at least one.
        if (slotsPerBuddy <= 1)
            throw new AssertionError("slotsPerBuddy=" + slotsPerBuddy);

		// Must be at least two pointers since we will change at least one.
		assert npointers > 1 : "npointers=" + npointers;
		
		// The first slot in the buddy hash table in the parent.
		final int firstSlot = buddyOffset;
		
		// The last slot in the buddy hash table in the parent.
		final int lastSlot = buddyOffset + slotsPerBuddy;

        /*
         * Count pointers to the old child page. There should be [npointers] of
         * them and they should be contiguous.
         * 
         * Note: We can test References here rather than comparing addresses
         * because we know that the parent and the old child are both mutable.
         * This means that their childRef is defined and their storage address
         * is NULL.
         */
		int firstPointer = -1;
		int nfound = 0;
		boolean discontiguous = false;
		for (int i = firstSlot; i < lastSlot; i++) {
			if (parent.childRefs[i] == oldChild.self) {
				if (firstPointer == -1)
					firstPointer = i;
				nfound++;
				if (((MutableDirectoryPageData) parent.data).childAddr[i] != IRawStore.NULL) {
					throw new RuntimeException(
							"Child address should be NULL since child is dirty");
				}
			} else {
				if (firstPointer != -1 && nfound != npointers) {
					discontiguous = true;
				}
			}
		}
		if (firstPointer == -1)
			throw new RuntimeException("No pointers to child");
		if (nfound != npointers)
			throw new RuntimeException("Expected " + npointers
					+ " pointers to child, but found=" + nfound);
		if (discontiguous)
			throw new RuntimeException(
					"Pointers to child are discontiguous in parent's buddy hash table.");

		// Update the upper 1/2 of the pointers to the new bucket.
		for (int i = firstPointer + (npointers >> 1); i < npointers; i++) {

			if (parent.childRefs[i] != oldChild.self)
				throw new RuntimeException("Does not point to old child.");
			
			// update the references to the new bucket.
			parent.childRefs[i] = (Reference) newChild.self;
			
		}
			
	} // updatePointersInParent

	/**
	 * Redistribute the buddy buckets.
	 * <p>
	 * Note: We are not changing the #of buckets, just their size and the
	 * page on which they are found. Any tuples in a source bucket will wind up
	 * in the "same" bucket afterwards, but the page and offset on the page of the
	 * bucket may have been changed and the size of the bucket will have doubled.
	 * <p>
	 * We proceed backwards, moving the upper half of the buddy buckets to the
	 * new bucket page first and then spreading out the lower half of the source
	 * page among the new bucket boundaries on the page.
	 * 
	 * @param oldDepth
	 *            The depth of the old {@link BucketPage} before the split.
	 * @param newDepth
	 *            The depth of the old and new {@link BucketPage} after the
	 *            split (this is just oldDepth+1).
	 * @param oldBucket
	 *            The old {@link BucketPage}.
	 * @param newBucket
	 *            The new {@link BucketPage}.
	 */
	private void redistributeBuddyBuckets(final int oldDepth,
			final int newDepth, final BucketPage oldBucket,
			final BucketPage newBucket) {

	    assert oldDepth + 1 == newDepth;
	    
		// #of slots on the bucket page (invariant given addressBits).
		final int slotsOnPage = (1 << addressBits);

		// #of address slots in each old buddy hash bucket.
		final int slotsPerOldBuddy = (1 << oldDepth);

		// #of address slots in each new buddy hash bucket.
		final int slotsPerNewBuddy = (1 << newDepth);

		// #of buddy tables on the old bucket page.
		final int oldBuddyCount = slotsOnPage / slotsPerOldBuddy;

		// #of buddy tables on the bucket pages after the split.
		final int newBuddyCount = slotsOnPage / slotsPerNewBuddy;

		final BucketPage srcPage = oldBucket;
		final MutableKeyBuffer srcKeys = (MutableKeyBuffer) oldBucket.getKeys();
		final MutableValueBuffer srcVals = (MutableValueBuffer) oldBucket
				.getValues();

		/*
		 * Move top 1/2 of the buddy buckets from the child to the new page.
		 */
		{

			// target is the new page.
			final BucketPage dstPage = newBucket;
			final MutableKeyBuffer dstKeys = (MutableKeyBuffer) dstPage
					.getKeys();
			final MutableValueBuffer dstVals = (MutableValueBuffer) dstPage
					.getValues();

			// index (vs offset) of first buddy in upper half of src page.
			final int firstSrcBuddyIndex = (oldBuddyCount >> 1);

			// exclusive upper bound for index (vs offset) of last buddy in
			// upper half of src page.
			final int lastSrcBuddyIndex = oldBuddyCount;

			// exclusive upper bound for index (vs offset) of last buddy in
			// upper half of target page.
			final int lastDstBuddyIndex = newBuddyCount;

			// work backwards over buddy buckets to avoid stomping data!
			for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
			srcBuddyIndex >= firstSrcBuddyIndex; //
			srcBuddyIndex--, dstBuddyIndex--//
			) {

				final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

				final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

				final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

				for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot++) {

					if (log.isTraceEnabled())
						log.trace("moving: page(" + srcPage.toShortString()
								+ "=>" + dstPage.toShortString() + ")"
								+ ", buddyIndex(" + srcBuddyIndex + "=>"
								+ dstBuddyIndex + ")" + ", slot(" + srcSlot
								+ "=>" + dstSlot + ")");

                    // Move the tuple at that slot TODO move metadata also.
					if(srcKeys.keys[srcSlot] == null)
					    continue;
					dstKeys.keys[dstSlot] = srcKeys.keys[srcSlot];
					dstVals.values[dstSlot] = srcVals.values[srcSlot];
					srcKeys.keys[srcSlot] = null;
					srcVals.values[srcSlot] = null;
                    dstKeys.nkeys++; // one more in that page.
                    srcKeys.nkeys--; // one less in this page.
                    dstVals.nvalues++; // one more in that page.
                    srcVals.nvalues--; // one less in this page.

				}

			}

		}

		/*
		 * Reposition the bottom 1/2 of the buddy buckets on the old page.
		 * 
		 * Again, we have to move backwards through the buddy buckets on the
		 * source page to avoid overwrites of data which has not yet been
		 * copied. Also, notice that the buddy bucket at index ZERO does not
		 * move - it is already in place even though it's size has doubled.
		 */
		{

			// target is the old page.
			final BucketPage dstPage = oldBucket;
			final MutableKeyBuffer dstKeys = (MutableKeyBuffer) dstPage
					.getKeys();
			final MutableValueBuffer dstVals = (MutableValueBuffer) dstPage
					.getValues();

			// index (vs offset) of first buddy in lower half of src page.
			final int firstSrcBuddyIndex = 0;

			// exclusive upper bound for index (vs offset) of last buddy in
			// lower half of src page.
			final int lastSrcBuddyIndex = (oldBuddyCount >> 1);

			// exclusive upper bound for index (vs offset) of last buddy in
			// upper half of target page (which is also the source page).
			final int lastDstBuddyIndex = newBuddyCount;

			/*
			 * Work backwards over buddy buckets to avoid stomping data!
			 * 
			 * Note: The slots for first buddy in the lower half of the source
			 * page DO NOT MOVE. The offset of that buddy in the first page
			 * remains unchanged. Only the size of the buddy is changed (it is
			 * doubled, just like all the other buddies on the page).
			 */
			for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
			srcBuddyIndex > firstSrcBuddyIndex; // DO NOT move 1st buddy bucket!
			srcBuddyIndex--, dstBuddyIndex--//
			) {

				final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

				final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

				final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

				for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot++) {

					if (log.isTraceEnabled())
						log.trace("moving: page(" + srcPage.toShortString()
								+ "=>" + dstPage.toShortString() + ")"
								+ ", buddyIndex(" + srcBuddyIndex + "=>"
								+ dstBuddyIndex + ")" + ", slot(" + srcSlot
								+ "=>" + dstSlot + ")");

					// Move the tuple at that slot TODO move metadata also.
                    if(srcKeys.keys[srcSlot] == null)
                        continue;
					dstKeys.keys[dstSlot] = srcKeys.keys[srcSlot];
					dstVals.values[dstSlot] = srcVals.values[srcSlot];
					srcKeys.keys[srcSlot] = null;
					srcVals.values[srcSlot] = null;
					// Note: movement within same page: nkeys/nvals don't change

				}

			}

		}

	}

	/**
	 * Adds a new level above a full {@link BucketPage}. The {@link BucketPage}
	 * must be at depth==addressBits (i.e., one buddy bucket on the page and one
	 * pointer from the parent into the {@link BucketPage}). Two new
	 * {@link BucketPage}s and a new {@link DirectoryPage} are recruited. The
	 * new {@link DirectoryPage} is initialized with pointers to the two
	 * {@link BucketPage}s and linked into the parent of the original
	 * {@link BucketPage}. The new {@link DirectoryPage} will be at maximum
	 * depth since it takes the place of the old {@link BucketPage} which was
	 * already at maximum depth. The tuples from the old {@link BucketPage} are
	 * reindexed, which distributes them between the two new {@link BucketPage}
	 * s.
	 * 
	 * @param oldPage
	 *            The full {@link BucketPage}.
	 * 
	 * @throws IllegalArgumentException
	 *             if any argument is <code>null</code>.
	 * @throws IllegalStateException
	 *             if the <i>oldPage</i> is not at maximum depth.
	 * @throws IllegalStateException
	 *             if the parent of the <i>oldPage</i> is not mutable.
	 */
	DirectoryPage addLevel2(final BucketPage oldPage) {
		final DirectoryPage newParent;
		
		if (oldPage == null)
			throw new IllegalArgumentException();

		if (oldPage.globalDepth != addressBits)
			throw new IllegalStateException();

		// The parent directory page above the old bucket page.
		final DirectoryPage pp = oldPage.getParentDirectory();

		if (!pp.isDirty())
			throw new IllegalStateException(); // parent must be mutable.

		if (log.isInfoEnabled())
			log.info("bucketPage=" + oldPage.toShortString());
		
		// #of slots on a page.
		final int slotsPerPage = 1 << addressBits;

		// allocate new nodes and relink the tree.
		{

			// 1/2 of the slots will point to each of the new bucket pages.
			final int npointers = slotsPerPage >> 1;

			// The local depth of the new bucket pages.
			final int localDepth = HTreeUtil.getLocalDepth(addressBits,
					addressBits/* globalDepth(newParent) */, npointers);

			newParent = new DirectoryPage(this, addressBits/* globalDepth */);
			nnodes++;

			final BucketPage a = new BucketPage(this, localDepth);
			final BucketPage b = new BucketPage(this, localDepth);
			nleaves++; // Note: only +1 since we will delete the oldPage.

			/*
			 * Replace the pointer to the oldPage in the parent with the new
			 * directory page.
			 */
			{
				boolean found = false;
				final long oldAddr = oldPage.isPersistent() ? oldPage
						.getIdentity() : 0L;
				final MutableDirectoryPageData data = (MutableDirectoryPageData) pp.data;
				for (int i = 0; i < slotsPerPage && !found; i++) {
					if (oldPage.isPersistent()) {
						if (data.childAddr[i] == oldAddr)
							found = true; // same address
					} else {
						if (pp.childRefs[i] == oldPage.self)
							found = true; // same reference
					}
					if (found) {
						pp.childRefs[i] = (Reference) newParent.self; // set ref
						data.childAddr[i] = 0L; // clear addr.
					}
				}
				if (!found)
					throw new AssertionError();
			}

			// Set the parent references on the new pages.
			newParent.parent = (Reference) pp.self;
			a.parent = (Reference) newParent.self;
			b.parent = (Reference) newParent.self;

			// Link the new bucket pages into the new parent directory page.
			for (int i = 0; i < slotsPerPage; i++) {
				newParent.childRefs[i] = (Reference) (i < npointers ? a.self
						: b.self);
			}

			if (oldPage.isPersistent()) {
				// delete oldPage.
				store.delete(oldPage.getIdentity());
			}

		}

		/*
		 * Reindex the tuples in the old bucket page
		 * 
		 * Note: This MUST be done as a low level operation in order to avoid
		 * duplicating raw records and in order to preserve per-tuple metadata,
		 * including version timestamps and deleted tuple markers.
		 * 
		 * Note: the page is full. there should be no empty slots (no null
		 * keys).
		 */
		// reindexTuples(oldPage, a, b);
		IRaba okeys = oldPage.getKeys();
		IRaba ovals = oldPage.getValues();
		for (int i = 0; i < slotsPerPage; i++) {

			// insertRawTuple(oldPage, i);
			if (okeys.get(i) != null)
				newParent.insertRawTuple(okeys.get(i), ovals.get(i), 0);

		}
		
		return newParent;

	}
	
    /**
     * Adds a new {@link DirectoryPage} when we need to split a child but
     * <code>globalDepth == localDepth</code>. The caller must retry the insert
     * after this method makes the structural change.
     * 
     * <h2>Design discussion</h2>
     * 
     * This method must maintain the invariant that the tree of page references
     * for the hash tree is a strict tree. That is, you can not have two
     * different pages each of which points to the same child. This would be a
     * concurrency nightmare as, e.g., splitting the child could require us to
     * propagate updates to multiple parents. However, even with that constraint
     * we have two options.
     * <p>
     * Given addressBits := 2, the following hash tree state is induced by
     * inserting the key sequence (0x01, 0x02, 0x03, 0x04).
     * 
     * <pre>
     * root := [2] (a,c,b,b)
     * a    := [2]   (1,2,3,4)
     * c    := [2]   (-,-,-,-)
     * b    := [1]   (-,-;-,-)
     * </pre>
     * 
     * where [x] is the depth of the buddies on the corresponding page and ";"
     * indicates a buddy bucket boundary while "," indicates a tuple boundary.
     * <p>
     * If we then attempt to insert a key which would be directed into (a)
     * 
     * <pre>
     * insert(0x20,...)
     * </pre>
     * 
     * then we must split (a) since depth(a):=2 and depth(root):=2. This will
     * introduce a new directory page (d).
     * 
     * <h3>depth(d) := 1</h3>
     * 
     * This gives us the following post-condition.
     * 
     * <pre>
     * root := [2] (d,d,b,b)
     * d    := [1]   (a,a;c,c)   // two ptrs to (d) so 2 buddies on the page
     * a    := [0]     (1;2;3;4) // depth changes since now 2 ptrs to (a)
     * c    := [0]     (-;-;-;-) // depth changes since now 2 ptrs to (c)
     * b    := [1]   (-,-;-,-)
     * </pre>
     * 
     * Regardless of the value of [addressBits], this design gives us
     * [addressBits] buddies on (d) and each buddy has two slots (since the
     * depth of (d) is ONE, each buddy on (d) has a one bit address space and
     * hence uses two slots). The depth(a) and depth(c) will always be reset to
     * ZERO (0) by this design since there will always be TWO pointers to (a)
     * and TWO pointers to (c) in (d). This design provides ONE (1) bit of
     * additional distinctions along the path for which we have exhausted the
     * hash tree address space.
     * 
     * <h3>depth(d) := addressBits</h3>
     * 
     * This gives us the following post-condition.
     * 
     * <pre>
     * root := [2] (a,c,b,b)
     * d    := [2]   (a,a,a,a)   // one ptr to (d) so 1 buddy on the page
     * a    := [0]     (1;2;3;4) // depth changes since now 4 ptrs to (a).
     * c    := [2]   (-,-,-,-)
     * b    := [1]   (-,-;-,-)
     * </pre>
     * 
     * In this design, we always wind up with ONE buddy on (d), the depth(d) is
     * [addressBits], and the depth(a) is reset to ZERO(0). This design focuses
     * the expansion in the address space of the hash tree narrowly on the
     * specific key prefix for which we have run out of distinctions and gives
     * us [addressBits] of additional distinctions along that path.
     * 
     * <h3>Conclusion</h3>
     * 
     * Both designs would appear to be valid. Neither one can lead to a
     * situation in which we have multiple parents for a child. In the first
     * design, the one-bit expansion means that we never have pointers to the
     * same child in more than one buddy bucket, and hence they will all be on
     * the same page. In the second design, the depth of the new directory page
     * is already at the maximum possible value so it can not be split again and
     * thus the pointers to the child will always remain on the same page.
     * <p>
     * It seems that the first design has the advantage of growing the #of
     * distinctions more slowly and sharing the new directory page among
     * multiple such distinctions (all keys having the same leading bit). In the
     * second design, we add a full [addressBits] at once to keys having the
     * same [addressBits] leading bits).
     * <p>
     * It would appear that any choice in the inclusive range (1:addressBits) is
     * permissible as in all cases the pointers to (a) will lie within a single
     * buddy bucket. By factoring out the #of additional bits of distinction to
     * be made when we split a directory page, we can defer this design either
     * to construction time (or perhaps even to runtime) decision. I have
     * therefore introduced an additional parameter on the {@link HTree} for
     * this purpose.
     * 
     * @param oldParentIsAlwaysRoot
     *            The parent {@link DirectoryPage}.
     * @param buddyOffsetInChild
     *            The buddyOffset within the <i>parent</i>. This identifies
     *            which buddy hash table in the parent must be its pointers
     *            updated such that it points to both the original child and new
     *            child.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>splitBits</i> is non-positive.
     * @throws IllegalArgumentException
     *             if <i>splitBits</i> is GT {@link #getAddressBits()}.
     * @throws IllegalStateException
     *             if the depth of the child is GTE the depth of the parent.
     * @throws IllegalStateException
     *             if the <i>parent<i/> is read-only.
     * @throws IllegalStateException
     *             if the <i>oldBucket</i> is read-only.
     * @throws IllegalStateException
     *             if the parent of the <i>oldBucket</i> is not the given
     *             <i>parent</i>.
     * 
     *             FIXME [oldParent] is not required if this is always invoked
     *             with [root] as the [oldParent]. However, we do need to know
     *             which path through the tree is being increased so we can
     *             update the appropriate pointers in the root. This is the
     *             [buddyOffset] of the direct child of the root on the path to
     *             the {@link BucketPage} which which needs to be split.
     *             
     *             @deprecated by the new implementation.
     */
    // Note: package private for unit tests.
    void addLevel(final DirectoryPage oldParentIsAlwaysRoot,
            final int buddyOffsetInChild, final int splitBits) {
        //, final AbstractPage child) {
        
        if (oldParentIsAlwaysRoot == null)
            throw new IllegalArgumentException();
//        if (oldParentIsAlwaysRoot != root)
//            throw new IllegalArgumentException();
//        if (childIsUnused == null)
//            throw new IllegalArgumentException();
//        if (childIsUnused.globalDepth != oldParent.globalDepth) {
//            /*
//             * We only create a new directory page when the global and local
//             * depth are equal.
//             */
//            throw new IllegalStateException();
//        }
        if (buddyOffsetInChild < 0)
            throw new IllegalArgumentException();
        if (buddyOffsetInChild >= (1 << addressBits)) {
            /*
             * Note: This check is against the maximum possible slot index. The
             * actual max buddyOffset depends on parent.globalBits also since
             * (1<<parent.globalBits) gives the #of slots per buddy and the
             * allowable buddyOffset values must fall on an buddy hash table
             * boundary.
             */
            throw new IllegalArgumentException();
        }
        if (splitBits <= 0)
            throw new IllegalArgumentException();
        if (splitBits > addressBits)
            throw new IllegalArgumentException();
        if ((buddyOffsetInChild + splitBits) >= (1 << addressBits)) {
            /*
             * [buddyOffset] is the slot index of the first slot for the buddy
             * hash table in the parent. [splitBits] is the #of address bits to
             * copy into the new directory page. Therefore, [buddyOffset +
             * splitBits] must be GTE ZERO (0) and LT [addressBits].
             */
            throw new IllegalArgumentException();
        }
        if (oldParentIsAlwaysRoot.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
//        if (childIsUnused.isReadOnly()) // must be mutable.
//            throw new IllegalStateException();
//        if (childIsUnused.parent != oldParent.self) // must be same Reference.
//            throw new IllegalStateException();

        if (log.isDebugEnabled())
            log.debug("parent=" + oldParentIsAlwaysRoot.toShortString() + ", buddyOffset="
                    + buddyOffsetInChild);// + ", child=" + childIsUnused);

        // Allocate a new directory page. .
        final DirectoryPage newParent = new DirectoryPage(this, splitBits/* globalDepth */);

        // Set the parent Reference on the new dir page to the old dir page.
        newParent.parent = (Reference) oldParentIsAlwaysRoot.self;

        // One more directory page.
        nnodes++;
        
        assert splitBits == newParent.globalDepth;

        // #of buddy hash tables on the new directory page.
        final int nbuddies = (1 << addressBits) / (1 << newParent.globalDepth);
        
        // #of address slots in each buddy hash table for the new dir page.
        final int nslots = (1 << newParent.globalDepth);

        /*
         * This is a nested loop which copies the pointers to the relevant child
         * pages into the new directory page. We then go through and set each of
         * the slots from which we copied a pointer to be a pointer to the new
         * directory page.
         * 
         * The #of pointers to be copied depends on [splitBits] and defines the
         * local depth of the new directory page. If the local depth of the new
         * directory page is to be ONE (1), then we must copy 1/2 of the
         * pointers from the parent. If the local depth of the new directory
         * page is to be [addressBis], then we must copy 1 of the pointers from
         * the parent.
         * 
         * The outer loop visits the slots we need to copy in the parent.
         * 
         * The inner loop fills each the buddy hash table in the new directory
         * with the current pointer from the outer loop.
         */
        {
            final int lastSrc = (buddyOffsetInChild + nbuddies);
            
            // for each pointer to be copied from the parent.
            int dst = 0; // target slot in the new directory page.
            for (int src = buddyOffsetInChild; src < lastSrc; src++) {

                // pointer to be copied.
                final Reference<AbstractPage> ref = oldParentIsAlwaysRoot.childRefs[src];

                // fill the buddy hash table on the new parent with that ptr.
                for (int i = 0; i < nslots; i++) {

                    newParent.childRefs[dst] = ref;

                    dst++;

                }

            }

            /*
             * Replace the pointer to the child page in the old parent with the
             * pointer to the new directory page.
             */
            for (int src = buddyOffsetInChild; src < lastSrc; src++) {

                oldParentIsAlwaysRoot.childRefs[src] = (Reference) newParent.self;

            }

        }

        /*
         * We need to update the parent reference on each page whose pointer was
         * moved into the new directory page and recompute the global depth of
         * the page as well. Both of these pieces of information are transient,
         * so we only do this for pointers to pages that are currently
         * materialized.
         * 
         * The parent of a page whose pointer was moved needs to be updated
         * because the parent is now the new directory page.
         * 
         * The global depth of a page whose pointer was moved needs to be
         * updated since the #of pointers to that page changed. This can be done
         * by counting the #of pointers in any buddy hash table of the new
         * parent to the child. Since all pointers in a buddy hash table on the
         * new parent point to the child page, the #of pointers in a buddy hash
         * table in the new parent is just the #of slots in a buddy hash table
         * for the new parent.
         * 
         * Note: We have to do this for each buddy hash table on the new
         * directory page. 
         */
        {

            int aBuddyOffset = 0;
            for (int i = 0; i < nbuddies; i++) {

                final Reference<AbstractPage> ref = newParent.childRefs[aBuddyOffset];

                final AbstractPage aChild = ref == null ? null : ref.get();

                if (aChild == null) {
                    // Only update materialized pages.
                    continue;
                }
                
                // Each buddy hash table in the new parent was filled by a single
                // pointer so npointers := nslots
                final int npointers = nslots;

                // recompute the local depth of the child page.
                final int localDepth = HTreeUtil.getLocalDepth(addressBits,
                        newParent.globalDepth, npointers);

                // update the cached local depth on the child page.
                aChild.globalDepth = localDepth;

                // Update the parent reference on the child.
                aChild.parent = (Reference) newParent.self;

                aBuddyOffset += nslots;
                
            }
            
        }

    }

    /**
     * Allocate a sibling bucket of a full bucket page (one buddy bucket) and
     * index the tuples in the caller's bucket between the old bucket and the
     * new bucket. This method MUST be invoked immediately after a new level has
     * been added to the {@link HTree} in order to bring the {@link HTree} into
     * an internally consistent state.
     * <p>
     * If the operation can not succeed because the tuples can not be reindexed
     * into the original {@link BucketPage} and a new {@link BucketPage} given
     * the depth of their common parent then this method will return
     * <code>false</code> and the structure of the {@link HTree} WILL NOT be
     * modified. This contract makes it possible to use this method to decide
     * when incremental structural changes (via add level and split directory)
     * have introduced sufficient distinctions to split a full
     * {@link BucketPage}.
     * <p>
     * While this method is "safe" in the sense described above, the
     * {@link HTree} is NOT in a consistent state following an add level
     * operation until the full {@link BucketPage} has been successfully split
     * using this method. The caller is responsible for executing sufficient add
     * level and/or split directory operations such that the full
     * {@link BucketPage} can be split successfully by this method.
     * 
     * @param parent
     *            The parent {@link DirectoryPage}.
     * @param buddyOffset
     *            The buddyOffset within the <i>parent</i>. This identifies
     *            which buddy hash table in the parent must have its pointers
     *            updated such that it points to both the original child and new
     *            child.
     * @param prefixLength
     *            The bit length of the MSB prefix to the <i>parent</i>
     *            {@link DirectoryPage}.
     * @param oldBucket
     *            The child {@link BucketPage} to be re-indexed.
     * 
     * @return <code>true</code> iff the operation could be carried out without
     *         causing any buddy bucket in either the old {@link BucketPage} or
     *         the new {@link BucketPage} to overflow.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalStateException
     *             if the <i>parent</i> is read-only.
     * @throws IllegalStateException
     *             if the <i>oldBucket</i> is read-only.
     * @throws IllegalStateException
     *             if the parent of the <i>oldBucket</i> is not the given
     *             <i>parent</i>.
     * 
     * @see #addLevel(DirectoryPage, int, int, AbstractPage)
     * 
     * @deprecated no longer used with new {@link #addLevel2(BucketPage)}.
     */
    // Note: package private for unit tests.
    private boolean splitAndReindexFullBucketPage(final DirectoryPage parent,
            final int buddyOffset, final int prefixLength,
            final BucketPage oldBucket) {

        if (parent == null)
            throw new IllegalArgumentException();
        if (oldBucket == null)
            throw new IllegalArgumentException();
        if (buddyOffset < 0)
            throw new IllegalArgumentException();
        if (buddyOffset >= (1 << addressBits)) {
            /*
             * Note: This check is against the maximum possible slot index. The
             * actual max buddyOffset depends on parent.globalBits also since
             * (1<<parent.globalBits) gives the #of slots per buddy and the
             * allowable buddyOffset values must fall on an buddy hash table
             * boundary.
             */
            throw new IllegalArgumentException();
        }
        if ((buddyOffset + splitBits) >= (1 << addressBits)) {
            /*
             * [buddyOffset] is the slot index of the first slot for the buddy
             * hash table in the parent. [splitBits] is the #of address bits to
             * copy into the new directory page. Therefore, [buddyOffset +
             * splitBits] must be GTE ZERO (0) and LT [addressBits].
             */
            throw new IllegalArgumentException();
        }
        if (prefixLength < 0)
            throw new IllegalArgumentException();
        if (parent.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if (oldBucket.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if (oldBucket.parent != parent.self) // must be same Reference.
            throw new IllegalStateException();
        // TODO There must be ONE (1) reference to the old bucket in the parent.
        if (log.isDebugEnabled())
            log.debug("parent=" + parent.toShortString() + ", buddyOffset="
                    + buddyOffset + ", prefixLength=" + prefixLength
                    + ", oldBucket=" + oldBucket.toShortString());

        final int oldDepth = oldBucket.globalDepth;
        final int newDepth = oldDepth + 1;
        
        // Save a copy of the parent's state.
        final Reference<AbstractPage>[] savedChildRefs;
        final IDirectoryData savedData;
        {
            // Note: This is no longer "free" since is allocating a copy of the parent's data.
            savedChildRefs = new Reference[parent.childRefs.length];

            System.arraycopy(parent.childRefs/* src */, 0/* srcPos */,
                    savedChildRefs/* dest */, 0/* destPos */,
                    parent.childRefs.length/* length */);

            savedData = new MutableDirectoryPageData(addressBits,
                    parent.data);

        }
        
        // Allocate a new bucket page (globalDepth is increased by one).
        final BucketPage newBucket = new BucketPage(this, newDepth);

        assert newBucket.isDirty();
        
        // Set the parent reference on the new bucket.
        newBucket.parent = (Reference) parent.self;
        
        // Increase global depth on the old page also.
        oldBucket.globalDepth = newDepth;

        // One more bucket page in the hash tree.
        nleaves++; 

        // update the pointers in the parent.
        updatePointersInParent(parent, buddyOffset, oldDepth, oldBucket,
                newBucket);

        // attempt to reindex the tuples.
        if (!reindexTuples(parent, buddyOffset, prefixLength, oldBucket,
                newBucket)) {
            /*
             * Reindex failed. Return immediately. NO SIDE EFFECTS ON THE HTREE.
             */
            {

                // Restore child reference[].
                System.arraycopy(savedChildRefs/* src */, 0/* srcPos */,
                        parent.childRefs/* dest */, 0/* destPos */,
                        parent.childRefs.length/* length */);

                // Restore the persistent data record on the parent.
                ((MutableDirectoryPageData)parent.data).copyFrom(savedData);
                
                // Restore bucket depth.
                oldBucket.globalDepth = oldDepth;
                
                // Restore #of leaves in the HTree.
                nleaves--;

            }
            
            return false;
        }

        return true;
        
    }

    /**
     * Re-index the tuples in (a), distributing them between (a) and (b)
     * according to the hash bits which are in play at that level of the
     * {@link HTree}.
     * 
     * @param parent
     *            The parent {@link DirectoryPage}.
     * @param buddyOffset
     *            The buddyOffset within the <i>parent</i>. This identifies
     *            which buddy hash table in the parent must be its pointers
     *            updated such that it points to both the original child and new
     *            child.
     * @param prefixLength
     *            The bit length of the MSB prefix to the <i>parent</i>
     *            {@link DirectoryPage}.
     * @param a
     *            The original bucket.
     * @param b
     *            The new sibling bucket.
     * 
     *            TODO Handle case where the original {@link BucketPage} will
     *            have more than buddy bucket after reindexing (that is, the
     *            parent has a depth LT addressBits).
     * 
     *            TODO This method should return <code>true</code> iff the
     *            indexing operation was successful and should not have a side
     *            effect if the operation could not be completed (because some
     *            buddy bucket would have overflowed).
     *            
     *            @deprecated by the alternative implementation.
     */
    private boolean reindexTuples(final DirectoryPage parent,
            final int buddyOffset, final int prefixLength, final BucketPage a,
            final BucketPage b) {

        if (parent == null)
            throw new IllegalArgumentException();
        if (a == null)
            throw new IllegalArgumentException();
        if (b == null)
            throw new IllegalArgumentException();
        if (a.isReadOnly())
            throw new IllegalStateException();
        if (b.isReadOnly())
            throw new IllegalStateException();
        if (a.parent.get() != parent)
            throw new IllegalStateException();
        if (b.parent.get() != parent)
            throw new IllegalStateException();

        if (log.isDebugEnabled())
            log.debug("parent=" + parent.toShortString() + ", buddyOffset="
                    + buddyOffset + ", prefixLength=" + prefixLength
                    + ", oldChild=" + a.toShortString() + ", newChild="
                    + b.toShortString());

        // #of buddy tables on a page (post-condition for both bucket pages).
        final int nbuddies = (1 << addressBits) / (1 << parent.globalDepth);

        // #of address slots in each buddy hash table (post-condition).
        final int slotsPerBuddy = (1 << parent.globalDepth);

        // The #of hash bucket buddies across both bucket pages.
        final int nbins = nbuddies << 1;

        /**
         * A counter per buddy hash bucket. The index into [bins] is the
         * <code>page * nbuddies</code>, where the original page is page:=0 and
         * the new page is page:=1. If any counter exceeds <i>slotsPerBuddy</i>
         * then the reindex operation will fail since the corresponding buddy
         * bucket would overflow. The counter is incremented before we copy the
         * tuple to the target page so we detect an overflow before it occurs.
         */
        final int[] bins = new int[nbins];
        
        // Setup [t] as a temporary page with [a]'s data and clear [a]'s data.
        final BucketPage t = new BucketPage(this, a.globalDepth);//Note: the depth of the temporary BucketPage does not really matter. It could be addressBits since it is ignored by this code.
        {
            final ILeafData tmp = a.data;
            a.data = t.data;
            t.data = tmp;
        }

        /*
         * Visit each tuple which was in [a], figure out which of the two bucket
         * pages it needs to be in, and insert the tuple into the appropriate
         * bucket page.
         */
        
        // #of slots in [a]'s data.
        final IRaba keys = t.data.getKeys();
        final IRaba vals = t.data.getValues();
        final int m = 1 << addressBits; // aka fan-out or branching factor.
        assert m == keys.capacity();
//        // #of tuples inserted into [a] and [b] respectively.
//        int na = 0, nb = 0;
        for (int sourceIndex = 0; sourceIndex < m; sourceIndex++) {

            // Since the bucket is full, no entry should be null.
            assert !keys.isNull(sourceIndex);

            /*
             * Re-insert the tuple
             * 
             * Note: A re-insert via the top-level entry point is logically
             * correct. However, it will: (a) cause the raw record to be
             * materialized and written onto the backing store a second time
             * (which is wasteful in the extreme); (b) have a side-effect on the
             * revision timestamps (if we support them); and (c) cause deleted
             * tuples to "reappear" (if we support delete markers). For all of
             * these reasons, we need to handle the "re-insert" of the tuples
             * at a much lower level in the API.
             */

            // materialize the key.
            final byte[] key = keys.get(sourceIndex);

            /*
             * Figure out whether the tuple would be directed into (a) or (b) by
             * the parent. Then figure out which buddy bucket on the target page
             * will get that tuple.
             */
            final int hashBitsParent = parent.getLocalHashCode(key, prefixLength);

            // Find the child which will get this tuple.
            final AbstractPage child = parent.getChild(hashBitsParent, buddyOffset);
            
            // The child must be a bucket page.
            assert child.isLeaf(); 
            
            // The child be one of the two bucket pages we are working with.
            assert (child == a || child == b);

            // True if the tuple is re-indexed back into (a).
            final boolean isA = child == a;
            
            // The page index component into [bins].
            final int page = isA ? 0 : 1;

            final BucketPage x = (BucketPage) child;

            // find the offset of the buddy hash table in the child.
            final int targetBuddyOffset = HTreeUtil
                    .getBuddyOffset(hashBitsParent, parent.globalDepth,
                            child.globalDepth/* localDepthOfChild */);

            // next free slot in the target buddy bucket.
            final int targetSlotInBuddy = bins[page * nbuddies
                    + targetBuddyOffset]++;

            if (targetSlotInBuddy >= slotsPerBuddy) {

                /*
                 * The buddy hash table has overflowed.
                 * 
                 * Note: Undo the changes to the original bucket page such that
                 * this method does not have a side effect if the index fails.
                 */

                log.warn("Buddy bucket overflow: page=" + page
                        + ", targetBuddyOffset=" + targetBuddyOffset
                        + ", bins=" + Arrays.toString(bins));

                // restore the data for the original page.
                a.data = t.data;
                
                return false;
                
            }
            
            // The index in the child where we will write the tuple.
            final int targetIndexOnPage = targetBuddyOffset * slotsPerBuddy
                    + targetSlotInBuddy;

            assert x.data.getKeys().isNull(targetIndexOnPage);
            
            // Copy the key.
            x.data.getKeys().set(targetIndexOnPage, key);
            
            // Copy the value.
            if (!vals.isNull(sourceIndex))
                x.data.getValues().set(targetIndexOnPage, vals.get(sourceIndex));

            // TODO copy versionTimestamp and delete marker metadata too.

//            // Increment the index of the next target tuple in this child.
//            if (isA)
//                na++;
//            else
//                nb++;
            
        }
        
        // Success
        return true;

    }

	/**
	 * Re-index the tuples in an old {@link BucketPage}, distributing them
	 * between (a) and (b) according to the hash bits which are in play at that
	 * level of the {@link HTree}.
	 * 
	 * @param oldPage
	 *            The old {@link BucketPage} whose tuples will be reindexed.
	 * @param a
	 *            The original bucket.
	 * @param b
	 *            The new sibling bucket.
	 * 
	 * @deprecated No longer used with new {@link #addLevel2(BucketPage)}.
	 */
	private void reindexTuples(final BucketPage oldPage, final BucketPage a,
			final BucketPage b) {

        if (a == null)
            throw new IllegalArgumentException();
        if (b == null)
            throw new IllegalArgumentException();
        if (a.isReadOnly())
            throw new IllegalStateException();
        if (b.isReadOnly())
            throw new IllegalStateException();
		if (b.globalDepth != a.globalDepth) {
			/*
			 * Must be at the same depth (these are newly created bucket pages
			 * of a common parent).
			 */
			throw new IllegalStateException();
		}
        final DirectoryPage parent = a.getParentDirectory();
        if (a.parent.get() != parent)
            throw new IllegalStateException();
        if (b.parent.get() != parent)
            throw new IllegalStateException();
		if (parent.globalDepth != addressBits) {
			/*
			 * The parent must be a directory with depth=addressBits, which is a
			 * precondition of the method which invokes this (addLevel()).
			 * Hence, the buddyOffset in the parent will be ZERO (0) since there
			 * is only one buddy in the parent when its depth := addressBits.
			 */
			throw new IllegalStateException();
		}
		// prefixLength to the parent directory page.
		final int prefixLength = parent.getPrefixLength();
		// Note: buddyOffset in parent will be zero per above.
		final int buddyOffset = 0; // since depth==addressBits, only 1 buddy.
		
		if (log.isDebugEnabled())
			log.debug("oldPage=" + oldPage.toShortString() + ", buddyOffset="
					+ buddyOffset + ", prefixLength=" + prefixLength + ", a="
					+ a.toShortString() + ", b=" + b.toShortString());

        // #of buddy buckets on a page (post-condition for both bucket pages).
        final int nbuddies = (1 << addressBits) / (1 << a.globalDepth);

        // #of address slots in each buddy hash bucket (post-condition).
        final int slotsPerBuddy = (1 << a.globalDepth);

        // The #of hash bucket buddies across both bucket pages.
        final int nbins = nbuddies << 1;

        /**
         * A counter per buddy hash bucket. The index into [bins] is the
         * <code>page * nbuddies</code>, where the original page is page:=0 and
         * the new page is page:=1. If any counter exceeds <i>slotsPerBuddy</i>
         * then the reindex operation will fail since the corresponding buddy
         * bucket would overflow. The counter is incremented before we copy the
         * tuple to the target page so we detect an overflow before it occurs.
         */
        final int[] bins = new int[nbins];
        
        final BucketPage t = oldPage;

		/*
		 * Visit each tuple which was in [oldPage], figure out which of the two
		 * bucket pages it needs to be in, and insert the tuple into the
		 * appropriate bucket page.
		 */
        
        final IRaba keys = t.data.getKeys();
        final IRaba vals = t.data.getValues();
        final int m = 1 << addressBits; // aka fan-out or branching factor.
        assert m == keys.capacity();

        for (int sourceIndex = 0; sourceIndex < m; sourceIndex++) {

            // Since the bucket is full, no entry should be null.
            assert !keys.isNull(sourceIndex);

            /*
             * Re-insert the tuple
             * 
             * Note: A re-insert via the top-level entry point is logically
             * correct. However, it will: (a) cause the raw record to be
             * materialized and written onto the backing store a second time
             * (which is wasteful in the extreme); (b) have a side-effect on the
             * revision timestamps (if we support them); and (c) cause deleted
             * tuples to "reappear" (if we support delete markers). For all of
             * these reasons, we need to handle the "re-insert" of the tuples
             * at a much lower level in the API.
             */

            // materialize the key.
            final byte[] key = keys.get(sourceIndex);

			/*
			 * Figure out whether the tuple would be directed into (a) or (b) by
			 * the parent. Then figure out which buddy bucket on the target page
			 * will get that tuple.
			 */
			final int hashBitsParent = parent.getLocalHashCode(key,
					prefixLength);

			// Find the child which will get this tuple.
			final AbstractPage child = parent.getChild(hashBitsParent,
					buddyOffset);

			// The child must be a bucket page.
			assert child.isLeaf();

			// The child be one of the two bucket pages we are working with.
            assert (child == a || child == b);

            // True if the tuple is re-indexed back into (a).
            final boolean isA = child == a;
            
//            // The page index component into [bins].
//            final int page = isA ? 0 : 1;

            final BucketPage x = (BucketPage) child;

            // find the offset of the buddy hash table in the child.
            final int targetBuddyOffset = HTreeUtil
                    .getBuddyOffset(hashBitsParent, parent.globalDepth,
                            child.globalDepth/* localDepthOfChild */);

            // next free slot in the target buddy bucket.
            final int targetSlotInBuddy = bins[targetBuddyOffset]++;

            if (targetSlotInBuddy >= slotsPerBuddy) {

                /*
                 * The buddy hash table has overflowed.
                 * 
                 * Note: Undo the changes to the original bucket page such that
                 * this method does not have a side effect if the index fails.
                 */

				throw new RuntimeException("Buddy bucket overflow: page="
						+ (isA?"a":"b") + ", targetBuddyOffset=" + targetBuddyOffset
						+ ", bins=" + Arrays.toString(bins));

            }
            
			// The index in the child where we will write the tuple.
			final int targetIndexOnPage = targetBuddyOffset + targetSlotInBuddy;

            assert x.data.getKeys().isNull(targetIndexOnPage);
            
            // Copy the key.
            x.data.getKeys().set(targetIndexOnPage, key);
            
            // Copy the value.
            if (!vals.isNull(sourceIndex))
                x.data.getValues().set(targetIndexOnPage, vals.get(sourceIndex));

            // TODO copy versionTimestamp and delete marker metadata too.

        }
        
    }

	/**
	 * Handle split if localDepth LT globalDepth (so there is more than one
	 * buddy directory on the page) but we need to increase the prefix bit
	 * length in order to make a distinction in some child of this directory
	 * page. This doubles the size of each buddy hash table, thus increasing the
	 * prefix length of the paths to the children by one bit.
	 * <p>
	 * This allocates a new directory page; updates the references to the old
	 * directory page in the parent (one half will point to the old directory
	 * page, the other half will point to the new directory page). Since the #of
	 * references to the old page has changed, its local depth is now different
	 * (there are 1/2 as many references to the old directory page, which means
	 * that it's local depth will be increased by one).
	 * <p>
	 * The buddy hash tables on the old directory page are expanded to twice
	 * their original size, and half of them wind up on the new directory page.
	 * All references in a given buddy hash table are to the same child page, so
	 * the empty slots are filled in as appropriate.
	 * 
	 * @param parent
	 *            The parent {@link DirectoryPage}.
	 * @param buddyOffset
	 *            The buddyOffset within the <i>parent</i>. This identifies
	 *            which buddy hash table in the parent must be its pointers
	 *            updated such that it points to both the original child and new
	 *            child.
	 * @param oldChild
	 *            The child {@link DirectoryPage} to be split.
	 * 
	 * @throws IllegalArgumentException
	 *             if any argument is <code>null</code>.
	 * @throws IllegalStateException
	 *             if the depth of the child is GTE the depth of the parent.
	 * @throws IllegalStateException
	 *             if the <i>parent<i/> is read-only.
	 * @throws IllegalStateException
	 *             if the <i>oldChild</i> is read-only.
	 * @throws IllegalStateException
	 *             if the parent of the <i>oldChild</i> is not the given
	 *             <i>parent</i>.
	 * 
	 * @deprecated since directory pages appear to always be at maximum depth
	 *             (depth:=addressBits) there is no reason to ever split a
	 *             directory page.
	 */
    private void splitDirectoryPage(final DirectoryPage parent, final int buddyOffset,
            final DirectoryPage oldChild) {

        if (parent == null)
            throw new IllegalArgumentException();
        if (oldChild == null)
            throw new IllegalArgumentException();
        if (oldChild.globalDepth >= parent.globalDepth) {
            /*
             * In this case we have to introduce a new directory level instead
             * (increasing the height of the tree at that point).
             */
            throw new IllegalStateException();
        }
        if (buddyOffset < 0)
            throw new IllegalArgumentException();
        if (buddyOffset >= (1 << addressBits)) {
            /*
             * Note: This check is against the maximum possible slot index. The
             * actual max buddyOffset depends on parent.globalBits also since
             * (1<<parent.globalBits) gives the #of slots per buddy and the
             * allowable buddyOffset values must fall on an buddy hash table
             * boundary.
             */
            throw new IllegalArgumentException();
        }
        if(parent.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if(oldChild.isReadOnly()) // must be mutable.
            throw new IllegalStateException();
        if (oldChild.parent != parent.self) // must be same Reference.
            throw new IllegalStateException();
        
        if (log.isDebugEnabled())
            log.debug("parent=" + parent.toShortString() + ", buddyOffset="
                    + buddyOffset + ", child=" + oldChild.toShortString());

        final int oldDepth = oldChild.globalDepth;
        final int newDepth = oldDepth + 1;

        // Allocate a new bucket page (globalDepth is increased by one).
        final DirectoryPage newChild = new DirectoryPage(this, newDepth);

        assert newChild.isDirty();
        
        // Set the parent reference on the new bucket.
        newChild.parent = (Reference) parent.self;
        
        // Increase global depth on the old page also.
        oldChild.globalDepth = newDepth;

        nnodes++; // One more directory page in the hash tree. 

        // update the pointers in the parent.
        updatePointersInParent(parent, buddyOffset, oldDepth, oldChild,
                newChild);
        
        // redistribute buddy buckets between old and new pages.
        redistributeBuddyTables(oldDepth, newDepth, oldChild, newChild);

        // TODO assert invariants?
        
    }

	/**
	 * Redistribute the buddy hash tables in a {@link DirectoryPage}.
	 * <p>
	 * Note: We are not changing the #of hash tables, just their size and the
	 * page on which they are found. Any reference in a source buddy hash table
	 * will wind up in the "same" buddy hash table afterwards, but the page and
	 * offset on the page of the buddy hash table may have been changed and the
	 * size of the buddy hash table will have doubled.
	 * <p>
	 * When a {@link DirectoryPage} is split, the size of each buddy hash table
	 * is doubled. The additional slots in each buddy hash table are filled in
	 * by (a) spacing out the old slot entries in each buddy hash table; and (b)
	 * filling in the uncovered slot with a copy of the previous slot.
	 * <p>
	 * We proceed backwards, moving the upper half of the buddy hash tables to
	 * the new directory page first and then spreading out the lower half of the
	 * source page among the new buddy hash table boundaries on the source page.
	 * 
	 * @param oldDepth
	 *            The depth of the old {@link DirectoryPage} before the split.
	 * @param newDepth
	 *            The depth of the old and new {@link DirectoryPage} after the
	 *            split (this is just oldDepth+1).
	 * @param oldDir
	 *            The old {@link DirectoryPage}.
	 * @param newDir
	 *            The new {@link DirectoryPage}.
	 * 
	 * @deprecated with
	 *             {@link #splitDirectoryPage(DirectoryPage, int, DirectoryPage)}
	 */
    private void redistributeBuddyTables(final int oldDepth,
            final int newDepth, final DirectoryPage oldDir,
            final DirectoryPage newDir) {

        assert oldDepth + 1 == newDepth;
        
        // #of slots on the directory page (invariant given addressBits).
        final int slotsOnPage = (1 << addressBits);

        // #of address slots in each old buddy hash table.
        final int slotsPerOldBuddy = (1 << oldDepth);

        // #of address slots in each new buddy hash table.
        final int slotsPerNewBuddy = (1 << newDepth);

        // #of buddy tables on the old bucket directory.
        final int oldBuddyCount = slotsOnPage / slotsPerOldBuddy;

        // #of buddy tables on the directory page after the split.
        final int newBuddyCount = slotsOnPage / slotsPerNewBuddy;

        final DirectoryPage srcPage = oldDir;
        final long[] srcAddrs = ((MutableDirectoryPageData) oldDir.data).childAddr;
        final Reference<AbstractPage>[] srcRefs = oldDir.childRefs;

        /*
         * Move top 1/2 of the buddy hash tables from the child to the new page.
         */
        {

            // target is the new page.
            final DirectoryPage dstPage = newDir;
            final long[] dstAddrs = ((MutableDirectoryPageData) dstPage.data).childAddr;
            final Reference<AbstractPage>[] dstRefs = dstPage.childRefs;

            // index (vs offset) of first buddy in upper half of src page.
            final int firstSrcBuddyIndex = (oldBuddyCount >> 1);

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of src page.
            final int lastSrcBuddyIndex = oldBuddyCount;

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of target page.
            final int lastDstBuddyIndex = newBuddyCount;

            // work backwards over buddies to avoid stomping data!
            for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
            srcBuddyIndex >= firstSrcBuddyIndex; //
            srcBuddyIndex--, dstBuddyIndex--//
            ) {

                final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

                final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

                final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

                for (int srcSlot = firstSrcSlot, dstSlot = firstDstSlot; srcSlot < lastSrcSlot; srcSlot++, dstSlot += 2) {

                    if (log.isTraceEnabled())
                        log.trace("moving: page(" + srcPage.toShortString()
                                + "=>" + dstPage.toShortString() + ")"
                                + ", buddyIndex(" + srcBuddyIndex + "=>"
                                + dstBuddyIndex + ")" + ", slot(" + srcSlot
                                + "=>" + dstSlot + ")");

                    for (int i = 0; i < 2; i++) {
                        // Copy data to slot
                        dstAddrs[dstSlot + i] = srcAddrs[srcSlot];
                        dstRefs[dstSlot + i] = srcRefs[srcSlot];
                    }

                }

            }

        }

        /*
         * Reposition the bottom 1/2 of the buddy buckets on the old page.
         * 
         * Again, we have to move backwards through the buddy tables on the
         * source page to avoid overwrites of data which has not yet been
         * copied. Also, notice that the buddy table at index ZERO does not
         * move - it is already in place even though it's size has doubled.
         */
        {

            // target is the old page.
            final DirectoryPage dstPage = oldDir;
            final long[] dstAddrs = ((MutableDirectoryPageData) dstPage.data).childAddr;
            final Reference<AbstractPage>[] dstRefs = dstPage.childRefs;

            // index (vs offset) of first buddy in lower half of src page.
            final int firstSrcBuddyIndex = 0;

            // exclusive upper bound for index (vs offset) of last buddy in
            // lower half of src page.
            final int lastSrcBuddyIndex = (oldBuddyCount >> 1);

            // exclusive upper bound for index (vs offset) of last buddy in
            // upper half of target page (which is also the source page).
            final int lastDstBuddyIndex = newBuddyCount;

            /*
             * Work backwards over buddy buckets to avoid stomping data!
             * 
             * Note: Unlike with a BucketPage, we have to spread out the data in
             * the slots of the first buddy hash table on the lower half of the
             * page as well to fill in the uncovered slots. This means that we
             * have to work backwards over the slots in each source buddy table
             * to avoid stomping our data.
             */
            for (int srcBuddyIndex = lastSrcBuddyIndex - 1, dstBuddyIndex = lastDstBuddyIndex - 1; //
            srcBuddyIndex >= firstSrcBuddyIndex; // 
            srcBuddyIndex--, dstBuddyIndex--//
            ) {

                final int firstSrcSlot = srcBuddyIndex * slotsPerOldBuddy;

                final int lastSrcSlot = (srcBuddyIndex + 1) * slotsPerOldBuddy;

//                final int firstDstSlot = dstBuddyIndex * slotsPerNewBuddy;

                final int lastDstSlot = (dstBuddyIndex + 1) * slotsPerNewBuddy;

                for (int srcSlot = lastSrcSlot-1, dstSlot = lastDstSlot-1; srcSlot >= firstSrcSlot; srcSlot--, dstSlot -= 2) {

                    if (log.isTraceEnabled())
                        log.trace("moving: page(" + srcPage.toShortString()
                                + "=>" + dstPage.toShortString() + ")"
                                + ", buddyIndex(" + srcBuddyIndex + "=>"
                                + dstBuddyIndex + ")" + ", slot(" + srcSlot
                                + "=>" + dstSlot + ")");

                    for (int i = 0; i < 2; i++) {
                        // Copy data to slot.
                        dstAddrs[dstSlot - i] = srcAddrs[srcSlot];
                        dstRefs[dstSlot - i] = srcRefs[srcSlot];
                    }
                    
                }

            }

        }

    }

//	/**
//	 * Reindex all tuples spanned by a {@link DirectoryPage}. This method is
//	 * used when the prefixLength is changed, whether by adding a new level or
//	 * by splitting a directory. Both actions can cause the tuples below that
//	 * point in the tree to shift from one buddy bucket to another since a new
//	 * distinction has been introduced in the {@link DirectoryPage}.
//	 * <p>
//	 * This implementation builds a new {@link HTree} sharing the same backing
//	 * store whose root is a {@link DirectoryPage} having the same depth as the
//	 * given {@link DirectoryPage}. The {@link BucketPage}s below the given
//	 * {@link DirectoryPage} are visited and each tuple is inserted into the new
//	 * htree root.
//	 * <p>
//	 * Unlike a normal {@link HTree}, the {@link HTree} into which the tuples
//	 * are being inserted may have a root {@link DirectoryPage} whose depth is
//	 * less than <i>addressBits</i>. This means that we must determine the buddy
//	 * hash table on the root {@link DirectoryPage} into which the key would be
//	 * inserted. This requires us to note the prefixLength to the given
//	 * {@link DirectoryPage} and to compute the buddyOffset into the root
//	 * {@link DirectoryPage} based on that prefix length and the depth of the
//	 * parent of the source {@link DirectoryPage}. Those values can then be
//	 * passed into an appropriate insert() method (they are ZERO(s) for a normal
//	 * {@link HTree}).
//	 * <p>
//	 * A post-order iterator is used to visit the pages spanned by the given
//	 * {@link DirectoryPage}. Each page is released (freed) on the backing store
//	 * after it has been processed.
//	 * <p>
//	 * Note: When more than one add level and/or split directory operation is
//	 * required to transform a tree such that it can accept a new key, all such
//	 * transforms should be carried out and the resulting tree reindexed using
//	 * this method as an after action in order to minimize the amount of work
//	 * required to bring the index back into an internally consistent state.
//	 * 
//	 * @param d
//	 *            The {@link DirectoryPage}.
//	 */
//    void reindexDirectory(final DirectoryPage d) {
//
//		if (log.isInfoEnabled()) {
//			log.info("directoryPage=" + d.toShortString());
//		}
//    	
//    	assert d.isDirty(); // must be mutable (not persistent).
//
//		/*
//		 * Setup a temporary HTree backed by the same persistence store. The
//		 * root of the temporary HTree will be at the same depth as the
//		 * directory page to be reindexed.
//		 * 
//		 * FIXME We need an alternative constructor for the HTree which accepts
//		 * [prefixLength], the depth of the parent of the given directory page,
//		 * and the depth of the given directory page. Those can be stored on
//		 * fields named [rootPrefixLength] (zero for a normal HTree),
//		 * [externalGlobalDepth] (addressBits for a normal HTree), and
//		 * [rootGlobalDepth] (addressBits for a normal HTree).
//		 * 
//		 * FIXME Modify insert(key,val) to use those new fields. It will
//		 * initialize its [prefixLength] from [rootPrefixLength]. It will need
//		 * to compute the [buddyOffset] from the depth of the root (the given
//		 * directory) and the depth of the external parent of the root (the
//		 * parent of the given directory).
//		 * 
//		 * FIXME Update the addLevels() unit test to invoke reindexDirectory()
//		 * after the splitDirectory() call where things are going wrong now. If
//		 * this improves matters, then write more unit tests for
//		 * reindexDirectory().
//		 */
//		final HTree tmp;
//		{
//
//			final int rootPrefixLength = d.getPrefixLength();
//			final DirectoryPage pd = d.getParentDirectory();
//			final int externalPageDepth = pd == null ? 0 : pd.globalDepth;
//
//			final int rootPageDepth = d.globalDepth;
//
//			tmp = new HTree(store, addressBits/* addressBits */, rawRecords,
//					rootPrefixLength, externalPageDepth, rootPageDepth);
//		}
//    	
//    	final Iterator<AbstractPage> itr = d.postOrderIterator();
//    	
//    	while(itr.hasNext()) {
//    		
//    		final AbstractPage page = itr.next();
//    		
//    		if(page.isLeaf()) {
//
//				/*
//				 * Insert into the temporary HTree.
//				 * 
//				 * Note: In order to avoid materializing raw records this needs
//				 * to operate against the actual byte[] value stored in the
//				 * leaf. Going through a higher level API, such as ITuple, will
//				 * cause a raw record to be materialized from the backing
//				 * store!!!
//				 * 
//				 * FIXME If we support additional metadata (version timestamps
//				 * or delete markers) then those need to be handled here as
//				 * well.
//				 */
//    			
//    			final BucketPage bucketPage = (BucketPage)page;
//    			
//    			final IRaba keys = bucketPage.getKeys();
//
//    			final IRaba vals = bucketPage.getValues();
//    			
//				final int slotsPerPage = 1 << addressBits;
//
//				for (int i = 0; i < slotsPerPage; i++) {
//
//					if (keys.isNull(i))
//						continue;
//
//					final byte[] key = keys.get(i);
//					
//					final byte[] val = vals.get(i);
//					
//					tmp.insert(key, val);
//
//				}
//    			
//    		}
//    		
//			if (page.isPersistent()) {
//
//				// delete the source page.
//				store.delete(page.getIdentity());
//
//			}
//
//    	}
//    	
//		/*
//		 * Steal the data for the root of the temporary htree.
//		 */
//
//    	d.data = tmp.root.data;
//		
//    	System.arraycopy(tmp.root.childRefs/* src */, 0/* srcOff */,
//				d.childRefs/* dst */, 0/* dstOff */, tmp.root.childRefs.length/* length */);
//    	
//    	/*
//    	 * link any childRefs to point to new parent
//    	 */
//     	for (int i = 0; i < d.childRefs.length; i++) {
//    		d.childRefs[i].get().parent = (Reference<DirectoryPage>) d.self;
//    	}
//    }
    
//	/**
//	 * Validate pointers in buddy hash table in the parent against the global
//	 * depth as self-reported by the child. By definition, the global depth of
//	 * the child is based on the global depth of the parent and the #of pointers
//	 * to the child in the parent. The {@link AbstractPage#globalDepth} value is
//	 * the cached result of that computation. It can be cross checked by
//	 * counting the actual number of pointers in the parent and comparing that
//	 * with this formula:
//	 * 
//	 * <pre>
//	 * npointers := 1 &lt;&lt; (parent.globalDepth - child.globalDepth)
//	 * </pre>
//	 * 
//	 * This method validates that the cached value of global depth on the child
//	 * is consistent with the #of pointers in the specified buddy hash table of
//	 * the parent. However, this validate depends on the global depth of the
//	 * parent itself being correct.
//	 * <p>
//	 * Note: While other buddy hash tables in the parent can point into the same
//	 * child page. However, due to the buddy offset computation they will not
//	 * point into the same buddy on the same child.
//	 * 
//	 * @param parent
//	 *            The parent {@link DirectoryPage}.
//	 * @param buddyOffset
//	 *            The buddyOffset within the <i>parent</i>. This identifies
//	 *            which buddy hash table in the parent should have references to
//	 *            the child.
//	 * @param child
//	 *            The child {@link AbstractPage}.
//	 */
//	private void validatePointersInParent(final DirectoryPage parent,
//			final int buddyOffset, final AbstractPage child) {
//
//		// #of address slots in the parent buddy hash table.
//		final int slotsPerBuddy = (1 << parent.globalDepth);
//
//		// #of pointers expected in the parent buddy hash table.
//		final int npointers = 1 << (parent.globalDepth - child.globalDepth);
//		
//		// The first slot in the buddy hash table in the parent.
//		final int firstSlot = buddyOffset;
//		
//		// The last slot in the buddy hash table in the parent.
//		final int lastSlot = buddyOffset + slotsPerBuddy;
//
//		if (parent.isDirty() && parent.getIdentity() != IRawStore.NULL)
//			throw new RuntimeException(
//					"Parent address should be NULL since parent is dirty");
//
//		if (child.isDirty() && child.getIdentity() != IRawStore.NULL)
//			throw new RuntimeException(
//					"Child address should be NULL since child is dirty");
//		
//		/*
//		 * Count pointers to the child page. There should be [npointers] of them
//		 * and they should be contiguous. Since the child is materialized (we
//		 * have its reference) we can test pointers. If the child is dirty, then
//		 * all slots in the parent having pointers to the child should be
//		 * associated with NULL addresses for the child. If the child is clean,
//		 * then all slots in the parent having references for the child should
//		 * have the same non-zero address for the child. Also, any slot in the
//		 * parent having the address of a persistent child should have the same
//		 * Reference object, which should be child.self.
//		 */
//		int firstPointer = -1;
//		int nfound = 0;
//		boolean discontiguous = false;
//		for (int i = firstSlot; i < lastSlot; i++) {
//			final boolean slotRefIsChild = parent.childRefs[i] == child.self;
//			final boolean slotAddrIsChild = parent.getChildAddr(i) == child
//					.getIdentity();
//			if (slotAddrIsChild && !slotRefIsChild)
//				throw new RuntimeException(
//						"Child reference in parent should be child since addr in parent is child addr");
//			final boolean slotIsChild = slotRefIsChild || slotAddrIsChild;
//			if (slotIsChild) {
//				if (child.isDirty()) {
//					// A dirty child must have a NULL address in the parent.
//					if (parent.data.getChildAddr(i) != IRawStore.NULL)
//						throw new RuntimeException(
//								"Child address in parent should be NULL since child is dirty");
//				} else {
//					// A clean child must have a non-NULL address in the parent.1
//					if (parent.data.getChildAddr(i) == IRawStore.NULL)
//						throw new RuntimeException(
//								"Child address in parent must not be NULL since child is clean");
//				}
//				if (firstPointer == -1)
//					firstPointer = i;
//				nfound++;
//			} else {
//				if (firstPointer != -1 && nfound != npointers) {
//					discontiguous = true;
//				}
//			}
//		}
//		if (firstPointer == -1)
//			throw new RuntimeException("No pointers to child");
//		if (nfound != npointers) {
//			/*
//			 * This indicates either a problem in maintaining the pointers
//			 * (References and/or addresses) in the parent for the child, a
//			 * problem where child.self is not a unique Reference for the child,
//			 * and/or a problem with the cached [globalDepth] for the child.
//			 */
//			throw new RuntimeException("Expected " + npointers
//					+ " pointers to child, but found=" + nfound);
//		}
//		if (discontiguous)
//			throw new RuntimeException(
//					"Pointers to child are discontiguous in parent's buddy hash table.");
//	} // validatePointersInParent

//	/**
//	 * TODO Scan the entire parent for addresses and References to each child. Any
//	 * time we see the address of a child, then the Reference must be
//	 * [child.self]. Any time we see a reference which is not child.self, then
//	 * the address for that slot must not be the same as the address of the
//	 * child.
//	 * <p>
//	 * Note: Since the child is materialized, any Reference in the parent to
//	 * that child should be [child.self]. Due to the buddy page system, when we
//	 * load a child we need to scan the parent across all buddy hash tables and
//	 * set the Reference on any slot having the address of that child. Likewise,
//	 * when the child becomes dirty, we need to clear the address of the child
//	 * in any slot of any buddy hash table in the parent.
//	 */
//	private void validateChildren(final DirectoryPage parent) {
//		
//	}
	
	/**
     * Persistence capable abstract base class for HTree pages.
     * 
     * @author thompsonbry
     */
	static abstract class AbstractPage 
	extends PO 
	implements //IAbstractNode?,
			IAbstractNodeData {

	    @Override
	    public String toShortString() {
            
	        return super.toShortString() + "{d=" + globalDepth + "}";
	        
	    }
	    
	    /**
	     * The HTree.
	     * 
	     * Note: This field MUST be patched when the node is read from the store.
	     * This requires a custom method to read the node with the HTree reference
	     * on hand so that we can set this field.
	     */
	    final transient protected AbstractHTree htree;

		/**
		 * The parent of this node. This is null for the root node. The parent
		 * is required in order to set the persistent identity of a newly
		 * persisted child node on its parent. The reference to the parent will
		 * remain strongly reachable as long as the parent is either a root
		 * (held by the {@link HTree}) or a dirty child (held by the
		 * {@link DirectoryPage}). The parent reference is set when a node is
		 * attached as the child of another node.
		 * <p>
		 * Note: When a node is cloned by {@link #copyOnWrite()} the parent
		 * references for its <em>clean</em> children are set to the new copy of
		 * the node. This is referred to in several places as "stealing" the
		 * children since they are no longer linked back to their old parents
		 * via their parent reference.
		 */
	    transient protected Reference<DirectoryPage> parent = null;

		/**
		 * <p>
		 * A {@link Reference} to this {@link AbstractPage}. This is created
		 * when the node is created and is reused by a children of the node as
		 * the {@link Reference} to their parent. This results in few
		 * {@link Reference} objects in use by the HTree since it effectively
		 * provides a canonical {@link Reference} object for any given
		 * {@link AbstractPage}.
		 * </p>
		 */
	    transient protected final Reference<? extends AbstractPage> self;
	    
	    /**
	     * The #of times that this node is present on the {@link HardReferenceQueue} .
	     * This value is incremented each time the node is added to the queue and is
	     * decremented each time the node is evicted from the queue. On eviction, if
	     * the counter is zero(0) after it is decremented then the node is written
	     * on the store. This mechanism is critical because it prevents a node
	     * entering the queue from forcing IO for the same node in the edge case
	     * where the node is also on the tail on the queue. Since the counter is
	     * incremented before it is added to the queue, it is guaranteed to be
	     * non-zero when the node forces its own eviction from the tail of the
	     * queue. Preventing this edge case is important since the node can
	     * otherwise become immutable at the very moment that it is touched to
	     * indicate that we are going to update its state, e.g., during an insert,
	     * split, or remove operation. This mechanism also helps to defer IOs since
	     * IO can not occur until the last reference to the node is evicted from the
	     * queue.
	     * <p>
	     * Note that only mutable {@link BTree}s may have dirty nodes and the
	     * {@link BTree} is NOT thread-safe for writers so we do not need to use
	     * synchronization or an AtomicInteger for the {@link #referenceCount}
	     * field.
	     */
	    transient protected int referenceCount = 0;
        
		/**
		 * The size of the address space (in bits) for each buddy hash table on
		 * a directory page. The global depth of a node is defined recursively
		 * as the local depth of that node within its parent. The global/local
		 * depth are not stored explicitly. Instead, the local depth is computed
		 * dynamically when the child will be materialized by counting the #of
		 * pointers to the the child in the appropriate buddy hash table in the
		 * parent. This local depth value is passed into the constructor when
		 * the child is materialized to set the global depth of the child.
		 */
		protected int globalDepth;

		/**
		 * The size of the address space (in bits) for each buddy hash table on
		 * a directory page. The legal range is <code>[0:addressBits-1]</code>.
		 * <p>
		 * When the global depth is increased, the hash table requires twice as
		 * many slots on the page. This forces the split of the directory page
		 * onto two pages in order to accommodate the additional space
		 * requirements. The maximum global depth is <code>addressBits</code>,
		 * at which point the hash table fills the entire directory page. The
		 * minimum global depth is ZERO (0), at which point the buddy hash table
		 * has a single slot.
		 * <p>
		 * The global depth of a child page is just the local depth of the
		 * directory page in its parent. The global depth of the child page
		 * is often called its <em>local depth</em>.
		 * 
		 * TODO Since the root directory does not have a parent, its global
		 * depth is recorded in the checkpoint record [actually, the global
		 * depth of the root directory might always be <i>addressBits</i> in
		 * which case it does not need to be stored anywhere].
		 */
    	public int getGlobalDepth() {
    		return globalDepth;
    	}

		/**
		 * The #of buddy tables (buckets) on a directory (bucket) page. This
		 * depends solely on <i>addressBits</i> (a constant) and
		 * <i>globalDepth</i> and is given by
		 * <code>(2^addressBits) / (2^globalBits)</code>.
		 */
		public int getNumBuddies() {
			final int nbuddies = (1 << htree.addressBits) / (1 << globalDepth);
			return nbuddies;
		}

		/**
		 * The #of directory entries in a buddy hash table for this directory
		 * page. This depends solely on the <i>globalDepth</i> of this directory
		 * page and is given by <code>2^globalDepth</code>.
		 */
		public int getSlotsPerBuddy() {
			final int slotsPerBuddy = (1 << globalDepth);
			return slotsPerBuddy;
		}

		/**
		 * Return the prefix length of the page (the #of bits of the key which
		 * have been consumed by the parent directory pages before reaching this
		 * page).
		 */
		final public int getPrefixLength() {

			int ret = 0;
			
			DirectoryPage dp = parent != null ? parent.get() : null;
			
			while (dp != null) {
			
				ret += dp.globalDepth;
				
				dp = dp.parent != null ? dp.parent.get() : null;
			
			}
			
			return ret;
		
		}

		/**
		 * Computed by recursing to the root and counting the levels. The root
		 * is at depth ZERO (0).
		 * 
		 * @return The level in the {@link HTree}.
		 */
		final public int getLevel() {

			int ret = 0;
			
			DirectoryPage dp = parent != null ? parent.get() : null;
			
			while (dp != null) {
			
				ret++;
				
				dp = dp.parent != null ? dp.parent.get() : null;

			}
			
			return ret;

		}

		/**
		 * Return the bits from the key which are relevant to the current
		 * directory page (varient for unsigned byte[] keys). This depends on
		 * the <i>prefixLength</i> to be ignored, the <i>globalDepth</i> of this
		 * directory page, and the key.
		 * 
		 * @param key
		 *            The key.
		 * @param prefixLength
		 *            The #of MSB bits in the key which are to be ignored at
		 *            this level of the hash tree. This is computed dynamically
		 *            during recursive traversal of the hash tree. This is ZERO
		 *            (0) for the root directory. It is incremented by
		 *            <i>globalDepth</i> (the #of address bits used by a given
		 *            node) at each level of recursion for insert, lookup, etc.
		 * 
		 * @return The int32 value containing the relevant bits from the key.
		 */
		public int getLocalHashCode(final byte[] key, final int prefixLength) {
			
			return BytesUtil.getBits(key, prefixLength, globalDepth);
			
		}

		/**
		 * Return the bits from the key which are relevant to the current
		 * directory page (variant for int32 keys). This depends on the
		 * <i>prefixLength</i> to be ignored, the <i>globalDepth</i> of this
		 * directory page, and the key.
		 * 
		 * @param key
		 *            The key.
		 * @param prefixLength
		 *            The #of MSB bits in the key which are to be ignored at
		 *            this level of the hash tree. This is computed dynamically
		 *            during recursive traversal of the hash tree. This is ZERO
		 *            (0) for the root directory. It is incremented by
		 *            <i>globalDepth</i> (the #of address bits used by a given
		 *            node) at each level of recursion for insert, lookup, etc.
		 * 
		 * @return The int32 value containing the relevant bits from the key.
		 */
		public int getLocalHashCode(final int key, final int prefixLength) {
			
			return BytesUtil.getBits(key, prefixLength, globalDepth);
			
		}
		

		public DirectoryPage getParentDirectory() {
			return parent != null ? parent.get() : null;
		}

        /**
         * Disallowed.
         */
        private AbstractPage() {

            throw new UnsupportedOperationException();
            
        }

		/**
		 * All constructors delegate to this constructor to set the htree
		 * reference and core metadata.
		 * 
		 * @param htree
		 *            The {@link HTree} to which the page belongs.
		 * @param dirty
		 *            Used to set the {@link PO#dirty} state. All nodes and
		 *            leaves created by non-deserialization constructors begin
		 *            their life cycle as <code>dirty := true</code> All nodes
		 *            or leaves de-serialized from the backing store begin their
		 *            life cycle as clean (dirty := false). This we read nodes
		 *            and leaves into immutable objects, those objects will
		 *            remain clean. Eventually a copy-on-write will create a
		 *            mutable node or leaf from the immutable one and that node
		 *            or leaf will be dirty.
		 * @param globalDepth
		 *            The size of the address space (in bits) for each buddy
		 *            hash table (bucket) on a directory (bucket) page. The
		 *            global depth of a node is defined recursively as the local
		 *            depth of that node within its parent. The global/local
		 *            depth are not stored explicitly. Instead, the local depth
		 *            is computed dynamically when the child will be
		 *            materialized by counting the #of pointers to the the child
		 *            in the appropriate buddy hash table in the parent. This
		 *            local depth value is passed into the constructor when the
		 *            child is materialized and set as the global depth of the
		 *            child.
		 */
		protected AbstractPage(final HTree htree, final boolean dirty,
				final int globalDepth) {

			if (htree == null)
				throw new IllegalArgumentException();

			if (globalDepth < 0)
				throw new IllegalArgumentException();
			
			if (globalDepth > htree.addressBits)
				throw new IllegalArgumentException();

            this.htree = htree;

            this.globalDepth = globalDepth;
            
            // reference to self: reused to link parents and children.
            this.self = htree.newRef(this);
            
            if (!dirty) {

                /*
                 * Nodes default to being dirty, so we explicitly mark this as
                 * clean. This is ONLY done for the de-serialization constructors.
                 */

                setDirty(false);

            }
            
            // Add to the hard reference queue.
            htree.touch(this);
            
        }

		public void delete() throws IllegalStateException {
			
	        if( deleted ) {
	            
	            throw new IllegalStateException();
	            
	        }

	        /*
	         * Release the state associated with a node or a leaf when it is marked
	         * as deleted, which occurs only as a side effect of copy-on-write. This
	         * is important since the node/leaf remains on the hard reference queue
	         * until it is evicted but it is unreachable and its state may be
	         * reclaimed immediately.
	         */
	        
	        parent = null; // Note: probably already null.
	        
	        // release the key buffer.
	        /*nkeys = 0; */
//	        keys = null;

	        // Note: do NOT clear the referenceCount.
	        
	        if( identity != NULL ) {
	            
	            /*
	             * Deallocate the object on the store.
	             * 
	             * Note: This operation is not meaningful on an append only store.
	             * If a read-write store is defined then this is where you would
	             * delete the old version.
	             * 
	             * Note: Do NOT clear the [identity] field in delete().  copyOnWrite()
	             * depends on the field remaining defined on the cloned node so that
	             * it may be passed on.
	             */

//	            btree.store.delete(identity);
	            
	        }
	        
	        deleted = true;
			
		}

		/**
		 * Dump the data onto the {@link PrintStream}.
		 * 
		 * @param level
		 *            The logging level.
		 * @param out
		 *            Where to write the dump.
		 * @param height
		 *            The height of this node in the tree or -1 iff you need to
		 *            invoke this method on a node or leaf whose height in the
		 *            tree is not known.
		 * @param recursive
		 *            When true, the node will be dumped recursively using a
		 *            pre-order traversal.
		 * @param materialize
		 *            When <code>true</code>, children will be materialized as
		 *            necessary to dump the tree.
		 *            
		 * @return <code>true</code> unless an inconsistency was detected.
		 */
		abstract protected boolean dump(Level level, PrintStream out,
				int height, boolean recursive, boolean materialize);

		/** Pretty print the tree from this level on down. */
		abstract void PP(StringBuilder sb);
		
		/** Return a very short id used by {@link #PP()}. */
		protected String PPID() {

			final int hash = hashCode() % 100;

			// Note: fixes up the string if hash is only one digit.
			final String hashStr = "#" + (hash < 10 ? "0" : "") + hash;

			return (isLeaf() ? "B" : "D") + hashStr;
		
		}
		
		abstract void insertRawTuple(final byte[] key, final byte[] val, final int buddy);

	} // class AbstractPage

	/**
	 * An {@link HTree} bucket page (leaf). The bucket page is comprised of one
	 * or more buddy hash buckets. The #of buddy hash buckets is determined by
	 * the address bits of the hash tree and the global depth of the bucket
	 * page.
	 * <p>
	 * The entire bucket page is logically a fixed size array of tuples. The
	 * individual buddy hash buckets are simply offsets into that logical tuple
	 * array. While inserts of distinct keys drive splits, inserts of identical
	 * keys do not. Therefore, this simple logical picture is upset by the need
	 * for a bucket to hold an arbitrary number of tuples having the same key.
	 * <p>
	 * Each tuple is represented by a key, a value, and various metadata bits
	 * using the {@link ILeafData} API. The tuple keys are always inline within
	 * the page and are often 32-bit integers. The tuple values may be either
	 * "raw records" on the backing {@link IRawStore} or inline within the page.
	 * 
	 * TODO One way to tradeoff the simplicity of a local tuple array with the
	 * requirement to hold an arbitrary number of duplicate keys within a bucket
	 * is to split the bucket if it becomes full regardless of whether or not
	 * there are duplicate keys.
	 * <p>
	 * Splitting a bucket doubles its size which causes a new bucket page to be
	 * allocated to store 1/2 of the data. If the keys can be differentiated by
	 * increasing the local depth, then this is the normal case and the tuples
	 * are just redistributed among buddy buckets on the original bucket page
	 * and the new bucket page. If the keys are identical but we force a split
	 * anyway, then we will still have fewer buckets on the original page and
	 * they will be twice as large. The duplicate keys will all wind up in the
	 * same buddy bucket after the split, but at least the buddy bucket is 2x
	 * larger. This process can continue until there is only a single buddy
	 * bucket on the page (the global depth of the parent is the same as the
	 * global depth of the buddy bucket). At that point, a "full" page can
	 * simply "grow" by permitting more and more tuples into the page (as long
	 * as those tuples have the same key). We could also chain overflow pages at
	 * that point - it all amounts to the same thing. An attempt to insert a
	 * tuple having a different key into a page in which all keys are known to
	 * be identical will immediately trigger a split. In this case we could
	 * avoid some effort if we modified the directory structure to impose a
	 * split since we already know that we have only two distinct keys (the one
	 * found in all tuples of the bucket and the new key). When a bucket page
	 * reaches this condition of containing only duplicate keys we could of
	 * course compress the keys enormously since they are all the same.
	 * <p>
	 * This works well when we only store the address of the objects in the
	 * bucket (rather than the objects themselves, e.g., raw records mode) and
	 * choose the address bits based on the expected size of a tuple record.
	 * However, we can also accommodate tuples with varying size (think binding
	 * sets) with in page locality if split the buddy bucket with the most bytes
	 * when an insert into the page would exceed the target page size. This
	 * looks pretty much like the case above, except that we split buddy buckets
	 * based on not only whether their alloted #of slots are filled by tuples
	 * but also based on the data on the page.
	 * 
	 * TODO Delete markers will also require some thought. Unless we can purge
	 * them out at the tx commit point, we can wind up with a full bucket page
	 * consisting of a single buddy bucket filled with deleted tuples all having
	 * the same key.
	 */
    static class BucketPage extends AbstractPage implements ILeafData { // TODO IBucketData

        /**
         * The data record. {@link MutableBucketData} is used for all mutation
         * operations. {@link ReadOnlyLeafData} is used when the {@link Leaf} is
         * made persistent. A read-only data record is automatically converted into
         * a {@link MutableBucketData} record when a mutation operation is requested.
         * <p>
         * Note: This is package private in order to expose it to {@link Node}.
         */
        ILeafData data;
        
        public AbstractFixedByteArrayBuffer data() {
            return data.data();
        }

        public boolean getDeleteMarker(int index) {
            return data.getDeleteMarker(index);
        }

        public int getKeyCount() {
            return data.getKeyCount();
        }

        public IRaba getKeys() {
            return data.getKeys();
        }

        public long getMaximumVersionTimestamp() {
            return data.getMaximumVersionTimestamp();
        }

        public long getMinimumVersionTimestamp() {
            return data.getMinimumVersionTimestamp();
        }

        public long getNextAddr() {
            return data.getNextAddr();
        }

        public long getPriorAddr() {
            return data.getPriorAddr();
        }

        public long getRawRecord(int index) {
            return data.getRawRecord(index);
        }

//        public int getSpannedTupleCount() {
//            return data.getSpannedTupleCount();
//        }

        public int getValueCount() {
            return data.getValueCount();
        }

        public IRaba getValues() {
            return data.getValues();
        }

        public long getVersionTimestamp(int index) {
            return data.getVersionTimestamp(index);
        }

        public boolean hasDeleteMarkers() {
            return data.hasDeleteMarkers();
        }

        public boolean hasRawRecords() {
            return data.hasRawRecords();
        }

        public boolean hasVersionTimestamps() {
            return data.hasVersionTimestamps();
        }

        public boolean isCoded() {
            return data.isCoded();
        }

        public boolean isDoubleLinked() {
            return data.isDoubleLinked();
        }

        public boolean isLeaf() {
            return data.isLeaf();
        }

        public boolean isReadOnly() {
            return data.isReadOnly();
        }

        /**
         * Create a new empty bucket.
         * 
         * @param htree
         *            A reference to the owning {@link HTree}.
		 * @param globalDepth
		 *            The size of the address space (in bits) for each buddy
		 *            hash table on a directory page. The global depth of a node
		 *            is defined recursively as the local depth of that node
		 *            within its parent. The global/local depth are not stored
		 *            explicitly. Instead, the local depth is computed
		 *            dynamically when the child will be materialized by
		 *            counting the #of pointers to the the child in the
		 *            appropriate buddy hash table in the parent. This local
		 *            depth value is passed into the constructor when the child
		 *            is materialized and set as the global depth of the child.
         */
        BucketPage(final HTree htree, final int globalDepth) {

			super(htree, true/* dirty */, globalDepth);
            
            data = new MutableBucketData(//
                    (1 << htree.addressBits), // fan-out
                    htree.versionTimestamps,//
                    htree.deleteMarkers,//
                    htree.rawRecords//
                    );

        }

		/**
		 * Return <code>true</code> if there is at lease one tuple in the buddy
		 * hash bucket for the specified key.
		 * 
		 * @param key
		 *            The key.
		 * @param buddyOffset
		 *            The offset within the {@link BucketPage} of the buddy hash
		 *            bucket to be searched.
		 *            
		 * @return <code>true</code> if a tuple is found in the buddy hash
		 *         bucket for the specified key.
		 */
		boolean contains(final byte[] key, final int buddyOffset) {

			if (key == null)
				throw new IllegalArgumentException();

			// #of slots on the page.
			final int slotsOnPage = (1 << htree.addressBits);

			// #of address slots in each buddy hash table.
			final int slotsPerBuddy = (1 << globalDepth);

//			// #of buddy tables on a page.
//			final int nbuddies = (slotsOnPage) / slotsPerBuddy;

			final int lastSlot = buddyOffset + slotsPerBuddy;

			// range check buddyOffset.
			if (buddyOffset < 0 || buddyOffset >= slotsOnPage)
				throw new IndexOutOfBoundsException();

			/*
			 * Locate the first unassigned tuple in the buddy bucket.
			 * 
			 * TODO Faster comparison with a coded key in the raba by either (a)
			 * asking the raba to do the equals() test; or (b) copying the key
			 * from the raba into a buffer which we reuse for each test. This is
			 * another way in which the hash table keys raba differs from the
			 * btree keys raba.
			 */
			final IRaba keys = getKeys();
			for (int i = buddyOffset; i < lastSlot; i++) {
				if (!keys.isNull(i)) {
					if(BytesUtil.bytesEqual(key,keys.get(i))) {
						return true;
					}
				}
			}
			return false;
		}

		/**
		 * Return the first value found in the buddy hash bucket for the
		 * specified key.
		 * 
		 * @param key
		 *            The key.
		 * @param buddyOffset
		 *            The offset within the {@link BucketPage} of the buddy hash
		 *            bucket to be searched.
		 * 
		 * @return The value associated with the first tuple found in the buddy
		 *         hash bucket for the specified key and <code>null</code> if no
		 *         such tuple was found. Note that the return value is not
		 *         diagnostic if the application allows <code>null</code> values
		 *         into the index.
		 */
		final byte[] lookupFirst(final byte[] key, final int buddyOffset) {

			if (key == null)
				throw new IllegalArgumentException();

			// #of slots on the page.
			final int slotsOnPage = (1 << htree.addressBits);

			// #of address slots in each buddy hash table.
			final int slotsPerBuddy = (1 << globalDepth);

//			// #of buddy tables on a page.
//			final int nbuddies = (slotsOnPage) / slotsPerBuddy;

			final int lastSlot = buddyOffset + slotsPerBuddy;

			// range check buddyOffset.
			if (buddyOffset < 0 || buddyOffset >= slotsOnPage)
				throw new IndexOutOfBoundsException();

			/*
			 * Locate the first unassigned tuple in the buddy bucket.
			 * 
			 * TODO Faster comparison with a coded key in the raba by either (a)
			 * asking the raba to do the equals() test; or (b) copying the key
			 * from the raba into a buffer which we reuse for each test. This is
			 * another way in which the hash table keys raba differs from the
			 * btree keys raba.
			 */
			final IRaba keys = getKeys();
			for (int i = buddyOffset; i < lastSlot; i++) {
				if (!keys.isNull(i)) {
					if(BytesUtil.bytesEqual(key,keys.get(i))) {
						return getValues().get(i);
					}
				}
			}
			return null;
		}

		/**
		 * Return an iterator which will visit each tuple in the buddy hash
		 * bucket for the specified key.
		 * 
		 * @param key
		 *            The key.
		 * @param buddyOffset
		 *            The offset within the {@link BucketPage} of the buddy hash
		 *            bucket to be searched.
		 * 
		 * @return An iterator which will visit each tuple in the buddy hash
		 *         table for the specified key and never <code>null</code>.
		 * 
		 *         TODO Specify the contract for concurrent modification both
		 *         here and on the {@link HTree#lookupAll(byte[])} methods.
		 */
		final ITupleIterator lookupAll(final byte[] key, final int buddyOffset) {

			return new BuddyBucketTupleIterator(key, this, buddyOffset);

		}

		/**
		 * Insert the tuple into the buddy bucket.
		 * 
		 * @param key
		 *            The key (all bits, all bytes).
		 * @param val
		 *            The value (optional).
		 * @param parent
		 *            The parent {@link DirectoryPage} and never
		 *            <code>null</code> (this is required for the copy-on-write
		 *            pattern).
		 * @param buddyOffset
		 *            The offset into the child of the first slot for the buddy
		 *            hash table or buddy hash bucket.
		 * 
		 * @return <code>false</code> iff the buddy bucket must be split.
		 * 
		 * @throws IllegalArgumentException
		 *             if <i>key</i> is <code>null</code>.
		 * @throws IllegalArgumentException
		 *             if <i>parent</i> is <code>null</code>.
		 * @throws IndexOutOfBoundsException
		 *             if <i>buddyOffset</i> is out of the allowed range.
		 */
		boolean insert(final byte[] key, final byte[] val,
				final DirectoryPage parent, 
				final int buddyOffset) {

			if (key == null)
				throw new IllegalArgumentException();

			if (parent == null)
				throw new IllegalArgumentException();

			// #of slots on the page.
			final int slotsOnPage = (1 << htree.addressBits);

			// #of address slots in each buddy hash table.
			final int slotsPerBuddy = (1 << globalDepth);

			// #of buddy tables on a page.
			final int nbuddies = (slotsOnPage) / slotsPerBuddy;

			final int lastSlot = buddyOffset + slotsPerBuddy;

			// range check buddyOffset.
			if (buddyOffset < 0 || buddyOffset >= slotsOnPage)
				throw new IndexOutOfBoundsException();

			// TODO if(!mutable) copyOnWrite().insert(key,val,parent,buddyOffset);

            /*
             * Locate the first unassigned tuple in the buddy bucket.
             * 
             * Note: Given the IRaba data structure, this will require us to
             * examine the keys for a null. The "keys" rabas do not allow nulls,
             * so we will need to use a "values" raba (nulls allowed) for the
             * bucket keys. Unless we keep the entries in a buddy bucket dense
             * (maybe making them dense when they are persisted for faster
             * scans, but why bother for mutable buckets?) we will have to scan
             * the entire buddy bucket to find an open slot (or just to count
             * the #of slots which are currently in use).
             * 
             * TODO Cache the location of the last known empty slot. If it is in
             * the same buddy bucket then we can use it immediately. Otherwise
             * we can scan for the first empty slot in the given buddy bucket.
             */
            final MutableKeyBuffer keys = (MutableKeyBuffer) getKeys();
            final MutableValueBuffer vals = (MutableValueBuffer) getValues();
			
			for (int i = buddyOffset; i < lastSlot; i++) {
				if (keys.isNull(i)) {
					keys.nkeys++;
					keys.keys[i] = key;
					vals.nvalues++;
					vals.values[i] = val;
					// TODO deleteMarker:=false
					// TODO versionTimestamp:=...
					((HTree)htree).nentries++;
					// insert Ok.
					return true;
				}
			}

			/*
			 * Any buddy bucket which is full is split unless it is the sole
			 * buddy in the page since a split doubles the size of the buddy
			 * bucket (unless it is the only buddy on the page) and the tuple
			 * can therefore be inserted after a split. [This rule is not
			 * perfect if we allow splits to be driven by the bytes on a page,
			 * but it should still be Ok.]
			 * 
			 * Before we can split the sole buddy bucket in a page, we need to
			 * know whether or not the keys are identical. If they are then we
			 * let the page grow rather than splitting it. This can be handled
			 * insert of bucketPage.insert(). It can have a boolean which is set
			 * false as soon as it sees a key which is not the equals() to the
			 * probe key (in all bits).
			 * 
			 * Note that an allowed split always leaves enough room for another
			 * tuple (when considering only the #of tuples and not their bytes
			 * on the page). We can still be "out of space" in terms of bytes on
			 * the page, even for a single tuple. In this edge case, the tuple
			 * should really be a raw record. That is easily controlled by
			 * having a maximum inline value byte[] length for a page - probably
			 * on the order of pageSize/16 which works out to 256 bytes for a 4k
			 * page.
			 */
			if (nbuddies != 1) {
				/*
				 * Force a split since there is more than one buddy on the page.
				 */
				return false;
			}

			/*
			 * There is only one buddy on the page. Now we have to figure out
			 * whether or not all keys are duplicates.
			 */
			boolean identicalKeys = true;
			for (int i = buddyOffset; i < buddyOffset + slotsPerBuddy; i++) {
				if(!BytesUtil.bytesEqual(key,keys.get(i))) {
					identicalKeys = false;
					break;
				}
			}
			if(!identicalKeys) {
				/*
				 * Force a split since it is possible to redistribute some
				 * tuples.
				 */
				return false;
			}

            /*
             * Since the page is full, we need to grow the page (or chain an
             * overflow page) rather than splitting the page.
             * 
             * TODO Maybe the easiest thing to do is just double the target #of
             * slots on the page. We would rely on keys.capacity() in this case
             * rather than #slots. In fact, we could just reenter the method
             * above after doubling as long as we rely on keys.capacity() in the
             * case where nbuddies==1. [Unit test for this case.]
             */

			throw new UnsupportedOperationException(
                    "Must overflow since all keys on full buddy bucket are duplicates.");
            
        }

		/**
		 * Insert used when addLevel() is invoked to copy a tuple from an
		 * existing bucket page into another bucket page. This method is very
		 * similar to {@link #insert(byte[], byte[], DirectoryPage, int)}. The
		 * critical differences are: (a) it correctly handles raw records (they
		 * are not materialized during the copy); (b) it correctly handles
		 * version counters and delete markers; and (c) the #of tuples in the
		 * index is unchanged.
		 * 
		 * @param srcPage
		 *            The source {@link BucketPage}.
		 * @param srcSlot
		 *            The slot in that {@link BucketPage} having the tuple to be
		 *            copied.
		 * @param key
		 *            The key (already materialized).
		 * @param parent
		 *            The parent {@link DirectoryPage} and never
		 *            <code>null</code> (this is required for the copy-on-write
		 *            pattern).
		 * @param buddyOffset
		 *            The offset into the child of the first slot for the buddy
		 *            hash table or buddy hash bucket.
		 * 
		 * @return <code>false</code> iff the buddy bucket must be split.
		 * 
		 * @throws IllegalArgumentException
		 *             if <i>key</i> is <code>null</code>.
		 * @throws IllegalArgumentException
		 *             if <i>parent</i> is <code>null</code>.
		 * @throws IndexOutOfBoundsException
		 *             if <i>buddyOffset</i> is out of the allowed range.
		 */
		boolean insertRawTuple(final BucketPage srcPage, final int srcSlot,
				final byte[] key, final DirectoryPage parent,
				final int buddyOffset) {

			if (key == null)
				throw new IllegalArgumentException();

			if (parent == null)
				throw new IllegalArgumentException();

			// #of slots on the page.
			final int slotsOnPage = (1 << htree.addressBits);

			// #of address slots in each buddy hash table.
			final int slotsPerBuddy = (1 << globalDepth);

			// #of buddy tables on a page.
			final int nbuddies = (slotsOnPage) / slotsPerBuddy;

			final int lastSlot = buddyOffset + slotsPerBuddy;

			// range check buddyOffset.
			if (buddyOffset < 0 || buddyOffset >= slotsOnPage)
				throw new IndexOutOfBoundsException();

			// TODO if(!mutable) copyOnWrite().insert(key,val,parent,buddyOffset);

            /*
             * Locate the first unassigned tuple in the buddy bucket.
             * 
             * Note: Given the IRaba data structure, this will require us to
             * examine the keys for a null. The "keys" rabas do not allow nulls,
             * so we will need to use a "values" raba (nulls allowed) for the
             * bucket keys. Unless we keep the entries in a buddy bucket dense
             * (maybe making them dense when they are persisted for faster
             * scans, but why bother for mutable buckets?) we will have to scan
             * the entire buddy bucket to find an open slot (or just to count
             * the #of slots which are currently in use).
             * 
             * TODO Cache the location of the last known empty slot. If it is in
             * the same buddy bucket then we can use it immediately. Otherwise
             * we can scan for the first empty slot in the given buddy bucket.
             */
            final MutableKeyBuffer keys = (MutableKeyBuffer) getKeys();
            final MutableValueBuffer vals = (MutableValueBuffer) getValues();
			
			for (int i = buddyOffset; i < lastSlot; i++) {
				if (keys.isNull(i)) {
					keys.nkeys++;
					keys.keys[i] = key;
					vals.nvalues++;
					vals.values[i] = srcPage.getKeys().get(srcSlot); // Note: DOES NOT Materialize a raw record!!!!
					// TODO deleteMarker:=false
					// TODO versionTimestamp:=...
//					((HTree)htree).nentries++; // DO NOT increment nentries!!!!
					// insert Ok.
					return true;
				}
			}

			/*
			 * Any buddy bucket which is full is split unless it is the sole
			 * buddy in the page since a split doubles the size of the buddy
			 * bucket (unless it is the only buddy on the page) and the tuple
			 * can therefore be inserted after a split. [This rule is not
			 * perfect if we allow splits to be driven by the bytes on a page,
			 * but it should still be Ok.]
			 * 
			 * Before we can split the sole buddy bucket in a page, we need to
			 * know whether or not the keys are identical. If they are then we
			 * let the page grow rather than splitting it. This can be handled
			 * insert of bucketPage.insert(). It can have a boolean which is set
			 * false as soon as it sees a key which is not the equals() to the
			 * probe key (in all bits).
			 * 
			 * Note that an allowed split always leaves enough room for another
			 * tuple (when considering only the #of tuples and not their bytes
			 * on the page). We can still be "out of space" in terms of bytes on
			 * the page, even for a single tuple. In this edge case, the tuple
			 * should really be a raw record. That is easily controlled by
			 * having a maximum inline value byte[] length for a page - probably
			 * on the order of pageSize/16 which works out to 256 bytes for a 4k
			 * page.
			 */
			if (nbuddies != 1) {
				/*
				 * Force a split since there is more than one buddy on the page.
				 */
				return false;
			}

			/*
			 * There is only one buddy on the page. Now we have to figure out
			 * whether or not all keys are duplicates.
			 */
			boolean identicalKeys = true;
			for (int i = buddyOffset; i < buddyOffset + slotsPerBuddy; i++) {
				if(!BytesUtil.bytesEqual(key,keys.get(i))) {
					identicalKeys = false;
					break;
				}
			}
			if(!identicalKeys) {
				/*
				 * Force a split since it is possible to redistribute some
				 * tuples.
				 */
				return false;
			}

            /*
             * Since the page is full, we need to grow the page (or chain an
             * overflow page) rather than splitting the page.
             * 
             * TODO Maybe the easiest thing to do is just double the target #of
             * slots on the page. We would rely on keys.capacity() in this case
             * rather than #slots. In fact, we could just reenter the method
             * above after doubling as long as we rely on keys.capacity() in the
             * case where nbuddies==1. [Unit test for this case.]
             */

			throw new UnsupportedOperationException(
                    "Must overflow since all keys on full buddy bucket are duplicates.");
            
        }

		/**
		 * Return an iterator visiting all the non-deleted, non-empty tuples
		 * on this {@link BucketPage}.
		 */
		ITupleIterator tuples() {

			return new InnerBucketPageTupleIterator(IRangeQuery.DEFAULT);
		
		}

		/**
		 * Visits the non-empty tuples in each {@link BucketPage} visited by the
		 * source iterator.
		 */
		private class InnerBucketPageTupleIterator<E> implements
				ITupleIterator<E> {

			private final int slotsPerPage = 1 << htree.addressBits;

			private int nextNonEmptySlot = 0;

			private final Tuple<E> tuple;

			InnerBucketPageTupleIterator(final int flags) {

				// look for the first slot.
				if (findNextSlot()) {

					this.tuple = new Tuple<E>(htree, flags);

				} else {

					// Nothing will be visited.
					this.tuple = null;
					
				}
				
			}
			
			/**
			 * Scan to the next non-empty slot in the current {@link BucketPage}.
			 * 
			 * @return <code>true</code> iff there is a non-empty slot on the
			 *         current {@link BucketPage}.
			 */
			private boolean findNextSlot() {
				final IRaba keys = getKeys();
				for (; nextNonEmptySlot < slotsPerPage; nextNonEmptySlot++) {
					if (keys.isNull(nextNonEmptySlot))
						continue;
					return true;
				}
				// The current page is exhausted.
				return false;
			}

			public boolean hasNext() {

				return nextNonEmptySlot < slotsPerPage;

			}

			public ITuple<E> next() {
				if (!hasNext())
					throw new NoSuchElementException();
				// Copy the data for the current tuple into the Tuple buffer.
				tuple.copy(nextNonEmptySlot, BucketPage.this);
				/*
				 * Advance to the next slot on the current page. if there is non,
				 * then the current page reference will be cleared and we will need
				 * to fetch a new page in hasNext() on the next invocation.
				 */
				nextNonEmptySlot++; // skip past the current tuple.
				findNextSlot(); // find the next non-null slot (next tuple).
				// Return the Tuple buffer.
				return tuple;
			}
			
			public void remove() {
				throw new UnsupportedOperationException();
			}

		}
		
		@Override
		public void PP(final StringBuilder sb) {

			sb.append(PPID() + " [" + globalDepth + "] " + indent(getLevel()));
			

			sb.append("("); // start of address map.

			// #of buddy tables on a page.
			final int nbuddies = (1 << htree.addressBits) / (1 << globalDepth);

			// #of address slots in each buddy hash table.
			final int slotsPerBuddy = (1 << globalDepth);

			for (int i = 0; i < nbuddies; i++) {

				if (i > 0) // buddy boundary marker
					sb.append(";");

				for (int j = 0; j < slotsPerBuddy; j++) {

					if (j > 0) // slot boundary marker.
						sb.append(",");

					final int slot = i * slotsPerBuddy + j;

					sb.append(PPVAL(slot));
					
				}

			}

			sb.append(")"); // end of tuples			
		
			sb.append("\n");

		}

		/**
		 * Pretty print a value from the tuple at the specified slot on the
		 * page.
		 * 
		 * @param index
		 *            The slot on the page.
		 * 
		 * @return The pretty print representation of the value associated with
		 *         the tuple at that slot.
		 * 
		 *         TODO Either indirect for raw records or write out the addr of
		 *         the raw record.
		 */
		private String PPVAL(final int index) {

			if (getKeys().isNull(index))
				return "-";
			
			final byte[] key = getKeys().get(index);
			
			final String keyStr = BytesUtil.toString(key) + "(" + BytesUtil.toBitString(key)
					+ ")";
			
			final String valStr;

			if (false/*showValues*/) {
			
				final byte[] value = getValues().get(index);
				
				valStr = BytesUtil.toString(value);
			
			} else {
				
				valStr = null;
				
			}

			if(valStr == null) {

				return keyStr;
			
			}

			return keyStr + "=>" + valStr;

		}

		/**
	     * Human readable representation of the {@link ILeafData} plus transient
	     * information associated with the {@link BucketPage}.
	     */
	    @Override
	    public String toString() {

	        final StringBuilder sb = new StringBuilder();

	        sb.append(super.toString());

	        sb.append("{ isDirty="+isDirty());

	        sb.append(", isDeleted="+isDeleted());
	        
	        sb.append(", addr=" + identity);

	        final DirectoryPage p = (parent == null ? null : parent.get());

	        sb.append(", parent=" + (p == null ? "N/A" : p.toShortString()));
            sb.append(", globalDepth=" + getGlobalDepth());
            sb.append(", nbuddies=" + (1 << htree.addressBits) / (1 << globalDepth));
            sb.append(", slotsPerBuddy="+(1 << globalDepth));
            if (data == null) {

	            // No data record? (Generally, this means it was stolen by copy on
	            // write).
	            sb.append(", data=NA}");

	            return sb.toString();
	            
	        }

	        sb.append(", nkeys=" + getKeyCount());
	        
//	        sb.append(", minKeys=" + minKeys());
//
//	        sb.append(", maxKeys=" + maxKeys());
	        
	        DefaultLeafCoder.toString(this, sb);

	        sb.append("}");

	        return sb.toString();
	        
	    }

	    protected boolean dump(final Level level, final PrintStream out,
				final int height, final boolean recursive,
				final boolean materialize) {

	        final boolean debug = level.toInt() <= Level.DEBUG.toInt();
	        
	        // Set to false iff an inconsistency is detected.
	        boolean ok = true;

			if (parent == null || parent.get() == null) {
	            out.println(indent(height) + "ERROR: parent not set");
				ok = false;
	        }
			
			if (globalDepth > parent.get().globalDepth) {
	            out.println(indent(height) + "ERROR: localDepth exceeds globalDepth of parent");
				ok = false;
			}

			/*
			 * FIXME Count the #of pointers in each buddy hash table of the
			 * parent to each buddy bucket in this bucket page and verify that
			 * the globalDepth on the child is consistent with the pointers in
			 * the parent.
			 * 
			 * FIXME The same check must be performed for the directory page to
			 * cross validate the parent child linking pattern with the
			 * transient cached globalDepth fields.
			 */
			
	        if (debug || ! ok ) {
	            
	            out.println(indent(height) + toString());

	        }

	        return ok;
	        
		}

        /**
         * From the current bit resolution, determines how many extra bits are
         * required to ensure the current set of bucket values can be split.
         * <p>
         * The additional complexity of determining whether the page can really
         * be split is left to the parent. A new directory, covering the
         * required prefixBits would inititally be created with depth 1. But if
         * the specified bit is discriminated within buddy buckets AND other
         * bits do not further separate the buckets then the depth of the
         * directory will need to be increased before the bucket page can be
         * split.
         * 
         * @return bit depth increase from current offset required -or-
         *         <code>-1</code> if it is not possible to split the page no
         *         matter how many bits we have.
         */
	    int distinctBitsRequired() {
	    	final int currentResolution = getPrefixLength(); // start offset of this page
	    	int testPrefix = currentResolution+1;
	    	
	    	final IRaba keys = data.getKeys();
	    	final int nkeys = keys.size();
	    	
	    	int maxPrefix = 0;
	    	for (int t = 1; t < nkeys; t++) {
	    		final byte[] k = keys.get(t);
	    		final int klen = k == null ? 0 : k.length;
	    		maxPrefix = maxPrefix > klen ? maxPrefix : klen;
	    	}
	    	maxPrefix *= 8; // convert max bytes to max bits
	    	
	    	assert nkeys > 1;
	    	
	    	while (testPrefix < maxPrefix) {
	    	    final boolean bitset = BytesUtil.getBit(keys.get(0), testPrefix);
		    	for (int t = 1; t < nkeys; t++) {
		    		final byte[] k = keys.get(t);
		    		if (bitset != (k == null ? false : BytesUtil.getBit(keys.get(t), testPrefix))) {
		    			return testPrefix - currentResolution;
		    		}
		    	}
		    	testPrefix++;
	    	}
	    	
	    	return -1;
	    }

	    /**
	     * To insert in a BucketPage must handle split
	     * 
	     * @see com.bigdata.htree.HTree.AbstractPage#insertRawTuple(byte[], byte[], int)
	     */
		void insertRawTuple(final byte[] key, final byte[] val, final int buddy) {
			final int slotsPerBuddy = (1 << htree.addressBits);
            final MutableKeyBuffer keys = (MutableKeyBuffer) getKeys();
            final MutableValueBuffer vals = (MutableValueBuffer) getValues();
			
			if (true) {
				// just fit somewhere in page
				for (int i = 0; i < slotsPerBuddy; i++) {
					if (keys.isNull(i)) {
						keys.nkeys++;
						keys.keys[i] = key;
						vals.nvalues++;
						vals.values[i] = val;
						// TODO deleteMarker:=false
						// TODO versionTimestamp:=...
						// do not increment on raw insert, since this is only ever (for now) a re-organisation
						// ((HTree)htree).nentries++;
						// insert Ok.
						return;
					}
				}				
			} else { // if mapping buddy explicitly
				final int buddyStart = buddy * slotsPerBuddy;
				final int lastSlot = buddyStart + slotsPerBuddy;
				
				for (int i = buddyStart; i < lastSlot; i++) {
					if (keys.isNull(i)) {
						keys.nkeys++;
						keys.keys[i] = key;
						vals.nvalues++;
						vals.values[i] = val;
						// TODO deleteMarker:=false
						// TODO versionTimestamp:=...
						((HTree)htree).nentries++;
						// insert Ok.
						return;
					}
				}
			}
			
			// unable to insert
			 if (globalDepth == htree.addressBits) {
				 // max depth so add level
				 DirectoryPage np = ((HTree) htree).addLevel2(this);
             	
				 np.insertRawTuple(key, val, 0);
			 } else {
				 // otherwise split page by asking parent to split and re-inserting values
				 
				 final DirectoryPage parent = getParentDirectory();
				 parent.split(this);
			 }
		}

    } // class BucketPage
	
	/**
	 * An {@link HTree} directory page (node). Each directory page will hold one
	 * or more "buddy" hash tables. The #of buddy hash tables on the page
	 * depends on the globalDepth of the page and the addressBits as computed by
	 * {@link DirectoryPage#getNumBuddies()}.
	 */
    static class DirectoryPage extends AbstractPage implements IDirectoryData {

    	/**
    	 * Transient references to the children.
    	 */
		final Reference<AbstractPage>[] childRefs;
		
		/**
		 * Persistent data.
		 */
        IDirectoryData data;

		/**
		 * Get the child indexed by the key.
		 * <p>
		 * Note: The recursive descent pattern requires the caller to separately
		 * compute the buddy index before each descent into a child.
		 * 
		 * @param hashBits
		 *            The relevant bits from the key.
		 * @param buddyOffset
		 *            The offset into the child of the first slot for the buddy
		 *            hash table or buddy hash bucket.
		 * 
		 * @return The child indexed by the key.
		 * 
		 * @see HTreeUtil#getBuddyOffset(int, int, int)
		 */
		protected AbstractPage getChild(final int hashBits,
				final int buddyOffset) {

			// width of a buddy hash table in pointer slots.
			final int tableWidth = (1 << globalDepth);

			// index position of the start of the buddy hash table in the page.
			final int tableOffset = (tableWidth * buddyOffset);

			// index of the slot in the buddy hash table for the given hash
			// bits.
			final int index = tableOffset + hashBits;

			return getChild(index);
			
		}

		/**
		 * Locate original references, halving number of references to new pages
		 * @param bucketPage - original bucket
		 */
		public void split(final BucketPage bucketPage) {
			int start = 0;
			for (int s = 0; s < this.getChildCount(); s++) {
				if (bucketPage == getChild(s)) {
					start = s;
					break;
				}
			}
			int last = start;			
			for (int s = start+1; s < this.getChildCount(); s++) {
				if (bucketPage == getChild(s)) {
					last++;
				} else {
					break;
				}
			}
			final int orig = last - start + 1;
			
			assert orig > 1;
			assert orig % 2 == 0;
			
			final int crefs = orig >> 1; // half references for each new child
			
			final BucketPage a = new BucketPage((HTree) htree, bucketPage.globalDepth+1);
			a.parent = (Reference<DirectoryPage>) self;
			for (int s = start; s < start + crefs; s++) {
				childRefs[s] = (Reference<AbstractPage>) a.self;
			}
			final BucketPage b = new BucketPage((HTree) htree, bucketPage.globalDepth+1);
			b.parent = (Reference<DirectoryPage>) self;
			for (int s = start+crefs; s < start + orig; s++) {
				childRefs[s] = (Reference<AbstractPage>) b.self;
			}
			
			// Now insert raw values from original page
			final ITupleIterator tuples = bucketPage.tuples();
			while (tuples.hasNext()) {
				ITuple tuple = tuples.next();
				insertRawTuple(tuple.getKey(), tuple.getValue(), 0);
			}
		}

		/**
		 * Return the child at the specified index in the {@link DirectoryPage}.
		 * 
		 * @param index
		 *            The index of the slot in the {@link DirectoryPage}. If the
		 *            child must be materialized, the buddyOffset will be
		 *            computed based on the globalDepth and the #of pointers to
		 *            that child will be computed so its depth may be set.
		 *            
		 * @return The child at that index.
		 */
		private AbstractPage getChild(final int index) {

			// width of a buddy hash table (#of pointer slots).
			final int tableWidth = 1 << globalDepth;

			// offset in [0:nbuddies-1] to the start of the buddy spanning that
			// index.
			final int tableOffset = index / tableWidth;

			/*
			 * Look at the entry in the buddy hash table. If there is a
			 * reference to the child and that reference has not been cleared,
			 * then we are done and we can return the child reference and the
			 * offset of the buddy table or bucket within the child. 
			 */
			final Reference<AbstractPage> ref = childRefs[index];
			
			AbstractPage child = ref == null ? null : ref.get();
			
			if (child != null) {

				return child;
				
			}
			
			/*
			 * We need to get the address of the child, figure out the local
			 * depth of the child (by counting the #of points in the buddy
			 * bucket to that child), and then materialize the child from its
			 * address.
			 * 
			 * TODO This all needs to go through a memoizer pattern.
			 */
			final long addr = data.getChildAddr(index);

			/*
			 * Scan to count pointers to child within the buddy hash table.
			 */
			final int npointers;
			{

				int n = 0; // npointers

				final int lastIndex = (tableOffset + tableWidth);

				for (int i = tableOffset; i < lastIndex; i++) {

					if (data.getChildAddr(i) == addr)
						n++;

				}

				assert n > 0;

				npointers = n;

			}

			/*
			 * Find the local depth of the child within this node. this becomes
			 * the global depth of the child.
			 */
			final int localDepth = HTreeUtil.getLocalDepth(htree.addressBits,
					globalDepth, npointers);

			child = htree
					.readNodeOrLeaf(addr, localDepth/* globalDepthOfChild */);

			/*
			 * Set the reference for each slot in the buddy bucket which pointed
			 * at that child. There will be [npointers] such slots.
			 */
			{

				int n = 0;

				final int lastIndex = (tableOffset + tableWidth);

				for (int i = tableOffset; i < lastIndex; i++) {

					if (data.getChildAddr(i) == addr) {

						childRefs[i] = (Reference) child.self;
						
						n++;
						
					}

				}

				assert n == npointers;

			}
			
			return child;
			
		}
    	
        public AbstractFixedByteArrayBuffer data() {
            return data.data();
        }

		/**
		 * {@inheritDoc}
		 * <p>
		 * Note: This method can only be used once you have decoded the hash
		 * bits from the key and looked up the offset of the buddy hash table on
		 * the page and the offset of the slot within the buddy hash table for
		 * the desired key. You must also know the localDepth of the child when
		 * it is materialized so that information can be set on the child, where
		 * is becomes the globalDepth of the child.
		 * 
		 * @see #getChildAddrByHashCode(int, int)
		 */
        public long getChildAddr(int index) {
            return data.getChildAddr(index);
        }

        public int getChildCount() {
            return data.getChildCount();
        }

        public long getMaximumVersionTimestamp() {
            return data.getMaximumVersionTimestamp();
        }

        public long getMinimumVersionTimestamp() {
            return data.getMinimumVersionTimestamp();
        }

        public boolean hasVersionTimestamps() {
            return data.hasVersionTimestamps();
        }

        public boolean isCoded() {
            return data.isCoded();
        }

        public boolean isLeaf() {
            return data.isLeaf();
        }

        public boolean isReadOnly() {
            return data.isReadOnly();
        }

		/**
		 * @param htree
		 *            The owning hash tree.
		 * @param globalDepth
		 *            The size of the address space (in bits) for each buddy
		 *            hash table on a directory page. The global depth of a node
		 *            is defined recursively as the local depth of that node
		 *            within its parent. The global/local depth are not stored
		 *            explicitly. Instead, the local depth is computed
		 *            dynamically when the child will be materialized by
		 *            counting the #of pointers to the the child in the
		 *            appropriate buddy hash table in the parent. This local
		 *            depth value is passed into the constructor when the child
		 *            is materialized and set as the global depth of the child.
		 */
		@SuppressWarnings("unchecked")
		public DirectoryPage(final HTree htree, final int globalDepth) {

			super(htree, true/* dirty */, globalDepth);

			childRefs = new Reference[(1 << htree.addressBits)];
			
			data = new MutableDirectoryPageData(htree.addressBits,
					htree.versionTimestamps);

        }

		/**
		 * Iterator visits children recursively expanding each child with a
		 * post-order traversal of its children and finally visits this node
		 * itself.
		 */
		@SuppressWarnings("unchecked")
		public Iterator<AbstractPage> postOrderIterator() {

			/*
			 * Iterator append this node to the iterator in the post-order
			 * position.
			 */

			return new Striterator(postOrderIterator2())
					.append(new SingleValueIterator(this));

		}
	    
	    /**
	     * Visits the children (recursively) using post-order traversal, but does
	     * NOT visit this node.
	     */
	    @SuppressWarnings("unchecked")
	    private Iterator<AbstractPage> postOrderIterator2() {

			/*
			 * Iterator visits the direct children, expanding them in turn with
			 * a recursive application of the post-order iterator.
			 */

			return new Striterator(childIterator()).addFilter(new Expander() {

				private static final long serialVersionUID = 1L;

				/*
				 * Expand each child in turn.
				 */
				protected Iterator expand(final Object childObj) {

					/*
					 * A child of this node.
					 */

					final AbstractPage child = (AbstractPage) childObj;

					if (!child.isLeaf()) {

						/*
						 * The child is a Node (has children).
						 * 
						 * Visit the children (recursive post-order traversal).
						 */

						// BTree.log.debug("child is node: " + child);
						final Striterator itr = new Striterator(
								((DirectoryPage) child).postOrderIterator2());

						// append this node in post-order position.
						itr.append(new SingleValueIterator(child));

						return itr;

					} else {

						/*
						 * The child is a leaf.
						 */

						// BTree.log.debug("child is leaf: " + child);

						// Visit the leaf itself.
						return new SingleValueIterator(child);

					}
				}
			});

		}
	    
	    /**
	     * Iterator visits the direct child nodes in the external key ordering.
	     */
	    private Iterator<AbstractPage> childIterator() {

	        return new ChildIterator();

	    }

	    /**
	     * Visit the distinct children exactly once (if there are multiple
	     * pointers to a given child, that child is visited just once).
	     */
	    private class ChildIterator implements Iterator<AbstractPage> {

	    	final private int slotsPerPage = 1<<globalDepth;
	    	private int slot = 0;
	    	private AbstractPage child = null;
	    	
	    	private ChildIterator() {
	    		nextChild(); // materialize the first child.
	    	}

			/**
			 * Advance to the next distinct child. The first time, this will
			 * always return the child in slot zero on the page. Thereafter, it
			 * will skip over pointers to the same child and return the next
			 * distinct child.
			 * 
			 * @return <code>true</code> iff there is another distinct child
			 *         reference.
			 */
	    	private boolean nextChild() {
				for (; slot < slotsPerPage; slot++) {
					final AbstractPage tmp = getChild(slot);
					if (tmp != child) {
						child = tmp;
						return true;
					}
				}
				return false;
	    	}
	    	
			public boolean hasNext() {
				/*
				 * Return true if there is another child to be visited.
				 * 
				 * Note: This depends on nextChild() being invoked by the
				 * constructor and by next().
				 */
				return slot < slotsPerPage;
			}

			public AbstractPage next() {
				if (!hasNext())
					throw new NoSuchElementException();
				final AbstractPage tmp = child;
				nextChild(); // advance to the next child (if any).
				return tmp;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
	    	
	    }
	    
	    /**
	     * Human readable representation of the {@link Node}.
	     */
	    @Override
	    public String toString() {

	        final StringBuilder sb = new StringBuilder();

	        // sb.append(getClass().getName());
	        sb.append(super.toString());

	        sb.append("{ isDirty=" + isDirty());

	        sb.append(", isDeleted=" + isDeleted());

	        sb.append(", addr=" + identity);

	        final DirectoryPage p = (parent == null ? null : parent.get());

	        sb.append(", parent=" + (p == null ? "N/A" : p.toShortString()));

	        sb.append(", isRoot=" + (htree.root == this)); 

	        if (data == null) {

	            // No data record? (Generally, this means it was stolen by copy on
	            // write).
	            sb.append(", data=NA}");

	            return sb.toString();

	        }
	        sb.append(", globalDepth=" + getGlobalDepth());
            sb.append(", nbuddies=" + (1 << htree.addressBits) / (1 << globalDepth));
            sb.append(", slotsPerBuddy="+(1 << globalDepth));
//	        sb.append(", minKeys=" + minKeys());
//
//	        sb.append(", maxKeys=" + maxKeys());

	        toString(this, sb);

	        // indicate if each child is loaded or unloaded.
	        {

	            final int nchildren = getChildCount();

	            sb.append(", children=[");

	            for (int i = 0; i < nchildren; i++) {

	                if (i > 0)
	                    sb.append(", ");

	                final AbstractPage child = childRefs[i] == null ? null
	                        : childRefs[i].get();

	                sb.append(child == null ? "U" : "L");

	            }

	            sb.append("]");

	        }

	        sb.append("}");

	        return sb.toString();

	    }

		/**
		 * TODO We should dump each bucket page once. This could be done either
		 * by dumping each buddy bucket on the page separately or by skipping
		 * through the directory page until we get to the next bucket page and
		 * then dumping that.
		 * 
		 * TODO The directory page validation should include checks on the
		 * bucket references and addresses. For a given buddy hash table, the
		 * reference and address should pairs should be consistent if either the
		 * reference or the address appears in another slot of that table. Also,
		 * there can not be "gaps" between observations of a reference to a
		 * given bucket - once you see another bucket reference a previously
		 * observed reference can not then appear.
		 * 
		 * @see HTree#validatePointersInParent(DirectoryPage, int, AbstractPage)
		 */
	    @Override
		protected boolean dump(Level level, PrintStream out,
			int height, boolean recursive, boolean materialize) {

	        // True iff we will write out the node structure.
	        final boolean debug = level.toInt() <= Level.DEBUG.toInt();

	        // Set true iff an inconsistency is detected.
	        boolean ok = true;

//	        final int branchingFactor = this.getBranchingFactor();
//	        final int nkeys = getKeyCount();
//	        final int minKeys = this.minKeys();
//	        final int maxKeys = this.maxKeys();

			if (this == htree.root) {
				if (parent != null) {
					out
							.println(indent(height)
									+ "ERROR: this is the root, but the parent is not null.");
					ok = false;
				}
	        } else {
	            /*
	             * Note: there is a difference between having a parent reference and
	             * having the parent be strongly reachable. However, we actually want
	             * to maintain both -- a parent MUST always be strongly reachable
	             * ... UNLESS you are doing a fast forward or reverse leaf scan
	             * since the node hierarchy is not being traversed in that case.
	             */
	            if (parent == null) {
	                out
	                        .println(indent(height)
	                                + "ERROR: the parent reference MUST be defined for a non-root node.");
	                ok = false;
	            } else if (parent.get() == null) {
	                out.println(indent(height)
	                        + "ERROR: the parent is not strongly reachable.");
	                ok = false;
	            }
	        }

			if (debug) {
	            out.println(indent(height) + toString());
	        }

	        /*
	         * Look for inconsistencies for children. A dirty child MUST NOT have an
	         * entry in childAddr[] since it is not persistent and MUST show up in
	         * dirtyChildren. Likewise if a child is NOT dirty, then it MUST have an
	         * entry in childAddr and MUST NOT show up in dirtyChildren.
	         * 
	         * This also verifies that all entries beyond nchildren (nkeys+1) are
	         * unused.
	         */
			for (int i = 0; i < (1 << htree.addressBits); i++) {

                /*
                 * Scanning a valid child index.
                 * 
                 * Note: This is not fetching the child if it is not in memory
                 * -- perhaps it should using its persistent id?
                 */

                final AbstractPage child = (childRefs[i] == null ? null
                        : childRefs[i].get());

                if (child != null) {

                    if (child.parent == null || child.parent.get() == null) {
                        /*
                         * the reference to the parent MUST exist since the we
                         * are the parent and therefore the parent is strongly
                         * reachable.
                         */
                        out.println(indent(height) + "  ERROR child[" + i
                                + "] does not have parent reference.");
                        ok = false;
                    }

                    if (child.parent.get() != this) {
                        out.println(indent(height) + "  ERROR child[" + i
                                + "] has wrong parent.");
                        ok = false;
                    }

                    if (child.isDirty()) {
                        /*
                         * Dirty child. The parent of a dirty child MUST also be
                         * dirty.
                         */
                        if (!isDirty()) {
                            out.println(indent(height) + "  ERROR child[" + i
                                    + "] is dirty, but its parent is clean");
                            ok = false;
                        }
                        if (childRefs[i] == null) {
                            out.println(indent(height) + "  ERROR childRefs["
                                    + i + "] is null, but the child is dirty");
                            ok = false;
                        }
                        if (getChildAddr(i) != NULL) {
                            out.println(indent(height) + "  ERROR childAddr["
                                    + i + "]=" + getChildAddr(i)
                                    + ", but MUST be " + NULL
                                    + " since the child is dirty");
                            ok = false;
                        }
                    } else {
                        /*
                         * Clean child (ie, persistent). The parent of a clean
                         * child may be either clear or dirty.
                         */
                        if (getChildAddr(i) == NULL) {
                            out.println(indent(height) + "  ERROR childKey["
                                    + i + "] is " + NULL
                                    + ", but child is not dirty");
                            ok = false;
                        }
                    }

                }

	        }

	        if (!ok && !debug) {

	            // @todo show the node structure with the errors since we would not
	            // have seen it otherwise.

	        }

	        if (recursive) {

	            /*
	             * Dump children using pre-order traversal.
	             */

	            final Set<AbstractPage> dirty = new HashSet<AbstractPage>();

				for (int i = 0; i < (1 << htree.addressBits); i++) {

					if (childRefs[i] == null
							&& !isReadOnly()
							&& ((MutableDirectoryPageData) data).childAddr[i] == 0) {

                        /*
                         * This let's us dump a tree with some kinds of
                         * structural problems (missing child reference or key).
                         */

                        out.println(indent(height + 1)
                                + "ERROR can not find child at index=" + i
                                + ", skipping this index.");

                        ok = false;

	                    continue;

	                }

	                /*
	                 * Note: this works around the assert test for the index in
	                 * getChild(index) but is not able/willing to follow a childKey
	                 * to a child that is not memory resident.
	                 */
	                // AbstractNode child = getChild(i);
	                final AbstractPage child = childRefs[i] == null ? null
	                        : childRefs[i].get();

	                if (child != null) {

	                    if (child.parent == null) {

	                        out
	                                .println(indent(height + 1)
	                                        + "ERROR child does not have parent reference at index="
	                                        + i);

	                        ok = false;

	                    }

	                    if (child.parent.get() != this) {

	                        out
	                                .println(indent(height + 1)
	                                        + "ERROR child has incorrect parent reference at index="
	                                        + i);

	                        ok = false;

	                    }

	                    if (child.isDirty()) {

	                        dirty.add(child);

	                    }

						if (!child.dump(level, out, height + 1, true,
								materialize)) {

	                        ok = false;

	                    }

	                }

	            }

	        }

	        return ok;

	    }

	    /**
	     * Utility method formats the {@link IDirectoryData}.
	     * 
	     * @param data
	     *            A data record.
	     * @param sb
	     *            The representation will be written onto this object.
	     * 
	     * @return The <i>sb</i> parameter.
	     */
	    static public StringBuilder toString(final IDirectoryData data,
	            final StringBuilder sb) {

	        final int nchildren = data.getChildCount();

	        sb.append(", nchildren=" + nchildren);

//	        sb.append(", spannedTupleCount=" + data.getSpannedTupleCount());
//
//	        sb.append(",\nkeys=" + data.getKeys());

	        {

	            sb.append(",\nchildAddr=[");

	            for (int i = 0; i < nchildren; i++) {

	                if (i > 0)
	                    sb.append(", ");

	                sb.append(data.getChildAddr(i));

	            }

	            sb.append("]");

	        }

//	        {
//
//	            sb.append(",\nchildEntryCount=[");
//
//	            for (int i = 0; i < nchildren; i++) {
//
//	                if (i > 0)
//	                    sb.append(", ");
//
//	                sb.append(data.getChildEntryCount(i));
//
//	            }
//
//	            sb.append("]");
//
//	        }

	        if(data.hasVersionTimestamps()) {
	            
	            sb.append(",\nversionTimestamps={min="
	                    + data.getMinimumVersionTimestamp() + ",max="
	                    + data.getMaximumVersionTimestamp() + "}");

	        }
	        
	        return sb;

	    }

		@Override
		public void PP(final StringBuilder sb) {
			
			sb.append(PPID() + " [" + globalDepth + "] " + indent(getLevel()) );
			
			sb.append("("); // start of address map.

			// #of buddy tables on a page.
			final int nbuddies = (1 << htree.addressBits) / (1 << globalDepth);

			// #of address slots in each buddy hash table.
			final int slotsPerBuddy = (1 << globalDepth);

			for (int i = 0; i < nbuddies; i++) {

				if (i > 0) // buddy boundary marker
					sb.append(";");

				for (int j = 0; j < slotsPerBuddy; j++) {

					if (j > 0) // slot boundary marker.
						sb.append(",");

					final int slot = i * slotsPerBuddy + j;

					final AbstractPage child = getChild(slot);

					sb.append(child.PPID());

				}

			}

			sb.append(")"); // end of address map.

			sb.append("\n");

			final Iterator<AbstractPage> itr = childIterator();
			
			while(itr.hasNext()) {
			
				final AbstractPage child = itr.next();
				
				child.PP(sb);
		
			}

		}
		
		/*
		 * All directories are at max depth, so we just need to determine prefix
		 * and test key to locate child page o accept value 
		 */
		void insertRawTuple(final byte[] key, final byte[] val, final int buddy) {
			
			assert buddy == 0;
			
			final int pl = getPrefixLength();
			final int hbits = BytesUtil.getBits(key, pl, htree.addressBits);
			AbstractPage cp = getChild(hbits);
			
			cp.insertRawTuple(key, val, getChildBuddy(hbits));
		}

		/*
		 * Checks child buddies, looking for previous references and incrementing
		 */
		private int getChildBuddy(int hbits) {
			int cbuddy = 0;
			final AbstractPage cp= getChild(hbits);
			while (hbits > 0 && cp == getChild(--hbits))
				cbuddy++;
			
			return cbuddy;
		}
	    
    } // class DirectoryPage

    /**
     * Iterator visits all tuples in a buddy bucket having the desired key. 
     * <p>
     * Note: This implementation is NOT thread-safe.
     */
	private static class BuddyBucketTupleIterator<E> implements
			ITupleIterator<E> {

		/** The key. */
		private final byte[] key;
		/** The bucket. */
		private final BucketPage bucket;
		/** The index of the first slot in the buddy bucket. */
		private final int buddyOffset;
		/** The index of the last slot in the buddy bucket. */
		private final int lastSlot;
		/** The index of the next slot to be visited. This is set by the
		 * constructor to the first slot and the tuple at that slot is pre-fetched.
		 */
		private int index;
	    private int lastVisited = -1;
	    private final AbstractTuple<E> tuple;

		public BuddyBucketTupleIterator(final byte[] key,
				final BucketPage bucket, final int buddyOffset) {

			if (key == null)
				throw new IllegalArgumentException();

			if (bucket == null)
				throw new IllegalArgumentException();

			this.key = key;
			this.bucket = bucket;
			this.buddyOffset = buddyOffset;

			// #of slots on the page.
			final int slotsOnPage = (1 << bucket.htree.addressBits);

			// #of address slots in each buddy hash table.
			final int slotsPerBuddy = (1 << bucket.globalDepth);

			// the index of the last slot in the buddy bucket.
			lastSlot = buddyOffset + slotsPerBuddy;

			// range check buddyOffset.
			if (buddyOffset < 0 || buddyOffset >= slotsOnPage)
				throw new IndexOutOfBoundsException();

			// The first slot to test.
			index = buddyOffset;

			tuple = new Tuple<E>(bucket.htree, IRangeQuery.DEFAULT);
			
		}
		
	    /**
	     * Examines the entry at {@link #index}. If it passes the criteria for an
	     * entry to visit then return true. Otherwise increment the {@link #index}
	     * until either all entries in this leaf have been exhausted -or- the an
	     * entry is identified that passes the various criteria.
	     */
	    public boolean hasNext() {

    		final IRaba keys = bucket.getKeys();

    		for( ; index >= buddyOffset && index < lastSlot; index++) {
	         
//	            /*
//	             * TODO Skip deleted entries unless specifically requested.
//	             */
//	            if (hasDeleteMarkers && !visitDeleted
//	                    && leaf.getDeleteMarker(index)) {
//
//	                // skipping a deleted version.
//	                continue;
//	                
//	            }

				if (!keys.isNull(index)) {
					if (BytesUtil.bytesEqual(key, keys.get(index))) {
						// entry @ index is next to visit.
						return true;
					}
				}

			} // next index.

	        // nothing left to visit in this buddy bucket.
	        return false;
	        
	    }

	    public ITuple<E> next() {

	        if (!hasNext()) {

	            throw new NoSuchElementException();

	        }

	        lastVisited = index++;

	        tuple.copy(lastVisited, bucket);
	        
	        return tuple;
	        
	    }

		/**
		 * Operation is not supported.
		 * 
		 * TODO ITupleCursor and delete-behind are two ways to achive this. See
		 * {@link LeafTupleIterator#remove()}. I also did a listener based
		 * iterator for GOM which supports concurrent mutation (as long as you
		 * obey the thread safety for the API).
		 */
	    public void remove() {

	    	throw new UnsupportedOperationException();
	    	
		}

	} // class BuddyBucketTupleIterator

	/**
	 * A key-value pair used to facilitate some iterator constructs.
	 */
	private static class Tuple<E> extends AbstractTuple<E> {
	    
	    /**
	     * 
	     * @param btree
	     * @param flags
	     */
	    public Tuple(final AbstractHTree htree, final int flags) {

	        super(flags);

	        if (htree == null)
	            throw new IllegalArgumentException();

//	        tupleSer = htree.getIndexMetadata().getTupleSerializer();
	        tupleSer = null;// TODO required for getObject()
	        
	    }

	    public int getSourceIndex() {
	        
	        return 0;
	        
	    }

	    private final ITupleSerializer tupleSer;

	    public ITupleSerializer getTupleSerializer() {
	        
	        return tupleSer;
	        
	    }
	    
	}

	/**
	 * Pretty print the {@link HTree}.
	 */
	String PP() {
		
		final StringBuilder sb = new StringBuilder();
		
		sb.append("Total Entries: " + nentries + "\n");
		
		root.PP(sb);
		
		return sb.toString();
	
	}

	public long rangeCount() {
	
		return nentries;
	
	}

	/**
	 * Simple iterator visits all tuples in the {@link HTree} in order by the
	 * effective prefix of their keys. Since the key is typically a hash of some
	 * fields in the associated application data record, this will normally
	 * visit application data records in what appears to be an arbitrary order.
	 * Tuples within a buddy hash bucket will be delivered in a random order.
	 * <p>
	 * Note: The {@link HTree} does not currently maintain metadata about the
	 * #of spanned tuples in a {@link DirectoryPage}. Without that we can not
	 * provide fast range counts, linear list indexing, etc.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ITupleIterator rangeIterator() {
		
		return new BucketPageTupleIterator(this,
				IRangeQuery.DEFAULT/* flags */, new Striterator(
						root.postOrderIterator()).addFilter(new Filter() {
					private static final long serialVersionUID = 1L;
					public boolean isValid(final Object obj) {
						return ((AbstractPage) obj).isLeaf();
					}
				}));
	
	}

	/**
	 * Visits the values stored in the {@link HTree} in order by the effective
	 * prefix of their keys. Since the key is typically a hash of some fields in
	 * the associated application data record, this will normally visit
	 * application data records in what appears to be an arbitrary order. Tuples
	 * within a buddy hash bucket will be delivered in a random order.
	 * <p>
	 * Note: This allocates a new byte[] for each visited value. It is more
	 * efficient to reuse a buffer for each visited {@link Tuple}. This can be
	 * done using {@link #rangeIterator()}.
	 * 
	 * TODO Must resolve references to raw records.
	 */
	@SuppressWarnings("unchecked")
	public Iterator<byte[]> values() {
		
		return new Striterator(rangeIterator()).addFilter(new Resolver(){
			private static final long serialVersionUID = 1L;
			protected Object resolve(final Object obj) {
				return ((ITuple<?>)obj).getValue();
			}});
		
	}

	/**
	 * Visits the non-empty tuples in each {@link BucketPage} visited by the
	 * source iterator.
	 * 
	 * TODO This might be reworked as an expander and an iterator visiting the
	 * tuples on a single bucket page.  That could provide more reuse.  See
	 * {@link BucketPage#tuples()}
	 */
	private static class BucketPageTupleIterator<E> implements
			ITupleIterator<E> {

		private final Iterator<BucketPage> src;
		private BucketPage currentBucketPage = null;
		private int nextNonEmptySlot = 0;

		private final Tuple<E> tuple;

		BucketPageTupleIterator(final HTree htree, final int flags,
				final Iterator<BucketPage> src) {

			if (htree == null)
				throw new IllegalArgumentException();

			if(src == null)
				throw new IllegalArgumentException();
			
			this.tuple = new Tuple<E>(htree,flags);
			
			this.src = src;
			
		}
		
		/**
		 * Scan to the next non-empty slot in the current {@link BucketPage}.
		 * 
		 * @return <code>true</code> iff there is a non-empty slot on the
		 *         current {@link BucketPage}.
		 */
		private boolean findNextSlot() {
			if (currentBucketPage == null)
				throw new IllegalStateException();
			final int slotsPerPage = 1 << currentBucketPage.htree.addressBits;
			final IRaba keys = currentBucketPage.getKeys();
			for (; nextNonEmptySlot < slotsPerPage; nextNonEmptySlot++) {
				if (keys.isNull(nextNonEmptySlot))
					continue;
				return true;
			}
			// The current page is exhausted.  We need to fetch another page.
			currentBucketPage = null;
			return false;
		}
		
		public boolean hasNext() {
			if(currentBucketPage != null) {
				return true;
			}
			// Scan for the next bucket page having a visitable tuple.
			while(src.hasNext()) {
				currentBucketPage = src.next();
				nextNonEmptySlot = 0;
				if (findNextSlot())
					return true;
			}
			return false;
		}

		public ITuple<E> next() {
			if (!hasNext())
				throw new NoSuchElementException();
			// Copy the data for the current tuple into the Tuple buffer.
			tuple.copy(nextNonEmptySlot, currentBucketPage);
			/*
			 * Advance to the next slot on the current page. if there is non,
			 * then the current page reference will be cleared and we will need
			 * to fetch a new page in hasNext() on the next invocation.
			 */
			nextNonEmptySlot++; // skip past the current tuple.
			findNextSlot(); // find the next non-null slot (next tuple).
			// Return the Tuple buffer.
			return tuple;
		}
		
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}
	
}
