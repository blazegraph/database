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
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractTuple;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.LeafTupleIterator;
import com.bigdata.btree.MutableLeafData;
import com.bigdata.btree.Node;
import com.bigdata.btree.PO;
import com.bigdata.btree.data.DefaultLeafCoder;
import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableKeyBuffer;
import com.bigdata.btree.raba.MutableValueBuffer;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rawstore.IRawStore;

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
 */
public class HTree extends AbstractHTree 
//	implements 
//	IIndex, 
//  ISimpleBTree//, IAutoboxBTree, ILinearList, IBTreeStatistics, ILocalBTreeView
{

    private static final transient Logger log = Logger.getLogger(HTree.class);

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
	protected int nnodes;

	/**
	 * The #of {@link BucketPage}s in the {@link HTree}. This is one (1) for a
	 * new {@link HTree} (one directory page and one bucket page).
	 */
	protected int nleaves;

	/**
	 * The #of entries in the {@link HTree}. This is ZERO (0) for a new
	 * {@link HTree}.
	 */
	protected int nentries;
    
    final public int getNodeCount() {
        
        return nnodes;
        
    }

    final public int getLeafCount() {
        
        return nleaves;
        
    }

    final public int getEntryCount() {
        
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
     */
    public HTree(final IRawStore store, final int addressBits) {

//    	super(store, nodeFactory, readOnly, addressBits, metadata, recordCompressorFactory);
		super(store, false/*readOnly*/, addressBits);
    	
//        if (pageSize <= 0)
//            throw new IllegalArgumentException("pageSize must be positive.");
//
//        if ((pageSize & -pageSize) != pageSize)
//            throw new IllegalArgumentException("pageSize not power of 2: "
//                    + pageSize);

        // @todo from IndexMetadata
        this.versionTimestamps = false;
        this.deleteMarkers = false;
        this.rawRecords = true;

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
		
			b.parent = (Reference) r.self; // TODO who has responsibility to set the parent reference?

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
			prefixLength = prefixLength + child.globalDepth;
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
			prefixLength = prefixLength + child.globalDepth;
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
			prefixLength = prefixLength + child.globalDepth;
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
				
				// attempt to insert the tuple into the bucket.
				
				if(!bucketPage.insert(key, value, current, buddyOffset)) {
				
					// TODO if(parent.isReadOnly()) parent = copyOnWrite();
					
					if (current.globalDepth == child.globalDepth) {

						/*
						 * There is only one buddy hash bucket on the page. To
						 * split the page, we have to introduce a new directory
						 * page above it.
						 * 
						 * TODO This code path is to introduce new directory
						 * page if sole buddy bucket is full. However, we might
						 * also need a code path to split the buddy hash table
						 * in the directory. The way things are falling out
						 * right now I do not see how this would happen as the
						 * global depth of a directory is always the same as
						 * address bits, but maybe I am missing something....
						 */
						addDirectoryPageAndSplitBucketPage(current,
								buddyOffset, bucketPage);
						
						// The children of [current] have changed so we will
						// search current again.
						continue;
						
					}

					// globalDepth >= localDepth
					splitBucketsOnPage(current, buddyOffset, bucketPage);
					
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
			// buddy hash table. TODO child.globalDepth might always be
			// [addressBits] for a directory page...
			prefixLength = prefixLength + child.globalDepth;
			
			// find the offset of the buddy hash table in the child.
			buddyOffset = HTreeUtil
					.getBuddyOffset(hashBits, current.globalDepth,
							child.globalDepth/* localDepthOfChild */);
			
			// update current so we can search in the child.
			current = (DirectoryPage) child;
			
		}

	} // insert()

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
	 * 
	 * @param parent
	 *            The parent {@link DirectoryPage}.
	 * @param buddyOffset
	 *            The buddyOffset within the <i>parent</i>. This identifies
	 *            which buddy hash table in the parent must be its pointers
	 *            updated such that it points to both the original child and new
	 *            child.
	 * @param oldBucket
	 *            The child {@link BucketPage}.
	 * 
	 * @throws IllegalArgumentException
	 *             if any argument is <code>null</code>.
	 * @throws IllegalStateException
	 *             if the depth of the child is GTE the depth of the parent.
	 * @throws IllegalStateException
	 *             if the <i>parent<i/> is read-only.
	 * @throws IllegalStateException
	 *             if the <i>oldBucket</i> is read-only.
	 * @throws IllegalStateException
	 *             if the parent of the <oldBucket</i> is not the given
	 *             <i>parent</i>.
	 */
	private void splitBucketsOnPage(final DirectoryPage parent,
			final int buddyOffset, final BucketPage oldBucket) {

		if (parent == null)
			throw new IllegalArgumentException();
		if (oldBucket == null)
			throw new IllegalArgumentException();
		if (oldBucket.globalDepth >= parent.globalDepth) {
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
		if(oldBucket.isReadOnly()) // must be mutable.
			throw new IllegalStateException();
		if (oldBucket.parent != parent.self) // must be same Reference.
			throw new IllegalStateException();
		
		if (log.isDebugEnabled())
			log.debug("parent=" + parent.toShortString() + ", buddyOffset="
					+ buddyOffset + ", child=" + oldBucket);

		final int oldDepth = oldBucket.globalDepth;
		final int newDepth = oldDepth + 1;

		// Allocate a new bucket page (globalDepth is increased by one).
		final BucketPage newBucket = new BucketPage(this, newDepth);

		assert newBucket.isDirty();
		
		// Set the parent reference on the new bucket.
		newBucket.parent = (Reference) parent.self;
		
		// Increase global depth on the old page also.
		oldBucket.globalDepth = newDepth;

		nleaves++; // One more bucket page in the hash tree. 

		// update the pointers in the parent.
		updatePointersInParent(parent, buddyOffset, oldDepth, oldBucket,
				newBucket);
		
		// redistribute buddy buckets between old and new pages.
		redistributeBuddyBuckets(oldDepth, newDepth, oldBucket, newBucket);

		// TODO assert invariants?
		
	}

	/**
	 * Update pointers in buddy hash table in the parent in order to link the
	 * new {@link BucketPage} into the parent {@link DirectoryPage}.
	 * <p>
	 * There will be [npointers] slots in the appropriate buddy hash table in
	 * the parent {@link DirectoryPage} which point to the old
	 * {@link BucketPage}. The upper 1/2 of those pointers will be modified to
	 * point to the new {@link BucketPage}. The lower 1/2 of the pointers will
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
	 * @param oldBucket
	 *            The old {@link BucketPage}.
	 * @param newBucket
	 *            The new {@link BucketPage}.
	 */
	private void updatePointersInParent(final DirectoryPage parent,
			final int buddyOffset, final int oldDepth,
			final BucketPage oldBucket, final BucketPage newBucket) {

		// #of address slots in the parent buddy hash table.
		final int slotsPerBuddy = (1 << parent.globalDepth);

		// #of pointers in the parent buddy hash table to the old bucket.
		final int npointers = 1 << (parent.globalDepth - oldDepth);
		
		// Must be at least two slots since we will change at least one.
		assert slotsPerBuddy > 1 : "slotsPerBuddy=" + slotsPerBuddy;

		// Must be at least two pointers since we will change at least one.
		assert npointers > 1 : "npointers=" + npointers;
		
		// The first slot in the buddy hash table in the parent.
		final int firstSlot = buddyOffset;
		
		// The last slot in the buddy hash table in the parent.
		final int lastSlot = buddyOffset + slotsPerBuddy;

		/*
		 * Count pointers to the old bucket page. There should be
		 * [npointers] of them and they should be contiguous.
		 * 
		 * Note: We can test References here rather than comparing addresses
		 * because we know that the parent and the old bucket are both
		 * mutable. This means that their childRef is defined and their
		 * storage address is NULL.
		 * 
		 * TODO This logic should be in DirectoryPage#dump()
		 */
		int firstPointer = -1;
		int nfound = 0;
		boolean discontiguous = false;
		for (int i = firstSlot; i < lastSlot; i++) {
			if (parent.childRefs[i] == oldBucket.self) {
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

			if (parent.childRefs[i] != oldBucket.self)
				throw new RuntimeException("Does not point to old child.");
			
			// update the references to the new bucket.
			parent.childRefs[i] = (Reference) newBucket.self;
			
		}
			
	} // updatePointersInParent

	/**
	 * Redistribute the buddy buckets.
	 * <p>
	 * Note: We are not changing the #of buddy buckets, just their size and the
	 * page on which they are found. Any tuples in a source bucket will wind up
	 * in the same bucket afterwards, but the page and offset on the page of the
	 * buddy bucket may have been changed.
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

		// #of slots on the bucket page (invariant given addressBits).
		final int slotsOnPage = (1 << addressBits);

		// #of address slots in each old buddy hash bucket.
		final int slotsPerOldBuddy = (1 << oldDepth);

		// #of address slots in each new buddy hash bucket.
		final int slotsPerNewBuddy = (1 << newDepth);

		// #of buddy tables on the old bucket page.
		final int oldBuddyCount = (slotsOnPage) / slotsPerOldBuddy;

		// #of buddy tables on the bucket pages after the split.
		final int newBuddyCount = (slotsOnPage) / slotsPerNewBuddy;

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
					dstKeys.keys[dstSlot] = srcKeys.keys[srcSlot];
					dstVals.values[dstSlot] = srcVals.values[srcSlot];
					srcKeys.keys[srcSlot] = null;
					srcVals.values[srcSlot] = null;

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
					dstKeys.keys[dstSlot] = srcKeys.keys[srcSlot];
					dstVals.values[dstSlot] = srcVals.values[srcSlot];
					srcKeys.keys[srcSlot] = null;
					srcVals.values[srcSlot] = null;

				}

			}

		}

	}

	/**
	 * Split when <code>globalDepth == localDepth</code>. This case requires the
	 * introduction of a new {@link DirectoryPage}.
	 * 
	 * @param parent
	 *            The parent.
	 * @param buddyOffset
	 *            The offset of the buddy hash table within the parent.
	 * @param oldBucket
	 *            The {@link BucketPage} to be split.
	 */
	private void addDirectoryPageAndSplitBucketPage(final DirectoryPage parent,
			final int buddyOffset, final BucketPage oldBucket) {

		throw new UnsupportedOperationException();

	}

	/**
	 * Validate pointers in buddy hash table in the parent against the global
	 * depth as self-reported by the child. By definition, the global depth of
	 * the child is based on the global depth of the parent and the #of pointers
	 * to the child in the parent. The {@link AbstractPage#globalDepth} value is
	 * the cached result of that computation. It can be cross checked by
	 * counting the actual number of pointers in the parent and comparing that
	 * with this formula:
	 * 
	 * <pre>
	 * npointers := 1 &lt;&lt; (parent.globalDepth - child.globalDepth)
	 * </pre>
	 * 
	 * This method validates that the cached value of global depth on the child
	 * is consistent with the #of pointers in the specified buddy hash table of
	 * the parent. However, this validate depends on the global depth of the
	 * parent itself being correct.
	 * <p>
	 * Note: While other buddy hash tables in the parent can point into the same
	 * child page. However, due to the buddy offset computation they will not
	 * point into the same buddy on the same child.
	 * 
	 * @param parent
	 *            The parent {@link DirectoryPage}.
	 * @param buddyOffset
	 *            The buddyOffset within the <i>parent</i>. This identifies
	 *            which buddy hash table in the parent should have references to
	 *            the child.
	 * @param child
	 *            The child {@link AbstractPage}.
	 */
	private void validatePointersInParent(final DirectoryPage parent,
			final int buddyOffset, final AbstractPage child) {

		// #of address slots in the parent buddy hash table.
		final int slotsPerBuddy = (1 << parent.globalDepth);

		// #of pointers expected in the parent buddy hash table.
		final int npointers = 1 << (parent.globalDepth - child.globalDepth);
		
		// The first slot in the buddy hash table in the parent.
		final int firstSlot = buddyOffset;
		
		// The last slot in the buddy hash table in the parent.
		final int lastSlot = buddyOffset + slotsPerBuddy;

		if (parent.isDirty() && parent.getIdentity() != IRawStore.NULL)
			throw new RuntimeException(
					"Parent address should be NULL since parent is dirty");

		if (child.isDirty() && child.getIdentity() != IRawStore.NULL)
			throw new RuntimeException(
					"Child address should be NULL since child is dirty");
		
		/*
		 * Count pointers to the child page. There should be [npointers] of them
		 * and they should be contiguous. Since the child is materialized (we
		 * have its reference) we can test pointers. If the child is dirty, then
		 * all slots in the parent having pointers to the child should be
		 * associated with NULL addresses for the child. If the child is clean,
		 * then all slots in the parent having references for the child should
		 * have the same non-zero address for the child. Also, any slot in the
		 * parent having the address of a persistent child should have the same
		 * Reference object, which should be child.self.
		 */
		int firstPointer = -1;
		int nfound = 0;
		boolean discontiguous = false;
		for (int i = firstSlot; i < lastSlot; i++) {
			final boolean slotRefIsChild = parent.childRefs[i] == child.self;
			final boolean slotAddrIsChild = parent.getChildAddr(i) == child
					.getIdentity();
			if (slotAddrIsChild && !slotRefIsChild)
				throw new RuntimeException(
						"Child reference in parent should be child since addr in parent is child addr");
			final boolean slotIsChild = slotRefIsChild || slotAddrIsChild;
			if (slotIsChild) {
				if (child.isDirty()) {
					// A dirty child must have a NULL address in the parent.
					if (parent.data.getChildAddr(i) != IRawStore.NULL)
						throw new RuntimeException(
								"Child address in parent should be NULL since child is dirty");
				} else {
					// A clean child must have a non-NULL address in the parent.1
					if (parent.data.getChildAddr(i) == IRawStore.NULL)
						throw new RuntimeException(
								"Child address in parent must not be NULL since child is clean");
				}
				if (firstPointer == -1)
					firstPointer = i;
				nfound++;
			} else {
				if (firstPointer != -1 && nfound != npointers) {
					discontiguous = true;
				}
			}
		}
		if (firstPointer == -1)
			throw new RuntimeException("No pointers to child");
		if (nfound != npointers) {
			/*
			 * This indicates either a problem in maintaining the pointers
			 * (References and/or addresses) in the parent for the child, a
			 * problem where child.self is not a unique Reference for the child,
			 * and/or a problem with the cached [globalDepth] for the child.
			 */
			throw new RuntimeException("Expected " + npointers
					+ " pointers to child, but found=" + nfound);
		}
		if (discontiguous)
			throw new RuntimeException(
					"Pointers to child are discontiguous in parent's buddy hash table.");
	} // validatePointersInParent

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
         * The data record. {@link MutableLeafData} is used for all mutation
         * operations. {@link ReadOnlyLeafData} is used when the {@link Leaf} is
         * made persistent. A read-only data record is automatically converted into
         * a {@link MutableLeafData} record when a mutation operation is requested.
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
            
			// TODO keysRaba must allow nulls!
            data = new MutableLeafData(//
            		(1<<htree.addressBits), // fan-out
                    htree.versionTimestamps,//
                    htree.deleteMarkers,//
                    htree.rawRecords//
                    );
//            new MutableBucketData(data)

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
			 * FIXME We need a modified MutableKeysRaba for this purpose. It
			 * will have to allow nulls in the middle (unless we compact
			 * everything). [We will have to compact prior to coding in order
			 * for the assumptions of the values coder to be maintained.] It
			 * would be pretty easy to just swap in the first undeleted tuple
			 * but that will not keep things dense. Likewise, Monet style
			 * cracking could only be done within a buddy bucket rather than
			 * across all buddies on a page.
			 */
			final MutableKeyBuffer keys = (MutableKeyBuffer)getKeys();
			final MutableValueBuffer vals = (MutableValueBuffer)getValues();
			
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
        	throw new UnsupportedOperationException();
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
					hashBits, npointers);

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

	
}
