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

import java.lang.ref.Reference;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ISimpleBTree;
import com.bigdata.btree.Leaf;
import com.bigdata.btree.MutableLeafData;
import com.bigdata.btree.Node;
import com.bigdata.btree.PO;
import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
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
	implements 
//	IIndex, 
	ISimpleBTree//, IAutoboxBTree, ILinearList, IBTreeStatistics, ILocalBTreeView
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

	public boolean isReadOnly() {
		return false; // TODO set by ctor.
	}

    /**
     * The root of the btree. This is initially a leaf until the leaf is split,
     * at which point it is replaced by a node. The root is also replaced each
     * time copy-on-write triggers a cascade of updates.
     * <p>
     * The hard reference to the root node is cleared if the index is
     * {@link #close() closed}. This method automatically {@link #reopen()}s
     * the index if it is closed, making it available for use.
     */
    final protected AbstractPage getRoot() {

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
		 * global depth is always the #of address bits (which is the maximum
		 * value that global depth can take on for a given hash tree). There is
		 * a single bucket page and all entries in the root directory page point
		 * to that bucket. This means that the local depth of the initial bucket
		 * page will be zero (you can prove this for yourself by consulting the
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
		assert r.getSlotsPerBuddy() == 1;
		assert r.getNumBuddies() == (1 << addressBits);

		// Data for the root.
		final MutableDirectoryPageData rdata = (MutableDirectoryPageData) r.data;

        // Initial bucket.
		final BucketPage b = new BucketPage(this, 0/* globalDepth */);

		final int nslots = r.getSlotsPerBuddy() * r.getNumBuddies();

		for (int i = 0; i < nslots; i++) {
		
			r.refs[i] = (Reference)b.self;
			rdata.childAddr[i] = 0L; // TODO addr(b) plus ref of [b].
			
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
	 * be provided for this purpose. E.g., ISimpleHTree.  Custom serialization
	 * and even a custom mutable leaf data record should be used with this case.
	 */
    
	public boolean contains(byte[] key) {
		// TODO Auto-generated method stub
		return false;
	}

    public void insert(final Object obj) {
        insert(obj.hashCode(), SerializerUtil.serialize(obj));
    }

    public void insert(final int key, final byte[] val) {
    	// TODO code path for native int32 keys.
    	throw new UnsupportedOperationException();
    }

	public byte[] insert(byte[] key, byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

    public void lookup(final Object obj) {
        lookup(obj.hashCode());
    }

    public void lookup(final int key) {
    	// TODO code path for native int32 keys.
    	throw new UnsupportedOperationException();
    }

	public byte[] lookup(byte[] key) {
		// TODO Return 1st match or null iff not found.
		return null;
	}

	public byte[] remove(byte[] key) {
		// TODO Remove 1st match, returning value.
		return null;
	}

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
		 * a directory page. The legal range is <code>[0:addressBits]</code>.
		 * <p>
		 * When the global depth is increased, the hash table requires twice as
		 * many slots on the page. This forces the split of the directory page
		 * onto two pages in order to accommodate the additional space
		 * requirements. The maximum global depth is <code>addressBits</code>,
		 * at which point the hash table fills the entire directory page. The
		 * minimum global depth is ZERO (0), at which point the buddy hash table
		 * has a single slot.
		 * <p>
		 * The global depth of a directory page is just the local depth of the
		 * directory page in its parent.
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
		 * directory page. This depends on the <i>prefixLength</i> to be
		 * ignored, the <i>globalDepth</i> of this directory page, and the key.
		 * 
		 * @param key
		 *            The key.
		 * @param prefixLength
		 *            The #of MSB bits in the key which are to be ignored at
		 *            this level of the hash tree. This is computed dynamically
		 *            during recursive traversal of the hash tree. This is ZERO
		 *            (0) for the root directory. It is incremented by
		 *            <i>globalDepth</i> at each level of recursion for insert,
		 *            lookup, etc.
		 * 
		 * @return The int32 value containing the relevant bits from the key.
		 */
		public int getLocalHashCode(final byte[] key, final int prefixLength) {
		
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
	 * While the #of tuples in a buddy is governed by the global depth of the
	 * bucket, the #of tuples in a bucket page consisting of a single buddy
	 * bucket might be governed by the target page size. (Perhaps we could even
	 * split buddy buckets if the page size is exceeded?) 
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

        public int getSpannedTupleCount() {
            return data.getSpannedTupleCount();
        }

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
            
            data = new MutableLeafData(htree.branchingFactor,
                    htree.versionTimestamps, htree.deleteMarkers,
                    htree.rawRecords);
//            new MutableBucketData(data)

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
		final Reference<AbstractPage>[] refs;
		
		/**
		 * Persistent data.
		 */
        IDirectoryData data;

		/**
		 * Get the child indexed by the key.
		 * <p>
		 * Note: The recursive descent pattern requires the caller to separately
		 * compute the buddyOffset before each descent into a child.
		 * 
		 * @param key
		 *            The key.
		 * @param prefixLength
		 *            The #of MSB bits in the key which are to be ignored at
		 *            this level of the hash tree. This is computed dynamically
		 *            during recursive traversal of the hash tree. This is ZERO
		 *            (0) for the root directory. It is incremented by
		 *            <i>globalDepth</i> at each level of recursion for insert,
		 *            lookup, etc.
		 * @param buddyOffset
		 *            The offset into the child of the first slot for the buddy
		 *            hash table or buddy hash bucket.
		 * 
		 * @return The child indexed by the key.
		 * 
		 * @see HTreeUtil#getBuddyOffset(int, int, int)
		 * 
		 *      TODO This method signature would require us to invoke
		 *      {@link #getLocalHashCode(byte[], int)} twice (once here and once
		 *      in support of obtaining the buddyOffset). That suggests that the
		 *      caller should invoke the method once and then use the
		 *      int32:hashBits variant and we would then drop this version of
		 *      the method.
		 */
		protected AbstractPage getChild(final byte[] key,
				final int prefixLength, final int buddyOffset) {

			return getChild(getLocalHashCode(key, prefixLength), buddyOffset);

		}

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
			final Reference<AbstractPage> ref = refs[index];
			
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

						refs[i] = (Reference) child.self;
						
						n++;
						
					}

				}

				assert n == npointers;

			}
			
			return child;
			
		}

//		/**
//		 * Return the address of the child page indexed by the relevant bits of
//		 * the key.
//		 * 
//		 * @param hashBits
//		 *            An int32 extract revealing only the relevant bits of the
//		 *            key for the current {@link DirectoryPage}.
//		 * @param offset
//		 *            Index position of the start of the buddy hash table in the
//		 *            page. This is known to the caller when inspecting the
//		 *            parent directory and is the index of the slot within the
//		 *            buddy hash table.
//		 * 
//		 * @return The address of the child.
//		 */
//		public long getChildAddrByHashCode(final int hashBits, final int offset) {
//			// width of a buddy hash table in pointer slots.
//			final int tableWidth = (1 << globalDepth);
//			// index position of the start of the buddy hash table in the page.
//			final int tableOffset = (tableWidth * offset);
//			// index of the slot in the buddy hash table for the given hash
//			// bits.
//			final int index = tableOffset + hashBits;
//			// the address of the child for that slot.
//			final long addr = data.getChildAddr(index);
//			return addr;
//		}

//		/**
//		 * The #of bits in the key examined at a given level in the tree. The
//		 * range is [0:addressBits].
//		 * <p>
//		 * This depends solely on the <i>globalDepth</i> of a directory page and
//		 * the #of pointers to child (<i>npointers</i>) in that directory page.
//		 * The local depth is obtained counting the #of slots in the appropriate
//		 * buddy hash table of the parent which are mapped onto the same child.
//		 * Given 2^(d-i) such entries in the parent, the local depth of that
//		 * child is i. Some fancy bit work may be used to solve for i.
//		 * <p>
//		 * The local depth of a node may not exceed the global depth of its
//		 * parent. If splitting a buddy bucket would cause the local depth to
//		 * exceed the global depth of the parent, then the global depth of the
//		 * parent is increased, which causes the parent to be split in turn.
//		 * Since the root directory does not have a parent its local depth is
//		 * not defined.
//		 * 
//		 * @param child
//		 *            A reference to a direct child of this
//		 *            {@link DirectoryPage}.
//		 * 
//		 * @throws IllegalArgumentException
//		 *             if the <i>child</i> is <code>null</code>
//		 * @throws IllegalArgumentException
//		 *             if the <i>child</i> is not a direct child of this
//		 *             {@link DirectoryPage}.
//		 */
//		int getLocalDepth(final AbstractPage child) {
//
//			final int npointers = countPointers(child);
//
//			return HTreeUtil.getLocalDepth(htree.addressBits, globalDepth,
//					npointers);
//
//    	}
//
//		/**
//		 * Return the #of references to the child.
//		 * 
//		 * @param child
//		 *            A reference to a direct child of this
//		 *            {@link DirectoryPage}.
//		 *            
//		 * @return The #of references to that child.
//		 * 
//		 * @throws IllegalArgumentException
//		 *             if the <i>child</i> is <code>null</code>
//		 * @throws IllegalArgumentException
//		 *             if the <i>child</i> is not a direct child of this
//		 *             {@link DirectoryPage}.
//		 */
//    	int countPointers(final AbstractPage child) {
//
//    		if (child == null)
//				throw new IllegalArgumentException();
//			if (child.parent.get() != this)
//				throw new IllegalArgumentException();
//
////			/*
////			 * The index of the first slot in this directory for the buddies of
////			 * that child.
////			 */
////			final int i = (index / globalDepth) << 1;
////			
////			/*
////			 * Count the #of buddies for that child.
////			 */
////   		return 0;
//			throw new UnsupportedOperationException();
//			
//    	}
    	
    	/*
    	 * end of section for loading a child.
    	 */
    	
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
		public DirectoryPage(final HTree htree, final int globalDepth) {

			super(htree, true/* dirty */, globalDepth);

			refs = new Reference[(1 << htree.addressBits)];
			
			data = new MutableDirectoryPageData(htree.addressBits,
					htree.versionTimestamps);

        }

    } // class DirectoryPage

}
