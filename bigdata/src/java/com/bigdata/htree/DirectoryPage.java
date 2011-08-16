package com.bigdata.htree;

import java.io.PrintStream;
import java.lang.ref.Reference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Level;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.Node;
import com.bigdata.htree.AbstractHTree.ChildMemoizer;
import com.bigdata.htree.AbstractHTree.LoadChildRequest;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.concurrent.Memoizer;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * An {@link HTree} directory page (node). Each directory page will hold one or
 * more "buddy" hash tables. The #of buddy hash tables on the page depends on
 * the globalDepth of the page and the addressBits as computed by
 * {@link DirectoryPage#getNumBuddies()}.
 */
class DirectoryPage extends AbstractPage implements IDirectoryData {
	static int createdPages = 0;
	
    /**
     * The depth of a bucket page which overflows is always <i>addressBits</i>.
     */
    final int getOverflowPageDepth() {
     
        return htree.addressBits;
        
    }
	
	/**
	 * Transient references to the children.
	 */
	// Note: cleared by copyOnWrite when we steal the array.
	/*final*/ Reference<AbstractPage>[] childRefs;

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
	 *            The offset into the child of the first slot for the buddy hash
	 *            table or buddy hash bucket.
	 * 
	 * @return The child indexed by the key.
	 * 
	 * @see HTreeUtil#getBuddyOffset(int, int, int)
	 */
	protected AbstractPage getChild(final int hashBits, final int buddyOffset) {

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
	 * Split the child {@link BucketPage}.
	 */
	void _splitBucketPage(final BucketPage bucketPage) {

        // Note: this.getChildCount() is less direct.
		final int slotsOnPage = 1 << htree.addressBits;
		int start = 0;
		for (int s = 0; s < slotsOnPage; s++) {
			if (bucketPage.self == childRefs[s]) {
				start = s;
				break;
			}
		}
		int last = start;
		for (int s = start + 1; s < slotsOnPage; s++) {
			if (bucketPage.self == childRefs[s]) {
				last++;
			} else {
				break;
			}
		}
		final int npointers = last - start + 1;

		assert npointers > 1;
		assert npointers % 2 == 0;

		final int crefs = npointers >> 1; // half references for each new child

		final BucketPage a = new BucketPage((HTree) htree,
				bucketPage.globalDepth + 1);
		a.parent = (Reference<DirectoryPage>) self;
		for (int s = start; s < start + crefs; s++) {
			childRefs[s] = (Reference<AbstractPage>) a.self;
		}
		final BucketPage b = new BucketPage((HTree) htree,
				bucketPage.globalDepth + 1);
		b.parent = (Reference<DirectoryPage>) self;
		for (int s = start + crefs; s <= last; s++) {
			childRefs[s] = (Reference<AbstractPage>) b.self;
		}

		// Now insert raw values from original page
//		final ITupleIterator tuples = bucketPage.tuples();
//		while (tuples.hasNext()) {
//			final ITuple tuple = tuples.next();
//			insertRawTuple(tuple.getKey(), tuple.getValue(), 0);
//		}
        final int bucketSlotsPerPage = bucketPage.slotsOnPage();
        for (int i = 0; i < bucketSlotsPerPage; i++) {
            ((HTree) htree).insertRawTuple(bucketPage, i);
        }
        // ...and finally delete old page
        if (bucketPage.isPersistent()) {
            htree.deleteNodeOrLeaf(bucketPage.getIdentity());
        }
        bucketPage.delete();
	}

	/**
	 * Return the {@link Reference} for the child at that index.
	 * 
	 * @param index
	 *            The index
	 * 
	 * @return The {@link Reference}.
	 */
	Reference<AbstractPage> getChildRef(final int index) {

		return childRefs[index];

	}

	/**
	 * This method must be invoked on a parent to notify the parent that the
	 * child has become persistent. The method scans the weak references for the
	 * children, finds the index for the specified child, and then sets the
	 * corresponding index in the array of child addresses.
	 * 
	 * @param child
	 *            The child.
	 * 
	 * @exception IllegalStateException
	 *                if the child is not persistent.
	 * @exception IllegalArgumentException
	 *                if the child is not a child of this node.
	 */
	void setChildAddr(final AbstractPage child) {

		assert !isReadOnly();

		if (!child.isPersistent()) {

			// The child does not have persistent identity.
			throw new IllegalStateException();

		}

		final int slotsPerPage = 1 << htree.addressBits;
		
		if (childRefs == null)
			throw new IllegalStateException("childRefs must not be NULL");

		boolean found = false;
		for (int i = 0; i < slotsPerPage; i++) {

			if (childRefs[i] == child.self) {

				((MutableDirectoryPageData) data).childAddr[i] = child
						.getIdentity();

                found = true;

            } else if (found) {

                // no more pointers to that child.
                break;
                
            }

		}

	}

	/**
	 * Return the child at the specified index in the {@link DirectoryPage}.
	 * 
	 * @param index
	 *            The index of the slot in the {@link DirectoryPage}. If the
	 *            child must be materialized, the buddyOffset will be computed
	 *            based on the globalDepth and the #of pointers to that child
	 *            will be computed so its depth may be set.
	 * 
	 * @return The child at that index.
	 */
	AbstractPage getChild(final int index) {

        if (htree.memo == null) {

            /*
             * Optimization for the mutable B+Tree.
             * 
             * Note: This optimization depends on the assumption that concurrent
             * operations are never submitted to the mutable B+Tree. In fact,
             * the UnisolatedReadWriteIndex *DOES* allow concurrent readers (it
             * uses a ReentrantReadWriteLock). Therefore this code path is now
             * expressed conditionally on whether or not the Memoizer object is
             * initialized by AbstractBTree.
             * 
             * Note: Since the caller is single-threaded for the mutable B+Tree
             * we do not need to use the Memoizer, which just delegates to
             * _getChild(index). This saves us some object creation and overhead
             * for this case.
             */

            return _getChild(index, null/* req */);
            
        }

        /*
         * If we can resolve a hard reference to the child then we do not need
         * to look any further.
         */
//        synchronized (childRefs) 
        {

            /*
             * Note: we need to synchronize on here to ensure visibility for
             * childRefs[index] (in case it was updated in another thread). This
             * is true even for the mutable B+Tree since the caller could use
             * different threads for different operations. However, this
             * synchronization will never be contended for the mutable B+Tree.
             */
      	
            final Reference<AbstractPage> childRef = childRefs[index];

            final AbstractPage child = childRef == null ? null : childRef.get();

            if (child != null) {

                // Already materialized.
        		htree.touch(child);
        		
                return child;

            }

        }

        /*
         * Otherwise we need to go through the Memoizer pattern to achieve
         * non-blocking access. It will wind up delegating to _getChild(int),
         * which is immediately below. However, it will ensure that one and only
         * one thread executes _getChild(int) for a given parent and child
         * index. That thread will update childRefs[index]. Any concurrent
         * requests for the same child will wait for the FutureTask inside of
         * the Memoizer and then return the new value of childRefs[index].
         */

        return htree.loadChild(this, index);

    }

    /**
     * Method conditionally reads the child at the specified index from the
     * backing store and sets its reference on the appropriate element of
     * {@link #childRefs}. This method assumes that external mechanisms
     * guarantee that no other thread is requesting the same child via this
     * method at the same time. For the mutable B+Tree, that guarantee is
     * trivially given by its single-threaded constraint. For the read-only
     * B+Tree, {@link AbstractHTree#loadChild(DirectoryPage, int)} provides this
     * guarantee using a {@link Memoizer} pattern. This method explicitly
     * handshakes with the {@link ChildMemoizer} to clear the {@link FutureTask}
     * from the memoizer's internal cache as soon as the reference to the child
     * has been set on the appropriate element of {@link #childRefs}.
     * 
     * @param index
     *            The index of the child.
     * @param req
     *            The key we need to remove the request from the
     *            {@link ChildMemoizer} cache (and <code>null</code> if this
     *            method is not invoked by the memoizer pattern).
     * 
     * @return The child and never <code>null</code>.
     */
    AbstractPage _getChild(final int index, final LoadChildRequest req) {

		/*
		 * Make sure that the child is not reachable. It could have been
		 * concurrently set even if the caller had tested this and we do not
		 * want to read through to the backing store unless we need to.
		 * 
		 * Note: synchronizing on childRefs[] should not be necessary. For a
		 * read-only B+Tree, the synchronization is provided by the Memoizer
		 * pattern. For a mutable B+Tree, the synchronization is provided by the
		 * single-threaded contract for mutation and by the requirement to use a
		 * construct, such as a Queue or the UnisolatedReadWriteIndex, which
		 * imposes a memory barrier when passing a B+Tree instance between
		 * threads.
		 * 
		 * See http://www.cs.umd.edu/~pugh/java/memoryModel/archive/1096.html
		 */
        AbstractPage child;
        synchronized (childRefs) {

            /*
             * Note: we need to synchronize on here to ensure visibility for
             * childRefs[index] (in case it was updated in another thread).
             */
            final Reference<AbstractPage> childRef = childRefs[index];

            child = childRef == null ? null : childRef.get();

            if (child != null) {

                // Already materialized.
        		htree.touch(child);
        		
                return child;

            }

        }

        /*
         * The child needs to be read from the backing store.
         */

		// width of a buddy hash table (#of pointer slots).
		final int tableWidth = 1 << globalDepth;

		// offset in [0:nbuddies-1] to the start of the buddy spanning that
		// index.
		final int tableOffset = index / tableWidth;

		/*
		 * We need to get the address of the child, figure out the local depth
		 * of the child (by counting the #of points in the buddy bucket to that
		 * child), and then materialize the child from its address.
		 */
		final long addr = data.getChildAddr(index);

        if (addr == IRawStore.NULL) {

            // dump(Level.DEBUG, System.err);
            /*
             * Note: It appears that this can be triggered by a full disk, but I
             * am not quite certain how a full disk leads to this condition.
             * Presumably the full disk would cause a write of the child to
             * fail. In turn, that should cause the thread writing on the B+Tree
             * to fail. If group commit is being used, the B+Tree should then be
             * discarded and reloaded from its last commit point.
             */
            throw new AssertionError(
                    "Child does not have persistent identity: this=" + this
                            + ", index=" + index);

        }

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
         * Find the local depth of the child within this node. this becomes the
         * global depth of the child.
         * 
         * Note: This winds up being invoked to compute the depth of an overflow
         * directory page when we first descend from a normal directory page.
         */
        final int localDepth = HTreeUtil.getLocalDepth(htree.addressBits,
                globalDepth, npointers);

        /*
         * Read the child from the backing store (potentially reads through to
         * the disk).
         * 
         * Note: This is guaranteed to not do duplicate reads. There are two
         * cases. (A) The mutable B+Tree. Since the mutable B+Tree is single
         * threaded, this case is trivial. (B) The read-only B+Tree. Here our
         * guarantee is that the caller is in ft.run() inside of the Memoizer,
         * and that ensures that only one thread is executing for a given
         * LoadChildRequest object (the input to the Computable). Note that
         * LoadChildRequest MUST meet the criteria for a hash map for this
         * guarantee to obtain.
         */

		child = htree.readNodeOrLeaf(addr);
		
        // set the depth on the child.
        if (!child.isLeaf() && ((DirectoryPage) child).isOverflowDirectory()) {
            /*
             * Note: The global depth of an overflow page is always set to this
             * constant. It should be ignored for code paths which maintain the
             * overflow directory and overflow bucket pages.
             */
            child.globalDepth = getOverflowPageDepth();
        } else {
            child.globalDepth = localDepth;
        }

		/*
		 * Set the reference for each slot in the buddy bucket which pointed at
		 * that child. There will be [npointers] such slots.
         * 
         * Note: This code block is synchronized in order to facilitate the safe
         * publication of the change in childRefs[index] to other threads.
         */
        synchronized (childRefs) {

			int n = 0;

			final int lastIndex = (tableOffset + tableWidth);

			for (int i = tableOffset; i < lastIndex; i++) {

				if (data.getChildAddr(i) == addr) {

		            /*
		             * Since the childRefs[index] element has not been updated we do so
		             * now while we are synchronized.
		             * 
		             * Note: This paranoia test could be tripped if the caller allowed
		             * concurrent requests to enter this method for the same child. In
		             * that case childRefs[index] could have an uncleared reference to
		             * the child. This would indicate a breakdown in the guarantee we
		             * require of the caller.
		             */
		            assert childRefs[i] == null || childRefs[i].get() == null : "Child is already set: this="
		                    + this + ", index=" + i;

		            childRefs[i] = (Reference) child.self;

					n++;

				}

			}

			// patch parent reference since loaded from store.
			child.parent = (Reference) this.self;

			assert n == npointers;

		}

        /*
         * Clear the future task from the memoizer cache.
         * 
         * Note: This is necessary in order to prevent the cache from retaining
         * a hard reference to each child materialized for the B+Tree.
         * 
         * Note: This does not depend on any additional synchronization. The
         * Memoizer pattern guarantees that only one thread actually call
         * ft.run() and hence runs this code.
         */
        if (req != null) {

            htree.memo.removeFromCache(req);

        }

		htree.touch(child);
		
        return child;

    }
    
//    private AbstractPage fixme(final int index) {
//    	
//		/*
//		 * Look at the entry in the buddy hash table. If there is a reference to
//		 * the child and that reference has not been cleared, then we are done
//		 * and we can return the child reference and the offset of the buddy
//		 * table or bucket within the child.
//		 */
//		final Reference<AbstractPage> ref = childRefs[index];
//
//		AbstractPage child = ref == null ? null : ref.get();
//
//		if (child != null) {
//
//			return child;
//
//		}
//
//		// width of a buddy hash table (#of pointer slots).
//		final int tableWidth = 1 << globalDepth;
//
//		// offset in [0:nbuddies-1] to the start of the buddy spanning that
//		// index.
//		final int tableOffset = index / tableWidth;
//
//		/*
//		 * We need to get the address of the child, figure out the local depth
//		 * of the child (by counting the #of points in the buddy bucket to that
//		 * child), and then materialize the child from its address.
//		 * 
//		 * fixme MEMORIZER : This all needs to go through a memoizer pattern.
//		 * The hooks for that should be on AbstractHTree(.memo), but I have not
//		 * yet ported that code.
//		 */
//		final long addr = data.getChildAddr(index);
//
//		/*
//		 * Scan to count pointers to child within the buddy hash table.
//		 */
//		final int npointers;
//		{
//
//			int n = 0; // npointers
//
//			final int lastIndex = (tableOffset + tableWidth);
//
//			for (int i = tableOffset; i < lastIndex; i++) {
//
//				if (data.getChildAddr(i) == addr)
//					n++;
//
//			}
//
//			assert n > 0;
//
//			npointers = n;
//
//		}
//
//		/*
//		 * Find the local depth of the child within this node. this becomes the
//		 * global depth of the child.
//		 */
//		final int localDepth = HTreeUtil.getLocalDepth(htree.addressBits,
//				globalDepth, npointers);
//
//		child = htree.readNodeOrLeaf(addr, localDepth/* globalDepthOfChild */);
//
//		/*
//		 * Set the reference for each slot in the buddy bucket which pointed at
//		 * that child. There will be [npointers] such slots.
//		 */
//		{
//
//			int n = 0;
//
//			final int lastIndex = (tableOffset + tableWidth);
//
//			for (int i = tableOffset; i < lastIndex; i++) {
//
//				if (data.getChildAddr(i) == addr) {
//
//					childRefs[i] = (Reference) child.self;
//
//					n++;
//
//				}
//
//			}
//
//			assert n == npointers;
//
//		}
//
//		return child;
//
//	}

	public AbstractFixedByteArrayBuffer data() {
		return data.data();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Note: This method can only be used once you have decoded the hash bits
	 * from the key and looked up the offset of the buddy hash table on the page
	 * and the offset of the slot within the buddy hash table for the desired
	 * key. You must also know the localDepth of the child when it is
	 * materialized so that information can be set on the child, where is
	 * becomes the globalDepth of the child.
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

    /**
     * The result depends on the backing {@link IDirectoryData} implementation.
     * The {@link DirectoryPage} will be mutable when it is first created and is
     * made immutable when it is persisted. If there is a mutation operation,
     * the backing {@link IDirectoryData} is automatically converted into a
     * mutable instance.
     */
	public boolean isReadOnly() {
		return data.isReadOnly();
	}

    /**
     * @param htree
     *            The owning hash tree.
     * @param overflowDirectory
     *            <code>true</code> iff this is an overflow directory page.
     * @param globalDepth
     *            The size of the address space (in bits) for each buddy hash
     *            table on a directory page. The global depth of a node is
     *            defined recursively as the local depth of that node within its
     *            parent. The global/local depth are not stored explicitly.
     *            Instead, the local depth is computed dynamically when the
     *            child will be materialized by counting the #of pointers to the
     *            the child in the appropriate buddy hash table in the parent.
     *            This local depth value is passed into the constructor when the
     *            child is materialized and set as the global depth of the
     *            child.
     */
	@SuppressWarnings("unchecked")
    public DirectoryPage(final HTree htree, final boolean overflowDirectory,
            final int globalDepth) {

		super(htree, true/* dirty */, globalDepth);

		childRefs = new Reference[(1 << htree.addressBits)];

		data = new MutableDirectoryPageData(overflowDirectory, htree.addressBits,
				htree.versionTimestamps);

		createdPages++;

	}

	/**
	 * Deserialization constructor - {@link #globalDepth} MUST be set by the
	 * caller.
	 * 
	 * @param htree
	 * @param addr
	 * @param data
	 */
	DirectoryPage(final HTree htree, final long addr, final IDirectoryData data) {

		super(htree, false/* dirty */, 0/*unknownGlobalDepth*/);

        setIdentity(addr);

		this.data = data;

        childRefs = new Reference[(1 << htree.addressBits)];

		createdPages++;

	}

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node (must be immutable).
     * 
     * @param triggeredByChildId
     *            The persistent identity of the child that triggered the copy
     *            constructor. This should be the immutable child NOT the one
     *            that was already cloned. This information is used to avoid
     *            stealing the original child since we already made a copy of
     *            it. It is {@link #NULL} when this information is not
     *            available, e.g., when the copyOnWrite action is triggered by a
     *            join() and we are cloning the sibling before we redistribute a
     *            key to the node/leaf on which the join was invoked.
     * 
     * @todo We could perhaps replace this with the conversion of the
     *       INodeData:data field to a mutable field since the code which
     *       invokes copyOnWrite() no longer needs to operate on a new Node
     *       reference. However, I need to verify that nothing else depends on
     *       the new Node, e.g., the dirty flag, addr, etc.
     * 
     * @todo Can't we just test to see if the child already has this node as its
     *       parent reference and then skip it? If so, then that would remove a
     *       troublesome parameter from the API.
     */
	protected DirectoryPage(final DirectoryPage src,
			final long triggeredByChildId) {

        super(src);

		createdPages++;

        assert !src.isDirty();

        assert src.isReadOnly();
        // assert src.isPersistent();

		/*
		 * Steal/clone the data record.
		 * 
		 * Note: The copy constructor is invoked when we need to begin mutation
		 * operations on an immutable node or leaf, so make sure that the data
		 * record is mutable.
		 */
		final int slotsOnPage = 1 << htree.addressBits;

        assert src.data != null;
		this.data = src.isReadOnly() ? new MutableDirectoryPageData(
				htree.addressBits, src.data) : src.data;
        assert this.data != null;

        // clear reference on source.
        src.data = null;

        /*
         * Steal strongly reachable unmodified children by setting their parent
         * fields to the new node. Stealing the child means that it MUST NOT be
         * used by its previous ancestor (our source node for this copy).
         */

        childRefs = src.childRefs;
        src.childRefs = null;
        
        // childLocks = src.childLocks; src.childLocks = null;

        for (int i = 0; i < slotsOnPage; i++) {
        	
            final AbstractPage child = childRefs[i] == null ? null
                    : childRefs[i].get();

			/*
			 * Note: Both child.identity and triggeredByChildId will always be
			 * 0L for a transient B+Tree since we never assign persistent
			 * identity to the nodes and leaves. Therefore [child.identity !=
			 * triggeredByChildId] will fail for ALL children, including the
			 * trigger, and therefore fail to set the parent on any of them. The
			 * [btree.store==null] test handles this condition and always steals
			 * the child, setting its parent to this new node.
			 * 
			 * FIXME It is clear that testing on child.identity is broken in
			 * some other places for the transient store. [This comment is
			 * carried over from the B+Tree code.]
			 */
            if (child != null
                    && (htree.store == null || child.getIdentity() != triggeredByChildId)) {

                /*
                 * Copy on write should never trigger for a dirty node and only
                 * a dirty node can have dirty children.
                 */
                assert !child.isDirty();

                // Steal the child.
                child.parent = (Reference) this.self;
                // child.parent = btree.newRef(this);

                // // Keep a reference to the clean child.
                // childRefs[i] = new WeakReference<AbstractNode>(child);

            }

        }

    }

    /**
	 * Iterator visits children, recursively expanding each child with a
	 * post-order traversal of its children and finally visits this node itself.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Iterator<AbstractPage> postOrderNodeIterator(
			final boolean dirtyNodesOnly, final boolean nodesOnly) {

		if (dirtyNodesOnly && !dirty) {

			return EmptyIterator.DEFAULT;

		}

		/*
		 * Iterator append this node to the iterator in the post-order position.
		 */

		return new Striterator(postOrderIterator1(dirtyNodesOnly, nodesOnly))
				.append(new SingleValueIterator(this));

	}

	/**
	 * Iterator visits children recursively expanding each child with a
	 * post-order traversal of its children and finally visits this node itself.
	 */
	@SuppressWarnings("unchecked")
	public Iterator<AbstractPage> postOrderIterator() {

		/*
		 * Iterator append this node to the iterator in the post-order position.
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
		 * Iterator visits the direct children, expanding them in turn with a
		 * recursive application of the post-order iterator.
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
	 * Visit the distinct children exactly once (if there are multiple pointers
	 * to a given child, that child is visited just once).
	 */
	private class ChildIterator implements Iterator<AbstractPage> {

		final private int slotsPerPage = 1 << htree.addressBits;
		private int slot = 0;
		private AbstractPage child = null;

		private ChildIterator() {
			nextChild(); // materialize the first child.
		}

        /**
         * Advance to the next distinct child, materializing it if necessary.
         * The first time, this will always return the child in slot zero on the
         * page. Thereafter, it will skip over pointers to the same child and
         * return the next distinct child.
         * <p>
         * Note: For the special case of an overflow directory we allow a
         * <code>null</code> pointer for a child at a given index.
         * 
         * @return <code>true</code> iff there is another distinct child
         *         reference.
         */
		private boolean nextChild() {
		    final boolean isOverflowDirectory = isOverflowDirectory();
			for (; slot < slotsPerPage; slot++) {
			    AbstractPage tmp = deref(slot);
                if (isOverflowDirectory && tmp == null
                        && data.getChildAddr(slot) == NULL) {
			        // Null pointers allowed in an overflow directory page.
			        continue;
			    }
                tmp = tmp == null ? getChild(slot) : tmp;
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
		sb.append(", slotsPerBuddy=" + (1 << globalDepth));
		// sb.append(", minKeys=" + minKeys());
		//
		// sb.append(", maxKeys=" + maxKeys());

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
	 * Visits the children (recursively) using post-order traversal, but does
	 * NOT visit this node.
	 */
	@SuppressWarnings("unchecked")
	private Iterator<AbstractPage> postOrderIterator1(
			final boolean dirtyNodesOnly, final boolean nodesOnly) {

		/*
		 * Iterator visits the direct children, expanding them in turn with a
		 * recursive application of the post-order iterator.
		 * 
		 * When dirtyNodesOnly is true we use a child iterator that makes a best
		 * effort to only visit dirty nodes. Especially, the iterator MUST NOT
		 * force children to be loaded from disk if the are not resident since
		 * dirty nodes are always resident.
		 * 
		 * The iterator must touch the node in order to guarantee that a node
		 * will still be dirty by the time that the caller visits it. This
		 * places the node onto the hard reference queue and increments its
		 * reference counter. Evictions do NOT cause IO when the reference is
		 * non-zero, so the node will not be made persistent as a result of
		 * other node touches. However, the node can still be made persistent if
		 * the caller explicitly writes the node onto the store.
		 */

		// BTree.log.debug("node: " + this);
		return new Striterator(childIterator(dirtyNodesOnly))
				.addFilter(new Expander() {

					private static final long serialVersionUID = 1L;

					/*
					 * Expand each child in turn.
					 */
					protected Iterator expand(final Object childObj) {

						/*
						 * A child of this node.
						 */

						final AbstractPage child = (AbstractPage) childObj;

						if (dirtyNodesOnly && !child.isDirty()) {

							return EmptyIterator.DEFAULT;

						}

						if (child instanceof DirectoryPage) {

							/*
							 * The child is a Node (has children).
							 */

							// visit the children (recursive post-order
							// traversal).
							final Striterator itr = new Striterator(
									((DirectoryPage) child).postOrderIterator1(
											dirtyNodesOnly, nodesOnly));

							// append this node in post-order position.
							itr.append(new SingleValueIterator(child));

							return itr;

						} else {

							/*
							 * The child is a leaf.
							 */

							// Visit the leaf itself.
							if (nodesOnly)
								return EmptyIterator.DEFAULT;

							return new SingleValueIterator(child);

						}
					}
				});

	}

	/**
	 * Iterator visits the direct child nodes in the external key ordering.
	 * 
	 * @param dirtyNodesOnly
	 *            When true, only the direct dirty child nodes will be visited.
	 */
	public Iterator<AbstractPage> childIterator(final boolean dirtyNodesOnly) {

		if (dirtyNodesOnly) {

			return new DirtyChildIterator(this);

		} else {

			return new ChildIterator();

		}

	}

	/**
	 * TODO We should dump each bucket page once. This could be done either by
	 * dumping each buddy bucket on the page separately or by skipping through
	 * the directory page until we get to the next bucket page and then dumping
	 * that.
	 * 
	 * TODO The directory page validation should include checks on the bucket
	 * references and addresses. For a given buddy hash table, the reference and
	 * address should pairs should be consistent if either the reference or the
	 * address appears in another slot of that table. Also, there can not be
	 * "gaps" between observations of a reference to a given bucket - once you
	 * see another bucket reference a previously observed reference can not then
	 * appear.
	 * 
	 * @see HTree#validatePointersInParent(DirectoryPage, int, AbstractPage)
	 */
	@Override
	protected boolean dump(Level level, PrintStream out, int height,
			boolean recursive, boolean materialize) {

		// True iff we will write out the node structure.
		final boolean debug = level.toInt() <= Level.DEBUG.toInt();

		// Set true iff an inconsistency is detected.
		boolean ok = true;

		// final int branchingFactor = this.getBranchingFactor();
		// final int nkeys = getKeyCount();
		// final int minKeys = this.minKeys();
		// final int maxKeys = this.maxKeys();

		if (this == htree.root) {
			if (parent != null) {
				out.println(indent(height)
						+ "ERROR: this is the root, but the parent is not null.");
				ok = false;
			}
		} else {
			/*
			 * Note: there is a difference between having a parent reference and
			 * having the parent be strongly reachable. However, we actually
			 * want to maintain both -- a parent MUST always be strongly
			 * reachable ... UNLESS you are doing a fast forward or reverse leaf
			 * scan since the node hierarchy is not being traversed in that
			 * case.
			 */
			if (parent == null) {
				out.println(indent(height)
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
			 * Note: This is not fetching the child if it is not in memory --
			 * perhaps it should using its persistent id?
			 */

			final AbstractPage child = (childRefs[i] == null ? null
					: childRefs[i].get());

			if (child != null) {

				if (child.parent == null || child.parent.get() == null) {
					/*
					 * the reference to the parent MUST exist since the we are
					 * the parent and therefore the parent is strongly
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
						out.println(indent(height) + "  ERROR childRefs[" + i
								+ "] is null, but the child is dirty");
						ok = false;
					}
					if (getChildAddr(i) != NULL) {
						out.println(indent(height) + "  ERROR childAddr[" + i
								+ "]=" + getChildAddr(i) + ", but MUST be "
								+ NULL + " since the child is dirty");
						ok = false;
					}
				} else {
					/*
					 * Clean child (ie, persistent). The parent of a clean child
					 * may be either clear or dirty.
					 */
					if (getChildAddr(i) == NULL) {
						out.println(indent(height) + "  ERROR childKey[" + i
								+ "] is " + NULL + ", but child is not dirty");
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

				if (childRefs[i] == null && !isReadOnly()
						&& ((MutableDirectoryPageData) data).childAddr[i] == 0) {

					/*
					 * This let's us dump a tree with some kinds of structural
					 * problems (missing child reference or key).
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

						out.println(indent(height + 1)
								+ "ERROR child does not have parent reference at index="
								+ i);

						ok = false;

					}

					if (child.parent.get() != this) {

						out.println(indent(height + 1)
								+ "ERROR child has incorrect parent reference at index="
								+ i);

						ok = false;

					}

					if (child.isDirty()) {

						dirty.add(child);

					}

					if (!child.dump(level, out, height + 1, true, materialize)) {

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

		// sb.append(", spannedTupleCount=" + data.getSpannedTupleCount());
		//
		// sb.append(",\nkeys=" + data.getKeys());

		{

			sb.append(",\nchildAddr=[");

			for (int i = 0; i < nchildren; i++) {

				if (i > 0)
					sb.append(", ");

				sb.append(data.getChildAddr(i));

			}

			sb.append("]");

		}

		// {
		//
		// sb.append(",\nchildEntryCount=[");
		//
		// for (int i = 0; i < nchildren; i++) {
		//
		// if (i > 0)
		// sb.append(", ");
		//
		// sb.append(data.getChildEntryCount(i));
		//
		// }
		//
		// sb.append("]");
		//
		// }

		if (data.hasVersionTimestamps()) {

			sb.append(",\nversionTimestamps={min="
					+ data.getMinimumVersionTimestamp() + ",max="
					+ data.getMaximumVersionTimestamp() + "}");

		}

		return sb;

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

				final AbstractPage child = getChild(slot);

				sb.append(child.PPID());

			}

		}

		sb.append(")"); // end of address map.

		sb.append("\n");

		final Iterator<AbstractPage> itr = childIterator();

		while (itr.hasNext()) {

			final AbstractPage child = itr.next();

			child.PP(sb);

		}

	}

//	@Override
//	void insertRawTuple(final byte[] key, final byte[] val, final int buddy) {
//
//		assert buddy == 0;
//
//		final int pl = getPrefixLength();
//		final int hbits = getLocalHashCode(key, pl);
//		final AbstractPage cp = getChild(hbits); // removed eager copyOnWrite()
//
//		cp.insertRawTuple(key, val, getChildBuddy(hbits));
//	}

//	/*
//	 * Checks child buddies, looking for previous references and incrementing
//	 */
//	private int getChildBuddy(int hbits) {
//		int cbuddy = 0;
//		final AbstractPage cp = getChild(hbits);
//		while (hbits > 0 && cp == getChild(--hbits))
//			cbuddy++;
//
//		return cbuddy;
//	}

    /**
     * Invoked by {@link #copyOnWrite()} to clear the persistent address for a
     * child on a cloned parent and set the reference to the cloned child.
     * 
     * @param oldChildAddr
     *            The persistent address of the old child. The entries to be
     *            updated are located based on this argument. It is an error if
     *            this address is not found in the list of child addresses for
     *            this {@link Node}.
     * @param newChild
     *            The reference to the new child.
     */
	// FIXME Reconcile two versions of replaceChildRef
	// FIXME Reconcile pattern for deleting a persistent object (htree AND btree)
    void replaceChildRef(final long oldChildAddr, final AbstractPage newChild) {

        assert oldChildAddr != NULL || htree.store == null;
        assert newChild != null;

        // This node MUST have been cloned as a pre-condition, so it can not
        // be persistent.
        assert !isPersistent();
        assert !isReadOnly();

        // The newChild MUST have been cloned and therefore MUST NOT be
        // persistent.
        assert !newChild.isPersistent();

        assert !isReadOnly();

		final MutableDirectoryPageData data = (MutableDirectoryPageData) this.data;

		final int slotsOnPage = 1 << htree.addressBits;

		// Scan for location in weak references.
		int npointers = 0;
		boolean found = false;
		for (int i = 0; i < slotsOnPage; i++) {

            if (data.childAddr[i] == oldChildAddr) {

                found = true;
                
                // remove from cache and free the oldChildAddr if the Strategy
                // supports it.
                // TODO keep this in case we add in the store cache again.
//				if (htree.storeCache != null) {
//					// remove from cache.
//					htree.storeCache.remove(oldChildAddr);
//				}
                // free the oldChildAddr if the Strategy supports it
            	// - and only if not already deleted!
            	if (npointers == 0)
            		htree.deleteNodeOrLeaf(oldChildAddr);
                // System.out.println("Deleting " + oldChildAddr);

                // Clear the old key.
                data.childAddr[i] = NULL;

                // Stash reference to the new child.
                // childRefs[i] = btree.newRef(newChild);
                childRefs[i] = (Reference) newChild.self;

                // // Add the new child to the dirty list.
                // dirtyChildren.add(newChild);

                // Set the parent on the new child.
                // newChild.parent = btree.newRef(this);
                newChild.parent = (Reference) this.self;

                npointers++;
                
            } else if (found) {

                // No more pointers to that child.
                break;
                
            }

        }

		if (npointers == 0)
			throw new IllegalArgumentException("Not our child : oldChildAddr="
					+ oldChildAddr);

    }
    
    void replaceChildRef(final Reference<?> oldRef, final AbstractPage newChild) {

        final int slotsOnPage = 1 << htree.addressBits;

		final MutableDirectoryPageData data = (MutableDirectoryPageData) this.data;
		
		// Scan for location in weak references.
		int npointers = 0;
		for (int i = 0; i < slotsOnPage; i++) {

            if (childRefs[i] == oldRef) {

                // Clear the old key.
                data.childAddr[i] = NULL;

                // Stash reference to the new child.
                // childRefs[i] = btree.newRef(newChild);
                childRefs[i] = (Reference) newChild.self;

                newChild.parent = (Reference) this.self;

                npointers++;
            }

        }
		
		assert npointers > 0;
    	
    }

	int activeBucketPages() {
		int ret = 0;
		Iterator<AbstractPage> children = childIterator();
		while (children.hasNext()) {
			ret += children.next().activeBucketPages();
		}
		return ret;
	}

	int activeDirectoryPages() {
		int ret = 1;
		Iterator<AbstractPage> children = childIterator();
		while (children.hasNext()) {
			ret += children.next().activeDirectoryPages();
		}
		return ret;
	}

	void _addLevel(final BucketPage bucketPage) {
		assert !isReadOnly();
		assert !isOverflowDirectory();

		/**
		 * TBD: Since for _addLevel to be called there should only be a single reference to
		 * bucketPage, this directory MUST be at global depth.  BUT, rather than
		 * replacing the only the old bucket page with the reference to the new
		 * directory, we should/could create a directory of half this directory's depth
		 * and insert the requisite references
		 */
		
        // Create new directory to insert
        final DirectoryPage ndir = new DirectoryPage((HTree) htree,
                false, // overflowDirectory
                htree.addressBits/* globalDepth */);

        ((HTree) htree).nnodes++;

		// And new bucket pages for the new directory
		final BucketPage a = new BucketPage((HTree) htree, 1);
		final BucketPage b = new BucketPage((HTree) htree, 1);
		
		((HTree) htree).nleaves++; // Note: only +1 since we will delete the oldPage.
		
		// Link the new bucket pages into the new parent directory page.
		a.parent = (Reference<DirectoryPage>) ndir.self;
		b.parent = (Reference<DirectoryPage>) ndir.self;
		final int bucketRefs = (1 << htree.addressBits) >> 1; // half total number of directory slots
		for (int i = 0; i < bucketRefs; i++) {
			ndir.childRefs[i] = (Reference) a.self;
			ndir.childRefs[i+bucketRefs] = (Reference) b.self;
		}
		
		// now replace the reference to the old bucket page with the new directory
		replaceChildRef(bucketPage.self, ndir);
		
		// insert old tuples
		final int bucketSlotsPerPage = bucketPage.slotsOnPage();
		for (int i = 0; i < bucketSlotsPerPage; i++) {
			((HTree) htree).insertRawTuple(bucketPage, i);
		}

		// ...and finally delete old page
		if (bucketPage.isPersistent()) {
			htree.deleteNodeOrLeaf(bucketPage.getIdentity());
		}
		bucketPage.delete();
	}

	/**
	 * This method should only be called when using a DirectoryPage as a BucketPage
	 * Blob.  The final child in the blob is used for insert by default.
	 * 
	 * @param child - the child to be added
	 */
	void _addChild(final AbstractPage child) {
		assert isOverflowDirectory();
		assert !isReadOnly();
		
		// find available slot
		final MutableDirectoryPageData pdata = (MutableDirectoryPageData) data;
		
		for (int i = 0; i < pdata.childAddr.length; i++) {

		    final AbstractPage aChild = childRefs[i] == null ? null
                    : childRefs[i].get();
            
		    if (aChild == null && pdata.childAddr[i] == NULL) {

				childRefs[i] = (Reference<AbstractPage>) child.self;
				
				child.parent = (Reference<DirectoryPage>) self;
				
				return;
			
		    }
		    
		}
		
		// else insert new level above this one and add child to that
		final DirectoryPage pd = getParentDirectory();
		if (pd.isOverflowDirectory()) { // already handles blobs
			assert false; // unsure for now
		} else {
            final DirectoryPage blob = new DirectoryPage((HTree) htree,
                    true/*overflowDirectory*/,
                    getOverflowPageDepth());
			blob._addChild(this);
			blob._addChild(child);
			
			pd.replaceChildRef(this.self, blob);
		}
	}

    /**
     * Return the last non-<code>null</code> child of an overflow directory
     * page.
     */
	AbstractPage lastChild() {
        assert isOverflowDirectory();
        for (int i = data.getChildCount() - 1; i >= 0; i--) {
            final AbstractPage aChild = deref(i);
            if (aChild != null) {
                htree.touch(aChild);
                return aChild;
            }
            if (data.getChildAddr(i) != NULL)
                return getChild(i);
        }
		
		throw new AssertionError();
	}

    /**
     * Double indirection dereference for the specified index. If the
     * {@link Reference} is <code>null</code>, returns <code>null</code>.
     * Otherwise returns {@link Reference#get()}.
     */
	private AbstractPage deref(final int index) {

	    return childRefs[index] == null ? null  : childRefs[index].get();

	}
	
	public boolean isOverflowDirectory() {
	    
	    return data.isOverflowDirectory();
	    
	}
	
	/**
	 * Use Striterator Expander to optimize iteration
	 */
	public ITupleIterator getTuples() {
		// start with child nodes
		final Striterator tups = new Striterator(childIterator());
		
		// expand child contents
		tups.addFilter(new Expander() {

			protected Iterator expand(Object obj) {
				if (obj instanceof BucketPage) {
					return ((BucketPage) obj).tuples();
				} else {
					return ((DirectoryPage) obj).getTuples();
				}					
			}
			
		});
		
		// wrap striterator for return type
		return new ITupleIterator() {
			
			public ITuple next() {
				return (ITuple) tups.next();
			}

			public boolean hasNext() {
				return tups.hasNext();
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
		
	}

	@Override
	boolean isClean() {
		for (int i = 0; i < childRefs.length; i++) {
			if (childRefs[i] != null) {
				final AbstractPage node = childRefs[i].get();
				if (node != null && !node.isClean()) {
					return false;
				}
			}
		}
		
		return !isDirty();
	}
}
