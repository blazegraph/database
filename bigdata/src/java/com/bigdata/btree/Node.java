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
 * Created on Nov 15, 2006
 */
package com.bigdata.btree;

import java.io.PrintStream;
import java.lang.ref.Reference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.log4j.Level;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTree.ChildMemoizer;
import com.bigdata.btree.AbstractBTree.LoadChildRequest;
import com.bigdata.btree.data.DefaultNodeCoder;
import com.bigdata.btree.data.INodeData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableKeyBuffer;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.concurrent.LatchedExecutor;
import com.bigdata.util.concurrent.Memoizer;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * A non-leaf node.
 * </p>
 * <h2>Per-child min/max revision timestamps and timestamp revision filtering</h2>
 * 
 * In order to track the min/max timestamp on the {@link Node} we must also
 * track the min/max timestamp for each direct child of that {@link Node}. While
 * this inflates the size of the {@link INodeData} data record considerably, we
 * are required to track those per-child data in order to avoid a scan of the
 * children when we need to recompute the min/max timestamp for the {@link Node}
 * . The IO latency costs of that scan are simply not acceptable, especially for
 * large branching factors. The min/max timestamp on the {@link Node} is ONLY
 * used for filtering iterators based on a desired tuple revision range. This is
 * why the choice to support tuple revision filters is its own configuration
 * option.
 * 
 * FIXME An alternative to per-child min/max tuple revision timestamps would be
 * the concurrent materialization of the direct children. These data are only
 * mutable for B+Tree instances with relatively small branching factors. They
 * are immutable for the {@link IndexSegment}. However, the per-{@link Node}
 * min/max timestamp also make the tuple revision filtering more efficient since
 * we can prune the search before we materialize the child.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Node extends AbstractNode<Node> implements INodeData {

    /**
     * The data record. {@link MutableNodeData} is used for all mutation
     * operations. {@link ReadOnlyNodeData} is used when the {@link Node} is
     * made persistent. A read-only data record is automatically converted into
     * a {@link MutableNodeData} record when a mutation operation is requested.
     * <p>
     * Note: This is package private in order to expose it to {@link Leaf}.
     * 
     * @todo consider volatile and private for {@link Node#data} and
     *       {@link Leaf#data} with accessors and settors at package private
     *       where necessary.
     */
    INodeData data;

    /**
     * <p>
     * Weak references to child nodes (may be nodes or leaves). The capacity of
     * this array is m, where m is the {@link #branchingFactor}. Valid indices
     * are in [0:nkeys+1] since nchildren := nkeys+1 for a {@link Node}.
     * </p>
     * <p>
     * This array is dimensioned to one more than the maximum capacity so that
     * the child reference corresponding to the key that causes overflow and
     * forces the split may be inserted. This greatly simplifies the logic for
     * computing the split point and performing the split.
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
    transient/* volatile */Reference<AbstractNode<?>>[] childRefs;

    // /**
    // * An array of objects used to provide a per-child lock in order to allow
    // * maximum concurrency in {@link #getChild(int)}.
    // * <p>
    // * Note: this array is not allocated for a mutable btree since the caller
    // * will be single threaded and locking is therefore not required in
    // * {@link #getChild(int)}. We only need locking for read-only btrees since
    // * they allow concurrent readers.
    // *
    // * @todo There is a LOT of overhead to creating all these objects. They
    // are
    // * only really useful in high concurrent read scenarios such as highly
    // * concurrent query against an index. However, those situations
    // * typically have a lot of buffered B+Tree nodes so the in-memory
    // * footprint for this array is not really worth it.
    // * <p>
    // * This might be viable if we had an AtomicBitVector class since we
    // * could typically get by with 1-2 longs of data in that case.
    // */
    // transient private Object[] childLocks;

    /**
     * Return <code>((branchingFactor + 1) &lt;&lt; 1) - 1</code>
     */
    protected final int minKeys() {

        // /*
        // * Compute the minimum #of children/values. This is the same whether
        // * this is a Node or a Leaf.
        // */
        // final int minChildren = (btree.branchingFactor + 1) >> 1;
        //
        // // this.minKeys = isLeaf() ? minChildren : minChildren - 1;
        //
        // return minChildren - 1;
        return btree.minChildren - 1;

    }

    /**
     * Return <code>branchingFactor - 1</code>, which is the maximum #of keys
     * for a {@link Node}.
     */
    protected final int maxKeys() {

        // // The maximum #of keys is easy to compute.
        // this.maxKeys = isLeaf() ? branchingFactor : branchingFactor - 1;

        return btree.branchingFactor - 1;

    }

    /**
     * Range check a child index.
     * 
     * @param index
     *            The index of a child in [0:nkeys+1].
     * @return <code>true</code>
     * 
     * @throws IndexOutOfBoundsException
     *             if the index is not in the legal range.
     */
    final protected boolean rangeCheckChildIndex(final int index) {

        if (index < 0 || index > data.getKeyCount() + 1)
            throw new IndexOutOfBoundsException();

        return true;

    }

    /**
     * Return the {@link Reference} for the child. This is part of the internal
     * API.
     * 
     * @param index
     *            The index of the child.
     * 
     * @return The {@link Reference} for that child. This will be
     *         <code>null</code> if the child is not buffered. Note that a non-
     *         <code>null</code> return MAY still return <code>null</code> from
     *         {@link Reference#get()}.
     */
    public final Reference<AbstractNode<?>> getChildRef(final int index) {

        assert rangeCheckChildIndex(index);

        return childRefs[index];

    }

    final public INodeData getDelegate() {

        return data;

    }

    /*
     * INodeData.
     */

    /**
     * Always returns <code>false</code>.
     */
    final public boolean isLeaf() {

        return false;

    }

    /**
     * The result depends on the backing {@link INodeData} implementation. The
     * {@link Node} will be mutable when it is first created and is made
     * immutable when it is persisted. If there is a mutation operation, the
     * backing {@link INodeData} is automatically converted into a mutable
     * instance.
     */
    final public boolean isReadOnly() {

        return data.isReadOnly();

    }

    final public boolean isCoded() {

        return data.isCoded();

    }

    final public AbstractFixedByteArrayBuffer data() {

        return data.data();

    }

    final public int getKeyCount() {

        return data.getKeyCount();

    }

    final public IRaba getKeys() {

        return data.getKeys();

    }

    final public int getChildCount() {

        return data.getChildCount();

    }

    public final int getSpannedTupleCount() {

        return data.getSpannedTupleCount();

    }

    public final long getChildAddr(final int index) {

        return data.getChildAddr(index);

    }

    final public int getChildEntryCount(final int index) {

        return data.getChildEntryCount(index);

    }

    final public long getMaximumVersionTimestamp() {

        return data.getMaximumVersionTimestamp();

    }

    public long getMinimumVersionTimestamp() {

        return data.getMinimumVersionTimestamp();

    }

    final public boolean hasVersionTimestamps() {

        return data.hasVersionTimestamps();

    }

    /**
     * Apply the delta to the per-child count for this node and then recursively
     * ascend up the tree applying the delta to all ancestors of this node. This
     * is invoked solely by the methods that add and remove entries from a leaf
     * as those are the only methods that change the #of entries spanned by a
     * parent node. Methods that split, merge, or redistribute keys have a net
     * zero effect on the #of entries spanned by the parent.
     * <p>
     * This also updates {@link #getMinimumVersionTimestamp()} and
     * {@link #getMaximumVersionTimestamp()} iff version timestamps are enabled
     * and the child has a minimum (maximum) version timestamp GE the minimum
     * (maximum) version timestamp of this node.
     * 
     * @param child
     *            The direct child.
     * @param delta
     *            The change in the #of spanned children.
     */
    final protected void updateEntryCount(final AbstractNode<?> child,
            final int delta) {

        final int index = getIndexOf(child);

        assert !isReadOnly();

        final MutableNodeData data = (MutableNodeData) this.data;

        data.childEntryCounts[index] += delta;

        data.nentries += delta;

        assert data.childEntryCounts[index] > 0;

        assert data.nentries > 0;

        if (child.hasVersionTimestamps()) {

            final long cmin = child.getMinimumVersionTimestamp();

            final long cmax = child.getMaximumVersionTimestamp();

            if (cmin < data.minimumVersionTimestamp)
                data.minimumVersionTimestamp = cmin;

            if (cmax > data.maximumVersionTimestamp)
                data.maximumVersionTimestamp = cmax;

        }

        if (parent != null) {

            parent.get().updateEntryCount(this, delta);

        }

    }

    /**
     * Update the {@link #getMinimumVersionTimestamp()} and
     * {@link #getMaximumVersionTimestamp()}. This is invoked when the min/max
     * in the child has changed without a corresponding change to the #of
     * spanned tuples. E.g., when an insert() causes a tuple to be updated
     * rather than added.
     * 
     * @param child
     *            The direct child.
     */
    final protected void updateMinMaxVersionTimestamp(
            final AbstractNode<?> child) {

        assert !isReadOnly();

        final MutableNodeData data = (MutableNodeData) this.data;

        final long cmin = child.getMinimumVersionTimestamp();

        final long cmax = child.getMaximumVersionTimestamp();

        if (cmin < data.minimumVersionTimestamp)
            data.minimumVersionTimestamp = cmin;

        if (cmax > data.maximumVersionTimestamp)
            data.maximumVersionTimestamp = cmax;

        if (parent != null) {

            parent.get().updateMinMaxVersionTimestamp(child);

        }

    }

    /**
     * De-serialization constructor.
     * <p>
     * Note: The de-serialization constructor (and ONLY the de-serialization
     * constructor) ALWAYS creates a clean node. Therefore the {@link PO#dirty}
     * flag passed up from this constructor has the value <code>false</code>.
     * 
     * @param btree
     *            The tree to which the node belongs.
     * @param addr
     *            The persistent identity of the node.
     * @param data
     *            The data record.
     */
    @SuppressWarnings("unchecked")
    protected Node(final AbstractBTree btree, final long addr,
            final INodeData data) {

        super(btree, false /* The node is NOT dirty */);

        final int branchingFactor = btree.branchingFactor;

        // assert branchingFactor >= Options.MIN_BRANCHING_FACTOR;
        //        
        // assert nkeys < branchingFactor;

        assert data != null;

        // assert childAddr.length == branchingFactor + 1;
        //
        // assert childEntryCounts.length == branchingFactor + 1;

        setIdentity(addr);

        this.data = data;

        // this.nentries = nentries;
        //        
        // // this.nkeys = keys.size();
        //        
        // this.keys = keys; // steal reference.
        //        
        // this.childAddr = childAddr;
        //
        // this.childEntryCounts = childEntryCounts;

        childRefs = new Reference[branchingFactor + 1];

        // childLocks = newChildLocks(btree, data.getKeys().size());

        // // must clear the dirty flag since we just de-serialized this node.
        // setDirty(false);

    }

    /**
     * Used to create a new node when a node is split.
     */
    @SuppressWarnings("unchecked")
    protected Node(final BTree btree) {

        super(btree, true /* dirty */);

        final int branchingFactor = btree.branchingFactor;

        data = new MutableNodeData(branchingFactor, btree.getIndexMetadata()
                .getVersionTimestamps());

        childRefs = new Reference[branchingFactor + 1];

        // childLocks = newChildLocks(btree, 0/* nkeys */);

        // nentries = 0;
        //
        // keys = new MutableKeyBuffer(branchingFactor);
        //
        // childAddr = new long[branchingFactor + 1];
        //
        // childEntryCounts = new int[branchingFactor + 1];

    }

    /**
     * This constructor is used when splitting the a root {@link Leaf} or a root
     * {@link Node}. The resulting node has a single child reference and NO
     * keys. The #of entries allocated to the child is the #of remaining in that
     * child <em>after</em> the split.
     * 
     * @param btree
     *            A mutable btree.
     * @param oldRoot
     *            The node that was previously the root of the tree (either a
     *            node or a leaf).
     * @param nentries
     *            The #of entries spanned by the oldRoot <em>before</em> the
     *            split.
     */
    @SuppressWarnings("unchecked")
    protected Node(final BTree btree, final AbstractNode oldRoot,
            final int nentries) {

        super(btree, true /* dirty */);

        // Verify that this is the root.
        assert oldRoot == btree.root;

        // The old root must be dirty when it is being split.
        assert oldRoot.isDirty();

        final int branchingFactor = btree.branchingFactor;

        childRefs = new Reference[branchingFactor + 1];

        // Note: child locks are only for read-only btrees and this ctor is only
        // for a split of the root leaf so we never use child locks for this
        // case.
        assert !btree.isReadOnly();
        // childLocks = null; // newChildLocks(btree);

        final MutableNodeData data;
        this.data = data = new MutableNodeData(branchingFactor, btree
                .getIndexMetadata().getVersionTimestamps());

        // #of entries spanned by the old root _before_ this split.
        data.nentries = nentries;

        // keys = new MutableKeyBuffer(branchingFactor);
        //
        // childAddr = new long[branchingFactor + 1];
        //
        // childEntryCounts = new int[branchingFactor + 1];

        /*
         * Replace the root node on the tree.
         */

        final boolean wasDirty = btree.root.dirty;

        btree.root = this;

        if (!wasDirty) {

            btree.fireDirtyEvent();

        }

        /*
         * Attach the old root to this node.
         */

        childRefs[0] = oldRoot.self;
        // childRefs[0] = btree.newRef(oldRoot);

        // #of entries from the old root _after_ the split.
        data.childEntryCounts[0] = oldRoot.getSpannedTupleCount();

        // dirtyChildren.add(oldRoot);

        oldRoot.parent = this.self;
        // oldRoot.parent = btree.newRef(this);

        /*
         * The tree is deeper since we just split the root node.
         */
        btree.height++;

        final int requiredQueueCapacity = 2 * (btree.height + 2);

        if (requiredQueueCapacity > btree.writeRetentionQueue.capacity()) {

            /*
             * FIXME Automatically extend the hard reference queue capacity such
             * that (a) this constraint described below is never violated; and
             * (b) the percentage of distinct nodes (or nodes and leaves) on the
             * queue compared to the nodes (or nodes and leaves) in the tree
             * either remains constant or does not degrade overly quickly.
             * 
             * Constraint: The capacity of the hard reference queue needs to be
             * at least the height of the tree + 2 (or twice that if we touch
             * nodes on the way down and then on the way up again) so that a
             * split can not cause any dirty node in the path to the leaf or its
             * sibling to be evicted while we are processing that split. Note
             * that there is no chance of the nodes being swept by the JVM since
             * there are hard references to them on the stack, but if the are
             * evicted then the copy-on-write mechanism could be defeated since
             * a dirty parent could be evicted, forcing the child to become
             * immutable right when we are trying to operate on it.
             * 
             * Performance: Given a constant capacity for the hard reference
             * queue the percentage of distinct nodes on the queue out of the
             * total #of nodes in the tree will drop as the tree grows. This
             * translates directly into more (de-)serialization of nodes and
             * more disk seeks (if the store is not fully buffered).
             * 
             * However, simply increasing the queue capacity will cause more
             * data to be buffered pending serialization and therefore will
             * increase the commit latency. Instead, we should probably
             * introduce a secondary hard reference retention mechanism based
             * more directly on the #of nodes that we want to retain in memory.
             * One approach is to say N-1 or N-2 levels of the tree, and that
             * might be a good heuristic. However, the select of the specific
             * nodes that are retained should probably be somewhat dynamic so
             * that we do not force the JVM to hold onto references for nodes
             * that we are never or only rarely visiting given the actual access
             * patterns in the application.
             */

            throw new UnsupportedOperationException(
                    "writeRetentionQueue: capacity="
                            + btree.writeRetentionQueue.capacity()
                            + ", but height=" + btree.height);

        }

        btree.nnodes++;

        btree.getBtreeCounters().rootsSplit++;

        if (BTree.log.isInfoEnabled() || BigdataStatics.debug) {

            // Note: nnodes and nleaves might not reflect rightSibling yet.

            final String msg = "BTree: increasing height: name="
                    + btree.metadata.getName() + ", height=" + btree.height
                    + ", m=" + btree.getBranchingFactor() + ", nentries="
                    + btree.nentries;

            if (BTree.log.isInfoEnabled())
                BTree.log.info(msg);

            if (BigdataStatics.debug)
                System.err.println(msg);

        }

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
    protected Node(final Node src, final long triggeredByChildId) {

        super(src);

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
        assert src.data != null;
        this.data = src.isReadOnly() ? new MutableNodeData(src
                .getBranchingFactor(), src.data) : src.data;
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

        final int nkeys = data.getKeyCount();

        for (int i = 0; i <= nkeys; i++) {

            final AbstractNode child = childRefs[i] == null ? null
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
             * some other places for the transient store.
             */
            if (child != null
                    && (btree.store == null || child.identity != triggeredByChildId)) {

                /*
                 * Copy on write should never trigger for a dirty node and only
                 * a dirty node can have dirty children.
                 */
                assert !child.isDirty();

                // Steal the child.
                child.parent = this.self;
                // child.parent = btree.newRef(this);

                // // Keep a reference to the clean child.
                // childRefs[i] = new WeakReference<AbstractNode>(child);

            }

        }

    }

    @Override
    public void delete() {

        super.delete();

        // clear state.
        childRefs = null;
        // childLocks = null;
        data = null;

    }

    /**
     * This method must be invoked on a parent to notify the parent that the
     * child has become persistent. The method scans the weak references for the
     * children, finds the index for the specified child, and then sets the
     * corresponding index in the array of child keys. The child is then removed
     * from the dirty list for this node.
     * 
     * @param child
     *            The child.
     * 
     * @exception IllegalStateException
     *                if the child is not persistent.
     * @exception IllegalArgumentException
     *                if the child is not a child of this node.
     */
    void setChildAddr(final AbstractNode<?> child) {

        if (!child.isPersistent()) {

            // The child does not have persistent identity.
            throw new IllegalStateException();

        }

        final int i = getIndexOf(child);

        assert !isReadOnly();

        ((MutableNodeData) data).childAddr[i] = child.getIdentity();

        // if (!dirtyChildren.remove(child)) {
        //
        // throw new AssertionError("Child was not on dirty list.");
        //
        // }

    }

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
    void replaceChildRef(final long oldChildAddr, final AbstractNode newChild) {

        assert oldChildAddr != NULL || btree.store == null;
        assert newChild != null;

        // This node MUST have been cloned as a pre-condition, so it can not
        // be persistent.
        assert !isPersistent();
        assert !isReadOnly();

        // The newChild MUST have been cloned and therefore MUST NOT be
        // persistent.
        assert !newChild.isPersistent();

        assert !isReadOnly();

        final MutableNodeData data = (MutableNodeData) this.data;

        final int nkeys = getKeyCount();

        // Scan for location in weak references.
        for (int i = 0; i <= nkeys; i++) {

            if (data.childAddr[i] == oldChildAddr) {

                /*
                 * Note: We can not check anything which depends on
                 * oldChild.data since that field was cleared when we cloned the
                 * oldChild to obtain a mutable node/leaf. Since
                 * oldChild.isPersistent() does not capture the correct
                 * semantics for a transient B+Tree (we are interested if the
                 * node was read-only), this entire section has been commented
                 * out.
                 */
                // if (true) {
                //
                // /*
                // * Do some paranoia checks.
                // */
                //
                // final AbstractNode oldChild = childRefs[i] != null ?
                // childRefs[i]
                // .get() : null;
                //
                // if (oldChild != null) {
                //
                // // assert oldChild.isPersistent();
                //
                // // assert !dirtyChildren.contains(oldChild);
                //
                // }
                //
                // }

                // Clear the old key.
                data.childAddr[i] = NULL;

                if (btree.storeCache!=null) {
                    // remove from cache.
                	btree.storeCache.remove(oldChildAddr);
                }
                // free the oldChildAddr if the Strategy supports it
                btree.store.delete(oldChildAddr);
                // System.out.println("Deleting " + oldChildAddr);

                // Stash reference to the new child.
                // childRefs[i] = btree.newRef(newChild);
                childRefs[i] = newChild.self;

                // // Add the new child to the dirty list.
                // dirtyChildren.add(newChild);

                // Set the parent on the new child.
                // newChild.parent = btree.newRef(this);
                newChild.parent = this.self;

                return;

            }

        }

        // System.err.println("this: "); dump(Level.DEBUG,System.err);
        // System.err.println("newChild: ");
        // newChild.dump(Level.DEBUG,System.err);
        throw new IllegalArgumentException("Not our child : oldChildAddr="
                + oldChildAddr);

    }

    @Override
    public Tuple insert(final byte[] key, final byte[] value,
            final boolean delete, final long timestamp, final Tuple tuple) {

        assert !deleted;

        if (btree.debug)
            assertInvariants();

        btree.touch(this);

        final int childIndex = findChild(key);

        final AbstractNode<?> child = getChild(childIndex);

        return child.insert(key, value, delete, timestamp, tuple);

    }

    @Override
    public Tuple lookup(final byte[] key, final Tuple tuple) {

        assert !deleted;

        if (btree.debug)
            assertInvariants();

        btree.touch(this);

        final int childIndex = findChild(key);

        final AbstractNode<?> child = getChild(childIndex);

        return child.lookup(key, tuple);

    }

    @Override
    public Tuple remove(final byte[] key, final Tuple tuple) {

        assert !deleted;

        if (btree.debug)
            assertInvariants();

        btree.touch(this);

        final int childIndex = findChild(key);

        final AbstractNode<?> child = getChild(childIndex);

        return child.remove(key, tuple);

    }

    /**
     * {@inheritDoc}
     * @todo convert to batch api and handle searchKeyOffset.
     */
    @Override
    public int indexOf(final byte[] key) {

        assert !deleted;

        btree.touch(this);

        final int childIndex = findChild(key);

        final AbstractNode<?> child = getChild(childIndex);

        /*
         * Compute running total to this child index plus [n], possible iff
         * successful search at the key level in which case we do not need to
         * pass n down.
         */

        int offset = 0;

        for (int i = 0; i < childIndex; i++) {

            offset += getChildEntryCount(i);

        }

        // recursive invocation, eventually grounds out on a leaf.
        int ret = child.indexOf(key);

        if (ret < 0) {

            // obtain "insert position".
            ret = -ret - 1;

            // add in the offset.
            ret += offset;

            // convert back to the "not found" form.
            return (-(ret) - 1);

        }

        // add in the offset.
        ret += offset;

        // return the index position of the key relative to this node.
        return ret;

    }

    /**
     * Range check an index into the keys of the node.
     * 
     * @param entryIndex
     *            The key index.
     * 
     * @return <code>true</code>
     * 
     * @throws IndexOutOfBoundsException
     *             if the index is LT ZERO (0) -or- GTE the
     *             {@link #getSpannedTupleCount()}
     */
    protected boolean rangeCheckSpannedTupleIndex(final int entryIndex) {

        final int nentries = data.getSpannedTupleCount();

        if (entryIndex < 0)
            throw new IndexOutOfBoundsException("negative: " + entryIndex);

        if (entryIndex >= nentries) {

            throw new IndexOutOfBoundsException("too large: entryIndex="
                    + entryIndex + ", but nentries=" + nentries);

        }

        return true;

    }

    /**
     * Recursive search for the key at the specified entry index.
     * 
     * @param entryIndex
     *            The index of the entry (relative to the first entry spanned by
     *            this node).
     * 
     * @return The key at that entry index.
     */
    @Override
    final public byte[] keyAt(final int entryIndex) {

        /* assert */rangeCheckSpannedTupleIndex(entryIndex);

        // index of the child that spans the desired entry.
        int childIndex = 0;

        // corrects the #of spanned entries by #skipped over.
        int remaining = entryIndex;

        final int nkeys = getKeyCount();

        // search for child spanning the desired entry index.
        for (; childIndex <= nkeys; childIndex++) {

            final int nspanned = getChildEntryCount(childIndex);

            if (remaining < nspanned) {

                // found the child index spanning the desired entry.
                break;

            }

            remaining -= nspanned;

            assert remaining >= 0;

        }

        final AbstractNode<?> child = getChild(childIndex);

        return child.keyAt(remaining);

    }

    /**
     * Recursive search for the value at the specified entry index.
     * 
     * @param entryIndex
     *            The index of the entry (relative to the first entry spanned by
     *            this node).
     * 
     * @return The value at that entry index.
     */
    @Override
    final public void valueAt(final int entryIndex, final Tuple tuple) {

        // Note: Made non-conditional since unit tests verify this.
        /* assert */rangeCheckSpannedTupleIndex(entryIndex);

        // index of the child that spans the desired entry.
        int childIndex = 0;

        // corrects the #of spanned entries by #skipped over.
        int remaining = entryIndex;

        final int nkeys = getKeyCount();

        // search for child spanning the desired entry index.
        for (; childIndex <= nkeys; childIndex++) {

            final int nspanned = getChildEntryCount(childIndex);

            if (remaining < nspanned) {

                // found the child index spanning the desired entry.
                break;

            }

            remaining -= nspanned;

            assert remaining >= 0;

        }

        final AbstractNode<?> child = getChild(childIndex);

        child.valueAt(remaining, tuple);

    }

    /**
     * Return the index of the child to be searched.
     * <p>
     * The interpretation of the key index for a node is as follows. When
     * searching nodes of the tree, we search for the index in keys[] of the
     * first key value greater than or equal (GTE) to the probe key. If the
     * match is equal, then we choose the child at index + 1. Otherwise we
     * choose the child having the same index as the GTE key match. For example,
     * 
     * <pre>
     * keys[]  : [ 5 9 12 ]
     * child[] : [ a b  c  d ]
     * </pre>
     * 
     * A probe with keys up to <code>4</code> matches at index zero (0) and we
     * choose the 1st child, a, which is at index zero (0).
     * <p>
     * A probe whose key is <code>5</code> matches at index zero (0) exactly and
     * we choose the child at <code>index + 1</code>, b, which is at index one
     * (1).
     * <p>
     * A probe with keys in [6:8] matches at index one (1) and we choose the 2nd
     * child, b, which is at index one (1). A probe with <code>9</code> also
     * matches at index one (1), but we choose <code>index+1</code> equals two
     * (2) since this is an exact key match.
     * <p>
     * A probe with keys in [10:11] matches at index two (2) and we choose the
     * 3rd child, c, which is at index two (2). A probe with <code>12</code>
     * also matches at index two (2), but we choose <code>index+1</code> equals
     * three (3) since this is an exact key match.
     * <p>
     * A probe with keys greater than 12 exceeds all keys in the node and always
     * matches the last child in that node. In this case, d, which is at index
     * three (3).
     * <p>
     * Note that we never stop a search on a node, even when there is an exact
     * match on a key. All values are stored in the leaves and we always descend
     * until we reach the leaf in which a value for the key would be stored. A
     * test on the keys of that leaf is then conclusive - either a value is
     * stored in the leaf for that key or it is not stored in the tree.
     * 
     * @param searchkey
     *            The probe key.
     * 
     * @return The child to be searched next for that key.
     */
    final protected int findChild(final byte[] searchKey) {

        int childIndex = this.getKeys().search(searchKey);

        if (childIndex >= 0) {

            /*
             * exact match - use the next child.
             */

            return childIndex + 1;

        } else {

            /*
             * There is no exact match on the key, so we convert the search
             * result to find the insertion point. The insertion point is always
             * an index whose current key (iff it is defined) is greater than
             * the probe key.
             * 
             * keys[] : [ 5 9 12 ]
             * 
             * The insertion point for key == 4 is zero.
             * 
             * The insertion point for key == 6 is one.
             * 
             * etc.
             * 
             * When the probe key is greater than any existing key, then the
             * insertion point is nkeys. E.g., the insertion point for key == 20
             * is 3.
             */

            /*
             * Convert the return by search to obtain the index of the child
             * that covers this key (a non-negative integer).
             */
            childIndex = -childIndex - 1;

            return childIndex;

        }

    }

    /**
     * <p>
     * Split an over-capacity node (a node with <code>maxKeys+1</code> keys),
     * creating a new rightSibling. The splitIndex is <code>(maxKeys+1)/2</code>
     * . The key at the splitIndex is the separatorKey. Unlike when we split a
     * {@link Leaf}, the separatorKey is lifted into the parent and does not
     * appear in either this node or the rightSibling after the split. All keys
     * and child references from <code>splitIndex+1</code> (inclusive) are moved
     * to the new rightSibling. The child reference at <code>splitIndex</code>
     * remains in this node.
     * </p>
     * <p>
     * If this node is the root of the tree (no parent), then a new root
     * {@link Node} is created without any keys and is made the parent of this
     * node.
     * </p>
     * <p>
     * In any case, we then insert( separatorKey, rightSibling ) into the parent
     * node, which may cause the parent node itself to split.
     * </p>
     * <p>
     * Note: splitting a node causes entry counts for the relocated childen to
     * be relocated to the new rightSibling but it does NOT change the #of
     * entries on the parent.
     * </p>
     */
    @Override
    protected IAbstractNode split() {

        assert isDirty(); // MUST be mutable.
        assert getKeyCount() == maxKeys() + 1; // MUST be over capacity by one.

        // cast to mutable implementation class.
        final BTree btree = (BTree) this.btree;

        btree.getBtreeCounters().nodesSplit++;

        /*
         * The #of entries spanned by this node _before_ the split.
         */
        final int nentriesBeforeSplit = getSpannedTupleCount();

        /*
         * The #of child references. (this is +1 since the node is over capacity
         * by one).
         */
        final int nchildren = btree.branchingFactor + 1;

        /*
         * Index at which to split the leaf. This is (maxKeys+1)/2, but that can
         * be simplified to branchingFactor/2 for a Node.
         */
        // final int splitIndex = (maxKeys() + 1) / 2;
        final int splitIndex = btree.branchingFactor >>> 1;

        /*
         * The key at that index, which becomes the separator key in the parent.
         * 
         * Note: Unlike a leaf, we are not free to choose the shortest separator
         * key in a node. This is because separator keys describe separations
         * among the leaves of the tree. This issue is covered by Bayer's
         * article on prefix trees.
         */
        final byte[] separatorKey = getKeys().get(splitIndex);

        // Create the new rightSibling node. It will be mutable.
        final Node rightSibling = new Node(btree);

        // Tunnel through to the mutable objects.
        final MutableNodeData data = (MutableNodeData) this.data;
        final MutableNodeData sdata = (MutableNodeData) rightSibling.data;
        final MutableKeyBuffer keys = data.keys;
        final MutableKeyBuffer skeys = sdata.keys;

        if (INFO) {
            log.info("this=" + this + ", nkeys=" + getKeyCount()
                    + ", splitIndex=" + splitIndex + ", separatorKey="
                    + keyAsString(separatorKey));
            // if(DEBUG) dump(Level.DEBUG,System.err);
        }

        /*
         * copy keys and values to the new rightSibling.
         */

        int j = 0;

        // // #of spanned entries being moved to the new rightSibling.
        // int nentriesMoved = 0;

        for (int i = splitIndex + 1; i < nchildren; i++, j++) {

            if (i + 1 < nchildren) {

                /*
                 * Note: keys[nchildren-1] is undefined.
                 */
                // rightSibling.setKey(j, getKey(i));
                rightSibling.copyKey(j, this.getKeys(), i);

            }

            rightSibling.childRefs[j] = childRefs[i];

            sdata.childAddr[j] = data.childAddr[i];

            final int childEntryCount = data.childEntryCounts[i];

            sdata.childEntryCounts[j] = childEntryCount;

            sdata.nentries += childEntryCount;

            data.nentries -= childEntryCount;

            // nentriesMoved += childEntryCounts[i];

            final AbstractNode tmp = (childRefs[i] == null ? null
                    : childRefs[i].get());

            if (tmp != null) {

                /*
                 * The child node is in memory.
                 * 
                 * Update its parent reference.
                 */

                // tmp.parent = btree.newRef(rightSibling);
                tmp.parent = rightSibling.self;

            }

            /*
             * Clear out the old keys and values, including keys[splitIndex]
             * which is being moved to the parent.
             */

            if (i + 1 < nchildren) {

                keys.keys[i] = null;

                /* nkeys--; */keys.nkeys--; // one less key here.

                /* rightSibling.nkeys++; */skeys.nkeys++; // more more key
                                                          // there.

            }

            childRefs[i] = null;

            data.childAddr[i] = NULL;

            data.childEntryCounts[i] = 0;

        }

        /*
         * Clear the key that is being move into the parent.
         */
        keys.keys[splitIndex] = null;

        /* nkeys--; */keys.nkeys--;

        Node p = getParent();

        if (p == null) {

            /*
             * Use a special constructor to split the root. The result is a new
             * node with zero keys and one child (this node).
             */

            p = new Node(btree, this, nentriesBeforeSplit);

        } else {

            assert !p.isReadOnly();

            // this node now has fewer entries
            ((MutableNodeData) p.data).childEntryCounts[p.getIndexOf(this)] -= sdata.nentries;

        }

        /*
         * insert(separatorKey,rightSibling) into the parent node. This may
         * cause the parent node itself to split.
         */
        p.insertChild(separatorKey, rightSibling);

        btree.nnodes++;

        // Return the high node.
        return rightSibling;

    }

    /**
     * Redistributes a key from the specified sibling into this node in order to
     * bring this node up to the minimum #of keys. This also updates a separator
     * key in the parent for the right most of (this, sibling).
     * 
     * When a key is redistributed from a sibling, the key in the sibling is
     * rotated into the parent where it replaces the current separatorKey and
     * that separatorKey is brought down into this node. The child corresponding
     * to the key is simply moved from the sibling into this node (rather than
     * rotating it through the parent).
     * 
     * While redistribution changes the #of entries spanned by the node and the
     * sibling and therefore must update {@link #childEntryCounts} on the shared
     * parent, it does not change the #of entries spanned by the parent.
     * 
     * @param sibling
     *            A direct sibling of this node (either the left or right
     *            sibling). The sibling MUST be mutable.
     * 
     * @todo change to redistribute keys until the node and the sibling have an
     *       equal number of keys. if the other sibling exists and is also
     *       materialized then redistribute keys with it as well? This is the
     *       B*-Tree variation - it has strengths and weaknesses. I do not think
     *       that it will be a big win here since the expected scenario is heavy
     *       writes, good retention of nodes, and de-serialization of nodes from
     *       a fully buffered journal (hence, no IOs even when we need to
     *       materialize a sibling).
     */
    @Override
    protected void redistributeKeys(final AbstractNode sibling,
            final boolean isRightSibling) {

        // the sibling of a Node must be a Node.
        final Node s = (Node) sibling;
        assert s != null;

        final int nkeys = getKeyCount();
        final int snkeys = s.getKeyCount();

        assert dirty;
        assert !deleted;
        assert !isPersistent();
        // verify that this leaf is deficient.
        assert nkeys < minKeys();
        // verify that this leaf is under minimum capacity by one key.
        assert nkeys == minKeys() - 1;

        // the sibling MUST be _OVER_ the minimum #of keys/values.
        assert snkeys > minKeys();
        assert s.dirty;
        assert !s.deleted;
        assert !s.isPersistent();

        final Node p = getParent();

        // children of the same node.
        assert s.getParent() == p;

        if (INFO) {
            log.info("this=" + this + ", sibling=" + sibling
                    + ", rightSibling=" + isRightSibling);
            // if(DEBUG) {
            // System.err.println("this"); dump(Level.DEBUG,System.err);
            // System.err.println("sibling");
            // sibling.dump(Level.DEBUG,System.err);
            // System.err.println("parent"); p.dump(Level.DEBUG,System.err);
            // }
        }

        /*
         * The index of this leaf in its parent. we note this before we start
         * mucking with the keys.
         */
        final int index = p.getIndexOf(this);

        // Tunnel through to the mutable keys object.
        final MutableKeyBuffer keys = (MutableKeyBuffer) this.getKeys();
        final MutableKeyBuffer skeys = (MutableKeyBuffer) s.getKeys();

        // Tunnel through to the mutable data records.
        final MutableNodeData data = (MutableNodeData) this.data;
        final MutableNodeData sdata = (MutableNodeData) s.data;
        final MutableNodeData pdata = (MutableNodeData) p.data;

        /*
         * determine which leaf is earlier in the key ordering and get the index
         * of the sibling.
         */
        if (isRightSibling/* keys[nkeys-1] < s.keys[0] */) {

            /*
             * redistributeKeys(this,rightSibling). all we have to do is replace
             * the separatorKey in the parent with the first key from the
             * rightSibling and copy the old separatorKey from the parent to the
             * end of the keys in this node. we then close up the hole that this
             * left at index 0 in the rightSibling.
             */

            // Mopy the first key/child from the rightSibling.
            // setKey(nkeys, p.getKey(index)); // copy the separatorKey from the
            // parent.
            copyKey(nkeys, p.getKeys(), index); // copy the separatorKey from
                                                // the parent.
            // p.setKey(index, s.getKey(0)); // update the separatorKey from the
            // rightSibling.
            p.copyKey(index, s.getKeys(), 0); // update the separatorKey from
                                              // the rightSibling.
            childRefs[nkeys + 1] = s.childRefs[0]; // copy the child from the
                                                   // rightSibling.
            data.childAddr[nkeys + 1] = sdata.childAddr[0];
            final int siblingChildCount = sdata.childEntryCounts[0]; // #of
                                                                     // spanned
                                                                     // entries
                                                                     // being
                                                                     // moved.
            data.childEntryCounts[nkeys + 1] = siblingChildCount;
            final AbstractNode child = childRefs[nkeys + 1] == null ? null
                    : childRefs[nkeys + 1].get();
            if (child != null) {
                child.parent = this.self;
                // child.parent = btree.newRef(this);
                // if( child.isDirty() ) {
                // if(!s.dirtyChildren.remove(child)) throw new
                // AssertionError();
                // if(!dirtyChildren.add(child)) throw new AssertionError();
                // }
            }

            // copy down the keys on the right sibling to cover up the hole.
            System.arraycopy(skeys.keys, 1, skeys.keys, 0, snkeys - 1);
            System.arraycopy(s.childRefs, 1, s.childRefs, 0, snkeys);
            System.arraycopy(sdata.childAddr, 1, sdata.childAddr, 0, snkeys);
            System.arraycopy(sdata.childEntryCounts, 1, sdata.childEntryCounts,
                    0, snkeys);

            // erase exposed key/value on rightSibling that is no longer
            // defined.
            skeys.keys[snkeys - 1] = null;
            s.childRefs[snkeys] = null;
            sdata.childAddr[snkeys] = NULL;
            sdata.childEntryCounts[snkeys] = 0;

            // update parent : N more entries spanned by this child.
            pdata.childEntryCounts[index] += siblingChildCount;
            // update parent : N fewer entries spanned by our right sibling.
            pdata.childEntryCounts[index + 1] -= siblingChildCount;

            // update #of entries spanned by this node.
            data.nentries += siblingChildCount;
            // update #of entries spanned by our rightSibling.
            sdata.nentries -= siblingChildCount;

            /* s.nkeys--; */skeys.nkeys--;
            /* this.nkeys++; */keys.nkeys++;

            if (btree.debug) {
                assertInvariants();
                s.assertInvariants();
            }

        } else {

            /*
             * redistributeKeys(leftSibling,this). all we have to do is copy
             * down the keys in this node by one position, copy the separatorKey
             * from the parent into the hole that we just opened up, copy the
             * child data across, and update the separatorKey in the parent with
             * the last key from the leftSibling.
             */

            // copy down by one.
            System.arraycopy(keys.keys, 0, keys.keys, 1, nkeys);
            System.arraycopy(childRefs, 0, childRefs, 1, nkeys + 1);
            System.arraycopy(data.childAddr, 0, data.childAddr, 1, nkeys + 1);
            System.arraycopy(data.childEntryCounts, 0, data.childEntryCounts,
                    1, nkeys + 1);

            // move the last key/child from the leftSibling to this node.
            // setKey(0, p.getKey(index-1)); // copy the separatorKey from the
            // parent.
            copyKey(0, p.getKeys(), index - 1); // copy the separatorKey from
                                                // the parent.
            // p.setKey(index-1, s.getKey(s.nkeys-1)); // update the
            // separatorKey
            p.copyKey(index - 1, s.getKeys(), snkeys - 1); // update the
                                                           // separatorKey
            childRefs[0] = s.childRefs[snkeys];
            data.childAddr[0] = sdata.childAddr[snkeys];
            final int siblingChildCount = sdata.childEntryCounts[snkeys];
            data.childEntryCounts[0] = siblingChildCount;
            final AbstractNode child = childRefs[0] == null ? null
                    : childRefs[0].get();
            if (child != null) {
                child.parent = this.self;
                // child.parent = btree.newRef(this);
                // if(child.isDirty()) {
                // if(!s.dirtyChildren.remove(child)) throw new
                // AssertionError();
                // if(!dirtyChildren.add(child)) throw new AssertionError();
                // }
            }
            skeys.keys[snkeys - 1] = null;
            s.childRefs[snkeys] = null;
            sdata.childAddr[snkeys] = NULL;
            sdata.childEntryCounts[snkeys] = 0;
            /* s.nkeys--; */skeys.nkeys--;
            /* this.nkeys++; */keys.nkeys++;

            // update parent : N more entries spanned by this child.
            pdata.childEntryCounts[index] += siblingChildCount;
            // update parent : N fewer entries spanned by our leftSibling.
            pdata.childEntryCounts[index - 1] -= siblingChildCount;
            // update #of entries spanned by this node.
            data.nentries += siblingChildCount;
            // update #of entries spanned by our leftSibling.
            sdata.nentries -= siblingChildCount;

            if (btree.debug) {
                assertInvariants();
                s.assertInvariants();
            }

        }

    }

    /**
     * Merge the keys and values from the sibling into this node, delete the
     * sibling from the store and remove the sibling from the parent. This will
     * trigger recursive {@link AbstractNode#join()} if the parent node is now
     * deficient. While this changes the #of entries spanned by the current node
     * it does NOT effect the #of entries spanned by the parent.
     * 
     * @param sibling
     *            A direct sibling of this node (does NOT need to be mutable).
     *            The sibling MUST have exactly the minimum #of keys.
     */
    @Override
    protected void merge(final AbstractNode sibling,
            final boolean isRightSibling) {

        // The sibling of a Node must be a Node.
        final Node s = (Node) sibling;
        assert s != null;
        assert !s.deleted;

        // Note: local var is updated within this method!
        int nkeys = getKeyCount();

        final int snkeys = s.getKeyCount();

        // verify that this node is deficient.
        assert nkeys < minKeys();
        // verify that this node is under minimum capacity by one key.
        assert nkeys == minKeys() - 1;
        // the sibling MUST at the minimum #of keys/values.
        assert snkeys == s.minKeys();

        final Node p = getParent();

        // children of the same node.
        assert s.getParent() == p;

        if (INFO) {
            log.info("this=" + this + ", sibling=" + sibling
                    + ", rightSibling=" + isRightSibling);
            // if(DEBUG) {
            // System.err.println("this"); dump(Level.DEBUG,System.err);
            // System.err.println("sibling");
            // sibling.dump(Level.DEBUG,System.err);
            // System.err.println("parent"); p.dump(Level.DEBUG,System.err);
            // }
        }

        final int siblingEntryCount = s.getSpannedTupleCount();

        /*
         * The index of this node in its parent. we note this before we start
         * mucking with the keys.
         */
        final int index = p.getIndexOf(this);

        /*
         * Tunnel through to the mutable data records.
         * 
         * Note: We do not require the sibling to be mutable. If it is not, then
         * we create a mutable copy of the sibling for use during this method.
         */
        final MutableNodeData data = (MutableNodeData) this.data;
        final MutableNodeData sdata = s.isReadOnly() ? new MutableNodeData(
                getBranchingFactor(), s.data) : (MutableNodeData) s.data;
        final MutableNodeData pdata = (MutableNodeData) p.data;

        // Tunnel through to the mutable keys objects.
        final MutableKeyBuffer keys = data.keys;
        final MutableKeyBuffer skeys = sdata.keys;

        // /*
        // * Tunnel through to the mutable keys object.
        // *
        // * Note: since we do not require the sibling to be mutable we have to
        // * test and convert the key buffer for the sibling to a mutable key
        // * buffer if the sibling is immutable. Also note that the sibling MUST
        // * have the minimum #of keys for a merge so we set the capacity of the
        // * mutable key buffer to that when we have to convert the siblings
        // keys
        // * into mutable form in order to perform the merge operation.
        // */
        // final MutableKeyBuffer keys = (MutableKeyBuffer) this.getKeys();
        // final MutableKeyBuffer skeys = (s.getKeys() instanceof
        // MutableKeyBuffer ? (MutableKeyBuffer) s.getKeys()
        // : new MutableKeyBuffer(getBranchingFactor(), s.getKeys()));

        /*
         * determine which node is earlier in the key ordering so that we know
         * whether the sibling's keys will be inserted at the front of this
         * nodes's keys or appended to this nodes's keys.
         */
        if (isRightSibling/* keys[nkeys-1] < s.keys[0] */) {

            /*
             * merge( this, rightSibling ). the keys and values from this node
             * will appear in their current position, the separatorKey from the
             * parent is appended after the last key in this node, and the keys
             * and children from the rightSibling are then appended as well.
             */

            /*
             * Get the separator key in the parent and append it to the keys in
             * this node.
             */
            // this.setKey(nkeys++, p.getKey(index));
            this.copyKey(nkeys, p.getKeys(), index);
            nkeys++; // update local var!
            keys.nkeys++;

            /*
             * Copy in the keys and children from the sibling. Note that the
             * children are copied to the position nkeys NOT nkeys+1 since the
             * first child needs to appear at the same position as the
             * separatorKey that we copied from the parent.
             */
            System.arraycopy(skeys.keys, 0, keys.keys, nkeys, snkeys);
            System.arraycopy(s.childRefs, 0, this.childRefs, nkeys, snkeys + 1);
            System.arraycopy(sdata.childAddr, 0, data.childAddr, nkeys,
                    snkeys + 1);
            System.arraycopy(sdata.childEntryCounts, 0, data.childEntryCounts,
                    nkeys, snkeys + 1);

            // update parent on children
            final Reference<Node> ref = (Reference<Node>) this.self;
            // final Reference<Node> weakRef = btree.newRef(this);
            for (int i = 0; i < snkeys + 1; i++) {
                final AbstractNode child = s.childRefs[i] == null ? null
                        : s.childRefs[i].get();
                if (child != null) {
                    child.parent = ref;
                    // if( child.isDirty() ) {
                    // // record hard references for dirty children.
                    // dirtyChildren.add(child);
                    // }
                }
            }

            /*
             * Adjust the #of keys in this leaf.
             */
            /* this.nkeys += s.nkeys; */keys.nkeys += snkeys;
            /*
             * Note: in this case we have to replace the separator key for this
             * node with the separator key for its right sibling.
             * 
             * Note: This temporarily causes the duplication of a separator key
             * in the parent. However, the separator key for the right sibling
             * will be deleted when the sibling is removed from the parent
             * below.
             */
            // p.setKey(index, p.getKey(index+1));
            p.copyKey(index, p.getKeys(), index + 1);

            // reallocate spanned entries from the sibling to this node.
            pdata.childEntryCounts[index] += siblingEntryCount;
            data.nentries += siblingEntryCount;

            if (btree.debug)
                assertInvariants();

        } else {

            /*
             * merge( leftSibling, this ). the keys and values from this node
             * will be move down by sibling.nkeys+1 positions, the keys and
             * values from the sibling will be copied into this node starting at
             * index zero(0), and finally the separatorKey from the parent will
             * be copied into the position after the last sibling key and before
             * the position of the first key copied down in this node to avoid
             * overwrite (that is, we copy the separatorKey from the parent to
             * this.keys[s.nkeys]).
             * 
             * Note: we do not update the separator key in the parent because
             * the separatorKey will be removed when we remove the leftSibling
             * from the parent at the end of this method.
             */

            // move keys and children down by sibling.nkeys+1 positions.
            System.arraycopy(keys.keys, 0, keys.keys, snkeys + 1, nkeys);
            System.arraycopy(this.childRefs, 0, this.childRefs, snkeys + 1,
                    nkeys + 1);
            System.arraycopy(data.childAddr, 0, data.childAddr, snkeys + 1,
                    nkeys + 1);
            System.arraycopy(data.childEntryCounts, 0, data.childEntryCounts,
                    snkeys + 1, nkeys + 1);

            // copy keys and values from the sibling to index 0 of this leaf.
            System.arraycopy(skeys.keys, 0, keys.keys, 0, snkeys);
            System.arraycopy(s.childRefs, 0, this.childRefs, 0, snkeys + 1);
            System.arraycopy(sdata.childAddr, 0, data.childAddr, 0, snkeys + 1);
            System.arraycopy(sdata.childEntryCounts, 0, data.childEntryCounts,
                    0, snkeys + 1);

            // copy the separatorKey from the parent.
            // this.setKey(s.nkeys, p.getKey(index - 1));
            this.copyKey(snkeys, p.getKeys(), index - 1);

            // update parent on children.
            final Reference<Node> ref = (Reference<Node>) this.self;
            // final Reference<Node> weakRef = btree.newRef(this);
            for (int i = 0; i < snkeys + 1; i++) {
                final AbstractNode child = s.childRefs[i] == null ? null
                        : s.childRefs[i].get();
                if (child != null) {
                    child.parent = ref;
                    // if( child.isDirty() ) {
                    // // record hard references for dirty children.
                    // dirtyChildren.add(child);
                    // }
                }
            }

            // we gain nkeys from the sibling and one key from the parent.
            /* this.nkeys += s.nkeys + 1; */keys.nkeys += snkeys + 1;

            // reallocate spanned entries from the sibling to this node.
            pdata.childEntryCounts[index] += s.getSpannedTupleCount();
            data.nentries += siblingEntryCount;

            if (btree.debug)
                assertInvariants();

        }

        /*
         * The sibling is now empty. We need to detach the sibling from its
         * parent node and then delete the sibling from the store.
         */
        p.removeChild(s);

    }

    /**
     * Invoked by {@link AbstractNode#split()} to insert a key and reference for
     * a child created when another child of this node is split. This method has
     * no effect on the #of entries spanned by the parent. *
     * <p>
     * Note: This operation is invoked only when a node or leaf is split. As
     * such, it can not cause the min/max tuple revision timestamp on this
     * {@link Node} to change since no tuples have been added or removed.
     * However, this method does need to record the min/max for the new
     * rightSibling.
     * 
     * @param key
     *            The key on which the old node was split.
     * @param child
     *            The new node.
     * 
     *            FIXME set min/max for the new child.
     */
    protected void insertChild(final byte[] key, final AbstractNode child) {

        if (btree.debug)
            assertInvariants();

        // assert key > IIndex.NEGINF && key < IIndex.POSINF;
        assert child != null;
        assert child.isDirty() : "child not dirty"; // always dirty since it was
                                                    // just created.
        assert isDirty() : "not dirty"; // must be dirty to permit mutation.

        /*
         * Find the location where this key belongs. When a new node is created,
         * the constructor sticks the child that demanded the split into
         * childRef[0]. So, when nkeys == 1, we have nchildren==1 and the key
         * goes into keys[0] but we have to copyDown by one anyway to avoid
         * stepping on the existing child.
         */
        int childIndex = this.getKeys().search(key);

        if (childIndex >= 0) {

            /*
             * The key is already present. This is an error.
             */

            // btree.dump(Level.DEBUG,System.err);

            throw new AssertionError("Split on existing key: childIndex="
                    + childIndex + ", key=" + keyAsString(key) + "\nthis="
                    + this + "\nchild=" + child);

        }

        final int nkeys = getKeyCount();

        // Convert the position to obtain the insertion point.
        childIndex = -childIndex - 1;

        assert childIndex >= 0 && childIndex <= nkeys;

        /*
         * copy down per-key data.
         */
        assert !isReadOnly();
        final MutableNodeData data = (MutableNodeData) this.data;
        final MutableKeyBuffer keys = (MutableKeyBuffer) this.getKeys();

        final int length = nkeys - childIndex;

        if (length > 0) {

            System.arraycopy(keys.keys, childIndex, keys.keys,
                    (childIndex + 1), length);

        }

        /*
         * copy down per-child data. #children == nkeys+1. child[0] is always
         * defined.
         */
        System.arraycopy(childRefs, childIndex + 1, childRefs, childIndex + 2,
                length);
        System.arraycopy(data.childAddr, childIndex + 1, data.childAddr,
                childIndex + 2, length);
        System.arraycopy(data.childEntryCounts, childIndex + 1,
                data.childEntryCounts, childIndex + 2, length);

        /*
         * Insert key at index.
         */
        // setKey(childIndex, key);
        keys.keys[childIndex] = key;
        // System.arraycopy(key, 0, keys.keys, childIndex, 1);

        /*
         * Insert child at index+1.
         */
        childRefs[childIndex + 1] = child.self;
        // childRefs[childIndex + 1] = btree.newRef(child);

        data.childAddr[childIndex + 1] = NULL;

        final int childEntryCount = child.getSpannedTupleCount();

        data.childEntryCounts[childIndex + 1] = childEntryCount;

        // if( parent != null ) {
        //            
        // parent.get().updateEntryCount(this, childEntryCount);
        //            
        // }

        // nentries += childEntryCount;

        // dirtyChildren.add(child);

        child.parent = this.self;
        // child.parent = btree.newRef(this);

        /* nkeys++; */keys.nkeys++;

        // Note: this tests the post-condition of the split.
        if (keys.nkeys == maxKeys() + 1) {

            /*
             * The node is over capacity so we split the node, creating a new
             * rightSibling and insert( separatorKey, rightSibling ) into the
             * parent.
             */

            final Node rightSibling = (Node) split();

            // assert additional post-split invariants.
            if (btree.debug) {
                getParent().assertInvariants();
                rightSibling.assertInvariants();
            }

            return;

        }

        if (btree.debug)
            assertInvariants();

    }

    /**
     * Return the left sibling. This is used by implementations of
     * {@link AbstractNode#join()} to explore their left sibling.
     * 
     * @param child
     *            The child (must be dirty).
     * @param materialize
     *            When true, the left sibling will be materialized if it exists
     *            but is not resident.
     * 
     * @return The left sibling or <code>null</code> if it does not exist -or-
     *         if it is not materialized and <code>materialized == false</code>.
     *         If the sibling is returned, then is NOT guaranteed to be mutable
     *         and the caller MUST invoke copy-on-write before attempting to
     *         modify the returned sibling.
     */
    protected AbstractNode getLeftSibling(final AbstractNode child,
            final boolean materialize) {

        final int i = getIndexOf(child);

        if (i == 0) {

            /*
             * There is no left sibling for this child that is a child of the
             * same parent.
             */

            return null;

        } else {

            final int index = i - 1;

            AbstractNode sibling = childRefs[index] == null ? null
                    : childRefs[index].get();

            if (sibling == null) {

                if (materialize) {

                    sibling = getChild(index);

                }

            } else {

                btree.touch(sibling);

            }

            return sibling;

        }

    }

    /**
     * Return the right sibling of the specified child of a common parent. This
     * method is invoked on the parent, passing in one child and returning its
     * right sibling under that common parent (if any). This is used by
     * implementations of {@link AbstractNode#join()} to explore their right
     * sibling.
     * 
     * @param child
     *            The child (must be dirty).
     * @param materialize
     *            When true, the left sibling will be materialized if it exists
     *            but is not resident.
     * 
     * @return The left sibling or <code>null</code> if it does not exist -or-
     *         if it is not materialized and <code>materialized == false</code>.
     *         If the sibling is returned, then is NOT guaranteed to be mutable
     *         and the caller MUST invoke copy-on-write before attempting to
     *         modify the returned sibling.
     */
    protected AbstractNode getRightSibling(final AbstractNode child,
            final boolean materialize) {

        final int i = getIndexOf(child);

        if (i == getKeyCount()) {

            /*
             * There is no right sibling for this child that is a child of the
             * same parent.
             */

            return null;

        } else {

            final int index = i + 1;

            AbstractNode sibling = childRefs[index] == null ? null
                    : childRefs[index].get();

            if (sibling == null) {

                if (materialize) {

                    sibling = getChild(index);

                }

            } else {

                btree.touch(sibling);

            }

            return sibling;

        }

    }

    /**
     * Return the index of the child among the direct children of this node.
     * 
     * @param child
     *            The child.
     * 
     * @return The index in {@link #childRefs} where that child is found. This
     *         may also be used as an index into {@link #childAddr} and
     *         {@link #childEntryCounts}.
     * 
     * @exception IllegalArgumentException
     *                iff child is not a child of this node.
     */
    protected int getIndexOf(final AbstractNode child) {

        assert child != null;
        assert child.parent.get() == this;

        /*
         * Scan for location in weak references.
         * 
         * Note: during reads, this method is used for range counts. During
         * writes it is used to update the entry counts on which the range
         * counts are based.
         * 
         * @todo Can this be made more efficient by considering the last key on
         * the child and searching the parent for the index that must correspond
         * to that child? Note that when merging two children the keys in the
         * parent might not be coherent depending on exactly when this method is
         * called - for things to be coherent you would have to discover the
         * index of the children before modifying their keys.
         * 
         * @todo for writes, 85% of the use of this method is
         * updateEntryCount(). Since that method is only called on update, we
         * would do well to buffer hard references during descent and test the
         * buffer in this method before performing a full search. Since
         * concurrent writers are not allowed, we only need a single buffer
         * whose height is the height of the tree. This should prove especially
         * beneficial for larger branching factors. For smaller branching
         * factors the cost might be so small as to be ignorable.
         * 
         * @see Leaf#merge(Leaf sibling,boolean isRightSibling)
         */

        final int nkeys = getKeyCount();

        for (int i = 0; i <= nkeys; i++) {

            if (childRefs[i] != null && childRefs[i].get() == child) {

                return i;

            }

        }

        throw new IllegalArgumentException("Not our child : child=" + child);

    }

    /**
     * Invoked when a non-root node or leaf has no more keys to detach the child
     * from its parent. If the node becomes deficient, then the node is joined
     * with one of its immediate siblings. If the node is the root of the tree,
     * then the root of the tree is also updated. The child is deleted as a
     * post-condition.
     * 
     * @param child
     *            The child (does NOT need to be mutable).
     * 
     * @todo I am a bit suspicious of this method. it appears to be removing the
     *       key and child at the same index rather than the key at index-1 and
     *       the child at index. This interacts with how the separator key gets
     *       updated (or appears to get updated) when a child is removed. That
     *       logic occurs in {@link Leaf#merge(AbstractNode, boolean)} and in
     *       {@link Node#merge(AbstractNode, boolean)}. It may be that I can
     *       simplify things a bit further by making this adjustment here and in
     *       those merge() methods.
     * 
     *       FIXME This should clear the min/max for the child in this node's
     *       data record. This is invoked by merge() on a leaf, then recursively
     *       if we need to join nodes. The caller should have already updated
     *       the min/max for the leaf's own data record and on this node's data
     *       record for that leaf. This method only needs to clear the min/max
     *       entry associated with the child that is being removed.
     */
    protected void removeChild(final AbstractNode child) {

        assert child != null;
        assert !child.deleted;
        assert child.parent.get() == this;

        assert dirty;
        assert !deleted;
        assert !isPersistent();
        assert !isReadOnly();

        // cast to mutable implementation class.
        final BTree btree = (BTree) this.btree;

        if (btree.debug)
            assertInvariants();

        if (INFO) {
            log.info("this=" + this + ", child=" + child);
            /*
             * Note: dumping [this] or the [child] will throw false exceptions
             * at this point - they are in an intermediate state.
             */
            // if(DEBUG) {
            // System.err.println("this"); dump(Level.DEBUG,System.err);
            // System.err.println("child"); child.dump(Level.DEBUG,System.err);
            // }
        }

        final int i = getIndexOf(child);

        /*
         * Note: these comments may be dated and need review.
         * 
         * Copy over the hole created when the child is removed from the node.
         * 
         * Given: v-- remove @ index = 0 index: 0 1 2 root keys : [ 21 24 0 ]
         * index leaf1 keys : [ 1 2 7 - ] 0 <--remove @ index = 0 leaf2 keys : [
         * 21 22 23 - ] 1 leaf4 keys : [ 24 31 - - ] 2
         * 
         * This can also be represented as
         * 
         * ( leaf1, 21, leaf2, 24, leaf4 )
         * 
         * and we remove the sequence ( leaf1, 21 ) leaving a well-formed node.
         * 
         * Remove(leaf1): index := 0 nkeys = 2 nchildren := nkeys(2) + 1 = 3
         * lenChildCopy := #children(3) - index(0) - 1 = 2 lenKeyCopy :=
         * lengthChildCopy - 1 = 1 copyChildren from index+1(1) to index(0)
         * lengthChildCopy(2) copyKeys from index+1(1) to index(0)
         * lengthKeyCopy(1) erase keys[ nkeys - 1 = 1 ] erase children[ nkeys =
         * 2 ]
         * 
         * post-condition: index: 0 1 2 root keys : [ 24 0 0 ] index leaf2 keys
         * : [ 21 22 23 - ] 0 leaf4 keys : [ 24 31 - - ] 1
         */

        {

            /*
             * Copy down to cover up the hole.
             */
            final int index = i;

            final int nkeys = getKeyCount();

            // #of children to copy (equivalent to nchildren - index - 1)
            final int lengthChildCopy = nkeys - index;

            // #of keys to copy.
            final int lengthKeyCopy = lengthChildCopy - 1;

            // Tunnel through to the mutable keys object.
            final MutableKeyBuffer keys = (MutableKeyBuffer) this.getKeys();
            final MutableNodeData data = (MutableNodeData) this.data;

            if (lengthKeyCopy > 0) {

                System.arraycopy(keys.keys, index + 1, keys.keys, index,
                        lengthKeyCopy);

            }

            if (lengthChildCopy > 0) {

                System.arraycopy(childRefs, index + 1, childRefs, index,
                        lengthChildCopy);

                System.arraycopy(data.childAddr, index + 1, data.childAddr,
                        index, lengthChildCopy);

                System.arraycopy(data.childEntryCounts, index + 1,
                        data.childEntryCounts, index, lengthChildCopy);

            }

            /*
             * Erase the data that were exposed by this operation. Note that
             * there is one fewer keys than children so ....
             */

            if (nkeys > 0) {

                // erase the last key position.
                keys.keys[nkeys - 1] = null;

            }

            // erase the last child position.
            childRefs[nkeys] = null;
            data.childAddr[nkeys] = NULL;
            data.childEntryCounts[nkeys] = 0;

            // Clear the parent on the old child.
            child.parent = null;

            // one less the key in this node.
            /* nkeys--; */keys.nkeys--;

            if (child.isLeaf()) {

                btree.nleaves--;

            } else {

                btree.nnodes--;

            }

            // Deallocate the child.
            child.delete();

        }

        if (btree.root == this) {

            /*
             * The root node is allowed to become deficient, but once we are
             * reduced to having no more keys in the root node it is replaced by
             * the last remaining child.
             */
            if (getKeyCount() == 0 && !isLeaf()) {

                final AbstractNode<?> lastChild = getChild(0);

                if (btree.debug)
                    lastChild.assertInvariants();

                if (INFO) {
                    log.info("replacing root: root=" + btree.root + ", node="
                            + this + ", lastChild=" + lastChild);
                    // System.err.println("root");
                    // btree.root.dump(Level.DEBUG,System.err);
                    // System.err.println("this");
                    // this.dump(Level.DEBUG,System.err);
                    // System.err.println("lastChild");
                    // lastChild.dump(Level.DEBUG,System.err);
                }

                final boolean wasDirty = btree.root.dirty;

                assert lastChild != null;

                // replace the root node with a root leaf.
                btree.root = lastChild;

                if (!wasDirty) {

                    btree.fireDirtyEvent();

                }

                // clear the parent reference since this is now the root.
                lastChild.parent = null;

                // one less level in the btree.
                btree.height--;

                // deallocate this node.
                this.delete();

                // one less node in the tree.
                btree.nnodes--;

                if (BTree.INFO) {
                    BTree.log.info("reduced tree height: height="
                            + btree.height + ", newRoot=" + btree.root);
                }

                btree.getBtreeCounters().rootsJoined++;

            }

        } else {

            /*
             * If a non-root node becomes deficient then it is joined with a
             * direct sibling. If this forces a merge with a sibling, then the
             * merged sibling will be removed from the parent which may force
             * the parent to become deficient in turn, and thereby trigger a
             * join() of the parent.
             */

            if (data.getKeyCount() < minKeys()) {

                join();

            }

        }

    }

    // /**
    // * This is invoked by {@link #removeChild(AbstractNode)} when the node is
    // * reduced to a single child in order to replace the reference to the node
    // * on its parent with the reference to the node's sole remaining child.
    // *
    // * @param oldChild
    // * The node.
    // * @param newChild
    // * The node's sole remaining child. This MAY be persistent since
    // * this operation does NOT change the persistent state of the
    // * newChild but only updates its transient state (e.g., its
    // * parent reference).
    // */
    // protected void replaceChild(AbstractNode oldChild,AbstractNode newChild)
    // {
    //        
    // assert oldChild != null;
    // assert !oldChild.isDeleted();
    // assert !oldChild.isPersistent();
    // assert oldChild.parent.get() == this;
    // assert oldChild.nkeys == 0;
    // assertInvariants();
    // oldChild.assertInvariants();
    // newChild.assertInvariants();
    //
    // assert newChild != null;
    // assert !newChild.isDeleted();
    // // assert !newChild.isPersistent(); // MAY be persistent - does not
    // matter.
    // assert newChild.parent.get() == oldChild;
    //
    // assert oldChild != newChild;
    //
    // assert !isDeleted();
    // assert !isPersistent();
    //
    // int i = getIndexOf( oldChild );
    //
    // // dirtyChildren.remove(oldChild);
    // //
    // // if (newChild.isDirty()) {
    // //
    // // dirtyChildren.add(newChild);
    // //
    // // }
    //
    // // set the persistent key for the new child.
    // childAddr[i] = (newChild.isPersistent() ? newChild.getIdentity() : NULL);
    //
    // // set the reference to the new child.
    // childRefs[i] = new WeakReference<AbstractNode>(newChild);
    //
    // // Reuse the weak reference from the oldChild.
    // newChild.parent = oldChild.parent;
    //
    // }

    /**
     * Return the child node or leaf at the specified index in this node. If the
     * node is not in memory then it is read from the store.
     * <p>
     * Note: This implementation DOES NOT cause concurrent threads to block
     * unless they are performing IO for the same child. A {@link Memoizer}
     * pattern is used to assign each concurrent thread a {@link FutureTask} on
     * which it waits for the result. Once the result is available, there is a
     * small <code>synchronized</code> block during which the concurrent
     * requests for a child will content to update the appropriate element in
     * {@link #childRefs}.
     * <p>
     * I believe the contention to update {@link #childRefs} is unavoidable. If
     * this object was made into an {@link AtomicReferenceArray} then we would
     * have difficulty when inserting and removing tuples since the backing
     * array is not visible. An array of {@link AtomicReference} objects would
     * not help since it would not ensure "publication" when the element was
     * changed from a <code>null</code> to an {@link AtomicReference}, only when
     * {@link AtomicReference#compareAndSet(Object, Object)} was used. Thus it
     * could only help if we pre-populated the array with
     * {@link AtomicReference} objects, which seems wasteful.
     * <p>
     * As always, the mutable B+Tree is single threaded so there are not added
     * synchronization costs. Concurrent readers can only arise for read-only
     * {@link BTree}s and for {@link IndexSegment}s.</strong>
     * 
     * @param index
     *            The index of the child to be read from the store (in
     *            [0:nkeys]).
     * 
     * @return The child node or leaf and never null.
     * 
     * @throws IndexOutOfBoundsException
     *             if index is negative.
     * @throws IndexOutOfBoundsException
     *             if index is GT the #of keys in the node (there is one more
     *             child than keys in a node).
     */
    final public AbstractNode getChild(final int index) {

        /*
         * I've take out this test since it turns out to be relatively
         * expensive!?! The interrupt status of the thread is now checked
         * exclusively when reading on the store.
         */
        // if(Thread.interrupted()) {
        // /*
        // * This method is called relatively often - it is used each time we
        // * descend the tree. We check whether or not the thread has been
        // * interrupted so that we can abort running tasks quickly.
        // */
        // throw new RuntimeException(new InterruptedException());
        // }

        if (index < 0 || index > data.getKeyCount()) {

            throw new IndexOutOfBoundsException("index=" + index + ", nkeys="
                    + data.getKeyCount());

        }

        // if (!btree.isReadOnly()) {
        if (btree.memo == null) {

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
        synchronized (childRefs) {

            /*
             * Note: we need to synchronize on here to ensure visibility for
             * childRefs[index] (in case it was updated in another thread). This
             * is true even for the mutable B+Tree since the caller could use
             * different threads for different operations. However, this
             * synchronization will never be contended for the mutable B+Tree.
             */

            final Reference<AbstractNode<?>> childRef = childRefs[index];

            final AbstractNode child = childRef == null ? null : childRef.get();

            if (child != null) {

                // Already materialized.
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

        return btree.loadChild(this, index);

    }

    /**
     * Method conditionally reads the child at the specified index from the
     * backing store and sets its reference on the appropriate element of
     * {@link #childRefs}. This method assumes that external mechanisms
     * guarantee that no other thread is requesting the same child via this
     * method at the same time. For the mutable B+Tree, that guarantee is
     * trivially given by its single-threaded constraint. For the read-only
     * B+Tree, {@link AbstractBTree#loadChild(Node, int)} provides this
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
    AbstractNode _getChild(final int index, final LoadChildRequest req) {

        /*
         * Make sure that the child is not reachable. It could have been
         * concurrently set even if the caller had tested this and we do not
         * want to read through to the backing store unless we need to.
         */
        AbstractNode child;
        synchronized (childRefs) {

            /*
             * Note: we need to synchronize on here to ensure visibility for
             * childRefs[index] (in case it was updated in another thread).
             */
            final Reference<AbstractNode<?>> childRef = childRefs[index];

            child = childRef == null ? null : childRef.get();

            if (child != null) {

                // Already materialized.
                return child;

            }

        }

        /*
         * The child needs to be read from the backing store.
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
        child = btree.readNodeOrLeaf(addr);

        /*
         * Update of the childRefs[index] element.
         * 
         * Note: This code block is synchronized in order to facilitate the safe
         * publication of the change in childRefs[index] to other threads.
         */
        synchronized (childRefs) {

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
            assert childRefs[index] == null || childRefs[index].get() == null : "Child is already set: this="
                    + this + ", index=" + index;

            // patch parent reference since loaded from store.
            child.parent = this.self;

            // patch the child reference.
            childRefs[index] = child.self;

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

            btree.memo.removeFromCache(req);

        }

        return child;

    }

    // /**
    // * Static helper method allocates the per-child lock objects.
    // * <p>
    // * Note that the mutable {@link BTree} imposes a single-threaded
    // constraint
    // * on its API so we do not need to do any locking for that case and this
    // * method will therefore return <code>null</code> if the owning B+Tree is
    // * mutable.
    // *
    // * @param btree
    // * The owning B+Tree.
    // *
    // * @param nkeys
    // * The #of keys, which is used to dimension the array.
    // *
    // * @return The array of lock objects -or- <code>null</code> if the btree
    // is
    // * mutable.
    // *
    // * @see #childLocks
    // *
    // * @todo Per-child locks will only be useful on nodes with a relatively
    // high
    // * probability of concurrent access. Therefore they should be
    // * conditionally enabled only to a depth of 0 (for the root's direct
    // * children) or 1 (for the children of the root's direct children).
    // * There is just not going to be any utility to this beyond that
    // * point, especially not on an {@link IndexSegment} with a relatively
    // * high branching factor. We could directly compute the probability of
    // * access to any given child based on the branching factor and the
    // * depth of the node in the B+Tree and the assumption of a uniform
    // * distribution of reads by concurrent threads [in fact, in many
    // * benchmark situations we are more likely to content for the same
    // * child unless the queries are parameterized].
    // */
    // static private final Object[] newChildLocks(final AbstractBTree btree,
    // final int nkeys) {
    //        
    // /*
    // * Note: Uncommenting this has the effect of disabling per-child
    // * locking.
    // */
    // // if(true) return null;
    //        
    // if (!btree.isReadOnly() || !btree.getIndexMetadata().getChildLocks()) {
    //
    // /*
    // * Either The mutable B+Tree has a single threaded constraint so we
    // * do not need to do any locking for that case and therefore we do
    // * not allocate the per-child locks here -or- child locks were
    // * disabled as a configuration option.
    // */
    //            
    // return null;
    //            
    // }
    //        
    // /*
    // * Note: The array is dimensioned to [branchingFactor] and not
    // * [branchingFactor+1]. The "overflow" slot of the various arrays are
    // * only used during node overflow/underflow operations. Those operations
    // * do not occur for a read-only B+Tree.
    // */
    // // final int n = btree.branchingFactor + 1;
    //        
    // // Note: We only need locks for the child entries that exist!
    // final int n = nkeys + 1;
    //        
    // final Object[] a = new Object[n];
    //        
    // for (int i = 0; i < n; i++) {
    //
    // a[i] = new Object();
    //            
    // }
    //        
    // return a;
    //        
    // }

    /**
     * Return the right-most child of this node.
     * 
     * @param nodesOnly
     *            when <code>true</code> the search will halt at the right-most
     *            non-leaf. Otherwise it will return the right-most leaf.
     * 
     * @return The right-most child of this node.
     */
    protected AbstractNode getRightMostChild(final boolean nodesOnly) {

        final AbstractNode<?> child = getChild(getKeyCount());

        assert child != null;

        if (child.isLeaf()) {

            if (nodesOnly) {

                return this;

            } else {

                return child;

            }

        }

        return ((Node) child).getRightMostChild(nodesOnly);

    }

    /**
     * Iterator visits children, recursively expanding each child with a
     * post-order traversal of its children and finally visits this node itself.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<AbstractNode> postOrderNodeIterator(
            final boolean dirtyNodesOnly) {

        if (dirtyNodesOnly && !dirty) {

            return EmptyIterator.DEFAULT;

        }

        /*
         * Iterator append this node to the iterator in the post-order position.
         */

        return new Striterator(postOrderIterator1(dirtyNodesOnly))
                .append(new SingleValueIterator(this));

    }

    /**
     * Iterator visits children in the specified half-open key range,
     * recursively expanding each child with a post-order traversal of its
     * children and finally visits this node itself.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<AbstractNode> postOrderIterator(final byte[] fromKey,
            final byte[] toKey) {

        /*
         * Iterator append this node to the iterator in the post-order position.
         */

        return new Striterator(postOrderIterator2(fromKey, toKey))
                .append(new SingleValueIterator(this));

    }

    /**
     * Visits the children (recursively) using post-order traversal, but does
     * NOT visit this node.
     */
    @SuppressWarnings("unchecked")
    private Iterator<AbstractNode> postOrderIterator1(
            final boolean dirtyNodesOnly) {

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
                    protected Iterator expand(Object childObj) {

                        /*
                         * A child of this node.
                         */

                        final AbstractNode child = (AbstractNode) childObj;

                        if (dirtyNodesOnly && !child.dirty) {

                            return EmptyIterator.DEFAULT;

                        }

                        if (child instanceof Node) {

                            /*
                             * The child is a Node (has children).
                             */

                            // BTree.log.debug("child is node: " + child);
                            // visit the children (recursive post-order
                            // traversal).
                            final Striterator itr = new Striterator(
                                    ((Node) child)
                                            .postOrderIterator1(dirtyNodesOnly));

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
     * Visits the children (recursively) using post-order traversal, but does
     * NOT visit this node.
     */
    @SuppressWarnings("unchecked")
    private Iterator<AbstractNode> postOrderIterator2(final byte[] fromKey,
            final byte[] toKey) {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the post-order iterator.
         * 
         * When dirtyNodesOnly is true we use a child iterator that makes a best
         * effort to only visit dirty nodes. Especially, the iterator MUST NOT
         * force children to be loaded from disk if the are not resident since
         * dirty nodes are always resident.
         * 
         * The iterator must touch the node in order to guarentee that a node
         * will still be dirty by the time that the caller visits it. This
         * places the node onto the hard reference queue and increments its
         * reference counter. Evictions do NOT cause IO when the reference is
         * non-zero, so the node will not be made persistent as a result of
         * other node touches. However, the node can still be made persistent if
         * the caller explicitly writes the node onto the store.
         */

        // BTree.log.debug("node: " + this);
        return new Striterator(childIterator(fromKey, toKey))
                .addFilter(new Expander() {

                    private static final long serialVersionUID = 1L;

                    /*
                     * Expand each child in turn.
                     */
                    protected Iterator expand(final Object childObj) {

                        /*
                         * A child of this node.
                         */

                        final AbstractNode child = (AbstractNode) childObj;

                        if (child instanceof Node) {

                            /*
                             * The child is a Node (has children).
                             * 
                             * Visit the children (recursive post-order
                             * traversal).
                             */

                            // BTree.log.debug("child is node: " + child);
                            final Striterator itr = new Striterator(
                                    ((Node) child).postOrderIterator2(fromKey,
                                            toKey));

                            // append this node in post-order position.
                            itr.append(new SingleValueIterator(child));

                            if ((btree.store instanceof Journal)
                                    && (((Journal) btree.store)
                                            .getReadExecutor() != null)) {

                                /*
                                 * Prefetch any child leaves we need to visit
                                 * and prefetch the right sibling of the node we
                                 * are about to visit if the iterator will span
                                 * that node as well.
                                 */
                                prefetchChildLeaves((Node) child, fromKey,
                                        toKey);

                            }

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
     * When we visit a node whose children are leaves, schedule memoization of
     * those leaves whose separator key in the node is LT the toKey
     * (non-blocking). If the rightSibling of the node would be visited by the
     * iterator, then prefetch of the rightSibling is also scheduled.
     * 
     * @param node
     *            A node whose children are leaves.
     * @param toKey
     *            The exclusive upper bound of some iterator.
     * 
     * @todo All memoization should go through the same thread pool in order to
     *       bound the #of threads actually reading on the disk. Right now, the
     *       memoizer runs in the caller's thread. If we modified it to submit a
     *       task to the readService to handle the memoization then we would
     *       bound the #of threads involved.
     * 
     * @todo PREFETCH : JSR 166 Fork/join would also be a good choice here.
     * 
     * @todo PREFETCH : Prefetch will not break if the iterator is closed. This
     *       is not a bug per se, but it might be something we want to support
     *       in the future.
     * 
     * @todo PREFETCH : Prefetch should be cancelled if the btree is closed. We
     *       could do this with an Executor wrapping the {@link LatchedExecutor}
     *       that kept track of the work queue for that executor and allowed us
     *       to cancel just the tasks submitted against it. This would also give
     *       us a means to report on the prefetch work queue length.
     * 
     * @todo PREFETCH : Only journal is supported right now.
     * 
     * @todo PREFETCH : The {@link IRangeQuery#CURSOR} mode is not supported yet.
     */
    protected void prefetchChildLeaves(final Node node, final byte[] fromKey,
                final byte[] toKey) {

        final int nkeys = node.getKeyCount();
        
        // figure out the first index to visit.
        final int fromIndex;
        {

            int index;

            if (fromKey != null) {

                index = node.getKeys().search(fromKey);

                if (index < 0) {

                    index = -index - 1;

                }

            } else {

                index = 0;

            }

            fromIndex = index;

        }

        // figure out the first index to NOT visit.
        final int toIndex;
        {

            int index;

            if (toKey != null) {

                index = node.getKeys().search(toKey);

                if (index < 0) {

                    index = -index - 1;

                }

            } else {

                index = nkeys;

            }

            toIndex = index;

        }

        /*
         * Submit taskS which will materialize the children in
         * [fromIndex,toIndex).
         * 
         * Note: We do not track the futures of these tasks. The tasks have a
         * side effect on the parent/child weak references among the nodes in
         * the B+Tree, on the backing hard reference ring buffer, and on the
         * cache of materialized disk records. That side effect is all that we
         * are seeking.
         * 
         * Note: If the B+Tree is concurrently closed, then these tasks will
         * error out. That is fine.
         */
        final Executor s = ((Journal) btree.store).getReadExecutor();

        for (int i = fromIndex; i < toIndex; i++) {
            
            final int index = i;
            
            s.execute(new Runnable() {

                public void run() {

                    if (!node.btree.isOpen()) {
                        // No longer open.
                        return;

                    }

                    // Materialize the child.
                    node.getChild(index);

                }

            });

        }
        
        if (toIndex == nkeys - 1) {

            /*
             * Prefetch the right sibling.
             */
            
            prefetchRightSibling(node, toKey);
            
        }
        
    }

    /**
     * If the caller's <i>toKey</i> is GT the separator keys for the children of
     * this node then the iterator will need to visit the rightSibling of the
     * node and this method will schedule the memoization of the node's
     * rightSibling in order to reduce the IO latency when the iterator visit
     * that rightSibling (non-blocking).
     * 
     * @param node
     *            A node.
     * @param toKey
     *            The exclusive upper bound of some iterator.
     */
    protected void prefetchRightSibling(final Node node, final byte[] toKey) {
        
        final int nkeys = node.getKeyCount();

        final byte[] lastSeparatorKey = node.getKeys().get(nkeys - 1);

        if (BytesUtil.compareBytes(toKey, lastSeparatorKey) <= 0) {

            /*
             * Since the toKey is LTE to the lastSeparatorKey on this node the
             * last tuple to be visited by the iterator is spanned by this node
             * and we will not visit the node's rightSibling.
             * 
             * @todo This test could be optimized if IRaba exposed a method for
             * unsigned byte[] comparisons against the coded representation of
             * the keys.
             */

            return;

        }

        // The parent of this node.
        final Node p = node.parent.get();

//        /*
//         * Test to see if the rightSibling is already materialized.
//         */
//        
//        Note: Don't bother testing as we will test in the task below anyway.
//        
//        Node rightSibling = (Node) p.getRightSibling(node,
//                false/* materialize */);
//
//        if (rightSibling != null) {
//
//            /*
//             * The rightSibling is already materialized and getRightSibling()
//             * touches the rightSibling as a side-effect so it will be retained
//             * longer. Return now as there is nothing to do.
//             */
//
//            return;
//            
//        }

        /*
         * Submit a task which will materialize that right sibling.
         * 
         * Note: This task will only materialize a rightSibling of a common
         * parent. If [node] is the last child of the parent [p] then you would
         * need to ascend to the parent of [p] and then desend again, which is
         * not the specified behavior for getRightSibling(). Since this is just
         * an optimization for IO scheduling, I think that it is fine as it is.
         * 
         * Note: We do not track the future of this task. The task will have a
         * side effect on the parent/child weak references among the nodes in
         * the B+Tree, on the backing hard reference ring buffer, and on the
         * cache of materialized disk records. That side effect is all that we
         * are seeking.
         * 
         * Note: If the B+Tree is concurrently closed, then this task will error
         * out.  That is fine.
         */
        final Executor s = ((Journal) btree.store).getReadExecutor();

        s.execute(new Runnable() {

            public void run() {

                if (!p.btree.isOpen()) {

                    // No longer open.
                    return;
                    
                }

                // Materialize the right sibling.
                p.getRightSibling(node, true/* materialize */);

            }

        });

    }
    
    /**
     * Iterator visits the direct child nodes in the external key ordering.
     * 
     * @param dirtyNodesOnly
     *            When true, only the direct dirty child nodes will be visited.
     */
    public Iterator<AbstractNode> childIterator(final boolean dirtyNodesOnly) {

        if (dirtyNodesOnly) {

            return new DirtyChildIterator(this);

        } else {

            return new ChildIterator(this);

        }

    }

    /**
     * Iterator visits the direct child nodes in the external key ordering.
     */
    public Iterator<AbstractNode> childIterator(final byte[] fromKey,
            final byte[] toKey) {

        return new ChildIterator(this, fromKey, toKey);

    }

    @Override
    public boolean dump(final Level level, final PrintStream out,
            final int height, final boolean recursive) {

        // True iff we will write out the node structure.
        final boolean debug = level.toInt() <= Level.DEBUG.toInt();

        // Set true iff an inconsistency is detected.
        boolean ok = true;

        final int branchingFactor = this.getBranchingFactor();
        final int nkeys = getKeyCount();
        final int minKeys = this.minKeys();
        final int maxKeys = this.maxKeys();

        if (parent != null && nkeys < minKeys) {
            // min keys failure.
            out.println(indent(height) + "ERROR: too few keys: m="
                    + branchingFactor + ", minKeys=" + minKeys + ", nkeys="
                    + nkeys + ", isLeaf=" + isLeaf());
            ok = false;
        }

        if (nkeys > maxKeys) {
            // max keys failure.
            out.println(indent(height) + "ERROR: too many keys: m="
                    + branchingFactor + ", maxKeys=" + maxKeys + ", nkeys="
                    + nkeys + ", isLeaf=" + isLeaf());
            ok = false;
        }

        { // nentries
            if (this == btree.root) {
                if (getSpannedTupleCount() != btree.getEntryCount()) {
                    out.println(indent(height)
                            + "ERROR: root node has nentries="
                            + getSpannedTupleCount()
                            + ", but btree has nentries="
                            + btree.getEntryCount());
                    ok = false;
                }
            }
            {
                int nentries = 0;
                for (int i = 0; i <= nkeys; i++) {
                    nentries += getChildEntryCount(i);
                    if (nentries <= 0) {
                        out.println(indent(height) + "ERROR: childEntryCount["
                                + i + "] is non-positive");
                        ok = false;
                    }
                }
                if (nentries != getSpannedTupleCount()) {
                    out.println(indent(height) + "ERROR: nentries("
                            + getSpannedTupleCount()
                            + ") does not agree with sum of per-child counts("
                            + nentries + ")");
                    ok = false;
                }
            }
        }

        if (this == btree.root) {
            if (parent != null) {
                out
                        .println(indent(height)
                                + "ERROR: this is the root, but the parent is not null.");
                ok = false;
            }
        } else {
            /*
             * Note: there is a difference between having a parent reference and
             * having the parent be stronly reachable. However, we actually want
             * to maintain both -- a parent MUST always be strongly reachable
             * ... UNLESS you are doing a fast forward or reverse leaf scan
             * since the node hierarchy is not being traversed in that case.
             * 
             * @todo Should we keep leaves using a fast forward or reverse scan
             * out of the hard reference cache since their parents are not
             * strongly reachable?
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

        // verify keys are monotonically increasing.
        try {
            assertKeysMonotonic();
        } catch (AssertionError ex) {
            out.println(indent(height) + "  ERROR: " + ex);
            ok = false;
        }
        if (debug) {
            out.println(indent(height) + toString());
            // out.println(indent(height) + "  parent="
            // + (parent == null ? null : parent.get()));
            // out.println(indent(height) + "  dirty=" + isDirty() + ", nkeys="
            // + nkeys + ", nchildren=" + (nkeys + 1) + ", minKeys="
            // + minKeys + ", maxKeys=" + maxKeys + ", branchingFactor="
            // + branchingFactor+", #entries="+getSpannedTupleCount());
            // out.println(indent(height) + "  keys=" + getKeys());
            // // out.println(indent(height) + "  childAddr="
            // // + Arrays.toString(childAddr));
            // out.print(indent(height) + "  childAddr/Refs=[");
            // for (int i = 0; i <= nkeys + 1; i++) {
            // if (i > 0)
            // out.print(", ");
            // out.print(getChildAddr(i));
            // out.print('(');
            // if (childRefs[i] == null) {
            // out.print("null");
            // } else {
            // // Non-recursive print of the reference.
            // final AbstractNode<?> child = childRefs[i].get();
            // out.print(child.getClass().getName() + "@"
            // + Integer.toHexString(child.hashCode()));
            // }
            // out.print(')');
            // }
            // out.println("]");
            // out.print(indent(height) + "  childEntryCounts=[");
            // for (int i = 0; i <= nkeys; i++) {
            // if (i > 0)
            // out.print(", ");
            // out.print(getChildEntryCount(i));
            // }
            // out.println("]");
            // // + Arrays.toString(childEntryCounts));
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
        for (int i = 0; i < branchingFactor + 1; i++) {

            if (i > nkeys) {

                /*
                 * Scanning past the last valid child index.
                 */

                if (!isReadOnly()
                        && ((MutableNodeData) data).childAddr[i] != NULL) {

                    out.println(indent(height) + "  ERROR childAddr[" + i
                            + "] should be " + NULL + ", not "
                            + ((MutableNodeData) data).childAddr[i]);

                    ok = false;

                }

                if (childRefs[i] != null) {

                    out.println(indent(height) + "  ERROR childRefs[" + i
                            + "] should be null, not " + childRefs[i]);

                    ok = false;

                }

            } else {

                /*
                 * Scanning a valid child index.
                 * 
                 * Note: This is not fetching the child if it is not in memory
                 * -- perhaps it should using its persistent id?
                 */

                final AbstractNode<?> child = (childRefs[i] == null ? null
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
                        // // some extra stuff used to track down a bug.
                        if (!ok) {
                            if (level == Level.DEBUG) {
                                // dump the child also and exit.
                                System.err.println("child");
                                child.dump(Level.DEBUG, System.err);
                                throw new AssertionError();
                            } else {
                                // recursive call to get debug level dump.
                                System.err.println("this");
                                this.dump(Level.DEBUG, System.err);
                            }
                        }
                    }

                    if (getChildEntryCount(i) != child.getSpannedTupleCount()) {
                        out.println(indent(height) + "  ERROR child[" + i
                                + "] spans " + child.getSpannedTupleCount()
                                + " entries, but childEntryCount[" + i + "]="
                                + getChildEntryCount(i));
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
                        // if (!dirtyChildren.contains(child)) {
                        // out
                        // .println(indent(height + 1)
                        // + "  ERROR child at index="
                        // + i
                        // + " is dirty, but not on the dirty list: child="
                        // + child);
                        // ok = false;
                        // }
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
                        // if (dirtyChildren.contains(child)) {
                        // out
                        // .println(indent(height)
                        // + "  ERROR child at index="
                        // + i
                        // + " is not dirty, but is on the dirty list: child="
                        // + child);
                        // ok = false;
                        // }
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

            final Set<AbstractNode<?>> dirty = new HashSet<AbstractNode<?>>();

            for (int i = 0; i <= /* nkeys */branchingFactor; i++) {

                if (childRefs[i] == null && !isReadOnly()
                        && ((MutableNodeData) data).childAddr[i] == 0) {

                    if (i <= nkeys) {

                        /*
                         * This let's us dump a tree with some kinds of
                         * structural problems (missing child reference or key).
                         */

                        out.println(indent(height + 1)
                                + "ERROR can not find child at index=" + i
                                + ", skipping this index.");

                        ok = false;

                    } else {

                        /*
                         * We expect null child entries beyond nkeys+1.
                         */

                    }

                    continue;

                }

                /*
                 * Note: this works around the assert test for the index in
                 * getChild(index) but is not able/willing to follow a childKey
                 * to a child that is not memory resident.
                 */
                // AbstractNode child = getChild(i);
                final AbstractNode<?> child = childRefs[i] == null ? null
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

                    // if (child.isDirty() && !dirtyChildren.contains(child)) {
                    //
                    // out
                    // .println(indent(height + 1)
                    // + "ERROR dirty child not in node's dirty list at index="
                    // + i);
                    //
                    // ok = false;
                    //
                    // }
                    //
                    // if (!child.isDirty() && dirtyChildren.contains(child)) {
                    //
                    // out
                    // .println(indent(height + 1)
                    // +
                    // "ERROR clear child found in node's dirty list at index="
                    // + i);
                    //
                    // ok = false;
                    //
                    // }

                    if (child.isDirty()) {

                        dirty.add(child);

                    }

                    if (i == 0) {

                        if (nkeys == 0) {

                            /*
                             * Note: a node with zero keys is valid. It MUST
                             * have a single child. Such nodes arise when
                             * splitting a node in a btree of order m := 3 when
                             * the splitIndex is computed as m/2-1 = 0. This is
                             * perfectly normal.
                             */

                        } else {
                            /*
                             * Note: All keys on the first child MUST be LT the
                             * first key on this node.
                             */
                            final byte[] k0 = getKeys().get(0);
                            final byte[] ck0 = child.getKeys().get(0);
                            if (BytesUtil.compareBytes(ck0, k0) >= 0) {
                                // if( child.compare(0,keys,0) >= 0 ) {

                                out
                                        .println(indent(height + 1)
                                                + "ERROR first key on first child must be LT "
                                                + keyAsString(k0)
                                                + ", but found "
                                                + keyAsString(ck0));

                                ok = false;

                            }

                            if (child.getKeyCount() >= 1) {

                                final byte[] ckn = child.getKeys().get(
                                        child.getKeyCount() - 1);
                                if (BytesUtil.compareBytes(ckn, k0) >= 0) {
                                    // if (child.compare(child.nkeys-1, keys, 0)
                                    // >= 0) {

                                    out
                                            .println(indent(height + 1)
                                                    + "ERROR last key on first child must be LT "
                                                    + keyAsString(k0)
                                                    + ", but found "
                                                    + keyAsString(ckn));

                                    ok = false;

                                }

                            }

                        }

                    } else if (i < nkeys) {

                        // Note: The delete rule does not preserve this
                        // characteristic since we do not
                        // update the separatorKey for a leaf when removing its
                        // left most key.
                        //
                        // if (child.isLeaf() && keys[i - 1] != child.keys[0]) {
                        //
                        // /*
                        // * While each key in a node always is the first key of
                        // * some leaf, we are only testing the direct children
                        // * here. Therefore if the children are not leaves then
                        // * we can not cross check their first key with the
                        // keys
                        // * on this node.
                        // */
                        // out.println(indent(height + 1)
                        // + "ERROR first key on child leaf must be "
                        // + keys[i - 1] + ", not " + child.keys[0]
                        // + " at index=" + i);
                        //
                        // ok = false;
                        //
                        // }

                    } else {

                        /*
                         * While there is a child for the last index of a node,
                         * there is no key for that index.
                         */

                    }

                    if (!child.dump(level, out, height + 1, true)) {

                        ok = false;

                    }

                }

            }

            // if (dirty.size() != dirtyChildren.size()) {
            //
            // out.println(indent(height + 1) + "ERROR found " + dirty.size()
            // + " dirty children, but " + dirtyChildren.size()
            // + " in node's dirty list");
            //
            // ok = false;
            //
            // }

        }

        return ok;

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

        final Node p = (parent == null ? null : parent.get());

        sb.append(", parent=" + (p == null ? "N/A" : p.toShortString()));

        if (data == null) {

            // No data record? (Generally, this means it was stolen by copy on
            // write).
            sb.append(", data=NA}");

            return sb.toString();

        }

        sb.append(", nkeys=" + getKeyCount());

        sb.append(", minKeys=" + minKeys());

        sb.append(", maxKeys=" + maxKeys());

        DefaultNodeCoder.toString(this, sb);

        // indicate if each child is loaded or unloaded.
        {

            final int nchildren = getChildCount();

            sb.append(", children=[");

            for (int i = 0; i < nchildren; i++) {

                if (i > 0)
                    sb.append(", ");

                final AbstractNode<?> child = childRefs[i] == null ? null
                        : childRefs[i].get();

                sb.append(child == null ? "U" : "L");

            }

            sb.append("]");

        }

        sb.append("}");

        return sb.toString();

    }

}
