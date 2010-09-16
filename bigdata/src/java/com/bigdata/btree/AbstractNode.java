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
import java.lang.ref.WeakReference;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.data.IAbstractNodeData;
import com.bigdata.btree.filter.EmptyTupleIterator;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableKeyBuffer;
import com.bigdata.cache.HardReferenceQueue;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract node supporting incremental persistence and copy-on-write semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractNode<T extends AbstractNode
/*
 * DO-NOT-USE-GENERIC-HERE. The compiler will fail under Linux (JDK 1.6.0_14,
 * _16).
 */
> extends PO implements IAbstractNode, IAbstractNodeData {

    /**
     * Log for node and leaf operations.
     * <dl>
     * <dt>info</dt>
     * <dd>A high level trace of insert, split, joint, and remove operations.
     * You MUST test on {@link Logger#isInfoEnabled()} before generating log
     * messages at this level to avoid string concatenation operations would
     * otherwise kill performance.</dd>
     * <dt></dt>
     * <dd>A low level trace including a lot of dumps of leaf and node state. *
     * You MUST test on {@link Logger#isDebugEnabled()} before generating log
     * messages at this level to avoid string concatenation operations would
     * otherwise kill performance.</dd>
     * </dl>
     * 
     * @see BTree#log
     * @see BTree#dumpLog
     */
    protected static final Logger log = Logger.getLogger(AbstractNode.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * The BTree.
     * 
     * Note: This field MUST be patched when the node is read from the store.
     * This requires a custom method to read the node with the btree reference
     * on hand so that we can set this field.
     */
    final transient protected AbstractBTree btree;

    /**
     * The parent of this node. This is null for the root node. The parent is
     * required in order to set the persistent identity of a newly persisted
     * child node on its parent. The reference to the parent will remain
     * strongly reachable as long as the parent is either a root (held by the
     * {@link BTree}) or a dirty child (held by the {@link Node}). The parent
     * reference is set when a node is attached as the child of another node.
     * <p>
     * Note: When a node is cloned by {@link #copyOnWrite()} the parent
     * references for its <em>clean</em> children are set to the new copy of the
     * node. This is referred to in several places as "stealing" the children
     * since they are no longer linked back to their old parents via their
     * parent reference.
     */
    transient protected Reference<Node> parent = null;

    /**
     * <p>
     * A {@link Reference} to this {@link Node}. This is created when the node
     * is created and is reused by a children of the node as the
     * {@link Reference} to their parent. This results in few {@link Reference}
     * objects in use by the B+Tree since it effectively provides a canonical
     * {@link Reference} object for any given {@link Node}.
     * </p>
     */
    transient protected final Reference<? extends AbstractNode<T>> self;
    
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
     * The minimum #of keys. For a {@link Node}, the minimum #of children is
     * <code>minKeys + 1</code>. For a {@link Leaf}, the minimum #of values
     * is <code>minKeys</code>.
     */
    abstract protected int minKeys();

    /**
     * The maximum #of keys. This is <code>branchingFactor - 1</code> for a
     * {@link Node} and <code>branchingFactor</code> for a {@link Leaf}. For a
     * {@link Node}, the maximum #of children is <code>maxKeys + 1</code>. For a
     * {@link Leaf}, the maximum #of values is <code>maxKeys</code>.
     */
    abstract protected int maxKeys();

    /**
     * Return the delegate {@link IAbstractNodeData} object.
     */
    abstract IAbstractNodeData getDelegate();
    
    public void delete() {
        
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
//        keys = null;

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

//            btree.store.delete(identity);
            
        }
        
        deleted = true;
        
    }

    /**
     * The parent iff the node has been added as the child of another node and
     * the parent reference has not been cleared.
     * 
     * @return The parent or null if (a) this is the root node or (b) the
     *         {@link WeakReference} to the parent has been cleared.
     */
    final public Node getParent() {

        Node p = null;

        if (parent != null) {

            /*
             * Note: Will be null if the parent reference has been cleared.
             */
            p = parent.get();

        }

        /*
         * The parent is allowed to be null iff this is the root of the
         * btree.
         */
        assert (this == btree.root && p == null) || p != null;

        return p;

    }

    /**
     * Disallowed.
     */
    private AbstractNode() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * All constructors delegate to this constructor to set the btree and
     * branching factor and to compute the minimum and maximum #of keys for the
     * node. This isolates the logic required for computing the minimum and
     * maximum capacity and encapsulates it as <code>final</code> data fields
     * rather than permitting that logic to be replicated throughout the code
     * with the corresponding difficulty in ensuring that the logic is correct
     * throughout.
     * 
     * @param btree
     *            The btree to which the node belongs.
     * @param branchingFactor
     *            The branching factor for the node. By passing the branching
     *            factor rather than using the branching factor declared on the
     *            btree we are able to support different branching factors at
     *            different levels of the tree.
     * @param dirty
     *            Used to set the {@link PO#dirty} state. All nodes and leaves
     *            created by non-deserialization constructors begin their life
     *            cycle as <code>dirty := true</code> All nodes or leaves
     *            de-serialized from the backing store begin their life cycle as
     *            clean (dirty := false). This we read nodes and leaves into
     *            immutable objects, those objects will remain clean. Eventually
     *            a copy-on-write will create a mutable node or leaf from the
     *            immutable one and that node or leaf will be dirty.
     */
    protected AbstractNode(final AbstractBTree btree, final boolean dirty) {

        assert btree != null;

        this.btree = btree;

        // reference to self: reused to link parents and children.
        this.self = btree.newRef(this);
        
        if (!dirty) {

            /*
             * Nodes default to being dirty, so we explicitly mark this as
             * clean. This is ONLY done for the de-serialization constructors.
             */

            setDirty(false);

        }
        
        // Add to the hard reference queue.
        btree.touch(this);
        
    }

    /**
     * Copy constructor.
     * <p>
     * Note: The copy constructor steals the state of the source node, creating
     * a new node with the same state but a distinct (and not yet assigned)
     * address on the backing store. If the source node has immutable data for
     * some aspect of its state, then a mutable copy of that data is made.
     * <p>
     * Note: The <strong>caller</strong> MUST {@link #delete()} the source node
     * after invoking this copy constructor. If the backing store supports the
     * operation, the source node will be reclaimed as free space at the next
     * commit.
     * <p>
     * The source node must be deleted since it is no longer accessible and
     * various aspects of its state have been stolen by the copy constructor. If
     * the btree is committed then both the delete of the source node and the
     * new tree structure will be made restart-safe atomically and all is well.
     * If the operation is aborted then both changes will be undone and all is
     * well. In no case can we access the source node after this operation
     * unless all changes have been aborted, in which case it will simply be
     * re-read from the backing store.
     * 
     * @param src
     *            The source node.
     */
    protected AbstractNode(final AbstractNode<T> src) {

        /*
         * Note: We do NOT clone the base class since this is a new persistence
         * capable object, but it is not yet persistent and we do not want to
         * copy the persistent identity of the source object.
         */
        this(src.btree, true/* dirty */);

        // This node must be mutable (it is a new node).
        assert isDirty();
        assert !isPersistent();
        
        /* The source must not be dirty.  We are cloning it so that we can
         * make changes on it.
         */
//        assert src != null;
        assert !src.isDirty();
//        assert src.isPersistent();
        assert src.isReadOnly();

        /*
         * Copy the parent reference. The parent must be defined unless the
         * source is the current root.
         * 
         * Note that we reuse the weak reference since it is immutable (it state
         * is only changed by the VM, not by the application).
         */

        assert src == btree.root
                || (src.parent != null && src.parent.get() != null);
        
        // copy the parent reference.
        this.parent = src.parent; // @todo clear src.parent (disconnect it)?
        
//        /*
//         * Steal/copy the keys.
//         * 
//         * Note: The copy constructor is invoked when we need to begin mutation
//         * operations on an immutable node or leaf, so make sure that the keys
//         * are mutable.
//         */
//        {
//
////            nkeys = src.nkeys;
//
//            if (src.getKeys() instanceof MutableKeyBuffer) {
//
//                keys = src.getKeys();
//
//            } else {
//
//                keys = new MutableKeyBuffer(src.getBranchingFactor(), src
//                        .getKeys());
//
//            }
//
//            // release reference on the source node.
////            src.nkeys = 0;
//            src.keys = null;
//            
//        }

    }

    /**
     * <p>
     * Return this leaf iff it is dirty (aka mutable) and otherwise return a
     * copy of this leaf. If a copy is made of the leaf, then a copy will also
     * be made of each immutable parent up to the first mutable parent or the
     * root of the tree, which ever comes first. If the root is copied, then the
     * new root will be set on the {@link BTree}. This method must MUST be
     * invoked any time an mutative operation is requested for the leaf.
     * </p>
     * <p>
     * Note: You can not modify a node that has been written onto the store.
     * Instead, you have to clone the node causing it and all nodes up to the
     * root to be dirty and transient. This method handles that cloning process,
     * but the caller MUST test whether or not the node was copied by this
     * method, MUST delegate the mutation operation to the copy iff a copy was
     * made, and MUST result in an awareness in the caller that the copy exists
     * and needs to be used in place of the immutable version of the node.
     * </p>
     * 
     * @return Either this leaf or a copy of this leaf.
     */
    protected AbstractNode<?> copyOnWrite() {
        
        // Always invoked first for a leaf and thereafter in its other form.
        assert isLeaf();
        
        return copyOnWrite(NULL);
        
    }

    /**
     * <p>
     * Return this node or leaf iff it is dirty (aka mutable) and otherwise
     * return a copy of this node or leaf. If a copy is made of the node, then a
     * copy will also be made of each immutable parent up to the first mutable
     * parent or the root of the tree, which ever comes first. If the root is
     * copied, then the new root will be set on the {@link BTree}. This method
     * must MUST be invoked any time an mutative operation is requested for the
     * leaf.
     * </p>
     * <p>
     * Note: You can not modify a node that has been written onto the store.
     * Instead, you have to clone the node causing it and all nodes up to the
     * root to be dirty and transient. This method handles that cloning process,
     * but the caller MUST test whether or not the node was copied by this
     * method, MUST delegate the mutation operation to the copy iff a copy was
     * made, and MUST be aware that the copy exists and needs to be used in
     * place of the immutable version of the node.
     * </p>
     * 
     * @param triggeredByChildId
     *            The persistent identity of child that triggered this event if
     *            any.
     * 
     * @return Either this node or a copy of this node.
     */
    protected AbstractNode<T> copyOnWrite(final long triggeredByChildId) {

//        if (isPersistent()) {
        if (!isReadOnly()) {

            /*
             * Since a clone was not required, we use this as an opportunity to
             * touch the hard reference queue. This helps us to ensure that
             * nodes which have been touched recently will remain strongly
             * reachable.
             */
            
            btree.touch(this);
            
            return this;

        }

        if (INFO) {
            log.info("this=" + this + ", trigger=" + triggeredByChildId);
//                if( DEBUG ) {
//                    System.err.println("this"); dump(Level.DEBUG,System.err);
//                }
        }

        // cast to mutable implementation class.
        final BTree btree = (BTree) this.btree;
        
        // identify of the node that is being copied and deleted.
        final long oldId = this.identity;

        // parent of the node that is being cloned (null iff it is the root).
        Node parent = this.getParent();

        // the new node (mutable copy of the old node).
        final AbstractNode newNode;

        if (this instanceof Node) {

            newNode = new Node((Node) this, triggeredByChildId);
            
            btree.getBtreeCounters().nodesCopyOnWrite++;

        } else {

            newNode = new Leaf((Leaf) this);

            btree.getBtreeCounters().leavesCopyOnWrite++;

        }

        // delete this node now that it has been cloned.
        this.delete();
        
        if (btree.root == this) {

            assert parent == null;

            // Update the root node on the btree.
            if(INFO)
                log.info("Copy-on-write : replaced root node on btree.");

            final boolean wasDirty = btree.root.dirty;
            
            assert newNode != null;
            
            btree.root = newNode;
            
            if (!wasDirty) {
                
                btree.fireDirtyEvent();
                
            }

        } else {

            /*
             * Recursive copy-on-write up the tree. This operations stops as
             * soon as we reach a parent node that is already dirty and
             * grounds out at the root in any case.
             */
            assert parent != null;

            if (!parent.isDirty()) {

                /*
                 * Note: pass up the identity of the old child since we want
                 * to avoid having its parent reference reset.
                 */
                parent = (Node) parent.copyOnWrite(oldId);

            }

            /*
             * Replace the reference to this child with the reference to the
             * new child. This makes the old child inaccessible via
             * navigation. It will be GCd once it falls off of the hard
             * reference queue.
             */
            parent.replaceChildRef(oldId, newNode);

        }

        return newNode;

    }

    public Iterator<AbstractNode> postOrderNodeIterator() {

        return postOrderNodeIterator(false);

    }

    /**
     * Post-order traversal of nodes and leaves in the tree. For any given
     * node, its children are always visited before the node itself (hence
     * the node occurs in the post-order position in the traversal). The
     * iterator is NOT safe for concurrent modification.
     * 
     * @param dirtyNodesOnly
     *            When true, only dirty nodes and leaves will be visited
     *            
     * @return Iterator visiting {@link AbstractNode}s.
     */
    abstract public Iterator<AbstractNode> postOrderNodeIterator(boolean dirtyNodesOnly);

    public ITupleIterator entryIterator() {

        return rangeIterator(null/* fromKey */, null/* toKey */,
                IRangeQuery.DEFAULT);
        
    }

    /**
     * Return an iterator that visits the entries in a half-open key range but
     * filters the values.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive). When
     *            <code>null</code> there is no upper bound.
     * @param flags
     *            indicating whether the keys and/or values will be
     *            materialized.
     */
    public ITupleIterator rangeIterator(final byte[] fromKey,
            final byte[] toKey, final int flags) {

        return new PostOrderEntryIterator(btree, postOrderIterator(fromKey,
                toKey), fromKey, toKey, flags);

    }

    /**
     * Post-order traversal of nodes and leaves in the tree with a key range
     * constraint. For any given node, its children are always visited before
     * the node itself (hence the node occurs in the post-order position in the
     * traversal). The iterator is NOT safe for concurrent modification.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return Iterator visiting {@link AbstractNode}s.
     */
    abstract public Iterator<AbstractNode> postOrderIterator(byte[] fromKey, byte[] toKey);

    /**
     * Helper class expands a post-order node and leaf traversal to visit the
     * entries in the leaves.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class PostOrderEntryIterator implements ITupleIterator {
        
        private final Tuple tuple;
        private final IStriterator src;

        public boolean hasNext() {
            
            return src.hasNext();
            
        }
        
        public ITuple next() {
            
            // Note: Expanded converts from node iterator to tuple iterator.

            return (ITuple) src.next();
            
        }
        
        public void remove() {
            
            src.remove();
            
        }
        
        public PostOrderEntryIterator(final AbstractBTree btree,
                final Iterator postOrderNodeIterator, final byte[] fromKey,
                final byte[] toKey, int flags) {
            
            assert postOrderNodeIterator != null;
            
            this.tuple = new Tuple(btree, flags);
            
            this.src = new Striterator(postOrderNodeIterator);
            
            src.addFilter(new Expander() {

                private static final long serialVersionUID = 1L;

                /*
                 * Expand the value objects for each leaf visited in the
                 * post-order traversal.
                 */
                protected Iterator expand(final Object childObj) {
                    
                    // A child of this node.
                    final AbstractNode<?> child = (AbstractNode<?>) childObj;

                    if (child instanceof Leaf) {

                        final Leaf leaf = (Leaf) child;

                        if (leaf.getKeys().isEmpty()) {

                            return EmptyTupleIterator.INSTANCE;

                        }

                        return new LeafTupleIterator(leaf, tuple, fromKey, toKey);

//                        return ((Leaf)child).entryIterator();

                    } else {

                        return EmptyTupleIterator.INSTANCE;

                    }
                }

            });
        
        }

    }
        
    /**
     * <p>
     * Invariants:
     * <ul>
     * <li>A node with nkeys + 1 children.</li>
     * <li>A node must have between [m/2:m] children (alternatively, between
     * [m/2-1:m-1] keys since nkeys + 1 == nchildren for a node).</li>
     * <li>A leaf has no children and has between [m/2:m] key-value pairs (the
     * same as the #of children on a node).</li>
     * <li>The root leaf may be deficient (may have less than m/2 key-value
     * pairs).</li>
     * </ul>
     * where <code>m</code> is the branching factor and a node is understood
     * to be a non-leaf node in the tree.
     * </p>
     * <p>
     * In addition, all leaves are at the same level (not tested by this
     * assertion).
     * </p>
     */
    protected final void assertInvariants() {

        /*
         * Either the root or the parent is reachable.
         */
        final IAbstractNode root = btree.root;

        assert root == this
                || (this.parent != null && this.parent.get() != null);

        if (root != this) {

            if ((btree instanceof IndexSegment)) {

                /*
                 * @todo back out underflow support.
                 * The leaves and nodes of an IndexSegment are allowed to
                 * underflow down to one key when the IndexSegment was generated
                 * using an overestimate of the actual tuple count.
                 */
                assert getKeyCount() >= 1;

            } else {

                // not the root, so the min #of keys must be observed.
                assert getKeyCount() >= minKeys();

            }

        }

        // max #of keys.
        assert getKeyCount() <= maxKeys();

    }
    
    /**
     * Verify keys are monotonically increasing.
     */
    protected final void assertKeysMonotonic() {

        if (getKeys() instanceof MutableKeyBuffer) {

            /*
             * iff mutable keys - immutable keys should be checked during
             * de-serialization or construction.
             */

            ((MutableKeyBuffer) getKeys()).assertKeysMonotonic();

        }

    }
    
    /**
     * Return a human readable representation of the key. The key is a variable
     * length unsigned byte[]. The returned string is a representation of that
     * unsigned byte[].  This is use a wrapper for {@link BytesUtil#toString()}.
     * 
     * @param key
     *            The key.
     */
    final static protected String keyAsString(final byte[] key) {
        
        return BytesUtil.toString(key);
        
    }
    
    /**
     * Copy a key from the source node into this node. This method does not
     * modify the source node. This method does not update the #of keys in this
     * node.
     * <p>
     * Note: Whenever possible the key reference is copied rather than copying
     * the data. This optimization is valid since we never modify the contents
     * of a key.
     * 
     * @param dstpos
     *            The index position to which the key will be copied on this
     *            node.
     * @param srckeys
     *            The source keys.
     * @param srcpos
     *            The index position from which the key will be copied.
     */
    final protected void copyKey(final int dstpos,
            final IRaba srckeys, final int srcpos) {

        assert dirty;
        
        ((MutableKeyBuffer) getKeys()).keys[dstpos] = srckeys.get(srcpos);
        
    }

    abstract public boolean isLeaf();

    final public int getBranchingFactor() {
        
        return btree.branchingFactor;
        
    }
    
    /**
     * <p>
     * Split a node or leaf that is over capacity (by one).
     * </p>
     * 
     * @return The high node (or leaf) created by the split.
     */
    abstract protected IAbstractNode split();
    
    /**
     * <p>
     * Join this node (must be deficient) with either its left or right sibling.
     * A join will either cause a single key and value (child) to be
     * redistributed from a sibling to this leaf (node) or it will cause a
     * sibling leaf (node) to be merged into this leaf (node). Both situations
     * also cause the separator key in the parent to be adjusted.
     * </p>
     * <p>
     * Join is invoked when a leaf has become deficient (too few keys/values).
     * This method is never invoked for the root leaf therefore the parent of
     * this leaf must be defined. Further, since the minimum #of children is two
     * (2) for the smallest branching factor three (3), there is always a
     * sibling to consider.
     * </p>
     * <p>
     * Join first considers the immediate siblings. if either is materialized
     * and has more than the minimum #of values, then it redistributes one key
     * and value (child) from the sibling into this leaf (node). If either
     * sibling is materialized and has only the minimum #of values, then it
     * merges this leaf (node) with that sibling.
     * </p>
     * <p>
     * If no materialized immediate sibling meets these criteria, then first
     * materialize and test the right sibling. if the right sibling does not
     * meet these criteria, then materialize and test the left sibling.
     * </p>
     * <p>
     * Note that (a) we prefer to merge a materialized sibling with this leaf to
     * materializing a sibling; and (b) merging siblings is the only way that a
     * separator key is removed from a parent. If the parent becomes deficient
     * through merging then join is invoked on the parent as well. Note that
     * join is never invoked on the root node (or leaf) since it by definition
     * has no siblings.
     * </p>
     * <p>
     * Note that we must invoked copy-on-write before modifying a sibling.
     * However, the parent of the leaf MUST already be mutable (aka dirty) since
     * that is a precondition for removing a key from the leaf. This means that
     * copy-on-write will not force the parent to be cloned.
     * </p>
     */
    protected void join() {
        
        /*
         * copyOnWrite() wants to know the child that triggered the action when
         * that information is available. However we do not have that
         * information in this case so we use a [null] trigger.
         */
//        final AbstractNode t = null; // a [null] trigger node.
        final long triggeredByChildId = NULL;

        // verify that this node is deficient.
        assert getKeyCount() < minKeys();
        // verify that this leaf is under minimum capacity by one key.
        assert getKeyCount() == minKeys() - 1;
        // verify that the node is mutable.
        assert isDirty();
        assert !isPersistent();
        // verify that the leaf is not the root.
        assert ((BTree)btree).root != this;
        
        final Node parent = getParent();

        if (INFO) {
            log.info("this="+this);
//            if(DEBUG) {
//                System.err.println("this"); dump(Level.DEBUG,System.err);
//            }
        }
        
        if( isLeaf() ) {

            btree.getBtreeCounters().leavesJoined++;

        } else {
            
            btree.getBtreeCounters().nodesJoined++;

        }

        /*
         * Look for, but do not materialize, the left and right siblings.
         * 
         * Note that we defer invoking copy-on-write for the left/right sibling
         * until we are sure which sibling we will use.
         */
        
        AbstractNode<?> rightSibling = parent.getRightSibling(this, false);

        AbstractNode<?> leftSibling = parent.getLeftSibling(this, false);

        /*
         * Prefer a sibling that is already materialized with enough keys to
         * share.
         */
        if (rightSibling != null
                && rightSibling.getKeyCount() > rightSibling.minKeys()) {

            redistributeKeys(rightSibling.copyOnWrite(triggeredByChildId), true);

            return;

        }

        if (leftSibling != null
                && leftSibling.getKeyCount() > leftSibling.minKeys()) {

            redistributeKeys(leftSibling.copyOnWrite(triggeredByChildId), false);

            return;

        }

        /*
         * If either sibling was not materialized, then materialize and test
         * that sibling.
         */
        if (rightSibling == null) {

            rightSibling = parent.getRightSibling(this, true);

            if (rightSibling != null
                    && rightSibling.getKeyCount() > rightSibling.minKeys()) {

                redistributeKeys(rightSibling.copyOnWrite(triggeredByChildId),
                        true);

                return;

            }

        }

        if (leftSibling == null) {

            leftSibling = parent.getLeftSibling(this, true);

            if (leftSibling != null
                    && leftSibling.getKeyCount() > leftSibling.minKeys()) {

                redistributeKeys(leftSibling.copyOnWrite(triggeredByChildId),false);
                
                return;
                
            }

        }

        /*
         * by now the left and right siblings have both been materialized. At
         * least one sibling must be non-null. Since neither sibling was over
         * the minimum, we now merge this node with a sibling and remove the
         * separator key from the parent.
         */
        
        if (rightSibling != null) {

            merge(rightSibling, true);

            return;

        } else if (leftSibling != null) {

            merge(leftSibling, false);

            return;
            
        } else {
            
            throw new AssertionError();
            
        }
        
    }
    
    /**
     * Return <code>true</code> if this node is the left-most node at its
     * level within the tree.
     * 
     * @return <code>true</code> iff the child is the left-most node at its
     *         level within the tree.
     */
    protected boolean isLeftMostNode() {

        final Node p = getParent();

        if (p == null) {

            // always true of the root.
            return true;

        }

        final int i = p.getIndexOf(this);

        if (i == 0) {

            /*
             * We are the left-most child of our parent node. Now recursively
             * check our parent and make sure that it is the left-most child of
             * its parent. This continues recursively until we either discover
             * an ancestor which is not the left-most child of its parent or we
             * reach the root.
             */

            return p.isLeftMostNode();
            
        }

        return false;
        
    }
    
    /**
     * Return <code>true</code> if this node is the right-most node at its
     * level within the tree.
     * 
     * @return <code>true</code> iff the child is the right-most node at its
     *         level within the tree.
     */
    protected boolean isRightMostNode() {

        final Node p = getParent();

        if (p == null) {

            // always true of the root.
            return true;

        }

        /*
         * Note: test against the #of keys in the parent to determine if we are
         * the right-most child, not the #of keys in this node.
         */
        
        final int i = p.getIndexOf(this);

        if (i == p.getKeyCount()) {

            /*
             * We are the right-most child of our parent node. Now recursively
             * check our parent and make sure that it is the right-most child of
             * its parent. This continues recursively until we either discover
             * an ancestor which is not the right-most child of its parent or we
             * reach the root.
             */

            return p.isRightMostNode();
            
        }

        return false;

    }
    
    /**
     * Redistribute the one key from the sibling into this node.
     * 
     * @param sibling
     *            The sibling.
     * @param isRightSibling
     *            True iff the sibling is the rightSibling of this node.
     * 
     * @todo redistribution should proceed until the node and the sibling have
     *       an equal #of keys (or perhaps more exactly until the node would
     *       have more keys than the sibling if another key was redistributed
     *       into the node from the sibling). this takes advantage of the fact
     *       that the node and the sibling are known to be in memory to bring
     *       them to the point where they are equally full. along the same lines
     *       when both siblings are resident we could actually redistribute keys
     *       from both siblings into the node until the keys were equally
     *       distributed among the node and its siblings.
     * 
     * @todo a b*-tree variant simply uses redistribution of keys among siblings
     *       during insert to defer a split until the node and its siblings are
     *       all full.
     */
    abstract protected void redistributeKeys(AbstractNode sibling,
            boolean isRightSibling);

    /**
     * Merge the sibling into this node.
     * 
     * @param sibling
     *            The sibling.
     * @param isRightSibling
     *            True iff the sibling is the rightSibling of this node.
     */
    abstract protected void merge(AbstractNode sibling, boolean isRightSibling);

    /**
     * Insert or update a value.
     * 
     * @param key
     *            The key (non-null).
     * @param val
     *            The value (may be null).
     * @param delete
     *            <code>true</code> iff the entry is to marked as deleted
     *            (delete markers must be supported for if this is true).
     * @param timestamp
     *            The timestamp associated with the version (the value is
     *            ignored unless version metadata is being maintained).
     * @param tuple
     *            A tuple that may be used to obtain the data and metadata for
     *            the pre-existing index entry overwritten by the insert
     *            operation (optional).
     * 
     * @return The <i>tuple</i> iff there was a pre-existing entry under that
     *         key and <code>null</code> otherwise.
     */
    abstract public Tuple insert(byte[] key, byte[] val, boolean delete, long timestamp,
            Tuple tuple);

    /**
     * Recursive search locates the appropriate leaf and removes the entry for
     * the key.
     * <p>
     * Note: It is an error to call this method if delete markers are in use.
     * 
     * @param searchKey
     *            The search key.
     * @param tuple
     *            A tuple that may be used to obtain the data and metadata for
     *            the pre-existing index entry that was either removed by the
     *            remove operation (optional).
     * 
     * @return The <i>tuple</i> iff there was a pre-existing entry under that
     *         key and <code>null</code> otherwise.
     */
    abstract public Tuple remove(byte[] searchKey,Tuple tuple);
    
    /**
     * Lookup a key.
     * 
     * @param searchKey
     *            The search key.
     * @param tuple
     *            A tuple that may be used to obtain the data and metadata for
     *            the pre-existing index entry (required).
     * 
     * @return The <i>tuple</i> iff there was a pre-existing entry under that
     *         key and <code>null</code> otherwise.
     */
    abstract public Tuple lookup(byte[] searchKey, Tuple tuple);
    
    /**
     * Recursive search locates the appropriate leaf and returns the index
     * position of the entry.
     * 
     * @param searchKey
     *            The search key.
     * 
     * @return the index of the search key, if found; otherwise,
     *         <code>(-(insertion point) - 1)</code>. The insertion point is
     *         defined as the point at which the key would be found it it were
     *         inserted into the btree without intervening mutations. Note that
     *         this guarantees that the return value will be >= 0 if and only if
     *         the key is found.
     */
    abstract public int indexOf(byte[] searchKey);
    
    /**
     * Recursive search locates the entry at the specified index position in the
     * btree and returns the key for that entry.
     * 
     * @param index
     *            The index position of the entry (origin zero and relative to
     *            this node or leaf).
     * 
     * @return The key at that index position.
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than the #of entries.
     */
    abstract public byte[] keyAt(int index);
    
    /**
     * Recursive search locates the entry at the specified index position in the
     * btree and returns the value for that entry.
     * 
     * @param index
     *            The index position of the entry (origin zero and relative to
     *            this node or leaf).
     * @param tuple 
     *            A tuple that may be used to obtain the data and metadata for
     *            the pre-existing index entry (required).
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than the #of entries.
     */
    abstract public void valueAt(int index, Tuple tuple);
    
    /**
     * Dump the data onto the {@link PrintStream} (non-recursive).
     * 
     * @param out
     *            Where to write the dump.
     * 
     * @return True unless an inconsistency was detected.
     */
    public boolean dump(final PrintStream out) {

        return dump(BTree.dumpLog.getEffectiveLevel(), out);

    }

    /**
     * Dump the data onto the {@link PrintStream}.
     * 
     * @param level
     *            The logging level.
     * @param out
     *            Where to write the dump.
     *            
     * @return True unless an inconsistency was detected.
     */
    public boolean dump(final Level level, final PrintStream out) {

        return dump(level, out, -1, false);

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
     *            invoke this method on a node or leaf whose height in the tree
     *            is not known.
     * @param recursive
     *            When true, the node will be dumped recursively using a
     *            pre-order traversal.
     * 
     * @return True unless an inconsistency was detected.
     */
    abstract public boolean dump(Level level, PrintStream out, int height, boolean recursive);

    /**
     * Returns a string that may be used to indent a dump of the nodes in
     * the tree.
     * 
     * @param height
     *            The height.
     *            
     * @return A string suitable for indent at that height.
     */
    protected static String indent(final int height) {

        if( height == -1 ) {
        
            // The height is not defined.
            
            return "";
            
        }
        
        return ws.substring(0, height * 4);

    }

    private static final transient String ws = "                                                                                                                                                                                                                  ";

}
