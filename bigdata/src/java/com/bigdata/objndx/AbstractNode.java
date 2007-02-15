/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 15, 2006
 */
package com.bigdata.objndx;

import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.objndx.EntryIterator.EntryFilter;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Striterator;

/**
 * Abstract node supporting incremental persistence and copy-on-write semantics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractNode extends PO implements IAbstractNode,
        IAbstractNodeData {

    /**
     * Log for node and leaf operations.
     * <dl>
     * <dt>info</dt>
     * <dd> A high level trace of insert, split, joint, and remove operations.
     * You MUST test on {@link #INFO} before generating log messages at this
     * level to avoid string concatenation operations would otherwise kill
     * performance.</dd>
     * <dt></dt>
     * <dd> A low level trace including a lot of dumps of leaf and node state. *
     * You MUST test on {@link #DEBUG} before generating log messages at this
     * level to avoid string concatenation operations would otherwise kill
     * performance.</dd>
     * </dl>
     * 
     * @see BTree#log
     * @see BTree#dumpLog
     */
    protected static final Logger log = Logger.getLogger(AbstractNode.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO.toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG.toInt();

    /**
     * The BTree.
     * 
     * Note: This field MUST be patched when the node is read from the store.
     * This requires a custom method to read the node with the btree reference
     * on hand so that we can set this field.
     */
    final transient protected AbstractBTree btree;

    /**
     * The branching factor (#of slots for keys or values).
     */
    final transient protected int branchingFactor;
    
    /**
     * The minimum #of keys. For a {@link Node}, the minimum #of children is
     * <code>minKeys + 1</code>. For a {@link Leaf}, the minimum #of values
     * is <code>minKeys</code>.
     */
    final transient protected int minKeys;
    
    /**
     * The maximum #of keys.  For a {@link Node}, the maximum #of children is
     * <code>maxKeys + 1</code>.  For a {@link Leaf}, the maximum #of values is
     * <code>maxKeys</code>.
     */
    final transient protected int maxKeys;

    /**
     * The #of valid keys for this node or leaf.  For a {@link Node}, the #of
     * children is always <code>nkeys+1</code>.  For a {@link Leaf}, the #of
     * values is always the same as the #of keys.
     * 
     * FIXME deprecate since also maintained by {@link IKeyBuffer}.
     */
    protected int nkeys = 0;
    
    /**
     * A representation of each key in the node or leaf. Each key is as a
     * variable length unsigned byte[]. There are various implementations of
     * {@link IKeyBuffer} that are optimized for mutable and immutable nodes.
     * 
     * The #of keys depends on whether this is a {@link Node} or a {@link Leaf}.
     * A leaf has one key per value - that is, the maximum #of keys for a leaf
     * is specified by the branching factor. In contrast a node has m-1 keys
     * where m is the maximum #of children (aka the branching factor). Therefore
     * this field is initialized by the {@link Leaf} or {@link Node} - NOT by
     * the {@link AbstractNode}.
     * 
     * For both a {@link Node} and a {@link Leaf}, this array is dimensioned to
     * accept one more key than the maximum capacity so that the key that causes
     * overflow and forces the split may be inserted. This greatly simplifies
     * the logic for computing the split point and performing the split.
     * Therefore you always allocate this object with a capacity <code>m</code>
     * keys for a {@link Node} and <code>m+1</code> keys for a {@link Leaf}.
     * 
     * @see Node#findChild(int searchKeyOffset, byte[] searchKey)
     * @see IKeyBuffer#search(int searchKeyOffset, byte[] searchKey)
     */
    protected IKeyBuffer keys;
    
    /**
     * The parent of this node. This is null for the root node. The parent is
     * required in order to set the persistent identity of a newly persisted
     * child node on its parent. The reference to the parent will remain
     * strongly reachable as long as the parent is either a root (held by the
     * {@link BTree}) or a dirty child (held by the {@link Node}). The parent
     * reference is set when a node is attached as the child of another node.
     * 
     * Note: When a node is cloned by {@link #copyOnWrite()} the parent
     * references for its <em>clean</em> children are set to the new copy of
     * the node. This is refered to in several places as "stealing" the children
     * since they are no longer linked back to their old parents via their
     * parent reference.
     */
    protected WeakReference<Node> parent = null;

    /**
     * The #of times that this node is present on the {@link HardReferenceQueue} .
     * This value is incremented each time the node is added to the queue and is
     * decremented each time the node is evicted from the queue. On eviction, if
     * the counter is zero(0) after it is decremented then the node is written
     * on the store. This mechanism is critical because it prevents a node
     * entering the queue from forcing IO for the same node in the edge case
     * where the node is also on the tail on the queue. Since the counter is
     * incremented before it is added to the queue, it is guarenteed to be
     * non-zero when the node forces its own eviction from the tail of the
     * queue. Preventing this edge case is important since the node can
     * otherwise become immutable at the very moment that it is touched to
     * indicate that we are going to update its state, e.g., during an insert,
     * split, or remove operation. This mechanism also helps to defer IOs since
     * IO can not occur until the last reference to the node is evicted from the
     * queue.
     */
    protected int referenceCount = 0;

    public void delete() {
        
        if( deleted ) {
            
            throw new IllegalStateException();
            
        }

        if( identity != NULL ) {
            
            /*
             * Deallocate the object on the store.
             * 
             * Note: This operation is not meaningful on an append only store.
             * If a read-write store is defined then this is where you would
             * delete the old version.
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
     */
    protected AbstractNode(AbstractBTree btree, int branchingFactor) {

        assert btree != null;

        assert branchingFactor>=BTree.MIN_BRANCHING_FACTOR;
        
        this.btree = btree;

        this.branchingFactor = branchingFactor;

        /*
         * Compute the minimum #of children/values. this is the same whether
         * this is a Node or a Leaf.
         */
        final int minChildren = (branchingFactor+1)>>1;
        
        this.minKeys = isLeaf() ? minChildren : minChildren - 1;
        
        // The maximum #of keys is easy to compute.
        this.maxKeys = isLeaf() ? branchingFactor : branchingFactor - 1;

        // Add to the hard reference queue.
        btree.touch(this);
        
    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node.
     */
    protected AbstractNode(AbstractNode src) {

        /*
         * Note: We do NOT clone the base class since this is a new
         * persistence capable object, but it is not yet persistent and we
         * do not want to copy the persistent identity of the source object.
         */
        this(src.btree,src.branchingFactor);

        // This node must be mutable (it is a new node).
        assert isDirty();
        assert !isPersistent();
        
        /* The source must not be dirty.  We are cloning it so that we can
         * make changes on it.
         */
        assert src != null;
        assert !src.isDirty();
        assert src.isPersistent();

        /*
         * Copy the parent reference. The parent must be defined unless the
         * source is the current root.
         * 
         * Note that we reuse the weak reference since it is immutable (it state
         * is only changed by the VM, not by the application).
         */

        assert src == btree.root
                || (src.parent != null && src.parent.get() != null);
        
        this.parent = src.parent;
        
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
    protected IAbstractNode copyOnWrite() {
        
        // Always invoked first for a leaf and thereafter in its other form.
        assert isLeaf();
        
        return copyOnWrite(null);
        
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
     * made, and MUST result in an awareness in the caller that the copy exists
     * and needs to be used in place of the immutable version of the node.
     * </p>
     * 
     * @param triggeredByChild
     *            The child that triggered this event if any.
     * 
     * @return Either this node or a copy of this node.
     */
    protected AbstractNode copyOnWrite(AbstractNode triggeredByChild) {

        if (isPersistent()) {

            if(INFO) {
                log.info("this="+this+", trigger="+triggeredByChild);
                if( DEBUG ) {
                    System.err.println("this"); dump(Level.DEBUG,System.err);
                    if( triggeredByChild != null ) {
                        System.err.println("trigger"); triggeredByChild.dump(Level.DEBUG,System.err);
                    }
                }
            }

            // cast to mutable implementation class.
            final BTree btree = (BTree) this.btree;
            
            AbstractNode newNode;

            if (this instanceof Node) {

                newNode = new Node((Node) this, triggeredByChild );
                
                btree.counters.nodesCopyOnWrite++;

            } else {

                newNode = new Leaf((Leaf) this);

                btree.counters.leavesCopyOnWrite++;

            }

            Node parent = this.getParent();

            if (btree.root == this) {

                assert parent == null;

                // Update the root node on the btree.
                log.info("Copy-on-write : replaced root node on btree.");

                btree.root = newNode;

            } else {

                /*
                 * Recursive copy-on-write up the tree. This operations stops as
                 * soon as we reach a parent node that is already dirty and
                 * grounds out at the root in any case.
                 */
                assert parent != null;

                if (!parent.isDirty()) {

                    /*
                     * Note: pass up the old child since we want to avoid having
                     * its parent reference reset.
                     */
                    parent = (Node) parent.copyOnWrite(this);

                }

                /*
                 * Replace the reference to this child with the reference to the
                 * new child. This makes the old child inaccessible via
                 * navigation. It will be GCd once it falls off of the hard
                 * reference queue.
                 */
                parent.replaceChildRef(this.getIdentity(), newNode);

            }

            return newNode;

        } else {

            /*
             * Since a clone was not required, we use this as an opportunity to
             * touch the hard reference queue. This helps us to ensure that
             * nodes which have been touched recently will remain strongly
             * reachable.
             */
            btree.touch(this);
            
            return this;

        }

    }

    public Iterator postOrderIterator() {

        return postOrderIterator(false);

    }

    /**
     * Post-order traveral of nodes and leaves in the tree. For any given
     * node, its children are always visited before the node itself (hence
     * the node occurs in the post-order position in the traveral). The
     * iterator is NOT safe for concurrent modification.
     * 
     * @param dirtyNodesOnly
     *            When true, only dirty nodes and leaves will be visited
     *            
     * @return Iterator visiting {@link AbstractNode}s.
     */
    abstract public Iterator postOrderIterator(boolean dirtyNodesOnly);

    public IEntryIterator entryIterator() {

        return new PostOrderEntryIterator(postOrderIterator());
        
    }

    /**
     * Return an iterator that visits the entries in a half-open key range.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive). When
     *            <code>null</code> there is no upper bound.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        return new PostOrderEntryIterator(postOrderIterator(fromKey, toKey),
                fromKey, toKey, null);

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
     * @param filter
     *            An optional filter that will be applied to the values before
     *            they are visited by the returned iterator.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey, EntryFilter filter) {

        return new PostOrderEntryIterator(postOrderIterator(fromKey, toKey),
                fromKey, toKey, filter);

    }

    /**
     * Post-order traveral of nodes and leaves in the tree with a key range
     * constraint. For any given node, its children are always visited before
     * the node itself (hence the node occurs in the post-order position in the
     * traveral). The iterator is NOT safe for concurrent modification.
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
    abstract public Iterator postOrderIterator(byte[] fromKey, byte[] toKey);

    /**
     * Helper class expands a post-order node and leaf traversal to visit the
     * entries in the leaves.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * FIXME when using an {@link IndexSegment} provide for direct leaf
     * successor scans.
     */
    private static class PostOrderEntryIterator extends Striterator implements IEntryIterator {
        
        /**
         * The key-value for each entry are set as a side-effect on a private
         * {@link Tuple} field so that this class can implement
         * {@link IEntryIterator}.
         * 
         * @see EntryIterator#EntryIterator(Leaf, Tuple)
         */
        final Tuple tuple = new Tuple();
        
        public PostOrderEntryIterator(Iterator postOrderIterator) {
            
            super(postOrderIterator);
            
            addFilter(new Expander() {

                private static final long serialVersionUID = 1L;

                /*
                 * Expand the value objects for each leaf visited in the
                 * post-order traversal.
                 */
                protected Iterator expand(Object childObj) {
                    /*
                     * A child of this node.
                     */
                    AbstractNode child = (AbstractNode) childObj;

                    if (child instanceof Leaf) {

                        Leaf leaf = (Leaf) child;
                        
                        if (leaf.nkeys == 0) {

                            return EmptyEntryIterator.INSTANCE;

                        }

                        return new EntryIterator(leaf,tuple);

//                        return ((Leaf)child).entryIterator();

                    } else {

                        return EmptyEntryIterator.INSTANCE;

                    }
                }

            });
        
        }

        public PostOrderEntryIterator(Iterator postOrderIterator,
                final byte[] fromKey, final byte[] toKey,
                final EntryFilter filter) {
            
            super(postOrderIterator);
            
            addFilter(new Expander() {

                private static final long serialVersionUID = 1L;

                /*
                 * Expand the value objects for each leaf visited in the
                 * post-order traversal.
                 */
                protected Iterator expand(Object childObj) {
                    /*
                     * A child of this node.
                     */
                    AbstractNode child = (AbstractNode) childObj;

                    if (child instanceof Leaf) {

                        Leaf leaf = (Leaf) child;
                        
                        if (leaf.nkeys == 0) {

                            return EmptyEntryIterator.INSTANCE;

                        }

                        return new EntryIterator(leaf, tuple, fromKey, toKey, filter );

//                        return ((Leaf)child).entryIterator();

                    } else {

                        return EmptyEntryIterator.INSTANCE;

                    }
                }

            });
        
        }

        public byte[] getKey() {
            
            return tuple.key;
            
        }

        public Object getValue() {
            
            return tuple.val;
            
        }
        
    }
    
//    /**
//     * Traversal of index keys in key order.
//     */
//    public Iterator keyIterator() {
//        
//        return new Striterator(entryIterator()).addFilter(new Resolver() {
//
//            private static final long serialVersionUID = 1L;
//
//            protected Object resolve(Object arg0) {
//                return null;
//            }
//            
//        });
//        
//    }
    
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

        try {

            /*
             * either the root or the parent is reachable.
             */
            IAbstractNode root = btree.root;
            
            assert root == this
                    || (this.parent != null && this.parent.get() != null);

            if (root != this) {

                // not the root, so the min #of keys must be observed.
                assert nkeys >= minKeys;

            }

            // max #of keys.
            assert nkeys <= maxKeys;

        } catch (AssertionError ex) {

            log.fatal("Invariants failed\n"
                    + ex.getStackTrace()[0].toString());
            
            dump(Level.FATAL, System.err);
            
            throw ex;
            
        }

    }
    
    /**
     * Verify keys are monotonically increasing.
     */
    protected final void assertKeysMonotonic() {

        if(keys instanceof MutableKeyBuffer) {

            /*
             * iff mutable keys - immutable keys should be checked during
             * de-serialization or construction.
             */
            
            ((MutableKeyBuffer)keys).assertKeysMonotonic();
            
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
    final static protected String keyAsString(byte[] key) {
        
        return BytesUtil.toString(key);
        
    }
    
    /**
     * Copy a key from the source node into this node. This method does not
     * modify the source node. This method does not update the #of keys in this
     * node.
     * 
     * Note: Whenever possible the key reference is copied rather than copying
     * the data. This optimization is valid since we never modify the contents
     * of a key.
     * 
     * @param dstpos
     *            The index position to which the key will be copied on this
     *            node.
     * @param src
     *            The source keys.
     * @param srcpos
     *            The index position from which the key will be copied.
     * 
     * @todo move to {@link MutableKeyBuffer}
     */
    final protected void copyKey(int dstpos, IKeyBuffer srckeys, int srcpos) {

        assert dirty;
        
        ((MutableKeyBuffer)keys).keys[dstpos] = srckeys.getKey(srcpos);
        
    }

    abstract public boolean isLeaf();

    final public int getBranchingFactor() {
        
        return branchingFactor;
        
    }
    
    final public int getKeyCount() {
        
        return nkeys;
        
    }
    
    final public IKeyBuffer getKeys() {
        
        return keys;
        
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
        final AbstractNode t = null; // a [null] trigger node.

        // verify that this node is deficient.
        assert nkeys < minKeys;
        // verify that this leaf is under minimum capacity by one key.
        assert nkeys == minKeys - 1;
        // verify that the node is mutable.
        assert isDirty();
        assert !isPersistent();
        // verify that the leaf is not the root.
        assert ((BTree)btree).root != this;
        
        final Node parent = getParent();

        if (INFO) {
            log.info("this="+this);
            if(DEBUG) {
                System.err.println("this"); dump(Level.DEBUG,System.err);
            }
        }
        
        if( this instanceof Leaf ) {

            btree.counters.leavesJoined++;

        } else {
            
            btree.counters.nodesJoined++;

        }

        /*
         * Look for, but do not materialize, the left and right siblings.
         * 
         * Note that we defer invoking copy-on-write for the left/right sibling
         * until we are sure which sibling we will use.
         */
        
        AbstractNode rightSibling = (AbstractNode) parent.getRightSibling(this,
                false);

        AbstractNode leftSibling = (AbstractNode) parent.getLeftSibling(this,
                false);
        
        /*
         * prefer a sibling that is already materialized.
         */
        if (rightSibling != null && rightSibling.nkeys > rightSibling.minKeys) {

            redistributeKeys(rightSibling.copyOnWrite(t), true);

            return;

        }

        if (leftSibling != null && leftSibling.nkeys > leftSibling.minKeys) {

            redistributeKeys(leftSibling.copyOnWrite(t), false);

            return;

        }
        
        /*
         * if either sibling was not materialized, then materialize and test
         * that sibling.
         */
        if( rightSibling == null ) {
            
            rightSibling = parent.getRightSibling(this,true);
            
            if( rightSibling != null && rightSibling.nkeys>rightSibling.minKeys  ) {

                redistributeKeys(rightSibling.copyOnWrite(t),true);
                
                return;
                
            }

        }

        if( leftSibling == null ) {
            
            leftSibling = parent.getLeftSibling(this,true);
            
            if( leftSibling != null && leftSibling.nkeys>leftSibling.minKeys ) {

                redistributeKeys(leftSibling.copyOnWrite(t),false);
                
                return;
                
            }

        }

        /*
         * by now the left and right siblings have both been materialized. At
         * least one sibling must be non-null. Since neither sibling was over
         * the minimum, we now merge this node with a sibling and remove the
         * separator key from the parent.
         */
        
        if( rightSibling != null ) {
            
            merge(rightSibling,true);
            
            return;
            
        } else if( leftSibling != null ) {
            
            merge(leftSibling,false);
            
            return;
            
        } else {
            
            throw new AssertionError();
            
        }
        
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
     * Batch insert of one or more tuples.
     * 
     * The behavior for each tuple is equivilent to a recursive search that
     * locates the approprate leaf and inserts or updates a tuple under the key.
     * The leaf is split iff necessary. Splitting the leaf can cause splits to
     * cascade up towards the root. If the root is split then the total depth of
     * the tree is inceased by one.
     * 
     * This operation can be very efficient if the tuples are presented in key
     * order.
     * 
     * @return The #of tuples processed.
     */
    abstract public int batchInsert(BatchInsert op);

    /**
     * Batch lookup of one or more tuples.
     * 
     * @return The #of tuples processed.
     */
    abstract public int batchLookup(BatchLookup op);

    /**
     * Insert or update a value.
     * 
     * @param key
     *            The key (non-null).
     * @param val
     *            The value (may be null).
     * 
     * @return The old value (may be null) or null if there was no entry for
     *         that key.
     */
    abstract public Object insert(byte[] key,Object val);
    
    /**
     * Lookup a single key.
     * 
     * @param key
     *            The key.
     *            
     * @return The value associated with that key (which may be null) or null if
     *         there is no entry for that key.
     */
    abstract public Object lookup(byte[] key);
    
    /**
     * Batch existence testing for one or more keys.
     * 
     * @return The #of tuples processed.
     */
    abstract public int batchContains(BatchContains op);

    /**
     * Return true iff there is an entry for the search key (this method should
     * be used in place of lookup if null keys are allowed for an index).
     * 
     * @param searchKey
     *            The search key.
     * 
     * @return True iff the search key is found in the index.
     */
    abstract public boolean contains(byte[] searchKey);
    
    /**
     * Batch removal of one or more tuples, returning their existing values by
     * side-effect.
     * 
     * @return The #of tuples processed.
     */
    abstract public int batchRemove(BatchRemove op);

    /**
     * Recursive search locates the appropriate leaf and removes the entry for
     * the key (if any) returning the old value for that entry or null if the
     * key was not found.
     * 
     * @param searchKey
     *            The search key.
     * 
     * @return The old value for the search key (may be null) or null if the key
     *         was not found.
     */
    abstract public Object remove(byte[] searchKey);
    
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
     * 
     * @return The value at that index position.
     * 
     * @exception IndexOutOfBoundsException
     *                if index is less than zero.
     * @exception IndexOutOfBoundsException
     *                if index is greater than the #of entries.
     */
    abstract public Object valueAt(int index);
    
    /**
     * Dump the data onto the {@link PrintStream} (non-recursive).
     * 
     * @param out
     *            Where to write the dump.
     * 
     * @return True unless an inconsistency was detected.
     */
    public boolean dump(PrintStream out) {

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
    public boolean dump(Level level,PrintStream out) {

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
    protected static String indent(int height) {

        if( height == -1 ) {
        
            // The height is not defined.
            
            return "";
            
        }
        
        return ws.substring(0, height * 4);

    }

    private static final transient String ws = "                                                                                                                                                                                                                  ";

}
