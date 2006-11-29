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

import com.bigdata.cache.HardReferenceQueue;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * Abstract node.
 * </p>
 * <p>
 * Note: For nodes in the index, the attributes dirty and persistent are 100%
 * correlated. Since only transient nodes may be dirty and only persistent nodes
 * are clean any time one is true the other is false.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractNode extends PO {

    /**
     * The BTree.
     * 
     * Note: This field MUST be patched when the node is read from the store.
     * This requires a custom method to read the node with the btree reference
     * on hand so that we can set this field.
     */
    final transient protected BTree btree;

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
     */
    protected int nkeys = 0;

    /**
     * The external keys for the B+Tree. The #of keys depends on whether this is
     * a {@link Node} or a {@link Leaf}. A leaf has one key per value - that
     * is, the maximum #of keys for a leaf is specified by the branching factor.
     * In contrast a node has m-1 keys where m is the maximum #of children (aka
     * the branching factor). Therefore this field is initialized by the
     * {@link Leaf} or {@link Node} - NOT by the {@link AbstractNode}.
     * 
     * The interpretation of the key index for a leaf is one to one - key[0]
     * corresponds to value[0].
     * 
     * For both a {@link Node} and a {@link Leaf}, this array is dimensioned to
     * one more than the maximum capacity so that the key that causes overflow
     * and forces the split may be inserted.  This greatly simplifies the logic
     * for computing the split point and performing the split.
     * 
     * @see #findChild(int key)
     * @see Search#search(int, int[], int)
     */
    protected int[] keys;

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
             */

            btree.store.delete(btree.asSlots(identity));
            
        }
        
        deleted = true;
        
    }

    /**
     * The parent iff the node has been added as the child of another node
     * and the parent reference has not been cleared.
     * 
     * @return The parent.
     */
    public Node getParent() {

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
    protected AbstractNode(BTree btree, int branchingFactor) {

        assert btree != null;

        assert branchingFactor>=BTree.MIN_BRANCHING_FACTOR;
        
        this.btree = btree;

        this.branchingFactor = branchingFactor;

//        // true iff the branching factor is odd (3,5,7,etc.)
//        final boolean odd = (branchingFactor&1) == 1;
        
        /*
         * Compute the minimum #of children/values. this is the same whether
         * this is a Node or a Leaf.
         */
        final int minChildren = (branchingFactor+1)>>1;
        
        this.minKeys = isLeaf() ? minChildren : minChildren - 1;
        
        // The maximum #of keys is easy to compute.
        this.maxKeys = isLeaf() ? branchingFactor : branchingFactor - 1;
        
        /*
         * If this is a {@link Node} then ensures that the btree holds a hard
         * reference to the node.
         */
        if( !isLeaf() ) {
                
            btree.nodes.add((Node)this);

        }

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

        assert src == btree.getRoot()
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
    protected AbstractNode copyOnWrite() {
        
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

            AbstractNode newNode;

            if (this instanceof Node) {

                newNode = new Node((Node) this, triggeredByChild );

            } else {

                newNode = new Leaf((Leaf) this);

            }

            Node parent = this.getParent();

            if (btree.root == this) {

                assert parent == null;

                // Update the root node on the btree.
                BTree.log.info("Copy-on-write : replaced root node on btree.");

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
             * Since a clone was requested, we use this as an opportunity to 
             * append a leaf onto the hard reference queue.  This helps us to
             * ensure that leaves which have been touched recently will remain
             * strongly reachable. 
             */
            if( isLeaf() ) {

                btree.touch(this);
                
            }
            
            return this;

        }

    }

    /**
     * Post-order traveral of nodes and leaves in the tree. For any given
     * node, its children are always visited before the node itself (hence
     * the node occurs in the post-order position in the traveral). The
     * iterator is NOT safe for concurrent modification.
     */
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
     */
    abstract public Iterator postOrderIterator(boolean dirtyNodesOnly);

    /**
     * Traversal of index values in key order.
     */
    public Iterator entryIterator() {

        /*
         * Begin with a post-order iterator.
         */
        return new Striterator(postOrderIterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand the value objects for each leaf visited in the post-order
             * traversal.
             */
            protected Iterator expand(Object childObj) {
                /*
                 * A child of this node.
                 */
                AbstractNode child = (AbstractNode) childObj;

                if (child instanceof Leaf) {

                    return ((Leaf) child).entryIterator();

                } else {

                    return EmptyIterator.DEFAULT;

                }
            }
        });

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

        try {

            /*
             * either the root or the parent is reachable.
             */
            assert btree.root == this
                    || (this.parent != null && this.parent.get() != null);

            if (btree.root != this) {

                // not the root, so the min #of keys must be observed.
                assert nkeys >= minKeys;

            }

            // max #of keys.
            assert nkeys <= maxKeys;

        } catch (AssertionError ex) {

            BTree.log.fatal("Invariants failed\n"
                    + ex.getStackTrace()[0].toString());
            
            dump(Level.FATAL, System.err);
            
            throw ex;
            
        }

    }

    /**
     * True iff this is a leaf node.
     */
    abstract public boolean isLeaf();

    /**
     * <p>
     * Split a node or leaf that is over capacity (by one).
     * </p>
     * 
     * @return The high node (or leaf) created by the split.
     */
    abstract protected AbstractNode split();
    
    /**
     * Join this node (must be deficient) with either its left or right sibling.
     * 
     * Invoked when a leaf has become deficient (too few keys/values). This
     * method is never invoked for the root leaf therefore the parent of this
     * leaf must be defined. Further, since the minimum #of children is two (2)
     * for the smallest branching factor three (3), there is always a sibling to
     * consider.
     * 
     * consider the immediate siblings. if either is materialized and has more
     * than the minimum #of values, then redistribute the values evenly between
     * this leaf and that sibling and update the separation key in the parent to
     * reflect the new partitioning of the values between the leaves. if either
     * is materialized and has only the minimum #of values, then merge this leaf
     * with that sibling and update the parent by removing the separator key.
     * 
     * If no materialized immediate sibling meets these criteria, then first
     * materialize and test the right sibling. if the right sibling does not
     * meet these criteria, then materialize and test the left sibling.
     * 
     * Note that (a) we prefer to merge a materialized sibling with this leaf to
     * materializing a sibling; and (b) merging siblings is the only way that a
     * separator key is removed from a parent. If the parent becomes deficient
     * through merging then join is invoked on the parent as well. Note that
     * join is never invoked on the root node (or leaf) since it by definition
     * has no siblings.
     * 
     * Note that we must invoked copy-on-write before modifying a sibling.
     * However, the parent of the leaf MUST already be mutable (aka dirty) since
     * that is a precondition for removing a key from the leaf. This means that
     * copy-on-write will not force the parent to be cloned.
     * 
     * Note: this logic in this method is identical for nodes and leaves and
     * could be refactored into {@link AbstractNode}. However, the logic for
     * merging and redistributing keys is specific to a {@link Node} or a
     * {@link Leaf} since they have different internal arrays that must be
     * considered.
     */
    protected void join() {

        // verify that this node is deficient.
        assert nkeys < minKeys;
        // verify that the node is mutable.
        assert isDirty();
        assert !isPersistent();
        // verify that the leaf is not the root.
        assert btree.root != this;
        
        final Node parent = getParent();

        /*
         * Look for, but do not materialize, the left and right siblings.
         * 
         * Note that we defer invoking copy-on-write for the left/right sibling
         * until we are sure which sibling we will use.
         */
        
        AbstractNode rightSibling = (AbstractNode) parent.getRightSibling(this,false);
        
        AbstractNode leftSibling = (AbstractNode) parent.getLeftSibling(this,false);
        
        /*
         * prefer a sibling that is already materialized.
         */
        if( rightSibling != null && rightSibling.nkeys>rightSibling.minKeys ) {

            redistributeKeys(rightSibling.copyOnWrite());
            
            return;
            
        }

        if( leftSibling != null && leftSibling.nkeys>leftSibling.minKeys ) {

            redistributeKeys(leftSibling.copyOnWrite());
            
            return;
            
        }
        
        /*
         * if either sibling was not materialized, then materialize and test
         * that sibling.
         */
        if( rightSibling == null ) {
            
            rightSibling = parent.getRightSibling(this,true);
            
            if( rightSibling != null && rightSibling.nkeys>rightSibling.minKeys  ) {

                redistributeKeys(rightSibling.copyOnWrite());
                
                return;
                
            }

        }

        if( leftSibling == null ) {
            
            leftSibling = parent.getLeftSibling(this,true);
            
            if( leftSibling != null && leftSibling.nkeys>leftSibling.minKeys ) {

                redistributeKeys(leftSibling.copyOnWrite());
                
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
            
            merge(rightSibling);
            
            return;
            
        } else if( leftSibling != null ) {
            
            merge(leftSibling);
            
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
     */
    abstract protected void redistributeKeys(AbstractNode sibling);
    
    /**
     * Merge the sibling into this node.
     * 
     * @param sibling
     *            The sibling.
     */
    abstract protected void merge(AbstractNode sibling);

    /**
     * Recursive search locates the approprate leaf and inserts the entry under
     * the key. The leaf is split iff necessary. Splitting the leaf can cause
     * splits to cascade up towards the root. If the root is split then the
     * total depth of the tree is inceased by one.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The value.
     * 
     * @return The previous value or <code>null</code> if the key was not
     *         found.
     */
    abstract public Object insert(int key, Object entry);

    /**
     * Recursive search locates the appropriate leaf and removes and returns
     * the pre-existing value stored under the key (if any).
     * 
     * @param key
     *            The external key.
     *            
     * @return The value or null if there was no entry for that key.
     */
    abstract public Object remove(int key);

    /**
     * Recursive search locates the entry for the probe key.
     * 
     * @param key
     *            The external key.
     * 
     * @return The entry or <code>null</code> iff there is no entry for
     *         that key.
     */
    abstract public Object lookup(int key);

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

    private static final String ws = "                                                                                                                                                                                                                  ";

}
