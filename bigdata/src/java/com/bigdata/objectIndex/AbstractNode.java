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
package com.bigdata.objectIndex;

import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.util.Iterator;

import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;
import com.bigdata.objectIndex.TestSimpleBTree.PO;

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
     * Negative infinity for the external keys.
     */
    static final int NEGINF = 0;

    /**
     * Positive infinity for the external keys.
     */
    static final int POSINF = Integer.MAX_VALUE;

    /**
     * The BTree.
     * 
     * Note: This field MUST be patched when the node is read from the store.
     * This requires a custom method to read the node with the btree reference
     * on hand so that we can set this field.
     */
    transient protected BTree btree;

    /**
     * The branching factor (#of slots for keys or values).
     */
    transient protected int branchingFactor;

    /**
     * The #of valid keys for this node.
     */
    protected int nkeys = 0;

    /**
     * The external keys for the B+Tree. The #of keys depends on whether this is
     * a {@link Node} or a {@link Leaf}. A leaf has one key per value - that
     * is, the maximum #of keys for a leaf is specified by the branching factor.
     * In contrast a node has n-1 keys where n is the maximum #of children (aka
     * the branching factor). Therefore this field is initialized by the
     * {@link Leaf} or {@link Node} - NOT by the {@link AbstractNode}.
     * 
     * The interpretation of the key index for a leaf is one to one - key[0]
     * corresponds to value[0].
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
     */
    protected WeakReference<Node> parent = null;

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
         * 
         * FIXME Restore this assertion - it is commented out to allow
         * the {@link TestNodeSerializer} to run.
         */
//        assert (this == btree.root && p == null) || p != null;

        return p;

    }

    /**
     * De-serialization constructor used by subclasses.
     */
    protected AbstractNode() {

    }

    /**
     * 
     * @param btree
     * @param branchingFactor
     */
    protected AbstractNode(BTree btree, int branchingFactor) {

        assert btree != null;

        assert branchingFactor>=BTree.MIN_BRANCHING_FACTOR;
        
        this.branchingFactor = branchingFactor;

        setBTree(btree);

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
        super();

        assert !isPersistent();

        assert src != null;

        this.branchingFactor = src.btree.branchingFactor;

        setBTree(src.btree);

    }

    /**
     * This method MUST be used to set the btree reference. This method
     * verifies that the btree reference is not already set. It is extended
     * by {@link Node#setBTree(BTree)} to ensure that the btree holds a hard
     * reference to the node.
     * 
     * @param btree
     *            The btree.
     */
    void setBTree(BTree btree) {

        assert btree != null;

        if (this.btree != null) {

            throw new IllegalStateException();

        }

        this.btree = btree;

    }

    /**
     * <p>
     * Return this node iff it is dirty and otherwise return a copy of this
     * node. If a copy is made of the node, then a copy will also be made of
     * each parent of the node up to the root of the tree and the new root
     * node will be set on the {@link BTree}. This method must MUST be
     * invoked any time an mutative operation is requested for the node. For
     * simplicity, it is invoked by the two primary mutative operations
     * {@link #insert(int, com.bigdata.objndx.TestSimpleBTree.Entry)} and
     * {@link #remove(int)}.
     * </p>
     * <p>
     * Note: You can not modify a node that has been written onto the store.
     * Instead, you have to clone the node causing it and all nodes up to
     * the root to be dirty and transient. This method handles that cloning
     * process, but the caller MUST test whether or not the node was copied
     * by this method, MUST delegate the mutation operation to the copy iff
     * a copy was made, and MUST result in an awareness in the caller that
     * the copy exists and needs to be used in place of the immutable
     * version of the node.
     * </p>
     * 
     * @return Either this node or a copy of this node.
     * 
     * FIXME The comments above are not quite correct since you also need to
     * invoke copyOnWrite when splits drive up the tree from a node or leaf
     * towards the root and during rotations.
     */
    protected AbstractNode copyOnWrite() {

        if (isPersistent()) {

            Node parent = this.getParent();

            AbstractNode newNode;

            if (this instanceof Node) {

                newNode = new Node((Node) this);

            } else {

                newNode = new Leaf((Leaf) this);

            }

            if (btree.root == this) {

                assert parent == null;

                // Update the root node on the btree.

                System.err
                        .println("Copy-on-write : replaced root node on btree.");

                btree.root = newNode;

            } else {

                /*
                 * Recursive copy-on-write up the tree. This operations
                 * stops as soon as we reach a parent node that is already
                 * dirty and grounds out at the root in any case.
                 */
                assert parent != null;

                if (!parent.isDirty()) {

                    Node newParent = (Node) parent.copyOnWrite();

                    newParent.replaceChildRef(this.getIdentity(), newNode);

                }

            }

            return newNode;

        } else {

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
             * Expand the {@link Entry} objects for each leaf visited in
             * the post-order traversal.
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
     * True iff this is a leaf node.
     */
    abstract public boolean isLeaf();

    /**
     * <p>
     * Split a node or leaf. This node (or leaf) is the "low" node (or leaf)
     * and will contain the lower half of the keys. The split creates a
     * "high" node (or leaf) to contain the high half of the keys. When this
     * is the root, the split also creates a new root {@link Node}.
     * </p>
     * <p>
     * Note: Splits are triggered on insert into a full node or leaf. The
     * first key of the high node is inserted into the parent of the split
     * node. The caller must test whether the key that is being inserted is
     * greater than or equal to this key. If it is, then the insert goes
     * into the high node. Otherwise it goes into the low node.
     * </p>
     * 
     * @param key
     *            The external key.
     * 
     * @return The high node (or leaf) created by the split.
     */
    abstract protected AbstractNode split();

    /**
     * Recursive search locates the approprate leaf and inserts the entry
     * under the key. The leaf is split iff necessary. Splitting the leaf
     * can cause splits to cascade up towards the root. If the root is split
     * then the total depth of the tree is inceased by one.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The value.
     */
    abstract public void insert(int key, Entry entry);

    /**
     * Recursive search locates the appropriate leaf and removes and returns
     * the pre-existing value stored under the key (if any).
     * 
     * @param key
     *            The external key.
     * @return The value or null if there was no entry for that key.
     */
    abstract public IObjectIndexEntry remove(int key);

    /**
     * Recursive search locates the entry for the probe key.
     * 
     * @param key
     *            The external key.
     * 
     * @return The entry or <code>null</code> iff there is no entry for
     *         that key.
     */
    abstract public IObjectIndexEntry lookup(int key);

    /**
     * Writes the node on the store. The node MUST be dirty. If the node has
     * a parent, then the parent is notified of the persistent identity
     * assigned to the node by the store.
     * 
     * @return The persistent identity assigned by the store.
     */
    long write() {

        assert isDirty();
        assert !isPersistent();

        /*
         * Write the dirty node on the store.
         */
        btree.writeNodeOrLeaf( this );

        // The parent should be defined unless this is the root node.
        Node parent = getParent();

        if (parent != null) {

            // parent must be dirty if child is dirty.
            assert parent.isDirty();

            // parent must not be persistent if it is dirty.
            assert !parent.isPersistent();

            /*
             * Set the persistent identity of the child on the
             * parent.
             * 
             * Note: A parent CAN NOT be serialized before all of
             * its children have persistent identity since it needs
             * to write the identity of each child in its
             * serialization record.
             */
            parent.setChildRef(this);

        }

        return getIdentity();

    }

    /**
     * Dump the data onto the {@link PrintStream} (non-recursive).
     * 
     * @param out
     *            Where to write the dump.
     * 
     * @return True unless an inconsistency was detected.
     */
    public boolean dump(PrintStream out) {

        return dump(out, 0, false);

    }

    /**
     * Dump the data onto the {@link PrintStream}.
     * 
     * @param out
     *            Where to write the dump.
     * @param height
     *            The height of this node in the tree.
     * @param recursive
     *            When true, the node will be dumped recursively using a
     *            pre-order traversal.
     *            
     * @return True unless an inconsistency was detected.
     */
    abstract public boolean dump(PrintStream out, int height, boolean recursive);

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

        return ws.substring(0, height * 4);

    }

    private static final String ws = "                                                                                                                                                                                                                  ";

}
