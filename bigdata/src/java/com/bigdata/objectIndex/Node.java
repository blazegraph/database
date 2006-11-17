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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * A non-leaf node.
 * </p>
 * <p>
 * A non-leaf node with <code>m</code> children has <code>m-1</code>
 * keys. The minimum #of keys that {@link Node} may have is one (1) while
 * the minimum #of children that a {@link Node} may have is two (2).
 * However, only the root {@link Node} is permitted to reach that minimum
 * (unless it is a leaf, in which case it may even be empty). All other
 * nodes or leaves have between <code>m/2</code> and <code>m</code>
 * keys, where <code>m</code> is the {@link AbstractNode#branchingFactor}.
 * </p>
 * <p>
 * Note: We MUST NOT hold hard references to leafs. Leaves account for most
 * of the storage costs of the BTree. We bound those costs by only holding
 * hard references to nodes and not leaves.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Node extends AbstractNode {

//    private static final long serialVersionUID = 1L;

    /**
     * Hard reference cache containing dirty child nodes (nodes or leaves).
     */
    transient protected Set<AbstractNode> dirtyChildren;

    /**
     * Weak references to child nodes (may be nodes or leaves). The capacity
     * of this array is m+1, where m is the {@link #branchingFactor}.
     */
    transient protected WeakReference<AbstractNode>[] childRefs;

    /**
     * <p>
     * The persistent keys of the childKeys nodes (may be nodes or leaves).
     * The capacity of this array is m+1, where m is the
     * {@link #branchingFactor}. The key is {@link #NULL} until the child
     * has been persisted. The protocol for persisting child nodes requires
     * that we use a pre-order traversal (the general case is a directed
     * graph) so that we can update the keys on the parent before the parent
     * itself is persisted.
     * </p>
     * <p>
     * Note: It is an error if there is an attempt to serialize a node
     * having a null entry in this array and a non-null entry in the
     * external {@link #keys} array.
     * </p>
     */
    long[] childKeys;

    /**
     * Extends the super class implementation to add the node to a hard
     * reference cache so that weak reference to this node will not be
     * cleared while the {@link BTree} is in use.
     */
    void setBTree(BTree btree) {

        super.setBTree(btree);

        btree.nodes.add(this);

    }

    /**
     * De-serialization constructor.
     * 
     * @deprecated Not required when using {@link NodeSerializer}.
     */
    public Node() {

    }

    /**
     * De-serialization constructor.
     */
    Node(BTree btree, long id, int branchingFactor, int nkeys, int[] keys, long[] childKeys) {

        super( btree, branchingFactor );

        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        assert nkeys < branchingFactor;

        assert keys.length == branchingFactor-1;

        assert childKeys.length == branchingFactor;
        
        setIdentity(id);

        this.nkeys = nkeys;
        
        this.keys = keys;
        
        this.childKeys = childKeys;

        dirtyChildren = new HashSet<AbstractNode>(branchingFactor);

        childRefs = new WeakReference[branchingFactor];

        // must clear the dirty since we just de-serialized this node.
        setDirty(false);

    }

    /**
     * Used to create a new node when a node is split.
     */
    Node(BTree btree) {

        super(btree, btree.branchingFactor);

        keys = new int[branchingFactor - 1];

        dirtyChildren = new HashSet<AbstractNode>(branchingFactor);

        childRefs = new WeakReference[branchingFactor];

        childKeys = new long[branchingFactor];

    }

    /**
     * This constructor is used when splitting the either the root {@link Leaf}
     * or a root {@link Node}. The resulting node has a single child reference
     * and NO keys.
     * 
     * @param btree
     *            Required solely to differentiate the method signature from the
     *            copy constructor.
     * @param oldRoot
     *            The node that was previously the root of the tree (either a
     *            node or a leaf).
     * 
     * @todo The key is ignored and could be removed from the constructor
     *       signature. The key gets added by
     *       {@link AbstractNode#insertChild(int, AbstractNode)}
     */
    Node(BTree btree, AbstractNode oldRoot) {

        super(btree, btree.branchingFactor);

        // Verify that this is the root.
        assert oldRoot == btree.root;

        // The old root must be dirty when it is being split.
        assert oldRoot.isDirty();

        keys = new int[branchingFactor - 1];

        dirtyChildren = new HashSet<AbstractNode>(branchingFactor);

        childRefs = new WeakReference[branchingFactor];

        childKeys = new long[branchingFactor];

        /*
         * Replace the root node on the tree.
         */
        btree.root = this;

        /*
         * Attach the old root to this node.
         */

        childRefs[0] = new WeakReference<AbstractNode>(oldRoot);

        dirtyChildren.add(oldRoot);

        oldRoot.parent = new WeakReference<Node>(this);

        /*
         * The tree is deeper since we just split the root node.
         */
        btree.height++;

        btree.nnodes++;

        // Note: #of nodes and leaves does not reflect right sibling yet.
        System.err.println("NEW ROOT: height="+btree.height);

    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node.
     */
    protected Node(Node src) {

        super(src);

        // This node must be mutable (it is a new node).
        assert isDirty();
        assert !isPersistent();
        
        /* The source must not be dirty.  We are cloning it so that we can
         * make changes on it.
         */
        assert !src.isDirty();
        assert src.isPersistent();

        keys = new int[branchingFactor - 1];

        nkeys = src.nkeys;

        dirtyChildren = new HashSet<AbstractNode>(branchingFactor);

        childRefs = new WeakReference[branchingFactor];

        childKeys = new long[branchingFactor];

        // Copy keys.
        System.arraycopy(src.keys, 0, keys, 0, nkeys);

        // Note: There is always one more child than keys for a Node.
        System.arraycopy(src.childKeys, 0, childKeys, 0, nkeys+1);

        /*
         * Steal strongly reachable unmodified children by setting their parent
         * fields to the new node. Stealing the parent means that the node MUST
         * NOT be used by its previous ancestor (our source node for this copy).
         * 
         * @todo Since the node state is unchanged (it is immutable) the slick
         * trick would be to wrap the state node with a flyweight node having a
         * different parent so that the node remained valid for its old parent.
         * This probably means making getParent() abstract and moving the parent
         * field into a flyweight class. This would help if we had to rollback
         * to a prior state of the tree without wanting to discard the
         * deserialized nodes.
         */
        for (int i = 0; i <= nkeys; i++) {

            AbstractNode child = src.childRefs[i] == null ? null
                    : src.childRefs[i].get();

            if (child != null) {

                /*
                 * Copy on write should never trigger for a dirty node and only
                 * a dirty node can have dirty children.
                 */
                assert !child.isDirty();

                // Steal the child.
                child.parent = new WeakReference<Node>(this);

                // Keep a reference to the clean child.
                childRefs[i] = new WeakReference<AbstractNode>(child);

            }
            
        }
        
        /*
         * Remove the source node from the btree since it has been replaced by
         * this node.
         * 
         * @todo mark the source as invalid if we develop a hash table based
         * cache for nodes to ensure that we never access it by mistake using
         * its persistent id. the current design only provides for access of
         * nodes by navigation from the root, so we can never visit a node once
         * it is no longer reachable from its parent.
         */ 
        btree.nodes.remove(src);

    }

    /**
     * Always returns <code>false</code>.
     */
    final public boolean isLeaf() {

        return false;

    }

    /**
     * This method must be invoked on a parent to notify the parent that the
     * child has become persistent. The method scans the weak references for
     * the children, finds the index for the specified child, and then sets
     * the corresponding index in the array of child keys. The child is then
     * removed from the dirty list for this node.
     * 
     * @param child
     *            The child.
     * 
     * @exception IllegalStateException
     *                if the child is not persistent.
     * @exception IllegalArgumentException
     *                if the child is not a child of this node.
     */
    void setChildRef(AbstractNode child) {

        if (!child.isPersistent()) {

            // The child does not have persistent identity.
            throw new IllegalStateException();

        }

        /*
         * Scan for location in weak references.
         * 
         * @todo Can this be made more efficient by considering the last key on
         * the child and searching the parent for the index that must correspond
         * to that child?
         */
        for (int i = 0; i < nkeys; i++) {

            if (childRefs[i].get() == child) {

                childKeys[i] = child.getIdentity();

                if (!dirtyChildren.remove(child)) {

                    throw new AssertionError("Child was not on dirty list.");

                }

                return;

            }

        }

        /*
         * @todo I have seen this triggered by increment writes of dirty
         * leaves using the doSplitWithIncreasingKeySequence at m := 20.
         */
        System.err.print("parent: ");
        this.dump(System.err);
        System.err.print("child : ");
        child.dump(System.err);
        throw new IllegalArgumentException("Not our child : child=" + child);

    }

    /**
     * Invoked by {@link #copyOnWrite()} to change the key for a child on a
     * cloned parent to a reference to a cloned child.
     * 
     * @param oldChildKey
     *            The key for the old child.
     * @param newChild
     *            The reference to the new child.
     */
    void replaceChildRef(long oldChildKey, AbstractNode newChild) {

        assert oldChildKey != NULL;
        assert newChild != null;

        // This node MUST have been cloned as a pre-condition, so it can not
        // be persistent.
        assert !isPersistent();

        // The newChild MUST have been cloned and therefore MUST NOT be
        // persistent.
        assert !newChild.isPersistent();

        // Scan for location in weak references.
        for (int i = 0; i < nkeys; i++) {

            if (childKeys[i] == oldChildKey) {

                if (true) {

                    /*
                     * Do some paranoia checks.
                     */

                    AbstractNode oldChild = childRefs[i] != null ? childRefs[i]
                            .get() : null;

                    if (oldChild != null) {

                        assert oldChild.isPersistent();

                        assert !dirtyChildren.contains(oldChild);

                    }

                }

                // Clear the old key.
                childKeys[i] = NULL;

                // Stash reference to the new child.
                childRefs[i] = new WeakReference<AbstractNode>(newChild);

                // Add the new child to the dirty list.
                dirtyChildren.add(newChild);

                // Set the parent on the new child.
                newChild.parent = new WeakReference<Node>(this);

                return;

            }

        }

        throw new IllegalArgumentException("Not our child : oldChildKey="
                + oldChildKey);

    }

    /**
     * Insert the entry under the key. This finds the index of the first
     * external key in the node whose value is greater than or equal to the
     * supplied key. The insert is then delegated to the child at position
     * <code>index - 1</code>.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The value.
     */
    public void insert(int key, Entry entry) {

        int index = findChild(key);

        AbstractNode child = getChild(index);

        child.insert(key, entry);

    }

    public IObjectIndexEntry lookup(int key) {

        int index = findChild(key);

        AbstractNode child = getChild(index);

        return child.lookup(key);

    }

    /**
     * Recursive descent until we reach the leaf that would hold the key.
     * 
     * @param key
     *            The external key.
     * 
     * @return The value stored under that key or null if the key was not
     *         found.
     */
    public IObjectIndexEntry remove(int key) {

        int index = findChild(key);

        AbstractNode child = getChild(index);

        return child.remove(key);

    }

    /**
     * Return the index of the child to be searched.
     * 
     * The interpretation of the key index for a node is as follows. When
     * searching nodes of the tree, we search for the index in keys[] of the
     * first key value greater than or equal (GTE) to the probe key and then
     * choose the child having the same index as the GTE key match. For
     * example,
     * 
     * <pre>
     * keys[]  : [ 5 9 12 ]
     * child[] : [ a b  c  d ]
     * </pre>
     * 
     * A probe with keys up to <code>5</code> matches at index zero (0)
     * and we choose the 1st child, a, which is at index zero (0).
     * 
     * A probe with keys in [6:9] matches at index one (1) and we choose the
     * 2nd child, b, which is at index one (1).
     * 
     * A probe with keys in [10:12] matches at index two (2) and we choose
     * the 3rd child, c, which is at index two (2).
     * 
     * A probe with keys greater than 12 exceeds all keys in the node and
     * always matches the last child in that node. In this case, d, which is
     * at index three (3).
     * 
     * Note that we never stop a search on a node, even when there is an
     * exact match on a key. All values are stored in the leaves and we
     * always descend until we reach the leaf in which a value for the key
     * would be stored. A test on the keys of that leaf is then conclusive -
     * either a value is stored in the leaf for that key or it is not stored
     * in the tree.
     * 
     * @param key
     *            The probe (an external key).
     * 
     * @return The child to be searched next for that key.
     */
    public int findChild(int key) {

        int index = Search.search(key, keys, nkeys);

        if (index >= 0) {

            /*
             * exact match.
             */

            return index;

        } else {

            /*
             * There is no exact match on the key, so we convert the search
             * result to find the insertion point. The insertion point is
             * always an index whose current key (iff it is defined) is
             * greater than the probe key.
             * 
             * keys[] : [ 5 9 12 ]
             * 
             * The insertion point for key == 4 is zero.
             * 
             * The insertion point for key == 6 is one.
             * 
             * etc.
             * 
             * When the probe key is greater than any existing key, then
             * the insertion point is nkeys.  E.g., the insertion point
             * for key == 20 is 3.
             */

            // Convert to obtain the insertion point.
            index = -index - 1;

            return index;

        }

    }

    /**
     * <p>
     * Split the node. The {@link BTree#nodeSplitter} is used to compute the
     * index (splitIndex) at which to split the node and a new rightSibling
     * {@link Node} is created. The key at the splitIndex is known as the
     * splitKey. The splitKey itself is lifted into the parent and does not
     * appear in either this node or the rightSibling after the split. All keys
     * and child references from splitIndex+1 are moved to the new rightSibling.
     * The child reference for splitIndex remains in this node.
     * </p>
     * <p>
     * If this node is the root of the tree (no parent), then a new root
     * {@link Node} is created without any keys and is made the parent of this
     * node.
     * </p>
     * <p>
     * In any case, we then insert( splitKey, rightSibling ) into the parent
     * node, which may cause the parent node itself to split.
     * </p>
     * 
     * @see INodeSplitPolicy
     * @see BTree#nodeSplitter
     * 
     * @todo Review copyOnWrite triggers when splitting a node. Basically,
     *       everything up to the root will need to be mutable even if the split
     *       does not reach that high.
     */
    protected AbstractNode split() {

        assert isDirty(); // MUST be mutable.

        // the #of child references.
        final int m = branchingFactor;
        
        // the index of the key to be inserted into the parent node.
        final int splitIndex = btree.nodeSplitter.splitNodeAt(this);

        // the key to be inserted into the parent node.
        final int splitKey = keys[splitIndex];

        // create the new rightSibling node.
        final Node rightSibling = new Node(btree);

        System.err.print("SPLIT NODE: m=" + m + ", splitIndex=" + splitIndex
                + ", splitKey=" + splitKey + ": ");
        dump(System.err);

        /* 
         * copy keys and values to the new rightSibling.
         */
        
        int j = 0;

        for (int i = splitIndex + 1 ; i < m; i++, j++) {

            if ( i + 1 < m) {
            
                /*
                 * Note: keys[m-1] is undefined.
                 */
                rightSibling.keys[j] = keys[i];
                
            }
            
            rightSibling.childRefs[j] = childRefs[i];
            
            rightSibling.childKeys[j] = childKeys[i];
            
            AbstractNode tmp = (childRefs[i] == null ? null : childRefs[i]
                    .get());
            
            if (tmp != null) {
        
                /*
                 * The child node is in memory.
                 * 
                 * Update its parent reference.
                 */
                
                tmp.parent = new WeakReference<Node>(rightSibling);
                
                if( tmp.isDirty()) {
                
                    /*
                     * Iff the child is dirty, then move the hard reference for
                     * dirty child to the rightSibling.
                     */
                    
                    dirtyChildren.remove(tmp);
                    
                    rightSibling.dirtyChildren.add(tmp);
                    
                }
                
            }

            /*
             * Clear out the old keys and values, including keys[splitIndex]
             * which is being moved to the parent.
             */

            if (i + 1 < m) {
            
                keys[i] = NEGINF;
                
                nkeys--; // one less key here.

                rightSibling.nkeys++; // more more key there.
                
            }
            
            childRefs[i] = null;
            
            childKeys[i] = NULL;

        }

        /* 
         * Clear the key that is being move into the parent.
         */
        keys[splitIndex] = NEGINF;
        
        nkeys--;
        
        // Note: A node may have zero keys, but it must have at least one child.
        assert nkeys >= 0;
        assert rightSibling.nkeys >= 0;

        Node p = getParent();

        if (p == null) {

            /*
             * Use a special constructor to split the root.  The result is a new
             * node with zero keys and one child (this node).
             */

            p = new Node(btree, this);

        }

        /* 
         * insert(splitKey,rightSibling) into the parent node.  This may cause
         * the parent node itself to split.
         */
        p.insertChild(splitKey, rightSibling);

        btree.nnodes++;

        // Return the high node.
        return rightSibling;

    }

    /**
     * Invoked to insert a key and reference for a child created when
     * another child of this node is split.
     * 
     * @param key
     *            The key on which the old node was split.
     * @param child
     *            The new node.
     */
    protected AbstractNode insertChild(int key, AbstractNode child) {

        assert key > NEGINF && key < POSINF;
        assert child != null;
        assert child.isDirty(); // always dirty since it was just created.
        assert isDirty(); // must be dirty to permit mutation.

        if (nkeys == keys.length) {

            /*
             * This node is full. First split the node, then figure out
             * which node the insert actually goes into and direct the
             * insert to that node.
             */

            /*Node node2 = (Node)*/ split();

            Node p = getParent();
            
            Node insertNode = (Node) p.getChild( p.findChild(key) );
            
            return insertNode.insertChild(key, child);

        }

        /*
         * Find the location where this key belongs. When a new node is
         * created, the constructor sticks the child that demanded the split
         * into childRef[0]. So, when nkeys == 1, we have nchildren==1 and
         * the key goes into keys[0] but we have to copyDown by one anyway
         * to avoid stepping on the existing child.
         */
        int index = Search.search(key, keys, nkeys);

        if (index >= 0) {

            /*
             * The key is already present. This is an error.
             */

            throw new AssertionError("Split on existing key: key=" + key);

        }

        // Convert the position to obtain the insertion point.
        index = -index - 1;

        assert index >= 0 && index <= nkeys;

        /*
         * copy down per-key data.
         */
        final int keyCount = nkeys - index;

        if (keyCount > 0)
            System.arraycopy(keys, index, keys, index + 1, keyCount);

        /*
         * Note: This is equivilent to (nkeys + 1) - (index+1). (nkeys+1) is
         * equivilent to nchildren while (index+1) is the position at which
         * the new child reference is written. So, childCount == keyCount.
         * 
         * @todo Refactor to use only one [count/length] parameter and
         * update comments.  Avoid invoking the copy operation when the count
         * is zero.
         */
        final int childCount = nkeys - index;

        /*
         * copy down per-child data. #children == nkeys+1. child[0] is
         * always defined.
         */
        System
                .arraycopy(childKeys, index + 1, childKeys, index + 2,
                        childCount);
        System
                .arraycopy(childRefs, index + 1, childRefs, index + 2,
                        childCount);

        /*
         * Insert key at index.
         */
        keys[index] = key;

        /*
         * Insert child at index+1.
         */
        childRefs[index + 1] = new WeakReference<AbstractNode>(child);

        childKeys[index + 1] = NULL;

        dirtyChildren.add(child);

        child.parent = new WeakReference<Node>(this);

        nkeys++;

        return this;

    }

    /**
     * Return the child node or leaf at the specified index in this node. If
     * the node is not in memory then it is read from the store.
     * 
     * @param index
     *            The index in [0:nkeys].
     * 
     * @return The child node or leaf.
     */
    public AbstractNode getChild(int index) {

        assert index >= 0 && index <= nkeys;

        WeakReference<AbstractNode> childRef = childRefs[index];

        AbstractNode child = null;

        if (childRef != null) {

            child = childRef.get();

        }

        if (child == null) {

            long key = childKeys[index];

            assert key != NULL;

            assert btree != null;

            child = btree.readNodeOrLeaf( key );

            /*
             * Patch btree reference since loaded from store. If the child
             * is a {@link Node}, then it is also inserted into the hard
             * reference cache of nodes maintained by the btree.
             */
            child.setBTree(btree);

            // patch parent reference since loaded from store.
            child.parent = new WeakReference<Node>(this);

            // patch the child reference.
            childRefs[index] = new WeakReference<AbstractNode>(child);

            if (child instanceof Leaf) {

                /*
                 * Leaves are added to a hard reference queue. On eviction
                 * from the queue the leaf is serialized. Once the leaf is
                 * no longer strongly reachable its weak references may be
                 * cleared by the VM.
                 */

                btree.leaves.append((Leaf) child);

            }

        }

        return child;

    }

    /**
     * Iterator visits children, recursively expanding each child with a
     * post-order traversal of its children and finally visits this node
     * itself.
     */
    public Iterator postOrderIterator(final boolean dirtyNodesOnly) {

        /*
         * Iterator append this node to the iterator in the post-order
         * position.
         */

        return new Striterator(postOrderIterator1(dirtyNodesOnly))
                .append(new SingleValueIterator(this));

    }

    /**
     * Visits the children (recursively) using post-order traversal, but
     * does NOT visit this node.
     */
    private Iterator postOrderIterator1(final boolean dirtyNodesOnly) {

        if (dirtyNodesOnly && !isDirty()) {

            return EmptyIterator.DEFAULT;

        }

        /*
         * Iterator visits the direct children, expanding them in turn with
         * a recursive application of the post-order iterator.
         */

        //            System.err.println("node: " + this);
        return new Striterator(childIterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(Object childObj) {

                /*
                 * A child of this node.
                 */

                AbstractNode child = (AbstractNode) childObj;

                if (child instanceof Node) {

                    /*
                     * The child is a Node (has children).
                     */

                    //                        System.err.println("child is node: " + child);
                    // visit the children (recursive post-order traversal).
                    Striterator itr = new Striterator(((Node) child)
                            .postOrderIterator1(dirtyNodesOnly));

                    // append this node in post-order position.
                    itr.append(new SingleValueIterator(child));

                    return itr;

                } else {

                    /*
                     * The child is a leaf.
                     */

                    //                        System.err.println("child is leaf: " + child);
                    // Visit the leaf itself.
                    return new SingleValueIterator(child);

                }
            }
        });

    }

    /**
     * Iterator visits the direct child nodes in the external key ordering.
     */
    public Iterator childIterator() {

        return new ChildIterator(this);

    }

    public boolean dump(PrintStream out, int height, boolean recursive) {

        boolean ok = true;
        out.println(indent(height) + "Node: " + toString());
        out.println(indent(height) + "  parent=" + getParent());
        out.println(indent(height) + "  dirty=" + isDirty() + ", nkeys="
                + nkeys + ", nchildren=" + (nkeys + 1) + ", branchingFactor="
                + branchingFactor);
        out.println(indent(height) + "  keys=" + Arrays.toString(keys));
        { // verify keys are monotonically increasing.
            int lastKey = NEGINF;
            for (int i = 0; i < nkeys; i++) {
                if (keys[i] <= lastKey) {
                    out.println(indent(height)
                            + "ERROR keys out of order at index=" + i
                            + ", lastKey=" + lastKey + ", keys[" + i + "]="
                            + keys[i]);
                    ok = false;
                }
                lastKey = keys[i];
            }
        }
        out.println(indent(height) + "  childKeys="
                + Arrays.toString(childKeys));
        out.print(indent(height) + "  childRefs=[");
        for (int i = 0; i < branchingFactor; i++) {
            if (i > 0)
                out.print(", ");
            if (childRefs[i] == null)
                out.print("null");
            else
                out.print(childRefs[i].get());
        }
        out.println("]");
        out.print(indent(height) + "  #dirtyChildren=" + dirtyChildren.size()
                + " : {");
        int n = 0;
        Iterator<AbstractNode> itr = dirtyChildren.iterator();
        while (itr.hasNext()) {
            if (n++ > 0)
                out.print(", ");
            out.print(itr.next());
        }
        out.println("}");

        /*
         * Look for inconsistencies for children. A dirty child MUST NOT
         * have an entry in childKeys[] since it is not persistent and MUST
         * show up in dirtyChildren. Likewise if a child is NOT dirty, then
         * it MUST have an entry in childKeys and MUST NOT show up in
         * dirtyChildren.
         * 
         * This also verifies that all entries beyond nchildren (nkeys+1)
         * are unused.
         */
        for (int i = 0; i < branchingFactor; i++) {

            if (i > nkeys) {

                /*
                 * Scanning past the last valid child index. 
                 */

                if (childKeys[i] != NULL) {

                    out.println(indent(height) + "  ERROR childKeys[" + i
                            + "] should be " + NULL + ", not " + childKeys[i]);

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
                 * Note: This is not fetching the child if it is not in
                 * memory -- perhaps it should using its persistent id?
                 */

                AbstractNode child = (childRefs[i] == null ? null
                        : childRefs[i].get());

                if (child != null) {

                    if( child.parent == null || child.parent.get() == null ) {
                        /*
                         * the reference to the parent MUST exist since the we
                         * are the parent and therefore the parent is strongly
                         * reachable.
                         */
                        out.println(indent(height) + "  ERROR child["
                                + i + "] does not have parent reference.");
                        ok = false;
                    }

                    if( child.parent.get() != this ) {
                        out.println(indent(height) + "  ERROR child["
                                + i + "] has wrong parent.");
                        ok = false;                        
                    }
                    
                    if (child.isDirty()) {
                        /*
                         * Dirty child.  The parent of a dirty child MUST also
                         * be dirty.
                         */
                        if( ! isDirty() ) {
                            out.println(indent(height) + "  ERROR child["
                                    + i + "] is dirty, but its parent is clean");
                            ok = false;
                        }
                        if (childRefs[i] == null) {
                            out.println(indent(height) + "  ERROR childRefs["
                                    + i + "] is null, but the child is dirty");
                            ok = false;
                        }
                        if (childKeys[i] != NULL) {
                            out.println(indent(height) + "  ERROR childKeys["
                                    + i + "]=" + childKeys[i]
                                    + ", but MUST be " + NULL
                                    + " since the child is dirty");
                            ok = false;
                        }
                        if (!dirtyChildren.contains(child)) {
                            out
                                    .println(indent(height + 1)
                                            + "  ERROR child at index="
                                            + i
                                            + " is dirty, but not on the dirty list: child="
                                            + child);
                            ok = false;
                        }
                    } else {
                        /*
                         * Clean child (ie, persistent).  The parent of a clean
                         * child may be either clear or dirty.
                         */
                        if (childKeys[i] == NULL) {
                            out.println(indent(height) + "  ERROR childKey["
                                    + i + "] is " + NULL
                                    + ", but child is not dirty");
                            ok = false;
                        }
                        if (dirtyChildren.contains(child)) {
                            out
                                    .println(indent(height)
                                            + "  ERROR child at index="
                                            + i
                                            + " is not dirty, but is on the dirty list: child="
                                            + child);
                            ok = false;
                        }
                    }

                }

            }

        }

        if (recursive) {

            /*
             * Dump children using pre-order traversal.
             */

            Set<AbstractNode> dirty = new HashSet<AbstractNode>();

            for (int i = 0; i <= nkeys; i++) {

                if (childRefs[i] == null && childKeys[i] == 0) {

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

                AbstractNode child = getChild(i);

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

                if (child.isDirty() && !dirtyChildren.contains(child)) {

                    out
                            .println(indent(height + 1)
                                    + "ERROR dirty child not in node's dirty list at index="
                                    + i);

                    ok = false;

                }

                if (!child.isDirty() && dirtyChildren.contains(child)) {

                    out
                            .println(indent(height + 1)
                                    + "ERROR clear child found in node's dirty list at index="
                                    + i);

                    ok = false;

                }

                if (child.isDirty()) {

                    dirty.add(child);

                }

                if (i == 0) {

                    if (nkeys == 0) {

                        /*
                         * Note: a node with zero keys is valid. It MUST have a
                         * single child. Such nodes arise when splitting a node
                         * in a btree of order m := 3 when the splitIndex is
                         * computed as m/2-1 = 0.  This is perfectly normal.
                         */
                        
                    } else {
                        /*
                         * Note: All keys on the first child MUST be LT the
                         * first key on this node.
                         */

                        if (child.keys[0] >= keys[0]) {

                            out
                                    .println(indent(height + 1)
                                            + "ERROR first key on first child must be LT "
                                            + keys[0] + ", but found "
                                            + child.keys[0]);

                            ok = false;

                        }

                        if (child.nkeys >= 1
                                && child.keys[child.nkeys - 1] >= keys[0]) {

                            out
                                    .println(indent(height + 1)
                                            + "ERROR last key on first child must be LT "
                                            + keys[0] + ", but found "
                                            + child.keys[child.nkeys - 1]);

                            ok = false;

                        }
                        
                    }
                    
                } else if (i < nkeys) {

                    if (child.isLeaf() && keys[i - 1] != child.keys[0]) {

                        /*
                         * While each key in a node always is the first key
                         * of some leaf, we are only testing the direct
                         * children here. Therefore if the children are not
                         * leaves then we can not cross check their first
                         * key with the keys on this node.
                         */
                        out.println(indent(height + 1)
                                + "ERROR first key on child leaf must be "
                                + keys[i - 1] + ", not " + child.keys[0]
                                + " at index=" + i);

                        ok = false;

                    }

                } else {

                    /*
                     * While there is a child for the last index of a node,
                     * there is no key for that index.
                     */

                }

                if (!child.dump(out, height + 1, true)) {

                    ok = false;

                }

            }

            if (dirty.size() != dirtyChildren.size()) {

                out.println(indent(height + 1) + "ERROR found " + dirty.size()
                        + " dirty children, but " + dirtyChildren.size()
                        + " in node's dirty list");

                ok = false;

            }

        }

        return ok;

    }

//    /**
//     * @deprecated Drop the Externalizable interface for Node and Leaf.
//     */
//    public void writeExternal(ObjectOutput out) throws IOException {
//        if (dirtyChildren.size() > 0) {
//            throw new IllegalStateException("Dirty children exist.");
//        }
//        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
//        assert nkeys > 0;
//        out.writeInt(branchingFactor);
//        out.writeInt(nkeys);
//        for (int i = 0; i < nkeys; i++) {
//            int key = keys[i];
//            assert key > NEGINF && key < POSINF;
//            out.writeInt(key);
//        }
//        // Note: nchildren == nkeys+1.
//        for (int i = 0; i <= nkeys; i++) {
//            long childKey = childKeys[i];
//            assert childKey != NULL;
//            out.writeLong(childKey);
//        }
//    }
//
//    /**
//     * @deprecated Drop the Externalizable interface for Node and Leaf.
//     */
//    public void readExternal(ObjectInput in) throws IOException,
//            ClassNotFoundException {
//        branchingFactor = in.readInt();
//        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
//        nkeys = in.readInt();
//        assert nkeys > 0;
//        keys = new int[branchingFactor - 1];
//        dirtyChildren = new HashSet<AbstractNode>(branchingFactor);
//        childRefs = new WeakReference[branchingFactor];
//        childKeys = new long[branchingFactor];
//        for (int i = 0; i < nkeys; i++) {
//            int key = in.read();
//            assert key > NEGINF && key < POSINF;
//            keys[i] = key;
//        }
//        // Note: nchildren == nkeys+1.
//        for (int i = 0; i <= nkeys; i++) {
//            long childKey = in.readLong();
//            assert childKey != NULL;
//            childKeys[i] = childKey;
//        }
//    }

}
