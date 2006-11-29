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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Level;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * A non-leaf node.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Node extends AbstractNode {

    /**
     * <p>
     * Hard reference cache containing dirty child nodes (nodes or leaves).
     * </p>
     * <p>
     * This cache is pre-sized to its maximum capacity, which is
     * <code>branchingFactor+1</code>. While there are at most
     * <code>branchingFactor</code> children for a node, the cache is sized
     * one larger to allow for the child reference corresponding to the key that
     * causes overflow and forces the split may be inserted. This greatly
     * simplifies the logic for computing the split point and performing the
     * split.
     * </p>
     */
    transient protected Set<AbstractNode> dirtyChildren;

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
     */
    transient protected WeakReference<AbstractNode>[] childRefs;

    /**
     * <p>
     * The persistent keys of the childKeys nodes (may be nodes or leaves). The
     * capacity of this array is m, where m is the {@link #branchingFactor}.
     * Valid indices are in [0:nkeys+1] since nchildren := nkeys+1 for a
     * {@link Node}. The key is {@link #NULL} until the child has been
     * persisted. The protocol for persisting child nodes requires that we use a
     * pre-order traversal (the general case is a directed graph) so that we can
     * update the keys on the parent before the parent itself is persisted.
     * </p>
     * <p>
     * Note: It is an error if there is an attempt to serialize a node having a
     * null entry in this array and a non-null entry in the external
     * {@link #keys} array.
     * </p>
     * <p>
     * This array is dimensioned to one more than the maximum capacity so that
     * the child reference corresponding to the key that causes overflow and
     * forces the split may be inserted. This greatly simplifies the logic for
     * computing the split point and performing the split.
     * </p>
     */
    long[] childKeys;

    /**
     * De-serialization constructor.
     * 
     * @param btree
     *            The tree to which the node belongs.
     * @param id
     *            The persistent identity of the node.
     * @param branchingFactor
     *            The branching factor for the node (the maximum #of children
     *            allowed for the node).
     * @param nkeys
     *            The #of defined keys in <i>keys</i>.
     * @param keys
     *            The external keys (the array reference is copied NOT the array
     *            contents). The array MUST be dimensioned to
     *            <code>branchingFactor</code> to allow room for the insert
     *            key that places the node temporarily over capacity during a
     *            split.
     * @param childKeys
     *            The persistent identity for the direct children (the array
     *            reference is copied NOT the array contents). The array MUST be
     *            dimensioned to <code>branchingFactor+1</code> to allow room
     *            for the insert key that places the node temporarily over
     *            capacity during a split.
     */
    protected Node(BTree btree, long id, int branchingFactor, int nkeys, int[] keys, long[] childKeys) {

        super( btree, branchingFactor );

        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        assert nkeys < branchingFactor;

        assert keys.length == branchingFactor;

        assert childKeys.length == branchingFactor+1;
        
        setIdentity(id);

        this.nkeys = nkeys;
        
        this.keys = keys;
        
        this.childKeys = childKeys;

        dirtyChildren = new HashSet<AbstractNode>(branchingFactor+1);

        childRefs = new WeakReference[branchingFactor+1];

        // must clear the dirty since we just de-serialized this node.
        setDirty(false);

    }

    /**
     * Used to create a new node when a node is split.
     */
    protected Node(BTree btree) {

        super(btree, btree.branchingFactor);

        keys = new int[branchingFactor];

        dirtyChildren = new HashSet<AbstractNode>(branchingFactor+1);

        childRefs = new WeakReference[branchingFactor+1];

        childKeys = new long[branchingFactor+1];

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
     */
    protected Node(BTree btree, AbstractNode oldRoot) {

        super(btree, btree.branchingFactor);

        // Verify that this is the root.
        assert oldRoot == btree.root;

        // The old root must be dirty when it is being split.
        assert oldRoot.isDirty();

        keys = new int[branchingFactor];

        dirtyChildren = new HashSet<AbstractNode>(branchingFactor+1);

        childRefs = new WeakReference[branchingFactor+1];

        childKeys = new long[branchingFactor+1];

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

        // Note: nnodes and nleaves might not reflect rightSibling yet.
        BTree.log.info("NEW ROOT: height=" + btree.height + ", utilization="
                + btree.getUtilization()[2] + "%");

    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node.
     * 
     * @param triggeredByChild
     *            The child that triggered the copy constructor. This should be
     *            the immutable child NOT the one that was already cloned. This
     *            information is used to avoid stealing the original child since
     *            we already made a copy of it.
     */
    protected Node(Node src, AbstractNode triggeredByChild) {

        super(src);
        
        assert triggeredByChild != null;

        keys = new int[branchingFactor];

        nkeys = src.nkeys;

        dirtyChildren = new HashSet<AbstractNode>(branchingFactor+1);

        childRefs = new WeakReference[branchingFactor+1];

        childKeys = new long[branchingFactor+1];

        /*
         * @todo Unless and until we have a means to recover leafs from a cache,
         * can we just steal the keys[] and values[] rather than making copies?
         * We are already doing this for the Leaf copy constructor, but I am not
         * convinced that the situations are the same without further
         * inspection. This is only at optimization - what is at stake is less
         * churn on the heap.
         */
        // Copy keys.
        System.arraycopy(src.keys, 0, keys, 0, nkeys);

        // Note: There is always one more child than keys for a Node.
        System.arraycopy(src.childKeys, 0, childKeys, 0, nkeys+1);

        /*
         * Steal strongly reachable unmodified children by setting their parent
         * fields to the new node. Stealing the parent means that the node MUST
         * NOT be used by its previous ancestor (our source node for this copy).
         * 
         * Note: Since the node state is unchanged (it is immutable) a slick
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

            if (child != null && child != triggeredByChild) {

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
         * @todo mark the [src] as invalid if we develop a hash table based
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
    void setChildKey(AbstractNode child) {

        if (!child.isPersistent()) {

            // The child does not have persistent identity.
            throw new IllegalStateException();

        }

        int i = getIndexOf( child );

        childKeys[i] = child.getIdentity();

        if (!dirtyChildren.remove(child)) {

            throw new AssertionError("Child was not on dirty list.");

        }
        
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
        for (int i = 0; i <= nkeys; i++) {

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
     * 
     * @return The previous value or <code>null</code> if the key was not
     *         found.
     */
    public Object insert(int key, Object entry) {
        
        assert !isDeleted();
        
        assertInvariants();

        int index = findChild(key);

        AbstractNode child = getChild(index);

        return child.insert(key, entry);

    }

    public Object lookup(int key) {

        assert !isDeleted();

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
    public Object remove(int key) {

        assert !isDeleted();

        int index = findChild(key);

        AbstractNode child = getChild(index);

        Object val = child.remove(key);
        
        return val;

    }

    /**
     * Return the index of the child to be searched.
     * 
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
     * 
     * A probe whose key is <code>5</code> matches at index zero (0) exactly
     * and we choose the child at <code>index + 1</code>, b, which is at
     * index one (1).
     * 
     * A probe with keys in [6:8] matches at index one (1) and we choose the 2nd
     * child, b, which is at index one (1). A probe with <code>9</code> also
     * matches at index one (1), but we choose <code>index+1</code> equals two
     * (2) since this is an exact key match.
     * 
     * A probe with keys in [10:11] matches at index two (2) and we choose the
     * 3rd child, c, which is at index two (2). A probe with <code>12</code>
     * also matches at index two (2), but we choose <code>index+1</code>
     * equals three (3) since this is an exact key match.
     * 
     * A probe with keys greater than 12 exceeds all keys in the node and always
     * matches the last child in that node. In this case, d, which is at index
     * three (3).
     * 
     * Note that we never stop a search on a node, even when there is an exact
     * match on a key. All values are stored in the leaves and we always descend
     * until we reach the leaf in which a value for the key would be stored. A
     * test on the keys of that leaf is then conclusive - either a value is
     * stored in the leaf for that key or it is not stored in the tree.
     * 
     * @param key
     *            The probe (an external key).
     * 
     * @return The child to be searched next for that key.
     * 
     * @see TestBTree#test_node_findChild()
     */
    public int findChild(int key) {

        int index = Search.search(key, keys, nkeys);

        if (index >= 0) {

            /*
             * exact match - use the next child.
             */

            return index + 1;

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
     * Split an over-capacity node (a node with <code>maxKeys+1</code> keys),
     * creating a new rightSibling. The splitIndex is <code>(maxKeys+1)/2</code>.
     * The key at the splitIndex is the separatorKey. Unlike when we split a
     * {@link Leaf}, the separatorKey is lifted into the parent and does not
     * appear in either this node or the rightSibling after the split. All keys
     * and child references from <code>splitIndex+1</code> (inclusive) are
     * moved to the new rightSibling. The child reference at
     * <code>splitIndex</code> remains in this node.
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
     */
    protected AbstractNode split() {

        assert isDirty(); // MUST be mutable.
        assert nkeys == maxKeys+1; // MUST be over capacity by one.

        /*
         * The #of child references. (this is +1 since the node is over capacity
         * by one).
         */
        final int nchildren = branchingFactor+1;
        
        /*
         * Index at which to split the leaf.
         */
        final int splitIndex = (maxKeys+1)/2;

        /*
         * The key at that index, which becomes the separator key in the parent.
         */
        final int separatorKey = keys[splitIndex];

        // create the new rightSibling node.
        final Node rightSibling = new Node(btree);

        if (btree.DEBUG) {
            BTree.log.debug("SPLIT NODE: nkeys=" + nkeys + ", splitIndex="
                    + splitIndex + ", separatorKey="
                    + separatorKey + ": ");
            dump(System.err);
        }

        /* 
         * copy keys and values to the new rightSibling.
         */
        
        int j = 0;

        for (int i = splitIndex + 1 ; i < nchildren; i++, j++) {

            if ( i + 1 < nchildren) {
            
                /*
                 * Note: keys[nchildren-1] is undefined.
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

            if (i + 1 < nchildren) {
            
                keys[i] = IBTree.NEGINF;
                
                nkeys--; // one less key here.

                rightSibling.nkeys++; // more more key there.
                
            }
            
            childRefs[i] = null;
            
            childKeys[i] = NULL;

        }

        /* 
         * Clear the key that is being move into the parent.
         */
        keys[splitIndex] = IBTree.NEGINF;
        
        nkeys--;

        Node p = getParent();

        if (p == null) {

            /*
             * Use a special constructor to split the root.  The result is a new
             * node with zero keys and one child (this node).
             */

            p = new Node(btree, this);

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
     * Invoked to insert a key and reference for a child created when
     * another child of this node is split.
     * 
     * @param key
     *            The key on which the old node was split.
     * @param child
     *            The new node.
     */
    protected void insertChild(int key, AbstractNode child) {

        assertInvariants();
        assert key > IBTree.NEGINF && key < IBTree.POSINF;
        assert child != null;
        assert child.isDirty(); // always dirty since it was just created.
        assert isDirty(); // must be dirty to permit mutation.

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
        final int length = nkeys - index;

        if (length > 0)
            System.arraycopy(keys, index, keys, index + 1, length);

        /*
         * copy down per-child data. #children == nkeys+1. child[0] is
         * always defined.
         */
        System.arraycopy(childKeys, index + 1, childKeys, index + 2, length);
        System.arraycopy(childRefs, index + 1, childRefs, index + 2, length);

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

        if (nkeys == maxKeys+1) {

            /*
             * The node is over capacity so we split the node, creating a new
             * rightSibling and insert( separatorKey, rightSibling ) into the
             * parent.
             */

            Node rightSibling = (Node) split();

            // assert additional post-split invariants.
            getParent().assertInvariants();
            rightSibling.assertInvariants();
            
            return;

        }

        assertInvariants();

    }

    /**
     * Invoked when a node has become deficient (too few children). This method
     * is responsible for producing a well-formed tree by either re-distributing
     * children among this node and an immediate sibling or by merging this node
     * and an immediate sibling.
     * 
     * consider the immediate siblings. if either is materialized and has more
     * than the minimum #of children, then redistribute the children evenly
     * between this node and that sibling and update the separation key in the
     * parent to reflect the new partitioning of the children. if either is
     * materialized and has only the minimum #of children, then merge this node
     * with that sibling and update the parent by removing the separator key.
     * 
     * If no materialized immediate sibling meets these criteria, then first
     * materialize and test the right sibling. if the right sibling does not
     * meet these criteria, then materialize and test the left sibling.
     * 
     * Note that (a) we prefer to merge a materialized sibling with this node to
     * materializing a sibling; and (b) merging siblings is the only way that a
     * separator key is removed from a parent. If the parent becomes deficient
     * through merging then join is invoked on the parent as well. Note that
     * join is never invoked on the root node (or leaf) since it by definition
     * has no siblings.
     * 
     * FIXME Implement Node.join()
     */
    protected void join() {
        
        // verify that this node is deficient.
        assert nkeys < minKeys;
        
        throw new UnsupportedOperationException();
        
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
     *         If the sibling is returned, then is NOT guarenteed to be mutable
     *         and the caller MUST invoke copy-on-write before attempting to
     *         modify the returned sibling.
     */
    public AbstractNode getLeftSibling(AbstractNode child, boolean materialize) {

        int i = getIndexOf(child);

        if (i == 0) {

            /*
             * There is no left sibling for this child.
             */
            return null;

        } else {

            int index = i - 1;

            AbstractNode sibling = childRefs[index] == null ? null
                    : childRefs[index].get();

            if (sibling == null && materialize) {

                sibling = getChild(index);

            } else {

                btree.touch(sibling);

            }

            return sibling;

        }

    }
    
    /**
     * Return the right sibling. This is used by implementations of
     * {@link AbstractNode#join()} to explore their right sibling.
     * 
     * @param child
     *            The child (must be dirty).
     * @param materialize
     *            When true, the left sibling will be materialized if it exists
     *            but is not resident.
     * 
     * @return The left sibling or <code>null</code> if it does not exist -or-
     *         if it is not materialized and <code>materialized == false</code>.
     *         If the sibling is returned, then is NOT guarenteed to be mutable
     *         and the caller MUST invoke copy-on-write before attempting to
     *         modify the returned sibling.
     */
    public AbstractNode getRightSibling(AbstractNode child, boolean materialize) {

        int i = getIndexOf(child);

        if (i == nkeys ) {

            /*
             * There is no right sibling for this child.
             */
            return null;

        } else {

            int index = i + 1;

            AbstractNode sibling = childRefs[index] == null ? null
                    : childRefs[index].get();

            if (sibling == null ) {
                
                if( materialize ) {

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
     * @return The index in {@link #childRefs} where that child is found.
     * 
     * @exception IllegalArgumentException
     *                iff child is not a child of this node.
     */
    protected int getIndexOf(AbstractNode child) {

        assert child != null;
        assert child.parent.get() == this;

        /*
         * Scan for location in weak references.
         * 
         * @todo Can this be made more efficient by considering the last key on
         * the child and searching the parent for the index that must correspond
         * to that child? Note that when merging two children the keys in the
         * parent might not be coherent depending on exactly when this method is
         * called - for things to be coherent you would have to discover the
         * index of the children before modifying their keys.
         * 
         * @see Leaf#merge(Leaf sibling)
         */

        for (int i = 0; i <= nkeys; i++) {

            if (childRefs[i] != null && childRefs[i].get() == child) {

                return i;
                
            }
            
        }

        throw new IllegalArgumentException("Not our child : child=" + child);
        
    }
    
    /**
     * Invoked when a non-root node or leaf has no more keys to detach the child
     * from its parent. If the node becomes deficiant, then the node is joined
     * witn one of its immediate siblings. If the node is the root of the tree,
     * then the root of the tree is also updated. The child is deleted as a
     * post-condition.
     * 
     * @param child
     *            The child (does NOT need to be mutable).
     */
    protected void removeChild(AbstractNode child) {
        
        assert child != null;
        assert !child.isDeleted();
        assert child.parent.get() == this;

        assert isDirty();
        assert !isDeleted();
        assert !isPersistent();

        assertInvariants();

        System.err.println("removeChild("+child+")");

        int i = getIndexOf(child);

        /*
         * Copy over the hole created when the child is removed
         * from the node.
         * 
         * Given:          v-- remove @ index = 0
         *       index:    0  1  2
         * root  keys : [ 21 24  0 ]
         *                              index
         * leaf1 keys : [  1  2  7  - ]   0 <--remove @ index = 0
         * leaf2 keys : [ 21 22 23  - ]   1
         * leaf4 keys : [ 24 31  -  - ]   2
         * 
         * This can also be represented as
         * 
         * ( leaf1, 21, leaf2, 24, leaf4 )
         * 
         * and we remove the sequence ( leaf1, 21 ) leaving a well-formed node.
         * 
         * Remove(leaf1):
         * index := 0
         * nkeys = 2 
         * nchildren := nkeys(2) + 1 = 3
         * lenChildCopy := #children(3) - index(0) - 1 = 2 
         * lenKeyCopy := lengthChildCopy - 1 = 1
         * copyChildren from index+1(1) to index(0) lengthChildCopy(2)
         * copyKeys from index+1(1) to index(0) lengthKeyCopy(1)
         * erase keys[ nkeys - 1 = 1 ]
         * erase children[ nkeys = 2 ]
         * 
         * post-condition:
         *       index:    0  1  2
         * root  keys : [ 24  0  0 ]
         *                              index
         * leaf2 keys : [ 21 22 23  - ]   0
         * leaf4 keys : [ 24 31  -  - ]   1
         */
            
        /*
         * Copy down to cover up the hole.
         */
        final int index = i;

        // #of children to copy (equivilent to nchildren - index - 1)
        final int lengthChildCopy = nkeys - index;

        // #of keys to copy.
        final int lengthKeyCopy = lengthChildCopy - 1;

        if (lengthKeyCopy > 0) {

            System.arraycopy(keys, index + 1, keys, index, lengthKeyCopy);

        }

        if (lengthChildCopy > 0) {

            System.arraycopy(childKeys, index + 1, childKeys, index,
                    lengthChildCopy);

            System.arraycopy(childRefs, index + 1, childRefs, index,
                    lengthChildCopy);

        }

        /*
         * Erase the data that were exposed by this operation. Note that there
         * is one fewer keys than children so ....
         */

        if (nkeys > 0) {

            // erase the last key position.
            keys[nkeys - 1] = IBTree.NEGINF;

        }

        // erase the last child position.
        childKeys[nkeys] = NULL;
        childRefs[nkeys] = null;

        // Remove the child from the dirty list.
        dirtyChildren.remove(child);

        // Clear the parent on the old child.
        child.parent = null;

        // one less the key in this node.
        nkeys--;

        if (child.isLeaf()) {

            btree.nleaves--;

        } else {

            btree.nnodes--;

        }

        // Deallocate the child.
        child.delete();

        if (btree.root == this) {

            /*
             * the root node is allowed to become deficient, but once we are
             * reduced to having no more keys in the root node it is replaced by
             * the last remaining child. that child MUST be a leaf which then
             * becomes the root leaf.
             */
            if (nkeys == 0 && !isLeaf()) {

                AbstractNode lastChild = getChild(0);

                lastChild.assertInvariants();

                assert lastChild.isLeaf();
                assert btree.nentries == lastChild.nkeys;

                System.err.println("Replacing root node: root=" + btree.root
                        + ", node=" + this + ", lastChild=" + lastChild);

                // replace the root node with a root leaf.
                btree.root = lastChild;

                // one less level in the btree.
                btree.height--;

                // deallocate this node.
                this.delete();

                // one less node in the tree.
                btree.nnodes--;

            }

        } else {

            /*
             * if a non-root node becomes deficient then it is joined with a
             * direct sibling. if this forces a merge with a sibling, then the
             * merged sibling will be removed from the parent which may force
             * the parent to become deficient in turn, and thereby trigger a
             * join() of the parent.
             */

            if (nkeys < minKeys) {

                join();

            }

        }

    }
    
    /**
     * This is invoked by {@link #removeChild(AbstractNode)} when the node is
     * reduced to a single child in order to replace the reference to the node
     * on its parent with the reference to the node's sole remaining child.
     * 
     * @param oldChild
     *            The node.
     * @param newChild
     *            The node's sole remaining child. This MAY be persistent since
     *            this operation does NOT change the persistent state of the
     *            newChild but only updates its transient state (e.g., its
     *            parent reference).
     */
    protected void replaceChild(AbstractNode oldChild,AbstractNode newChild) {
        
        assert oldChild != null;
        assert !oldChild.isDeleted();
        assert !oldChild.isPersistent();
        assert oldChild.parent.get() == this;
        assert oldChild.nkeys == 0;
        assertInvariants();
        oldChild.assertInvariants();
        newChild.assertInvariants();

        assert newChild != null;
        assert !newChild.isDeleted();
//        assert !newChild.isPersistent(); // MAY be persistent - does not matter.
        assert newChild.parent.get() == oldChild;

        assert oldChild != newChild;

        assert !isDeleted();
        assert !isPersistent();

        int i = getIndexOf( oldChild );

        dirtyChildren.remove(oldChild);

        if (newChild.isDirty()) {

            dirtyChildren.add(newChild);

        }

        // set the persistent key for the new child.
        childKeys[i] = (newChild.isPersistent() ? newChild.getIdentity() : NULL);

        // set the reference to the new child.
        childRefs[i] = new WeakReference<AbstractNode>(newChild);

        // Reuse the weak reference from the oldChild.
        newChild.parent = oldChild.parent;

    }
    
    /**
     * Return the child node or leaf at the specified index in this node. If the
     * node is not in memory then it is read from the store.
     * 
     * @param index
     *            The index in [0:nkeys].
     * 
     * @return The child node or leaf and never null.
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

            // patch parent reference since loaded from store.
            child.parent = new WeakReference<Node>(this);

            // patch the child reference.
            childRefs[index] = new WeakReference<AbstractNode>(child);

        }

        /*
         * @todo touch the node or leaf here? or here and in the return from
         * the recursive search and mutation methods?
         */
        
        assert child != null;

        return child;

    }

    /**
     * Iterator visits children, recursively expanding each child with a
     * post-order traversal of its children and finally visits this node
     * itself.
     */
    public Iterator postOrderIterator(final boolean dirtyNodesOnly) {

        if (dirtyNodesOnly && !isDirty()) {

            return EmptyIterator.DEFAULT;

        }

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

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the post-order iterator.
         * 
         * When dirtyNodesOnly is true we use a child iterator that makes a best
         * effort to only visit dirty nodes. Especially, the iterator MUST NOT
         * force children to be loaded from disk if the are not resident since
         * dirty nodes are always resident.
         * 
         * In order to guarentee that a node will still be dirty by the time
         * that the caller visits it the iterator must touch the node, thereby
         * placing it into the appropriate hard reference queue and incrementing
         * its reference counter. Evictions do NOT cause IO when the reference
         * is non-zero, so the node will not be made persistent as a result of
         * other node touches. However, the node can still be made persistent if
         * the caller explicitly writes the node onto the store.
         */

        // BTree.log.debug("node: " + this);
        return new Striterator(childIterator(dirtyNodesOnly)).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(Object childObj) {

                /*
                 * A child of this node.
                 */

                AbstractNode child = (AbstractNode) childObj;

                if (dirtyNodesOnly && !child.isDirty()) {

                    return EmptyIterator.DEFAULT;

                }

                if (child instanceof Node) {

                    /*
                     * The child is a Node (has children).
                     */

                    // BTree.log.debug("child is node: " + child);
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

                    // BTree.log.debug("child is leaf: " + child);
                    // Visit the leaf itself.
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
    public Iterator childIterator(boolean dirtyNodesOnly) {

        if( dirtyNodesOnly ) {
            
            return new DirtyChildIterator(this);
            
        } else {
            
            return new ChildIterator(this);
            
        }

    }

    public boolean dump(Level level, PrintStream out, int height, boolean recursive) {
        
        // True iff we will write out the node structure.
        final boolean debug = level.toInt() <= Level.DEBUG.toInt();

        // Set true iff an inconsistency is detected.
        boolean ok = true;
        
        if (parent != null && nkeys < minKeys) {
            // min keys failure.
            out.println("ERROR: too few keys: m=" + branchingFactor
                    + ", minKeys=" + minKeys + ", nkeys=" + nkeys + ", isLeaf="
                    + isLeaf());
            ok = false;
        }

        if (nkeys > maxKeys) {
            // max keys failure.
            out.println("ERROR: too many keys: m=" + branchingFactor
                    + ", maxKeys=" + maxKeys + ", nkeys=" + nkeys + ", isLeaf="
                    + isLeaf());
            ok = false;
        }

        if(this == btree.root) {
            if( parent != null ) {
                out.println("ERROR: this is the root, but the parent is not null.");
                ok = false;
            }
        } else {
            /*
             * Note: there is a difference between having a parent reference and
             * having the parent be reachable. However, we actually want to
             * maintain both -- a parent MUST always be reachable.
             */
            if( parent == null) {
                out.println("ERROR: the parent reference MUST be defined for a non-root node.");
                ok = false;
            } else if( parent.get() == null) {
                out.println("ERROR: the parent is not strongly reachable.");
                ok = false;
            }
        }

        if (debug) {
            out.println(indent(height) + "Node: " + toString());
            out.println(indent(height) + "  parent="
                    + (parent == null ? null : parent.get()));
            out.println(indent(height) + "  dirty=" + isDirty() + ", nkeys="
                    + nkeys + ", nchildren=" + (nkeys + 1) + ", minKeys="
                    + minKeys + ", maxKeys=" + maxKeys + ", branchingFactor="
                    + branchingFactor);
            out.println(indent(height) + "  keys=" + Arrays.toString(keys));
        }
        { // verify keys are monotonically increasing.
            int lastKey = IBTree.NEGINF;
            for (int i = 0; i < Math.min(nkeys,branchingFactor); i++) {
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
        if (debug) {
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
            out.print(indent(height) + "  #dirtyChildren="
                    + dirtyChildren.size() + " : {");
            int n = 0;
            Iterator<AbstractNode> itr = dirtyChildren.iterator();
            while (itr.hasNext()) {
                if (n++ > 0)
                    out.print(", ");
                out.print(itr.next());
            }
            out.println("}");
        }

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

        if( ! ok && ! debug ) {
            
            // FIXME show the node structure with the errors since we would not
            // have seen it otherwise.
            
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

                    /*
                     * Note: This condition is violated under our "delete" rule
                     * by removeChild().
                     */
                    
//                    if (child.isLeaf() && keys[i - 1] != child.keys[0]) {
//
//                        /*
//                         * While each key in a node always is the first key
//                         * of some leaf, we are only testing the direct
//                         * children here. Therefore if the children are not
//                         * leaves then we can not cross check their first
//                         * key with the keys on this node.
//                         */
//                        out.println(indent(height + 1)
//                                + "ERROR first key on child leaf must be "
//                                + keys[i - 1] + ", not " + child.keys[0]
//                                + " at index=" + i);
//
//                        ok = false;
//
//                    }

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

            if (dirty.size() != dirtyChildren.size()) {

                out.println(indent(height + 1) + "ERROR found " + dirty.size()
                        + " dirty children, but " + dirtyChildren.size()
                        + " in node's dirty list");

                ok = false;

            }

        }

        return ok;

    }

}
