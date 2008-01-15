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
public class Node extends AbstractNode implements INodeData {

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
    transient protected Reference<AbstractNode>[] childRefs;

    /**
     * <p>
     * The persistent keys of the childAddr nodes (may be nodes or leaves). The
     * capacity of this array is m, where m is the {@link #branchingFactor}.
     * Valid indices are in [0:nkeys+1] since nchildren := nkeys+1 for a
     * {@link Node}. The key is {@link #NULL} until the child has been
     * persisted. The protocol for persisting child nodes requires that we use a
     * pre-order traversal (the general case is a directed graph) so that we can
     * update the keys on the parent before the parent itself is persisted.
     * </p>
     * <p>
     * Note: It is an error if there is an attempt to serialize a node having a
     * null entry in this array and a non-null entry in the {@link #keys} array.
     * </p>
     * <p>
     * This array is dimensioned to one more than the maximum capacity so that
     * the child reference corresponding to the key that causes overflow and
     * forces the split may be inserted. This greatly simplifies the logic for
     * computing the split point and performing the split.
     * </p>
     */
    long[] childAddr;

    /**
     * The #of entries spanned by this node. This value should always be equal
     * to the sum of the defined values in {@link #childEntryCounts}.
     * 
     * When a node is split, the value is updated by subtracting off the counts
     * for the children that are being moved to the new sibling.
     * 
     * When a node is joined, the value is updated by adding in the counts for
     * the children that are being moved to the new sibling.
     * 
     * When a key is redistributed from a node to a sibling, the value is
     * updated by subtracting off the count for the child from the source
     * sibling and adding it in to this node.
     * 
     * This field is initialized by the various {@link Node} constructors.
     */
    int nentries;
    
    /**
     * The #of entries spanned by each direct child of this node.
     * 
     * The appropriate element in this array is incremented on all ancestor
     * nodes by {@link Leaf#insert(Object, Object)} and decremented on all
     * ancestors nodes by {@link Leaf#remove(Object)}. Since the ancestors are
     * guarenteed to be mutable as preconditions for those operations we are
     * able to traverse the {@link AbstractNode#parent} reference in a straight
     * forward manner.
     */
    int[] childEntryCounts;
    
    public int getEntryCount() {
    
        return nentries;
        
    }
    
    public int[] getChildEntryCounts() {
        
        return childEntryCounts;
        
    }

    /**
     * Apply the delta to the per-child count for this node and then recursively
     * ascend up the tree applying the delta to all ancestors of this node. This
     * is invoked solely by the methods that add and remove entries from a leaf
     * as those are the only methods that change the #of entries spanned by a
     * parent node.  Methods that split, merge, or redistribute keys have a net
     * zero effect on the #of entries spanned by the parent.
     * 
     * @param child
     *            The direct child.
     * @param delta
     *            The change in the #of spanned children.
     */
    protected void updateEntryCount(AbstractNode child, int delta) {
        
        int index = getIndexOf(child);
        
        childEntryCounts[ index ] += delta;
        
        nentries += delta;
        
        assert childEntryCounts[ index ] > 0;

        assert nentries > 0;
        
        if( parent != null ) {
            
            parent.get().updateEntryCount(this, delta);
            
        }
        
    }
    
//    public int getLeafCount() {
//        return nleaves;
//    }
//
//    public int getNodeCount() {
//        return nnodes;
//    }

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
     * @param branchingFactor
     *            The branching factor for the node (the maximum #of children
     *            allowed for the node).
     * @param stride
     *            The #of elements in a key.
     * @param nentries
     *            The #of entries spanned by this node.
     * @param nkeys
     *            The #of defined keys in <i>keys</i>.
     * @param keys
     *            The external keys (the array reference is copied NOT the array
     *            contents). The array MUST be dimensioned to
     *            <code>branchingFactor</code> to allow room for the insert
     *            key that places the node temporarily over capacity during a
     *            split.
     * @param childAddr
     *            The persistent identity for the direct children (the array
     *            reference is copied NOT the array contents). The array MUST be
     *            dimensioned to <code>branchingFactor+1</code> to allow room
     *            for the insert key that places the node temporarily over
     *            capacity during a split.
     * @param childEntryCounts
     *            The #of entries spanned by each direct child of this node (the
     *            array reference is copied NOT the array contents). The array
     *            MUST be dimensioned to <code>branchingFactor+1</code> to
     *            allow room for the insert key that places the node temporarily
     *            over capacity during a split.
     */
    protected Node(AbstractBTree btree, long addr, int branchingFactor,
            int nentries, IKeyBuffer keys, long[] childAddr,
            int[] childEntryCounts
            ) {

        super( btree, branchingFactor, false /* The node is NOT dirty */ );

        assert branchingFactor >= BTree.MIN_BRANCHING_FACTOR;
        
        assert nkeys < branchingFactor;

        assert childAddr.length == branchingFactor + 1;

        assert childEntryCounts.length == branchingFactor + 1;
        
        setIdentity(addr);

        this.nentries = nentries;
        
        this.nkeys = keys.getKeyCount();
        
        this.keys = keys; // steal reference.
        
        this.childAddr = childAddr;

        this.childEntryCounts = childEntryCounts;

        childRefs = new Reference[branchingFactor+1];

//        // must clear the dirty flag since we just de-serialized this node.
//        setDirty(false);

    }

    /**
     * Used to create a new node when a node is split.
     */
    protected Node(BTree btree) {

        super(btree, btree.branchingFactor, true /*dirty*/ );

        nentries = 0;
        
        keys = new MutableKeyBuffer(branchingFactor);

        childRefs = new Reference[branchingFactor+1];

        childAddr = new long[branchingFactor+1];

        childEntryCounts = new int[branchingFactor+1];

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
    protected Node(BTree btree, AbstractNode oldRoot, int nentries) {

        super(btree, btree.branchingFactor, true /*dirty*/ );

        // Verify that this is the root.
        assert oldRoot == btree.root;

        // The old root must be dirty when it is being split.
        assert oldRoot.isDirty();

        // #of entries spanned by the old root _before_ this split.
        this.nentries = nentries;
        
        keys = new MutableKeyBuffer( branchingFactor );

        childRefs = new Reference[branchingFactor+1];

        childAddr = new long[branchingFactor+1];

        childEntryCounts = new int[branchingFactor+1];

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

        childRefs[0] = btree.newRef(oldRoot);

        // #of entries from the old root _after_ the split.
        childEntryCounts[0] = oldRoot.getEntryCount();
        
//        dirtyChildren.add(oldRoot);

        oldRoot.parent = btree.newRef(this);

        /*
         * The tree is deeper since we just split the root node.
         */
        btree.height++;

        final int requiredQueueCapacity = 2 * (btree.height + 2);
        
        if ( requiredQueueCapacity > btree.writeRetentionQueue.capacity()) {
            
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
            
            throw new UnsupportedOperationException("writeRetentionQueue: capacity="
                    + btree.writeRetentionQueue.capacity() + ", but height="
                    + btree.height);
            
        }
        
        btree.nnodes++;

        btree.counters.rootsSplit++;

        // Note: nnodes and nleaves might not reflect rightSibling yet.
        if (btree.INFO) {
            BTree.log.info("increasing tree height: height=" + btree.height
                    + ", utilization=" + btree.getUtilization()[2] + "%");
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
     *            it. It is {@link #NULL} when this information is not available, e.g.,
     *            when the copyOnWrite action is triggered by a join() and we
     *            are cloning the sibling before we redistribute a key to the
     *            node/leaf on which the join was invoked.
     * 
     * FIXME Can't we just test to see if the child already has this node as its
     * parent reference and then skip it? If so, then that would remove a
     * toublesome parameter from the API.
     */
    protected Node(Node src, final long triggeredByChildId) {

        super(src);
        
        assert !src.isDirty();
        
        assert src.isPersistent();
        
//        assert triggeredByChild != null;

        nentries = src.nentries;
        
        /*
         * Steal the childAddr[] and childEntryCounts[] arrays.
         */
        {

            // steal reference and clear reference on the source.
            childAddr = src.childAddr; src.childAddr = null;
            
            // steal reference and clear reference on the source.
            childEntryCounts = src.childEntryCounts;

//            childAddr = new long[branchingFactor+1];
//
//            childEntryCounts = new int[branchingFactor+1];
//
//        // Note: There is always one more child than keys for a Node.
//        System.arraycopy(src.childAddr, 0, childAddr, 0, nkeys+1);
//
//        // Note: There is always one more child than keys for a Node.
//        System.arraycopy(src.childEntryCounts, 0, childEntryCounts, 0, nkeys+1);
            
        }

        /*
         * Steal strongly reachable unmodified children by setting their parent
         * fields to the new node. Stealing the child means that it MUST NOT be
         * used by its previous ancestor (our source node for this copy).
         */

//        childRefs = new WeakReference[branchingFactor+1];
        childRefs = src.childRefs; src.childRefs = null;

        for (int i = 0; i <= nkeys; i++) {

            AbstractNode child = childRefs[i] == null ? null : childRefs[i]
                    .get();

            if (child != null && child.identity != triggeredByChildId) {

                /*
                 * Copy on write should never trigger for a dirty node and only
                 * a dirty node can have dirty children.
                 */
                assert !child.isDirty();

                // Steal the child.
                child.parent = btree.newRef(this);

//                // Keep a reference to the clean child.
//                childRefs[i] = new WeakReference<AbstractNode>(child);
                    
            }
            
        }

    }

    public void delete() {
        
        super.delete();
        
        // clear state.
        childRefs = null;
        childAddr = null;
        nentries = 0;
        childEntryCounts = null;
        
    }
    
    /**
     * Always returns <code>false</code>.
     */
    final public boolean isLeaf() {

        return false;

    }

    final public int getChildCount() {
        
        return nkeys+1;
        
    }
    
    final public long[] getChildAddr() {
        
        return childAddr;
        
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

        childAddr[i] = child.getIdentity();

//        if (!dirtyChildren.remove(child)) {
//
//            throw new AssertionError("Child was not on dirty list.");
//
//        }
        
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

            if (childAddr[i] == oldChildKey) {

                if (true) {

                    /*
                     * Do some paranoia checks.
                     */

                    AbstractNode oldChild = childRefs[i] != null ? childRefs[i]
                            .get() : null;

                    if (oldChild != null) {

                        assert oldChild.isPersistent();

//                        assert !dirtyChildren.contains(oldChild);

                    }

                }

                // Clear the old key.
                childAddr[i] = NULL;

                // Stash reference to the new child.
                childRefs[i] = btree.newRef(newChild);

//                // Add the new child to the dirty list.
//                dirtyChildren.add(newChild);

                // Set the parent on the new child.
                newChild.parent = btree.newRef(this);

                return;

            }

        }

        System.err.println("this: "); dump(Level.DEBUG,System.err);
        System.err.println("newChild: "); newChild.dump(Level.DEBUG,System.err);
        throw new IllegalArgumentException("Not our child : oldChildKey="
                + oldChildKey);

    }

    /**
     * Insert or update one or more tuples. For each tuple processed, this finds
     * the index of the first key in the node whose value is greater than or
     * equal to the key associated with that tuple. The insert operation is then
     * delegated to the child node or leaf at position <code>index - 1</code>.
     * 
     * @return The #of tuples processed.
     */
    public int batchInsert(BatchInsert op) {

        assert !deleted;
        
        if(btree.debug) assertInvariants();

        btree.touch(this);

        int childIndex = findChild(op.keys[op.tupleIndex]);

        AbstractNode child = (AbstractNode) getChild(childIndex);

        return child.batchInsert(op);

    }

    public Object insert(byte[] key,Object value) {

        assert !deleted;
        
        if(btree.debug) assertInvariants();

        btree.touch(this);

        int childIndex = findChild(key);

        AbstractNode child = (AbstractNode) getChild(childIndex);

        return child.insert(key,value);

    }

    /**
     * Looks up one or more tuples. For each tuple processed, this finds the
     * index of the first key in the node whose value is greater than or equal
     * to the key associated with that tuple. The lookup operation is then
     * delegated to the child node or leaf at position <code>index - 1</code>.
     * 
     * @return The #of tuples processed.
     */
    public int batchLookup(BatchLookup op) {

        assert !deleted;
        
        if(btree.debug) assertInvariants();

        btree.touch(this);

        final int childIndex = findChild(op.keys[op.tupleIndex]);

        AbstractNode child = (AbstractNode)getChild(childIndex);

        return child.batchLookup(op);
        
    }
    
    public Object lookup(byte[] key) {

        assert !deleted;
        
        if(btree.debug) assertInvariants();

        btree.touch(this);

        final int childIndex = findChild(key);

        AbstractNode child = (AbstractNode)getChild(childIndex);

        return child.lookup(key);
        
    }
    
    /**
     * Existence test for up one or more tuples. For each tuple processed, this
     * finds the index of the first key in the node whose value is greater than
     * or equal to the key associated with that tuple. The lookup operation is
     * then delegated to the child node or leaf at position
     * <code>index - 1</code>.
     * 
     * @return The #of tuples processed.
     */
    public int batchContains(BatchContains op) {

        assert !deleted;
        
        if(btree.debug) assertInvariants();

        btree.touch(this);

        final int childIndex = findChild(op.keys[op.tupleIndex]);

        AbstractNode child = (AbstractNode)getChild(childIndex);

        return child.batchContains(op);
        
    }
    
    public boolean contains(byte[] searchKey) {

        assert !deleted;
        
        if(btree.debug) assertInvariants();

        btree.touch(this);

        final int childIndex = findChild(searchKey);

        AbstractNode child = (AbstractNode)getChild(childIndex);

        return child.contains(searchKey);
        
    }
    
    /**
     * Remove zero or more tuples. For each tuple processed, this finds the
     * index of the first key in the node whose value is greater than or equal
     * to the key associated with that tuple. The remove operation is then
     * delegated to the child node or leaf at position <code>index - 1</code>.
     * 
     * @return The #of tuples processed (not necessarily the #of tuples
     *         removed).
     */
    public int batchRemove(BatchRemove op) {

        assert !deleted;
        
        if(btree.debug) assertInvariants();
        
        btree.touch(this);

        int childIndex = findChild(op.keys[op.tupleIndex]);

        AbstractNode child = (AbstractNode)getChild(childIndex);

        return child.batchRemove(op);
        
    }
    
    public Object remove(byte[] key) {

        assert !deleted;
        
        if(btree.debug) assertInvariants();
        
        btree.touch(this);

        int childIndex = findChild(key);

        AbstractNode child = (AbstractNode)getChild(childIndex);

        return child.remove(key);
        
    }
    
    /**
     * @todo convert to batch api and handle searchKeyOffset.
     */
    public int indexOf(byte[] key) {

        assert !deleted;

        btree.touch(this);

        final int childIndex = findChild(key);

        AbstractNode child = (AbstractNode)getChild(childIndex);

        /*
         * Compute running total to this child index plus [n], possible iff
         * successful search at the key level in which case we do not need to
         * pass n down.
         */
        
        int offset = 0;
        
        for( int i=0; i<childIndex; i++) {
        
            offset += childEntryCounts[i];
            
        }
        
        // recursive invocation, eventually grounds out on a leaf.
        int ret = child.indexOf(key);

        if( ret<0 ) {
            
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
     * Recursive search for the key at the specified entry index.
     * 
     * @param entryIndex
     *            The index of the entry (relative to the first entry spanned by
     *            this node).
     * 
     * @return The key at that entry index.
     */
    public byte[] keyAt(final int entryIndex) {
        
        if (entryIndex < 0)
            throw new IndexOutOfBoundsException("negative: "+entryIndex);

        if (entryIndex >= nentries) {

            throw new IndexOutOfBoundsException("too large: entryIndex="
                    + entryIndex + ", but nentries=" + nentries);
            
        }

        // index of the child that spans the desired entry.
        int childIndex = 0;
        
        // corrects the #of spanned entries by #skipped over.
        int remaining = entryIndex;

        // search for child spanning the desired entry index.
        for( ; childIndex<=nkeys; childIndex++) {

            int nspanned = childEntryCounts[childIndex];
            
            if (remaining < nspanned) {
             
                // found the child index spanning the desired entry.
                break;
                
            }
            
            remaining -= nspanned;
            
            assert remaining >= 0;

        }

        AbstractNode child = getChild(childIndex);

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
    public Object valueAt(final int entryIndex) {
        
        if (entryIndex < 0)
            throw new IndexOutOfBoundsException("negative: "+entryIndex);

        if (entryIndex >= nentries) {

            throw new IndexOutOfBoundsException("too large: entryIndex="
                    + entryIndex + ", but nentries=" + nentries);
            
        }

        // index of the child that spans the desired entry.
        int childIndex = 0;
        
        // corrects the #of spanned entries by #skipped over.
        int remaining = entryIndex;

        // search for child spanning the desired entry index.
        for( ; childIndex<=nkeys; childIndex++) {

            int nspanned = childEntryCounts[childIndex];
            
            if (remaining < nspanned) {
             
                // found the child index spanning the desired entry.
                break;
                
            }
            
            remaining -= nspanned;
            
            assert remaining >= 0;

        }

        AbstractNode child = getChild(childIndex);

        return child.valueAt(remaining);
        
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
     * @param searchkey
     *            The probe key.
     * 
     * @return The child to be searched next for that key.
     */
    final protected int findChild(final byte[] searchKey) {

        int childIndex = this.keys.search(searchKey);

        if (childIndex >= 0) {

            /*
             * exact match - use the next child.
             */

            return childIndex + 1;

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
     * <p>
     * Note: splitting a node causes entry counts for the relocated childen to
     * be relocated to the new rightSibling but it does NOT change the #of
     * entries on the parent.
     * </p>
     */
    protected IAbstractNode split() {

        assert isDirty(); // MUST be mutable.
        assert nkeys == maxKeys+1; // MUST be over capacity by one.

        // cast to mutable implementation class.
        BTree btree = (BTree)this.btree;

        btree.counters.nodesSplit++;

        /*
         * The #of entries spanned by this node _before_ the split.
         */
        final int nentriesBeforeSplit = nentries;
        
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
         * 
         * Note: Unlike a leaf, we are not free to choose the shortest separator
         * key in a node. This is because separator keys describe separations
         * among the leaves of the tree. This issue is covered by Bayer's
         * article on prefix trees.
         */
        final byte[] separatorKey = keys.getKey(splitIndex);

        // create the new rightSibling node.
        final Node rightSibling = new Node(btree);

        /*
         * Tunnel through to the mutable keys object.
         */
        final MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;
        final MutableKeyBuffer skeys = (MutableKeyBuffer)rightSibling.keys;

        if (INFO) {
            log.info("this=" + this + ", nkeys=" + nkeys + ", splitIndex="
                    + splitIndex + ", separatorKey="
                    + keyAsString(separatorKey)
                    );
            if(DEBUG) dump(Level.DEBUG,System.err);
        }

        /* 
         * copy keys and values to the new rightSibling.
         */
        
        int j = 0;
        
//        // #of spanned entries being moved to the new rightSibling.
//        int nentriesMoved = 0;
        
        for (int i = splitIndex + 1 ; i < nchildren; i++, j++) {

            if ( i + 1 < nchildren) {
            
                /*
                 * Note: keys[nchildren-1] is undefined.
                 */
//                rightSibling.setKey(j, getKey(i));
                rightSibling.copyKey(j, this.keys, i);
                
            }
            
            rightSibling.childRefs[j] = childRefs[i];
            
            rightSibling.childAddr[j] = childAddr[i];

            final int childEntryCount = childEntryCounts[i];
            
            rightSibling.childEntryCounts[j] = childEntryCount;
            
            rightSibling.nentries += childEntryCount;
            
            this.nentries -= childEntryCount;
            
//            nentriesMoved += childEntryCounts[i];
            
            AbstractNode tmp = (childRefs[i] == null ? null : childRefs[i]
                    .get());
            
            if (tmp != null) {
        
                /*
                 * The child node is in memory.
                 * 
                 * Update its parent reference.
                 */
                
                tmp.parent = btree.newRef(rightSibling);
                
            }

            /*
             * Clear out the old keys and values, including keys[splitIndex]
             * which is being moved to the parent.
             */

            if (i + 1 < nchildren) {
            
                keys.zeroKey(i);
                
                nkeys--; keys.nkeys--; // one less key here.

                rightSibling.nkeys++; skeys.nkeys++; // more more key there.
                
            }
            
            childRefs[i] = null;
            
            childAddr[i] = NULL;

            childEntryCounts[i] = 0;
            
        }

        /* 
         * Clear the key that is being move into the parent.
         */
        keys.zeroKey(splitIndex);
        
        nkeys--; keys.nkeys--;
        
        Node p = getParent();

        if (p == null) {

            /*
             * Use a special constructor to split the root.  The result is a new
             * node with zero keys and one child (this node).
             */

            p = new Node(btree, this, nentriesBeforeSplit);

        } else {

            // this node now has fewer entries
            p.childEntryCounts[p.getIndexOf(this)] -= rightSibling.nentries;
        
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
    protected void redistributeKeys(AbstractNode sibling,boolean isRightSibling) {

        // the sibling of a Node must be a Node.
        final Node s = (Node) sibling;
        
        assert dirty;
        assert !deleted;
        assert !isPersistent();
        // verify that this leaf is deficient.
        assert nkeys < minKeys;
        // verify that this leaf is under minimum capacity by one key.
        assert nkeys == minKeys - 1;
        
        assert s != null;
        // the sibling MUST be _OVER_ the minimum #of keys/values.
        assert s.nkeys > minKeys;
        assert s.dirty;
        assert !s.deleted;
        assert !s.isPersistent();
        
        final Node p = getParent();
        
        // children of the same node.
        assert s.getParent() == p;
        
        if (INFO) {
            log.info("this="+this+", sibling="+sibling+", rightSibling="+isRightSibling);
            if(DEBUG) {
                System.err.println("this"); dump(Level.DEBUG,System.err);
                System.err.println("sibling"); sibling.dump(Level.DEBUG,System.err);
                System.err.println("parent"); p.dump(Level.DEBUG,System.err);
            }
        }
        
        /*
         * The index of this leaf in its parent. we note this before we
         * start mucking with the keys.
         */
        final int index = p.getIndexOf(this);
        
        /*
         * Tunnel through to the mutable keys object.
         */
        final MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;
        final MutableKeyBuffer skeys = (MutableKeyBuffer)s.keys;

        /*
         * determine which leaf is earlier in the key ordering and get the
         * index of the sibling.
         */
        if( isRightSibling/*keys[nkeys-1] < s.keys[0]*/) {
        
            /*
             * redistributeKeys(this,rightSibling). all we have to do is replace
             * the separatorKey in the parent with the first key from the
             * rightSibling and copy the old separatorKey from the parent to the
             * end of the keys in this node. we then close up the hole that this
             * left at index 0 in the rightSibling.
             */

            // Mopy the first key/child from the rightSibling.
//            setKey(nkeys, p.getKey(index)); // copy the separatorKey from the parent.
            copyKey(nkeys, p.keys, index); // copy the separatorKey from the parent.
//            p.setKey(index, s.getKey(0)); // update the separatorKey from the rightSibling.
            p.copyKey(index, s.keys, 0); // update the separatorKey from the rightSibling.
            childRefs[nkeys+1] = s.childRefs[0]; // copy the child from the rightSibling.
            childAddr[nkeys+1] = s.childAddr[0];
            final int siblingChildCount = s.childEntryCounts[0]; // #of spanned entries being moved.
            childEntryCounts[nkeys+1] = siblingChildCount;
            AbstractNode child = childRefs[nkeys+1]==null?null:childRefs[nkeys+1].get();
            if( child!=null ) {
                child.parent = btree.newRef(this);
//                if( child.isDirty() ) {
//                    if(!s.dirtyChildren.remove(child)) throw new AssertionError();
//                    if(!dirtyChildren.add(child)) throw new AssertionError();
//                }
            }
            
            // copy down the keys on the right sibling to cover up the hole.
            System.arraycopy(skeys.keys, 1, skeys.keys, 0, s.nkeys-1);
            System.arraycopy(s.childRefs, 1, s.childRefs, 0, s.nkeys);
            System.arraycopy(s.childAddr, 1, s.childAddr, 0, s.nkeys);
            System.arraycopy(s.childEntryCounts, 1, s.childEntryCounts, 0, s.nkeys);

            // erase exposed key/value on rightSibling that is no longer defined.
            skeys.zeroKey(s.nkeys-1);
            s.childRefs[s.nkeys] = null;
            s.childAddr[s.nkeys] = NULL;
            s.childEntryCounts[s.nkeys] = 0;

            // update parent : N more entries spanned by this child.
            p.childEntryCounts[index] += siblingChildCount;
            // update parent : N fewer entries spanned by our right sibling.
            p.childEntryCounts[index+1] -= siblingChildCount;

            // update #of entries spanned by this node.
            nentries += siblingChildCount;
            // update #of entries spanned by our rightSibling.
            s.nentries -= siblingChildCount;

            s.nkeys--; skeys.nkeys--;
            this.nkeys++; keys.nkeys++;
            
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
            System.arraycopy(childRefs, 0, childRefs, 1, nkeys+1);
            System.arraycopy(childAddr, 0, childAddr, 1, nkeys+1);
            System.arraycopy(childEntryCounts, 0, childEntryCounts, 1, nkeys+1);
            
            // move the last key/child from the leftSibling to this node.
//            setKey(0, p.getKey(index-1)); // copy the separatorKey from the parent.
            copyKey(0, p.keys, index-1); // copy the separatorKey from the parent.
//            p.setKey(index-1, s.getKey(s.nkeys-1)); // update the separatorKey
            p.copyKey(index-1, s.keys, s.nkeys-1); // update the separatorKey
            childRefs[0] = s.childRefs[s.nkeys];
            childAddr[0] = s.childAddr[s.nkeys];
            final int siblingChildCount = s.childEntryCounts[s.nkeys];
            childEntryCounts[0] = siblingChildCount;
            AbstractNode child = childRefs[0]==null?null:childRefs[0].get();
            if( child!=null ) {
                child.parent = btree.newRef(this);
//                if(child.isDirty()) {
//                    if(!s.dirtyChildren.remove(child)) throw new AssertionError();
//                    if(!dirtyChildren.add(child)) throw new AssertionError();
//                }
            }
            skeys.zeroKey(s.nkeys-1);
            s.childRefs[s.nkeys] = null;
            s.childAddr[s.nkeys] = NULL;
            s.childEntryCounts[s.nkeys] = 0;
            s.nkeys--; skeys.nkeys--;
            this.nkeys++; keys.nkeys++;

            // update parent : N more entries spanned by this child.
            p.childEntryCounts[index] += siblingChildCount;
            // update parent : N fewer entries spanned by our leftSibling.
            p.childEntryCounts[index-1] -= siblingChildCount;
            // update #of entries spanned by this node.
            nentries += siblingChildCount;
            // update #of entries spanned by our leftSibling.
            s.nentries -= siblingChildCount;

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
    protected void merge(AbstractNode sibling,boolean isRightSibling) {

        // the sibling of a Node must be a Node.
        final Node s = (Node) sibling;

        assert s != null;
        assert !s.deleted;
        // verify that this node is deficient.
        assert nkeys < minKeys;
        // verify that this node is under minimum capacity by one key.
        assert nkeys == minKeys - 1;
        // the sibling MUST at the minimum #of keys/values.
        assert s.nkeys == s.minKeys;

        final Node p = getParent();
        
        // children of the same node.
        assert s.getParent() == p;

        if (INFO) {
            log.info("this=" + this + ", sibling=" + sibling
                    + ", rightSibling=" + isRightSibling);
            if(DEBUG) {
                System.err.println("this"); dump(Level.DEBUG,System.err);
                System.err.println("sibling"); sibling.dump(Level.DEBUG,System.err);
                System.err.println("parent"); p.dump(Level.DEBUG,System.err);
            }
        }

        final int siblingEntryCount = s.getEntryCount();
        
        /*
         * The index of this node in its parent. we note this before we
         * start mucking with the keys.
         */
        final int index = p.getIndexOf(this);

        /*
         * Tunnel through to the mutable keys object.
         * 
         * Note: since we do not require the sibling to be mutable we have to
         * test and convert the key buffer for the sibling to a mutable key
         * buffer if the sibling is immutable. Also note that the sibling MUST
         * have the minimum #of keys for a merge so we set the capacity of the
         * mutable key buffer to that when we have to convert the siblings keys
         * into mutable form in order to perform the merge operation.
         */
        final MutableKeyBuffer keys = (MutableKeyBuffer) this.keys;
        final MutableKeyBuffer skeys = (s.keys instanceof MutableKeyBuffer ? (MutableKeyBuffer) s.keys
                : ((ImmutableKeyBuffer) s.keys).toMutableKeyBuffer());

        /*
         * determine which node is earlier in the key ordering so that we know
         * whether the sibling's keys will be inserted at the front of this
         * nodes's keys or appended to this nodes's keys.
         */
        if( isRightSibling/*keys[nkeys-1] < s.keys[0]*/) {
            
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
//            this.setKey(nkeys++, p.getKey(index));
            this.copyKey(nkeys, p.keys, index);
            nkeys++; keys.nkeys++;
            
            /*
             * Copy in the keys and children from the sibling. Note that the
             * children are copied to the position nkeys NOT nkeys+1 since the
             * first child needs to appear at the same position as the
             * separatorKey that we copied from the parent.
             */
            System.arraycopy(skeys.keys, 0, keys.keys, nkeys, s.nkeys);
            System.arraycopy(s.childRefs, 0, this.childRefs, nkeys, s.nkeys+1);
            System.arraycopy(s.childAddr, 0, this.childAddr, nkeys, s.nkeys+1);
            System.arraycopy(s.childEntryCounts, 0, this.childEntryCounts, nkeys, s.nkeys+1);
            
            // update parent on children
            Reference<Node> weakRef = btree.newRef(this);
            for( int i=0; i<s.nkeys+1; i++ ) {
                AbstractNode child = s.childRefs[i]==null?null:s.childRefs[i].get();
                if( child!=null) {
                    child.parent = weakRef;
//                    if( child.isDirty() ) {
//                        // record hard references for dirty children.
//                        dirtyChildren.add(child);
//                    }
                }
            }
            
            /* 
             * Adjust the #of keys in this leaf.
             */
            this.nkeys += s.nkeys; keys.nkeys += s.nkeys;
            /*
             * Note: in this case we have to replace the separator key for this
             * node with the separator key for its right sibling.
             * 
             * Note: This temporarily causes the duplication of a separator key
             * in the parent. However, the separator key for the right sibling
             * will be deleted when the sibling is removed from the parent
             * below.
             */
//            p.setKey(index, p.getKey(index+1));
            p.copyKey(index, p.keys, index+1);
            
            // reallocate spanned entries from the sibling to this node.
            p.childEntryCounts[index] += siblingEntryCount;
            this.nentries += siblingEntryCount;

            if(btree.debug) assertInvariants();
            
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
            System.arraycopy(keys.keys, 0, keys.keys, s.nkeys+1, this.nkeys);
            System.arraycopy(this.childRefs, 0, this.childRefs, s.nkeys+1, this.nkeys+1);
            System.arraycopy(this.childAddr, 0, this.childAddr, s.nkeys+1, this.nkeys+1);
            System.arraycopy(this.childEntryCounts, 0, this.childEntryCounts, s.nkeys+1, this.nkeys+1);
            
            // copy keys and values from the sibling to index 0 of this leaf.
            System.arraycopy(skeys.keys, 0, keys.keys, 0, s.nkeys);
            System.arraycopy(s.childRefs, 0, this.childRefs, 0, s.nkeys+1);
            System.arraycopy(s.childAddr, 0, this.childAddr, 0, s.nkeys+1);
            System.arraycopy(s.childEntryCounts, 0, this.childEntryCounts, 0, s.nkeys+1);

            // copy the separatorKey from the parent.
            //this.setKey(s.nkeys, p.getKey(index - 1));
            this.copyKey(s.nkeys, p.keys, index - 1);
            
            // update parent on children.
            Reference<Node> weakRef = btree.newRef(this);
            for( int i=0; i<s.nkeys+1; i++ ) {
                AbstractNode child = s.childRefs[i]==null?null:s.childRefs[i].get();
                if( child!=null) {
                    child.parent = weakRef;
//                    if( child.isDirty() ) {
//                        // record hard references for dirty children.
//                        dirtyChildren.add(child);
//                    }
                }
            }

            // we gain nkeys from the sibling and one key from the parent.
            this.nkeys += s.nkeys + 1; keys.nkeys += s.nkeys + 1;

            // reallocate spanned entries from the sibling to this node.
            p.childEntryCounts[index] += s.getEntryCount();
            this.nentries += siblingEntryCount;

            if(btree.debug) assertInvariants();
            
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
     * no effect on the #of entries spanned by the parent.
     * 
     * @param key
     *            The key on which the old node was split (this is an
     *            polymorphic array object NOT an autoboxed primitive).
     * 
     * @param child
     *            The new node.
     */
    protected void insertChild(byte[] key, AbstractNode child) {

        if(btree.debug) assertInvariants();
        
//        assert key > IIndex.NEGINF && key < IIndex.POSINF;
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
        int childIndex = this.keys.search(key);

        if (childIndex >= 0) {

            /*
             * The key is already present. This is an error.
             */

            btree.dump(Level.DEBUG,System.err);
            
            throw new AssertionError("Split on existing key: key="
                    + keyAsString(key));

        }

        // Convert the position to obtain the insertion point.
        childIndex = -childIndex - 1;

        assert childIndex >= 0 && childIndex <= nkeys;

        /*
         * copy down per-key data.
         */
        final MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;

        final int length = nkeys - childIndex;

        if (length > 0) {
            
            System.arraycopy(keys.keys, childIndex, keys.keys,
                    (childIndex + 1), length);

        }

        /*
         * copy down per-child data. #children == nkeys+1. child[0] is
         * always defined.
         */
        System.arraycopy(childAddr, childIndex + 1, childAddr, childIndex + 2, length);
        System.arraycopy(childRefs, childIndex + 1, childRefs, childIndex + 2, length);
        System.arraycopy(childEntryCounts, childIndex + 1, childEntryCounts, childIndex + 2, length);

        /*
         * Insert key at index.
         */
//        setKey(childIndex, key);
          keys.keys[childIndex] = key;
//        System.arraycopy(key, 0, keys.keys, childIndex, 1);

        /*
         * Insert child at index+1.
         */
        childRefs[childIndex + 1] = btree.newRef(child);

        childAddr[childIndex + 1] = NULL;

        final int childEntryCount = child.getEntryCount();
        
        childEntryCounts[ childIndex + 1 ] = childEntryCount;
        
//        if( parent != null ) {
//            
//            parent.get().updateEntryCount(this, childEntryCount);
//            
//        }
        
//        nentries += childEntryCount;
        
//        dirtyChildren.add(child);

        child.parent = btree.newRef(this);

        nkeys++; keys.nkeys++;

        if (nkeys == maxKeys+1) {

            /*
             * The node is over capacity so we split the node, creating a new
             * rightSibling and insert( separatorKey, rightSibling ) into the
             * parent.
             */

            Node rightSibling = (Node) split();

            // assert additional post-split invariants.
            if (btree.debug) {
                getParent().assertInvariants();
                rightSibling.assertInvariants();
            }
            
            return;

        }

        if(btree.debug) assertInvariants();

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
    protected AbstractNode getLeftSibling(AbstractNode child, boolean materialize) {

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

            if (sibling == null) {
                
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
    protected AbstractNode getRightSibling(AbstractNode child, boolean materialize) {

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
     * @return The index in {@link #childRefs} where that child is found. This
     *         may also be used as an index into {@link #childAddr} and
     *         {@link #childEntryCounts}.
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
         * @todo 85% of the use of this method is updateEntryCount(). Since that
         * method is only called on update, we would do well to buffer hard
         * references during descent and test the buffer in this method before
         * performing a full search. Since concurrent writers are not allowed,
         * we only need a single buffer whose height is the height of the tree.
         * This should prove especially beneficial for larger branching factors.
         * For smaller branching factors the cost might be so small as to be
         * ignorable.
         * 
         * @see Leaf#merge(Leaf sibling,boolean isRightSibling)
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
     */
    protected void removeChild(AbstractNode child) {
        
        assert child != null;
        assert !child.deleted;
        assert child.parent.get() == this;

        assert dirty;
        assert !deleted;
        assert !isPersistent();

        // cast to mutable implementation class.
        BTree btree = (BTree)this.btree;
        
        if(btree.debug) assertInvariants();

        if (INFO) {
            log.info("this="+this+", child="+child);
            /* Note: dumping [this] or the [child] will throw false exceptions at this point - they are in an intermediate state. */
//            if(DEBUG) {
//                System.err.println("this"); dump(Level.DEBUG,System.err);
//                System.err.println("child"); child.dump(Level.DEBUG,System.err);
//            }
        }

        int i = getIndexOf(child);

        /*
         * Note: these comments may be dated and need review.
         * 
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

        // Tunnel through to the mutable keys object.
        final MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;

        if (lengthKeyCopy > 0) {

            System.arraycopy(keys.keys, index + 1, keys.keys, index,
                    lengthKeyCopy);

        }

        if (lengthChildCopy > 0) {

            System.arraycopy(childAddr, index + 1, childAddr, index,
                    lengthChildCopy);

            System.arraycopy(childRefs, index + 1, childRefs, index,
                    lengthChildCopy);

            System.arraycopy(childEntryCounts, index + 1, childEntryCounts, index,
                    lengthChildCopy);

        }

        /*
         * Erase the data that were exposed by this operation. Note that there
         * is one fewer keys than children so ....
         */

        if (nkeys > 0) {

            // erase the last key position.
            keys.zeroKey(nkeys - 1);

        }

        // erase the last child position.
        childAddr[nkeys] = NULL;
        childRefs[nkeys] = null;
        childEntryCounts[nkeys] = 0;

        // Clear the parent on the old child.
        child.parent = null;

        // one less the key in this node.
        nkeys--; keys.nkeys--;

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
             * the last remaining child.
             */
            if (nkeys == 0 && !isLeaf()) {

                AbstractNode lastChild = getChild(0);

                if(btree.debug) lastChild.assertInvariants();

                if(DEBUG) {
                    log.debug("replacing root: root=" + btree.root + ", node="
                            + this + ", lastChild=" + lastChild);
                    System.err.println("root"); btree.root.dump(Level.DEBUG,System.err);
                    System.err.println("this"); this.dump(Level.DEBUG,System.err);
                    System.err.println("lastChild"); lastChild.dump(Level.DEBUG,System.err);
                }
                
                final boolean wasDirty = btree.root.dirty;
                
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

                if(btree.INFO) {
                    BTree.log.info("reduced tree height: height="
                            + btree.height + ", newRoot=" + btree.root);
                }
                
                btree.counters.rootsJoined++;

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
    
//    /**
//     * This is invoked by {@link #removeChild(AbstractNode)} when the node is
//     * reduced to a single child in order to replace the reference to the node
//     * on its parent with the reference to the node's sole remaining child.
//     * 
//     * @param oldChild
//     *            The node.
//     * @param newChild
//     *            The node's sole remaining child. This MAY be persistent since
//     *            this operation does NOT change the persistent state of the
//     *            newChild but only updates its transient state (e.g., its
//     *            parent reference).
//     */
//    protected void replaceChild(AbstractNode oldChild,AbstractNode newChild) {
//        
//        assert oldChild != null;
//        assert !oldChild.isDeleted();
//        assert !oldChild.isPersistent();
//        assert oldChild.parent.get() == this;
//        assert oldChild.nkeys == 0;
//        assertInvariants();
//        oldChild.assertInvariants();
//        newChild.assertInvariants();
//
//        assert newChild != null;
//        assert !newChild.isDeleted();
////        assert !newChild.isPersistent(); // MAY be persistent - does not matter.
//        assert newChild.parent.get() == oldChild;
//
//        assert oldChild != newChild;
//
//        assert !isDeleted();
//        assert !isPersistent();
//
//        int i = getIndexOf( oldChild );
//
////        dirtyChildren.remove(oldChild);
////
////        if (newChild.isDirty()) {
////
////            dirtyChildren.add(newChild);
////
////        }
//
//        // set the persistent key for the new child.
//        childAddr[i] = (newChild.isPersistent() ? newChild.getIdentity() : NULL);
//
//        // set the reference to the new child.
//        childRefs[i] = new WeakReference<AbstractNode>(newChild);
//
//        // Reuse the weak reference from the oldChild.
//        newChild.parent = oldChild.parent;
//
//    }
    
    /**
     * Return the child node or leaf at the specified index in this node. If the
     * node is not in memory then it is read from the store.
     * 
     * @param index
     *            The index in [0:nkeys].
     * 
     * @return The child node or leaf and never null.
     */
    final protected AbstractNode getChild(int index) {

        if(Thread.interrupted()) {
            /*
             * This method is called relatively often - it is used each time we
             * descend the tree. We check whether or not the thread has been
             * interrupted so that we can abort running tasks quickly.
             */
            throw new RuntimeException(new InterruptedException());
        }
        
        assert index >= 0 && index <= nkeys;

        final Reference<AbstractNode> childRef = childRefs[index];

        AbstractNode child = childRef == null ? null : childRef.get();

        if (child == null) {
            
            /*
             * Note: This is synchronized in order to make sure that concurrent
             * readers are single-threaded at the point where they read a node
             * or leaf from the store into the btree. This ensures that only one
             * thread will read the missing child in from the store and that
             * inconsistencies in the data structure can not arise from
             * concurrent readers.
             */

            synchronized (this) {

                long key = childAddr[index];

                if (key == NULL) {
                    dump(Level.DEBUG, System.err);
                    throw new AssertionError(
                            "Child does not have persistent identity: this="
                                    + this + ", index=" + index);
                }

                child = btree.readNodeOrLeaf(key);

                // patch parent reference since loaded from store.
                child.parent = btree.newRef(this);

                // patch the child reference.
                childRefs[index] = btree.newRef(child);

            }
        
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

        if (dirtyNodesOnly && !dirty) {

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
     * Iterator visits children in the specified half-open key range,
     * recursively expanding each child with a post-order traversal of its
     * children and finally visits this node itself.
     */
    public Iterator postOrderIterator(byte[] fromKey, byte[] toKey) {

        /*
         * Iterator append this node to the iterator in the post-order
         * position.
         */

        return new Striterator(postOrderIterator2(fromKey,toKey))
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
         * The iterator must touch the node in order to guarentee that a node
         * will still be dirty by the time that the caller visits it. This
         * places the node onto the hard reference queue and increments its
         * reference counter. Evictions do NOT cause IO when the reference is
         * non-zero, so the node will not be made persistent as a result of
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

                if (dirtyNodesOnly && !child.dirty) {

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
     * Visits the children (recursively) using post-order traversal, but
     * does NOT visit this node.
     */
    private Iterator postOrderIterator2(final byte[] fromKey, final byte[] toKey) {

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
        return new Striterator(childIterator(fromKey,toKey)).addFilter(new Expander() {

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

                    // BTree.log.debug("child is node: " + child);
                    // visit the children (recursive post-order traversal).
                    Striterator itr = new Striterator(((Node) child)
                            .postOrderIterator2(fromKey,toKey));

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

    /**
     * Iterator visits the direct child nodes in the external key ordering.
     */
    public Iterator childIterator(byte[] fromKey, byte[] toKey) {
        
        return new ChildIterator(this,fromKey,toKey);
        
    }
    
    public boolean dump(Level level, PrintStream out, int height, boolean recursive) {
        
        // True iff we will write out the node structure.
        final boolean debug = level.toInt() <= Level.DEBUG.toInt();

        // Set true iff an inconsistency is detected.
        boolean ok = true;
        
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

        {   // nentries
            if (this == btree.getRoot()) {
                if (this.nentries != btree.getEntryCount()) {
                    out.println(indent(height)
                            + "ERROR: root node has nentries=" + this.nentries
                            + ", but btree has nentries="
                            + btree.getEntryCount());
                    ok = false;
                }
            }
            {
                int nentries = 0;
                for (int i = 0; i <= nkeys; i++) {
                    nentries += childEntryCounts[i];
                    if (nentries <= 0) {
                        out.println(indent(height) + "ERROR: childEntryCount["
                                + i + "] is non-positive");
                        ok = false;
                    }
                }
                if (nentries != this.nentries) {
                    out.println(indent(height) + "ERROR: nentries("
                            + this.nentries
                            + ") does not agree with sum of per-child counts("
                            + nentries + ")");
                    ok = false;
                }
            }
        }

        if (this == btree.getRoot()) {
            if (parent != null) {
                out
                        .println(indent(height)
                                + "ERROR: this is the root, but the parent is not null.");
                ok = false;
            }
        } else {
            /*
             * Note: there is a difference between having a parent reference and
             * having the parent be reachable. However, we actually want to
             * maintain both -- a parent MUST always be reachable.
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
            out.println(indent(height) + "Node: " + toString());
            out.println(indent(height) + "  parent="
                    + (parent == null ? null : parent.get()));
            out.println(indent(height) + "  dirty=" + isDirty() + ", nkeys="
                    + nkeys + ", nchildren=" + (nkeys + 1) + ", minKeys="
                    + minKeys + ", maxKeys=" + maxKeys + ", branchingFactor="
                    + branchingFactor+", #entries="+nentries);
            out.println(indent(height) + "  keys=" + keys);
        }
        // verify keys are monotonically increasing.
        try {
            assertKeysMonotonic();
        } catch (AssertionError ex) {
            out.println(indent(height) + "  ERROR: "+ex);
            ok = false;
        }
        if (debug) {
            out.println(indent(height) + "  childAddr="
                    + Arrays.toString(childAddr));
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
            out.println(indent(height) + "  childEntryCounts="
                    + Arrays.toString(childEntryCounts));
        }

        /*
         * Look for inconsistencies for children. A dirty child MUST NOT
         * have an entry in childAddr[] since it is not persistent and MUST
         * show up in dirtyChildren. Likewise if a child is NOT dirty, then
         * it MUST have an entry in childAddr and MUST NOT show up in
         * dirtyChildren.
         * 
         * This also verifies that all entries beyond nchildren (nkeys+1)
         * are unused.
         */
        for (int i = 0; i < branchingFactor+1; i++) {

            if (i > nkeys) {

                /*
                 * Scanning past the last valid child index. 
                 */

                if (childAddr[i] != NULL) {

                    out.println(indent(height) + "  ERROR childAddr[" + i
                            + "] should be " + NULL + ", not " + childAddr[i]);

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
//                        // some extra stuff used to track down a bug.
                        if(!ok) {
                        if( level == Level.DEBUG ) {
                            // dump the child also and exit.
                            System.err.println("child"); child.dump(Level.DEBUG,System.err);
                            throw new AssertionError();
                        } else {
                            // recursive call to get debug level dump.
                            System.err.println("this"); this.dump(Level.DEBUG,System.err);
                        }
                        }
                    }

                    if( childEntryCounts[i] != child.getEntryCount() ) {
                        out.println(indent(height) + "  ERROR child[" + i
                                + "] spans " + child.getEntryCount()
                                + " entries, but childEntryCount[" + i + "]="
                                + childEntryCounts[i]);
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
                        if (childAddr[i] != NULL) {
                            out.println(indent(height) + "  ERROR childAddr["
                                    + i + "]=" + childAddr[i]
                                    + ", but MUST be " + NULL
                                    + " since the child is dirty");
                            ok = false;
                        }
//                        if (!dirtyChildren.contains(child)) {
//                            out
//                                    .println(indent(height + 1)
//                                            + "  ERROR child at index="
//                                            + i
//                                            + " is dirty, but not on the dirty list: child="
//                                            + child);
//                            ok = false;
//                        }
                    } else {
                        /*
                         * Clean child (ie, persistent).  The parent of a clean
                         * child may be either clear or dirty.
                         */
                        if (childAddr[i] == NULL) {
                            out.println(indent(height) + "  ERROR childKey["
                                    + i + "] is " + NULL
                                    + ", but child is not dirty");
                            ok = false;
                        }
//                        if (dirtyChildren.contains(child)) {
//                            out
//                                    .println(indent(height)
//                                            + "  ERROR child at index="
//                                            + i
//                                            + " is not dirty, but is on the dirty list: child="
//                                            + child);
//                            ok = false;
//                        }
                    }

                }

            }

        }

        if( ! ok && ! debug ) {
            
            // @todo show the node structure with the errors since we would not
            // have seen it otherwise.
            
        }
        
        if (recursive) {

            /*
             * Dump children using pre-order traversal.
             */

            Set<AbstractNode> dirty = new HashSet<AbstractNode>();

            for (int i = 0; i <= /*nkeys*/branchingFactor; i++) {

                if (childRefs[i] == null && childAddr[i] == 0) {

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
//                AbstractNode child = getChild(i);
                AbstractNode child = childRefs[i]==null?null:childRefs[i].get();
                
                if( child != null ) {
                    
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

//                if (child.isDirty() && !dirtyChildren.contains(child)) {
//
//                    out
//                            .println(indent(height + 1)
//                                    + "ERROR dirty child not in node's dirty list at index="
//                                    + i);
//
//                    ok = false;
//
//                }
//
//                if (!child.isDirty() && dirtyChildren.contains(child)) {
//
//                    out
//                            .println(indent(height + 1)
//                                    + "ERROR clear child found in node's dirty list at index="
//                                    + i);
//
//                    ok = false;
//
//                }

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
                        final byte[] k0 = keys.getKey(0);
                        final byte[] ck0 = child.keys.getKey(0);
                        if( BytesUtil.compareBytes(ck0,k0) >= 0 ) {
//                          if( child.compare(0,keys,0) >= 0 ) {

                            out
                                    .println(indent(height + 1)
                                            + "ERROR first key on first child must be LT "
                                            + keyAsString(k0) + ", but found "
                                            + keyAsString(ck0));

                            ok = false;

                        }

                        if (child.nkeys >= 1 ) {
                            
                            final byte[] ckn = child.keys.getKey(child.nkeys-1);
                            if (BytesUtil.compareBytes(ckn, k0) >= 0) {
//                            if (child.compare(child.nkeys-1, keys, 0) >= 0) {

                            out.println(indent(height + 1)
                                            + "ERROR last key on first child must be LT "
                                            + keyAsString(k0) + ", but found "
                                            + keyAsString(ckn));

                            ok = false;
                            
                            }

                        }
                        
                    }
                    
                } else if (i < nkeys) {

// Note: The delete rule does not preserve this characteristic since we do not
// update the separatorKey for a leaf when removing its left most key.
//
//                    if (child.isLeaf() && keys[i - 1] != child.keys[0]) {
//
//                        /*
//                         * While each key in a node always is the first key of
//                         * some leaf, we are only testing the direct children
//                         * here. Therefore if the children are not leaves then
//                         * we can not cross check their first key with the keys
//                         * on this node.
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

            }

//            if (dirty.size() != dirtyChildren.size()) {
//
//                out.println(indent(height + 1) + "ERROR found " + dirty.size()
//                        + " dirty children, but " + dirtyChildren.size()
//                        + " in node's dirty list");
//
//                ok = false;
//
//            }

        }

        return ok;

    }

}
