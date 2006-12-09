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
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.SingleValueIterator;

/**
 * <p>
 * A leaf.
 * </p>
 * <p>
 * Note: Leaves are NOT chained together for the object index since that
 * forms cycles that make it impossible to set the persistent identity for
 * both the prior and next fields of a leaf.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Leaf extends AbstractNode {

    /**
     * <p>
     * The values of the tree. There is one value per key for a leaf.
     * </p>
     * <p>
     * This array is dimensioned to one more than the maximum capacity so that
     * the value corresponding to the key that causes overflow and forces the
     * split may be inserted. This greatly simplifies the logic for computing
     * the split point and performing the split.
     * </p>
     */
    protected Object[] values;

    /**
     * De-serialization constructor.
     * 
     * @param btree
     *            The tree to which the leaf belongs.
     * @param id
     *            The persistent identity of this leaf (non-{@link IIdentityAccess#NULL}).
     * @param branchingFactor
     *            The branching factor for the leaf.
     * @param nkeys
     *            The #of valid elements in <i>keys</i> (also the #of valid
     *            elements in <i>values</i>).
     * @param keys
     *            The keys (the array reference is copied, NOT the array
     *            contents). The keys array MUST be dimensions to
     *            <code>branchingFactor+1</code> to allow room for the insert
     *            key that forces a split.
     * @param values
     *            The values (the array reference is copied, NOT the array
     *            values).The values array MUST be dimensions to
     *            <code>branchingFactor+1</code> to allow room for the value
     *            corresponding to the insert key that forces a split.
     */
    protected Leaf(BTree btree, long id, int branchingFactor, int nkeys,
            Object keys, Object[] values) {
        
        super(btree, branchingFactor );

        assert nkeys >=0 && nkeys<= branchingFactor;
        
        assertKeyTypeAndLength( btree, keys, branchingFactor+1 );
        
        assert values != null;

        assert values.length == branchingFactor+1;
        
        setIdentity(id);

        this.nkeys = nkeys;
        
        this.keys = keys;
        
        this.values = values;

        // must clear the dirty since we just de-serialized this leaf.
        setDirty(false);

        // Add to the hard reference queue.
        btree.touch(this);
        
    }

    /**
     * Creates a new leaf, increments the #of leaves that belong to the btree,
     * and adds the new leaf to the hard reference queue for the btree.
     * 
     * @param btree
     */
    protected Leaf(BTree btree) {

        super(btree, btree.getBrachingFactor());

        keys = new int[branchingFactor+1];

        values = new Object[branchingFactor+1];

        /*
         * Add to the hard reference queue. If the queue is full, then this will
         * force the incremental write whatever gets evicted from the queue.
         */
        btree.touch(this);
        
        btree.nleaves++;

    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node.
     * 
     * @see AbstractNode#copyOnWrite()
     */
    protected Leaf(Leaf src) {

        super(src);

        nkeys = src.nkeys;

        // nkeys == nvalues for a Leaf.
        keys = new int[branchingFactor+1];

        values = (Object[])new Object[branchingFactor+1];

        /*
         * Note: Unless and until we have a means to recover leafs from a cache,
         * we just steal the keys[] and values[] rather than making copies.
         */

        // steal keys.
        keys = src.keys; src.keys = null;
        
        // steal values.
        values = src.values; src.values = null;
        
//        // copy keys.
//        System.arraycopy(src.keys, 0, keys, 0, nkeys);
//
//        // copy values
//        btree.copyValues( src.values, this.values, nkeys );
        
        // Add to the hard reference queue.
        btree.touch(this);

    }

    /**
     * Always returns <code>true</code>.
     */
    final public boolean isLeaf() {

        return true;

    }

    /**
     * Inserts an entry under an external key. The caller MUST ensure by
     * appropriate navigation of parent nodes that the external key either
     * exists in or belongs in this node. If the leaf is full, then it is split
     * before the insert.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The new entry.
     * 
     * @return The previous value or <code>null</code> if the key was not
     *         found.
     * 
     * @todo add boolean replace parameter and return the existing value or null
     *       if there was no existing value. When replace is false and the key
     *       exists, return ??? to indicate that the value was not inserted?
     */
    public Object insert(Object key, Object entry) {

        assertInvariants();
        
        btree.touch(this);

        /*
         * Note: This is one of the few gateways for mutation of a leaf via the
         * main btree API (insert, lookup, delete). By ensuring that we have a
         * mutable leaf here, we can assert that the leaf must be mutable in
         * other methods.
         */
        Leaf copy = (Leaf) copyOnWrite();

        if (copy != this) {

            return copy.insert(key, entry);

        }

        /*
         * Search for the key.
         * 
         * Note: We do NOT search before triggering copy-on-write for an object
         * index since an insert always triggers a mutation.
         */
        int index = search(key);

        if (index >= 0) {

            /*
             * The key is already present in the leaf, so we are updating an
             * existing entry.
             */

            Object tmp = values[index];

            values[index] = entry;

            return tmp;

        }

        /*
         * The insert goes into this leaf.
         */
        
        // Convert the position to obtain the insertion point.
        index = -index - 1;

        // insert an entry under that key.
        insert(key, index, entry);

        // one more entry in the btree.
        btree.nentries++;

        if (INFO) {
            log.info("this="+this+", key="+key+", value="+entry);
            if(DEBUG) {
                System.err.println("this"); dump(Level.DEBUG,System.err);
            }
        }

        if (nkeys == maxKeys+1) {

            /*
             * The insert caused the leaf to overflow, so now we split the leaf.
             */

            Leaf rightSibling = (Leaf) split();

            // assert additional invarients post-split.
            rightSibling.assertInvariants();
            getParent().assertInvariants();

        }

        // assert invarients post-split.
        assertInvariants();

        // the key was not found.
        return null;

    }

    public Object lookup(Object key) {

        btree.touch(this);
        
        int index = search(key);

        if (index < 0) {

            // Not found.

            return null;

        }

        return values[index];

    }

    /**
     * <p>
     * Split an over capacity leaf (a leaf with maxKeys+1 keys), creating a new
     * rightSibling. The splitIndex (the index of the first key to move to the
     * rightSibling) is <code>(maxKeys+1)/2</code>. The key at the splitIndex
     * is also inserted as the new separatorKey into the parent. All keys and
     * values starting with splitIndex are moved to the new rightSibling. If
     * this leaf is the root of the tree (no parent), then a new root
     * {@link Node} is created without any keys and is made the parent of this
     * leaf. In any case, we then insert( separatorKey, rightSibling ) into the
     * parent node, which may cause the parent node itself to split.
     * </p>
     * 
     * @return The new rightSibling leaf.
     */
    protected AbstractNode split() {

        // MUST be mutable.
        assert isDirty();
        // MUST be an overflow.
        assert nkeys == maxKeys+1;

        btree.counters.leavesSplit++;

        /*
         * The splitIndex is the index of the first key/value to move to the new
         * rightSibling.
         */
        final int splitIndex = (maxKeys+1)/2;

        /*
         * The key at the splitIndex also serves as the separator key when we
         * insert( separatorKey, rightSibling ) into the parent.
         */
        final Object separatorKey = getKey(splitIndex);

        // the new rightSibling of this leaf.
        final Leaf rightSibling = new Leaf(btree);

        if (INFO) {
            log.info("this=" + this + ", nkeys=" + nkeys + ", splitIndex="
                    + splitIndex + ", separatorKey=" + separatorKey);
            if(DEBUG) dump(Level.DEBUG,System.err);
        }

        int j = 0;
        for (int i = splitIndex; i <= maxKeys; i++, j++) {

            // copy key and value to the new leaf.
//            rightSibling.setKey(j, getKey(i));
            rightSibling.copyKey(j,this,i);
            
            rightSibling.values[j] = values[i];

            // clear out the old keys and values.
            setKey(i, btree.NEGINF);
            values[i] = null;

            nkeys--; // one less key here.
            rightSibling.nkeys++; // more more key there.

        }

        Node p = getParent();

        if (p == null) {

            /*
             * Use a special constructor to split the root leaf. The result is a
             * new node with zero keys and one child (this leaf).
             */

            p = new Node(btree, this);

        }

        /* 
         * insert(splitKey,rightSibling) into the parent node.  This may cause
         * the parent node itself to split.
         */
        p.insertChild(separatorKey, rightSibling);

        // Return the high leaf.
        return rightSibling;

    }
    
    /**
     * Redistributes a key from the specified sibling into this leaf in order to
     * bring this leaf up to the minimum #of keys. This also updates the
     * separator key in the parent for the right most of (this, sibling).
     * 
     * @param sibling
     *            A direct sibling of this leaf (either the left or right
     *            sibling). The sibling MUST be mutable.
     */
    protected void redistributeKeys(AbstractNode sibling,boolean isRightSibling) {

        // the sibling of a leaf must be a leaf.
        final Leaf s = (Leaf) sibling;
        
        assert isDirty();
        assert !isDeleted();
        assert !isPersistent();
        // verify that this leaf is deficient.
        assert nkeys < minKeys;
        // verify that this leaf is under minimum capacity by one key.
        assert nkeys == minKeys - 1;
        
        assert s != null;
        // the sibling MUST be _OVER_ the minimum #of keys/values.
        assert s.nkeys > minKeys;
        assert s.isDirty();
        assert !s.isDeleted();
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
         * determine which leaf is earlier in the key ordering and get the
         * index of the sibling.
         */
        if( isRightSibling/*keys[nkeys-1] < s.keys[0]*/) {
        
            /*
             * redistributeKeys(this,rightSibling). all we have to do is move
             * the first key from the rightSibling to the end of the keys in
             * this leaf. we then close up the hole that this left at index 0 in
             * the rightSibling. finally, we update the separator key for the
             * rightSibling to the new key in its first index position.
             */

            // copy the first key from the rightSibling.
//            setKey(nkeys, s.getKey(0));
            copyKey(nkeys,s,0);
            values[nkeys] = s.values[0];
            
            // copy down the keys on the right sibling to cover up the hole.
            System.arraycopy(s.keys, 1, s.keys, 0, s.nkeys-1);
            System.arraycopy(s.values, 1, s.values, 0, s.nkeys-1);

            // erase exposed key/value on rightSibling that is no longer defined.
            s.setKey(s.nkeys-1, btree.NEGINF);
            s.values[s.nkeys-1] = null;

            // update the separator key for the rightSibling.
//            p.setKey(index, s.getKey(0));
            p.copyKey(index,s,0);

            s.nkeys--;
            this.nkeys++;
            
            assertInvariants();
            s.assertInvariants();

        } else {
            
            /*
             * redistributeKeys(leftSibling,this). all we have to do is copy
             * down the keys in this leaf by one position and move the last key
             * from the leftSibling into the first position in this leaf. We
             * then replace the separation key for this leaf on the parent with
             * the key that we copied from the leftSibling.
             */
            
            // copy down by one.
            System.arraycopy(keys, 0, keys, 1, nkeys);
            System.arraycopy(values, 0, values, 1, nkeys);
            
            // move the last key/value from the leftSibling to this leaf.
//            setKey(0, s.getKey(s.nkeys-1));
            copyKey(0,s,s.nkeys-1);
            values[0] = s.values[s.nkeys-1];
            s.setKey(s.nkeys-1, btree.NEGINF);
            s.values[s.nkeys-1] = null;
            s.nkeys--;
            this.nkeys++;
            
            // update the separator key for this leaf.
//            p.setKey(index-1,getKey(0));
            p.copyKey(index-1, this, 0);

            assertInvariants();
            s.assertInvariants();

        }

    }

    /**
     * Merge the keys and values from the sibling into this leaf, delete the
     * sibling from the store and remove the sibling from the parent. This will
     * trigger recursive {@link AbstractNode#join()} if the parent node is now
     * deficient.
     * 
     * @param sibling
     *            A direct sibling of this leaf (does NOT need to be mutable).
     *            The sibling MUST have exactly the minimum #of keys.
     */
    protected void merge(AbstractNode sibling, boolean isRightSibling) {
        
        // the sibling of a leaf must be a leaf.
        final Leaf s = (Leaf)sibling;
        
        assert s != null;
        assert !s.isDeleted();
        // verify that this leaf is deficient.
        assert nkeys < minKeys;
        // verify that this leaf is under minimum capacity by one key.
        assert nkeys == minKeys - 1;
        // the sibling MUST at the minimum #of keys/values.
        assert s.nkeys == s.minKeys;

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
         * determine which leaf is earlier in the key ordering so that we know
         * whether the sibling's keys will be inserted at the front of this
         * leaf's keys or appended to this leaf's keys.
         */
        if( isRightSibling /*keys[nkeys-1] < s.keys[0]*/) {
            
            /*
             * merge( this, rightSibling ). the keys and values from this leaf
             * will appear in their current position and the keys and values
             * from the rightSibling will be appended after the last key/value
             * in this leaf.
             */

            /*
             * The index of this leaf in its parent. we note this before we
             * start mucking with the keys.
             */
            final int index = p.getIndexOf(this);
            
            /*
             * Copy in the keys and values from the sibling.
             */
            System.arraycopy(s.keys, 0, this.keys, nkeys, s.nkeys);
            System.arraycopy(s.values, 0, this.values, nkeys, s.nkeys);
            
            /* 
             * Adjust the #of keys in this leaf.
             */
            this.nkeys += s.nkeys;

            /*
             * Note: in this case we have to replace the separator key for this
             * leaf with the separator key for its right sibling.
             * 
             * Note: This temporarily causes the duplication of a separator key
             * in the parent. However, the separator key for the right sibling
             * will be deleted when the sibling is removed from the parent
             * below.
             */
//            p.setKey(index, p.getKey(index+1));
            p.copyKey(index, p, index+1 );
            
            assertInvariants();
            
        } else {
            
            /*
             * merge( leftSibling, this ). the keys and values from this leaf
             * will be move down by sibling.nkeys positions and then the keys
             * and values from the sibling will be copied into this leaf
             * starting at index zero(0).
             * 
             * Note: we do not update the separator key in the parent because 
             * the separatorKey will be removed when we remove the leftSibling
             * from the parent at the end of this method.  This also has the
             * effect of giving this leaf its left sibling's separatorKey.
             */
            
            // move keys and values down by sibling.nkeys positions.
            System.arraycopy(this.keys, 0, this.keys, s.nkeys, this.nkeys);
            System.arraycopy(this.values, 0, this.values, s.nkeys, this.nkeys);
            
            // copy keys and values from the sibling to index 0 of this leaf.
            System.arraycopy(s.keys, 0, this.keys, 0, s.nkeys);
            System.arraycopy(s.values, 0, this.values, 0, s.nkeys);
            
            this.nkeys += s.nkeys;

            assertInvariants();
            
        }
        
        /*
         * The sibling leaf is now empty. We need to detach the leaf from its
         * parent node and then delete the leaf from the store.
         */
        p.removeChild(s);
        
    }
    
    /**
     * Inserts an entry under an external key. This method is invoked when a
     * {@link Search#search(int, int[], int)} has already revealed that
     * there is no entry for the key in the node.
     * 
     * @param key
     *            The external key.
     * @param index
     *            The index position for the new entry. Data already present
     *            in the leaf beyond this insertion point will be shifted
     *            down by one.
     * @param entry
     *            The new entry.
     */
    protected void insert(Object key, int index, Object entry) {

//        assert key != BTree.NEGINF;
        assert index >= 0 && index <= nkeys;
        assert entry != null;

        if (index < nkeys) {

            /* index = 2;
             * nkeys = 6;
             * 
             * [ 0 1 2 3 4 5 ]
             *       ^ index
             * 
             * count = keys - index = 4;
             */
            final int count = nkeys - index;
            
            assert count >= 1;

            copyDown(index, count);

        }

        /*
         * Insert at index.
         */
        setKey(index, key); // defined by AbstractNode
        values[index] = entry; // defined by Leaf.

        nkeys++;

    }

    /**
     * Copies all keys and values from the specified start index down by one in
     * order to make room to insert a key and value at that index.
     * 
     * @param index
     *            The index of the first key and value to be copied.
     */
    protected void copyDown(int index, int count) {

        /*
         * copy down per-key data (#values == nkeys).
         */
        System.arraycopy(keys, index, keys, index + 1, count);
        System.arraycopy(values, index, values, index + 1, count);

        /*
         * Clear the entry at the index. This is partly a paranoia check and
         * partly critical. Some per-key elements MUST be cleared and it is
         * much safer (and quite cheap) to clear them during copyDown()
         * rather than relying on maintenance elsewhere.
         */

        setKey(index, btree.NEGINF); // an invalid key.

        values[index] = null;

    }

    /**
     * If the key is found on this leaf, then ensures that the leaf is mutable
     * and removes the key and value.
     * 
     * @param key
     *            The external key.
     * 
     * @return The value stored under that key or null.
     */
    public Object remove(Object key) {

        assertInvariants();
        
        btree.touch(this);

        final int index = search(key);

        if (index < 0) {

            // Not found.

            return null;

        }

        /*
         * Note: This is one of the few gateways for mutation of a leaf via
         * the main btree API (insert, lookup, delete). By ensuring that we
         * have a mutable leaf here, we can assert that the leaf must be
         * mutable in other methods.
         */

        Leaf copy = (Leaf) copyOnWrite();

        if (copy != this) {

            return copy.remove(key);

        }

        // The value that is being removed.
        Object entry = values[index];

        if (INFO) {
            log.info("this="+this+", key="+key+", value="+entry+", index="+index);
            if(DEBUG) {
                System.err.println("this"); dump(Level.DEBUG,System.err);
            }
        }

        /*
         * Copy over the hole created when the key and value were removed
         * from the leaf.
         * 
         * Given: 
         * keys : [ 1 2 3 4 ]
         * vals : [ a b c d ]
         * 
         * Remove(1):
         * index := 0
         * length = nkeys(4) - index(0) - 1 = 3;
         * 
         * Remove(3):
         * index := 2;
         * length = nkeys(4) - index(2) - 1 = 1;
         * 
         * Remove(4):
         * index := 3
         * length = nkeys(4) - index(3) - 1 = 0;
         * 
         * Given: 
         * keys : [ 1      ]
         * vals : [ a      ]
         * 
         * Remove(1):
         * index := 0
         * length = nkeys(1) - index(0) - 1 = 0;
         */

        /*
         * Copy down to cover up the hole.
         */
        final int length = nkeys - index - 1;

        if (length > 0) {

            System.arraycopy(keys, index + 1, keys, index, length);

            System.arraycopy(values, index + 1, values, index, length);

        }

        /* 
         * Erase the key/value that was exposed by this operation.
         */
        setKey( nkeys-1, btree.NEGINF);
        values[ nkeys-1 ] = null;

        // One less entry in the tree.
        btree.nentries--;
        assert btree.nentries >= 0;

        // One less key in the leaf.
        nkeys--;
        
        if( btree.root != this ) {

            /*
             * this is not the root leaf.
             */
            
            if( nkeys < minKeys ) {
                
                /*
                 * The leaf is deficient. Join it with a sibling, causing their
                 * keys to be redistributed such that neither leaf is deficient.
                 * If there is only one other sibling and it has only the
                 * minimum #of values then the two siblings will be merged into
                 * a single leaf and their parent will have only a single child.
                 * Since the minimum #of children is two (2), having a single
                 * child makes the parent of this node deficient and it will be
                 * joined with one of its siblings. If necessary, this process
                 * will continue recursively up the tree. The root leaf never
                 * has any siblings and never experiences underflow so it may be
                 * legally reduced to zero values.
                 * 
                 * Note that the minmum branching factor (3) and the invariants
                 * together guarentee that there is at least one sibling. Also
                 * note that the minimum #of children for a node with the
                 * minimum branching factor is two (2) so a valid tree never has
                 * a node with a single sibling.
                 * 
                 * Note that we must invoked copy-on-write before modifying a
                 * sibling.  However, the parent of the leave MUST already be
                 * mutable (aka dirty) since that is a precondition for removing
                 * a key from the leaf.  This means that copy-on-write will not
                 * force the parent to be cloned.
                 */
                
                join();
                
            }
            
        }
            
        assertInvariants();
        
        return entry;

    }

    public Iterator postOrderIterator(final boolean dirtyNodesOnly) {

        if (dirtyNodesOnly && ! isDirty() ) {

            return EmptyIterator.DEFAULT;

        } else {

            return new SingleValueIterator(this);

        }

    }

    /**
     * Iterator visits the values in key order.
     * 
     * @todo define key range scan iterator. This would be implemented
     *       differently for a B+Tree since we could just scan leaves. For a
     *       B-Tree, we have to traverse the nodes (pre- or post-order?) and
     *       then visit only those keys that satisify the from/to
     *       constraint. We know when to stop in the leaf scan based on the
     *       keys, but we also have to check when to stop in the node scan
     *       or we will scan all nodes and not just those covering the key
     *       range.
     */
    public Iterator entryIterator() {

        if (nkeys == 0) {

            return EmptyIterator.DEFAULT;

        }

        return new EntryIterator(this);

    }

    public boolean dump(Level level, PrintStream out, int height, boolean recursive) {

        boolean debug = level.toInt() <= Level.DEBUG.toInt();
        
        // Set to false iff an inconsistency is detected.
        boolean ok = true;

        if ((btree.root != this) && (nkeys < minKeys)) {
            // min keys failure (the root may have fewer keys).
            out.println(indent(height) + "ERROR: too few keys: m="
                    + branchingFactor + ", minKeys=" + minKeys + ", nkeys="
                    + nkeys + ", isLeaf=" + isLeaf());
            ok = false;
        }

        if (nkeys > branchingFactor) {
            // max keys failure.
            out.println(indent(height) + "ERROR: too many keys: m="
                    + branchingFactor + ", maxKeys=" + maxKeys + ", nkeys="
                    + nkeys + ", isLeaf=" + isLeaf());
            ok = false;
        }

        if (height != -1 && height != btree.height) {

            out.println(indent(height) + "WARN: height=" + height
                    + ", but btree height=" + btree.height);
            ok = false;
            
        }

        // verify keys are monotonically increasing.
        try {
            assertKeysMonotonic();
        } catch (AssertionError ex) {
            out.println(indent(height) + "  ERROR: "+ex);
            ok = false;
        }
        
        if (debug || ! ok ) {
        
            out.println(indent(height) + "Leaf: " + toString());
            
            out.println(indent(height) + "  parent="
                    + (parent == null ? null : parent.get()));
            
            out.println(indent(height) + "  isRoot=" + (btree.root == this)
                    + ", dirty=" + isDirty() + ", nkeys=" + nkeys
                    + ", minKeys=" + minKeys + ", maxKeys=" + maxKeys
                    + ", branchingFactor=" + branchingFactor);
            
            out.println(indent(height) + "  keys=" + keysAsString(keys));
        
            out.println(indent(height) + "  vals=" + Arrays.toString(values));
            
        }

        return ok;

    }

}
