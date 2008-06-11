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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.WeakHashMap;

import org.apache.log4j.Level;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.SingleValueIterator;

/**
 * <p>
 * A leaf.
 * </p>
 * <p>
 * Note: Leaves are NOT chained together for the object index since that forms
 * cycles that make it impossible to set the persistent identity for both the
 * prior and next fields of a leaf.
 * </p>
 * 
 * FIXME write detailed unit tests when version metadata is being maintained
 * since the behavior of the leaf is quite different. In particular, the
 * behavior of remove() needs to be examined when deletion markers are and are
 * not supported.
 * 
 * FIXME modify to copy the keys and values by value (not reference) and to use
 * a compact (and compacting) record in order minimize both de-serialization
 * costs and GC costs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Leaf extends AbstractNode implements ILeafData {

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
    protected byte[][] values;
    
    /**
     * The deletion markers IFF isolation is supported by the {@link BTree}.
     */
    protected boolean[] deleteMarkers;
    
    /**
     * The version timestamps IFF isolation is supported by the {@link BTree}.
     */
    protected long[] versionTimestamps;
    
    public final long getVersionTimestamp(int index) {

        if (versionTimestamps == null)
            throw new UnsupportedOperationException();

        return versionTimestamps[index];

    }
    
    public final boolean getDeleteMarker(int index) {

        if (deleteMarkers == null)
            throw new UnsupportedOperationException();

        return deleteMarkers[index];

    }
    
    public byte[][] getValues() {
        
        return values;
        
    }
    
    /**
     * De-serialization constructor.
     * <p>
     * Note: The de-serialization constructor (and ONLY the de-serialization
     * constructor) ALWAYS creates a clean leaf. Therefore the {@link PO#dirty}
     * flag passed up from this constructor has the value <code>false</code>.
     * 
     * @param btree
     *            The tree to which the leaf belongs.
     * @param addr
     *            The address of this leaf.
     * @param branchingFactor
     *            The branching factor for the leaf.
     * @param keys
     *            The defined keys. The allowable #of keys for a mutable leaf
     *            MUST be dimensioned to <code>branchingFactor+1</code> to
     *            allow room for the insert key that forces a split.
     * @param values
     *            The values (the array reference is copied, NOT the array
     *            values). The values array MUST be dimensions to
     *            <code>branchingFactor+1</code> to allow room for the value
     *            corresponding to the insert key that forces a split.
     */
    protected Leaf(AbstractBTree btree, long addr, int branchingFactor, 
            IKeyBuffer keys, byte[][] values, long[] versionTimestamps,
            boolean[] deleteMarkers) {
        
        super(btree, branchingFactor, false /* The leaf is NOT dirty. */);

        assert nkeys >=0 && nkeys<= branchingFactor;
        
        assert values != null;

        assert values.length == branchingFactor+1;
        
        setIdentity(addr);

        this.nkeys = keys.getKeyCount();
        
        this.keys = keys; // steal reference.
        
        this.values = values; // steal reference.

        this.versionTimestamps = versionTimestamps;
        
        this.deleteMarkers = deleteMarkers;
        
//        // must clear the dirty since we just de-serialized this leaf.
//        setDirty(false);

//        // Add to the hard reference queue.
//        btree.touch(this);
        
    }

    /**
     * Creates a new leaf, increments the #of leaves that belong to the btree,
     * and adds the new leaf to the hard reference queue for the btree.
     * 
     * @param btree A mutable btree.
     */
    protected Leaf(BTree btree) {

        super(btree, btree.branchingFactor, true /*dirty*/ );

        keys = new MutableKeyBuffer( branchingFactor+1 );

        values = new byte[branchingFactor+1][];
        
        if(btree.getIndexMetadata().isIsolatable()) {
            
            versionTimestamps = new long[branchingFactor+1];

        }
        
        if(btree.getIndexMetadata().getDeleteMarkers()) {

            deleteMarkers = new boolean[branchingFactor+1];
            
        }
        
//        /*
//         * Add to the hard reference queue. If the queue is full, then this will
//         * force the incremental write whatever gets evicted from the queue.
//         */
//        btree.touch(this);
        
    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node (must be immutable).
     * 
     * @see AbstractNode#copyOnWrite()
     */
    protected Leaf(Leaf src) {

        super(src);

        assert !src.isDirty();
        assert src.isPersistent();
        
        /*
         * Steal the values[].
         */

        // steal reference and clear reference on the source node.
        values = src.values;

        versionTimestamps = src.versionTimestamps;
        
        deleteMarkers = src.deleteMarkers;
        
//        // Add to the hard reference queue.
//        btree.touch(this);

    }

    public void delete() {

        /*
         * Note: This event MUST go out before we clear [leafListners].
         * 
         * Note: Since we fire this event here we do NOT need to fire it
         * explicitly after a copy-on-write since copy-on-write ALWAYS calls
         * delete() on the original leaf if it makes a copy.
         */

        fireInvalidateLeafEvent();

        super.delete();

        // clear references.
        
        values = null;
        
        versionTimestamps = null;
        
        deleteMarkers = null;
        
        leafListeners = null;
        
    }
    
    /**
     * Always returns <code>true</code>.
     */
    final public boolean isLeaf() {

        return true;

    }

    /**
     * For a leaf the #of entries is always the #of keys.
     */
    final public int getEntryCount() {
        
        return nkeys;
        
    }
    
    final public int getValueCount() {
        
        return nkeys;
        
    }
    
    final public boolean isNull(int index) {
        
        return values[index] == null;
        
    }
    
    final public void copyValue(int index, OutputStream os) {
        
        if(deleteMarkers!=null && deleteMarkers[index]) {
            
            // deleted.
            
            throw new UnsupportedOperationException();
            
        }

        final byte[] val = values[index];
        
        if (val == null) {

            // no value (null).
            
            throw new UnsupportedOperationException();
            
        }

        try {
         
            // copy the value.
            
            os.write( val );
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    final public boolean hasDeleteMarkers() {
        
        return deleteMarkers != null; 
        
    }
    
    final public boolean hasVersionTimestamps() {
        
        return versionTimestamps != null; 
        
    }
    
    /**
     * Insert or update an entry in the leaf as appropriate. The caller MUST
     * ensure by appropriate navigation of parent nodes that the key for the
     * next tuple either exists in or belongs in this leaf. If the leaf
     * overflows then it is split after the insert.
     */
    public Tuple insert(byte[] searchKey, byte[] newval, boolean delete, long timestamp, Tuple tuple) {

        if (delete && deleteMarkers == null) {
            
            /*
             * You may not specify the delete flag unless delete markers are
             * being maintained.
             */
            
            throw new UnsupportedOperationException();
            
        }
        
        if(btree.debug) assertInvariants();
        
        btree.touch(this);

        /*
         * Note: This is one of the few gateways for mutation of a leaf via the
         * main btree API (insert, lookup, delete). By ensuring that we have a
         * mutable leaf here, we can assert that the leaf must be mutable in
         * other methods.
         */
        final Leaf copy = (Leaf) copyOnWrite();

        if (copy != this) {

            /*
             * This leaf has been copied so delegate the operation to the new
             * leaf.
             * 
             * Note: copy-on-write deletes [this] leaf and delete() notifies any
             * leaf listeners before it clears the [leafListeners] reference so
             * not only don't we have to do that here, but we can't since the
             * listeners would be cleared before we could fire off the event
             * ourselves.
             */
            
            return copy.insert(searchKey, newval, delete, timestamp, tuple);
            
        }

        /*
         * Search for the key.
         * 
         * Note: We do NOT search before triggering copy-on-write for an object
         * index since an insert/update always triggers a mutation.
         */

        // look for the search key in the leaf.
        int entryIndex = this.keys.search(searchKey);

        if (entryIndex >= 0) {

            /*
             * The key is already present in the leaf, so we are updating an
             * existing entry.
             */
            
            if (tuple != null) {

                /*
                 * Copy data and metadata for the old value stored under the
                 * search key.
                 */

                tuple.copy(entryIndex, this);
                
            }
            
            // update the entry on the leaf.
            this.values[entryIndex] = newval;

            if (deleteMarkers != null) {

                this.deleteMarkers[entryIndex] = delete;

            }

            if (versionTimestamps != null) {

                this.versionTimestamps[entryIndex] = timestamp;

            }
            
            // notify any listeners that this tuple's state has been changed.
            fireInvalidateTuple(entryIndex);

            // return the old value.
            return tuple;

        }

        /*
         * The insert goes into this leaf.
         */
        
        // Convert the position to obtain the insertion point.
        entryIndex = -entryIndex - 1;

        // insert an entry under that key.
        {

            if (entryIndex < nkeys) {

                /* index = 2;
                 * nkeys = 6;
                 * 
                 * [ 0 1 2 3 4 5 ]
                 *       ^ index
                 * 
                 * count = keys - index = 4;
                 */
                final int count = nkeys - entryIndex;
                
                assert count >= 1;

                copyDown(entryIndex, count);

            }

            // Insert at index.
            MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;
//            copyKey(entryIndex, searchKeys, tupleIndex);
            keys.keys[entryIndex] = searchKey; // note: presumes caller does not reuse the searchKeys!
            this.values[entryIndex] = newval;
            if (deleteMarkers != null) {
                deleteMarkers[entryIndex] = delete;
            }
            if (versionTimestamps != null) {
                versionTimestamps[entryIndex] = timestamp;
            }

            nkeys++; keys.nkeys++;

        }

        // one more entry in the btree.
        ((BTree)btree).nentries++;

        if( parent != null ) {
            
            parent.get().updateEntryCount(this,1);
            
        }

//        if (INFO) {
//            log.info("this="+this+", key="+key+", value="+entry);
//            if(DEBUG) {
//                System.err.println("this"); dump(Level.DEBUG,System.err);
//            }
//        }

        if (nkeys == maxKeys+1) {

            /*
             * The insert caused the leaf to overflow, so now we split the leaf.
             */

            Leaf rightSibling = (Leaf) split();

            // assert additional invarients post-split.
            if(btree.debug) {
                rightSibling.assertInvariants();
                getParent().assertInvariants();
            }

        }

        // assert invarients post-split.
        if(btree.debug) assertInvariants();
        
        /*
         * Notify any listeners that the tuples found in the leaf have been
         * changed (one was added but others will have been moved into a new
         * right sibling if the leaf was split).
         */
        fireInvalidateLeafEvent();

        // return null since there was no pre-existing entry.
        return null;
        
    }

    public Tuple lookup(byte[] searchKey, Tuple tuple) {

        btree.touch(this);

        final int entryIndex = keys.search(searchKey);

        if (entryIndex < 0) {

            // Not found.

            return null;

        }

        // Found.
        
        tuple.copy(entryIndex, this);
        
        return tuple;
        
    }

    public int indexOf(byte[] key) {

        btree.touch(this);
        
        return keys.search(key);

    }
    
    public byte[] keyAt(int entryIndex) {
        
        if (entryIndex < 0)
            throw new IndexOutOfBoundsException("negative: "+entryIndex);

        if (entryIndex >= nkeys)
            throw new IndexOutOfBoundsException("too large: "+entryIndex);
        
        return keys.getKey(entryIndex);
        
    }

    public void valueAt(int entryIndex, Tuple tuple) {
        
        if (entryIndex < 0)
            throw new IndexOutOfBoundsException("negative: "+entryIndex);

        if (entryIndex >= nkeys)
            throw new IndexOutOfBoundsException("too large: "+entryIndex);
        
        tuple.copy(entryIndex, this);
        
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
    protected IAbstractNode split() {

        // MUST be mutable.
        assert isDirty();
        // MUST be an overflow.
        assert nkeys == maxKeys+1;

        final BTree btree = (BTree)this.btree;
        
        btree.counters.leavesSplit++;

        // #of entries in the leaf before it is split.
        final int nentriesBeforeSplit = nkeys;
        
        /*
         * The splitIndex is the index of the first key/value to move to the new
         * rightSibling.
         */
        final int splitIndex = (maxKeys+1)/2;

        /*
         * The separatorKey is the shortest key that is less than or equal to
         * the key at the splitIndex and greater than the key at [splitIndex-1].
         * This also serves as the separator key when we insert( separatorKey,
         * rightSibling ) into the parent.
         */
        final byte[] separatorKey = BytesUtil.getSeparatorKey(//
                keys.getKey(splitIndex),//
                keys.getKey(splitIndex - 1)//
                );
//        final Object separatorKey = ArrayType.alloc(btree.keyType, 1, stride);
//        System.arraycopy(keys,splitIndex*stride,separatorKey,0,stride);
//        final Object separatorKey = getKey(splitIndex);
        
        // the new rightSibling of this leaf.
        final Leaf rightSibling = new Leaf(btree);

        /*
         * Tunnel through to the mutable keys object.
         */
        final MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;
        final MutableKeyBuffer skeys = (MutableKeyBuffer)rightSibling.keys;

        // increment #of leaves in the tree.
        btree.nleaves++;

        if (INFO) {
            log.info("this=" + this + ", nkeys=" + nkeys + ", splitIndex="
                    + splitIndex + ", separatorKey="
                    + keyAsString(separatorKey)
                    );
            if(DEBUG) dump(Level.DEBUG,System.err);
        }

        int j = 0;
        for (int i = splitIndex; i <= maxKeys; i++, j++) {

            // copy key and value to the new leaf.
//            rightSibling.setKey(j, getKey(i));
            rightSibling.copyKey(j,this.keys,i);
            
            rightSibling.values[j] = values[i];

            if (deleteMarkers != null) {
                rightSibling.deleteMarkers[j] = deleteMarkers[i];
            }

            if (versionTimestamps != null) {
                rightSibling.versionTimestamps[j] = versionTimestamps[i];
            }
            
            // clear out the old keys and values.
            keys.zeroKey(i);
            values[i] = null;
            if(deleteMarkers!=null) deleteMarkers[i] = false;
            if(versionTimestamps!=null) versionTimestamps[i] = 0L;

            nkeys--; keys.nkeys--; // one less key here.
            rightSibling.nkeys++; skeys.nkeys++; // more more key there.

        }

        Node p = getParent();

        if (p == null) {

            /*
             * Use a special constructor to split the root leaf. The result is a
             * new node with zero keys and one child (this leaf).  The #of entries
             * spanned by the new root node is the same as the #of entries found
             * on this leaf _before_ the split.
             */

            p = new Node((BTree)btree, this, nentriesBeforeSplit);

        } else {
            
            // this leaf now has fewer entries
            p.childEntryCounts[p.getIndexOf(this)] -= rightSibling.nkeys;
            
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
     * separator key in the parent for the right most of (this, sibling). While
     * the #of entries spanned by the children of the common parent is changed
     * by this method note that there is no net change in the #of entries
     * spanned by that parent node.
     * 
     * @param sibling
     *            A direct sibling of this leaf (either the left or right
     *            sibling). The sibling MUST be mutable.
     * 
     * FIXME modify to always choose the shortest separator key.
     */
    protected void redistributeKeys(AbstractNode sibling,boolean isRightSibling) {

        // the sibling of a leaf must be a leaf.
        final Leaf s = (Leaf) sibling;
        
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
             * redistributeKeys(this,rightSibling). all we have to do is move
             * the first key from the rightSibling to the end of the keys in
             * this leaf. we then close up the hole that this left at index 0 in
             * the rightSibling. finally, we update the separator key for the
             * rightSibling to the new key in its first index position.
             */

            // copy the first key from the rightSibling.
//            setKey(nkeys, s.getKey(0));
            copyKey(nkeys,s.keys,0);
            values[nkeys] = s.values[0];
            if(deleteMarkers!=null) deleteMarkers[nkeys] = s.deleteMarkers[0];
            if(versionTimestamps!=null) versionTimestamps[nkeys] = s.versionTimestamps[0];
            
            // copy down the keys on the right sibling to cover up the hole.
            System.arraycopy(skeys.keys, 1, skeys.keys, 0, s.nkeys-1);
            System.arraycopy(s.values, 1, s.values, 0, s.nkeys-1);
            if(deleteMarkers!=null)
                System.arraycopy(s.deleteMarkers, 1, s.deleteMarkers, 0, s.nkeys-1);
            if(versionTimestamps!=null)
                System.arraycopy(s.versionTimestamps, 1, s.versionTimestamps, 0, s.nkeys-1);

            // erase exposed key/value on rightSibling that is no longer defined.
            skeys.zeroKey(s.nkeys-1);
            s.values[s.nkeys-1] = null;
            if(deleteMarkers!=null) s.deleteMarkers[s.nkeys-1] = false;
            if(versionTimestamps!=null) s.versionTimestamps[s.nkeys-1] = 0L;

            s.nkeys--; skeys.nkeys--;
            this.nkeys++; keys.nkeys++;
            
            // update the separator key for the rightSibling.
//            p.setKey(index, s.getKey(0));
            p.copyKey(index,s.keys,0);

            // update parent : one more key on this child.
            p.childEntryCounts[index]++;
            // update parent : one less key on our right sibling.
            p.childEntryCounts[index+1]--;

            if (btree.debug) {
                assertInvariants();
                s.assertInvariants();
            }

        } else {
            
            /*
             * redistributeKeys(leftSibling,this). all we have to do is copy
             * down the keys in this leaf by one position and move the last key
             * from the leftSibling into the first position in this leaf. We
             * then replace the separation key for this leaf on the parent with
             * the key that we copied from the leftSibling.
             */
            
            // copy down by one.
            System.arraycopy(keys.keys, 0, keys.keys, 1, nkeys);
            System.arraycopy(values, 0, values, 1, nkeys);
            if(deleteMarkers!=null)
                System.arraycopy(deleteMarkers, 0, deleteMarkers, 1, nkeys);
            if(versionTimestamps!=null)
                System.arraycopy(versionTimestamps, 0, versionTimestamps, 1, nkeys);
            
            // move the last key/value from the leftSibling to this leaf (copy, then clear).
            // copy.
//            setKey(0, s.getKey(s.nkeys-1));
            copyKey(0,s.keys,s.nkeys-1);
            values[0] = s.values[s.nkeys-1];
            if(deleteMarkers!=null) deleteMarkers[0] = s.deleteMarkers[s.nkeys-1];
            if(versionTimestamps!=null) versionTimestamps[0] = s.versionTimestamps[s.nkeys-1];
            // clear
            skeys.zeroKey(s.nkeys-1);
            s.values[s.nkeys-1] = null;
            if(deleteMarkers!=null) s.deleteMarkers[s.nkeys-1] = false;
            if(versionTimestamps!=null) s.versionTimestamps[s.nkeys-1] = 0L;
            s.nkeys--; skeys.nkeys--;
            this.nkeys++; keys.nkeys++;
            
            // update the separator key for this leaf.
//            p.setKey(index-1,getKey(0));
            p.copyKey(index-1, this.keys, 0);

            // update parent : one more key on this child.
            p.childEntryCounts[index]++;
            // update parent : one less key on our left sibling.
            p.childEntryCounts[index-1]--;

            if (btree.debug) {
                assertInvariants();
                s.assertInvariants();
            }

        }

    }

    /**
     * Merge the keys and values from the sibling into this leaf, delete the
     * sibling from the store and remove the sibling from the parent. This will
     * trigger recursive {@link AbstractNode#join()} if the parent node is now
     * deficient. While this changes the #of entries spanned by the current node
     * it does NOT effect the #of entries spanned by the parent.
     * 
     * @param sibling
     *            A direct sibling of this leaf (does NOT need to be mutable).
     *            The sibling MUST have exactly the minimum #of keys.
     */
    protected void merge(AbstractNode sibling, boolean isRightSibling) {
        
        // the sibling of a leaf must be a leaf.
        final Leaf s = (Leaf)sibling;
        
        assert s != null;
        assert !s.deleted;
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
         * The index of this leaf in its parent. we note this before we
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
        final MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;
        final MutableKeyBuffer skeys = (s.keys instanceof MutableKeyBuffer ? (MutableKeyBuffer) s.keys
                : ((ImmutableKeyBuffer) s.keys)
                        .toMutableKeyBuffer());

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
             * Copy in the keys and values from the sibling.
             */
            
            System.arraycopy(skeys.keys, 0, keys.keys, nkeys, s.nkeys);
            
            System.arraycopy(s.values, 0, this.values, nkeys, s.nkeys);

            if(deleteMarkers!=null) {
                System.arraycopy(s.deleteMarkers, 0, this.deleteMarkers, nkeys, s.nkeys);
            }

            if(versionTimestamps!=null) {
                System.arraycopy(s.versionTimestamps, 0, this.versionTimestamps, nkeys, s.nkeys);
            }
            
            /* 
             * Adjust the #of keys in this leaf.
             */
            this.nkeys += s.nkeys; keys.nkeys += s.nkeys;

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
            p.copyKey(index, p.keys, index+1 );

            // reallocate spanned entries from the sibling to this node.
            p.childEntryCounts[index] += s.getEntryCount();
            
            if(btree.debug) assertInvariants();
            
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
            System.arraycopy(keys.keys, 0, keys.keys, s.nkeys, this.nkeys);
            System.arraycopy(this.values, 0, this.values, s.nkeys, this.nkeys);
            if(deleteMarkers!=null) {
                System.arraycopy(this.deleteMarkers, 0, this.deleteMarkers, s.nkeys, this.nkeys);
            }
            if(versionTimestamps!=null) {
                System.arraycopy(this.versionTimestamps, 0, this.versionTimestamps, s.nkeys, this.nkeys);
            }
            
            // copy keys and values from the sibling to index 0 of this leaf.
            System.arraycopy(skeys.keys, 0, keys.keys, 0, s.nkeys);
            System.arraycopy(s.values, 0, this.values, 0, s.nkeys);
            if(deleteMarkers!=null) {
                System.arraycopy(s.deleteMarkers, 0, this.deleteMarkers, 0, s.nkeys);
            }
            if(versionTimestamps!=null) {
                System.arraycopy(s.versionTimestamps, 0, this.versionTimestamps, 0, s.nkeys);
            }
            
            this.nkeys += s.nkeys; keys.nkeys += s.nkeys;

            // reallocate spanned entries from the sibling to this node.
            p.childEntryCounts[index] += s.getEntryCount();

            if(btree.debug) assertInvariants();
            
        }
        
        /*
         * The sibling leaf is now empty. We need to detach the leaf from its
         * parent node and then delete the leaf from the store.
         */
        p.removeChild(s);
        
    }

    /**
     * Copies all keys and values from the specified start index down by one in
     * order to make room to insert a key and value at that index.
     * 
     * @param entryIndex
     *            The index of the first key and value to be copied.
     */
    protected void copyDown(int entryIndex, int count) {

        /*
         * copy down per-key data (#values == nkeys).
         */

        // Tunnel through to the mutable keys object.
        final MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;

        System.arraycopy(keys.keys, entryIndex, keys.keys, entryIndex + 1,
                count);

        System.arraycopy(values, entryIndex, values, entryIndex + 1, count);

        if (deleteMarkers != null) {

            System.arraycopy(deleteMarkers, entryIndex, deleteMarkers,
                    entryIndex + 1, count);

        }

        if (versionTimestamps != null) {

            System.arraycopy(versionTimestamps, entryIndex, versionTimestamps,
                    entryIndex + 1, count);
        }
        
        /*
         * Clear the entry at the index. This is partly a paranoia check and
         * partly critical. Some per-key elements MUST be cleared and it is much
         * safer (and quite cheap) to clear them during copyDown() rather than
         * relying on maintenance elsewhere.
         */

        keys.zeroKey(entryIndex);

        values[entryIndex] = null;

        if(deleteMarkers!=null) {
        
            deleteMarkers[entryIndex] = false;
        
        }
        
        if(versionTimestamps!=null) {
            
            versionTimestamps[entryIndex] = 0L;
            
        }
        
    }

    public Tuple remove(byte[] key, Tuple tuple) {
        
        if(btree.debug) assertInvariants();
        
        btree.touch(this);

        final int entryIndex = keys.search(key);

        if (entryIndex < 0) {

            // Not found.

            return null;

        }

        /*
         * Note: This is one of the few gateways for mutation of a leaf via
         * the main btree API (insert, lookup, delete). By ensuring that we
         * have a mutable leaf here, we can assert that the leaf must be
         * mutable in other methods.
         */

        final Leaf copy = (Leaf) copyOnWrite();

        if (copy != this) {

            /*
             * Note: This leaf was copied so delegate to the new leaf (the old
             * leaf is now unused).
             * 
             * Note: copy-on-write deletes [this] leaf and delete() notifies any
             * leaf listeners before it clears the [leafListeners] reference so
             * not only don't we have to do that here, but we can't since the
             * listeners would be cleared before we could fire off the event
             * ourselves.
             */
            return copy.remove(key, tuple);
            
        }

//        // The value that is being removed.
//        final Object oldval = this.values[entryIndex];

        if (tuple != null) {
            
            /*
             * Copy data and metadata for the index entry that is being removed.
             */
        
            tuple.copy(entryIndex, this);
            
        }

        if (deleteMarkers != null) {

            /*
             * This operation is not allowed when delete markers are being
             * maintained. You use an insert(...) instead and specify delete :=
             * true.
             */
            
            throw new UnsupportedOperationException();
            
        }
        
// if (INFO) {
// log.info("this="+this+", key="+key+", value="+entry+", index="+entryIndex);
// if(DEBUG) {
//                System.err.println("this"); dump(Level.DEBUG,System.err);
//            }
//        }
        
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
        final int length = nkeys - entryIndex - 1;

        // Tunnel through to the mutable keys object.
        final MutableKeyBuffer keys = (MutableKeyBuffer)this.keys;

        if (length > 0) {

            System.arraycopy(keys.keys, entryIndex + 1, keys.keys, entryIndex,
                    length);

            System.arraycopy(this.values, entryIndex + 1, this.values,
                    entryIndex, length);

            if (versionTimestamps != null) {

                System.arraycopy(this.versionTimestamps, entryIndex + 1, this.versionTimestamps,
                    entryIndex, length);
                
            }

        }

        /* 
         * Erase the key/value that was exposed by this operation.
         */
        keys.zeroKey(nkeys - 1);
        this.values[nkeys - 1] = null;
        if(versionTimestamps!=null) {
            this.versionTimestamps[nkeys - 1] = 0L;
        }

        // One less entry in the tree.
        ((BTree)btree).nentries--;
        assert ((BTree)btree).nentries >= 0;

        // One less key in the leaf.
        nkeys--; keys.nkeys--;
                
        if( btree.root != this ) {

            /*
             * this is not the root leaf.
             */
        
            // update entry count on ancestors.
            
            parent.get().updateEntryCount(this,-1);
            
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
                 * Note that we must invoke copy-on-write before modifying a
                 * sibling.  However, the parent of the leaf MUST already be
                 * mutable (aka dirty) since that is a precondition for removing
                 * a key from the leaf.  This means that copy-on-write will not
                 * force the parent to be cloned.
                 */
                
                join();
                
            }
            
        }
            
        if(btree.debug) assertInvariants();
        
        /*
         * Notify any listeners that the tuple(s) in the leaf have been changed.
         */
        fireInvalidateLeafEvent();
        
        return tuple;
        
    }

    @SuppressWarnings("unchecked")
    public Iterator<AbstractNode> postOrderNodeIterator(final boolean dirtyNodesOnly) {

        if (dirtyNodesOnly && ! isDirty() ) {

            return EmptyIterator.DEFAULT;

        } else {

            return new SingleValueIterator(this);

        }

    }

    @SuppressWarnings("unchecked")
    public Iterator<AbstractNode> postOrderIterator(byte[] fromKey, byte[] toKey) {

        return new SingleValueIterator(this);

    }

    /**
     * Iterator visits the values in key order.
     */
    public ITupleIterator entryIterator() {

        if (nkeys == 0) {

            return EmptyEntryIterator.INSTANCE;

        }

        return new TupleIterator(this);

    }

    public boolean dump(Level level, PrintStream out, int height, boolean recursive) {

        boolean debug = level.toInt() <= Level.DEBUG.toInt();
        
        // Set to false iff an inconsistency is detected.
        boolean ok = true;

        if ((btree.getRoot() != this) && (nkeys < minKeys)) {
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

        if (height != -1 && height != btree.getHeight()) {

            out.println(indent(height) + "WARN: height=" + height
                    + ", but btree height=" + btree.getHeight());
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
            
            out.println(indent(height) + "  isRoot=" + (btree.getRoot() == this)
                    + ", dirty=" + isDirty() + ", nkeys=" + nkeys
                    + ", minKeys=" + minKeys + ", maxKeys=" + maxKeys
                    + ", branchingFactor=" + branchingFactor);
            
            // Note: key format is dumped by its object.
            out.println(indent(height) + "  keys=" + keys);
        
            // Note: signed byte[]s.
            out.println(indent(height) + "  vals=" + toString(values));
            
            if(deleteMarkers!=null) {
                
                out.println(indent(height) + "  deleted=" + Arrays.toString(deleteMarkers));
                
            }
            
            if(versionTimestamps!=null) {
                
                out.println(indent(height) + "  timestamps=" + Arrays.toString(versionTimestamps));
                
            }
            
        }

        return ok;

    }

    /**
     * Formats the data into a {@link String}.
     * 
     * @param data
     *            An array of <em>signed</em> byte arrays.
     */
    static private String toString(byte[][] data) {
       
        StringBuilder sb = new StringBuilder();
        
        final int n = data.length;
        
        sb.append("data(n=" + n + ")={");

        for (int i = 0; i < n; i++) {

            final byte[] a = data[i];
            
            sb.append("\n");

            sb.append("data[" + i + "]=");

            sb.append(Arrays.toString(a));

            if (i + 1 < n)
                sb.append(",");
            
        }
        
        sb.append("}");
        
        return sb.toString();
        
    }

    /**
     * An interface that may be used to register for and receive events when the
     * state of a {@link Leaf} is changed (update of a tuple, insert of a tuple,
     * or removal of a tuple) or when the leaf is discarded in response by
     * copy-on-write.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface ILeafListener {
       
        /**
         * Notice that the leaf state has changed and that the listener must not
         * assume: (a) that a tuple of interest still resides within the leaf
         * (it may have been moved up or down within the leaf or it may be in
         * another leaf altogether as a result of underflow or overflow); (b)
         * that the leaf is still in use (it may have been discarded by a
         * copy-on-write operation).
         */
        public void invalidateLeaf();
        
        /**
         * Notice that the state of a tuple in the leaf has been changed (the
         * tuple is still known to be located within the leaf).
         * 
         * @param index
         *            The index of the tuple whose state was changed.
         */
        public void invalidateTuple(int index);
        
    }
    
    /**
     * Listeners for {@link ILeafListener} events.
     * <p>
     * Note: The values in the map are <code>null</code>.
     * <p>
     * Note: Listeners are cleared from the map automatically by the JVM soon
     * after the listener becomes only weakly reachable.
     * <p>
     * Note: Mutable {@link BTree}s are single-threaded so there is no need to
     * synchronize access to this collection.
     * <p>
     * Note: These listeners are primarily used to support {@link ITupleCursor}s.
     * The #of listeners at any one time is therefore directly related to the
     * #of open iterators on the owning <em>mutable</em> {@link BTree}.
     * Normally that is ONE (1) since the {@link BTree} is not thread-safe for
     * mutation and each cursor has a current, prior, and next position meaning
     * that we have typically either NO listeners or the current and either
     * prior or next listener. This tends to make visiting the members of the
     * collection (when it is defined) very fast, especially since we do not
     * need to synchronize on anything.
     * <p>
     * Note: The trigger conditions for the events of interest to the listeners
     * are scattered throughout the {@link Leaf} class.
     */
    private transient WeakHashMap<ILeafListener,Void> leafListeners = null;

    /**
     * Register an {@link ILeafListener} with this {@link Leaf}. Listeners are
     * automatically removed by the JVM shortly after they become only weakly
     * reachable.
     * 
     * @param l
     *            The listener.
     * 
     * @throws IllegalStateException
     *             if the owning {@link AbstractBTree} is read-only.
     */
    final public void addLeafListener(ILeafListener l) {

        if (l == null)
            throw new IllegalArgumentException();

        btree.assertNotReadOnly();

        if(leafListeners==null) {
            
            leafListeners = new WeakHashMap<ILeafListener, Void>();
            
        }
        
        leafListeners.put(l, null);
        
    }

    /**
     * Fire an {@link ILeafListener#invalidateLeaf()} event to any registered
     * listeners.
     */
    final protected void fireInvalidateLeafEvent() {
        
        if(leafListeners == null) return;

        for(ILeafListener l : leafListeners.keySet()) {
            
            l.invalidateLeaf();
            
        }
        
    }

    /**
     * Fire an {@link ILeafListener#invalidateTuple(int)} event to any
     * registered listeners.
     * 
     * @param index
     *            The index of the tuple whose state was changed.
     */
    final protected void fireInvalidateTuple(int index) {

        if(leafListeners == null) return;

        for(ILeafListener l : leafListeners.keySet()) {
            
            l.invalidateTuple(index);
            
        }

    }

}
