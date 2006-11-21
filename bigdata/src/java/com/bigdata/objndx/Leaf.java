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
     * The values of the tree.
     */
    protected Object[] values;

    /**
     * De-serialization constructor.
     */
    protected Leaf(BTree btree, long id, int branchingFactor, int nkeys, int[] keys, Object[] values) {
        
        super(btree, branchingFactor );

        assert nkeys >=0 && nkeys<= branchingFactor;
        
        assert keys != null;
        
        assert keys.length == branchingFactor;
        
        assert values != null;

        assert values.length == branchingFactor;
        
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
    public Leaf(BTree btree) {

        super(btree, btree.getBrachingFactor());

        // nkeys == nvalues for a Leaf.
        keys = new int[branchingFactor];

        values = new Object[branchingFactor];

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
        keys = new int[branchingFactor];

        values = (Object[])new Object[branchingFactor];

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
    public Object insert(int key, Object entry) {

        btree.touch(this);

        /*
         * Note: This is one of the few gateways for mutation of a leaf via
         * the main btree API (insert, lookup, delete). By ensuring that we
         * have a mutable leaf here, we can assert that the leaf must be
         * mutable in other methods.
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
        int index = Search.search(key, keys, nkeys);

        if (index >= 0) {

            /*
             * The key is already present in the leaf, so we are updating an
             * existing entry.  For this case we do NOT split the leaf.
             */

            Object tmp = values[index];

            values[index] = entry;

            return tmp;

        }

        if (nkeys == branchingFactor) {

            /*
             * Split this leaf into a low leaf (this leaf) and a high leaf
             * (returned by split()). If the key is greater than or equal to the
             * first key in the high leaf then the insert is directed into the
             * high leaf.  Otherwise it does into this leaf (the low leaf).
             */

            Leaf leaf2 = (Leaf) split();

            if (key >= leaf2.keys[0]) {

                // direct insert into the high leaf.
                return leaf2.insert(key, entry);

            } else {

                // direct insert into the low leaf.
                return this.insert(key, entry);

            }
           
        }

        /*
         * The insert goes into this leaf and the leaf is not at capacity.
         */
        
        // Convert the position to obtain the insertion point.
        index = -index - 1;

        // insert an entry under that key.
        insert(key, index, entry);

        // one more entry in the btree.
        btree.nentries++;

        // the key was not found.
        return null;

    }

    public Object lookup(int key) {

        btree.touch(this);
        
        int index = Search.search(key, keys, nkeys);

        if (index < 0) {

            // Not found.

            return null;

        }

        return values[index];

    }

    /**
     * <p>
     * Split the leaf. The {@link BTree#leafSplitter} is used to compute the
     * index (splitIndex) at which to split the leaf and a new rightSibling
     * {@link Leaf} is created. All keys and values starting with that index are
     * moved to the new rightSibling. If this leaf is the root of the tree (no
     * parent), then a new root {@link Node} is created without any keys and is
     * made the parent of this leaf. In any case, we then insert( splitKey,
     * rightSibling ) into the parent node, which may cause the parent node
     * itself to split.
     * </p>
     * 
     * @return The new right sibling leaf.
     * 
     * @see ILeafSplitPolicy
     * @see BTree#leafSplitter
     */
    protected AbstractNode split() {

        assert isDirty(); // MUST be mutable.

        // #of keys / values in the leaf.
        final int m = branchingFactor;
        
        // index at which to split the leaf.
        final int splitIndex = btree.leafSplitter.splitLeafAt(this);
        
        // assert that the split will generate two non-empty leafs.
        assert splitIndex >= 1 && splitIndex < m;
        
        final int splitKey = keys[splitIndex];

        final Leaf rightSibling = new Leaf(btree);

        if (btree.DEBUG) {
            BTree.log.debug("SPLIT LEAF: m=" + m + ", splitIndex=" + splitIndex
                    + ", splitKey=" + splitKey + ": ");
            dump(System.err);
        }

        int j = 0;
        for (int i = splitIndex; i < m; i++, j++) {

            // copy key and value to the new leaf.
            rightSibling.keys[j] = keys[i];
            rightSibling.values[j] = values[i];

            // clear out the old keys and values.
            keys[i] = IBTree.NEGINF;
            values[i] = null;

            nkeys--; // one less key here.
            rightSibling.nkeys++; // more more key there.

        }

        // assert that the leaves are not empty.
        assert nkeys>0;
        assert rightSibling.nkeys>0;
        
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
        p.insertChild(splitKey, rightSibling);

        // Return the high leaf.
        return rightSibling;

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
    void insert(int key, int index, Object entry) {

        assert key != NULL;
        assert index >= 0 && index <= nkeys;
        assert entry != null;

        if (nkeys == keys.length) {

            throw new RuntimeException("Overflow: key=" + key + ", index="
                    + index);

        } else {

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
            keys[index] = key; // defined by AbstractNode
            values[index] = entry; // defined by Leaf.

        }

        nkeys++;

    }

    /**
     * Copies all keys and values from the specified start index down by one in
     * order to make room to insert a key and value at that index.
     * 
     * @param index
     *            The index of the first key and value to be copied.
     */
    void copyDown(int index, int count) {

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

        keys[index] = IBTree.NEGINF; // an invalid key.

        values[index] = null;

    }

    /**
     * If the key is found on this leaf, then ensures that the leaf is
     * mutable and removes the key and value.
     * 
     * @param key
     *            The external key.
     * 
     * @return The value stored under that key or null.
     * 
     * @todo Write unit tests for maintaining the tree in balance as entries
     *       are removed.
     */
    public Object remove(int key) {

        btree.touch(this);

        final int index = Search.search(key, keys, nkeys);

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

        assert nkeys>0;

        // The value that is being removed.
        Object entry = values[index];

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
        keys[ nkeys-1 ] = IBTree.NEGINF;
        values[ nkeys-1 ] = null;

        // One less key in the leaf.
        nkeys--;
        
        // One less entry in the tree.
        btree.nentries--;

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

    public boolean dump(PrintStream out, int height, boolean recursive) {

        return dump(BTree.dumpLog.getEffectiveLevel(), out, height, recursive);

    }
     
    public boolean dump(Level level, PrintStream out, int height, boolean recursive) {

        boolean debug = level.toInt() <= Level.DEBUG.toInt();
        
        // Set to false iff an inconsistency is detected.
        boolean ok = true;

        if (debug) {
        
            out.println(indent(height) + "Leaf: " + toString());
            
            out.println(indent(height) + "  parent=" + getParent());
            
            out.println(indent(height) + "  dirty=" + isDirty() + ", nkeys="
                    + nkeys + ", branchingFactor=" + branchingFactor);
            
            out.println(indent(height) + "  keys=" + Arrays.toString(keys));
        }
        
        { // verify keys are monotonically increasing.
            
            int lastKey = IBTree.NEGINF;
            
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
        
        if( debug ) {
        
            out.println(indent(height) + "  vals=" + Arrays.toString(values));
            
        }

        return ok;

    }

}
