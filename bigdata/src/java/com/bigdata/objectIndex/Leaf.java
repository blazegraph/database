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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

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

    private static final long serialVersionUID = 1L;

    /**
     * The values of the tree.
     */
    protected IObjectIndexEntry[] values;

    /**
     * De-serialization constructor.
     * 
     * @deprecated This is not required when using the {@link NodeSerializer}
     * and all of these clases should no longer implement {@link Serializable}
     * or {@link Externalizable}
     */
    public Leaf() {

    }

    /**
     * De-serialization constructor.
     */
    Leaf(BTree btree, long id, int nkeys, int[] keys, IObjectIndexEntry[] values) {
        
        super(btree);

        assert nkeys >=0 && nkeys< branchingFactor;
        
        assert keys != null;
        
        assert keys.length == branchingFactor;
        
        assert values != null;

        assert values.length == branchingFactor;
        
        setIdentity(id);

        this.nkeys = nkeys;
        
        this.keys = keys;
        
        this.values = values;

        // Add to the hard reference queue.
        btree.leaves.append(this);
        

    }

    
    public Leaf(BTree btree) {

        super(btree);

        // nkeys == nvalues for a Leaf.
        keys = new int[branchingFactor];

        values = new Entry[branchingFactor];

        // Add to the hard reference queue.
        btree.leaves.append(this);

    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source node.
     */
    protected Leaf(Leaf src) {

        super(src);

        // nkeys == nvalues for a Leaf.
        keys = new int[branchingFactor];

        values = new Entry[branchingFactor];

        for (int i = 0; i < nkeys; i++) {

            /*
             * Clone the value so that changes to the value in the new leaf
             * do NOT bleed into the immutable src leaf.
             */
            this.values[i] = new Entry(src.values[i]);

        }

        // Add to the hard reference queue.
        btree.leaves.append(this);

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
     * exists in or belongs in this node.  If the leaf is full, then it is
     * split before the insert.
     * 
     * @param key
     *            The external key.
     * @param entry
     *            The new entry.
     */
    public void insert(int key, Entry entry) {

        /*
         * Note: This is one of the few gateways for mutation of a leaf via
         * the main btree API (insert, lookup, delete). By ensuring that we
         * have a mutable leaf here, we can assert that the leaf must be
         * mutable in other methods.
         */
        Leaf copy = (Leaf) copyOnWrite();

        if (copy != this) {

            copy.insert(key, entry);

            return;

        }

        if (nkeys == branchingFactor) {

            /*
             * Split this leaf into a low leaf (this leaf) and a high leaf
             * (returned by split()). If the key is greater than or equal to
             * the first key in the high leaf then the insert is directed
             * into the high leaf.
             */

            Leaf leaf2 = (Leaf) split();

            if (key >= leaf2.keys[0]) {

                leaf2.insert(key, entry);

                return;

            }

        }

        int index = Search.search(key, keys, nkeys);

        if (index >= 0) {

            /*
             * The key is already present in the leaf.
             * 
             * @todo This is where we would handle replacement of the
             * existing value.
             * 
             * Note: We do NOT search before triggering copy-on-write for an
             * object index since an insert always triggers a mutation
             * (unlike a standard btree which can report that the key
             * already exists, we will actually modify the value as part of
             * the object index semantics).
             */

            return;

        }

        // Convert the position to obtain the insertion point.
        index = -index - 1;

        // insert an entry under that key.
        insert(key, index, entry);

        // one more entry in the btree.
        btree.nentries++;

    }

    public IObjectIndexEntry lookup(int key) {

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

        System.err.print("SPLIT LEAF: m=" + m + ", splitIndex=" + splitIndex
                + ", splitKey=" + splitKey + ": ");
        dump(System.err);

        int j = 0;
        for (int i = splitIndex; i < m; i++, j++) {

            // copy key and value to the new leaf.
            rightSibling.keys[j] = keys[i];
            rightSibling.values[j] = values[i];

            // clear out the old keys and values.
            keys[i] = NEGINF;
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

        btree.nleaves++;

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
    void insert(int key, int index, Entry entry) {

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

        keys[index] = NEGINF; // an invalid key.

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
     * @todo write unit tests for removing keys in terms of the keys and
     *       values remaining in the leaf, lookup reporting false
     *       afterwards, and not visiting the deleted entry.
     * 
     * @todo Write unit tests for maintaining the tree in balance as entries
     *       are removed.
     */
    public IObjectIndexEntry remove(int key) {

        assert key > NEGINF && key < POSINF;

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

        // The value that is being removed.
        IObjectIndexEntry entry = values[index];

        /*
         * Copy over the hole created when the key and value were removed
         * from the leaf. 
         */

        final int length = nkeys - index;

        if (length > 0) {

            System.arraycopy(keys, index + 1, keys, index, length);

            System.arraycopy(values, index + 1, values, index, length);

        } else {

            keys[index] = NEGINF;

            values[index] = null;

        }

        btree.nentries--;

        return entry;

    }

    public Iterator postOrderIterator(final boolean dirtyNodesOnly) {

        if (dirtyNodesOnly) {

            if (isDirty()) {

                return new SingleValueIterator(this);

            } else {

                return EmptyIterator.DEFAULT;

            }

        } else {

            return new SingleValueIterator(this);

        }

    }

    /**
     * Iterator visits the defined {@link Entry}s in key order.
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

        boolean ok = true;
        out.println(indent(height) + "Leaf: " + toString());
        out.println(indent(height) + "  parent=" + getParent());
        out.println(indent(height) + "  dirty=" + isDirty() + ", nkeys="
                + nkeys + ", branchingFactor=" + branchingFactor);
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
        out.println(indent(height) + "  vals=" + Arrays.toString(values));
        return ok;

    }

    /*
     * Note: Serialization is fat since values are not strongly typed.
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(branchingFactor);
        out.writeInt(nkeys);
        for (int i = 0; i < nkeys; i++) {
            int key = keys[i];
            assert keys[i] > NEGINF && keys[i] < POSINF;
            out.writeInt(key);
        }
        for (int i = 0; i < nkeys; i++) {
            Object value = values[i];
            assert value != null;
            out.writeObject(value);
        }
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        branchingFactor = in.readInt();
        nkeys = in.readInt();
        keys = new int[branchingFactor];
        values = new Entry[branchingFactor];
        for (int i = 0; i < nkeys; i++) {
            int key = in.readInt();
            assert keys[i] > NEGINF && keys[i] < POSINF;
            keys[i] = key;
        }
        for (int i = 0; i < nkeys; i++) {
            Entry value = (Entry) in.readObject();
            assert value != null;
            values[i] = value;
        }
    }

}
