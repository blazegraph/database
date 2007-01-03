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
 * Created on Nov 6, 2006
 */

package com.bigdata.objndx;

import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;

/**
 * Abstract test case defines utility methods for testing the object index and
 * its nodes and leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractObjectIndexTestCase extends AbstractBTreeTestCase {

    /**
     * The object identifier whose value has the semantics of [null].  The valid
     * range of object identifiers is (0:maxint). 
     */
    protected static final int NEGINF = 0;
    
    /**
     * 
     */
    public AbstractObjectIndexTestCase() {
    }

    /**
     * @param name
     */
    public AbstractObjectIndexTestCase(String name) {
        super(name);
    }

    /**
     * Return a random reference to a child node or leaf.  The reference is
     * only syntactically valid and MUST NOT be dereferenced.
     */
    private long nextNodeRef() {

        // random slot on the journal in [1:n-1].
        int firstSlot = r.nextInt(Integer.MAX_VALUE - 1) + 1;

        // #of bytes in the serialized record.
        int nbytes = r.nextInt(256)+26;

        // convert to an encoded slot allocation.
        return SlotMath.toLong(nbytes, firstSlot);

    }

    /**
     * Return a random key value. Keys are always generated in a progressive
     * (sorted) order. A key is an int32 within segment persistent identifier
     * that is mapped by the object index onto an {@link ISlotAllocation}. The
     * key value of zero(0) is reserved to indicate null. Positive and negative
     * infinity are defined by the object index.
     * 
     * @param lastKey
     *            The last key value generated and {@link Node#NEGINF_KEY} on
     *            the first invocation for a node.
     * 
     * @see Node#POSINF_KEY
     * @see Node#NEGINF_KEY
     */
    private int nextKey(int pageSize,int index,int lastKey) {
        
        /*
         * The smallest key that we can generate on this pass.
         */
        int min = lastKey + 1;
        
        /*
         * The #of legal key values remaining.
         */
        int range = Integer.MAX_VALUE-lastKey-1;
        
        assert range>0;
        
        /*
         * Divide up the remaining range by the key index position. This ensures
         * that we never run out of legal keys that we can assign until we have
         * first run out of keys that we need to assign.
         */

        int positionsRemaining = pageSize-index;
        
        assert positionsRemaining >= 0;
        
        // choose smaller ranges so that the keys tend to bunch up closer to zero.
//        range = Math.max((range / positionsRemaining)/100, 1);
        range = range / positionsRemaining;
        
        assert range < Integer.MAX_VALUE;
        
        /*
         * Generate a random key within the allowed range of legal keys.
         */
        
        int key = r.nextInt(range)+min;
        
        assert key > lastKey;
//        assert key > BTree.NEGINF;
        assert key < Integer.MAX_VALUE;
        
        return key;
        
    }
    
    /**
     * Generate a random entry.
     */
    public Object getRandomEntry() {

        return new SimpleEntry(r.nextInt());

    }

    /**
     * Generates a non-leaf node with random data.
     */
    public Node getRandomNode(BTree btree) {

        // #of keys per node.
        final int branchingFactor = btree.branchingFactor;
        
        final long id = nextNodeRef(); // ref. for this node.

        int nchildren = r.nextInt((branchingFactor+1)/2)+(branchingFactor+1)/2;
        assert nchildren>=(branchingFactor+1)/2;
        assert nchildren<=branchingFactor;
        int nkeys = nchildren-1;

//        // children are either all leaves or all nodes.
//        boolean isLeaf = r.nextBoolean();

        final int[] keys = new int[branchingFactor];
        
        final long[] children = new long[branchingFactor+1];

        final int[] childEntryCounts = new int[branchingFactor+1];
        
        // node with some valid keys and corresponding child refs.

        int lastKey = NEGINF;

        int nentries = 0;
        
        for (int i = 0; i < nkeys ; i++) {

            lastKey = keys[i] = nextKey(branchingFactor, i, lastKey);

            children[i] = nextNodeRef();

            childEntryCounts[i] = r.nextInt(10)+1; // some non-zero count.  
            
            nentries += childEntryCounts[i];
            
        }

        // children[nkeys] is always defined.

        children[nkeys] = nextNodeRef();

        childEntryCounts[nkeys] = r.nextInt(10)+1; // some non-zero count. 
        
        nentries += childEntryCounts[nkeys];
                
        /*
         * create the node and set it as the root to fake out the btree.
         */
        
        Node node = new Node(btree, id, branchingFactor, nentries, nkeys, keys,
                children, childEntryCounts);
        
        btree.root = node;

        return node;
        
    }

    /**
     * Generates a leaf node with random data.
     */
    public Leaf getRandomLeaf(BTree btree) {

        // #of keys per node.
        final int branchingFactor = btree.branchingFactor;

        long id = nextNodeRef(); // ref. for this leaf.

        int nkeys = r.nextInt((branchingFactor+1)/2)+(branchingFactor+1)/2;
        assert nkeys>=(branchingFactor+1)/2;
        assert nkeys<=branchingFactor;

        final int[] keys = new int[branchingFactor+1];

        final Object[] values = new Object[branchingFactor+1];

        // node with some valid keys and corresponding child refs.

        int lastKey = NEGINF;

        for (int i = 0; i < nkeys; i++) {

            /*
             * Reference is to a data version.
             */

            // the key.
            lastKey = keys[i] = nextKey(branchingFactor, i, lastKey);

            values[i] = getRandomEntry();

        }

//        /*
//         * Set up prior/next leaf node references.
//         */
//        
//        // true unless this is the first leaf node of the index.
//        boolean hasPrevious = r.nextInt(100)>0;
//        
//        // true unless this is the last leaf node of the index.
//        boolean hasNext = r.nextInt(100)>0;
//        
//        long previous = hasPrevious ?nextNodeRef(true,nodeSer) : 0L;
//        
//        long next = hasNext ?nextNodeRef(true,nodeSer) : 0L;

        /*
         * create the leaf and set it as the root to fake out the btree.
         */

        Leaf leaf = new Leaf(btree,id,branchingFactor, nkeys,keys,values); //,previous,next);
        
        btree.root = leaf;
        
        return leaf;

    }

    /**
     * Generates a node or leaf (randomly) with random data.
     */
    public AbstractNode getRandomNodeOrLeaf(BTree ndx) {

        if( r.nextBoolean() ) {
            
            return getRandomNode(ndx);
            
        } else {
            
            return getRandomLeaf(ndx);
            
        }
        
    }
    
}
