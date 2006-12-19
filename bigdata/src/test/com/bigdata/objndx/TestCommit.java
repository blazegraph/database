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
 * Created on Nov 17, 2006
 */

package com.bigdata.objndx;

import junit.framework.TestCase2;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.journal.IRawStore;
import com.bigdata.objndx.BTree.BTreeMetadata;

/**
 * Unit tests for commit functionality that do not trigger copy-on-write.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME This suite should verify that 
 */
public class TestCommit extends TestCase2 {

    /**
     * 
     */
    public TestCommit() {
    }

    /**
     * @param name
     */
    public TestCommit(String name) {
        super(name);
    }

    /*
     * Tests of commit processing (without triggering copy-on-write).
     */
    
    /**
     * Test commit of a new tree (the root is a leaf node).
     */
    public void test_commit01() {

        IRawStore store = new SimpleStore();

        final int branchingFactor = 4;
        
        final long addrMetadata;
        final long rootId;
        {

            BTree btree = new BTree(store,
                    ArrayType.INT,
                    branchingFactor,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                            BTree.DEFAULT_HARD_REF_QUEUE_SCAN),
                            Integer.valueOf(0),
                            null, // no comparator for primitive key type.
                            Int32OIdKeySerializer.INSTANCE,
                            SimpleEntry.Serializer.INSTANCE);

            assertTrue(btree.root.isDirty());

            // Commit of tree with dirty root.
            addrMetadata = btree.write();

            assertFalse(btree.root.isDirty());

            rootId = btree.root.getIdentity();
            
            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

        }
        
        final long metadata2;
        {

            // Load the tree.
            BTree btree = new BTree(store, new BTreeMetadata(BTree
                    .getTransitionalRawStore(store), addrMetadata),
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                            BTree.DEFAULT_HARD_REF_QUEUE_SCAN),
                            Integer.valueOf(0),
                            null, // no comparator for primitive key type.
                            Int32OIdKeySerializer.INSTANCE,
                            SimpleEntry.Serializer.INSTANCE);

            // verify addrRoot.
            assertEquals(rootId,btree.root.getIdentity());
            assertFalse(btree.root.isDirty());

            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

            /*
             * Commit of tree with clean root writes a new metadata record but
             * does not change the addrRoot.
             */
            metadata2 = btree.write();
            assertNotSame( addrMetadata, metadata2 );

        }

        {   // re-verify.

            // Load the tree.
            BTree btree = new BTree(store, new BTreeMetadata(BTree
                    .getTransitionalRawStore(store), addrMetadata),
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            BTree.DEFAULT_HARD_REF_QUEUE_CAPACITY,
                            BTree.DEFAULT_HARD_REF_QUEUE_SCAN),
                            Integer.valueOf(0),
                            null, // no comparator for primitive key type.
                            Int32OIdKeySerializer.INSTANCE,
                    SimpleEntry.Serializer.INSTANCE);

            // verify addrRoot.
            assertEquals(rootId,btree.root.getIdentity());

            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

        }
        
    }

}
