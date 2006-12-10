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
import com.bigdata.objndx.ndx.IntegerComparator;

/**
 * Unit tests for commit functionality that do not trigger copy-on-write.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Test commit protocol with more complex trees and also validate the
 * state change for dirty and identity attributes for nodes and leaves as they
 * are written by the commit protocol.
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
        
        final long metadataId;
        final long rootId;
        {

            BTree btree = new BTree(store,
                    ArrayType.INT,
                    branchingFactor,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            BTree.DEFAULT_LEAF_QUEUE_CAPACITY,
                            BTree.DEFAULT_LEAF_QUEUE_SCAN),
                            Integer.valueOf(0),
                            null, // no comparator for primitive key type.
                            Int32OIdKeySerializer.INSTANCE,
                    new SimpleEntry.Serializer());

            assertTrue(btree.root.isDirty());

            // Commit of tree with dirty root.
            metadataId = btree.commit();

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
            BTree btree = new BTree(store, metadataId,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            BTree.DEFAULT_LEAF_QUEUE_CAPACITY,
                            BTree.DEFAULT_LEAF_QUEUE_SCAN),
                            Integer.valueOf(0),
                            null, // no comparator for primitive key type.
                            Int32OIdKeySerializer.INSTANCE,
                    new SimpleEntry.Serializer());

            // verify rootId.
            assertEquals(rootId,btree.root.getIdentity());
            assertFalse(btree.root.isDirty());

            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

            /*
             * Commit of tree with clean root writes a new metadata record but
             * does not change the rootId.
             */
            metadata2 = btree.commit();
            assertNotSame( metadataId, metadata2 );

        }

        {   // re-verify.

            // Load the tree.
            BTree btree = new BTree(store, metadataId,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            BTree.DEFAULT_LEAF_QUEUE_CAPACITY,
                            BTree.DEFAULT_LEAF_QUEUE_SCAN),
                            Integer.valueOf(0),
                            null, // no comparator for primitive key type.
                            Int32OIdKeySerializer.INSTANCE,
                    new SimpleEntry.Serializer());

            // verify rootId.
            assertEquals(rootId,btree.root.getIdentity());

            assertEquals("height", 0, btree.height);
            assertEquals("#nodes", 0, btree.nnodes);
            assertEquals("#leaves", 1, btree.nleaves);
            assertEquals("#entries", 0, btree.nentries);

        }
        
    }

//    /**
//     * Test commit of a tree with some structure.
//     * 
//     * FIXME Re-write commit test to use insert to store keys and drive splits and to verify
//     * the post-condition structure with both node and entry iterators.
//     */
//    public void test_commit02() {
//
//        SimpleStore<Integer, PO> store = new SimpleStore<Integer, PO>();
//
//        final int branchingFactor = 4;
//        
//        int metadataId;
//        int node1Id;
//        int leaf1Id, leaf2Id, leaf3Id;
//        {
//
//            BTree btree = new BTree(store, branchingFactor);
//
//            /*
//             * Replace the root leaf with a Node. This allows us to write the commit
//             * test without having to rely on logic to split the root leaf on
//             * overflow.
//             */
//            btree.root = new Node(btree);
//
//            // Create children and populate the tree structure.
//            Node root = (Node) btree.getRoot();
//            Leaf leaf1 = new Leaf(btree);
//            Node node1 = new Node(btree);
//            Leaf leaf2 = new Leaf(btree);
//            Leaf leaf3 = new Leaf(btree);
//
//            root.addChild(1, node1);
//            root.addChild(2, leaf1);
//
//            node1.addChild(1, leaf2);
//            node1.addChild(2, leaf3);
//
//            assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1, leaf1,
//                    root }, root.postOrderIterator());
//
//            metadataId = btree.commit();
//
//            node1Id = node1.getIdentity();
//
//            leaf1Id = leaf1.getIdentity();
//            leaf2Id = leaf2.getIdentity();
//            leaf3Id = leaf3.getIdentity();
//
//        }
//
//        {
//
//            // Load the btree.
//            BTree btree = new BTree(store, branchingFactor,metadataId);
//
//            Node root = (Node) btree.getRoot();
//            assertEquals(metadataId, root.getIdentity());
//            assertEquals(null,root.getParent());
//            
//            Node node1 = (Node) root.getChild(0);
//            assertEquals(node1Id, node1.getIdentity());
//            assertEquals(root,node1.getParent());
//            
//            Leaf leaf2 = (Leaf) node1.getChild(0);
//            assertEquals(leaf2Id, leaf2.getIdentity());
//            assertEquals(node1,leaf2.getParent());
//            
//            Leaf leaf3 = (Leaf) node1.getChild(1);
//            assertEquals(leaf3Id, leaf3.getIdentity());
//            assertEquals(node1,leaf3.getParent());
//            
//            Leaf leaf1 = (Leaf) root.getChild(1);
//            assertEquals(leaf1Id, leaf1.getIdentity());
//            assertEquals(root,leaf1.getParent());
//            
//            // verify post-order iterator for root.
//            assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1, leaf1,
//                    root }, root.postOrderIterator());
//
//        }
//
//    }
//
//    /*
//     * Tests of copy-on-write semantics.
//     */
//    
//    /**
//     * This simple test of the copy-on-write mechanism sets up a btree with an
//     * immutable (aka persistent) root {@link Node} and then adds a child node
//     * (a leaf). Adding the child to the immutable root triggers copy-on-write
//     * which forces cloning of the original root node. The test verifies that
//     * the new tree has the expected structure and that the old tree was not
//     * changed by this operation.
//     */
//    public void test_copyOnWrite01() {
//
//        SimpleStore<Integer, PO> store = new SimpleStore<Integer, PO>();
//
//        final int branchingFactor = 4;
//        
//        final int metadataId;
//        final int rootId;
//        final BTree oldBTree;
//        {
//            /*
//             * Replace the root leaf with a node and write it on the store. This
//             * gives us an immutable root node as a precondition for what
//             * follows.
//             */
//            BTree btree = new BTree(store, branchingFactor);
//
//            Node root = new Node(btree);
//            btree.root = root; // Note: replace root on the btree.
//
//            rootId = root.write();
//
//            metadataId = btree.commit();
//            
//            assertTrue("persistent", root.isPersistent());
//            assertEquals("identity", rootId, root.getIdentity());
//            
//            oldBTree = btree;
//
//        }
//
//        int newMetadataId;
//        int newRootId;
//        int leaf1Id;
//        { // Add a leaf node - this should trigger copy-on-write.
//
//            // load the btree.
//            BTree btree = new BTree(store,branchingFactor,metadataId);
//
//            Node root = (Node) btree.getRoot();
//            assertTrue("persistent", root.isPersistent());
//            assertEquals("identity", rootId, root.getIdentity());
//
//            Leaf leaf1 = new Leaf(btree);
//
//            Node newRoot = root.addChild(/* external id */2, leaf1);
//            assertNotSame(newRoot, root);
//            assertEquals(newRoot,btree.getRoot()); // check : btree root was set?
//            assertTrue("persistent", root.isPersistent());
//            assertFalse("persistent", newRoot.isPersistent());
//
//            newMetadataId = btree.commit();
//            
//            newRootId = newRoot.getIdentity();
//            
//            leaf1Id = leaf1.getIdentity();
//            
//        }
//
//        { // Verify read back from the store.
//
//            BTree btree = new BTree(store,branchingFactor,newMetadataId);
//
//            // Verify we got the new root.
//            Node root = (Node) btree.getRoot();
//            assertTrue("persistent", root.isPersistent());
//            assertEquals("identity", newRootId, root.getIdentity());
//
//            // Read back the child at index position 0.
//            Leaf leaf1 = (Leaf) root.getChild(0);
//            assertTrue("persistent", leaf1.isPersistent());
//            assertEquals("identity", leaf1Id, leaf1.getIdentity());
//
//            assertSameIterator(new AbstractNode[] { leaf1, root }, root
//                    .postOrderIterator());
//            
//        }
//        
//        { // Verify the old tree is unchanged.
//            
//            /*
//             * Note: We do NOT reload the old tree for this test since we want
//             * to make sure that its state was not modified and we have not done
//             * a commit so a reload would just cause changes to be discarded if
//             * there were any.
//             */
//            
//            Node root = (Node) oldBTree.getRoot();
//            
//            assertSameIterator(new AbstractNode[] { root }, root
//                    .postOrderIterator());
//
//        }
//
//    }
//
//    /**
//     * <p>
//     * Test of copy on write when the pre-condition is a committed tree with the
//     * following committed structure:
//     * </p>
//     * 
//     * <pre>
//     *  root
//     *    leaf1
//     *    node1
//     *      leaf2
//     *      leaf3
//     * </pre>
//     * 
//     * The test adds a leaf to node1 in the 3rd position. This should cause
//     * node1 and the root node to both be cloned and a new root node set on the
//     * tree. The post-modification structure is:
//     * 
//     * <pre>
//     *  root
//     *    leaf1
//     *    node1
//     *      leaf2
//     *      leaf3
//     *      leaf4
//     * </pre>
//     * 
//     * @todo Do tests of copy-on-write that go down to the key-value level.
//     */
//    public void test_copyOnWrite02() {
//
//        SimpleStore<Integer, PO> store = new SimpleStore<Integer, PO>();
//
//        final int branchingFactor = 4;
//        
//        final int metadataId;
//        final int rootId_v0;
//        final int leaf1Id_v0;
//        final int leaf3Id_v0;
//        final int leaf2Id_v0;
//        final int node1Id_v0;
//        {
//
//            BTree btree = new BTree(store, branchingFactor);
//
//            // Create a root node and its children.
//            Node root = new Node(btree);
//            btree.root = root; // replace the root with a Node.
//            Leaf leaf1 = new Leaf(btree);
//            Node node1 = new Node(btree);
//            Leaf leaf2 = new Leaf(btree);
//            Leaf leaf3 = new Leaf(btree);
//
//            root.addChild(1, leaf1);
//            root.addChild(2, node1);
//
//            node1.addChild(1, leaf2);
//            node1.addChild(2, leaf3);
//
//            // verify post-order iterator for root.
//            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node1,
//                    root }, root.postOrderIterator());
//
//            metadataId = btree.commit();
//            leaf1Id_v0 = leaf1.getIdentity();
//            leaf2Id_v0 = leaf2.getIdentity();
//            leaf3Id_v0 = leaf3.getIdentity();
//            node1Id_v0 = node1.getIdentity();
//            rootId_v0  = btree.root.getIdentity();
//            
//        }
//
//        final int rootId_v1, leaf4Id_v0, node1Id_v1;
//        {
//
//            /*
//             * Read the tree back from the store and re-verify the tree
//             * structure.
//             */
//            
//            BTree btree = new BTree(store,branchingFactor,metadataId);
//
//            Node root = (Node) btree.getRoot();
//            assertEquals(rootId_v0,root.getIdentity());
//
//            Leaf leaf1 = (Leaf) root.getChild(0);
//            assertEquals(leaf1Id_v0, leaf1.getIdentity());
//            assertEquals(root,leaf1.getParent());
//
//            Node node1 = (Node) root.getChild(1);
//            assertEquals(node1Id_v0, node1.getIdentity());
//            assertEquals(root,node1.getParent());
//            
//            Leaf leaf2 = (Leaf) node1.getChild(0);
//            assertEquals(leaf2Id_v0, leaf2.getIdentity());
//            assertEquals(node1,leaf2.getParent());
//            
//            Leaf leaf3 = (Leaf) node1.getChild(1);
//            assertEquals(leaf3Id_v0, leaf3.getIdentity());
//            assertEquals(node1,leaf3.getParent());
//
//            // re-verify post-order iterator for root.
//            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node1,
//                    root }, root.postOrderIterator());
//
//            /*
//             * Add a new leaf to node1.  This triggers copy-on-write.
//             */
//            
//            Leaf leaf4 = new Leaf(btree);
//            Node node1_v1 = node1.addChild(/*external key*/3, leaf4);
//
//            // Verify that node1 was changed.
//            assertNotSame( node1_v1, node1 );
//            
//            // Verify that the root node was changed.
//            Node root_v1 = (Node) btree.getRoot();
//            assertNotSame( root, root_v1 );
//
//            // verify post-order iterator for the original root.
//            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, 
//                    node1, root }, root.postOrderIterator());
//
//            // verify post-order iterator for the new root.
//            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, leaf4,
//                    node1_v1, root_v1 }, root_v1.postOrderIterator());
//
//            /*
//             * Commit the tree, get the current identity for all nodes, and
//             * verify that the nodes that should have been cloned by
//             * copy-on-write do in fact have different persistent identity while
//             * those that should not do not.
//             */
//            
//            rootId_v1 = btree.commit();
//            assertEquals( rootId_v1, root_v1.getIdentity() );
//            assertNotSame( rootId_v0, rootId_v1 ); // changed.
//
//            assertEquals( leaf1Id_v0, leaf1.getIdentity() ); // unchanged.
//            assertEquals( leaf2Id_v0, leaf2.getIdentity() ); // unchanged.
//            assertEquals( leaf3Id_v0, leaf3.getIdentity() ); // unchanged.
//            
//            leaf4Id_v0 = leaf4.getIdentity(); // new.
//            
//            node1Id_v1 = node1.getIdentity();
//            assertNotSame( node1Id_v0, node1Id_v1 ); // changed.
//            
//        }
//        
//        { // @todo reload and re-verify the new structure.
//            
//            
//        }
//        
//        {
//            // @todo verify the old tree was unchanged.
//        }
//
//    }

}
