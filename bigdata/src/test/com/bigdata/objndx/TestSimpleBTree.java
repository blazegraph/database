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
 * Created on Nov 8, 2006
 */

package com.bigdata.objndx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase2;

import com.bigdata.cache.HardReferenceCache;
import com.bigdata.cache.HardReferenceCache.HardReferenceCacheEvictionListener;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Test suite that seeks to develop a persistence capable B-Tree supporting
 * copy-on-write semantics. The nodes of the tree should be wired into memory
 * while the leaves of the tree should be written incrementally as they are
 * evicted from a hard reference queue. During a commit, a pre-order traversal
 * should write any dirty leaves to the store followed by their parents up to
 * the root of the tree.
 * 
 * Note: The index does not support traversal with concurrent modification of
 * its structure (adding or removing keys, nodes, or leaves).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSimpleBTree extends TestCase2 {

    /**
     * 
     */
    public TestSimpleBTree() {
    }

    /**
     * @param arg0
     */
    public TestSimpleBTree(String arg0) {

        super(arg0);

    }

    /**
     * Test binary search for keys in a node. The binary search routine is
     * implemented just once, by {@link AbstractNode#binarySearch(int)}. This
     * test sets up some keys, adjusts the #of defined keys, and then verifies
     * both correct lookup of keys that exist and the correct insertion point
     * when the key does not exist.
     */
    public void test_binarySearch01()
    {
    
        // The general formula for the record offset is:
        //
        //    offset := sizeof(record) * ( index - 1 )
        //
        // The general formula for the insertion point is:
        //
        //    insert := - ( offset + 1 )
        //
        // where [offset] is the offset of the record before which the
        // new record should be inserted.

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 6;
        
        BTree btree = new BTree(store, branchingFactor);

        Leaf leaf = (Leaf) btree.getRoot();

        int[] keys = leaf.keys;

        int i = 0;
        keys[i++] = 5;  // offset := 0, insert before := -1
        keys[i++] = 7;  // offset := 1, insert before := -2
        keys[i++] = 9;  // offset := 2, insert before := -3
        keys[i++] = 11; // offset := 3, insert before := -4
        keys[i++] = 13; // offset := 4, insert before := -5
                        //              insert  after := -6
        leaf.nkeys = 5;

        //
        // verify offset of record found.
        //

        // Verify finds the first record in the array.
        assertEquals(0, leaf.binarySearch(5));

        // Verify finds the 2nd record in the array.
        assertEquals(1, leaf.binarySearch(7));

        // Verify finds the penultimate record in the array.
        assertEquals(3, leaf.binarySearch(11));

        // Verify finds the last record in the array.
        assertEquals(4, leaf.binarySearch(13));

        //
        // verify insertion points (key not found).
        //

        // Verify insertion point for key less than any value in the
        // array.
        assertEquals(-1, leaf.binarySearch(4));

        // Verify insertion point for key between first and 2nd
        // records.
        assertEquals(-2, leaf.binarySearch(6));

        // Verify insertion point for key between penultimate and last
        // records.
        assertEquals(-5, leaf.binarySearch(12));

        // Verify insertion point for key greater than the last record.
        assertEquals(-6, leaf.binarySearch(14));

    }

    /**
     * Test ability to insert entries into a leaf. Random (legal) external keys
     * are inserted into a leaf until the leaf would overflow. The random keys
     * are then sorted and compared with the actual keys in the leaf. If the
     * keys were inserted correctly into the leaf then the two arrays of keys
     * will have the same values in the same order.
     * 
     * @todo Write tests that trigger overflow.
     * @todo Write tests for insert into a {@link Node}.
     */
    public void test_insertIntoLeaf01() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 20;
        
        BTree btree = new BTree(store, branchingFactor);

        Leaf root = (Leaf) btree.getRoot();
        
        // array of inserted keys.
        int[] expectedKeys = new int[branchingFactor];
        
        Random r = new Random();
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            int key = r.nextInt(Node.POSINF-1)+1;
            
            int index = root.binarySearch(key);
            
            if( index >= 0 ) {
                
                /*
                 * The key is already present in the leaf.
                 */
                
                continue;
                
            }
        
            // Convert the position to obtain the insertion point.
            index = -index - 1;

            // save the key.
            expectedKeys[ nkeys ] = key;
            
            System.err.println("Will insert: key=" + key + " at index=" + index
                    + " : nkeys=" + nkeys);

            // insert an entry under that key.
            root.insert(key, index, new Entry() );
            
            nkeys++;
            
            assertEquals( nkeys, root.nkeys );
            
        }

        // sort the keys that we inserted.
        Arrays.sort(expectedKeys);
        
        // verify that the leaf has the same keys in the same order.
        assertEquals( expectedKeys, root.keys );
        
    }
    
    /*
     * Test structural modification (adding and removing child nodes).
     */

    /**
     * Test ability to add a child {@link Node}.
     */
    public void test_addChild01() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, 4);

        Node node = new Node(btree);

        assertNull(node.parent);
        assertEquals(0, node.nkeys);
        assertEquals(null, node.childRefs[0]);

        Node child = new Node(btree);
        assertFalse( child.isLeaf() );

        int externalKey = 1; // arbitrary but valid external key.
        node.addChild(externalKey, child);

        assertEquals(1, node.nkeys);
        assertEquals(node, child.parent.get());
        assertEquals(child, node.childRefs[0].get());
        assertEquals(child, node.getChild(0));

    }

    /**
     * Test ability to add a child {@link Leaf}.
     */
    public void test_addChild02() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, 4);

        Node node = new Node(btree);

        assertNull(node.parent);
        assertEquals(0, node.nkeys);
        assertEquals(null, node.childRefs[0]);

        Leaf child = new Leaf(btree);
        assertTrue( child.isLeaf() );

        int externalKey = 1; // arbitrary but valid external key.
        node.addChild(externalKey, child);

        assertEquals(1, node.nkeys);
        assertEquals(node, child.parent.get());
        assertEquals(child, node.childRefs[0].get());
        assertEquals(child, node.getChild(0));

    }

    /*
     * Test iterators -- assumes that structural modification (adding and
     * removing child nodes) has already been tested.
     * 
     * @todo Figure out how to write tests for the postOrderIterator that looks
     * for correct non-visitation of nodes that are not dirty.
     */

    /**
     * <p>
     * Test creates a simple tree and verifies the visitation order. The nodes
     * and leaves in the tree are NOT persistent so that this test minimizes the
     * interaction with the copy-on-write persistence mechanisms. The nodes and
     * leaves of the tree are arranged as follows:
     * </p>
     * 
     * <pre>
     *  root
     *   leaf1
     *   node1
     *    leaf2
     *    leaf3
     * </pre>
     */
    public void test_postOrderIterator01() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, 4);

        // Create a root node and its children.
        Node root = new Node(btree);
        Leaf leaf1 = new Leaf(btree);
        Node node1 = new Node(btree);
        Leaf leaf2 = new Leaf(btree);
        Leaf leaf3 = new Leaf(btree);

        root.addChild(1, leaf1);
        root.addChild(2, node1);

        node1.addChild(1, leaf2);
        node1.addChild(2, leaf3);

        System.err.println("root : " + root);
        System.err.println("leaf1: " + leaf1);
        System.err.println("node1: " + node1);
        System.err.println("leaf2: " + leaf2);
        System.err.println("leaf3: " + leaf3);

        // verify simple child iterator.
        assertSameIterator(new AbstractNode[] { leaf1, node1 }, root
                .childIterator());

        // verify simple child iterator.
        assertSameIterator(new AbstractNode[] { leaf2, leaf3 }, node1
                .childIterator());

        // verify post-order iterator for leaves.
        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());

        // verify post-order iterator for leaves.
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        // verify post-order iterator for leaves.
        assertSameIterator(new AbstractNode[] { leaf3 }, leaf3
                .postOrderIterator());

        // verify post-order iterator for node1.
        assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1 }, node1
                .postOrderIterator());

        // verify post-order iterator for root.
        assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node1,
                root }, root.postOrderIterator());

    }

    /**
     * <p>
     * Test creates a simple tree and verifies the visitation order. The nodes
     * and leaves in the tree are NOT persistent so that this test minimizes the
     * interaction with the copy-on-write persistence mechanisms. The nodes and
     * leaves of the tree are arranged as follows:
     * </p>
     * 
     * <pre>
     *  root
     *   node1
     *    leaf2
     *    leaf3
     *   leaf1
     * </pre>
     */
    public void test_postOrderIterator02() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, 4);

        // Create a root node and its children.
        Node root = new Node(btree);
        Leaf leaf1 = new Leaf(btree);
        Node node1 = new Node(btree);
        Leaf leaf2 = new Leaf(btree);
        Leaf leaf3 = new Leaf(btree);

        root.addChild(1, node1);
        root.addChild(2, leaf1);

        node1.addChild(1, leaf2);
        node1.addChild(2, leaf3);

        System.err.println("root : " + root);
        System.err.println("leaf1: " + leaf1);
        System.err.println("node1: " + node1);
        System.err.println("leaf2: " + leaf2);
        System.err.println("leaf3: " + leaf3);

        // verify simple child iterator.
        assertSameIterator(new AbstractNode[] { node1, leaf1 }, root
                .childIterator());

        // verify simple child iterator.
        assertSameIterator(new AbstractNode[] { leaf2, leaf3 }, node1
                .childIterator());

        // verify post-order iterator for leaves.
        assertSameIterator(new AbstractNode[] { leaf1 }, leaf1
                .postOrderIterator());

        // verify post-order iterator for leaves.
        assertSameIterator(new AbstractNode[] { leaf2 }, leaf2
                .postOrderIterator());

        // verify post-order iterator for leaves.
        assertSameIterator(new AbstractNode[] { leaf3 }, leaf3
                .postOrderIterator());

        // verify post-order iterator for node1.
        assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1 }, node1
                .postOrderIterator());

        // verify post-order iterator for root.
        assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1, leaf1,
                root }, root.postOrderIterator());

    }

    /**
     * <p>
     * Test creates a simple tree and verifies the visitation order. The nodes
     * and leaves in the tree are NOT persistent so that this test minimizes the
     * interaction with the copy-on-write persistence mechanisms. The nodes and
     * leaves of the tree are arranged as follows:
     * </p>
     * 
     * <pre>
     *  root
     *    node1
     *      leaf1
     *      node2
     *        leaf2
     *        leaf3
     *      leaf4
     *    leaf5
     *    node3
     *      leaf6
     * </pre>
     */
    public void test_postOrderIterator03() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        BTree btree = new BTree(store, 4);

        // Create a root node and its children.
        Node root = new Node(btree);
        Node node1 = new Node(btree);
        Node node2 = new Node(btree);
        Node node3 = new Node(btree);
        Leaf leaf1 = new Leaf(btree);
        Leaf leaf2 = new Leaf(btree);
        Leaf leaf3 = new Leaf(btree);
        Leaf leaf4 = new Leaf(btree);
        Leaf leaf5 = new Leaf(btree);
        Leaf leaf6 = new Leaf(btree);

        root.addChild(1, node1);
        root.addChild(2, leaf5);
        root.addChild(3, node3);

        node1.addChild(1, leaf1);
        node1.addChild(2, node2);
        node1.addChild(3, leaf4);

        node2.addChild(1, leaf2);
        node2.addChild(2, leaf3);

        node3.addChild(1, leaf6);

        // verify simple child iterator.
        assertSameIterator(new AbstractNode[] { node1, leaf5, node3 }, root
                .childIterator());

        // verify simple child iterator.
        assertSameIterator(new AbstractNode[] { leaf1, node2, leaf4 }, node1
                .childIterator());

        // verify simple child iterator.
        assertSameIterator(new AbstractNode[] { leaf2, leaf3 }, node2
                .childIterator());

        // verify simple child iterator.
        assertSameIterator(new AbstractNode[] { leaf6 }, node3.childIterator());

        // verify post-order iterator for node3.
        assertSameIterator(new AbstractNode[] { leaf6, node3 }, node3
                .postOrderIterator());

        // verify post-order iterator for node2.
        assertSameIterator(new AbstractNode[] { leaf2, leaf3, node2 }, node2
                .postOrderIterator());

        // verify post-order iterator for node1.
        assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node2,
                leaf4, node1 }, node1.postOrderIterator());

        // verify post-order iterator for root.
        assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node2,
                leaf4, node1, leaf5, leaf6, node3, root }, root
                .postOrderIterator());

    }

    /*
     * Test persistence.
     */

    /**
     * Verify that we can create and persist the root node (a leaf). Verify that
     * we can recover the node.
     */
    public void test_persistence01() {

        final Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 4;
       
        final int leafId;
        {

            BTree btree = new BTree(store, branchingFactor);

            Leaf root = (Leaf) btree.getRoot();

            leafId = root.write();

            assertTrue("persistent", root.isPersistent());
            assertEquals("identity", leafId, root.getIdentity());

        }

        {

            BTree btree = new BTree(store, branchingFactor, leafId);
            
            Leaf root = (Leaf) btree.getRoot();
            
            assertTrue("persistent", root.isPersistent());
            assertEquals("identity", leafId, root.getIdentity());

        }

    }

    /**
     * Create a small tree with a root node and two leaves and verify that we
     * can persist and recover it.
     */
    public void test_persistence02() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 4;
            
        final int rootId;
        {
            
            BTree btree = new BTree(store, branchingFactor);

            Node root = new Node(btree);
            btree.root = root; // Note: replace root on the btree.

            Leaf leaf1 = new Leaf(btree);
            Leaf leaf2 = new Leaf(btree);
            Leaf leaf3 = new Leaf(btree);

            root.addChild(1, leaf1);
            root.addChild(2, leaf2);
            root.addChild(3, leaf3);

            int leaf1Id = leaf1.write(); // write on store.
            assertTrue("persistent", leaf1.isPersistent());
            assertEquals("identity", leaf1Id, leaf1.getIdentity());

            int leaf2Id = leaf2.write();
            assertTrue("persistent", leaf2.isPersistent());
            assertEquals("identity", leaf2Id, leaf2.getIdentity());

            int leaf3Id = leaf3.write();
            assertTrue("persistent", leaf3.isPersistent());
            assertEquals("identity", leaf3Id, leaf3.getIdentity());

            rootId = root.write();
            assertTrue("persistent", root.isPersistent());
            assertEquals("identity", rootId, root.getIdentity());

        }

        {

            BTree btree = new BTree(store,branchingFactor,rootId);
            
            Node root = (Node) btree.getRoot();
            assertTrue( root.isPersistent() );
            assertEquals( rootId, root.getIdentity() );

            Leaf leaf1 = (Leaf) root.getChild(0);
            assertEquals( root, leaf1.getParent() );
            assertTrue( leaf1.isPersistent() );
            assertFalse( leaf1.isDirty() );
            
            Leaf leaf2 = (Leaf) root.getChild(1);
            assertEquals( root, leaf2.getParent() );
            assertTrue( leaf2.isPersistent() );
            assertFalse( leaf2.isDirty() );

            Leaf leaf3 = (Leaf) root.getChild(2);
            assertEquals( root, leaf3.getParent() );
            assertTrue( leaf3.isPersistent() );
            assertFalse( leaf3.isDirty() );

        }

    }

    /*
     * Tests of commit processing (without triggering copy-on-write).
     */
    
    /**
     * Test commit of a new tree (the root is a leaf node).
     */
    public void test_commit01() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 4;
        
        final int rootId;
        {

            BTree btree = new BTree(store, branchingFactor);

            // Commit of tree with dirty root.
            rootId = btree.commit();

            Leaf root = (Leaf) store.read(rootId);

            assertEquals(rootId, root.getIdentity());

            // Commit of tree with clean root is NOP and returns existing
            // rootId.
            assertEquals(rootId, btree.commit());

        }
        
        {

            // Load the tree.
            BTree btree = new BTree(store, branchingFactor, rootId);

            // Commit of tree with clean root is NOP and returns existing
            // rootId.
            assertEquals( rootId, btree.commit() );

        }

    }

    /**
     * Test commit of a tree with some structure.
     */
    public void test_commit02() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 4;
        
        int rootId;
        int node1Id;
        int leaf1Id, leaf2Id, leaf3Id;
        {

            BTree btree = new BTree(store, branchingFactor);

            /*
             * Replace the root leaf with a Node. This allows us to write the commit
             * test without having to rely on logic to split the root leaf on
             * overflow.
             */
            btree.root = new Node(btree);

            // Create children and populate the tree structure.
            Node root = (Node) btree.getRoot();
            Leaf leaf1 = new Leaf(btree);
            Node node1 = new Node(btree);
            Leaf leaf2 = new Leaf(btree);
            Leaf leaf3 = new Leaf(btree);

            root.addChild(1, node1);
            root.addChild(2, leaf1);

            node1.addChild(1, leaf2);
            node1.addChild(2, leaf3);

            assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1, leaf1,
                    root }, root.postOrderIterator());

            rootId = btree.commit();

            node1Id = node1.getIdentity();

            leaf1Id = leaf1.getIdentity();
            leaf2Id = leaf2.getIdentity();
            leaf3Id = leaf3.getIdentity();

        }

        {

            // Load the btree.
            BTree btree = new BTree(store, branchingFactor,rootId);

            Node root = (Node) btree.getRoot();
            assertEquals(rootId, root.getIdentity());
            assertEquals(null,root.getParent());
            
            Node node1 = (Node) root.getChild(0);
            assertEquals(node1Id, node1.getIdentity());
            assertEquals(root,node1.getParent());
            
            Leaf leaf2 = (Leaf) node1.getChild(0);
            assertEquals(leaf2Id, leaf2.getIdentity());
            assertEquals(node1,leaf2.getParent());
            
            Leaf leaf3 = (Leaf) node1.getChild(1);
            assertEquals(leaf3Id, leaf3.getIdentity());
            assertEquals(node1,leaf3.getParent());
            
            Leaf leaf1 = (Leaf) root.getChild(1);
            assertEquals(leaf1Id, leaf1.getIdentity());
            assertEquals(root,leaf1.getParent());
            
            // verify post-order iterator for root.
            assertSameIterator(new AbstractNode[] { leaf2, leaf3, node1, leaf1,
                    root }, root.postOrderIterator());

        }

    }

    /*
     * Tests of copy-on-write semantics.
     */
    
    /**
     * This simple test of the copy-on-write mechanism sets up a btree with an
     * immutable (aka persistent) root {@link Node} and then adds a child node
     * (a leaf). Adding the child to the immutable root triggers copy-on-write
     * which forces cloning of the original root node. The test verifies that
     * the new tree has the expected structure and that the old tree was not
     * changed by this operation.
     */
    public void test_copyOnWrite01() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 4;
        
        final int rootId;
        final BTree oldBTree;
        {
            /*
             * Replace the root leaf with a node and write it on the store. This
             * gives us an immutable root node as a precondition for what
             * follows.
             */
            BTree btree = new BTree(store, branchingFactor);

            Node root = new Node(btree);
            btree.root = root; // Note: replace root on the btree.

            rootId = root.write();
            assertTrue("persistent", root.isPersistent());
            assertEquals("identity", rootId, root.getIdentity());
            
            oldBTree = btree;

        }

        int newRootId;
        int leaf1Id;
        { // Add a leaf node - this should trigger copy-on-write.

            // load the btree.
            BTree btree = new BTree(store,branchingFactor,rootId);

            Node root = (Node) btree.getRoot();
            assertTrue("persistent", root.isPersistent());
            assertEquals("identity", rootId, root.getIdentity());

            Leaf leaf1 = new Leaf(btree);

            Node newRoot = root.addChild(/* external id */2, leaf1);
            assertNotSame(newRoot, root);
            assertEquals(newRoot,btree.getRoot()); // check : btree root was set?
            assertTrue("persistent", root.isPersistent());
            assertFalse("persistent", newRoot.isPersistent());

            newRootId = btree.commit();

            assertEquals(newRootId,newRoot.getIdentity());
            
            leaf1Id = leaf1.getIdentity();
            
        }

        { // Verify read back from the store.

            BTree btree = new BTree(store,branchingFactor,newRootId);

            // Verify we got the new root.
            Node root = (Node) btree.getRoot();
            assertTrue("persistent", root.isPersistent());
            assertEquals("identity", newRootId, root.getIdentity());

            // Read back the child at index position 0.
            Leaf leaf1 = (Leaf) root.getChild(0);
            assertTrue("persistent", leaf1.isPersistent());
            assertEquals("identity", leaf1Id, leaf1.getIdentity());

            assertSameIterator(new AbstractNode[] { leaf1, root }, root
                    .postOrderIterator());
            
        }
        
        { // Verify the old tree is unchanged.
            
            /*
             * Note: We do NOT reload the old tree for this test since we want
             * to make sure that its state was not modified and we have not done
             * a commit so a reload would just cause changes to be discarded if
             * there were any.
             */
            
            Node root = (Node) oldBTree.getRoot();
            
            assertSameIterator(new AbstractNode[] { root }, root
                    .postOrderIterator());

        }

    }

    /**
     * <p>
     * Test of copy on write when the pre-condition is a committed tree with the
     * following committed structure:
     * </p>
     * 
     * <pre>
     *  root
     *    leaf1
     *    node1
     *      leaf2
     *      leaf3
     * </pre>
     * 
     * The test adds a leaf to node1 in the 3rd position. This should cause
     * node1 and the root node to both be cloned and a new root node set on the
     * tree. The post-modification structure is:
     * 
     * <pre>
     *  root
     *    leaf1
     *    node1
     *      leaf2
     *      leaf3
     *      leaf4
     * </pre>
     * 
     * @todo Do tests of copy-on-write that go down to the key-value level.
     */
    public void test_copyOnWrite02() {

        Store<Integer, PO> store = new Store<Integer, PO>();

        final int branchingFactor = 4;
        
        final int rootId_v0;
        final int leaf1Id_v0;
        final int leaf3Id_v0;
        final int leaf2Id_v0;
        final int node1Id_v0;
        {

            BTree btree = new BTree(store, branchingFactor);

            // Create a root node and its children.
            Node root = new Node(btree);
            btree.root = root; // replace the root with a Node.
            Leaf leaf1 = new Leaf(btree);
            Node node1 = new Node(btree);
            Leaf leaf2 = new Leaf(btree);
            Leaf leaf3 = new Leaf(btree);

            root.addChild(1, leaf1);
            root.addChild(2, node1);

            node1.addChild(1, leaf2);
            node1.addChild(2, leaf3);

            // verify post-order iterator for root.
            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node1,
                    root }, root.postOrderIterator());

            rootId_v0 = btree.commit();
            leaf1Id_v0 = leaf1.getIdentity();
            leaf2Id_v0 = leaf2.getIdentity();
            leaf3Id_v0 = leaf3.getIdentity();
            node1Id_v0 = node1.getIdentity();
            
        }

        final int rootId_v1, leaf4Id_v0, node1Id_v1;
        {

            /*
             * Read the tree back from the store and re-verify the tree
             * structure.
             */
            
            BTree btree = new BTree(store,branchingFactor,rootId_v0);

            Node root = (Node) btree.getRoot();
            assertEquals(rootId_v0,root.getIdentity());

            Leaf leaf1 = (Leaf) root.getChild(0);
            assertEquals(leaf1Id_v0, leaf1.getIdentity());
            assertEquals(root,leaf1.getParent());

            Node node1 = (Node) root.getChild(1);
            assertEquals(node1Id_v0, node1.getIdentity());
            assertEquals(root,node1.getParent());
            
            Leaf leaf2 = (Leaf) node1.getChild(0);
            assertEquals(leaf2Id_v0, leaf2.getIdentity());
            assertEquals(node1,leaf2.getParent());
            
            Leaf leaf3 = (Leaf) node1.getChild(1);
            assertEquals(leaf3Id_v0, leaf3.getIdentity());
            assertEquals(node1,leaf3.getParent());

            // re-verify post-order iterator for root.
            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, node1,
                    root }, root.postOrderIterator());

            /*
             * Add a new leaf to node1.  This triggers copy-on-write.
             */
            
            Leaf leaf4 = new Leaf(btree);
            Node node1_v1 = node1.addChild(/*external key*/3, leaf4);

            // Verify that node1 was changed.
            assertNotSame( node1_v1, node1 );
            
            // Verify that the root node was changed.
            Node root_v1 = (Node) btree.getRoot();
            assertNotSame( root, root_v1 );

            // verify post-order iterator for the original root.
            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, 
                    node1, root }, root.postOrderIterator());

            // verify post-order iterator for the new root.
            assertSameIterator(new AbstractNode[] { leaf1, leaf2, leaf3, leaf4,
                    node1_v1, root_v1 }, root_v1.postOrderIterator());

            /*
             * Commit the tree, get the current identity for all nodes, and
             * verify that the nodes that should have been cloned by
             * copy-on-write do in fact have different persistent identity while
             * those that should not do not.
             */
            
            rootId_v1 = btree.commit();
            assertEquals( rootId_v1, root_v1.getIdentity() );
            assertNotSame( rootId_v0, rootId_v1 ); // changed.

            assertEquals( leaf1Id_v0, leaf1.getIdentity() ); // unchanged.
            assertEquals( leaf2Id_v0, leaf2.getIdentity() ); // unchanged.
            assertEquals( leaf3Id_v0, leaf3.getIdentity() ); // unchanged.
            
            leaf4Id_v0 = leaf4.getIdentity(); // new.
            
            node1Id_v1 = node1.getIdentity();
            assertNotSame( node1Id_v0, node1Id_v1 ); // changed.
            
        }
        
        { // @todo reload and re-verify the new structure.
            
            
        }
        
        {
            // @todo verify the old tree was unchanged.
        }

    }
    
    /**
     * Persistence store.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     * @param <T>
     */
    public static class Store<K, T extends PO> {

        /**
         * Key factory.
         */
        private int nextKey = 1;

        /**
         * "Persistence store" - access to objects by the key.
         */
        private final Map<K, byte[]> store = new HashMap<K, byte[]>();

        private byte[] serialize(T po) {

            try {

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(po);
                oos.flush();
                oos.close();
                byte[] bytes = baos.toByteArray();
                return bytes;

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

        }

        private T deserialize(byte[] bytes) {

            try {

                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais);
                T po = (T) ois.readObject();
                return po;

            } catch (Exception ex) {

                throw new RuntimeException(ex);
            }

        }

        public K nextId() {

            /*
             * Note: The use of generics breaks down here since we need a
             * primitive data type to serve as the value space for the key
             * factory and a means to assign new keys within that value space.
             * This can not be defined "generically", but it rather data type
             * and store semantics specific.
             */
            K key = (K) new Integer(nextKey++);

            return key;

        }

        public T read(K key) {

            byte[] bytes = store.get(key);

            if (bytes == null)
                throw new IllegalArgumentException("Not found: key=" + key);

            T value = deserialize(bytes);

            // Note: breaks generic isolation.
            value.setIdentity(((Integer) key).intValue()); // set the key - no
                                                            // back references
                                                            // exist yet.

            value.setDirty(false); // just read from the store.

            return value;

        }

        public K insert(T value) {

            assert value != null;
            assert value.isDirty();
            assert !value.isPersistent();

            K key = nextId();

            write(key, value);

            return key;

        }

        protected void write(K key, T value) {

            assert key != null;
            assert value != null;
            assert value.isDirty();

            byte[] bytes = serialize(value);

            store.put(key, bytes);

            /*
             * Set identity on persistent object.
             * 
             * Note: breaks generic isolation.
             */
            value.setIdentity(((Integer) key).intValue());

            // just wrote on the store.
            value.setDirty(false);

        }

        // Note: Must also mark the object as invalid.
        public void delete(K key) {

            store.remove(key);

        }

    }

    /**
     * Hard reference cache eviction listener for leaves.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     *            The key type.
     * @param <T>
     *            The value type.
     */
    public static class LeafEvictionListener<K, T extends PO> implements
            HardReferenceCacheEvictionListener<T> {

        /**
         * Persistence store.
         */
        private final Store<K, T> store;

        public LeafEvictionListener(Store<K, T> store) {

            assert store != null;

            this.store = store;

        }

        public void evicted(HardReferenceCache<T> cache, T ref) {

            assert ref instanceof Leaf;
            
            if( ref.isDirty() ) {

                ((Leaf)ref).write();
                
            }

        }

    }

    /**
     * An interface that declares how we access the persistent identity of an
     * object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IIdentityAccess {

        /**
         * Null reference for the store (zero).
         */
        public final int NULL = 0;

        /**
         * The persistent identity.
         * 
         * @exception IllegalStateException
         *                if the object is not persistent.
         */
        public int getIdentity() throws IllegalStateException;

        /**
         * True iff the object is persistent.
         */
        public boolean isPersistent();

    }

    /**
     * An interface that declares how we access the dirty state of an object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IDirty {

        public void setDirty(boolean dirty);

        public boolean isDirty();

    }

    /**
     * A persistent object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     */
    abstract public static class PO implements IIdentityAccess, IDirty,
            Externalizable {

        /**
         * The persistent identity (defined when the object is actually
         * persisted).
         */
        transient private int key = NULL;

        public boolean isPersistent() {

            return key != NULL;

        }

        public int getIdentity() throws IllegalStateException {

            if (key == NULL)
                throw new IllegalStateException();

            return key;

        }

        /**
         * Used by the store to set the persistent identity.
         * 
         * Note: This method should not be public.
         * 
         * @param key
         *            The key.
         * 
         * @throws IllegalStateException
         *             If the key is already defined.
         */
        void setIdentity(int key) throws IllegalStateException {

            if (key == NULL)
                throw new IllegalArgumentException();

            if (this.key != NULL)
                throw new IllegalStateException();

            this.key = key;

        }

        /**
         * New objects are considered to be dirty. When an object is
         * deserialized from the store the dirty flag MUST be explicitly
         * cleared.
         */
        transient private boolean dirty = true;

        public boolean isDirty() {

            return dirty;

        }

        public void setDirty(boolean dirty) {

            this.dirty = dirty;

        }

        /**
         * Extends the basic behavior to display the persistent identity of the
         * object iff the object is persistent.
         */
        public String toString() {

            if (key != NULL) {

                return super.toString() + "#" + key;

            } else {

                return super.toString();

            }

        }

    }

    /**
     * <p>
     * BTree encapsulates metadata about the persistence capable index, but is
     * not itself a persistent object.
     * </p>
     * <p>
     * Note: No mechanism is exposed for recovering a node or leaf of the tree
     * other than the root by its key. This is because the parent reference on
     * the node (or leaf) can only be set when it is read from the store in
     * context by its parent node.
     * </p>
     * <p>
     * Note: This implementation is NOT thread-safe. The object index is
     * intended for use within a single-threaded context.
     * </p>
     * <p>
     * Note: This iterators exposed by this implementation do NOT support
     * concurrent structural modification. Concurrent inserts or removals of
     * keys MAY produce incoherent traversal whether or not they result in
     * addition or removal of nodes in the tree.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Track the height, #of nodes (or nodes + leaves), and #of entries in
     *       the btree.
     */
    public static class BTree {

        /**
         * The persistence store.
         */
        final protected Store<Integer, PO> store;

        /**
         * The branching factor for the btree.
         */
        final protected int branchingFactor;

        /**
         * A hard reference hash map for nodes in the btree is used to ensure
         * that nodes remain wired into memory. Dirty nodes are written to disk
         * during commit using a pre-order traversal that first writes any dirty
         * leaves and then (recursively) their parent nodes.
         * 
         * @todo Make sure that nodes are eventually removed from this set.
         *       There are two ways to make that happen. One is to just use a
         *       ring buffer with a large capacity.  This will serve a bit like
         *       an MRU.  The other is to remove nodes from this set explicitly
         *       on certain conditions.  For example, when a copy is made of an
         *       immutable node the immutable node might be removed from this
         *       set.
         */
        final Set<Node> nodes = new HashSet<Node>();

        /**
         * Leaves are added to a hard reference queue when they are created or
         * read from the store. On eviction from the queue the leaf is
         * serialized by {@link #listener} against the {@link #store}. Once the
         * leaf is no longer strongly reachable its weak references may be
         * cleared by the VM.
         * 
         * @todo Write tests to verify incremental write of leaves driven by
         *       eviction from this hard reference queue. This will require
         *       controlling the cache size and #of references scanned in order
         *       to force triggering of leaf eviction under controller
         *       circumstances.
         */
        final HardReferenceCache<PO> leaves;
        
        /**
         * Writes dirty leaves onto the {@link #store} as they are evicted.
         */
        final LeafEvictionListener<Integer, PO> listener;
        
        /**
         * The root of the btree. This is initially a leaf until the leaf is
         * split, at which point it is replaced by a node. The root is also
         * replaced each time copy-on-write triggers a cascade of updates.
         */
        AbstractNode root;

        /**
         * The root of the btree. This is initially a leaf until the leaf is
         * split, at which point it is replaced by a node. The root is also
         * replaced each time copy-on-write triggers a cascade of updates.
         */
        public AbstractNode getRoot() {

            return root;

        }

        /**
         * Constructor for a new btree.
         * 
         * @param store
         *            The persistence store.
         * @param branchingFactor
         *            The branching factor.
         */
        public BTree(Store<Integer, PO> store, int branchingFactor) {

            assert store != null;
            assert branchingFactor > 0 && (branchingFactor & 1) == 0;

            this.store = store;
            
            this.branchingFactor = branchingFactor;
            
            listener = new LeafEvictionListener<Integer, PO>(store);
            
            leaves = new HardReferenceCache<PO>(listener,1000);
            
            this.root = new Leaf(this);

        }

        /**
         * Constructor for an existing btree.
         * 
         * @param store
         *            The persistence store.
         * @param branchingFactor
         *            The branching factor.
         * @param rootId
         *            The persistent identifier of the root of the btree.
         */
        public BTree(Store<Integer, PO> store, int branchingFactor, int rootId) {

            assert store != null;
            assert branchingFactor > 0 && (branchingFactor & 1) == 0;

            this.store = store;
            
            this.branchingFactor = branchingFactor;
            
            listener = new LeafEvictionListener<Integer, PO>(store);
            
            leaves = new HardReferenceCache<PO>(listener,1000);
            
            this.root = (AbstractNode)store.read(rootId);
            
            this.root.btree = this; // patch btree reference.

        }

        /**
         * Commit dirty nodes using a post-order traversal that first writes any
         * dirty leaves and then (recursively) their parent nodes. The parent
         * nodes are guarenteed to be dirty if there is a dirty child so the
         * commit never triggers copy-on-write.
         * 
         * @return The persistent identity of the root of the tree.
         */
        public int commit() {

            if (!root.isDirty()) {

                /*
                 * Optimization : if the root node is not dirty then the
                 * children can not be dirty either.
                 */

                return root.getIdentity();

            }

            int ndirty = 0; // #of dirty nodes (node or leave) written by
                            // commit.
            int nleaves = 0; // #of dirty leaves written by commit.

            /*
             * Traverse tree, writing dirty nodes onto the store.
             * 
             * Note: This iterator only visits dirty nodes.
             */
            Iterator itr = root.postOrderIterator(true);

            while (itr.hasNext()) {

                AbstractNode node = (AbstractNode) itr.next();

                assert node.isDirty();
                
//                if (node.isDirty()) {

                    if (node != root) {

                        /*
                         * The parent MUST be defined unless this is the root
                         * node.
                         */

                        assertNotNull( node.getParent() );
                    
                    }

                    // write the dirty node on the store.
                    node.write();

                    ndirty++;

                    if (node instanceof Leaf)
                        nleaves++;

//                }

            }

            System.err.println("commit: " + ndirty + " dirty nodes (" + nleaves
                    + " leaves)");

            return root.getIdentity();

        }

        /**
         * Add / update an entry in the object index.
         * 
         * @param id
         *            The persistent id.
         * @param slots
         *            The slots on which the current version is written.
         */
        public void put(int id,ISlotAllocation slots) {
           
            assert id > AbstractNode.NEGINF && id < AbstractNode.POSINF;

            // FIXME Implement find/insert.
            
        }
        
        /**
         * Return the slots on which the current version of the object is
         * written.
         * 
         * @param id
         *            The persistent id.
         * @return The slots on which the current version is written.
         * 
         * @todo Implement get(id) : ISlotAllocation.
         */
        public ISlotAllocation get(int id) {
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * Mark the object as deleted.
         * 
         * @param id
         *            The persistent id.
         * 
         * @todo Implement delete(id).
         */
        public void delete(int id) {
            
            throw new UnsupportedOperationException();
            
        }
        
    }

    /**
     * <p>
     * Abstract node.
     * </p>
     * <p>
     * Note: For nodes in the index, the attributes dirty and persistent are
     * 100% correlated. Since only transient nodes may be dirty and only
     * persistent nodes are clean any time one is true the other is false.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public abstract static class AbstractNode extends PO {

        /**
         * Negative infinity for the external keys.
         */
        static final int NEGINF = 0;

        /**
         * Positive infinity for the external keys.
         */
        static final int POSINF = Integer.MAX_VALUE;

        /**
         * The BTree.
         * 
         * Note: This field MUST be patched when the node is read from the
         * store. This requires a custom method to read the node with the btree
         * reference on hand so that we can set this field.
         * 
         * Note: We also need the branching factor on hand when we deserialize a
         * node. That is why {@link #branchingFactor} is currently defined as a
         * constant.
         */
        transient protected BTree btree;

        /**
         * The branching factor (#of slots for keys or values).
         */
        transient protected int branchingFactor;

        /**
         * The #of valid keys for this node.
         */
        protected int nkeys = 0;

        /**
         * The external keys for the B+Tree. (The length of this array is the
         * branching factor in force for this node).
         */
        protected int[] keys;

        /**
         * The parent of this node. This is null for the root node. The parent
         * is required in order to set the persistent identity of a newly
         * persisted child node on its parent. The reference to the parent will
         * remain strongly reachable as long as the parent is either a root
         * (held by the {@link BTree}) or a dirty child (held by the
         * {@link Node}). The parent reference is set when a node is attached
         * as the child of another node.
         */
        protected WeakReference<Node> parent = null;

        /**
         * The parent iff the node has been added as the child of another node
         * and the parent reference has not been cleared.
         * 
         * @return The parent.
         */
        public Node getParent() {

            Node p = null;
            
            if (parent != null) {

                /*
                 * Note: Will be null if the parent reference has been cleared.
                 */
                p = parent.get();

            }

            /*
             * The parent is allowed to be null iff this is the root of the
             * btree.
             */
            assert (this == btree.root && p == null) || p != null;
            
            return p;

        }

        /**
         * De-serialization constructor used by subclasses.
         */
        protected AbstractNode() {
            
        }

        public AbstractNode(BTree btree) {

            assert btree != null;

            this.btree = btree;

            this.branchingFactor = btree.branchingFactor;
            
            this.keys = new int[branchingFactor];
            
        }

        /**
         * Copy constructor.
         * 
         * @param src
         *            The source node.
         */
        protected AbstractNode(AbstractNode src) {

            /*
             * Note: We do NOT clone the base class since this is a new
             * persistence capable object.
             */
            super();

            assert ! isPersistent();
            
            assert src != null;

            this.btree = src.btree;

            this.branchingFactor =  btree.branchingFactor;
            
            this.keys = new int[branchingFactor];
            
            this.nkeys = src.nkeys;

            for (int i = 0; i < nkeys; i++) {

                this.keys[i] = src.keys[i];

            }

        }

        /**
         * Used to patch the btree reference during de-serialization.
         * 
         * @param btree
         */
        void setBTree(BTree btree) {

            assert btree != null;

            if (this.btree != null)
                throw new IllegalStateException();

            this.btree = btree;

        }

        /**
         * <p>
         * Return this node iff it is dirty and otherwise return a copy of this
         * node. If a copy is made of the node, then a copy will also be made of
         * each parent of the node up to the root of the tree and the new root
         * node will be set on the {@link BTree}.
         * </p>
         * <p>
         * This method must MUST be invoked any time an mutative operation is
         * requested for the node. You can not modify a node that has been
         * written onto the store. Instead, you have to clone the node causing
         * it and all nodes up to the root to be dirty and transient. This
         * method handles that cloning process, but the caller MUST test whether
         * or not the node was copied by this method, MUST delegate the mutation
         * operation to the copy iff a copy was made, and MUST result in an
         * awareness in the caller that the copy exists and needs to be used in
         * place of the immutable version of the node.
         * </p>
         * 
         * @return Either this node or a copy of this node.
         */
        protected AbstractNode copyOnWrite() {

            if (isPersistent()) {

                Node parent = this.getParent();
                
                AbstractNode newNode;
                
                if (this instanceof Node) {

                    newNode = new Node((Node) this);

                } else {

                    newNode = new Leaf((Leaf) this);

                }

                if( btree.root == this ) {
                    
                    assert parent == null;
                    
                    // Update the root node on the btree.

                    System.err.println("Copy-on-write : replaced root node on btree.");
                    
                    btree.root = newNode;
                    
                } else {
                    
                    assert parent != null;
                    
                    if( ! parent.isDirty() ) {

                        Node newParent = (Node) parent.copyOnWrite();
                        
                        newParent.replaceChildRef(this.getIdentity(),newNode);
                    
                    }
                    
                }
                
                return newNode;

            } else {

                return this;

            }

        }

        /**
         * Post-order traveral of nodes and leaves in the tree. For any given
         * node, its children are always visited before the node itself (hence
         * the node occurs in the post-order position in the traveral). The
         * iterator is NOT safe for concurrent modification.
         */
        public Iterator postOrderIterator() {
            
            return postOrderIterator( false );
            
        }

        /**
         * Post-order traveral of nodes and leaves in the tree. For any given
         * node, its children are always visited before the node itself (hence
         * the node occurs in the post-order position in the traveral). The
         * iterator is NOT safe for concurrent modification.
         * 
         * @param dirtyNodesOnly
         *            When true, only dirty nodes and leaves will be visited
         */
        abstract public Iterator postOrderIterator(boolean dirtyNodesOnly);
        
        /**
         * Traversal of index values in key order.
         */
        public Iterator entryIterator() {

            /*
             * Begin with a post-order iterator.
             */
            return new Striterator(postOrderIterator()).addFilter(new Expander() {
                /*
                 * Expand the {@link Entry} objects for each leaf visited in
                 * the post-order traversal.
                 */
                protected Iterator expand(Object childObj) {
                    /*
                     * A child of this node.
                     */
                    AbstractNode child = (AbstractNode) childObj;

                    if( child instanceof Leaf ) {

                        return ((Leaf)child).entryIterator();

                    } else {
                        
                        return EmptyIterator.DEFAULT;
                        
                    }
                }
            });
            
        }

        /**
         * True iff this is a leaf node.
         */
        abstract public boolean isLeaf();
        
        /**
         * Binary search on the array of external keys.
         * 
         * @param key
         *            The key for the search.
         * 
         * @return index of the search key, if it is contained in the array;
         *         otherwise, <code>(-(insertion point) - 1)</code>. The
         *         insertion point is defined as the point at which the key
         *         would be inserted into the array. Note that this guarantees
         *         that the return value will be >= 0 if and only if the key is
         *         found.
         */
        final public int binarySearch(final int key) {

            int low = 0;

            int high = nkeys - 1;

            while (low <= high) {

                final int mid = (low + high) >> 1;

                final int midVal = keys[mid];

                if (midVal < key) {

                    low = mid + 1;
                }

                else if (midVal > key) {

                    high = mid - 1;

                } else {

                    // Found: return offset.

                    return mid;

                }

            }

            // Not found: return insertion point.

            final int offset = low;

            return -(offset + 1);

        }

        /**
         * <p>
         * Copy down the elements of the per-key arrays from the index to the
         * #of keys currently defined thereby creating an unused position at
         * <i>index</i>. The #of keys is NOT modified by this method.
         * </p>
         * <p>
         * This method MUST be extended by subclasses that declare additional
         * per-key data.
         * </p>
         * 
         * @param index
         *            The index.
         * 
         * @param count
         *            The #of elements to be copied (computed as {@link #nkeys} -
         *            <i>index</i>).
         */
        protected void copyDown(int index, int count) {
            
            /*
             * copy down per-key data.
             */
            System.arraycopy( keys, index, keys, index+1, count );
            
            /*
             * Clear the entry at the index. This is part paranoia check and
             * partly critical. Some per-key elements MUST be cleared and it is
             * much safer (and quite cheap) to clear them during copyDown()
             * rather than relying on maintenance elsewhere.
             */
            keys[ index ] = NEGINF; // an invalid key.

        }
        
        /**
         * Writes the node on the store. The node MUST be dirty. If the node has
         * a parent, then the parent is notified of the persistent identity
         * assigned to the node by the store.
         * 
         * @return The persistent identity assigned by the store.
         */
        int write() {

            assert isDirty();
            assert !isPersistent();
            
            // write the dirty node on the store.
            btree.store.insert(this);

            // The parent should be defined unless this is the root node.
            Node parent = getParent();
            
            if (parent != null) {

                // parent must be dirty if child is dirty.
                assert parent.isDirty();

                // parent must not be persistent if it is dirty.
                assert !parent.isPersistent();

                /*
                 * Set the persistent identity of the child on the
                 * parent.
                 * 
                 * Note: A parent CAN NOT be serialized before all of
                 * its children have persistent identity since it needs
                 * to write the identity of each child in its
                 * serialization record.
                 */
                parent.setChildRef(this);

            }
         
            return getIdentity();
            
        }
        
    }

    /**
     * Visits the direct children of a node in the external key ordering.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ChildIterator implements Iterator<AbstractNode> {

        private final Node node;

        private int index = 0;

        public ChildIterator(Node node) {

            assert node != null;

            this.node = node;

        }

        public boolean hasNext() {

            return index < node.nkeys;

        }

        public AbstractNode next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            return node.getChild(index++);
        }

        public void remove() {

            throw new UnsupportedOperationException();

        }

    }

    /**
     * Visits the {@link Entry}s of a {@link Leaf} in the external key ordering.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo write test suite for {@link AbstractNode#entryIterator()}.
     */
    static class EntryIterator implements Iterator<Entry> {

        private final Leaf leaf;

        private int index = 0;

        public EntryIterator(Leaf leaf) {

            assert leaf != null;

            this.leaf = leaf;

        }

        public boolean hasNext() {

            return index < leaf.nkeys;

        }

        public Entry next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            return leaf.values[index++];
            
        }

        public void remove() {

            throw new UnsupportedOperationException();

        }

    }

    // /**
    // * Resolve a child index to the child node.
    // *
    // * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
    // Thompson</a>
    // * @version $Id$
    // */
    // static class AbstractNodeResolver extends Resolver {
    //
    // final private Node node;
    //
    // /**
    // *
    // * @param node The parent whose children will be resolved.
    // */
    // public AbstractNodeResolver(Node node) {
    //
    // assert node != null;
    //
    // this.node = node;
    //
    // }
    //
    // /**
    // * Re-skins the generic object as an instance of the class specified to
    // * the constructor.
    // */
    // final protected Object resolve(Object obj) {
    //
    // Integer index = (Integer) obj;
    //
    // return node.getChild(index);
    //            
    // }
    //
    // }

    /**
     * A non-leaf.
     * 
     * Note: We MUST NOT hold hard references to leafs. Leaves account for most
     * of the storage costs of the BTree. We bound those costs by only holding
     * hard references to nodes and not leaves.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Node extends AbstractNode {

        private static final long serialVersionUID = 1L;

        /**
         * Hard reference cache containing dirty child nodes (nodes or leaves).
         */
        transient protected Set<AbstractNode> dirtyChildren = new HashSet<AbstractNode>();

        /**
         * Weak references to child nodes (may be nodes or leaves).
         */
        transient protected WeakReference<AbstractNode>[] childRefs;

        /**
         * The keys of the childKeys nodes (may be nodes or leaves). The key is
         * [null] until the child has been persisted. The protocol for
         * persisting the childKeys requires that we use a pre-order traversal
         * (the general case is a directed graph) so that we can update the keys
         * on the parent before the parent is serialized.
         * 
         * Note: It is an error if there is an attempt to serialize a node
         * having a null entry in this array and a non-null entry in the
         * external {@link #keys} array.
         */
        protected int[] childKeys;

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
         */
        public Node() {

        }

        public Node(BTree btree) {

            super(btree);

            childRefs = new WeakReference[branchingFactor];
            
            childKeys = new int[branchingFactor];

        }

        /**
         * Copy constructor.
         * 
         * @param src
         *            The source node.
         */
        protected Node(Node src) {

            super(src);

            assert isDirty();
            assert ! isPersistent();

            childRefs = new WeakReference[branchingFactor];
            
            childKeys = new int[branchingFactor];

            for (int i = 0; i < nkeys; i++) {

                this.childKeys[i] = src.childKeys[i];

            }

            // Add to the hard reference cache for nodes.
            btree.nodes.add(this);
            
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

            // Scan for location in weak references.
            for (int i = 0; i < nkeys; i++) {

                if (childRefs[i].get() == child) {

                    childKeys[i] = child.getIdentity();

                    if (!dirtyChildren.remove(child)) {

                        throw new AssertionError("Child was not on dirty list.");

                    }

                    return;

                }

            }

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
        void replaceChildRef(int oldChildKey, AbstractNode newChild ) {

            assert oldChildKey != NULL;
            assert newChild != null;
            
            // This node MUST have been cloned as a pre-condition, so it can not
            // be persistent.
            assert ! isPersistent();

            // The newChild MUST have been cloned and therefore MUST NOT be
            // persistent.
            assert ! newChild.isPersistent();
            
            // Scan for location in weak references.
            for (int i = 0; i < nkeys; i++) {

                if (childKeys[i] == oldChildKey ) {

                    if (true) {
                        
                        /*
                         * Do some paranoia checks.
                         */
                        
                        AbstractNode oldChild = childRefs[i] != null ? childRefs[i]
                                .get()
                                : null;

                        if( oldChild != null ) {

                            assert oldChild.isPersistent();
                            
                            assert ! dirtyChildren.contains(oldChild);

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
         * Add a child node or leaf.
         * 
         * @param key
         *            The external key value.
         * @param child
         *            The node or leaf.
         * 
         * @return Either this node or a copy of this node if this node was
         *         persistent and copy-on-write was triggered.
         * 
         * FIXME This does not handle the insertion sort.
         * 
         * FIXME This does not split nodes on overflow.
         * 
         * FIXME This should probably accept the index position rather than the
         * key (or in addition to the key). This will provide symmetry with
         * respect to {@link #getChild(int index)}
         */
        public Node addChild(int key, AbstractNode child) {

            Node copy = (Node) copyOnWrite();

            if (copy != this) {

                return copy.addChild(key, child);

            } else {

                assert key > NEGINF && key < POSINF;
                assert child != null;

                keys[nkeys] = key;

                if (child.isPersistent()) {

                    childRefs[nkeys] = new WeakReference<AbstractNode>(child);
                    childKeys[nkeys] = child.getIdentity();

                } else {

                    dirtyChildren.add(child);
                    child.parent = new WeakReference<Node>(this);
                    childRefs[nkeys] = new WeakReference<AbstractNode>(child);
                    childKeys[nkeys] = 0;

                }

                nkeys++;

                setDirty(true);

                return this;

            }

        }

        /**
         * Return the child node or leaf at the specified index in this node.
         * 
         * @param index
         *            The index in [0:nkeys-1].
         * 
         * @return The child node or leaf.
         */
        public AbstractNode getChild(int index) {

            assert index >= 0 && index < nkeys;

            WeakReference<AbstractNode> childRef = childRefs[index];

            AbstractNode child = null;

            if (childRef != null) {

                child = childRef.get();

            }

            if (child == null) {

                int key = childKeys[index];

                assert key != NULL;

                assert btree != null;

                child = (AbstractNode) btree.store.read(key);

                // patch btree reference since loaded from store.
                child.btree = btree;
                
                // patch parent reference since loaded from store.
                child.parent = new WeakReference<Node>(this);

                // patch the child reference.
                childRefs[index] = new WeakReference<AbstractNode>(child);

                if (child instanceof Node) {

                    /*
                     * Nodes are inserted into this hash set when the are
                     * created. The read above from the store occurs if a node
                     * has not been read into memory yet, in which case we have
                     * to insert it into this hash set so that it will remain
                     * strongly reachable.
                     */
                    btree.nodes.add((Node) child);

                } else {
                    
                    /*
                     * Leaves are added to a hard reference queue. On eviction
                     * from the queue the leaf is serialized. Once the leaf is
                     * no longer strongly reachable its weak references may be
                     * cleared by the VM.
                     */

                    btree.leaves.append((Leaf)child);
                    
                }

            }

            return child;

        }

        protected void copyDown(int index, int count) {

            super.copyDown(index,count);
            
            System.arraycopy( childKeys, index, childKeys, index+1, count );
            System.arraycopy( childRefs, index, childRefs, index+1, count );
            
            childKeys[ index ] = NULL;
            childRefs[ index ] = null;
            
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

            if( dirtyNodesOnly && ! isDirty() ) {
            
                return EmptyIterator.DEFAULT;
                
            }
            
            /*
             * Iterator visits the direct children, expanding them in turn with
             * a recursive application of the post-order iterator.
             */

//            System.err.println("node: " + this);

            return new Striterator(childIterator()).addFilter(new Expander() {
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

        public void writeExternal(ObjectOutput out) throws IOException {
            if (dirtyChildren.size() > 0) {
                throw new IllegalStateException("Dirty children exist.");
            }
            out.writeInt(branchingFactor);
            out.writeInt(nkeys);
            for (int i = 0; i < nkeys; i++) {
                int key = keys[i];
                int childKey = childKeys[i];
                assert key > NEGINF && key < POSINF;
                assert childKey != NULL;
                out.writeInt(key);
                out.writeInt(childKey);
            }
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            branchingFactor = in.readInt();
            nkeys = in.readInt();
            keys = new int[branchingFactor];
            childRefs = new WeakReference[branchingFactor];
            childKeys = new int[branchingFactor];
            for (int i = 0; i < nkeys; i++) {
                int key = in.readInt();
                int childKey = in.readInt();
                assert key > NEGINF && key < POSINF;
                assert childKey != NULL;
                keys[i] = key;
                childKeys[i] = childKey;
            }
        }

    }

    /**
     * An entry in a {@link Leaf}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Reconcile with {@link IObjectIndexEntry} and {@link NodeSerializer}.
     */
    public static class Entry implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * Create a new entry.
         */
        public Entry() {

        }

        /**
         * Copy constructor.
         * 
         * @param src
         *            The source to be copied.
         */
        public Entry(Entry src) {

        }

    }

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
    public static class Leaf extends AbstractNode {

        private static final long serialVersionUID = 1L;
        
        /**
         * The values of the tree.
         */
        protected Entry[] values;

        /**
         * De-serialization constructor.
         */
        public Leaf() {

        }

        public Leaf(BTree btree) {

            super(btree);

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
         * Inserts an entry under an external key. This method is invoked when a
         * {@link #binarySearch(int)} has already revealed that there is no
         * entry for the key in the node.
         * 
         * @param id
         *            The external key.
         * @param index
         *            The index position for the new entry. Data already present
         *            in the leaf beyond this insertion point will be shifted
         *            down by one.
         * @param entry
         *            The new entry.
         * 
         * @todo insert of child on Node.
         * @todo handle node overflow
         * @todo handle leaf overflow
         */
        void insert( int id, int index, Entry entry ) {

            assert id != NULL;
            assert index >=0 && index <= nkeys;
            assert entry != null;
            
            if( nkeys == keys.length ) {
                
                throw new RuntimeException("Overflow");
                
            } else {

                if( index < nkeys ) {
                    
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

                    copyDown( index, count );
                    
                }
                
                /*
                 * Insert at index.
                 */
                keys[ index ] = id; // defined by AbstractNode
                values[ index ] = entry; // defined by Leaf.
                
              }
            
            nkeys++;
            
        }
        
        protected void copyDown(int index, int count) {

            super.copyDown(index, count);

            System.arraycopy( values, index, values, index+1, count );
            
            values[ index ] = null;
            
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
         */
        public Iterator entryIterator() {
         
            if (nkeys == 0)
                return EmptyIterator.DEFAULT;

            return new EntryIterator(this);
            
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

}
