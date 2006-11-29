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
 * Created on Nov 27, 2006
 */

package com.bigdata.objndx;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Level;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSplitJoinThreeLevels extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestSplitJoinThreeLevels() {
    }

    /**
     * @param name
     */
    public TestSplitJoinThreeLevels(String name) {
        super(name);
    }

    /**
     * Test ability to split and join a tree of order m == 3 driven by the
     * insertion and then the removal of a known sequence of keys. This test
     * checks the state of the tree after each operation against the expected
     * postconditions for that operation. In particular, testing at m == 3 helps
     * to check for fenceposts in the split/join logic.
     * 
     * Note: a branching factor of three (3) is equivilent to a 2-3 tree, where
     * the minimum #of children (for a node) or values (for a leaf) is two (2)
     * and the maximum #of children (for a node) or values (for a leaf) is three
     * (3). This makes it very easy to provoke splits and joins.
     * 
     * FIXME write tests for split/join of nodes for all of the edge conditions
     * at m == 3, e.g., join(this,rightSibling), join(this,leftSibling), causing
     * merges with the right or left sibling, etc.
     */
    public void test_splitJoinBranchingFactor3() {

        /*
         * Generate keys, values, and visitation order.
         */
        // keys
        final int[] keys = new int[]{5,6,7,8,3,4,2,1};
        // values
        final SimpleEntry[] vals = new SimpleEntry[keys.length];
        // permutation vector for visiting values in key order.
        final int[] order = new int[keys.length];
        // generate values and visitation order.
        {
            for( int i=0; i<keys.length; i++ ) {
                vals[i] = new SimpleEntry(keys[i]);
            }
            System.arraycopy(keys, 0, order, 0, keys.length);
            Arrays.sort(order);
            System.err.println("keys="+Arrays.toString(keys));
            System.err.println("vals="+Arrays.toString(vals));
            System.err.println("order="+Arrays.toString(order));
        }
        
        final int m = 3;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        assertTrue(btree.dump(System.err));

        Leaf a = (Leaf) btree.getRoot();
        assertKeys(new int[]{},a);
        assertValues(new Object[]{},a);
        
        int n = 0;
        
        { // insert(5,5)
            int key = keys[n];
            SimpleEntry val = vals[n++];
            assert key == 5 && val.id() == key;
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5},a);
            assertValues(new Object[]{vals[0]},a);
            assertTrue(btree.dump(System.err));
        }

        { // insert(6,6)
            int key = keys[n];
            SimpleEntry val = vals[n++];
            assert key == 6 && val.id() == key;
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5,6},a);
            assertValues(new Object[]{vals[0],vals[1]},a);
            assertTrue(btree.dump(System.err));
        }
        
        /*
         * fills the root leaf to capacity.
         * 
         * postcondition:
         * 
         * keys: [ 5 6 7 ]
         */
        { // insert(7,7)
            int key = keys[n];
            SimpleEntry val = vals[n++];
            assert key == 7 && val.id() == key;
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            // validate root leaf.
            assertKeys(new int[]{5,6,7},a);
            assertValues(new Object[]{vals[0],vals[1],vals[2]},a);
            assertTrue(btree.dump(System.err));
        }

        /*
         * splits the root leaf
         * 
         * split(a)->(a,b), c is the new root.
         * 
         * postcondition:
         * 
         * c.keys[ 7 - x ]
         * c.clds[ a b - ]
         * 
         * a.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Node c;
        final Leaf b;
        { // insert(8,8)
            int key = keys[n];
            SimpleEntry val = vals[n++];
            assert key == 8 && val.id() == key;
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate new root (c).
            c = (Node)btree.getRoot();
            assertKeys(new int[]{7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.getChild(1));
            assertNull(c.childRefs[2]);
            b = (Leaf)c.getChild(1);
            
            // validate original leaf (a).
            assertKeys(new int[]{5,6},a);
            assertValues(new Object[]{vals[0],vals[1]},a);
            
            // validate new leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{vals[2],vals[3]},b);
            
            assertTrue(btree.dump(System.err));
        }
        
        /*
         * insert(3,3)
         * 
         * postcondition:
         * 
         * c.keys[ 7 - x ]
         * c.clds[ a b - ]
         * 
         * a.keys[ 3 5 6 ]
         * b.keys[ 7 8 - ]
         */
        {
            int key = keys[n];
            SimpleEntry val = vals[n++];
            assert key == 3 && val.id() == key;
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            // validate original leaf (a).
            assertKeys(new int[]{3,5,6},a);
            assertValues(new Object[]{vals[4],vals[0],vals[1]},a);
            
        }

        /*
         * insert(4,4), causing split(a)->(a,d) and bringing (c) to capacity.
         * 
         * postcondition:
         * 
         * c.keys[ 5 7 x ]
         * c.clds[ a d b ]
         * 
         * a.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Leaf d;
        {
            int key = keys[n];
            SimpleEntry val = vals[n++];
            assert key == 4 && val.id() == key;
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate root (c).
            assertKeys(new int[]{5,7},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.childRefs[1]);
            d = (Leaf) c.getChild(1);
            assertEquals(b,c.getChild(2));
            
            // validate original leaf (a).
            assertKeys(new int[]{3,4},a);
            assertValues(new Object[]{vals[4],vals[5]},a);
            
            // validate new leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{vals[0],vals[1]},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{vals[2],vals[3]},b);
            
        }
        
        /*
         * insert(2,2), bringing (a) to capacity again.
         */
        {
            int key = keys[n];
            SimpleEntry val = vals[n++];
            assert key == 2 && val.id() == key;
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));
            
            // validate original leaf (a).
            assertKeys(new int[]{2,3,4},a);
            assertValues(new Object[]{vals[6],vals[4],vals[5]},a);
            
        }
        
        /*
         * insert(1,1) causing (a) to split(a)->(a,e). Since the root (c) is
         * already at capacity this also causes the root to split(c)->(c,f) and
         * creating a new root(g).
         * 
         * postcondition:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d b - ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        final Leaf e;
        final Node f, g;
        {
            
            int key = keys[n];
            SimpleEntry val = vals[n++];
            assert key == 1 && val.id() == key;
            assertNull(btree.remove(key)); // not found / no change.
            assertNull(btree.lookup(key)); // not found.
            assertNull(btree.insert(key,val)); // insert.
            assertEquals(val,btree.lookup(key)); // found.
            assertTrue(btree.dump(Level.DEBUG,System.err));

            // validate the new root(g).
            assertNotSame(c,btree.getRoot());
            g = (Node)btree.getRoot();
            assertKeys(new int[]{5},g);
            assertEquals(c,g.getChild(0));
            assertNotNull(g.childRefs[1]);
            f = (Node) g.getChild(1);
            assertNull(g.childRefs[2]);
            
            // validate old root (c).
            assertKeys(new int[]{3},c);
            assertEquals(a,c.getChild(0));
            assertNotNull(c.childRefs[1]);
            e = (Leaf) c.getChild(1);
            assertNull(c.childRefs[2]);
            
            // validate node(f) split from the old root split(c)->(c,f).
            assertKeys(new int[]{7},f);
            assertEquals(d,f.getChild(0));
            assertEquals(b,f.getChild(1));
            assertNull(f.childRefs[2]);
            
            // validate original leaf (a), which was re-split into (a,e).
            assertKeys(new int[]{1,2},a);
            assertValues(new Object[]{vals[7],vals[6]},a);
            
            // validate new leaf (e).
            assertKeys(new int[]{3,4},e);
            assertValues(new Object[]{vals[4],vals[5]},e);
            
            // validate new leaf (d).
            assertKeys(new int[]{5,6},d);
            assertValues(new Object[]{vals[0],vals[1]},d);
            
            // validate leaf (b).
            assertKeys(new int[]{7,8},b);
            assertValues(new Object[]{vals[2],vals[3]},b);

        }
        
        /*
         * At this point the tree is setup and we start deleting keys. We delete
         * the keys in reverse order and verify that joins correctly reduce the
         * tree as each node or leaf is reduced below its minimum.
         *  
         * postcondition:
         * 
         * g.keys[ 5 - x ]
         * g.clds[ c f - ]
         * 
         * c.keys[ 3 - x ]
         * c.clds[ a e - ]
         * 
         * f.keys[ 7 - x ]
         * f.clds[ d b - ]
         * 
         * a.keys[ 1 2 - ]
         * e.keys[ 3 4 - ]
         * d.keys[ 5 6 - ]
         * b.keys[ 7 8 - ]
         */
        
        fail("Delete keys reducing the tree back to an empty tree.");
        
    }
    
    /**
     * <p>
     * Test ability to split the root leaf. A Btree is created with a known
     * capacity. The root leaf is filled to capacity and then split. The keys
     * are choosen so as to create room for an insert into the left and right
     * leaves after the split. The state of the root leaf before the split is:
     * </p>
     * 
     * <pre>
     *   root keys : [ 1 11 21 31 ]
     * </pre>
     * 
     * <p>
     * The root leaf is split by inserting the external key <code>15</code>.
     * The state of the tree after the split is:
     * </p>
     * 
     * <pre>
     *   m     = 4 (branching factor)
     *   m/2   = 2 (index of first key moved to the new leaf)
     *   m/2-1 = 1 (index of last key retained in the old leaf).
     *              
     *   root  keys : [ 21 ]
     *   leaf1 keys : [  1 11 15  - ]
     *   leaf2 keys : [ 21 31  -  - ]
     * </pre>
     * 
     * <p>
     * The test then inserts <code>2</code> (goes into leaf1, filling it to
     * capacity), <code>22</code> (goes into leaf2, testing the edge condition
     * for inserting the key greater than the split key), and <code>24</code>
     * (goes into leaf2, filling it to capacity). At this point the tree looks
     * like this:
     * </p>
     * 
     * <pre>
     *   root  keys : [ 21 ]
     *   leaf1 keys : [  1  2 11 15 ]
     *   leaf2 keys : [ 21 22 24 31 ]
     * </pre>
     * 
     * <p>
     * The test now inserts <code>7</code>, causing leaf1 into split (note
     * that the leaves are named by their creation order, not their traveral
     * order):
     * </p>
     * 
     * <pre>
     *   root  keys : [ 11 21 ]
     *   leaf1 keys : [  1  2  7  - ]
     *   leaf3 keys : [ 11 15  -  - ]
     *   leaf2 keys : [ 21 22 24 31 ]
     * </pre>
     * 
     * <p>
     * The test now inserts <code>23</code>, causing leaf2 into split:
     * </p>
     * 
     * <pre>
     *   root  keys : [ 11 21 24 ]
     *   leaf1 keys : [  1  2  7  - ]
     *   leaf3 keys : [ 11 15  -  - ]
     *   leaf2 keys : [ 21 22 23  - ]
     *   leaf4 keys : [ 24 31  -  - ]
     * </pre>
     * 
     * <p>
     * At this point the root node is at capacity and another split of a leaf
     * will cause the root node to split and increase the height of the btree.
     * To prepare for this, we insert <4> (into leaf1), <code>17</code> and
     * <code>18</code> (into leaf3), and <code>35</code> and <code>40</code>
     * (into leaf4). This gives us the following scenario.
     * </p>
     * 
     * <pre>
     *   root  keys : [ 11 21 24 ]
     *   leaf1 keys : [  1  2  4  7 ]
     *   leaf3 keys : [ 11 15 17 18 ]
     *   leaf2 keys : [ 21 22 23  - ]
     *   leaf4 keys : [ 24 31 35 40 ]
     * </pre>
     * 
     * <p>
     * Note that leaf2 has a hole that can not be filled by an insert since
     * the key <code>24</code> is already in leaf4.
     * </p>
     * <p>
     * Now we insert <code>50</code> into leaf4, forcing leaf4 to split, which
     * in turn requires the root node to split. The result is as follows (note
     * that the old root is now named 'node1' and 'node2' is the new non-root
     * node created by the split of the old root node).  The split is not made
     * until the insert reaches the leaf, discovers that the key is not already
     * present, and then discovers that the leaf is full.  The key does into
     * the new leaf, leaf5.
     * </p> 
     * <pre>
     *   root  keys : [ 21  -  - ]
     *   node1 keys : [ 11  -  - ]
     *   node2 keys : [ 24 40  - ]
     *   leaf1 keys : [  1  2  4  7 ]
     *   leaf3 keys : [ 11 15 17 18 ]
     *   leaf2 keys : [ 21 22 23  - ]
     *   leaf4 keys : [ 24 31 35  - ]
     *   leaf5 keys : [ 40 50  -  - ]
     * </pre>
     * 
     * @todo Force at least one other leaf to split and verify the outcome.
     *       Ideally, carry through the example until we can force the root
     *       to split one more time.
     */
    public void test_splitJoinBranchingFactor4() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        int[] keys = new int[]{1,11,21,31};
        
        for( int i=0; i<m; i++ ) {
         
            int key = keys[i];
            
            SimpleEntry entry = new SimpleEntry();
            
            assertNull(btree.lookup(key));
            
            btree.insert(key, entry);
            
            assertEquals(entry,btree.lookup(key));
            
        }

        // Verify leaf is full.
        assertEquals( m, leaf1.nkeys );
        
        // Verify keys.
        assertEquals( keys, leaf1.keys );
        
        // Verify root node has not been changed.
        assertEquals( leaf1, btree.getRoot() );

        /*
         * split the root leaf.
         */

        // Insert [key := 15] goes into leaf1 (forces split).
        System.err.print("leaf1 before split : "); leaf1.dump(System.err);
        SimpleEntry splitEntry = new SimpleEntry();
        assertNull(btree.lookup(15));
        btree.insert(15,splitEntry);
        System.err.print("leaf1 after split : "); leaf1.dump(System.err);
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,11,15,0},leaf1.keys);
        assertEquals(splitEntry,btree.lookup(15));

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        /*
         * Verify things about the new root node.
         */
        assertTrue( btree.getRoot() instanceof Node );
        Node root = (Node) btree.getRoot();
        System.err.print("root after split : "); root.dump(System.err);
        assertEquals("root.nkeys",1,root.nkeys);
        assertEquals("root.keys",new int[]{21,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertNotNull(root.getChild(1));
        assertNotSame(leaf1,root.getChild(1));

        /*
         * Verify things about the new leaf node, which we need to access
         * from the new root node.
         */
        Leaf leaf2 = (Leaf)root.getChild(1);
        System.err.print("leaf2 after split : "); leaf2.dump(System.err);
        assertEquals("leaf2.nkeys",m/2,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,31,0,0},leaf2.keys);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());

        // Insert [key := 2] goes into leaf1, filling it to capacity.
        btree.insert(2,new SimpleEntry());
        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,11,15},leaf1.keys);

        // Insert [key := 22] goes into leaf2 (tests edge condition)
        btree.insert(22,new SimpleEntry());
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,31,0},leaf2.keys);

        // Insert [key := 24] goes into leaf2, filling it to capacity.
        btree.insert(24,new SimpleEntry());
        assertEquals("leaf2.nkeys",4,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,24,31},leaf2.keys);

//        System.err.print("root  final : "); root.dump(System.err);
//        System.err.print("leaf1 final : "); leaf1.dump(System.err);
//        System.err.print("leaf2 final : "); leaf2.dump(System.err);
        
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 8, btree.nentries);

        /*
         * Insert into leaf1, causing it to split. The split will cause a new
         * child to be added to the root. Verify the post-conditions.
         */
        
        // Insert [key := 7] goes into leaf1, forcing split.
        assertEquals("leaf1.nkeys",m,leaf1.nkeys);
        System.err.print("root  before split: ");root.dump(System.err);
        System.err.print("leaf1 before split: ");leaf1.dump(System.err);
        btree.insert(7,new SimpleEntry());
        System.err.print("root  after split: ");root.dump(System.err);
        System.err.print("leaf1 after split: ");leaf1.dump(System.err);
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,7,0},leaf1.keys);

        assertEquals("root.nkeys",2,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf2,root.getChild(2));

        Leaf leaf3 = (Leaf)root.getChild(1);
        assertNotNull( leaf3 );
        assertEquals("leaf3.nkeys",2,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,0,0},leaf3.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 3, btree.nleaves);
        assertEquals("#entries", 9, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, root },
                root.postOrderIterator());

        /*
         * Insert into leaf2, causing it to split. The split will cause a new
         * child to be added to the root. At this point the root node is at
         * capacity.  Verify the post-conditions.
         */
        
        // Insert [key := 23] goes into leaf2, forcing split.
        assertEquals("leaf2.nkeys",m,leaf2.nkeys);
        System.err.print("root  before split: ");root.dump(System.err);
        System.err.print("leaf2 before split: ");leaf2.dump(System.err);
        btree.insert(23,new SimpleEntry());
        System.err.print("root  after split: ");root.dump(System.err);
        System.err.print("leaf2 after split: ");leaf2.dump(System.err);
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,23,0},leaf2.keys);

        assertEquals("root.nkeys",3,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,24},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf3,root.getChild(1));
        assertEquals(leaf2,root.getChild(2));

        Leaf leaf4 = (Leaf)root.getChild(3);
        assertNotNull( leaf4 );
        assertEquals("leaf4.nkeys",2,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,0,0},leaf4.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 4, btree.nleaves);
        assertEquals("#entries", 10, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, leaf4,
                root }, root.postOrderIterator());

        /*
         * At this point the root node is at capacity and another split of a
         * leaf will cause the root node to split and increase the height of the
         * btree. We prepare for that scenario now by filling up a few of the
         * leaves to capacity.
         */
        
        // Save a reference to the root node before the split.
        Node node1 = (Node) btree.root;
        assertEquals(root,node1);

        // Insert [key := 4] into leaf1, filling it to capacity.
        btree.insert(4,new SimpleEntry());
        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,4,7},leaf1.keys);

        // Insert [key := 17] into leaf3.
        btree.insert(17,new SimpleEntry());
        assertEquals("leaf3.nkeys",3,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,17,0},leaf3.keys);

        // Insert [key := 18] into leaf3, filling it to capacity.
        btree.insert(18,new SimpleEntry());
        assertEquals("leaf3.nkeys",4,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,17,18},leaf3.keys);

        // Insert [key := 35] into leaf4.
        btree.insert(35,new SimpleEntry());
        assertEquals("leaf4.nkeys",3,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,35,0},leaf4.keys);

        // Insert [key := 40] into leaf4, filling it to capacity.
        btree.insert(40,new SimpleEntry());
        assertEquals("leaf4.nkeys",4,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,35,40},leaf4.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 4, btree.nleaves);
        assertEquals("#entries", 15, btree.nentries);
        
        /*
         * verify iterator (no change).
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, leaf4,
                root }, root.postOrderIterator());

        /*
         * Force leaf4 to split.
         */
        
        System.err.print("Tree pre-split");
        assertTrue(btree.dump(System.err));
        
        // Insert [key := 50] into leaf4, forcing it to split.  The insert
        // goes into the _new_ leaf.
        leaf4.dump(System.err);
        leaf4.getParent().dump(System.err);
        btree.insert(50,new SimpleEntry());
        leaf4.dump(System.err);
        leaf4.getParent().dump(System.err);

        /*
         * verify keys the entire tree, starting at the new root.
         */

        System.err.print("Tree post-split");
        assertTrue(btree.dump(System.err));
        
        assertNotSame(root,btree.root); // verify new root.
        root = (Node)btree.root;
        assertEquals("root.nkeys",1,root.nkeys);
        assertEquals("root.keys",new int[]{21,0,0},root.keys);
        assertEquals(node1,root.getChild(0));
        assertNotNull(root.getChild(1));
        Node node2 = (Node)root.getChild(1);
        
        assertEquals("node1.nkeys",1,node1.nkeys);
        assertEquals("node1.keys",new int[]{11,0,0},node1.keys);
        assertEquals(root,node1.getParent());
        assertEquals(leaf1,node1.getChild(0));
        assertEquals(leaf3,node1.getChild(1));

        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,4,7},leaf1.keys);
        assertEquals(node1,leaf1.getParent());

        assertEquals("leaf3.nkeys",4,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,17,18},leaf3.keys);
        assertEquals(node1,leaf3.getParent());
        
        assertEquals("node2.nkeys",2,node2.nkeys);
        assertEquals("node2.keys",new int[]{24,40,0},node2.keys);
        assertEquals(root,node2.getParent());
        assertEquals(leaf2,node2.getChild(0));
        assertEquals(leaf4,node2.getChild(1));
        assertNotNull(node2.getChild(2));
        Leaf leaf5 = (Leaf)node2.getChild(2);

        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,23,0},leaf2.keys);
        assertEquals(node2,leaf2.getParent());

        assertEquals("leaf4.nkeys",3,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,35,0},leaf4.keys);
        assertEquals(node2,leaf4.getParent());
        
        assertEquals("leaf5.nkeys",2,leaf5.nkeys);
        assertEquals("leaf5.keys",new int[]{40,50,0,0},leaf5.keys);
        assertEquals(node2,leaf5.getParent());

        assertEquals("height", 2, btree.height);
        assertEquals("#nodes", 3, btree.nnodes);
        assertEquals("#leaves", 5, btree.nleaves);
        assertEquals("#entries", 16, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, node1, leaf2, 
                leaf4, leaf5, node2, root }, root.postOrderIterator());
     
        fail("Delete keys reducing the tree back to an empty tree.");

    }
    
    /**
     * <p>
     * Test ability to remove keys and to remove child leafs as they become
     * empty. A Btree is created with a known capacity. The root leaf is filled
     * to capacity and then split. The keys are choosen so as to create room for
     * an insert into the left and right leaves after the split. The state of
     * the root leaf before the split is:
     * </p>
     * 
     * <pre>
     *    root keys : [ 1 11 21 31 ]
     * </pre>
     * 
     * <p>
     * The root leaf is split by inserting the external key <code>15</code>.
     * The state of the tree after the split is:
     * </p>
     * 
     * <pre>
     *    m     = 4 (branching factor)
     *    m/2   = 2 (index of first key moved to the new leaf)
     *    m/2-1 = 1 (index of last key retained in the old leaf).
     *               
     *    root  keys : [ 21 ]
     *    leaf1 keys : [  1 11 15  - ]
     *    leaf2 keys : [ 21 31  -  - ]
     * </pre>
     * 
     * <p>
     * The test then inserts <code>2</code> (goes into leaf1, filling it to
     * capacity), <code>22</code> (goes into leaf2, testing the edge condition
     * for inserting the key greater than the split key), and <code>24</code>
     * (goes into leaf2, filling it to capacity). At this point the tree looks
     * like this:
     * </p>
     * 
     * <pre>
     *    root  keys : [ 21 ]
     *    leaf1 keys : [  1  2 11 15 ]
     *    leaf2 keys : [ 21 22 24 31 ]
     * </pre>
     * 
     * <p>
     * The test now inserts <code>7</code>, causing leaf1 into split (note
     * that the leaves are named by their creation order, not their traveral
     * order):
     * </p>
     * 
     * <pre>
     *    root  keys : [ 11 21 ]
     *    leaf1 keys : [  1  2  7  - ]
     *    leaf3 keys : [ 11 15  -  - ]
     *    leaf2 keys : [ 21 22 24 31 ]
     * </pre>
     * 
     * <p>
     * The test now inserts <code>23</code>, causing leaf2 into split:
     * </p>
     * 
     * <pre>
     *    root  keys : [ 11 21 24 ]
     *    leaf1 keys : [  1  2  7  - ]
     *    leaf3 keys : [ 11 15  -  - ]
     *    leaf2 keys : [ 21 22 23  - ]
     *    leaf4 keys : [ 24 31  -  - ]
     * </pre>
     * 
     * <p>
     * At this point the root node is at capacity and we are ready to begin
     * deleting keys. We begin by deleting <code>11</code> and then
     * <code>15</code>. This reduces leaf3 to zero keys, at which point the
     * leaf should be removed from the root node and deallocated on the store:
     * </p>
     * 
     * <pre>
     *    root  keys : [ 21 24 0 ]
     *    leaf1 keys : [  1  2  7  - ]
     *    leaf2 keys : [ 21 22 23  - ]
     *    leaf4 keys : [ 24 31  -  - ]
     * </pre>
     * 
     * @todo reconcile with {@link #test_splitJoinBranchingFactor4()}, which
     * builds the same tree.  Just combine the tests and let the combined test
     * work backwards until the tree is empty again.
     */
    public void test_removeStructure01() {

        final int m = 4;

        BTree btree = getBTree(m);

        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        
        Leaf leaf1 = (Leaf) btree.getRoot();
        
        /*
         * generate keys and values.
         */
        
        // keys
        int[] keys = new int[]{1,11,21,31,15,2,22,24,7,23};

        // values
        SimpleEntry[] vals = new SimpleEntry[keys.length];

        // key value mapping used once we begin to delete keys.
        Map<Integer,SimpleEntry> map = new TreeMap<Integer,SimpleEntry>();

        // generate values and populate the key-value mapping.
        for( int i=0; i<vals.length; i++ ) {
         
            vals[i] = new SimpleEntry();
            
            map.put(keys[i], vals[i]);
            
        }

        /*
         * Populate the root leaf.
         */
        int n = 0;
        btree.insert(keys[n], vals[n]); n++;
        btree.insert(keys[n], vals[n]); n++;
        btree.insert(keys[n], vals[n]); n++;
        btree.insert(keys[n], vals[n]); n++;

        // Verify leaf is full.
        assertEquals( m, leaf1.nkeys );
        
        // Verify keys.
        assertEquals( new int[]{1,11,21,31}, leaf1.keys );
        
        // Verify root node has not been changed.
        assertEquals( leaf1, btree.getRoot() );

        /*
         * split the root leaf.
         */

        // Insert [key := 15] goes into leaf1 (forces split).
        System.err.print("leaf1 before split : "); leaf1.dump(System.err);
        SimpleEntry splitEntry = vals[n];
        assertNull(btree.lookup(15));
        assert keys[n] == 15;
        btree.insert(keys[n],splitEntry); n++;
        System.err.print("leaf1 after split : "); leaf1.dump(System.err);
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,11,15,0},leaf1.keys);
        assertEquals(splitEntry,btree.lookup(15));

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);

        /*
         * Verify things about the new root node.
         */
        assertTrue( btree.getRoot() instanceof Node );
        Node root = (Node) btree.getRoot();
        System.err.print("root after split : "); root.dump(System.err);
        assertEquals("root.nkeys",1,root.nkeys);
        assertEquals("root.keys",new int[]{21,0,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertNotNull(root.getChild(1));
        assertNotSame(leaf1,root.getChild(1));

        /*
         * Verify things about the new leaf node, which we need to access
         * from the new root node.
         */
        Leaf leaf2 = (Leaf)root.getChild(1);
        System.err.print("leaf2 after split : "); leaf2.dump(System.err);
        assertEquals("leaf2.nkeys",m/2,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,31,0,0},leaf2.keys);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf2, root }, root
                .postOrderIterator());
    
        // Insert [key := 2] goes into leaf1, filling it to capacity.
        assert keys[n] == 2;
        btree.insert(keys[n],vals[n]); n++;
        assertEquals("leaf1.nkeys",4,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,11,15},leaf1.keys);

        // Insert [key := 22] goes into leaf2 (tests edge condition)
        assert keys[n] == 22;
        btree.insert(keys[n],vals[n]); n++;
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,31,0},leaf2.keys);

        // Insert [key := 24] goes into leaf2, filling it to capacity.
        assert keys[n] == 24;
        btree.insert(keys[n],vals[n]); n++;
        assertEquals("leaf2.nkeys",4,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,24,31},leaf2.keys);

//        System.err.print("root  final : "); root.dump(System.err);
//        System.err.print("leaf1 final : "); leaf1.dump(System.err);
//        System.err.print("leaf2 final : "); leaf2.dump(System.err);
        
        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 8, btree.nentries);

        /*
         * Insert into leaf1, causing it to split. The split will cause a new
         * child to be added to the root. Verify the post-conditions.
         */
        
        // Insert [key := 7] goes into leaf1, forcing split.
        assertEquals("leaf1.nkeys",m,leaf1.nkeys);
        System.err.print("root  before split: ");root.dump(System.err);
        System.err.print("leaf1 before split: ");leaf1.dump(System.err);
        assert keys[n] == 7;
        btree.insert(keys[n],vals[n]); n++;
        System.err.print("root  after split: ");root.dump(System.err);
        System.err.print("leaf1 after split: ");leaf1.dump(System.err);
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,7,0},leaf1.keys);

        assertEquals("root.nkeys",2,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf2,root.getChild(2));

        Leaf leaf3 = (Leaf)root.getChild(1);
        assertNotNull( leaf3 );
        assertEquals("leaf3.nkeys",2,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,0,0},leaf3.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 3, btree.nleaves);
        assertEquals("#entries", 9, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, root },
                root.postOrderIterator());

        /*
         * Insert into leaf2, causing it to split. The split will cause a new
         * child to be added to the root. At this point the root node is at
         * capacity.  Verify the post-conditions.
         */
        
        // Insert [key := 23] goes into leaf2, forcing split.
        assertEquals("leaf2.nkeys",m,leaf2.nkeys);
        System.err.print("root  before split: ");root.dump(System.err);
        System.err.print("leaf2 before split: ");leaf2.dump(System.err);
        assert keys[n] == 23;
        btree.insert(keys[n],vals[n]); n++;
        System.err.print("root  after split: ");root.dump(System.err);
        System.err.print("leaf2 after split: ");leaf2.dump(System.err);
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,23,0},leaf2.keys);

        assertEquals("root.nkeys",3,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,24},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf3,root.getChild(1));
        assertEquals(leaf2,root.getChild(2));

        Leaf leaf4 = (Leaf)root.getChild(3);
        assertNotNull( leaf4 );
        assertEquals("leaf4.nkeys",2,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,0,0},leaf4.keys);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 4, btree.nleaves);
        assertEquals("#entries", 10, btree.nentries);

        /*
         * verify iterator.
         */
        assertSameIterator(new AbstractNode[] { leaf1, leaf3, leaf2, leaf4,
                root }, root.postOrderIterator());

        assertTrue(btree.dump(Level.DEBUG,System.out));

        /*
         * Validate the keys and entries.
         */
        {

            assertEquals("#entries", keys.length, btree.size());

            for (int i = 0; i < keys.length; i++) {

                int key = keys[i];

                SimpleEntry val = vals[i];

                assertEquals("lookup(" + key + ")", val,
                        btree.lookup(key));

            }

        }

        /*
         * Begin deleting keys:
         * 
         * before:         v-- remove @ index = 1
         *       index:    0  1  2
         * root  keys : [ 11 21 24 ]
         *                              index
         * leaf1 keys : [  1  2  7  - ]   0
         * leaf3 keys : [ 11 15  -  - ]   1 <--remove @ index = 1
         * leaf2 keys : [ 21 22 23  - ]   2
         * leaf4 keys : [ 24 31  -  - ]   3
         * 
         * This can also be represented as
         * 
         * ( leaf1, 11, leaf3, 21, leaf2, 24, leaf4 )
         * 
         * and we remove the sequence ( leaf3, 21 ) leaving a well-formed node.
         * 
         * removeChild(leaf3) - occurs when we delete key 15 and then key 11.
         * nkeys := 3
         * nchildren := nkeys(3) + 1 = 4
         * index := 1
         * lengthChildCopy = nchildren(4) - index(1) - 1 = 2;
         * lengthKeyCopy = lengthChildCopy - 1 = 1;
         * copyChildren from index+1(2) to index(1) lengthChildCopy(2)
         * copyKeys from index+1(2) to index(1) lengthKeyCopy(1)
         * erase keys[ nkeys - 1 = 2 ]
         * erase children[ nkeys = 3 ]
         * 
         * after:
         *       index:    0  1  2
         * root  keys : [ 11 24  0 ]
         *                              index 
         * leaf1 keys : [  1  2  7  - ]   0
         * leaf2 keys : [ 21 22 23  - ]   1
         * leaf4 keys : [ 24 31  -  - ]   2
         */

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 4, btree.nleaves);
        assertEquals("#entries", 10, btree.nentries);
        assertTrue(btree.dump(System.out));

        // validate pre-conditions for the root.
        assertEquals("root.nkeys",3,root.nkeys);
        assertEquals("root.keys",new int[]{11,21,24},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf3,root.getChild(1));
        assertEquals(leaf2,root.getChild(2));
        assertEquals(leaf4,root.getChild(3));

        // verify the pre-condition for leaf3.
        assertEquals("leaf3.nkeys",2,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,15,0,0},leaf3.keys);
        
        // delete 15
        assertEquals(map.get(15),btree.remove(15));
        assertEquals("leaf3.nkeys",1,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{11,0,0,0},leaf3.keys);
        
        // delete 11, causes leaf3 to be removed from the root node and deleted.
        assertEquals(map.get(11),btree.remove(11));
        assertEquals("leaf3.nkeys",0,leaf3.nkeys);
        assertEquals("leaf3.keys",new int[]{0,0,0,0},leaf3.keys);
        assertTrue("leaf3.isDeleted", leaf3.isDeleted());
        // verify gone from the root node. 
        assertEquals("root.nkeys",2,root.nkeys);
        assertEquals("root.keys",new int[]{11,24,0},root.keys);
        assertEquals(leaf1,root.getChild(0));
        assertEquals(leaf2,root.getChild(1));
        assertEquals(leaf4,root.getChild(2));
        assertNull(root.childRefs[3]);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 3, btree.nleaves);
        assertEquals("#entries", 8, btree.nentries);
        assertTrue(btree.dump(System.out));

        /*
         * delete keys 1, 2, and 7 causing leaf1 to be deleted and the key 21
         * to be removed from the root node.
         * 
         * before: 
         * root  keys : [ 11 24  0 ]
         * leaf1 keys : [  1  2  7  - ]
         * leaf2 keys : [ 21 22 23  - ]
         * leaf4 keys : [ 24 31  -  - ]
         *    
         * after:
         * root  keys : [ 24  0  0]
         * leaf2 keys : [ 21 22 23  - ]
         * leaf4 keys : [ 24 31  -  - ]
         */
        // verify leaf1 preconditions.
        assertEquals("leaf1.nkeys",3,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{1,2,7,0},leaf1.keys);
        // delete 1 and verify postconditions.
        assertEquals(map.get(1),btree.remove(1));
        assertEquals("leaf1.nkeys",2,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{2,7,0,0},leaf1.keys);
        // delete 2 and verify postconditions.
        assertEquals(map.get(2),btree.remove(2));
        assertEquals("leaf1.nkeys",1,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{7,0,0,0},leaf1.keys);
        // delete 7 and verify postconditions (this causes leaf1 to be removed)
        assertEquals(map.get(7),btree.remove(7));
        assertEquals("leaf1.nkeys",0,leaf1.nkeys);
        assertEquals("leaf1.keys",new int[]{0,0,0,0},leaf1.keys);
        assertTrue("leaf1.isDeleted", leaf1.isDeleted());
        // verify gone from the root node. 
        assertEquals("root.nkeys",1,root.nkeys);
        assertEquals("root.keys",new int[]{24,0,0},root.keys);
        assertEquals(leaf2,root.getChild(0));
        assertEquals(leaf4,root.getChild(1));
        assertNull(root.childRefs[2]);
        assertNull(root.childRefs[3]);

        assertEquals("height", 1, btree.height);
        assertEquals("#nodes", 1, btree.nnodes);
        assertEquals("#leaves", 2, btree.nleaves);
        assertEquals("#entries", 5, btree.nentries);
        assertTrue(btree.dump(System.out));

        /*
         * Delete keys 31, and 24 causing leaf4 to be deleted and the key 24 to
         * be removed from the root node. At this point we have only a single
         * child leaf in the root node. During a split a new node will
         * temporarily have no keys and only a single child, but we do not
         * permit that state to persist in the tree. Therefore we replace the
         * root node with leaf2 and deallocate the root node. leaf2 now becomes
         * the new root leaf of the tree.
         * 
         * before: 
         * root  keys : [ 24  0  0]
         * leaf2 keys : [ 21 22 23  - ]
         * leaf4 keys : [ 24 31  -  - ]
         * 
         * intermediate state before the root node is replaced by leaf2:
         * root  keys : [  0  0  0]
         * leaf2 keys : [ 21 22 23  - ]
         * 
         * after: leaf2 is now the root leaf.
         * leaf2 keys : [ 21 22 23  - ]
         */
        // verify leaf4 preconditions.
        assertEquals("leaf4.nkeys",2,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{24,31,0,0},leaf4.keys);
        // delete 24 and verify postconditions.
        assertEquals(map.get(24),btree.remove(24));
        assertEquals("leaf4.nkeys",1,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{31,0,0,0},leaf4.keys);
        // delete 31 and verify postconditions.
        assertEquals(map.get(31),btree.remove(31));
        assertEquals("leaf4.nkeys",0,leaf4.nkeys);
        assertEquals("leaf4.keys",new int[]{0,0,0,0},leaf4.keys);
        assertTrue("leaf4.isDeleted", leaf4.isDeleted());
        // verify gone from the root node. 
        assertEquals("root.nkeys",0,root.nkeys);
        assertEquals("root.keys",new int[]{0,0,0},root.keys);
        assertEquals(leaf2,root.getChild(0)); // @todo is this reference cleared?
        assertNull(root.childRefs[1]);
        assertNull(root.childRefs[2]);
        assertNull(root.childRefs[3]);
        assertTrue(root.isDeleted());
        assertEquals(leaf2,btree.root);
        assertFalse(leaf2.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 3, btree.nentries);
        assertTrue(btree.dump(System.out));
        
        /*
         * Delete all keys remaining in the root leaf and verify that the root
         * leaf is allowed to become empty.
         * 
         * before:
         * leaf2 keys : [ 21 22 23  - ]
         * 
         * after:
         * leaf2 keys : [  -  -  -  - ]
         */

        // preconditions.
        assertEquals("leaf2.nkeys",3,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{21,22,23,0},leaf2.keys);
        // delete 21 and verify postconditions.
        assertEquals(map.get(21),btree.remove(21));
        assertEquals("leaf2.nkeys",2,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{22,23,0,0},leaf2.keys);
        // delete 23 and verify postconditions.
        assertEquals(map.get(23),btree.remove(23));
        assertEquals("leaf2.nkeys",1,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{22,0,0,0},leaf2.keys);
        // delete 22 and verify postconditions.
        assertEquals(map.get(22),btree.remove(22));
        assertEquals("leaf2.nkeys",0,leaf2.nkeys);
        assertEquals("leaf2.keys",new int[]{0,0,0,0},leaf2.keys);
        assertFalse(leaf2.isDeleted());
        
        assertEquals("height", 0, btree.height);
        assertEquals("#nodes", 0, btree.nnodes);
        assertEquals("#leaves", 1, btree.nleaves);
        assertEquals("#entries", 0, btree.nentries);
        assertTrue(btree.dump(System.out));
        
    }

}
