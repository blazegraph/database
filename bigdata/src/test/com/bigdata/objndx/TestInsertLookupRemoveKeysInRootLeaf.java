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
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * insert, lookup, and value scan for leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestInsertLookupRemoveKeysInRootLeaf extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestInsertLookupRemoveKeysInRootLeaf() {
    }

    /**
     * @param name
     */
    public TestInsertLookupRemoveKeysInRootLeaf(String name) {
        super(name);
    }

    /**
     * Test ability to insert entries into a leaf. Random (legal) external keys
     * are inserted into a leaf until the leaf would overflow. The random keys
     * are then sorted and compared with the actual keys in the leaf. If the
     * keys were inserted correctly into the leaf then the two arrays of keys
     * will have the same values in the same order.
     */
    public void test_insertKeyIntoLeaf01() {

        final int branchingFactor = 20;
        
        IBTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();
        
        // array of inserted keys.
        int[] expectedKeys = new int[branchingFactor];
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            int key = r.nextInt(IBTree.POSINF-1)+1;
            
            int index = Search.search(key, root.keys, root.nkeys);
            
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
            root.insert(key, index, new SimpleEntry() );
            
            nkeys++;
            
            assertEquals( nkeys, root.nkeys );
            
        }

        // sort the keys that we inserted.
        Arrays.sort(expectedKeys);
        
        // verify that the leaf has the same keys in the same order.
        assertEquals( expectedKeys, root.keys );
        
    }
    
    /**
     * Test ability to insert entries into a leaf. Random (legal) external keys
     * are inserted into a leaf until the leaf would overflow. The random keys
     * are then sorted and compared with the actual keys in the leaf. If the
     * keys were inserted correctly into the leaf then the two arrays of keys
     * will have the same values in the same order. (This is a variation on the
     * previous test that inserts keys into the leaf using a slightly higher
     * level API).
     */
    public void test_insertKeyIntoLeaf02() {

        final int branchingFactor = 20;
        
        IBTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();

        // array of inserted keys.
        int[] expectedKeys = new int[branchingFactor];
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            int key = r.nextInt(IBTree.POSINF-1)+1;

            int nkeysBefore = root.nkeys;
            
            boolean exists = root.lookup(key) != null;
            
            root.insert(key, new SimpleEntry() );

            if( nkeysBefore == root.nkeys ) {
                
                // key already exists.
                assertTrue(exists);
                
                continue;
                
            }
            
            assertFalse(exists);
            
            // save the key.
            expectedKeys[ nkeys ] = key;
            
            nkeys++;
            
            assertEquals( nkeys, root.nkeys );
            
        }

        // sort the keys that we inserted.
        Arrays.sort(expectedKeys);
        
        // verify that the leaf has the same keys in the same order.
        assertEquals( expectedKeys, root.keys );
        
    }

    /**
     * Test ability to insert entries into a leaf. Known keys and values are
     * generated and inserted into the leaf in a random order. The sequence of
     * keys and values in the leaf is then compared with the pre-generated
     * sequences known to the unit test. The correct behavior of the
     * {@link Leaf#entryIterator()} is also tested.
     * 
     * @see Leaf#insert(int, com.bigdata.objndx.TestBTree.SimpleEntry)
     * @see Leaf#lookup(int)
     * @see Leaf#entryIterator()
     */
    public void test_insertKeyIntoLeaf03() {

        final int branchingFactor = 20;
        
        IBTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();

        // array of keys to insert.
        int[] expectedKeys = new int[branchingFactor];

        // the value to insert for each key.
        SimpleEntry[] expectedValues = new SimpleEntry[branchingFactor];
        
        /*
         * Generate keys and values. The keys are a monotonic progression with
         * random non-zero intervals.
         */
        
        int lastKey = IBTree.NEGINF;
        
        for( int i=0; i<branchingFactor; i++ ) {
            
            int key = lastKey + r.nextInt(100) + 1;
            
            expectedKeys[ i ] = key;
            
            expectedValues[ i ] = new SimpleEntry();
            
            lastKey = key; 
            
        }
        
        for( int i=0; i<branchingFactor; i++ ) {

            int key = expectedKeys[i];
            
            SimpleEntry value = expectedValues[i];
            
            assertEquals(i, root.nkeys );
            
            assertNull("Not expecting to find key=" + key, root.lookup(key));
            
//            root.dump(System.err);
            
            root.insert(key, value );
            
//            root.dump(System.err);

            assertEquals("nkeys(i=" + i + " of " + branchingFactor + ")", i + 1,
                    root.nkeys);
            
            assertEquals("value(i=" + i + " of " + branchingFactor + ")",
                    value, root.lookup(key));
            
            // verify the values iterator
            Object[] tmp = new Object[root.nkeys];
            for( int j=0; j<root.nkeys; j++ ) {
                tmp[j] = root.values[j];
            }
            assertSameIterator( "values", tmp, root.entryIterator() ); 
            
        }

        // verify that the leaf has the same keys in the same order.
        assertEquals( "keys", expectedKeys, root.keys );

        // verify that the leaf has the same values in the same order.
        assertEquals( "values", expectedValues, root.values );
        
        // verify the expected behavior of the iterator.
        assertSameIterator( "values", expectedValues, root.entryIterator() );
        
    }

    /**
     * Test insert, lookup and remove of keys in the root leaf.
     */
    public void test_insertLookupRemoveFromLeaf01() {

        final int m = 4;
        
        final IBTree btree = getBTree(m);

        final Leaf root = (Leaf) btree.getRoot();
        
        Object e1 = new SimpleEntry();
        Object e2 = new SimpleEntry();
        Object e3 = new SimpleEntry();
        Object e4 = new SimpleEntry();

        /*
         * fill the root leaf.
         */
        assertNull(root.insert(4, e4));
        assertNull(root.insert(2, e2));
        assertNull(root.insert(1, e1));
        assertNull(root.insert(3, e3));

        /*
         * verify that re-inserting does not split the leaf and returns the old
         * value.
         */
        assertEquals(e4,root.insert(4,e4));
        assertEquals(e2,root.insert(2,e2));
        assertEquals(e1,root.insert(1,e1));
        assertEquals(e3,root.insert(3,e3));
        assertEquals(root,btree.getRoot());
        
        // validate
        assertEquals(4,root.nkeys);
        assertEquals(new int[]{1,2,3,4},root.keys);
        assertSameIterator(new Object[]{e1,e2,e3,e4}, root.entryIterator());

        // remove (2).
        assertEquals(e2,root.remove(2));
        assertEquals(3,root.nkeys);
        assertEquals(new int[]{1,3,4,0},root.keys);
        assertSameIterator(new Object[]{e1,e3,e4}, root.entryIterator());

        // remove (1).
        assertEquals(e1,root.remove(1));
        assertEquals(2,root.nkeys);
        assertEquals(new int[]{3,4,0,0},root.keys);
        assertSameIterator(new Object[]{e3,e4}, root.entryIterator());

        // remove (4).
        assertEquals(e4,root.remove(4));
        assertEquals(1,root.nkeys);
        assertEquals(new int[]{3,0,0,0},root.keys);
        assertSameIterator(new Object[]{e3}, root.entryIterator());

        // remove (3).
        assertEquals(e3,root.remove(3));
        assertEquals(0,root.nkeys);
        assertEquals(new int[]{0,0,0,0},root.keys);
        assertSameIterator(new Object[]{}, root.entryIterator());

        assertNull(root.remove(1));
        assertNull(root.remove(2));
        assertNull(root.remove(3));
        assertNull(root.remove(4));
        
    }
    
    /**
     * Test insert and removal of keys in the root leaf.
     */
    public void test_insertLookupRemoveFromLeaf02() {

        final int m = 4;
        
        final BTree btree = getBTree(m);

        Map<Integer,SimpleEntry> expected = new TreeMap<Integer,SimpleEntry>();
        
        Integer k1 = 1;
        Integer k2 = 2;
        Integer k3 = 3;
        Integer k4 = 4;
        
        SimpleEntry e1 = new SimpleEntry();
        SimpleEntry e2 = new SimpleEntry();
        SimpleEntry e3 = new SimpleEntry();
        SimpleEntry e4 = new SimpleEntry();

        Integer[] keys = new Integer[] {k1,k2,k3,k4};
        SimpleEntry[] vals = new SimpleEntry[]{e1,e2,e3,e4};
        
        for( int i=0; i<1000; i++ ) {
            
            boolean insert = r.nextBoolean();
            
            int index = r.nextInt(m);
            
            Integer key = keys[index];
            
            SimpleEntry val = vals[index];
            
            if( insert ) {
                
//                System.err.println("insert("+key+", "+val+")");
                SimpleEntry old = expected.put(key, val);
                
                SimpleEntry old2 = (SimpleEntry) btree.insert(key.intValue(), val);
                
//                btree.dump(Level.DEBUG,System.err);
                
                assertEquals(old, old2);

                // verify that the root leaf was not split.
                assertEquals("height",0,btree.height);
                assertTrue(btree.root instanceof Leaf);

            } else {
                
//                System.err.println("remove("+key+")");
                SimpleEntry old = expected.remove(key);
                
                SimpleEntry old2 = (SimpleEntry) btree.remove(key.intValue());
                
//                btree.dump(Level.DEBUG,System.err);
                
                assertEquals(old, old2);
                
            }

            if( i % 100 == 0 ) {

                /*
                 * Validate the keys and entries.
                 */
                
                assertEquals("#entries", expected.size(), btree.size());
                
                Iterator<Map.Entry<Integer,SimpleEntry>> itr = expected.entrySet().iterator();
                
                while( itr.hasNext()) { 
                    
                    Map.Entry<Integer,SimpleEntry> entry = itr.next();
                    
                    assertEquals("lookup(" + entry.getKey() + ")", entry
                            .getValue(), btree
                            .lookup(entry.getKey().intValue()));
                    
                }
                
            }
            
        }

    }

}
