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
 * Created on Nov 27, 2006
 */

package com.bigdata.btree;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Level;

import com.bigdata.io.SerializerUtil;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Test insert, lookup, and value scan for leaves.
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
        
        BTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.root;
        
        // array of inserted keys.
        byte[][] expectedKeys = new byte[branchingFactor][];
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            byte[] key = new byte[3];
            r.nextBytes(key);
            
            int index = root.keys.search(key);
            
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
            
            System.err.println("Will insert: key=" + BytesUtil.toString(key)
                    + " at index=" + index + " : nkeys=" + nkeys);

            // insert an entry under that key.
            btree.insert(key, new SimpleEntry() );
            
            nkeys++;
            
            assertEquals( nkeys, root.nkeys );
            
        }

        // sort the keys that we inserted.
        Arrays.sort(expectedKeys,
                BytesUtil.UnsignedByteArrayComparator.INSTANCE);
        
        assertTrue(root.dump(Level.DEBUG,System.err));
        
        // verify that the leaf has the same keys in the same order.
        assertKeys( expectedKeys, root);
        
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
        
        BTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.root;

        // array of inserted keys.
        byte[][] expectedKeys = new byte[branchingFactor][];
        
        int nkeys = 0;
        
        while( nkeys < branchingFactor ) {
            
            // Valid random key.
            byte[] key = new byte[3];
            r.nextBytes(key);

            int nkeysBefore = root.nkeys;
            
            boolean exists = btree.lookup(key) != null;
            
            btree.insert(key, new SimpleEntry() );

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
        Arrays.sort(expectedKeys,
                BytesUtil.UnsignedByteArrayComparator.INSTANCE);
        
        assertTrue(root.dump(Level.DEBUG,System.err));

        // verify that the leaf has the same keys in the same order.
        assertKeys( expectedKeys, root );
        
    }

    /**
     * Test ability to insert entries into a leaf. Known keys and values are
     * generated and inserted into the leaf in a random order. The sequence of
     * keys and values in the leaf is then compared with the pre-generated
     * sequences known to the unit test. The correct behavior of the
     * {@link Leaf#entryIterator()} is also tested.
     * 
     * @see Leaf#insert(int, com.bigdata.btree.TestBTree.SimpleEntry)
     * @see Leaf#lookup(int)
     * @see Leaf#entryIterator()
     */
    public void test_insertKeyIntoLeaf03() {

        final int branchingFactor = 20;
        
        BTree btree = getBTree(branchingFactor);

        Leaf root = (Leaf) btree.getRoot();

        // array of keys to insert.
        byte[][] expectedKeys = new byte[branchingFactor][];

        // the value to insert for each key.
        SimpleEntry[] expectedValues = new SimpleEntry[branchingFactor];
        
        /*
         * Generate keys and values. The keys are a monotonic progression with
         * random non-zero intervals.
         */
        
        int lastKey = 1;
        
        for( int i=0; i<branchingFactor; i++ ) {
            
            int key = lastKey + r.nextInt(100) + 1;
            
            expectedKeys[ i ] = keyBuilder.reset().append(key).getKey();
            
            expectedValues[ i ] = new SimpleEntry();
            
            lastKey = key; 
            
        }
        
        for( int i=0; i<branchingFactor; i++ ) {

            byte[] key = expectedKeys[i];
            
            SimpleEntry value = expectedValues[i];
            
            assertEquals(i, root.nkeys );
            
            assertNull("Not expecting to find key=" + BytesUtil.toString(key),
                    btree.lookup(key));
            
//            root.dump(System.err);
            
            btree.insert(key, value );
            
//            root.dump(System.err);

            assertEquals("nkeys(i=" + i + " of " + branchingFactor + ")", i + 1,
                    root.nkeys);
            
            assertEquals("value(i=" + i + " of " + branchingFactor + ")",
                    value, btree.lookup(key));
            
            // verify the values iterator
            byte[][] tmp = new byte[root.nkeys][];
            for( int j=0; j<root.nkeys; j++ ) {
                tmp[j] = root.values[j];
            }
            assertSameIterator( "values", tmp, new Striterator(root.entryIterator()).addFilter(new Resolver(){

                @Override
                protected Object resolve(Object arg0) {
                    return ((ITuple)arg0).getValue();
                }
                
            }) ); 
            
        }

        assertTrue(root.dump(Level.DEBUG,System.err));

        // verify that the leaf has the same keys in the same order.
        assertKeys( expectedKeys, root);

        // verify that the leaf has the same values in the same order.
        assertValues( expectedValues, root );
        
        // verify the expected behavior of the iterator.
        assertSameIterator( "values", expectedValues, root.entryIterator() );
        
    }

    /**
     * Test insert, lookup and remove of keys in the root leaf.
     */
    public void test_insertLookupRemoveFromLeaf01() {

        final int m = 4;
        
        final BTree btree = getBTree(m);

        final Leaf root = (Leaf) btree.root;
        
        Object e1 = new SimpleEntry();
        Object e2 = new SimpleEntry();
        Object e3 = new SimpleEntry();
        Object e4 = new SimpleEntry();

        /*
         * fill the root leaf.
         */
        assertNull(btree.insert(KeyBuilder.asSortKey(4), e4));
        assertNull(btree.insert(KeyBuilder.asSortKey(2), e2));
        assertNull(btree.insert(KeyBuilder.asSortKey(1), e1));
        assertNull(btree.insert(KeyBuilder.asSortKey(3), e3));

        /*
         * verify that re-inserting does not split the leaf and returns the old
         * value.
         */
        assertEquals(e4,btree.insert(KeyBuilder.asSortKey(4),e4));
        assertEquals(e2,btree.insert(KeyBuilder.asSortKey(2),e2));
        assertEquals(e1,btree.insert(KeyBuilder.asSortKey(1),e1));
        assertEquals(e3,btree.insert(KeyBuilder.asSortKey(3),e3));
        assertEquals(root,btree.root);
        
        // validate
        assertKeys(new int[]{1,2,3,4},root);
        assertSameIterator(new Object[]{e1,e2,e3,e4}, root.entryIterator());

        // remove (2).
        assertEquals(e2,btree.remove(KeyBuilder.asSortKey(2)));
        assertKeys(new int[]{1,3,4},root);
        assertSameIterator(new Object[]{e1,e3,e4}, root.entryIterator());

        // remove (1).
        assertEquals(e1,btree.remove(KeyBuilder.asSortKey(1)));
        assertKeys(new int[]{3,4},root);
        assertSameIterator(new Object[]{e3,e4}, root.entryIterator());

        // remove (4).
        assertEquals(e4,btree.remove(KeyBuilder.asSortKey(4)));
        assertKeys(new int[]{3},root);
        assertSameIterator(new Object[]{e3}, root.entryIterator());

        // remove (3).
        assertEquals(e3,btree.remove(KeyBuilder.asSortKey(3)));
        assertKeys(new int[]{},root);
        assertSameIterator(new Object[]{}, root.entryIterator());

        assertNull(btree.remove(KeyBuilder.asSortKey(1)));
        assertNull(btree.remove(KeyBuilder.asSortKey(2)));
        assertNull(btree.remove(KeyBuilder.asSortKey(3)));
        assertNull(btree.remove(KeyBuilder.asSortKey(4)));
        
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
            
            final Integer ikey = keys[index];
            
            final byte[] key = KeyBuilder.asSortKey(ikey);
            
            SimpleEntry val = vals[index];
            
            if( insert ) {
                
//                System.err.println("insert("+key+", "+val+")");
                SimpleEntry old = expected.put(ikey, val);
                
                SimpleEntry old2 = (SimpleEntry) SerializerUtil.deserialize(btree.insert(key, val));
                
//                btree.dump(Level.DEBUG,System.err);
                
                assertEquals(old, old2);

                // verify that the root leaf was not split.
                assertEquals("height",0,btree.height);
                assertTrue(btree.root instanceof Leaf);

            } else {
                
//                System.err.println("remove("+key+")");
                SimpleEntry old = expected.remove(ikey);
                
                SimpleEntry old2 = (SimpleEntry) SerializerUtil.deserialize(btree.remove(key));
                
//                btree.dump(Level.DEBUG,System.err);
                
                assertEquals(old, old2);
                
            }

            if( i % 100 == 0 ) {

                /*
                 * Validate the keys and entries.
                 */
                
                assertEquals("#entries", expected.size(), btree.getEntryCount());
                
                Iterator<Map.Entry<Integer,SimpleEntry>> itr = expected.entrySet().iterator();
                
                while( itr.hasNext()) { 
                    
                    Map.Entry<Integer,SimpleEntry> entry = itr.next();
                    
                    final byte[] tmp = KeyBuilder.asSortKey(entry.getKey());
                    
                    assertEquals("lookup(" + entry.getKey() + ")", entry
                            .getValue(), btree
                            .lookup(tmp));
                    
                }
                
            }
            
        }

    }

}
