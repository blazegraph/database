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
 * Created on Jan 9, 2007
 */

package com.bigdata.btree;

import org.apache.log4j.Level;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.Leaf;

/**
 * batch api tests for insert, lookup, and remove on the root leaf.
 * 
 * @todo write batch api and tests for indexOf, keyAt, and valueAt.
 * 
 * @todo verify optimization of batch insert, lookup, and remove. large batch
 *       inserts into an empty tree should be done using a special case bulk
 *       loader for ordered data that is (or should be) part of
 *       IndexSegmentBuilder.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestInsertLookupRemoveOnRootLeafWithBatchApi extends
        AbstractBTreeTestCase {

    /**
     * 
     */
    public TestInsertLookupRemoveOnRootLeafWithBatchApi() {
    }

    /**
     * @param name
     */
    public TestInsertLookupRemoveOnRootLeafWithBatchApi(String name) {
        super(name);
    }

    /**
     * @todo write correct rejection tests for lookup() and remove().
     */
    public void test_insert_correctRejection() {
        
        final BTree btree = getBTree(3);

        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v7 = new SimpleEntry(7);
        
        final byte[][] keys = new byte[][] { new byte[] { 3 },
                new byte[] { 5 }, new byte[] { 7 } };

        final Object[] values = new Object[] { v3, v5, v7 };
        
        // invalid keys (null)
        try {
            btree.insert(new BatchInsert(1, null, values));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // invalid keys (length)
        try {
            btree.insert(new BatchInsert(2, new byte[1][], values));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // invalid values (null)
        try {
            btree.insert(new BatchInsert(1, keys, null));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // invalid values (length)
        try {
            btree.insert(new BatchInsert(2, keys, new Object[]{"x"}));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // invalid tuple count (negative)
        try {
            btree.insert(new BatchInsert(-1, keys, values));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // invalid tuple count (zero)
        try {
            btree.insert(new BatchInsert(0, keys, values));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        // tuple count too large.
        try {
            btree.insert(new BatchInsert(4, keys, values));
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

    }
    
    /**
     * Verifies batch insert on an empty root leaf. Once the root leaf has been
     * filled, verifies that batch insert updates the value for some of the
     * keys.
     */
    public void test_insertLookupRemove01() {
    
        final int m = 3;
        final BTree btree = getBTree(m);

        final SimpleEntry v3 = new SimpleEntry(3);
        final SimpleEntry v5 = new SimpleEntry(5);
        final SimpleEntry v7 = new SimpleEntry(7);
        
        final Leaf root = (Leaf) btree.getRoot();
        
        byte[][] keys = new byte[][] { new byte[]{3}, new byte[]{5}, new byte[]{7} };
        Object[] values = new Object[] { v3, v5, v7 };        
        btree.insert(new BatchInsert(3, keys, values));
        assertTrue( root.dump(Level.DEBUG,System.err));

        // check effect on the leaf.
        assertKeys(keys, root);
        assertValues(new Object[]{v3,v5,v7}, root);
        
        // each values is set to null since no pre-existing entry. 
        assertEquals("values", new Object[] { null, null, null }, values );
        
        /*
         * setup another batch insert that will update the tuple for two of the
         * keys.
         */
        
        final Object v5a = new SimpleEntry(500);
        final Object v7a = new SimpleEntry(700);
        keys = new byte[][]{new byte[]{5},new byte[]{7}};
        values = new Object[] { v5a, v7a };
        btree.insert(new BatchInsert(2, keys, values));
        assertTrue( root.dump(Level.DEBUG,System.err) );

        // check effect on the leaf.
        assertKeys(new byte[][] { new byte[] { 3 }, new byte[] { 5 },
                new byte[] { 7 } }, root);
        assertValues(new Object[]{v3,v5a,v7a}, root);

        // verify the value of the existing tuples are returned.
        assertEquals("values", new Object[] { v5, v7 }, values );

        /*
         * lookup all keys
         */
        keys = new byte[][]{new byte[]{3},new byte[]{5}, new byte[]{7}};
        values = new Object[3];
        btree.lookup(new BatchLookup(3,keys,values));
        assertTrue( root.dump(Level.DEBUG,System.err) );
        
        // verify the value of the existing tuples are returned.
        assertEquals("values", new Object[] { v3, v5a, v7a }, values );
        
        /*
         * lookup a subset of the keys. 
         */
        keys = new byte[][]{new byte[]{3},new byte[]{7}};
        values = new Object[2];
        btree.lookup(new BatchLookup(2,keys,values));
        assertTrue( root.dump(Level.DEBUG,System.err) );
        
        // verify the value of the existing tuples are returned.
        assertEquals("values", new Object[] { v3, v7a }, values );
        
        /*
         * lookup a mixture of keys that do exist and keys that do not. 
         */
        keys = new byte[][]{new byte[]{3},new byte[]{4}, new byte[]{7}};
        values = new Object[3];
        btree.lookup(new BatchLookup(3,keys,values));
        assertTrue( root.dump(Level.DEBUG,System.err) );
        
        // verify the value of the existing tuples are returned.
        assertEquals("values", new Object[] { v3, null, v7a }, values );
        
        /*
         * remove a subset of the keys.
         */
        keys = new byte[][]{new byte[]{3},new byte[]{7}};
        values = new Object[2];
        btree.remove(new BatchRemove(2,keys,values));
        assertTrue( root.dump(Level.DEBUG,System.err) );

        // check effect on the leaf.
        assertKeys(new byte[][]{new byte[]{5}}, root);
        assertValues(new Object[]{v5a}, root);

        // verify the value of the existing tuples are returned.
        assertEquals("values", new Object[] { v3, v7a }, values );

        /*
         * remove all keys, including some that no longer have entries.
         */
        keys = new byte[][]{new byte[]{3},new byte[]{5}, new byte[]{7}};
        values = new Object[3];
        btree.remove(new BatchRemove(3,keys,values));
        assertTrue( root.dump(Level.DEBUG,System.err) );

        // check effect on the leaf.
        assertKeys(new byte[][]{}, root);
        assertValues(new Object[]{}, root);

        // verify the value of the existing tuples are returned.
        assertEquals("values", new Object[] { null, v5a, null}, values );

        /*
         * verify that we can insert a key with a null value.
         */
        keys = new byte[][]{new byte[]{4}};
        values = new Object[]{null};
        btree.insert(new BatchInsert(1,keys,values));
        assertTrue( root.dump(Level.DEBUG,System.err) );
        
        // check effect on the leaf.
        assertKeys(new byte[][]{new byte[]{4}}, root);
        assertValues(new Object[]{null}, root);

        // verify the value of the existing tuples are returned.
        assertEquals("values", new Object[] { null }, values );

    }
    
    /**
     * Insert keys {5,6,7,8,3,4,2,1}.
     * 
     * @todo note that the batch api can be expected to build different
     *       structures if the keys are sorted (just as the per-tuple api would
     *       build a different structure if it had sorted keys). when the keys
     *       are presented in the same order, then the same tree should be
     *       built.
     */
    public void test_smallTree01() {
        
        final int m = 3;
        final BTree btree = getBTree(m);

        byte[][] keys = new byte[][] { new byte[] { 5 }, new byte[] { 6 },
                new byte[] { 7 }, new byte[] { 8 }, new byte[] { 3 },
                new byte[] { 4 }, new byte[] { 2 }, new byte[] { 1 } };
        
        SimpleEntry v1 = new SimpleEntry(1);
        SimpleEntry v2 = new SimpleEntry(2);
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v4 = new SimpleEntry(4);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v6 = new SimpleEntry(6);
        SimpleEntry v7 = new SimpleEntry(7);
        SimpleEntry v8 = new SimpleEntry(8);
        Object[] values = new Object[]{v5,v6,v7,v8,v3,v4,v2,v1};

        btree.insert(new BatchInsert(values.length, keys, values));
        
        assertTrue(btree.dump(Level.DEBUG,System.err));

        // @todo verify in more detail.
        assertSameIterator(new Object[] { v1, v2, v3, v4, v5, v6, v7, v8 },
                btree.entryIterator());
                
    }
    
}
