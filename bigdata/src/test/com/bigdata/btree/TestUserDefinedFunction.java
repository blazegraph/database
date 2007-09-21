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
 * Created on Feb 2, 2007
 */

package com.bigdata.btree;

import java.util.UUID;

/**
 * Test suite for {@link UserDefinedFunction}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUserDefinedFunction extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestUserDefinedFunction() {
    }

    /**
     * @param name
     */
    public TestUserDefinedFunction(String name) {
        super(name);
    }

    /**
     * Test uses a counter to assign one up integer values to keys when they are
     * not found on insert into the btree.
     */
    public void test_counter() {
        
        BTree btree = getBTree(3);
        
        AutoIncCounter counter = new AutoIncCounter();
        
        assertSameIterator(new Object[]{},btree.entryIterator());

        assertEquals(0,((Integer)btree.insert(3, counter)).intValue());
        assertSameIterator(new Object[]{0},btree.entryIterator());

        assertEquals(0,((Integer)btree.insert(3, counter)).intValue());
        assertSameIterator(new Object[]{0},btree.entryIterator());
        
        assertEquals(1,((Integer)btree.insert(5, counter)).intValue());
        assertSameIterator(new Object[]{0,1},btree.entryIterator());

        assertEquals(1,((Integer)btree.insert(5, counter)).intValue());
        assertSameIterator(new Object[]{0,1},btree.entryIterator());

        assertEquals(2,((Integer)btree.insert(7, counter)).intValue());
        assertSameIterator(new Object[]{0,1,2},btree.entryIterator());

        assertEquals(2,((Integer)btree.insert(7, counter)).intValue());
        assertSameIterator(new Object[]{0,1,2},btree.entryIterator());

        assertEquals(3,((Integer)btree.insert(1, counter)).intValue());
        assertSameIterator(new Object[]{3,0,1,2},btree.entryIterator());

    }

    /**
     * Auto-increment counter.
     * <p>
     * 
     * @todo This is not a general purpose auto-increment counter since (a) it
     *       fails to maintain state with the btree; and (b) it does not handle
     *       a partitioned or distributed index. In order to maintain state with
     *       the btree the counter needs to be part of the metadata record for
     *       the btree, which is easy enough. <br>
     *       Supporting a partitioned or distributed index is more challenging.
     *       The most obvious approach is change the goal from a globally
     *       coherent one up counter to a globally unique identifier. One
     *       approach is to simply assign a {@link UUID} which is both stateless and
     *       simple. However, if there are dependent indices then merging must
     *       be used to reconcile the UUIDs assigned in different transactions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AutoIncCounter implements UserDefinedFunction {

        private static final long serialVersionUID = 1L;
        
        private int counter = 0;
        
        private Object retval;
        
        /**
         * If the key is found then we do not update the value.
         */
        public Object found(byte[] key, Object val) {
            
            this.retval = val;
            
            return val;
            
        }

        /**
         * If the key is not found then we insert the current value of the
         * counter and increment the counter.
         */
        public Object notFound(byte[] key) {
            
            retval = Integer.valueOf(counter++);
            
            return retval;
            
        }
        
        /**
         * Return the value that we just set or the old value if we did not
         * update the counter.
         */
        public Object returnValue(byte[] key,Object oldval) {
            
            return retval;
            
        }
        
    }

    /**
     * Test ability to conditionally insert a key.
     *
     * @see ConditionalInsert
     */
    public void test_conditionalInsert() {
        
        BTree btree = getBTree(3);
        
        final byte[] k3 = i2k(3);
        final byte[] k5 = i2k(5);
        final byte[] k7 = i2k(7);
        
        final Object v3 = "v3";
        final Object v3a = "v3a";
        final Object v5 = "v5";
        final Object v7 = "v7";

        /*
         * insert v3 conditionally and verify that the entry is never updated
         * by a conditional insert.
         */
        
        assertEquals(null,btree.lookup(k3));
        assertFalse(btree.contains(k3));

        assertEquals(null,btree.insert(k3,new ConditionalInsert(v3)));
        assertEquals(v3,btree.lookup(k3));
        
        assertEquals(v3,btree.insert(k3,new ConditionalInsert(v3)));
        assertEquals(v3,btree.lookup(k3));
        
        assertEquals(v3,btree.insert(k3,new ConditionalInsert(null)));
        assertEquals(v3,btree.lookup(k3));
        
        assertEquals(v3,btree.insert(k3,new ConditionalInsert(v5)));
        assertEquals(v3,btree.lookup(k3));

        /*
         * verify that the entry is updated by an unconditional insert.
         */
        assertEquals(v3,btree.insert(k3,v3a));
        assertEquals(v3a,btree.lookup(k3));

        /*
         * verify conditional insert of a null value and that the null value
         * is not replaced by subequent conditional inserts.
         */
        assertEquals(null,btree.lookup(k5));
        assertFalse(btree.contains(k5));

        assertEquals(null,btree.insert(k5,new ConditionalInsert(null)));
        assertEquals(null,btree.lookup(k5));
        assertTrue(btree.contains(k5));

        assertEquals(null,btree.insert(k5,new ConditionalInsert(v5)));
        assertEquals(null,btree.lookup(k5));
        assertTrue(btree.contains(k5));

        /*
         * now replace the null value with a non-conditional insert.
         */
        assertEquals(null,btree.insert(k5,v5));
        assertEquals(v5,btree.lookup(k5));
        assertTrue(btree.contains(k5));
        
    }

    /**
     * Test ability to conditionally insert a key with a null value.
     * 
     * @see ConditionalInsertNoValue
     */
    public void test_conditionInsertNoValue() {
        
        fail("write tests");
        
    }
    
}
