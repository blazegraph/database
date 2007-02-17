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
 * Created on Feb 13, 2007
 */

package com.bigdata.isolation;

import com.bigdata.objndx.AbstractBTree;
import com.bigdata.objndx.AbstractBTreeTestCase;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IBatchOp;
import com.bigdata.objndx.IRangeQuery;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link UnisolatedBTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUnisolatedBTree extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestUnisolatedBTree() {
    }

    /**
     * @param name
     */
    public TestUnisolatedBTree(String name) {
        super(name);
    }

    public void assertEquals(Value expected, Value actual) {
        
        assertEquals("versionCounter", expected.versionCounter,
                actual.versionCounter);

        assertEquals("deleted", expected.deleted, actual.deleted);

        assertEquals("datum", expected.datum, actual.datum);
        
    }
    
    /**
     * test constructors and re-load of attributes set by constructors.
     */
    public void test_ctor() {
    
        final IRawStore store = new SimpleMemoryRawStore();
        final int branchingFactor = 4;
        IConflictResolver conflictResolver = new NoConflictResolver();
        {
            
            UnisolatedBTree btree = new UnisolatedBTree(store,branchingFactor,null);
            
            assertTrue(store==btree.getStore());
            assertEquals(branchingFactor,btree.getBranchingFactor());
            assertNull(btree.getConflictResolver());

            final long addr = btree.write();
            
            btree = (UnisolatedBTree)BTreeMetadata.load(store, addr);

            assertTrue(store==btree.getStore());
            assertEquals(branchingFactor,btree.getBranchingFactor());
            assertNull(btree.getConflictResolver());

        }
        {
            
            UnisolatedBTree btree = new UnisolatedBTree(store, branchingFactor,
                    conflictResolver);

            assertTrue(store == btree.getStore());
            assertEquals(branchingFactor, btree.getBranchingFactor());
            assertTrue(conflictResolver == btree.getConflictResolver());

            final long addr = btree.write();

            btree = (UnisolatedBTree)BTreeMetadata.load(store, addr);

            assertTrue(store == btree.getStore());
            assertEquals(branchingFactor, btree.getBranchingFactor());
            assertTrue(btree.getConflictResolver() instanceof NoConflictResolver);
            
        }
        
    }

    /**
     * Test on a single key using the point api to verify that versions are
     * tracked correctly on insert and remove operations and that contains and
     * lookup behave as expected.
     */
    public void test_crud_pointApi() {
        
        final byte[] k3= new byte[]{3};
        final byte[] v3a = new byte[]{3};
        final byte[] v3b = new byte[]{3,1};
        final byte[] v3c = new byte[]{3,2};
        
        UnisolatedBTree btree = new UnisolatedBTree(new SimpleMemoryRawStore(),
                3, null);

        /*
         * Preconditions for the key.
         */
        assertFalse(btree.contains(k3));
        assertEquals(null, (byte[]) btree.lookup(k3));

        /*
         * Insert a value under a key.
         */
        assertEquals(null, btree.insert(k3, v3a));
        assertTrue(btree.contains(k3));
        assertEquals(v3a, (byte[]) btree.lookup(k3));
        // note: uses the root node to test the actual value in the tree.
        assertEquals(new Value((short) 1, false, v3a), (Value) btree.getRoot().lookup(k3));

        /*
         * Update the value under the key.
         */
        assertEquals(v3a, btree.insert(k3, v3b));
        assertEquals(v3b, (byte[]) btree.lookup(k3));
        // note: uses the root node to test the actual value in the tree.
        assertEquals(new Value((short) 2, false, v3b), (Value) btree.getRoot().lookup(k3));
        assertTrue(btree.contains(k3));
        
        /*
         * Update the value under the key to a null value.
         */
        assertEquals(v3b, btree.insert(k3, null));
        assertEquals(null, (byte[]) btree.lookup(k3));
        // note: uses the root node to test the actual value in the tree.
        assertEquals(new Value((short) 3, false, null), (Value) btree.getRoot().lookup(k3));
        assertTrue(btree.contains(k3));
        
        /*
         * Remove the value under the key - leaving a deletion marker. the key
         * now reports false for contains and lookup will return null since it
         * does not differentiate for the caller between a null value, a
         * deletion marker, and a missing key.
         */
        assertEquals(null, btree.remove(k3));
        assertEquals(null, (byte[]) btree.lookup(k3));
        // note: uses the root node to test the actual value in the tree.
        assertEquals(new Value((short) 4, true, null), (Value) btree.getRoot().lookup(k3));
        // note: uses the root node to test the actual value in the tree.
        assertTrue(btree.getRoot().contains(k3)); // found by super class
        assertFalse(btree.contains(k3)); // but not by the UnisolatedBTree.
        
        /*
         * Update the value under the deleted key.
         */
        assertEquals(null, btree.insert(k3, v3c));
        assertEquals(v3c, (byte[]) btree.lookup(k3));
        // note: uses the root node to test the actual value in the tree.
        assertEquals(new Value((short) 5, false, v3c), (Value) btree.getRoot().lookup(k3));
        assertTrue(btree.contains(k3));
        
    }

    /**
     * Tests of the methods that operation on entry indices (aka the linear list
     * api).
     */
    public void test_linearListApi() {
        
        final byte[] k3 = new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};
        
        final byte[] v3 = new byte[]{3};
        final byte[] v5 = new byte[]{5};
        final byte[] v7 = new byte[]{7};

        final byte[] v5a = new byte[]{5,1};

        UnisolatedBTree btree = new UnisolatedBTree(new SimpleMemoryRawStore(),
                3, null);
        
        btree.insert(k3,v3);
        btree.insert(k5,v5);
        btree.insert(k7,v7);
        
        assertEquals(0,btree.indexOf(k3));
        assertEquals(1,btree.indexOf(k5));
        assertEquals(2,btree.indexOf(k7));
        assertEquals(k3,btree.keyAt(0));
        assertEquals(k5,btree.keyAt(1));
        assertEquals(k7,btree.keyAt(2));
        assertEquals(v3,btree.valueAt(0));
        assertEquals(v5,btree.valueAt(1));
        assertEquals(v7,btree.valueAt(2));

        btree.remove(k5);

        assertEquals(0,btree.indexOf(k3));
        assertEquals(1,btree.indexOf(k5));
        assertEquals(2,btree.indexOf(k7));
        assertEquals(k3,btree.keyAt(0));
        assertEquals(k5,btree.keyAt(1)); // Note: allowed since it is costly to defeat.
        assertEquals(k7,btree.keyAt(2));
        assertEquals(v3,btree.valueAt(0));
        assertEquals(null,btree.valueAt(1)); // since the key was deleted.
        assertEquals(v7,btree.valueAt(2));
        
        btree.insert(k5,v5a);

        assertEquals(0,btree.indexOf(k3));
        assertEquals(1,btree.indexOf(k5));
        assertEquals(2,btree.indexOf(k7));
        assertEquals(k3,btree.keyAt(0));
        assertEquals(k5,btree.keyAt(1));
        assertEquals(k7,btree.keyAt(2));
        assertEquals(v3,btree.valueAt(0));
        assertEquals(v5a,btree.valueAt(1));
        assertEquals(v7,btree.valueAt(2));

    }

    /**
     * Tests the {@link IRangeQuery} interface and
     * {@link AbstractBTree#entryIterator()}.
     * {@link IRangeQuery#rangeCount(byte[], byte[])} will over estimate if
     * there are deleted entries while the iterators MUST NOT visit deleted
     * entries.
     */
    public void test_rangeQueryApi() {

        final byte[] k3 = new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};
        
        final byte[] v3 = new byte[]{3};
        final byte[] v5 = new byte[]{5};
        final byte[] v7 = new byte[]{7};

        final byte[] v5a = new byte[]{5,1};

        UnisolatedBTree btree = new UnisolatedBTree(new SimpleMemoryRawStore(),
                3, null);
        
        /*
         * fill the root leaf.
         */
        btree.insert(k3,v3);
        btree.insert(k5,v5);
        btree.insert(k7,v7);

        assertEquals(3,btree.getEntryCount());
        assertEquals(3,btree.rangeCount(null, null));
        assertEquals(2,btree.rangeCount(k3, k7));
        assertEquals(1,btree.rangeCount(k3, k5));
        assertEquals(1,btree.rangeCount(null, k5));
        assertEquals(0,btree.rangeCount(null, k3));
        assertEquals(0,btree.rangeCount(k5, k5));
        assertEquals(1,btree.rangeCount(k5, k7));
        assertEquals(2,btree.rangeCount(k5, null));
        assertSameIterator(new Object[]{v3,v5,v7},btree.entryIterator());
        assertSameIterator(new Object[]{v3,v5,v7},btree.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3,v5},btree.rangeIterator(k3,k7));
        assertSameIterator(new Object[]{v3},btree.rangeIterator(k3,k5));
        assertSameIterator(new Object[]{v3},btree.rangeIterator(null,k5));
        assertSameIterator(new Object[]{},btree.rangeIterator(null,k3));
        assertSameIterator(new Object[]{},btree.rangeIterator(k5,k5));
        assertSameIterator(new Object[]{v5},btree.rangeIterator(k5,k7));
        assertSameIterator(new Object[]{v5,v7},btree.rangeIterator(k5,null));

        /*
         * delete one entry. this will not change the rangeCounts, but it does
         * effect the iterators which MUST NOT visit the deleted value.
         */
        btree.remove(k5);

        assertEquals(3,btree.getEntryCount());
        assertEquals(3,btree.rangeCount(null, null));
        assertEquals(2,btree.rangeCount(k3, k7));
        assertEquals(1,btree.rangeCount(k3, k5));
        assertEquals(1,btree.rangeCount(null, k5));
        assertEquals(0,btree.rangeCount(null, k3));
        assertEquals(0,btree.rangeCount(k5, k5));
        assertEquals(1,btree.rangeCount(k5, k7));
        assertEquals(2,btree.rangeCount(k5, null));
        assertSameIterator(new Object[]{v3,v7},btree.entryIterator());
        assertSameIterator(new Object[]{v3,v7},btree.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3},btree.rangeIterator(k3,k7));
        assertSameIterator(new Object[]{v3},btree.rangeIterator(k3,k5));
        assertSameIterator(new Object[]{v3},btree.rangeIterator(null,k5));
        assertSameIterator(new Object[]{},btree.rangeIterator(null,k3));
        assertSameIterator(new Object[]{},btree.rangeIterator(k5,k5));
        assertSameIterator(new Object[]{},btree.rangeIterator(k5,k7));
        assertSameIterator(new Object[]{v7},btree.rangeIterator(k5,null));

        /*
         * re-insert the deleted value.
         */
        btree.insert(k5,v5a);

        assertEquals(3,btree.getEntryCount());
        assertEquals(3,btree.rangeCount(null, null));
        assertEquals(2,btree.rangeCount(k3, k7));
        assertEquals(1,btree.rangeCount(k3, k5));
        assertEquals(1,btree.rangeCount(null, k5));
        assertEquals(0,btree.rangeCount(null, k3));
        assertEquals(0,btree.rangeCount(k5, k5));
        assertEquals(1,btree.rangeCount(k5, k7));
        assertEquals(2,btree.rangeCount(k5, null));
        assertSameIterator(new Object[]{v3,v5a,v7},btree.entryIterator());
        assertSameIterator(new Object[]{v3,v5a,v7},btree.rangeIterator(null,null));
        assertSameIterator(new Object[]{v3,v5a},btree.rangeIterator(k3,k7));
        assertSameIterator(new Object[]{v3},btree.rangeIterator(k3,k5));
        assertSameIterator(new Object[]{v3},btree.rangeIterator(null,k5));
        assertSameIterator(new Object[]{},btree.rangeIterator(null,k3));
        assertSameIterator(new Object[]{},btree.rangeIterator(k5,k5));
        assertSameIterator(new Object[]{v5a},btree.rangeIterator(k5,k7));
        assertSameIterator(new Object[]{v5a,v7},btree.rangeIterator(k5,null));

    }

    /**
     * @todo test the batch apis. all methods must work with {@link Value}s
     *       (the test for this could be a test of the
     *       {@link IBatchOp#apply(com.bigdata.objndx.ISimpleBTree)}
     *       implementations in the btree package since we apply that method in
     *       a trivial manner to support the batch api.
     */
    public void test_crud_batchApi() {
        
        fail("write test");
        
    }

    /**
     * Tests restart-safety of data, including deletion markers.
     */
    public void test_restartSafe() {
        
        IRawStore store = new SimpleMemoryRawStore();

        final byte[] k3 = new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};
        
        final byte[] v3 = new byte[]{3};
        final byte[] v5 = new byte[]{5};
        final byte[] v7 = new byte[]{7};

        final byte[] v5a = new byte[]{5,1};
        final byte[] v7a = new byte[]{7,1};

        UnisolatedBTree btree = new UnisolatedBTree(store, 3, null);
        
        /*
         * fill the root leaf.
         */
        btree.insert(k3,v3);
        btree.insert(k5,v5);
        btree.insert(k7,v7);

        assertSameIterator(new Object[]{v3,v5,v7},btree.entryIterator());

        /*
         * write out the btree and re-load it.
         */
        final long addr1 = btree.write();
        
        btree = (UnisolatedBTree)BTreeMetadata.load(store, addr1);
        
        assertSameIterator(new Object[]{v3,v5,v7},btree.entryIterator());

        /*
         * delete a key, verify, write out the btree, re-load it and re-verify.
         */
        
        btree.remove(k5);
        
        assertSameIterator(new Object[]{v3,v7},btree.entryIterator());

        final long addr2 = btree.write();
        
        btree = (UnisolatedBTree)BTreeMetadata.load(store, addr2);
        
        assertSameIterator(new Object[]{v3,v7},btree.entryIterator());

        /*
         * update a key, verify, write out the btree, re-load it and re-verify. 
         */
        
        btree.insert(k7,v7a);
        
        assertSameIterator(new Object[]{v3,v7a},btree.entryIterator());

        final long addr3 = btree.write();
        
        btree = (UnisolatedBTree)BTreeMetadata.load(store, addr3);
        
        assertSameIterator(new Object[]{v3,v7a},btree.entryIterator());
        
        /*
         * update a deleted key, verify, write out the btree, re-load it and
         * re-verify.
         */
        
        btree.insert(k5,v5a);
        
        assertSameIterator(new Object[]{v3,v5a,v7a},btree.entryIterator());

        final long addr4 = btree.write();
        
        btree = (UnisolatedBTree)BTreeMetadata.load(store, addr4);
        
        assertSameIterator(new Object[]{v3,v5a,v7a},btree.entryIterator());
        
    }
    
    /**
     * Does not resolve any conflicts.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class NoConflictResolver implements IConflictResolver {

        private static final long serialVersionUID = 1L;

        public byte[] resolveConflict(byte[] key, Value comittedValue,
                Value txEntry) throws RuntimeException {

            throw new WriteWriteConflictException();

        }

    }

}
