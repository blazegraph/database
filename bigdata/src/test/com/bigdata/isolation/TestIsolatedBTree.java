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
 * Created on Feb 13, 2007
 */

package com.bigdata.isolation;

import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.journal.TestTx;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link IsolatedBTree}.
 * <p>
 * The tests for this class have more degrees of freedom to cover when compared
 * to {@link TestUnisolatedBTree} since we must also test based on different
 * preconditions in the {@link UnisolatedBTree} to which the
 * {@link IsolatedBTree} reads through.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo review tests of the point apis, especially with respect to
 *       {@link TestTx}. all of the nitty gritty should be first tested in this
 *       suite, including the specific values of the version counters and the
 *       presence / absence of deletion markers, since those data are protected.
 * 
 * @todo test the rangeCount and rangeIterator api. the former will over
 *       estimate if there are deleted entries while the latter must not visit
 *       deleted entries (or may it -- we will need to see them for mergeDown()
 *       and validate())?
 * 
 * @todo write tests of validate() and mergeDown(). note that these are also
 *       tested by {@link TestTx} and friends. However, we can verify the
 *       version counters and delete flags in this package.
 */
public class TestIsolatedBTree extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIsolatedBTree() {
    }

    /**
     * @param name
     */
    public TestIsolatedBTree(String name) {
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
            
            UnisolatedBTree src = new UnisolatedBTree(store, branchingFactor,
                    UUID.randomUUID(), null);

            IsolatedBTree btree = new IsolatedBTree(store,src);
            
            assertTrue(store==btree.getStore());
            assertEquals(branchingFactor,btree.getBranchingFactor());
            assertNull(btree.getConflictResolver());

            final long addr = btree.write();
            
            // special constructor is required.
            btree = (IsolatedBTree) new IsolatedBTree(store, BTreeMetadata
                    .read(store, addr), src);

            assertTrue(store==btree.getStore());
            assertEquals(branchingFactor,btree.getBranchingFactor());
            assertNull(btree.getConflictResolver());

        }
        {
            
            UnisolatedBTree src = new UnisolatedBTree(store, branchingFactor,
                    UUID.randomUUID(), conflictResolver);

            IsolatedBTree btree = new IsolatedBTree(store,src);

            assertTrue(store == btree.getStore());
            assertEquals(branchingFactor, btree.getBranchingFactor());
            assertTrue(conflictResolver == btree.getConflictResolver());

            final long addr = btree.write();

            // special constructor is required.
            btree = (IsolatedBTree) new IsolatedBTree(store, BTreeMetadata
                    .read(store, addr), src);

            assertTrue(store == btree.getStore());
            assertEquals(branchingFactor, btree.getBranchingFactor());
            assertTrue(btree.getConflictResolver() instanceof NoConflictResolver);
            
        }
        
    }

    /**
     * Unit test for {@link IsolatedBTree#contains(byte[])}.
     */
    public void test_contains_lookup_emptyWriteSet() {
        
        final byte[] k3 = new byte[]{3};
        final byte[] k5 = new byte[]{5};
        final byte[] k7 = new byte[]{7};

        final byte[] v3 = new byte[] { 3 };
        final byte[] v3a = new byte[] { 3, 1 };
        
        final byte[] v5 = new byte[] { 5 };

        /*
         * Setup the index that will be isolated.
         */
        UnisolatedBTree src = new UnisolatedBTree(new SimpleMemoryRawStore(),
                3, UUID.randomUUID(), null);

        src.insert(k3, v3);
        src.insert(k3, v3a);
        src.insert(k5, v5);
        src.remove(k5);
        
        assertTrue(src.contains(k3)); // written twice.
        assertFalse(src.contains(k5)); // written and then deleted.
        assertFalse(src.contains(k7)); // never written.
        
        assertEquals(v3a,src.lookup(k3)); // written twice.
        assertNull(src.lookup(k5)); // written and then deleted.
        assertNull(src.lookup(k7)); // never written.

        /*
         * Isolate the index and verify the behavior of contains with an empty
         * write set.
         */
        IsolatedBTree iso = new IsolatedBTree(src.getStore(),src);
        
        assertTrue(iso.contains(k3)); // written twice.
        assertFalse(iso.contains(k5)); // written and then deleted.
        assertFalse(iso.contains(k7)); // never written.

        assertEquals(v3a, iso.lookup(k3)); // written twice.
        assertNull(iso.lookup(k5)); // written and then deleted.
        assertNull(iso.lookup(k7)); // never written.
        
    }
    
//    /**
//     * Test on a single key using the point api to verify that versions are
//     * tracked correctly on insert and remove operations and that contains and
//     * lookup behave as expected.
//     */
//    public void test_crud_pointApi() {
//        
//        final byte[] k3= new byte[]{3};
//        final byte[] v3a = new byte[]{3};
//        final byte[] v3b = new byte[]{3,1};
//        final byte[] v3c = new byte[]{3,2};
//        
//        UnisolatedBTree btree = new UnisolatedBTree(new SimpleMemoryRawStore(),
//                3, null);
//
//        /*
//         * Preconditions for the key.
//         */
//        assertFalse(btree.contains(k3));
//        assertEquals(null, (byte[]) btree.lookup(k3));
//
//        /*
//         * Insert a value under a key.
//         */
//        assertEquals(null, btree.insert(k3, v3a));
//        assertTrue(btree.contains(k3));
//        assertEquals(v3a, (byte[]) btree.lookup(k3));
//        // note: uses the root node to test the actual value in the tree.
//        assertEquals(new Value((short) 1, false, v3a), (Value) btree.getRoot().lookup(k3));
//
//        /*
//         * Update the value under the key.
//         */
//        assertEquals(v3a, btree.insert(k3, v3b));
//        assertEquals(v3b, (byte[]) btree.lookup(k3));
//        // note: uses the root node to test the actual value in the tree.
//        assertEquals(new Value((short) 2, false, v3b), (Value) btree.getRoot().lookup(k3));
//        assertTrue(btree.contains(k3));
//        
//        /*
//         * Update the value under the key to a null value.
//         */
//        assertEquals(v3b, btree.insert(k3, null));
//        assertEquals(null, (byte[]) btree.lookup(k3));
//        // note: uses the root node to test the actual value in the tree.
//        assertEquals(new Value((short) 3, false, null), (Value) btree.getRoot().lookup(k3));
//        assertTrue(btree.contains(k3));
//        
//        /*
//         * Remove the value under the key - leaving a deletion marker. the key
//         * now reports false for contains and lookup will return null since it
//         * does not differentiate for the caller between a null value, a
//         * deletion marker, and a missing key.
//         */
//        assertEquals(null, btree.remove(k3));
//        assertEquals(null, (byte[]) btree.lookup(k3));
//        // note: uses the root node to test the actual value in the tree.
//        assertEquals(new Value((short) 4, true, null), (Value) btree.getRoot().lookup(k3));
//        // note: uses the root node to test the actual value in the tree.
//        assertTrue(btree.getRoot().contains(k3)); // found by super class
//        assertFalse(btree.contains(k3)); // but not by the UnisolatedBTree.
//        
//        /*
//         * Update the value under the deleted key.
//         */
//        assertEquals(null, btree.insert(k3, v3c));
//        assertEquals(v3c, (byte[]) btree.lookup(k3));
//        // note: uses the root node to test the actual value in the tree.
//        assertEquals(new Value((short) 5, false, v3c), (Value) btree.getRoot().lookup(k3));
//        assertTrue(btree.contains(k3));
//        
//    }
//
//    /**
//     * Tests of the methods that operation on entry indices (aka the linear list
//     * api).
//     */
//    public void test_linearListApi() {
//        
//        final byte[] k3 = new byte[]{3};
//        final byte[] k5 = new byte[]{5};
//        final byte[] k7 = new byte[]{7};
//        
//        final byte[] v3 = new byte[]{3};
//        final byte[] v5 = new byte[]{5};
//        final byte[] v7 = new byte[]{7};
//
//        final byte[] v5a = new byte[]{5,1};
//
//        UnisolatedBTree btree = new UnisolatedBTree(new SimpleMemoryRawStore(),
//                3, null);
//        
//        btree.insert(k3,v3);
//        btree.insert(k5,v5);
//        btree.insert(k7,v7);
//        
//        assertEquals(0,btree.indexOf(k3));
//        assertEquals(1,btree.indexOf(k5));
//        assertEquals(2,btree.indexOf(k7));
//        assertEquals(k3,btree.keyAt(0));
//        assertEquals(k5,btree.keyAt(1));
//        assertEquals(k7,btree.keyAt(2));
//        assertEquals(v3,btree.valueAt(0));
//        assertEquals(v5,btree.valueAt(1));
//        assertEquals(v7,btree.valueAt(2));
//
//        btree.remove(k5);
//
//        assertEquals(0,btree.indexOf(k3));
//        assertEquals(1,btree.indexOf(k5));
//        assertEquals(2,btree.indexOf(k7));
//        assertEquals(k3,btree.keyAt(0));
//        assertEquals(k5,btree.keyAt(1)); // Note: allowed since it is costly to defeat.
//        assertEquals(k7,btree.keyAt(2));
//        assertEquals(v3,btree.valueAt(0));
//        assertEquals(null,btree.valueAt(1)); // since the key was deleted.
//        assertEquals(v7,btree.valueAt(2));
//        
//        btree.insert(k5,v5a);
//
//        assertEquals(0,btree.indexOf(k3));
//        assertEquals(1,btree.indexOf(k5));
//        assertEquals(2,btree.indexOf(k7));
//        assertEquals(k3,btree.keyAt(0));
//        assertEquals(k5,btree.keyAt(1));
//        assertEquals(k7,btree.keyAt(2));
//        assertEquals(v3,btree.valueAt(0));
//        assertEquals(v5a,btree.valueAt(1));
//        assertEquals(v7,btree.valueAt(2));
//
//    }
//    
//    public void test_crud_batchApi() {
//        
//        fail("write test");
//    }
//
//    public void test_restartSafe() {
//        
//        fail("write test");
//    }

    /**
     * Unit test for the rangeIterator verifies that we have access to the
     * version counter and delete markers which are required in order to produce
     * a fused view from the resources comprising an index partition.
     */
    public void test_rangeIterator01() {
       
        fail("write tests - Values must be visible to iterator");
        
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
