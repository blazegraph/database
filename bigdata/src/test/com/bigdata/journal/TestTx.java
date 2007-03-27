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
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.isolation.IsolatedBTree;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.objndx.IIndex;

/**
 * Test suite for fully-isolated read-write transactions.
 * 
 * @todo verify with writes on multiple indices.
 * 
 * @todo Show that abort does not leave anything lying around, both that would
 *       break isolation (unlikely) or just junk that lies around unreclaimed on
 *       the slots (or in the index nodes themselves).
 * 
 * @todo Verify correct abort after 'prepare'.
 * 
 * @todo Issue warning or throw exception when closing journal with active
 *       transactions? Provide a 'force' and timeout option? This was all
 *       implemented for the DBCache implementation so the code and tests can
 *       just be migrated.  See {@link Journal#shutdown()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTx extends ProxyTestCase {
    
    public TestTx() {
    }

    public TestTx(String name) {
        super(name);
    }

    /**
     * Test verifies that a transaction may start when there are (a) no commits
     * on the journal; and (b) no indices have been registered.
     * 
     * @todo In the current implementation the transaction will be unable to
     *       isolate an index if the index has not been registered already by an
     *       unisolated transaction.
     */
    public void test_noIndicesRegistered() {

        Journal journal = new Journal(getProperties());

        journal.commit();
        
        final long tx = journal.newTx(IsolationEnum.ReadWrite);
        
        /*
         * nothing written on this transaction.
         */
        
        // commit.
        assertTrue(journal.commit(tx)!=0L);

        journal.closeAndDelete();
        
    }

    /**
     * Verify that an index is not visible in the tx until the native
     * transaction in which it is registered has already committed before
     * the tx starts. 
     */
    public void test_indexNotVisibleUnlessCommitted() {
       
        Journal journal = new Journal(getProperties());

        String name = "abc";
        
        // register index in unisolated scope, but do not commit yet.
        journal.registerIndex(name, new UnisolatedBTree(journal, 3, UUID
                .randomUUID()));
        
        // start tx1.
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        // the index is not visible in tx1.
        assertNull(journal.getIndex(name,tx1));
        
        // do unisolated commit.
        assertTrue(journal.commit()!=0L);
        
        // start tx2.
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);

        // the index still is not visible in tx1.
        assertNull(journal.getIndex(name,tx1));

        // the index is visible in tx2.
        assertNotNull(journal.getIndex(name,tx2));
        
        journal.abort(tx1);
        
        journal.abort(tx2);
        
        journal.closeAndDelete();
        
    }

    /**
     * Test verifies that you always get the same object back when you ask for
     * an isolated named index.  This is important both to conserve resources
     * and since the write set is in the isolated index -- you lose it and it
     * is gone.
     */
    public void test_sameIndexObject() {

        Journal journal = new Journal(getProperties());
        
        final String name = "abc";

        {
            
            journal.registerIndex(name, new UnisolatedBTree(journal,UUID.randomUUID()));
            
            journal.commit();
            
        }

        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        final IIndex ndx1 = journal.getIndex(name,tx1);
        
        assertNotNull(ndx1);
        
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);

        final IIndex ndx2 = journal.getIndex(name,tx2);

        assertTrue(tx1 != tx2);

        assertTrue(ndx1 != ndx2);
        
        assertNotNull(ndx2);
        
        assertTrue( ndx1 == journal.getIndex(name,tx1));

        assertTrue( ndx2 == journal.getIndex(name,tx2));
        
        journal.closeAndDelete();
        
    }
    
    /**
     * Create a journal, setup an index, write an entry on that index, and
     * commit the store. Setup a transaction and verify that we can isolated
     * that index and read the written value. Write a value on the unisolated
     * index and verify that it is not visible within the transaction.
     */
    public void test_readIsolation() {
        
        Journal journal = new Journal(getProperties());
        
        final String name = "abc";
        
        final int branchingFactor = 3;
        
        final byte[] k1 = new byte[]{1};
        final byte[] k2 = new byte[]{2};

        final byte[] v1 = new byte[]{1};
        final byte[] v2 = new byte[]{2};
        
        {

            /*
             * register the index, write an entry on the unisolated index,
             * and commit the journal. 
             */
            
            IIndex index = journal.registerIndex(name, new UnisolatedBTree(
                    journal, branchingFactor, UUID.randomUUID()));
        
            assertNull(index.insert(k1, v1));
            
            assert(journal.commit()!=0L);
            
        }
        
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);
    
        {

            /*
             * verify that the write is visible in a transaction that starts
             * after the commit.
             */
            
            IIndex index  = journal.getIndex(name,tx1);
            
            assertTrue(index.contains(k1));
            
            assertEquals(v1,(byte[])index.lookup(k1));
            
        }

        {
    
            /*
             * obtain the unisolated index and write another entry and commit
             * the journal. 
             */
            
            IIndex index  = journal.getIndex(name);
            
            assertNull(index.insert(k2, v2));
            
            assertTrue(journal.commit()!=0L);
            
        }
        
        {
            
            /*
             * verify that the entry written on the unisolated index is not
             * visible to the transaction that started before that write.
             */

            IIndex index = journal.getIndex(name,tx1);
            
            assertTrue(index.contains(k1));
            assertFalse(index.contains(k2));

        }
        
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);
        
        {

            /*
             * start another transaction and verify that the 2nd committed
             * write is now visible to that transaction.
             */
            
            IIndex index = journal.getIndex(name,tx2);
            
            assertTrue(index.contains(k1));
            assertTrue(index.contains(k2));

        }
        
        journal.abort(tx1);
        
        journal.abort(tx2);

        journal.closeAndDelete();
        
    }

    /**
     * Test verifies that an isolated write is visible inside of a transaction
     * (tx1) but not in a concurrent transaction (tx2) and not in the unisolated
     * index until the tx1 commits. Once the tx1 commits, the write is visible
     * in the unisolated index. The write never becomes visible in tx2.  If tx2
     * attempts to write a value under the same key then a write-write conflict
     * is reported and validation fails.
     */
    public void test_writeIsolation() {
        
        Journal journal = new Journal(getProperties());
        
        final String name = "abc";
        
        final int branchingFactor = 3;
        
        final byte[] k1 = new byte[]{1};

        final byte[] v1 = new byte[]{1};
        final byte[] v1a = new byte[]{1,1};
        
        {

            /*
             * register an index and commit the journal. 
             */
            
            journal.registerIndex(name, new UnisolatedBTree(journal,
                    branchingFactor, UUID.randomUUID()));
            
            assert(journal.commit()!=0L);
            
        }

        /*
         * create two transactions.
         */
        
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);
        
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);

        {
            
            /*
             * write an entry in tx1.  verify that the entry is not visible
             * in the unisolated index or in the index as isolated by tx2.
             */
            
            IsolatedBTree ndx1 = (IsolatedBTree)journal.getIndex(name,tx1);
            
            assertFalse(ndx1.contains(k1));
            
            assertNull(ndx1.insert(k1,v1));
            
            // check the version counter in tx1.
            assertEquals("versionCounter", 0, ndx1.getValue(k1)
                    .getVersionCounter());
            
            // not visible in the other tx.
            assertFalse(journal.getIndex(name,tx2).contains(k1));

            // not visible in the unisolated index.
            assertFalse(journal.getIndex(name).contains(k1));

            /*
             * commit tx1. verify that the write is still not visible in tx2 but
             * that it is now visible in the unisolated index.
             */
            
            // commit tx1.
            assertTrue(journal.commit(tx1)!=0L);
            
            // still not visible in the other tx.
            assertFalse(journal.getIndex(name,tx2).contains(k1));

            // but now visible in the unisolated index.
            assertTrue(journal.getIndex(name).contains(k1));

            // check the version counter in the unisolated index.
            assertEquals("versionCounter", 1, ((UnisolatedBTree) journal
                    .getIndex(name)).getValue(k1).getVersionCounter());

            /*
             * write a conflicting entry in tx2 and verify that validation of
             * tx2 fails.
             */

            assertNull(journal.getIndex(name,tx2).insert(k1,v1a));

            // check the version counter in tx2.
            assertEquals("versionCounter", 0, ((IsolatedBTree) journal
                    .getIndex(name,tx2)).getValue(k1).getVersionCounter());

            ITx tmp = journal.getTx(tx2);
            
            try {

                journal.commit(tx2);
                
                fail("Expecting: "+ValidationError.class);
                
            } catch(ValidationError ex) {
                
                System.err.println("Ignoring expected exception: "+ex);
                
            }
            
            assertTrue(tmp.isAborted());
            
        }
        
        journal.closeAndDelete();
        
    }

    //
    // Delete object.
    //

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0 and then
     * another version (v1) is written on tx0 under the same key. Both
     * transactions prepare and commit. The end state is that (id0,v1) is
     * visible in the database after the commit.
     */
    public void test_delete001() {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        {
            /*
             * register an index and commit the journal.
             */
            journal.registerIndex(name, new UnisolatedBTree(journal, UUID
                    .randomUUID()));
            
            journal.commit();

        }

        /*
         * create transactions.
         */

        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        /*
         * Write v0 on tx0.
         */
        final byte[] id0 = new byte[] { 1 };
        final byte[] v0 = getRandomData().array();
        journal.getIndex(name,tx0).insert(id0, v0);
        assertEquals(v0, journal.getIndex(name,tx0).lookup(id0));

        /*
         * Verify that the version does NOT show up in a concurrent transaction.
         */
         assertFalse(journal.getIndex(name,tx1).contains(id0));

         // delete the version.
         assertEquals(v0, (byte[]) journal.getIndex(name,tx0)
                .remove(id0));

         // no longer visible in that transaction.
         assertFalse(journal.getIndex(name,tx0).contains(id0));

         /*
          * Test delete after delete (succeeds, but returns null).
          */
         assertNull(journal.getIndex(name,tx0).remove(id0));

         /*
          * Test write after delete (succeeds, returning null).
          */
         final byte[] v1 = getRandomData().array();
         assertNull(journal.getIndex(name,tx0).insert(id0, v1));

         // Still not visible in concurrent transaction.
         assertFalse(journal.getIndex(name,tx1).contains(id0));

         // Still not visible in global scope.
         assertFalse(journal.getIndex(name).contains(id0));

         // Prepare and commit tx0.
         assertNotSame(0L,journal.commit(tx0));

         // Still not visible in concurrent transaction.
         assertFalse(journal.getIndex(name,tx1).contains(id0));

         // Now visible in global scope.
         assertTrue(journal.getIndex(name).contains(id0));

         // Prepare and commit tx1 (no writes).
         assertNotSame(0L,journal.commit(tx1));

         // Still visible in global scope.
         assertTrue(journal.getIndex(name).contains(id0));

         journal.closeAndDelete();
    
    }

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0 and then
     * another version (v1) is written on tx0 under the same key and isolation
     * is re-verified. Finally, v1 is deleted from tx0. Both transactions
     * prepare and commit. The end state is that no entry for id0 is visible in
     * the database after the commit.
     */
    public void test_delete002() {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        {
            /*
             * register an index and commit the journal.
             */
            journal.registerIndex(name, new UnisolatedBTree(journal, UUID.randomUUID()));
            
            journal.commit();

        }

        /*
         * create transactions.
         */

        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        /*
         * Write v0 on tx0.
         */
        final byte[] id0 = new byte[] { 1 };
        final byte[] v0 = getRandomData().array();
        journal.getIndex(name,tx0).insert(id0, v0);
        assertEquals(v0, journal.getIndex(name,tx0).lookup(id0));

        /*
         * Verify that the version does NOT show up in a concurrent transaction.
         */
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // delete the version.
        assertEquals(v0, (byte[]) journal.getIndex(name,tx0)
                .remove(id0));

        // no longer visible in that transaction.
        assertFalse(journal.getIndex(name,tx0).contains(id0));

        /*
         * Test delete after delete (succeeds, but returns null).
         */
        assertNull(journal.getIndex(name,tx0).remove(id0));

        /*
         * Test write after delete (succeeds, returning null).
         */
        final byte[] v1 = getRandomData().array();
        assertNull(journal.getIndex(name,tx0).insert(id0, v1));

        // Still not visible in concurrent transaction.
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        /*
         * Delete v1.
         */
        assertEquals(v1, (byte[]) journal.getIndex(name,tx0).remove(id0));

        // Still not visible in concurrent transaction.
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        /*
         * Prepare and commit tx0.
         * 
         * Note: We MUST NOT propagate a delete marker onto the unisolated index
         * since no entry for that key is visible was visible when the tx0
         * began.
         */
        assertNotSame(0L, journal.commit(tx0));

        // Still not visible in concurrent transaction.
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        // Prepare and commit tx1 (no writes).
        assertNotSame(0L,journal.commit(tx1));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        journal.closeAndDelete();

    }

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a key (id0). The test verifies the write and verifies that the
     * write is not visible in tx1. The v0 is then deleted from tx0. Another
     * version (v1) is written on tx1 under the same key and isolation is
     * re-verified. Both transactions prepare and commit. Since no entry for id0
     * was pre-existing in the global scope the delete in tx0 does not propagate
     * into the global scope and the end state is that the write (id0,v1) from
     * tx1 is visible in the database after the commit.
     */
    public void test_delete003() {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        {
            /*
             * register an index and commit the journal.
             */
            journal.registerIndex(name, new UnisolatedBTree(journal, UUID.randomUUID()));
            
            journal.commit();

        }

        /*
         * create transactions.
         */

        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        /*
         * Write v0 on tx0.
         */
        final byte[] id0 = new byte[] { 1 };
        final byte[] v0 = getRandomData().array();
        journal.getIndex(name,tx0).insert(id0, v0);
        assertEquals(v0, journal.getIndex(name,tx0).lookup(id0));

        /*
         * Verify that the version does NOT show up in a concurrent transaction.
         */
        assertFalse(journal.getIndex(name,tx1).contains(id0));

        // delete the version.
        assertEquals(v0, (byte[]) journal.getIndex(name,tx0)
                .remove(id0));

        // no longer visible in that transaction.
        assertFalse(journal.getIndex(name,tx0).contains(id0));
        
        /*
         * write(id0,v1) in tx1.
         */
        final byte[] v1 = getRandomData().array();
        assertNull(journal.getIndex(name,tx1).insert(id0, v1));

        // Still not visible in concurrent transaction.
        assertFalse(journal.getIndex(name,tx0).contains(id0));

        // Still not visible in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        /*
         * Prepare and commit tx0.
         * 
         * Note: We MUST NOT propagate a delete marker onto the unisolated index
         * since no entry for that key is visible was visible when the tx0
         * began.
         */
        assertNotSame(0L, journal.commit(tx0));

        // Prepare and commit tx1.
        assertNotSame(0L,journal.commit(tx1));

        // (id0,v1) is now visible in global scope.
        assertTrue(journal.getIndex(name).contains(id0));
        assertEquals(v1, (byte[])journal.getIndex(name).lookup(id0));

        journal.closeAndDelete();

    }

// //
// // Isolation.
// //
//    
//    /**
//     * Test verifies some aspects of transactional isolation. A transaction
//     * (tx0) is created from a journal with nothing written on it. A data
//     * version (v0) is then written onto the journal outside of the
//     * transactional scope and we verify that the version is visible on the
//     * journal but not in tx0. Another transaction (tx1) is created and we
//     * version that the written version is visible. We then update the version
//     * on the journal and verify that the change is NOT visible to either
//     * transaction. We then delete the version on the journal and verify that
//     * the change is not visible to either transaction. A 2nd version is then
//     * written in both tx0 and tx1 and everything is reverified. The version is
//     * then deleted on tx1 (reverified). A 3rd version is written on tx0
//     * (reverified). Finally, we delete the version on tx0 (reverified). At this
//     * point the most recent version has been deleted on the journal and in both
//     * transactions.
//     * 
//     * FIXME This test depends on some edge features (the ability to write in
//     * the global scope while concurrent transactions are running). Write a
//     * variant that does not use that feature.
//     */
//
//    public void test_isolation001() throws IOException {
//        
//        final Properties properties = getProperties();
//
//        try {
//            
//            Journal journal = new Journal(properties);
//
//            // Transaction begins before the write.
//            IIndexStore tx0 = new Tx(journal,0);
//
//            // Write a random data version for id 0.
//            final int id0 = 1;
//            final ByteBuffer expected_id0_v0 = getRandomData();
//            journal.write( id0, expected_id0_v0);
//            assertEquals(expected_id0_v0.array(),journal.read( id0, null));
//            final ISlotAllocation slots_v0 = journal.objectIndex.get(0);
//
//            /*
//             * Verify that the version does NOT show up in a transaction created
//             * before the write. If the version shows up here it most likely
//             * means that the transaction is reading from the current object
//             * index state, rather than from the object index state at the time
//             * that the transaction began.
//             */
//            assertNotFound(tx0.read(id0, null));
//
//            // Transaction begins after the write.
//            IIndexStore tx1 = new Tx(journal,1);
//
//            /*
//             * Verify that the version shows up in a transaction created after
//             * the write.
//             */
//            assertEquals(expected_id0_v0.array(),tx1.read(id0, null));
//
//            /*
//             * Update the version outside of the transaction.  This change SHOULD
//             * NOT be visible to either transaction.
//             */
//            final ByteBuffer expected_id0_v1 = getRandomData();
//            journal.write( id0, expected_id0_v1);
////            final ISlotAllocation slots_v1 = journal.objectIndex.getSlots(0);
//            /*
//             * FIXME This is failing because the journal is not looking for
//             * whether or not concurrent transactions are running. When they are
//             * we can not immediately deallocate the slots for a version
//             * overwritten in the global scope. Those slot allocations need to
//             * be queued up on the global object index for eventual
//             * deallocation. That deallocation can not occur until all
//             * transactions which can read that version have prepared/aborted.
//             * This entire feature (updating the global scope outside of a
//             * transaction) is a bit edgy and needs more thought.
//             */
//            assertEquals(slots_v0,journal.objectIndex.get(0));
//            assertEquals(expected_id0_v1.array(),journal.read( id0, null));
//            assertNotFound(tx0.read(id0, null));
//            assertEquals(expected_id0_v0.array(),tx1.read(id0, null));
//
//            /*
//             * Delete the version on the journal. This change SHOULD NOT be
//             * visible to either transaction.
//             */
//            journal.delete(id0);
//            assertDeleted(journal, id0);
//            assertNotFound(tx0.read(id0, null));
//            assertEquals(expected_id0_v0.array(),tx1.read(id0, null));
//
//            /*
//             * Write a version on tx1 and verify that we read that version from
//             * tx1 rather than the version written in the journal scope before
//             * the transaction began. Verify that the written version does not
//             * show up either on the journal or in tx1.
//             */
//            final ByteBuffer expected_tx1_id0_v0 = getRandomData();
//            tx1.write(id0, expected_tx1_id0_v0);
//            assertDeleted(journal, id0);
//            assertNotFound(tx0.read(id0, null));
//            assertEquals(expected_tx1_id0_v0.array(),tx1.read(id0, null));
//
//            /*
//             * Write a version on tx0 and verify that we read that version from
//             * tx0 rather than the version written in the journal scope before
//             * the transaction began. Verify that the written version does not
//             * show up either on the journal or in tx1.
//             */
//            final ByteBuffer expected_tx0_id0_v0 = getRandomData();
//            tx0.write(id0, expected_tx0_id0_v0);
//            assertDeleted(journal, id0);
//            assertEquals(expected_tx0_id0_v0.array(),tx0.read(id0, null));
//            assertEquals(expected_tx1_id0_v0.array(),tx1.read(id0, null));
//
//            /*
//             * Write a 2nd version on tx0 and reverify.
//             */
//            final ByteBuffer expected_tx0_id0_v1 = getRandomData();
//            tx0.write(id0, expected_tx0_id0_v1);
//            assertDeleted(journal, id0);
//            assertEquals(expected_tx0_id0_v1.array(),tx0.read(id0, null));
//            assertEquals(expected_tx1_id0_v0.array(),tx1.read(id0, null));
//
//            /*
//             * Write a 2nd version on tx1 and reverify.
//             */
//            final ByteBuffer expected_tx1_id0_v1 = getRandomData();
//            tx1.write(id0, expected_tx1_id0_v1);
//            assertDeleted(journal, id0);
//            assertEquals(expected_tx0_id0_v1.array(),tx0.read(id0, null));
//            assertEquals(expected_tx1_id0_v1.array(),tx1.read(id0, null));
//
//            /*
//             * Delete the version on tx1 and reverify.
//             */
//            tx1.delete(id0);
//            assertDeleted(journal, id0);
//            assertEquals(expected_tx0_id0_v1.array(),tx0.read(id0, null));
//            assertDeleted(tx1, id0);
//
//            /*
//             * Write a 3rd version on tx0 and reverify.
//             */
//            final ByteBuffer expected_tx0_id0_v2 = getRandomData();
//            tx0.write(id0, expected_tx0_id0_v2);
//            assertDeleted(journal, id0);
//            assertEquals(expected_tx0_id0_v2.array(),tx0.read(id0, null));
//            assertDeleted(tx1, id0);
//            
//            /*
//             * Delete the version on tx0 and reverify.
//             */
//            tx0.delete(id0);
//            assertDeleted(journal, id0);
//            assertDeleted(tx0, id0);
//            assertDeleted(tx1, id0);
//
//            /*
//             * @todo Define the outcome of validation if tx0 and tx1 commit in
//             * this scenario. I would think that the commits would validate
//             * since no version of the data exists either on the journal in
//             * global scope or on either transaction. The only reason why this
//             * might be problematic is that we have allowed a change made
//             * directly to the global scope while transactions are running.
//             */
//            
//            journal.close();
//            
//        } finally {
//
//            deleteTestJournalFile();
//            
//        }
//        
//    }
//    
//    /**
//     * Test writes multiple versions on a single transaction and verifies that
//     * the correct version may be read back at any time. There are two data
//     * items, id0 and id1. A pre-existing version is written onto the journal
//     * for id0 before the transaction starts. Multiple versions are then written
//     * onto the journal for each identifier and we verify that versions written
//     * within the transaction that are subsequently overwritten are
//     * synchronously deallocated while the pre-existing version on the journal
//     * is not. Finally, the versions are deleted and we again verify the correct
//     * deallocation strategy. After the transaction commits, the pre-existing
//     * version is still allocated but unreachable. We then do a GC of the
//     * transaction and verify that the pre-existing version is finally
//     * deallocated.
//     * 
//     * @throws IOException
//     */
//    public void test_writeMultipleVersions() throws IOException {
//
//        final Properties properties = getProperties();
//        
//        try {
//            
//            Journal journal = new Journal(properties);
//
//            // Two versions of id0.
//            final int id0 = 1;
//            
//            // pre-existing version of id0.
//            final ByteBuffer expected_preExistingVersion = getRandomData();
//
//            // Two versions of id0 written during tx0.
//            final ByteBuffer expected0v0 = getRandomData();
//            final ByteBuffer expected0v1 = getRandomData();
//            
//            // Three versions of id1.
//            final int id1 = 2;
//            final ByteBuffer expected1v0 = getRandomData();
//            final ByteBuffer expected1v1 = getRandomData();
//            final ByteBuffer expected1v2 = getRandomData();
//
//            // Write pre-existing version of id0 onto the journal.
//            journal.write(id0,expected_preExistingVersion);
//            ISlotAllocation slots_preExistingVersion = journal.objectIndex.get(id0);
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            assertVersionCounter(journal, id0, 0);
//            
//            // Start transaction.
//            Tx tx0 = new Tx(journal,0);
//            
//            // precondition tests, write id0 version0, postcondition tests.
//            assertEquals(expected_preExistingVersion.array(),tx0.read(id0,null));
//            
//            assertNotFound(tx0.read(id1,null));
//
//            // write id0 version0.
//            tx0.write(id0,expected0v0);
//            assertVersionCounter(journal, id0, 0); // there is a pre-existing version on the journal.
//            assertVersionCounter(tx0, id0, 0);
//
//            final ISlotAllocation slots_id0_v0 = tx0.getObjectIndex().get(id0);
//            
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, true);
//            
//            assertEquals(expected0v0.array(),tx0.read(id0, null));
//            
//            assertNotFound(tx0.read(id1,null));
//
//            // write id1 version0, postcondition tests.
//            tx0.write(id1,expected1v0);
//            // Note: no version of id1 is on the journal.
//            assertVersionCounter(tx0, id1, 0);
//            
//            final ISlotAllocation slots_id1_v0 = tx0.getObjectIndex().get(id1);
//
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, true);
//
//            assertSlotAllocationState(slots_id1_v0, journal.allocationIndex, true);
//
//            assertEquals(expected0v0.array(),tx0.read(id0, null));
//            
//            assertEquals(expected1v0.array(),tx0.read(id1, null));
//            
//            // write id1 version1, postcondition tests.
//            tx0.write(id1,expected1v1);
//            // Note: no version of id1 is on the journal.
//            assertVersionCounter(tx0, id1, 0); // counter is only changed by commit.
//
//            final ISlotAllocation slots_id1_v1 = tx0.getObjectIndex().get(id1);
//
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, true);
//
//            assertSlotAllocationState(slots_id1_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v1, journal.allocationIndex, true);
//
//            assertEquals(expected0v0.array(),tx0.read( id0, null));
//            
//            assertEquals(expected1v1.array(),tx0.read( id1, null));
//            
//            // write id1 version2, postcondition tests.
//            tx0.write(id1,expected1v2);
//            // Note: no version of id1 is on the journal.
//            assertVersionCounter(tx0, id1, 0); // counter is only changed by commit.
//
//            final ISlotAllocation slots_id1_v2 = tx0.getObjectIndex().get(id1);
//
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, true);
//
//            assertSlotAllocationState(slots_id1_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v1, journal.allocationIndex, false);
//            
//            assertSlotAllocationState(slots_id1_v2, journal.allocationIndex, true);
//            
//            assertEquals(expected0v0.array(),tx0.read( id0, null));
//            
//            assertEquals(expected1v2.array(),tx0.read( id1, null));
//
//            // write id0 version1, postcondition tests.
//            tx0.write(id0,expected0v1);
//            assertVersionCounter(journal, id0, 0); // there is a pre-existing version on the journal.
//            assertVersionCounter(tx0, id0, 0); // counter is only changed by commit.
//
//            final ISlotAllocation slots_id0_v1 = tx0.getObjectIndex().get(id0);
//
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id0_v1, journal.allocationIndex, true);
//
//            assertSlotAllocationState(slots_id1_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v1, journal.allocationIndex, false);
//            
//            assertSlotAllocationState(slots_id1_v2, journal.allocationIndex, true);
//
//            assertEquals(expected0v1.array(),tx0.read(id0, null));
//            
//            assertEquals(expected1v2.array(),tx0.read(id1, null));
//            
//            // delete id1, postcondition tests.
//
//            tx0.delete(id1);
//            // Note: There is not a version of id1 on the journal.
//            assertVersionCounter(tx0, id1, 0); // counter is only changed by commit.
//            
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id0_v1, journal.allocationIndex, true);
//
//            assertSlotAllocationState(slots_id1_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v1, journal.allocationIndex, false);
//            
//            assertSlotAllocationState(slots_id1_v2, journal.allocationIndex, false);
//
//            assertEquals(expected0v1.array(),tx0.read(id0, null));
//            
//            assertDeleted(tx0, id1);
//
//            // delete id0, postcondition tests.
//
//            tx0.delete(id0);
//            assertVersionCounter(journal, id0, 0); // pre-existing version on the journal.
//            assertVersionCounter(tx0, id0, 0); // counter is only changed by commit.
//            
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id0_v1, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v1, journal.allocationIndex, false);
//            
//            assertSlotAllocationState(slots_id1_v2, journal.allocationIndex, false);
//
//            assertDeleted(tx0, id0);
//            
//            assertDeleted(tx0, id1);
//
//            /*
//             * Commit the transaction.
//             */
//            tx0.prepare();
//            
//            tx0.commit();
//
//            /*
//             * Note: Since id0 was pre-existing, the version counter for id0 was
//             * incremented even though the version was deleted!
//             */
//            assertVersionCounter(journal, id0, 1);
//            
//            /*
//             * Note: Since id1 was NOT pre-existing, there is no entry left in
//             * the global object index. This also means that there is no defined
//             * version counter for id1.
//             */
//            assertNotFound(journal.read(id1,null));
//            
//            // Note: Still allocated!
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, true);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id0_v1, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v1, journal.allocationIndex, false);
//            
//            assertSlotAllocationState(slots_id1_v2, journal.allocationIndex, false);
//
//            // id0 is deleted, even though the slots for the version are still allocated.
//            assertDeleted(journal, id0);
//            
//            // Garbage collection for tx0.
//            tx0.gc();
//
//            // Note: Finally deallocated!
//            assertSlotAllocationState(slots_preExistingVersion, journal.allocationIndex, false);
//            
//            assertSlotAllocationState(slots_id0_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id0_v1, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v0, journal.allocationIndex, false);
//
//            assertSlotAllocationState(slots_id1_v1, journal.allocationIndex, false);
//            
//            assertSlotAllocationState(slots_id1_v2, journal.allocationIndex, false);
//
//            /*
//             * @todo Should this report "not found" or "deleted"?
//             */
////            assertNotFound(journal.read( id0, null ));
//            assertDeleted(journal, id0);
//
//            journal.close();
//
//        } finally {
//
//            deleteTestJournalFile();
//            
//        }
//
//    }


    /*
     * Transaction semantics tests.
     */
    
    /**
     * Simple test of commit semantics (no conflict). Four transactions are
     * started: tx0, which starts first. tx1 which starts next and on which we
     * will write one data version; tx2, which begins after tx1 but before tx1
     * commits - the change will NOT be visible in this transaction; and tx3,
     * which begins after tx1 commits - the change will be visible in this
     * transaction.
     */
    public void test_commit_noConflict01() {

        Journal journal = new Journal(getProperties());
        
        final String name = "abc";
        
        {
            
            journal.registerIndex(name,new UnisolatedBTree(journal,UUID.randomUUID()));
            
            journal.commit();
            
        }

        /*
         * Transaction that starts before the transaction on which we write. The
         * change will not be visible in this scope.
         */
        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        // transaction on which we write and later commit.
        final long tx1 = journal.newTx(IsolationEnum.ReadWrite);

        // new transaction - commit will not be visible in this scope.
        final long tx2 = journal.newTx(IsolationEnum.ReadWrite);

        final byte[] id1 = new byte[]{1};

        final byte[] v0 = getRandomData().array();

        // write data version on tx1
        assertNull(journal.getIndex(name,tx1).insert(id1, v0));

        // data version visible in tx1.
        assertEquals(v0, (byte[])journal.getIndex(name,tx1).lookup(id1));

        // data version not visible in global scope.
        assertNull(journal.getIndex(name).lookup(id1));

        // data version not visible in tx0.
        assertNull(journal.getIndex(name,tx0).lookup(id1));

        // data version not visible in tx2.
        assertNull(journal.getIndex(name,tx2).lookup(id1));

        // commit.
        journal.commit(tx1);

        // data version now visible in global scope.
        assertEquals(v0, (byte[])journal.getIndex(name).lookup(id1));

        // new transaction - commit is visible in this scope.
        final long tx3 = journal.newTx(IsolationEnum.ReadWrite);

        // data version still not visible in tx0.
        assertNull(journal.getIndex(name,tx0).lookup(id1));

        // data version still not visible in tx2.
        assertNull(journal.getIndex(name,tx2).lookup(id1));

        // data version visible in the new tx (tx3).
        assertEquals(v0, (byte[])journal.getIndex(name,tx3).lookup(id1));

        /*
         * commit tx0 - nothing was written, no conflict should result.
         */
        journal.commit(tx0);

        /*
         * commit tx1 - nothing was written, no conflict should result.
         */
        journal.commit(tx2);

        // commit tx3 - nothing was written, no conflict should result.
        journal.commit(tx3);

        // data version in global scope was not changed by any other commit.
        assertEquals(v0, (byte[]) journal.getIndex(name).lookup(
                id1));

        journal.closeAndDelete();

    }

    /**
     * Test in which a transaction deletes a pre-existing version (that is, a
     * version that existed in global scope when the transaction was started).
     */
    public void test_deletePreExistingVersion_noConflict() {

        Journal journal = new Journal(getProperties());

        final String name = "abc";
        
        {
            
            journal.registerIndex(name, new UnisolatedBTree(journal,UUID.randomUUID()));
            
            journal.commit();
            
        }
        
        final byte[] id0 = new byte[]{1};

        final byte[] v0 = getRandomData().array();

        // data version not visible in global scope.
        assertNull(journal.getIndex(name).lookup(id0));

        // write data version in global scope.
        journal.getIndex(name).insert(id0, v0);

        // data version visible in global scope.
        assertEquals(v0, (byte[])journal.getIndex(name).lookup(id0));

        // commit the unisolated write.
        journal.commit();
        
        // start transaction.
        final long tx0 = journal.newTx(IsolationEnum.ReadWrite);

        // data version visible in the transaction.
        assertEquals(v0, (byte[])journal.getIndex(name,tx0).lookup(id0));

        // delete version in transaction scope.
        assertEquals(v0,(byte[])journal.getIndex(name,tx0).remove(id0));

        // FIXME IsolatedBTree has remaining problems and needs a better
        // test suite!
        // data version not visible in transaction.
        assertFalse(journal.getIndex(name,tx0).contains(id0));
        assertNull(journal.getIndex(name,tx0).lookup(id0));

        // data version still visible in global scope.
        assertTrue(journal.getIndex(name).contains(id0));
        assertEquals(v0, (byte[])journal.getIndex(name).lookup(id0));

        // commit.
        journal.commit(tx0);

        // data version now deleted in global scope.
        assertFalse(journal.getIndex(name).contains(id0));

        journal.closeAndDelete();

    }

// /*
// * read-write conflicts.
// */
//
// /**
// * <p>
// * Read-write conflicts result in the retention of old versions until the
//     * readers complete. This test writes a data version (v0) for id0 in the
//     * global scope on the journal. Two transactions are then created, tx1 and
//     * tx2. tx1 writes a new version and then commits. Since tx2 still has the
//     * ability to read the prior version, the prior version MUST remain
//     * allocated on the journal and accessable via the tx2 object index. Tx2
//     * commits. Since there is no longer any transaction that can see v0, we
//     * invoke garbage collection on tx1, which causes the overwritten version to
//     * be deallocated.
//     * </p>
//     */
//    public void test_readWriteConflict01() throws IOException {
//
//        final Properties properties = getProperties();
//        
//        /*
//         * Setup a conflict resolver that will throw an exception if a
//         * write-write conflict is detected (no write-write conflicts are
//         * expected by this test).
//         */
//        properties.setProperty("conflictResolver",ConflictResolverAlwaysFails.class.getName());
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            // verify the conflict resolver.
//            assertNotNull( journal.getConflictResolver() );
//            assertTrue( journal.getConflictResolver() instanceof ConflictResolverAlwaysFails );
//
//            int id0 = 1;
//            
//            // create random data for versions.
//            ByteBuffer expected_id0_v0 = getRandomData();
//            ByteBuffer expected_id0_v1 = getRandomData();
//
//            // data version not visible in global scope.
//            assertNotFound(journal.read( id0, null));
//
//            /*
//             * write data version in global scope.
//             */
//            journal.write(id0, expected_id0_v0);
//
//            // The slots on which the first data version is written.
//            final ISlotAllocation slots_v0 = journal.objectIndex.get(id0);
//            
//            // Verify the data version is visible in global scope.
//            assertEquals(expected_id0_v0.array(), journal.read(id0, null));
//
//            // start transaction.
//            Tx tx1 = new Tx(journal, 1);
//
//            // start transaction.
//            Tx tx2 = new Tx(journal, 2);
//            
//            /*
//             * Verify that the data version is visible in the transaction scope
//             * where it will be overwritten. Note that we do NOT test the other
//             * concurrent transaction since actually reading the version in that
//             * transaction might trigger different code paths.
//             */
//            assertEquals(expected_id0_v0.array(), tx1.read(id0, null));
//
//            // overwrite data version in transaction scope.
//            tx1.write(id0, expected_id0_v1);
//            assertVersionCounter(journal, id0, 0);
//            assertVersionCounter(tx1, id0, 0);
//                        
//            // slot allocation in global scope is unchanged.
//            assertEquals(slots_v0,journal.objectIndex.get(id0));
//
//            // data version in global scope is unchanged.
//            assertEquals(expected_id0_v0.array(), journal.read(id0, null));
//
//            // Get the slots on which the 2nd data version was written.
//            final ISlotAllocation slots_v1 = tx1.getObjectIndex().get(id0);
//
//            // prepare
//            tx1.prepare();
//
//            // commit.
//            tx1.commit();
//
//            assertVersionCounter(journal, id0, 1);
//
//            // The v0 slots are still allocated.
//            assertSlotAllocationState(slots_v0, journal.allocationIndex,true);
//
//            // The v1 slots are still allocated.
//            assertSlotAllocationState(slots_v1, journal.allocationIndex,true);
//
//            // The entry in the scope is consistent with the v1 allocation.
//            assertEquals(slots_v1,journal.objectIndex.get(id0));
//
//            // new data version now visible in global scope.
//            assertEquals(expected_id0_v1.array(), journal.read(id0, null));
//
//            // The entry in the tx2 object index is consistent with the v0
//            // allocatation (it was not overwritten when the tx1 committed).
//            assertEquals(slots_v0,tx2.getObjectIndex().get(id0));
//
//            // Read the version in tx2 (just to prove that we can do it).
//            assertEquals(expected_id0_v0.array(), tx2.read(id0, null));
//
//            // Commit tx2 (we could have as easily aborted tx2 for this test).
//            tx2.prepare();
//            tx2.commit();
//            
//            /*
//             * Sweap overwritten versions written by or visible to tx1 and
//             * earlier transactions - this MUST deallocate the overwritten
//             * version (v0).
//             * 
//             * Note: This method MUST NOT be invoked while tx2 is active since
//             * tx2 has visiblity onto the same ground state as tx1.
//             */
//            tx1.gc();
//
//            // The v0 slots are now deallocated.
//            assertSlotAllocationState(slots_v0, journal.allocationIndex,false);
//
//            // The v1 slots are still allocated.
//            assertSlotAllocationState(slots_v1, journal.allocationIndex,true);
//
//            // The entry in the global object index is consistent with the v1
//            // allocatation.
//            assertEquals(slots_v1,journal.objectIndex.get(id0));
//
//            // The v1 version is still visible in global scope.
//            assertEquals(expected_id0_v1.array(), journal.read(id0, null));
//
//            /*
//             * GC(tx2) - this MUST be a NOP.
//             */
//            tx2.gc();
//            
//            // The v0 slots are still deallocated.
//            assertSlotAllocationState(slots_v0, journal.allocationIndex,false);
//
//            // The v1 slots are still allocated.
//            assertSlotAllocationState(slots_v1, journal.allocationIndex,true);
//
//            // The entry in the global object index is consistent with the v1
//            // allocatation.
//            assertEquals(slots_v1,journal.objectIndex.get(id0));
//
//            // The v1 version is still visible in global scope.
//            assertEquals(expected_id0_v1.array(), journal.read(id0, null));
//            
//            journal.close();
//
//        } finally {
//
//            deleteTestJournalFile();
//
//        }
//
//    }
}
