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

import java.util.Properties;

import com.bigdata.isolation.IsolatedBTree;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.objndx.IIndex;

/**
 * Test suite for fully-isolated read-write transactions.
 * 
 * @todo Test suite for transaction isolation with respect to the underlying
 *       journal. The tests in this suite are designed to verify isolation of
 *       changes within the scope of the transaction when compared to the last
 *       committed state of the journal. This basically amounts to verifying
 *       that operations read through the transaction scope object index into
 *       the journal scope object index.
 * 
 * @todo Do stress test with writes, reads, and deletes.
 * 
 * @todo Work through tests of the commit logic and verify the post-conditions
 *       for successful commit vs abort of a transaction. Verification must
 *       occur on many levels. For example, there are post-conditions for
 *       versions that must or must not be accessible, for whether or not the
 *       journal is restart safe (this is not tested so far), for the specifics
 *       of the object index and slot allocation index, etc.
 * 
 * @todo Work through backward validation, data type specific state based
 *       conflict resolution, and merging down the object indices onto the
 *       journal during the commit.
 * 
 * @todo Show that abort does not leave anything lying around, both that would
 *       break isolation (unlikely) or just junk that lies around unreclaimed on
 *       the slots (or in the index nodes themselves).
 * 
 * @todo Write-write conflicts either result in successful reconcilation via
 *       state-based conflict resolution or an abort of the transaction that is
 *       validating. Write tests to verify that write-write conflicts can be
 *       detected and provide versions of those tests where the conflict can and
 *       can not be validated and verify the end state in each case. State-based
 *       validation requires transparency at the object level, including the
 *       ability to deserialize versions into objects, to compare objects for
 *       consistency, to merge data into the most recent version where possible
 *       and according to data type specific rules, and to destructively merge
 *       objects when the conflict arises on <em>identity</em> rather than
 *       state. <br>
 *       An example of an identity based conflict is when two objects are
 *       created that represent URIs in an RDF graph. Since the lexicon for an
 *       RDF graph generally requires uniqueness - it certainly does for the
 *       RDFS store based on GOM - those objects must be merged into a single
 *       object since they have the same identity. For an RDFS store validation
 *       on the lexicon or statements ALWAYS succeeds since they are always
 *       consistent. For the GOM RDFS implementation, validation requires
 *       combining the various link sets so that all statements referencing the
 *       same lexical item can be found from the suriving object.<br>
 *       While the change is detected based on a clustered index, and hence both
 *       objects are in the same segment, destructive merging based can
 *       propagate changes to objects, e.g., in order to obtain a consistent
 *       link set or link set index. Unless the data structures provide for
 *       encapsulation, e.g., by defining objects that serve as collectors for
 *       the link set members in a given segment, that change could propagate
 *       beyond the segment in which it is detected. If changes can propagate in
 *       that manner then care MUST be taken to ensure that validation
 *       terminates.<br>
 *       In order to make validation extensible we will have to declare
 *       validation rules to the database, perhaps as a parameter when a
 *       transaction starts or - to enforce the data type specificity at the
 *       risk of tighter integration of components - as part of the schema
 *       declaration. Declare IConflictResolver that either merges state into
 *       object in the transaction or causes the tx to abort.
 * 
 * @todo Verify correct abort after 'prepare'.
 * 
 * @todo Issue warning or throw exception when closing journal with active
 *       transactions? Provide a 'force' and timeout option? This was all
 *       implemented for the DBCache implementation so the code and tests can
 *       just be migrated.
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

        Properties properties = getProperties();

        Journal journal = new Journal(properties);

        journal.commit();
        
        ITx tx = journal.newTx();
        
        /*
         * nothing written on this transaction.
         */
        
        tx.prepare();
        
        // commit.
        assertTrue(tx.commit()!=0L);

        journal.close();
        
    }

    /**
     * Verify that an index is not visible in the tx until the native
     * transaction in which it is registered has already committed before
     * the tx starts. 
     */
    public void test_indexNotVisibleUnlessCommitted() {
       
        Properties properties = getProperties();

        Journal journal = new Journal(properties);

        String name = "abc";
        
        // register index in unisolated scope, but do not commit yet.
        journal.registerIndex(name, new UnisolatedBTree(journal, 3));
        
        // start tx1.
        ITx tx1 = journal.newTx();

        // the index is not visible in tx1.
        assertNull(tx1.getIndex(name));
        
        // do unisolated commit.
        assertTrue(journal.commit()!=0L);
        
        // start tx2.
        ITx tx2 = journal.newTx();

        // the index still is not visible in tx1.
        assertNull(tx1.getIndex(name));

        // the index is visible in tx2.
        assertNotNull(tx2.getIndex(name));
        
        tx1.abort();
        
        tx2.abort();
        
        journal.close();
        
    }
    
    /**
     * Create a journal, setup an index, write an entry on that index, and
     * commit the store. Setup a transaction and verify that we can isolated
     * that index and read the written value. Write a value on the unisolated
     * index and verify that it is not visible within the transaction.
     */
    public void test_readIsolation() {
        
        Properties properties = getProperties();

        Journal journal = new Journal(properties);
        
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
                    journal, branchingFactor));
        
            assertNull(index.insert(k1, v1));
            
            assert(journal.commit()!=0L);
            
        }
        
        ITx tx1 = journal.newTx();
    
        {

            /*
             * verify that the write is visible in a transaction that starts
             * after the commit.
             */
            
            IIndex index  = tx1.getIndex(name);
            
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

            IIndex index = tx1.getIndex(name);
            
            assertTrue(index.contains(k1));
            assertFalse(index.contains(k2));

        }
        
        ITx tx2 = journal.newTx();
        
        {

            /*
             * start another transaction and verify that the 2nd committed
             * write is now visible to that transaction.
             */
            
            IIndex index = tx2.getIndex(name);
            
            assertTrue(index.contains(k1));
            assertTrue(index.contains(k2));

        }
        
        tx1.abort();

        journal.close();
        
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
        
        Properties properties = getProperties();

        Journal journal = new Journal(properties);
        
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
                    branchingFactor));
            
            assert(journal.commit()!=0L);
            
        }

        /*
         * create two transactions.
         */
        
        ITx tx1 = journal.newTx();
        
        ITx tx2 = journal.newTx();

        {
            
            /*
             * write an entry in tx1.  verify that the entry is not visible
             * in the unisolated index or in the index as isolated by tx2.
             */
            
            IsolatedBTree ndx1 = (IsolatedBTree)tx1.getIndex(name);
            
            assertFalse(ndx1.contains(k1));
            
            assertNull(ndx1.insert(k1,v1));
            
            // check the version counter in tx1.
            assertEquals("versionCounter", 0, ndx1.getValue(k1)
                    .getVersionCounter());
            
            // not visible in the other tx.
            assertFalse(tx2.getIndex(name).contains(k1));

            // not visible in the unisolated index.
            assertFalse(journal.getIndex(name).contains(k1));

            /*
             * commit tx1. verify that the write is still not visible in tx2 but
             * that it is now visible in the unisolated index.
             */
            
            // prepare tx1.
            tx1.prepare();

            // commit tx1.
            assertTrue(tx1.commit()!=0L);
            
            // still not visible in the other tx.
            assertFalse(tx2.getIndex(name).contains(k1));

            // but now visible in the unisolated index.
            assertTrue(journal.getIndex(name).contains(k1));

            // check the version counter in the unisolated index.
            assertEquals("versionCounter", 1, ((UnisolatedBTree) journal
                    .getIndex(name)).getValue(k1).getVersionCounter());

            /*
             * write a conflicting entry in tx2 and verify that validation of
             * tx2 fails.
             */

            assertNull(tx2.getIndex(name).insert(k1,v1a));

            // check the version counter in tx2.
            assertEquals("versionCounter", 0, ((IsolatedBTree) tx2
                    .getIndex(name)).getValue(k1).getVersionCounter());

            try {

                tx2.prepare();
                
                fail("Expecting: "+IllegalStateException.class);
                
            } catch(IllegalStateException ex) {
                
                System.err.println("Ignoring expected exception: "+ex);
                
            }
            
            assertTrue(tx2.isAborted());
            
        }
        
        journal.close();
        
    }
    
//    //
//    // Delete object.
//    //
//
//    /**
//     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
//     * tx0 for a persistent identifier. The test verifies the write and verifies
//     * that the write is not visible in tx1. The v0 is then deleted from tx0.
//     * Since no version ever existing in the global scope for that persistent
//     * identifier, the test verifies that the slots allocated to the version
//     * were immediately deallocated when the version was deleted. Tx0 and tx1
//     * are then committed.
//     * 
//     * @todo Do some more simple tests where a few objects are written, read
//     *       back, deleted one by one, and verify that they can no longer be
//     *       read.
//     * 
//     * FIXME Write a version of this test where the object is pre-existing in
//     * the global state and then deleted within the transaction. The delete MUST
//     * NOT be visible to a concurrent transaction. A GC after the transactions
//     * commit should cause the pre-existing version to be deallocated.
//     */
//    public void test_delete001() throws IOException {
//
//        final Properties properties = getProperties();
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            Tx tx0 = new Tx(journal,0);
//
//            Tx tx1 = new Tx(journal,1);
//
//            /*
//             * Write v0 on tx0.
//             */
//            final int id0 = 1;
//            final ByteBuffer expected_id0_v0 = getRandomData(journal);
//            tx0.write(id0, expected_id0_v0);
//            assertEquals(expected_id0_v0.array(),tx0.read(id0, null));
//
//            /*
//             * Verify that the version does NOT show up in a concurrent
//             * transaction. If the version shows up here it most likely means
//             * that the transaction is reading from the current object index
//             * state, rather than from the object index state at the time that
//             * the transaction began.
//             */
//            assertNotFound(tx1.read(id0, null));
//
//            // The slot allocation for the version that we are about to delete.
//            final ISlotAllocation slots = tx0.getObjectIndex().get(id0);
//            assertNotNull(slots);
//            assertSlotAllocationState(slots, journal.allocationIndex, true);
//            
//            // delete the version.
//            tx0.delete(id0);
//
//            /*
//             * Since the version only existed within the transaction, verify
//             * that the slots were synchronously deallocated when the version
//             * was deleted.
//             */
//            assertSlotAllocationState(slots, journal.allocationIndex, false);
//
//            // Verify the persistent identifier is now correctly marked as
//            // deleted in the transaction's object index.
//            try {
//                tx0.getObjectIndex().get(id0);
//                fail("Expecting: "+DataDeletedException.class);
//            }
//            catch(DataDeletedException ex) {
//                System.err.println("Ignoring expected exception: "+ex);
//            }
//
//            /*
//             * Test read after delete.
//             */
//            assertDeleted(tx0,id0);
//
//            /*
//             * Test delete after delete.
//             */
//            try {
//                
//                tx0.delete(id0);
//
//                fail("Expecting " + DataDeletedException.class);
//                
//            } catch (DataDeletedException ex) {
//                
//                System.err.println("Ignoring expected exception: " + ex);
//                
//            }
//
//            /*
//             * Test write after delete.
//             */
//            try {
//                
//                tx0.write(id0, getRandomData(journal));
//
//                fail("Expecting " + DataDeletedException.class);
//                
//            } catch (DataDeletedException ex) {
//                
//                System.err.println("Ignoring expected exception: " + ex);
//                
//            }
//
//            // Still not visible in concurrent transaction.
//            assertNotFound(tx1.read(id0, null));
//
//            // Still not visible in global scope.
//            assertNotFound(journal.read(id0, null));
//
//            tx0.prepare();
//            tx0.commit();
//
//            // Still not visible in concurrent transaction.
//            assertNotFound(tx1.read(id0, null));
//
//            // Still not visible in global scope.
//            assertNotFound(journal.read(id0, null));
//
//            tx1.prepare();
//            tx1.commit();
//
//            // Still not visible in global scope.
//            assertNotFound(journal.read(id0, null));
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
//    //
//    // Isolation.
//    //
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
//            IStore tx0 = new Tx(journal,0);
//
//            // Write a random data version for id 0.
//            final int id0 = 1;
//            final ByteBuffer expected_id0_v0 = getRandomData(journal);
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
//            IStore tx1 = new Tx(journal,1);
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
//            final ByteBuffer expected_id0_v1 = getRandomData(journal);
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
//            final ByteBuffer expected_tx1_id0_v0 = getRandomData(journal);
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
//            final ByteBuffer expected_tx0_id0_v0 = getRandomData(journal);
//            tx0.write(id0, expected_tx0_id0_v0);
//            assertDeleted(journal, id0);
//            assertEquals(expected_tx0_id0_v0.array(),tx0.read(id0, null));
//            assertEquals(expected_tx1_id0_v0.array(),tx1.read(id0, null));
//
//            /*
//             * Write a 2nd version on tx0 and reverify.
//             */
//            final ByteBuffer expected_tx0_id0_v1 = getRandomData(journal);
//            tx0.write(id0, expected_tx0_id0_v1);
//            assertDeleted(journal, id0);
//            assertEquals(expected_tx0_id0_v1.array(),tx0.read(id0, null));
//            assertEquals(expected_tx1_id0_v0.array(),tx1.read(id0, null));
//
//            /*
//             * Write a 2nd version on tx1 and reverify.
//             */
//            final ByteBuffer expected_tx1_id0_v1 = getRandomData(journal);
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
//            final ByteBuffer expected_tx0_id0_v2 = getRandomData(journal);
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
//            final ByteBuffer expected_preExistingVersion = getRandomData(journal);
//
//            // Two versions of id0 written during tx0.
//            final ByteBuffer expected0v0 = getRandomData(journal);
//            final ByteBuffer expected0v1 = getRandomData(journal);
//            
//            // Three versions of id1.
//            final int id1 = 2;
//            final ByteBuffer expected1v0 = getRandomData(journal);
//            final ByteBuffer expected1v1 = getRandomData(journal);
//            final ByteBuffer expected1v2 = getRandomData(journal);
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
        

//    /**
//     * Verifies that a READ is not permitted after a PREPARE and that the
//     * attempt results in an aborted transaction.
//     * 
//     * @throws IOException
//     */
//    public void test_runStateMachine_prepareRead_correctRejection() throws IOException {
//
//        final Properties properties = getProperties();
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            final long ts0 = 0;
//            
//            Tx tx0 = new Tx(journal, ts0);
//
//            tx0.prepare();
//
//            // can not read, write, delete or prepare after 'prepare'.
//            try {
//                tx0.read(1, null);
//                fail("Expecting: " + IllegalStateException.class);
//            } catch (IllegalStateException ex) {
//                System.err.println("Ignoring expected exception: " + ex);
//            }
//
//            assertFalse( tx0.isActive() );
//            assertFalse( tx0.isPrepared() );
//            assertTrue( tx0.isAborted() );
//            assertFalse( tx0.isCommitted() );
//            assertTrue( tx0.isComplete() );
//
//            assertFalse(journal.activeTx.containsKey(ts0));
//            assertFalse(journal.preparedTx.containsKey(ts0));
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
//     * Verifies that a WRITE is not permitted after a PREPARE and that the
//     * attempt results in an aborted transaction.
//     * 
//     * @throws IOException
//     */
//    public void test_runStateMachine_prepareWrite_correctRejection() throws IOException {
//
//        final Properties properties = getProperties();
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            final long ts0 = 0;
//            
//            Tx tx0 = new Tx(journal, ts0);
//
//            tx0.prepare();
//
//            // can not read, write, delete or prepare after 'prepare'.
//            try {
//                tx0.write(1, getRandomData(journal));
//                fail("Expecting: " + IllegalStateException.class);
//            } catch (IllegalStateException ex) {
//                System.err.println("Ignoring expected exception: " + ex);
//            }
//
//            assertFalse( tx0.isActive() );
//            assertFalse( tx0.isPrepared() );
//            assertTrue( tx0.isAborted() );
//            assertFalse( tx0.isCommitted() );
//            assertTrue( tx0.isComplete() );
//
//            assertFalse(journal.activeTx.containsKey(ts0));
//            assertFalse(journal.preparedTx.containsKey(ts0));
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
//     * Verifies that a DELETE is not permitted after a PREPARE and that the
//     * attempt results in an aborted transaction.
//     * 
//     * @throws IOException
//     */
//    public void test_runStateMachine_prepareDelete_correctRejection() throws IOException {
//
//        final Properties properties = getProperties();
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            final long ts0 = 0;
//            
//            Tx tx0 = new Tx(journal, ts0);
//
//            tx0.prepare();
//
//            // can not read, write, delete or prepare after 'prepare'.
//            try {
//                tx0.delete(1);
//                fail("Expecting: " + IllegalStateException.class);
//            } catch (IllegalStateException ex) {
//                System.err.println("Ignoring expected exception: " + ex);
//            }
//
//            assertFalse( tx0.isActive() );
//            assertFalse( tx0.isPrepared() );
//            assertTrue( tx0.isAborted() );
//            assertFalse( tx0.isCommitted() );
//            assertTrue( tx0.isComplete() );
//
//            assertFalse(journal.activeTx.containsKey(ts0));
//            assertFalse(journal.preparedTx.containsKey(ts0));
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
//    /*
//     * Transaction semantics tests.
//     */
//    
//    /**
//     * Simple test of commit semantics (no conflict). Four transactions are
//     * started: tx0, which starts first. tx1 which starts next and on which we
//     * will write one data version; tx2, which begins after tx1 but before tx1
//     * commits - the change will NOT be visible in this transaction; and tx3,
//     * which begins after tx1 commits - the change will be visible in this
//     * transaction.
//     */
//    
//    public void test_commit_noConflict01() throws IOException {
//
//        final Properties properties = getProperties();
//        
//        try {
//            
//            Journal journal = new Journal(properties);
//
//            /*
//             * Transaction that starts before the transaction on which we write.
//             * The change will not be visible in this scope.
//             */
//            Tx tx0 = new Tx(journal,0);
//            
//            // transaction on which we write and later commit.
//            Tx tx1 = new Tx(journal,1);
//            
//            // new transaction - commit will not be visible in this scope.
//            Tx tx2 = new Tx(journal,2);
//            
//            int id1 = 1;
//            
//            ByteBuffer expected_id1_v0 = getRandomData(journal);
//            
//            // write data version on tx1
//            tx1.write(  id1, expected_id1_v0 );
//            assertVersionCounter(tx1, id1, 0);
//
//            // data version visible in tx1.
//            assertEquals( expected_id1_v0.array(), tx1.read(id1, null));
//
//            // data version not visible in global scope.
//            assertNotFound( journal.read( id1, null));
//
//            // data version not visible in tx0.
//            assertNotFound( tx0.read(id1, null));
//
//            // data version not visible in tx2.
//            assertNotFound( tx2.read(id1, null));
//
//            // prepare
//            tx1.prepare();
//
//            // commit.
//            tx1.commit();
//
//            /*
//             * Note: the version counter on the journal is ZERO (0) since tx1
//             * wrote the first version (there was no pre-existing version when
//             * tx1 started).
//             */
//            assertVersionCounter(journal, id1, 0);
//
//            // data version now visible in global scope.
//            assertEquals( expected_id1_v0.array(), journal.read(id1, null));
//
//            // new transaction - commit is visible in this scope.
//            Tx tx3 = new Tx(journal,3);
//            
//            // data version visible in the new tx.
//            assertEquals( expected_id1_v0.array(), tx3.read(id1, null));
//
//            // data version still not visible in tx0.
//            assertNotFound( tx0.read(id1, null));
//
//            // data version still not visible in tx2.
//            assertNotFound( tx2.read(id1, null));
//            
//            // committed data version visible in tx3.
//            assertEquals( expected_id1_v0.array(), tx3.read(id1, null));
//
//            /*
//             * commit tx0 - nothing was written, no conflict should result.
//             */
//            tx0.prepare();
//            tx0.commit();
//
//            /*
//             * commit tx1 - nothing was written, no conflict should result.
//             */
//            tx2.prepare();
//            tx2.commit();
//
//            // abort tx2 - nothing was written, no conflict should result.
//            tx3.prepare();
//            tx3.commit();
//
//            // Still zero since the version committed by tx1 has not been
//            // overwritten.
//            assertVersionCounter(journal, id1, 0);
//
//            // data version in global scope was not changed by any other commit.
//            assertEquals( expected_id1_v0.array(), journal.read(id1, null));
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
//     * Test in which a transaction deletes a pre-existing version (that is, a
//     * version that existed in global scope when the transaction was started).
//     */
//    public void test_deletePreExistingVersion_noConflict() throws IOException {
//
//        final Properties properties = getProperties();
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            int id0 = 1;
//            
//            ByteBuffer expected_id0_v0 = getRandomData(journal);
//
//            // data version not visible in global scope.
//            assertNotFound(journal.read( id0, null));
//
//            // write data version in global scope.
//            journal.write(id0, expected_id0_v0);
//            assertVersionCounter(journal, id0, 0);
//
//            // data version visible in global scope.
//            assertEquals(expected_id0_v0.array(), journal.read(id0, null));
//
//            // start transaction.
//            Tx tx0 = new Tx(journal, id0);
//
//            // data version visible in global scope.
//            assertEquals(expected_id0_v0.array(), tx0.read(id0, null));
//
//            // delete version in transation scope.
//            tx0.delete(id0);
//            assertVersionCounter(tx0, id0, 0);
//            
//            // data version not visible in transaction.
//            assertDeleted(tx0, id0);
//            
//            // data version still visible in global scope.
//            assertEquals(expected_id0_v0.array(), journal.read(id0, null));
//
//            // prepare
//            tx0.prepare();
//
//            // commit.
//            tx0.commit();
//
//            assertVersionCounter(journal, id0, 1);
//
//            // data version now deleted in global scope.
//            assertDeleted(journal,id0);
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
//    /*
//     * read-write conflicts.
//     */
//
//    /**
//     * <p>
//     * Read-write conflicts result in the retention of old versions until the
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
//            ByteBuffer expected_id0_v0 = getRandomData(journal);
//            ByteBuffer expected_id0_v1 = getRandomData(journal);
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
//    
//    /*
//     * FIXME Write tests for write-write conflicts.
//     * 
//     * The basic test verifies that a conflict can be detected.
//     * 
//     * Then verify that we can resolve the conflict or fail validation.
//     * 
//     * Then verify that we can resolve the versions for the conflict (both the
//     * last committed version and the proposed version).
//     * 
//     * Then verify that we can make a resolution resulting in a new version and
//     * that the new version is, in fact, committed.
//     * 
//     * Then verify that we can handle the bank account example.
//     * 
//     * Do tests that verify that multiple conflicts are correctly detected and
//     * resolved.
//     * 
//     * Then verify that we can handle examples in which we have to traverse an
//     * object graph during conflict resolution. (Really, two object graphs: a
//     * readOnlyTx started from the last committed state and the readWriteTx that
//     * we are trying to validate.) This last issue is by far the trickyest and
//     * may require support for concurrent modification of the transaction's
//     * object index during traveral.
//     */
//
//    /**
//     * Test of write-write conflict resolution. A version (v0) of id0 is written
//     * in the global scope. Two transactions are then created (tx1, tx2). We
//     * verify that v0 is visible to both transactions. tx1 then overwrites v0
//     * with v1, prepares and commits. Since there are no intervening commits,
//     * tx1 MUST validate. tx2 then overwrites v0 with its own version of v1. tx2
//     * then prepares. Since tx1 committed while tx2 was active, the version
//     * counter on id0 for the global state has been incremented and is now
//     * different from the version counter visible to tx2. The difference in the
//     * counter valid is detected during validation and prompts conflict
//     * resolution.
//     * 
//     * @todo The various other tests in this suite also need to verify the
//     *       version counters.
//     * 
//     * @todo Do variant with more than one intervening overwrite and commit in
//     *       order to verify that the version counter is going up by one each
//     *       time when compare to the version counter in the global object index
//     *       NOT the version counter at the time that the transaction started.
//     *       This could be its own form of a stress test with N transactions
//     *       performing overwrites.
//     * 
//     * @todo Do variants in which a DELETE operation "overwrites" and commits
//     *       and verify that the write-write conflict is correctly detected.
//     *       This tests for correct handling of version counter increments when
//     *       the "write" is a "delete".
//     */
//    public void test_writeWriteConflict01() throws IOException {
//        
//        final Properties properties = getProperties();
//
//        /*
//         * Setup the conflict resolver.
//         */
//        properties.setProperty("conflictResolver",
//                RandomVersionSingletonConflictResolver.class.getName());
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            // verify the conflict resolver.
//            assertNotNull( journal.getConflictResolver() );
//            assertTrue(journal.getConflictResolver() instanceof RandomVersionSingletonConflictResolver);
//            final RandomVersionSingletonConflictResolver conflictResolver = (RandomVersionSingletonConflictResolver) journal
//                    .getConflictResolver();
//
//            final int id0 = 1;
//            
//            // Create random data for versions.
//            ByteBuffer expected_v0 = getRandomData(journal);
//            ByteBuffer expected_tx1_v1 = getRandomData(journal);
//            ByteBuffer expected_tx2_v1 = getRandomData(journal);
//
//            // data version not visible in global scope.
//            assertNotFound(journal.read( id0, null));
//
//            // write data version in global scope.
//            journal.write(id0, expected_v0);
//            
//            // verify the version counter in the global scope.
//            assertVersionCounter(journal,id0,0);
//
//            // data version visible in global scope.
//            assertEquals(expected_v0.array(), journal.read(id0, null));
//
//            // Save the v0 slot allocation.
//            final ISlotAllocation slots_v0 = journal.objectIndex.get(id0);
//            
//            // start transaction.
//            Tx tx1 = new Tx(journal, 1);
//
//            // start transaction.
//            Tx tx2 = new Tx(journal, 2);
//            
//            /*
//             * Verify that the data version is visible in both transactions.
//             */
//            assertEquals(expected_v0.array(), tx1.read(id0, null));
//            assertEquals(expected_v0.array(), tx2.read(id0, null));
//
//            // overwrite data version in transaction scope.
//            tx1.write(id0, expected_tx1_v1);
//
//            /*
//             * Verify the version counter in the transaction. The counter MUST
//             * have the same value as in the global scope from which this
//             * transaction was started.  The value of the counter is incremented
//             * in the global scope once (and IF) this transaction commits.
//             */
//            assertVersionCounter(tx1,id0,0);
//
//            // Save the tx1(v1) slot allocation.
//            final ISlotAllocation slots_tx1_v1 = tx1.getObjectIndex().get(id0);
//
//            // Verify read-back of the version.
//            assertEquals(expected_tx1_v1.array(),tx1.read(id0,null));
//            
//            // data version still visible in global scope.
//            assertEquals(expected_v0.array(), journal.read(id0, null));
//
//            // prepare
//            tx1.prepare();
//
//            // commit.
//            tx1.commit();
//
//            /*
//             * Verify the version counter in the global scope. It MUST have been
//             * incremented by one.
//             */
//            assertVersionCounter(journal, id0, 1);
//
//            // new data version now visible in global scope.
//            assertEquals(expected_tx1_v1.array(), journal.read(id0, null));
//
//            /*
//             * Verify that v0 is STILL allocated since the version can be read
//             * by tx2.
//             */
//            assertSlotAllocationState(slots_v0, journal.allocationIndex, true);
//
//            // Read the version in tx2 (just to prove that we can do it).
//            assertEquals(expected_v0.array(), tx2.read(id0, null));
//
//            /*
//             * Overwrite data version in transaction scope. This will produce a
//             * write-write conflict since tx1 has already overwritten the same
//             * version and committed.
//             */
//            tx2.write(id0, expected_tx2_v1);
//
//            /*
//             * Verify the version counter in the transaction. The counter MUST
//             * have the same value as in the global scope from which this
//             * transaction was started. The value of the counter is incremented
//             * in the global scope once (and IF) this transaction commits.
//             * 
//             * If we fail to do copy on write for the global index entry then a
//             * write by tx1 before a write by tx2 will not be noticed since the
//             * post-increment version counter will be visible. This assertion
//             * looks for this problem.
//             */
//            assertVersionCounter(tx2,id0,0);
//
//            /*
//             * Note: The counter in the global scope is currently ONE (1), but
//             * that is NOT the counter that is visible to the transaction!!!
//             */
//            assertVersionCounter(journal,id0,1);
//
//            // Save the tx2(v1) slot allocation.
//            final ISlotAllocation slots_tx2_v1 = tx2.getObjectIndex().get(id0);
//
//            // Verify read-back of the version written by the transaction.
//            assertEquals(expected_tx2_v1.array(),tx2.read(id0,null));
//
//            // Verify read-back of the version in the global scope (the one committed by tx1).
//            assertEquals(expected_tx1_v1.array(),journal.read(id0,null));
//
//            // Verify that v0 is STILL allocated since tx2 has not committed and we have not GC'd.
//            assertSlotAllocationState(slots_v0, journal.allocationIndex, true);
//
//            // Verify that tx1(v1) is STILL allocated - this is the last committed version.
//            assertSlotAllocationState(slots_tx1_v1, journal.allocationIndex, true);
//
//            // Prepare tx2.  This MUST detect the write-write conflict.
//            tx2.prepare();
//
//            // Verify that a write-write conflict was reported.
//            assertTrue("Conflict was not reported", conflictResolver.resolvedConflict());
//            
//            // Commit.
//            tx2.commit();
//
//            // Verify that v0 is STILL allocated since we have not GC'd.
//            assertSlotAllocationState(slots_v0, journal.allocationIndex, true);
//
//            // Verify that tx1(v1) is STILL allocated since we have not GC'd.
//            assertSlotAllocationState(slots_tx1_v1, journal.allocationIndex, true);
//
//            // Verify that tx2(v1) is STILL allocated - this is now the last committed version.
//            assertSlotAllocationState(slots_tx2_v1, journal.allocationIndex, true);
//
//            /*
//             * Verify the version counter in the global scope. It MUST have been
//             * incremented by one.
//             */
//            assertVersionCounter(journal, id0, 2);
//
//            // Verify read-back of the version in the global scope.
//            assertEquals(expected_tx2_v1.array(),journal.read(id0,null));
//
//            /*
//             * Verify read-back of the version in the global scope - this is the
//             * random data generated by the conflict resolver helper class. If
//             * you see the version that tx1 or the version that tx2 tried to
//             * write then either validation did not detect or failed to impose
//             * the resolved version on the journal.
//             */
//            assertEquals(conflictResolver.getResolvedVersion().array(), journal
//                    .read( id0, null));
//
//            /*
//             * Sweap the version overwritten by tx1 (v0).
//             */
//            tx1.gc();
//            
//            // Verify that v0 is now deallocated.
//            assertSlotAllocationState(slots_v0, journal.allocationIndex, false);
//
//            // Verify that tx1(v1) is STILL allocated since we have not GC'd tx2.
//            assertSlotAllocationState(slots_tx1_v1, journal.allocationIndex, true);
//
//            // Verify that tx2(v1) is STILL allocated - this is now the last committed version.
//            assertSlotAllocationState(slots_tx2_v1, journal.allocationIndex, true);
//
//            /*
//             * Sweap the version overwritten by tx2 (tx1_v1).
//             */
//            tx2.gc();
//
//            // Verify that v0 is still deallocated.
//            assertSlotAllocationState(slots_v0, journal.allocationIndex, false);
//
//            // Verify that tx1(v1) is now deallocated.
//            assertSlotAllocationState(slots_tx1_v1, journal.allocationIndex, false);
//
//            // Verify that tx2(v1) is STILL allocated - this is now the last committed version.
//            assertSlotAllocationState(slots_tx2_v1, journal.allocationIndex, true);
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
//     * <p>
//     * Helper class always throws an exception.  This may be used to verify that
//     * write-write conflicts are NOT reported when none are expected.
//     * </p>
//     * <p>
//     * Note: This class has to be static since otherwise there is an implicit
//     * outer instance parameter to the constructor that means that the class
//     * can not be instantiated correctly by the journal.
//     * </p>
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static class ConflictResolverAlwaysFails implements IConflictResolver {
//
////        private final Journal journal;
//        private int counter = 0;
//
//        /**
//         * The #of conflicts that have been presented to this conflict resolver.
//         * 
//         * @return The #of conflicts presented.
//         */
//        public int getCounter() {
//            
//            return counter;
//            
//        }
//        
//        public ConflictResolverAlwaysFails(Journal journal) {
//            
//            assert journal != null;
//            
////            this.journal = journal;
//            
//        }
//        
//        public void resolveConflict(int id, IStore readOnlyTx, IStore tx ) throws RuntimeException {
//
//            counter++;
//            
//            System.err.println("Refusing to resolve conflict: counter="+counter);
//            
//            throw new RuntimeException("Refusing to resolve conflict: counter="+counter);
//            
//        }
//        
//    }
//
//    /**
//     * <p>
//     * Helper class resolves a single conflict by writing an updated version.
//     * The class notes whether or not it has been invoked and reports the resolved
//     * version so that it may be verified on the journal.
//     * </p>
//     * <p>
//     * Note: This class has to be static since otherwise there is an implicit
//     * outer instance parameter to the constructor that means that the class
//     * can not be instantiated correctly by the journal.
//     * </p>
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static class RandomVersionSingletonConflictResolver implements IConflictResolver {
//
//        private final Journal journal;
//        private ByteBuffer randomData = null;
//        
//        /**
//         * A random number generated - the seed is NOT fixed.
//         */
//        private Random r = new Random();
//
//        /**
//         * Returns random data that will fit in N slots. N is choosen randomly,
//         * the slotSize is assumed to be 128, and then the actual length is
//         * choosen randomly within that slot.
//         * 
//         * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code>
//         *         of random length and having random contents.
//         * 
//         * @see AbstractTestCase#getRandomData(Journal)
//         */
//        private ByteBuffer getRandomData(Journal journal) {
//            
//            final int slotSize = journal.slotMath.slotSize;
//            
//            final int nslots = r.nextInt(5)+1;
//            
//            final int nbytes = ((nslots - 1) * slotSize)
//                    + r.nextInt(slotSize) + 1;
//            
//            byte[] bytes = new byte[nbytes];
//            
//            r.nextBytes(bytes);
//            
//            return ByteBuffer.wrap(bytes);
//            
//        }
//
//        public RandomVersionSingletonConflictResolver(Journal journal) {
//            
//            assert journal != null;
//            
//            this.journal = journal;
//            
//        }
//
//        /**
//         * True iff a conflict has been reported (and hence resolve).
//         * 
//         * @return
//         */
//        public boolean resolvedConflict() {
//            
//            return randomData != null;
//            
//        }
//
//        /**
//         * Return the resolved data version.
//         * 
//         * @return
//         */
//        public ByteBuffer getResolvedVersion() {
//           
//            // fail the test if the resolver has not been invoked.
//            assertNotNull( "conflict was not reported", randomData );
//            
//            return randomData;
//            
//        }
//        
//        /**
//         * Resolve the conflict by creating a random data version.
//         */
//        public void resolveConflict(int id, IStore readOnlyTx, IStore tx)
//                throws RuntimeException {
// 
//            if( randomData != null ) {
//                
//                fail("Already invoked once.");
//                
//            }
//
//            System.err.println("Random resolution of conflict.");
//            
//            randomData = getRandomData(journal);
//            
//            tx.write(id, randomData);
//            
//        }
//        
//    }
        
}
