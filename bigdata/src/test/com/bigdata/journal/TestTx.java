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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * Test suite for transaction isolation with respect to the underlying journal.
 * The tests in this suite are designed to verify isolation of changes within
 * the scope of the transaction when compared to the last committed state of the
 * journal. This basically amounts to verifying that operations read through the
 * transaction scope object index into the journal scope object index.
 * 
 * @todo Do stress test with writes, reads, and deletes.
 * 
 * @todo Write lots of tests to handle the edge cases for overwrite and/or
 *       delete of pre-existing versions and versions that exist only within a
 *       tx scope.
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
 *       declaration.
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
    
    /**
     * 
     */
    public TestTx() {
    }

    public TestTx(String name) {
        super(name);
    }

    /**
     * Test verifiers that duplicate transaction identifiers are detected in the
     * case where the first transaction is active.
     */

    public void test_duplicateTransactionIdentifiers01() throws IOException {
        
        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);

        try {

            Journal journal = new Journal(properties);

            new Tx(journal,0);

            try {

                // Try to create another transaction with the same identifier.
                new Tx(journal,0);
                
                fail( "Expecting: "+IllegalStateException.class);
                
            }
            
            catch( IllegalStateException ex ) {
             
                System.err.println("Ignoring expected exception: "+ex);
                
            }
            
            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }       
        
    }
    
    /**
     * Test verifiers that duplicate transaction identifiers are detected in the
     * case where the first transaction has already prepared.
     * 
     * @todo The {@link Journal} does not maintain a collection of committed
     *       transaction identifier for transactions that have already
     *       committed. However, it might make sense to maintain a transient
     *       collection that is rebuilt on restart of those transactions that
     *       are waiting for GC. Also, it may be possible to summarily reject
     *       transaction identifiers if they are before a timestamp when a
     *       transaction service has notified the journal that no active
     *       transactions remain before that timestamp.  If those modifications
     *       are made, then add the appropriate tests here.
     */
    public void test_duplicateTransactionIdentifiers02() throws IOException {
        
        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);

        try {

            Journal journal = new Journal(properties);

            Tx tx0 = new Tx(journal,0);

            tx0.prepare();
            
            try {

                // Try to create another transaction with the same identifier.
                new Tx(journal,0);
                
                fail( "Expecting: "+IllegalStateException.class);
                
            }
            
            catch( IllegalStateException ex ) {
             
                System.err.println("Ignoring expected exception: "+ex);
                
            }
            
            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }       
        
    }
    
    //
    // Delete object.
    //

    /**
     * Two transactions (tx0, tx1) are created. A version (v0) is written onto
     * tx0 for a persistent identifier. The test verifies the write and verifies
     * that the write is not visible in tx1. The v0 is then deleted from tx0.
     * Since no version ever existing in the global scope for that persistent
     * identifier, the test verifies that the slots allocated to the version
     * were immediately deallocated when the version was deleted. Tx0 and tx1
     * are then committed.
     * 
     * @todo Do some more simple tests where a few objects are written, read
     *       back, deleted one by one, and verify that they can no longer be
     *       read.
     * 
     * FIXME Write a version of this test where the object is pre-existing in
     * the global state and then deleted within the transaction. The delete MUST
     * NOT be visible to a concurrent transaction. A GC after the transactions
     * commit should cause the pre-existing version to be deallocated.
     */
    
    public void test_delete001() throws IOException {

        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);

        try {

            Journal journal = new Journal(properties);

            Tx tx0 = new Tx(journal,0);

            Tx tx1 = new Tx(journal,1);

            /*
             * Write v0 on tx0.
             */
            final int id0 = 0;
            final ByteBuffer expected_id0_v0 = getRandomData(journal);
            tx0.write(id0, expected_id0_v0);
            assertEquals(expected_id0_v0.array(),tx0.read(id0, null));

            /*
             * Verify that the version does NOT show up in a concurrent
             * transaction. If the version shows up here it most likely means
             * that the transaction is reading from the current object index
             * state, rather than from the object index state at the time that
             * the transaction began.
             */
            assertNotFound(tx1.read(id0, null));

            // The slot allocation for the version that we are about to delete.
            final ISlotAllocation slots = tx0.objectIndex.getSlots(id0);
            assertNotNull(slots);
            assertSlotAllocationState(slots, journal.allocationIndex, true);
            
            // delete the version.
            tx0.delete(id0);

            /*
             * Since the version only existed within the transaction, verify
             * that the slots were synchronously deallocated when the version
             * was deleted.
             */
            assertSlotAllocationState(slots, journal.allocationIndex, false);

            // Verify the persistent identifier is now correctly marked as
            // deleted in the transaction's object index.
            try {
                tx0.objectIndex.getSlots(id0);
                fail("Expecting: "+DataDeletedException.class);
            }
            catch(DataDeletedException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            /*
             * Test read after delete.
             */
            assertDeleted(tx0,id0);

            /*
             * Test delete after delete.
             */
            try {
                
                tx0.delete(id0);

                fail("Expecting " + DataDeletedException.class);
                
            } catch (DataDeletedException ex) {
                
                System.err.println("Ignoring expected exception: " + ex);
                
            }

            /*
             * Test write after delete.
             */
            try {
                
                tx0.write(id0, getRandomData(journal));

                fail("Expecting " + DataDeletedException.class);
                
            } catch (DataDeletedException ex) {
                
                System.err.println("Ignoring expected exception: " + ex);
                
            }

            // Still not visible in concurrent transaction.
            assertNotFound(tx1.read(id0, null));

            // Still not visible in global scope.
            assertNotFound(journal.read(null, id0, null));

            tx0.prepare();
            tx0.commit();

            // Still not visible in concurrent transaction.
            assertNotFound(tx1.read(id0, null));

            // Still not visible in global scope.
            assertNotFound(journal.read(null, id0, null));

            tx1.prepare();
            tx1.commit();

            // Still not visible in global scope.
            assertNotFound(journal.read(null, id0, null));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }

    }
    
    //
    // Isolation.
    //
    
    /**
     * Test verifies some aspects of transactional isolation. A transaction
     * (tx0) is created from a journal with nothing written on it. A data
     * version (v0) is then written onto the journal outside of the
     * transactional scope and we verify that the version is visible on the
     * journal but not in tx0. Another transaction (tx1) is created and we
     * version that the written version is visible. We then update the version
     * on the journal and verify that the change is NOT visible to either
     * transaction. We then delete the version on the journal and verify that
     * the change is not visible to either transaction. A 2nd version is then
     * written in both tx0 and tx1 and everything is reverified. The version is
     * then deleted on tx1 (reverified). A 3rd version is written on tx0
     * (reverified). Finally, we delete the version on tx0 (reverified). At this
     * point the most recent version has been deleted on the journal and in both
     * transactions.
     * 
     * FIXME This test depends on some edge features (the ability to write in
     * the global scope while concurrent transactions are running). Write a
     * variant that does not use that feature.
     */

    public void test_isolation001() throws IOException {
        
        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            // Transaction begins before the write.
            Tx tx0 = new Tx(journal,0);

            // Write a random data version for id 0.
            final int id0 = 0;
            final ByteBuffer expected_id0_v0 = getRandomData(journal);
            journal.write(null, id0, expected_id0_v0);
            assertEquals(expected_id0_v0.array(),journal.read(null, id0, null));
            final ISlotAllocation slots_v0 = journal.objectIndex.getSlots(0);

            /*
             * Verify that the version does NOT show up in a transaction created
             * before the write. If the version shows up here it most likely
             * means that the transaction is reading from the current object
             * index state, rather than from the object index state at the time
             * that the transaction began.
             */
            assertNotFound(tx0.read(id0, null));

            // Transaction begins after the write.
            Tx tx1 = new Tx(journal,1);

            /*
             * Verify that the version shows up in a transaction created after
             * the write.
             */
            assertEquals(expected_id0_v0.array(),tx1.read(id0, null));

            /*
             * Update the version outside of the transaction.  This change SHOULD
             * NOT be visible to either transaction.
             */
            final ByteBuffer expected_id0_v1 = getRandomData(journal);
            journal.write(null, id0, expected_id0_v1);
//            final ISlotAllocation slots_v1 = journal.objectIndex.getSlots(0);
            /*
             * FIXME This is failing because the journal is not looking for
             * whether or not concurrent transactions are running. When they are
             * we can not immediately deallocate the slots for a version
             * overwritten in the global scope. Those slot allocations need to
             * be queued up on the global object index for eventual
             * deallocation. That deallocation can not occur until all
             * transactions which can read that version have prepared/aborted.
             * This entire feature (updating the global scope outside of a
             * transaction) is a bit edgy and needs more thought.
             */
            assertEquals(slots_v0,journal.objectIndex.getSlots(0));
            assertEquals(expected_id0_v1.array(),journal.read(null, id0, null));
            assertNotFound(tx0.read(id0, null));
            assertEquals(expected_id0_v0.array(),tx1.read(id0, null));

            /*
             * Delete the version on the journal. This change SHOULD NOT be
             * visible to either transaction.
             */
            journal.delete(null, id0);
            assertDeleted(journal, id0);
            assertNotFound(tx0.read(id0, null));
            assertEquals(expected_id0_v0.array(),tx1.read(id0, null));

            /*
             * Write a version on tx1 and verify that we read that version from
             * tx1 rather than the version written in the journal scope before
             * the transaction began. Verify that the written version does not
             * show up either on the journal or in tx1.
             */
            final ByteBuffer expected_tx1_id0_v0 = getRandomData(journal);
            tx1.write(id0, expected_tx1_id0_v0);
            assertDeleted(journal, id0);
            assertNotFound(tx0.read(id0, null));
            assertEquals(expected_tx1_id0_v0.array(),tx1.read(id0, null));

            /*
             * Write a version on tx0 and verify that we read that version from
             * tx0 rather than the version written in the journal scope before
             * the transaction began. Verify that the written version does not
             * show up either on the journal or in tx1.
             */
            final ByteBuffer expected_tx0_id0_v0 = getRandomData(journal);
            tx0.write(id0, expected_tx0_id0_v0);
            assertDeleted(journal, id0);
            assertEquals(expected_tx0_id0_v0.array(),tx0.read(id0, null));
            assertEquals(expected_tx1_id0_v0.array(),tx1.read(id0, null));

            /*
             * Write a 2nd version on tx0 and reverify.
             */
            final ByteBuffer expected_tx0_id0_v1 = getRandomData(journal);
            tx0.write(id0, expected_tx0_id0_v1);
            assertDeleted(journal, id0);
            assertEquals(expected_tx0_id0_v1.array(),tx0.read(id0, null));
            assertEquals(expected_tx1_id0_v0.array(),tx1.read(id0, null));

            /*
             * Write a 2nd version on tx1 and reverify.
             */
            final ByteBuffer expected_tx1_id0_v1 = getRandomData(journal);
            tx1.write(id0, expected_tx1_id0_v1);
            assertDeleted(journal, id0);
            assertEquals(expected_tx0_id0_v1.array(),tx0.read(id0, null));
            assertEquals(expected_tx1_id0_v1.array(),tx1.read(id0, null));

            /*
             * Delete the version on tx1 and reverify.
             */
            tx1.delete(id0);
            assertDeleted(journal, id0);
            assertEquals(expected_tx0_id0_v1.array(),tx0.read(id0, null));
            assertDeleted(tx1, id0);

            /*
             * Write a 3rd version on tx0 and reverify.
             */
            final ByteBuffer expected_tx0_id0_v2 = getRandomData(journal);
            tx0.write(id0, expected_tx0_id0_v2);
            assertDeleted(journal, id0);
            assertEquals(expected_tx0_id0_v2.array(),tx0.read(id0, null));
            assertDeleted(tx1, id0);
            
            /*
             * Delete the version on tx0 and reverify.
             */
            tx0.delete(id0);
            assertDeleted(journal, id0);
            assertDeleted(tx0, id0);
            assertDeleted(tx1, id0);

            /*
             * @todo Define the outcome of validation if tx0 and tx1 commit in
             * this scenario. I would think that the commits would validate
             * since no version of the data exists either on the journal in
             * global scope or on either transaction. The only reason why this
             * might be problematic is that we have allowed a change made
             * directly to the global scope while transactions are running.
             */
            
            journal.close();
            
        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }
    
    /**
     * Test writes multiple versions and verifies that the correct version may
     * be read back at any time. The last written version is then deleted and we
     * verify that read, write and delete operations all correctly report that
     * the data is deleted.
     * 
     * @throws IOException
     * 
     * FIXME Verify that we are immediately deallocating slots for the
     * historical versions.
     * 
     * FIXME When a version exists on the journal before the transaction starts
     * only the 2nd write should cause the prior version to be immediately
     * deallocated.  (This could be its own test.)
     */
    public void test_writeMultipleVersions() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            Tx tx0 = new Tx(journal,0);
            
            // Two versions of id0.
            final int id0 = 0;
            final ByteBuffer expected0v0 = getRandomData(journal);
            final ByteBuffer expected0v1 = getRandomData(journal);
            
            // Three versions of id1.
            final int id1 = 1;
            final ByteBuffer expected1v0 = getRandomData(journal);
            final ByteBuffer expected1v1 = getRandomData(journal);
            final ByteBuffer expected1v2 = getRandomData(journal);
            
            // precondition tests, write id0 version0, postcondition tests.
            assertNotFound(tx0.read(id0,null));
            
            assertNotFound(tx0.read(id1,null));

            tx0.write(id0,expected0v0);
            
            assertEquals(expected0v0.array(),tx0.read(id0, null));
            
            assertNotFound(tx0.read(id1,null));

            // write id1 version0, postcondition tests.
            tx0.write(id1,expected1v0);
            
            assertEquals(expected0v0.array(),tx0.read(id0, null));
            
            assertEquals(expected1v0.array(),tx0.read(id1, null));
            
            // write id1 version1, postcondition tests.
            tx0.write(id1,expected1v1);
            
            assertEquals(expected0v0.array(),tx0.read( id0, null));
            
            assertEquals(expected1v1.array(),tx0.read( id1, null));
            
            // write id1 version2, postcondition tests.
            tx0.write(id1,expected1v2);
            
            assertEquals(expected0v0.array(),tx0.read( id0, null));
            
            assertEquals(expected1v2.array(),tx0.read( id1, null));

            // write id0 version1, postcondition tests.
            tx0.write(id0,expected0v1);
            
            assertEquals(expected0v1.array(),tx0.read(id0, null));
            
            assertEquals(expected1v2.array(),tx0.read(id1, null));

            // delete id1, postcondition tests.

            tx0.delete(id1);
            
            assertEquals(expected0v1.array(),tx0.read(id0, null));
            
            assertDeleted(tx0, id1);

            // delete id0, postcondition tests.

            tx0.delete(id0);
            
            assertDeleted(tx0, id0);
            
            assertDeleted(tx0, id1);

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }
    
    /*
     * Transaction run state tests.
     */

    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activeAbort() throws IOException {
        
        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            Tx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));
            
            tx0.abort();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }
    
    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activePrepareAbort() throws IOException {
        
        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            Tx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));
            
            tx0.prepare();

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.prepareTx.containsKey(ts0));

            tx0.abort();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }

    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activePrepareCommit() throws IOException {
        
        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            Tx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));
            
            tx0.prepare();

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.prepareTx.containsKey(ts0));

            tx0.commit();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertTrue( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }

    /**
     * Simple test of the transaction run state machine. verifies that a 2nd
     * attempt to abort the same transaction results in an exception that does
     * not change the transaction run state.
     */
    public void test_runStateMachine_activeAbortAbort_correctRejection() throws IOException {
        
        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            Tx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));
            
            tx0.abort();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            try {
                tx0.abort();
                fail("Expecting: "+IllegalStateException.class);
            }
            catch( IllegalStateException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }

    /**
     * Simple test of the transaction run state machine verifies that a 2nd
     * attempt to prepare the same transaction results in an exception that
     * changes the transaction run state to 'aborted'.
     */
    public void test_runStateMachine_activePreparePrepare_correctRejection() throws IOException {
        
        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            Tx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));
            
            tx0.prepare();

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.prepareTx.containsKey(ts0));

            try {
                tx0.prepare();
                fail("Expecting: "+IllegalStateException.class);
            }
            catch( IllegalStateException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }

    /**
     * Simple test of the transaction run state machine verifies that a commit
     * out of order results in an exception that changes the transaction run
     * state to 'aborted'.
     */
    public void test_runStateMachine_activeCommit_correctRejection() throws IOException {
        
        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            long ts0 = 0;
            
            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            Tx tx0 = new Tx(journal,ts0);
            assertEquals(ts0,tx0.getId());
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));
            
            try {
                tx0.commit();
                fail("Expecting: "+IllegalStateException.class);
            }
            catch( IllegalStateException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }

    /**
     * Verifies that a READ is not permitted after a PREPARE and that the
     * attempt results in an aborted transaction.
     * 
     * @throws IOException
     */
    public void test_runStateMachine_prepareRead_correctRejection() throws IOException {

        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);

        try {

            Journal journal = new Journal(properties);

            final long ts0 = 0;
            
            Tx tx0 = new Tx(journal, ts0);

            tx0.prepare();

            // can not read, write, delete or prepare after 'prepare'.
            try {
                tx0.read(0, null);
                fail("Expecting: " + IllegalStateException.class);
            } catch (IllegalStateException ex) {
                System.err.println("Ignoring expected exception: " + ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }

    }
            
    /**
     * Verifies that a WRITE is not permitted after a PREPARE and that the
     * attempt results in an aborted transaction.
     * 
     * @throws IOException
     */
    public void test_runStateMachine_prepareWrite_correctRejection() throws IOException {

        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);

        try {

            Journal journal = new Journal(properties);

            final long ts0 = 0;
            
            Tx tx0 = new Tx(journal, ts0);

            tx0.prepare();

            // can not read, write, delete or prepare after 'prepare'.
            try {
                tx0.write(0, getRandomData(journal));
                fail("Expecting: " + IllegalStateException.class);
            } catch (IllegalStateException ex) {
                System.err.println("Ignoring expected exception: " + ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }

    }

    /**
     * Verifies that a DELETE is not permitted after a PREPARE and that the
     * attempt results in an aborted transaction.
     * 
     * @throws IOException
     */
    public void test_runStateMachine_prepareDelete_correctRejection() throws IOException {

        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);

        try {

            Journal journal = new Journal(properties);

            final long ts0 = 0;
            
            Tx tx0 = new Tx(journal, ts0);

            tx0.prepare();

            // can not read, write, delete or prepare after 'prepare'.
            try {
                tx0.delete(0);
                fail("Expecting: " + IllegalStateException.class);
            } catch (IllegalStateException ex) {
                System.err.println("Ignoring expected exception: " + ex);
            }

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.prepareTx.containsKey(ts0));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }

    }

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
    
    public void test_commit_noConflict01() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            /*
             * Transaction that starts before the transaction on which we write.
             * The change will not be visible in this scope.
             */
            Tx tx0 = new Tx(journal,0);
            
            // transaction on which we write and later commit.
            Tx tx1 = new Tx(journal,1);
            
            // new transaction - commit will not be visible in this scope.
            Tx tx2 = new Tx(journal,2);
                        
            ByteBuffer expected_id0_v0 = getRandomData(journal);
            
            // write data version on tx1
            tx1.write(  0, expected_id0_v0 );

            // data version visible in tx1.
            assertEquals( expected_id0_v0.array(), tx1.read(0, null));

            // data version not visible in global scope.
            assertNotFound( journal.read(null, 0, null));

            // data version not visible in tx0.
            assertNotFound( tx0.read(0, null));

            // data version not visible in tx2.
            assertNotFound( tx2.read(0, null));

            // prepare
            tx1.prepare();

            // commit.
            tx1.commit();

            // data version now visible in global scope.
            assertEquals( expected_id0_v0.array(), journal.read(null,0, null));

            // new transaction - commit is visible in this scope.
            Tx tx3 = new Tx(journal,3);
            
            // data version visible in the new tx.
            assertEquals( expected_id0_v0.array(), tx3.read(0, null));

            // data version still not visible in tx0.
            assertNotFound( tx0.read(0, null));

            // data version still not visible in tx2.
            assertNotFound( tx2.read(0, null));
            
            // committed data version visible in tx3.
            assertEquals( expected_id0_v0.array(), tx3.read(0, null));

            /*
             * commit tx0 - nothing was written, no conflict should result.
             */
            tx0.prepare();
            tx0.commit();

            /*
             * commit tx1 - nothing was written, no conflict should result.
             */
            tx2.prepare();
            tx2.commit();

            // abort tx2 - nothing was written, no conflict should result.
            tx3.prepare();
            tx3.commit();

            // data version in global scope was not changed by any other commit.
            assertEquals( expected_id0_v0.array(), journal.read(null,0, null));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }

    /**
     * Test in which a transaction deletes a pre-existing version (that is, a
     * version that existed in global scope when the transaction was started).
     */
    public void test_deletePreExistingVersion_noConflict() throws IOException {

        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);

        try {

            Journal journal = new Journal(properties);

            ByteBuffer expected_id0_v0 = getRandomData(journal);

            // data version not visible in global scope.
            assertNotFound(journal.read(null, 0, null));

            // write data version in global scope.
            journal.write(null,0, expected_id0_v0);

            // data version visible in global scope.
            assertEquals(expected_id0_v0.array(), journal.read(null,0, null));

            // start transaction.
            Tx tx0 = new Tx(journal, 0);

            // data version visible in global scope.
            assertEquals(expected_id0_v0.array(), tx0.read(0, null));

            // delete version in transation scope.
            tx0.delete(0);
            
            // data version not visible in transaction.
            assertDeleted(tx0, 0);
            
            // data version still visible in global scope.
            assertEquals(expected_id0_v0.array(), journal.read(null,0, null));

            // prepare
            tx0.prepare();

            // commit.
            tx0.commit();

            // data version now deleted in global scope.
            assertDeleted(journal,0);

            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }

    }

    /*
     * read-write conflicts.
     */

    /**
     * <p>
     * Read-write conflicts result in the retention of old versions until the
     * readers complete. This test writes a data version (v0) for id0 in the
     * global scope on the journal. Two transactions are then created, tx1 and
     * tx2. tx1 writes a new version and then commits. Since tx2 still has the
     * ability to read the prior version, the prior version MUST remain
     * allocated on the journal and accessable via the tx2 object index. Tx2
     * commits. Since there is no longer any transaction that can see v0, we
     * invoke garbage collection on tx1, which causes the overwritten version to
     * be deallocated.
     * </p>
     */
    public void test_readWriteConflict01() throws IOException {

        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);

        try {

            Journal journal = new Journal(properties);

            ByteBuffer expected_id0_v0 = getRandomData(journal);
            ByteBuffer expected_id0_v1 = getRandomData(journal);

            // data version not visible in global scope.
            assertNotFound(journal.read(null, 0, null));

            /*
             * write data version in global scope.
             */
            journal.write(null,0, expected_id0_v0);

            // The slots on which the first data version is written.
            final ISlotAllocation slots_v0 = journal.objectIndex.getSlots(0);
            
            // Verify the data version is visible in global scope.
            assertEquals(expected_id0_v0.array(), journal.read(null,0, null));

            // start transaction.
            Tx tx1 = new Tx(journal, 1);

            // start transaction.
            Tx tx2 = new Tx(journal, 2);
            
            /*
             * Verify that the data version is visible in the transaction scope
             * where it will be overwritten. Note that we do NOT test the other
             * concurrent transaction since actually reading the version in that
             * transaction might trigger different code paths.
             */
            assertEquals(expected_id0_v0.array(), tx1.read(0, null));

            // overwrite data version in transaction scope.
            tx1.write(0, expected_id0_v1);
            
            // slot allocation in global scope is unchanged.
            assertEquals(slots_v0,journal.objectIndex.getSlots(0));

            // data version in global scope is unchanged.
            assertEquals(expected_id0_v0.array(), journal.read(null,0, null));

            // Get the slots on which the 2nd data version was written.
            final ISlotAllocation slots_v1 = tx1.objectIndex.getSlots(0);

            // prepare
            tx1.prepare();

            // commit.
            tx1.commit();

            // The v0 slots are still allocated.
            assertSlotAllocationState(slots_v0, journal.allocationIndex,true);

            // The v1 slots are still allocated.
            assertSlotAllocationState(slots_v1, journal.allocationIndex,true);

            // The entry in the scope is consistent with the v1 allocation.
            assertEquals(slots_v1,journal.objectIndex.getSlots(0));

            // new data version now visible in global scope.
            assertEquals(expected_id0_v1.array(), journal.read(null,0, null));

            // The entry in the tx2 object index is consistent with the v0
            // allocatation (it was not overwritten when the tx1 committed).
            assertEquals(slots_v0,tx2.objectIndex.getSlots(0));

            // Read the version in tx2 (just to prove that we can do it).
            assertEquals(expected_id0_v0.array(), tx2.read(0, null));

            // Commit tx2 (we could have as easily aborted tx2 for this test).
            tx2.prepare();
            tx2.commit();
            
            /*
             * Sweap overwritten versions written by or visible to tx1 and
             * earlier transactions - this MUST deallocate the overwritten
             * version (v0).
             * 
             * Note: This method MUST NOT be invoked while tx2 is active since
             * tx2 has visiblity onto the same ground state as tx1.
             */
            tx1.gc();

            // The v0 slots are now deallocated.
            assertSlotAllocationState(slots_v0, journal.allocationIndex,false);

            // The v1 slots are still allocated.
            assertSlotAllocationState(slots_v1, journal.allocationIndex,true);

            // The entry in the global object index is consistent with the v1
            // allocatation.
            assertEquals(slots_v1,journal.objectIndex.getSlots(0));

            // The v1 version is still visible in global scope.
            assertEquals(expected_id0_v1.array(), journal.read(null,0, null));

            /*
             * GC(tx2) - this MUST be a NOP.
             */
            tx2.gc();
            
            // The v0 slots are still deallocated.
            assertSlotAllocationState(slots_v0, journal.allocationIndex,false);

            // The v1 slots are still allocated.
            assertSlotAllocationState(slots_v1, journal.allocationIndex,true);

            // The entry in the global object index is consistent with the v1
            // allocatation.
            assertEquals(slots_v1,journal.objectIndex.getSlots(0));

            // The v1 version is still visible in global scope.
            assertEquals(expected_id0_v1.array(), journal.read(null,0, null));
            
            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }

    }
    
    /*
     * FIXME Write tests for write-write conflicts.
     */

//    /**
//     * Test of write-write conflict resolution. A version (v0) is written in the
//     * global scope. Two transactions are then create (tx1, tx2). We verify that
//     * v0 is visible to tx1 and then we overwrite it in that transaction.
//     */
//    public void test_readWriteConflict02() throws IOException {
//        
//        final Properties properties = getProperties();
//
//        final String filename = getTestJournalFile();
//
//        properties.setProperty("file", filename);
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            ByteBuffer expected_id0_v0 = getRandomData(journal);
//            ByteBuffer expected_id0_v1 = getRandomData(journal);
//
//            // data version not visible in global scope.
//            assertNotFound(journal.read(null, 0, null));
//
//            /*
//             * write data version in global scope.
//             */
//            journal.write(null,0, expected_id0_v0);
//
//            // data version visible in global scope.
//            assertEquals(expected_id0_v0.array(), journal.read(null,0, null));
//
//            // Save the v0 slot allocation.
//            final ISlotAllocation slots_v0 = journal.objectIndex.getSlots(0);
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
//            assertEquals(expected_id0_v0.array(), tx1.read(0, null));
//
//            // overwrite data version in transaction scope.
//            tx1.write(0, expected_id0_v1);
//            
//            // data version still visible in global scope.
//            assertEquals(expected_id0_v0.array(), journal.read(null,0, null));
//
//            // prepare
//            tx1.prepare();
//
//            // commit.
//            tx1.commit();
//
//            // new data version now visible in global scope.
//            assertEquals(expected_id0_v1.array(), journal.read(null,0, null));
//
//            /*
//             * Verify that v0 is STILL allocated since the version can be read
//             * by tx2.
//             */
//            assertSlotAllocationState(slots_v0, journal.allocationIndex, true);
//
//            // Read the version in tx2 (just to prove that we can do it).
//            assertEquals(expected_id0_v0.array(), tx2.read(0, null));
//
//            // Commit tx2 (we could have as easily aborted tx2 for this test).
//            tx2.prepare();
//            tx2.commit();
//            
//            /*
//             * Sweap overwritten versions written by or visible to tx1 and
//             * earlier transactions - this should be a NOP.
//             */
//            assertEquals(0,journal.gcTx(tx1.getId()));
//
//            /*
//             * Verify that the slot is STILL allocated since we have not swept
//             * up to tx2.
//             */
//            assertTrue(journal.allocationIndex.isAllocated(firstSlot));
//
//            /*
//             * Sweap overwritten versions written by or visible to tx2 and
//             * earlier transactions - this should sweapt the versions written
//             * onto the journal in the global scope before tx2 executed.
//             */
//            assertEquals(1,journal.gcTx(tx2.getId()));
//
//            /*
//             * Verify that the slot is not deallocated.
//             */
//            assertFalse(journal.allocationIndex.isAllocated(firstSlot));
//
//            journal.close();
//
//        } finally {
//
//            deleteTestJournalFile(filename);
//
//        }
//
//    }

}
