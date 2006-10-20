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
 * @todo Work through tests of the commit logic and verify the post-conditions
 *       for successful commit vs abort of a transaction.
 * 
 * @todo Work through backward validatation, data type specific state based
 *       conflict resolution, and merging down the object indices onto the
 *       journal during the commit.
 * 
 * @todo Show that abort does not leave anything lying around, both that would
 *       break isolation (unlikely) or just junk that lies around unreclaimed on
 *       the slots (or in the index nodes themselves).
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
     * Test verifies some aspects of transactional isolation. A transaction
     * (tx0) is created from a journal with nothing written on it. A data
     * version is then written onto the journal outside of the transactional
     * scope and we verify that the version is visible on the journal but not in
     * tx0. Another transaction (tx1) is created and we version that the written
     * version is visible. We then update the version on the journal and verify
     * that they change is NOT visible to either transaction. We then delete the
     * version on the journal and verify that the change is not visible to
     * either transaction. A 2nd version is then written in both tx0 and tx1 and
     * everything is reverified. The version is then deleted on tx1
     * (reverified). A 3rd version is written on tx0 (reverified). Finally, we
     * delete the version on tx0 (reverified). At this point the most recent
     * version has been deleted on the journal and in both transactions.
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
             * @todo Since commit processing is not implemented, we can not go a
             * lot further with this test.
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
     * 
     * @todo detect duplicate transaction identifiers.
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
     * Simple test of commit semantics. Three transactions are started: tx0, on
     * which we will write one data version; tx1, which begins after tx0 but
     * before tx0 commits - the change will NOT be visible in this transaction;
     * and tx2, which begins after the commit - the change will be visible in
     * this transaction.
     * 
     * @todo Add another tx that starts "before" tx0 and verify that the change
     *       is NOT visible in that transaction. This will actually require
     *       renumbering the transactions. This could also be written as another
     *       test.
     * 
     * @todo We already have tests that create read-write conflicts above -
     *       modify them to do a commit or abort as necessary.
     * 
     * @todo Write tests that create write-write conflicts to verify that they
     *       can be detected and provide versions of those tests where the
     *       conflict and can not be validated and verify the end state in each
     *       case.
     * 
     * @todo Verify correct abort after 'prepare'.
     * 
     * @todo Issue warning or throw exception when closing journal with active
     *       transactions? Provide a 'force' and timeout option? This was all
     *       implemented for the DBCache implementation.
     */
    public void test_commit01() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            // transaction on which we write and later commit.
            Tx tx0 = new Tx(journal,0);
            
            // new transaction - commit will not be visible in this scope.
            Tx tx1 = new Tx(journal,1);
                        
            ByteBuffer expected_id0_v0 = getRandomData(journal);
            
            // write data version on tx0
            tx0.write(  0, expected_id0_v0 );

            // data version visible in tx0.
            assertEquals( expected_id0_v0.array(), tx0.read(0, null));

            // data version not visible in global scope.
            assertNotFound( journal.read(null, 0, null));

            // data version not visible in tx1.
            assertNotFound( tx1.read(0, null));

            // prepare
            tx0.prepare();

            // commit.
            tx0.commit();

            // data version now visible in global scope.
            assertEquals( expected_id0_v0.array(), journal.read(null,0, null));

            // new transaction - commit is visible in this scope.
            Tx tx2 = new Tx(journal,2);
            
            // data version now visible in global scope.
            assertEquals( expected_id0_v0.array(), tx2.read(0, null));

            // data version still not visible in tx1.
            assertNotFound( tx1.read(0, null));
            
            // committed data version visible in tx2.
            assertEquals( expected_id0_v0.array(), tx2.read(0, null));

            /*
             * abort tx1 - nothing was written.
             */
            tx1.abort();

            // abort tx2 - nothing was written.
            tx2.abort();
            
            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }

    }
    
}
