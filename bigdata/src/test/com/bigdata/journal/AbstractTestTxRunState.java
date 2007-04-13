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

package com.bigdata.journal;

import java.io.IOException;
import java.util.UUID;

import junit.framework.TestSuite;

import com.bigdata.btree.IIndex;
import com.bigdata.isolation.UnisolatedBTree;

/**
 * Test suite for the state machine governing the transaction {@link RunState}
 * transitions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractTestTxRunState extends ProxyTestCase {

    public static TestSuite suite() {
        
        TestSuite suite = new TestSuite("Transaction run state");

        suite.addTestSuite(TestReadCommitted.class);
        suite.addTestSuite(TestReadOnly.class);
        suite.addTestSuite(TestReadWrite.class);
        
        return suite;
        
    }
    
    /**
     * 
     */
    public AbstractTestTxRunState() {
    }

    /**
     * @param name
     */
    public AbstractTestTxRunState(String name) {
        super(name);
    }

    /**
     * Return a new transaction start time.
     */
    abstract public long newTx(Journal journal);
    
    public static class TestReadCommitted extends AbstractTestTxRunState {
        
        public TestReadCommitted() {
        }
        
        public TestReadCommitted(String name) {
            super(name);
        }

        public long newTx(Journal journal) {
            
            return journal.newTx(IsolationEnum.ReadCommitted);
            
        }
        
    }
    
    public static class TestReadOnly extends AbstractTestTxRunState {
        
        public TestReadOnly() {
        }
        
        public TestReadOnly(String name) {
            super(name);
        }

        public long newTx(Journal journal) {
            
            return journal.newTx(IsolationEnum.ReadOnly);
            
        }
        
    }
    
    public static class TestReadWrite extends AbstractTestTxRunState {
        
        public TestReadWrite() {
        }
        
        public TestReadWrite(String name) {
            super(name);
        }

        public long newTx(Journal journal) {
            
            return journal.newTx(IsolationEnum.ReadWrite);
            
        }
        
    }
    
    /*
     * Transaction run state tests.
     */
    
    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activeAbort() throws IOException {
        
        Journal journal = new Journal(getProperties());

        assertTrue(journal.activeTx.isEmpty());
        assertTrue(journal.preparedTx.isEmpty());

        final long ts0 = newTx(journal);
        final ITx tx0 = journal.getTx(ts0);
        assertEquals(ts0, tx0.getStartTimestamp());
        assertTrue(tx0 == journal.getTx(ts0));

        assertTrue(tx0.isActive());
        assertFalse(tx0.isPrepared());
        assertFalse(tx0.isAborted());
        assertFalse(tx0.isCommitted());
        assertFalse(tx0.isComplete());

        assertTrue(journal.activeTx.containsKey(ts0));
        assertFalse(journal.preparedTx.containsKey(ts0));

        journal.abort(ts0);
        
        /*
         * note: when the abort is asynchronous, i.e., for a read-write
         * transaction, this causes the test to wait until the abort task has
         * been executed.
         */ 
        journal.writeService.shutdown();

        assertFalse(tx0.isActive());
        assertFalse(tx0.isPrepared());
        assertTrue(tx0.isAborted());
        assertFalse(tx0.isCommitted());
        assertTrue(tx0.isComplete());

        assertFalse(journal.activeTx.containsKey(ts0));
        assertFalse(journal.preparedTx.containsKey(ts0));

        journal.closeAndDelete();

    }
    
    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activePrepareAbort() throws IOException {
        
            Journal journal = new Journal(getProperties());

            assertTrue(journal.activeTx.isEmpty());
            assertTrue(journal.preparedTx.isEmpty());

            final long ts0 = newTx(journal);
            final ITx tx0 = journal.getTx(ts0);
            assertEquals(ts0, tx0.getStartTimestamp());
            assertTrue(tx0 == journal.getTx(ts0));
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            final long commitTime = (tx0.isReadOnly()?0L:journal.nextTimestamp());
            tx0.prepare(commitTime);

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.preparedTx.containsKey(ts0));

            tx0.abort();

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertTrue( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.closeAndDelete();

    }

    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activePrepareCommit() throws IOException {
        
            Journal journal = new Journal(getProperties());

            assertTrue(journal.activeTx.isEmpty());
            assertTrue(journal.preparedTx.isEmpty());

            final long ts0 = newTx(journal);
            final ITx tx0 = journal.getTx(ts0);
            assertEquals(ts0, tx0.getStartTimestamp());
            assertTrue(tx0 == journal.getTx(ts0));
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            final long commitTime = (tx0.isReadOnly() ? 0L : journal
                .nextTimestamp());
            
            tx0.prepare(commitTime);

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.preparedTx.containsKey(ts0));

            assertEquals(commitTime,tx0.commit());

            assertFalse( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertTrue( tx0.isCommitted() );
            assertTrue( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.closeAndDelete();

    }

    /**
     * Simple test of the transaction run state machine. verifies that a 2nd
     * attempt to abort the same transaction results in an exception that does
     * not change the transaction run state.
     */
    public void test_runStateMachine_activeAbortAbort_correctRejection()
            throws IOException {

        Journal journal = new Journal(getProperties());

        assertTrue(journal.activeTx.isEmpty());
        assertTrue(journal.preparedTx.isEmpty());

        final long ts0 = newTx(journal);
        final ITx tx0 = journal.getTx(ts0);
        assertEquals(ts0, tx0.getStartTimestamp());
        assertTrue(tx0 == journal.getTx(ts0));

        assertTrue(tx0.isActive());
        assertFalse(tx0.isPrepared());
        assertFalse(tx0.isAborted());
        assertFalse(tx0.isCommitted());
        assertFalse(tx0.isComplete());

        assertTrue(journal.activeTx.containsKey(ts0));
        assertFalse(journal.preparedTx.containsKey(ts0));

        tx0.abort();

        assertFalse(tx0.isActive());
        assertFalse(tx0.isPrepared());
        assertTrue(tx0.isAborted());
        assertFalse(tx0.isCommitted());
        assertTrue(tx0.isComplete());

        assertFalse(journal.activeTx.containsKey(ts0));
        assertFalse(journal.preparedTx.containsKey(ts0));

        try {
            tx0.abort();
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        assertFalse(tx0.isActive());
        assertFalse(tx0.isPrepared());
        assertTrue(tx0.isAborted());
        assertFalse(tx0.isCommitted());
        assertTrue(tx0.isComplete());

        assertFalse(journal.activeTx.containsKey(ts0));
        assertFalse(journal.preparedTx.containsKey(ts0));

        journal.closeAndDelete();

    }

    /**
     * Simple test of the transaction run state machine verifies that a 2nd
     * attempt to prepare the same transaction results in an exception that
     * changes the transaction run state to 'aborted'.
     */
    public void test_runStateMachine_activePreparePrepare_correctRejection() throws IOException {
        
            Journal journal = new Journal(getProperties());

            assertTrue(journal.activeTx.isEmpty());
            assertTrue(journal.preparedTx.isEmpty());

            final long ts0 = newTx(journal);
            final ITx tx0 = journal.getTx(ts0);
            assertEquals(ts0, tx0.getStartTimestamp());
            assertTrue(tx0 == journal.getTx(ts0));
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
            final long commitTime = (tx0.isReadOnly() ? 0L : journal
                .nextTimestamp());
            tx0.prepare(commitTime);

            assertFalse( tx0.isActive() );
            assertTrue( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );

            assertFalse(journal.activeTx.containsKey(ts0));
            assertTrue(journal.preparedTx.containsKey(ts0));

            try {
                tx0.prepare(commitTime);
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
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.closeAndDelete();

    }

    /**
     * Simple test of the transaction run state machine verifies that a commit
     * out of order results in an exception that changes the transaction run
     * state to 'aborted'.
     */
    public void test_runStateMachine_activeCommit_correctRejection() throws IOException {
        
            Journal journal = new Journal(getProperties());

            assertTrue(journal.activeTx.isEmpty());
            assertTrue(journal.preparedTx.isEmpty());

            final long ts0 = newTx(journal);
            final ITx tx0 = journal.getTx(ts0);
            assertEquals(ts0, tx0.getStartTimestamp());
            assertTrue(tx0 == journal.getTx(ts0));
            
            assertTrue( tx0.isActive() );
            assertFalse( tx0.isPrepared() );
            assertFalse( tx0.isAborted() );
            assertFalse( tx0.isCommitted() );
            assertFalse( tx0.isComplete() );
            
            assertTrue(journal.activeTx.containsKey(ts0));
            assertFalse(journal.preparedTx.containsKey(ts0));
            
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
            assertFalse(journal.preparedTx.containsKey(ts0));

            journal.closeAndDelete();

    }

    /**
     * Verifies that access to, and operations on, a named indices is denied
     * after a PREPARE.
     * 
     * @throws IOException
     * 
     * @todo also test after an abort.
     */
    public void test_runStateMachine_prepared_correctRejection()
            throws IOException {

        Journal journal = new Journal(getProperties());

        String name = "abc";

        {

            journal.registerIndex(name, new UnisolatedBTree(journal, UUID
                    .randomUUID()));
        
            journal.commit();
            
        }

        final long tx0 = newTx(journal);

        ITx tmp = journal.getTx(tx0);

        assertNotNull(tmp);
        
        IIndex ndx = journal.getIndex(name,tx0);
        
        assertNotNull(ndx);

        // commit the journal.
        journal.commit(tx0);

        /*
         * Verify that you can not access a named index after 'prepare'.
         */
        try {
            journal.getIndex(name,tx0);
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        /*
         * Verify that operations on an pre-existing index reference are now
         * denied.
         */
        try {
            ndx.lookup(new byte[] { 1 });
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            ndx.contains(new byte[] { 1 });
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            ndx.remove(new byte[] { 1 });
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }
        try {
            ndx.insert(new byte[] { 1 }, new byte[] { 2 });
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            System.err.println("Ignoring expected exception: " + ex);
        }

        assertFalse(tmp.isActive());
        assertTrue(tmp.isPrepared());
        assertFalse(tmp.isAborted());
        assertFalse(tmp.isCommitted());
        assertFalse(tmp.isComplete());

        assertFalse(journal.activeTx.containsKey(tmp.getStartTimestamp()));
        assertFalse(journal.preparedTx.containsKey(tmp.getStartTimestamp()));
        assertNull(journal.getTx(tmp.getStartTimestamp()));
        
        journal.closeAndDelete();

    }

//    /**
//     * Verifies that access to, and operations on, a named indices is denied
//     * after an ABORT.
//     * 
//     * @throws IOException
//     */
//    public void test_runStateMachine_aborted_correctRejection()
//            throws IOException {
//
//        final Properties properties = getProperties();
//
//        Journal journal = new Journal(properties);
//
//        String name = "abc";
//
//        {
//
//            journal.registerIndex(name, new UnisolatedBTree(journal));
//        
//            journal.commit();
//            
//        }
//
//        ITx tx0 = journal.newTx(IsolationEnum.ReadWrite);
//
//        IIndex ndx = tx0.getIndex(name);
//        
//        assertNotNull(ndx);
//        
//        tx0.abort();
//
//        /*
//         * Verify that you can not access a named index.
//         */
//        try {
//            tx0.getIndex(name);
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//
//        /*
//         * Verify that operations on an pre-existing index reference are now
//         * denied.
//         */
//        try {
//            ndx.lookup(new byte[] { 1 });
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//        try {
//            ndx.contains(new byte[] { 1 });
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//        try {
//            ndx.remove(new byte[] { 1 });
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//        try {
//            ndx.insert(new byte[] { 1 }, new byte[] { 2 });
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//
//        assertFalse(tx0.isActive());
//        assertFalse(tx0.isPrepared());
//        assertTrue (tx0.isAborted());
//        assertFalse(tx0.isCommitted());
//        assertTrue (tx0.isComplete());
//
//        assertFalse(journal.activeTx.containsKey(tx0.getStartTimestamp()));
//        assertFalse(journal.preparedTx.containsKey(tx0.getStartTimestamp()));
//        assertNull(journal.getTx(tx0.getStartTimestamp()));
//        
//        journal.close();
//
//    }
//
//    /**
//     * Verifies that access to, and operations on, a named indices is denied
//     * after a COMMIT.
//     * 
//     * @throws IOException
//     */
//    public void test_runStateMachine_commit_correctRejection()
//            throws IOException {
//
//        final Properties properties = getProperties();
//
//        Journal journal = new Journal(properties);
//
//        String name = "abc";
//
//        {
//
//            journal.registerIndex(name, new UnisolatedBTree(journal));
//        
//            journal.commit();
//            
//        }
//
//        ITx tx0 = journal.newTx(IsolationEnum.ReadWrite);
//
//        IIndex ndx = tx0.getIndex(name);
//        
//        assertNotNull(ndx);
//        
//        tx0.prepare();
//        tx0.commit();
//
//        /*
//         * Verify that you can not access a named index.
//         */
//        try {
//            tx0.getIndex(name);
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//
//        /*
//         * Verify that operations on an pre-existing index reference are now
//         * denied.
//         */
//        try {
//            ndx.lookup(new byte[] { 1 });
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//        try {
//            ndx.contains(new byte[] { 1 });
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//        try {
//            ndx.remove(new byte[] { 1 });
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//        try {
//            ndx.insert(new byte[] { 1 }, new byte[] { 2 });
//            fail("Expecting: " + IllegalStateException.class);
//        } catch (IllegalStateException ex) {
//            System.err.println("Ignoring expected exception: " + ex);
//        }
//
//        assertFalse(tx0.isActive());
//        assertFalse(tx0.isPrepared());
//        assertFalse(tx0.isAborted());
//        assertTrue(tx0.isCommitted());
//        assertTrue(tx0.isComplete());
//
//        assertFalse(journal.activeTx.containsKey(tx0.getStartTimestamp()));
//        assertFalse(journal.preparedTx.containsKey(tx0.getStartTimestamp()));
//        assertNull(journal.getTx(tx0.getStartTimestamp()));
//        
//        journal.close();
//
//    }

}
