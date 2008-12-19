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

package com.bigdata.journal;

import java.io.IOException;
import java.util.UUID;

import junit.framework.TestSuite;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;

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

//        suite.addTestSuite(TestReadCommitted.class);
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
    
//    public static class TestReadCommitted extends AbstractTestTxRunState {
//        
//        public TestReadCommitted() {
//        }
//        
//        public TestReadCommitted(String name) {
//            super(name);
//        }
//
//        public long newTx(Journal journal) {
//            
////            return journal.newTx(IsolationEnum.ReadCommitted);
//            
//            return ITx.READ_COMMITTED;
//            
//        }
//        
//    }
    
    public static class TestReadOnly extends AbstractTestTxRunState {
        
        public TestReadOnly() {
        }
        
        public TestReadOnly(String name) {
            super(name);
        }

        public long newTx(Journal journal) {
            
            /*
             * FIXME MUST also test w/ tx created from a caller specified commit
             * point.
             */

            return journal.newTx(ITx.READ_COMMITTED);
            
        }
        
    }
    
    public static class TestReadWrite extends AbstractTestTxRunState {
        
        public TestReadWrite() {
        }
        
        public TestReadWrite(String name) {
            super(name);
        }

        public long newTx(Journal journal) {
            
            return journal.newTx(ITx.UNISOLATED);
            
        }
        
    }
    
    /*
     * Transaction run state tests.
     */
    
    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activeAbort() throws IOException {

        final Journal journal = new Journal(getProperties());
        try {
            
            // force a commit point on the journal.
            journal.registerIndex(new IndexMetadata("foo", UUID.randomUUID()));
            journal.commit();

            final long ts0 = newTx(journal);
            final ITx tx0 = journal.getTx(ts0);
            assertEquals(ts0, tx0.getStartTimestamp());
            assertTrue(tx0 == journal.getTx(ts0));

            assertTrue(tx0.isActive());
            assertFalse(tx0.isPrepared());
            assertFalse(tx0.isAborted());
            assertFalse(tx0.isCommitted());
            assertFalse(tx0.isComplete());

            // abort is synchronous
            journal.abort(ts0);

            // /*
            // * note: when the abort is asynchronous, i.e., for a read-write
            // * transaction, this causes the test to wait until the abort task
            // has
            // * been executed.
            // */
            // journal.writeService.shutdown();

            assertFalse(tx0.isActive());
            assertFalse(tx0.isPrepared());
            assertTrue(tx0.isAborted());
            assertFalse(tx0.isCommitted());
            assertTrue(tx0.isComplete());

        } finally {
            
            journal.destroy();
            
        }

    }
    
    /**
     * Simple test of the transaction run state machine.
     */
    public void test_runStateMachine_activePrepareAbort() throws IOException {
        
        final Journal journal = new Journal(getProperties());
        try {

            /*
             * Force a commit point on the journal.
             */
            journal.registerIndex(new IndexMetadata("foo", UUID.randomUUID()));
            journal.commit();

            final long ts0 = newTx(journal);
            final ITx tx0 = journal.getTx(ts0);
            assertEquals(ts0, tx0.getStartTimestamp());
            assertTrue(tx0 == journal.getTx(ts0));

            assertTrue(tx0.isActive());
            assertFalse(tx0.isPrepared());
            assertFalse(tx0.isAborted());
            assertFalse(tx0.isCommitted());
            assertFalse(tx0.isComplete());

            final long commitTime = (tx0.isReadOnly() || tx0.isEmptyWriteSet() ? 0L
                    : journal.nextTimestamp());
            tx0.prepare(commitTime);

            assertFalse(tx0.isActive());
            assertTrue(tx0.isPrepared());
            assertFalse(tx0.isAborted());
            assertFalse(tx0.isCommitted());
            assertFalse(tx0.isComplete());

            tx0.abort();

            assertFalse(tx0.isActive());
            assertFalse(tx0.isPrepared());
            assertTrue(tx0.isAborted());
            assertFalse(tx0.isCommitted());
            assertTrue(tx0.isComplete());

        } finally {
       
            journal.destroy();
            
        }

    }

//    /**
//     * Simple test of the transaction run state machine.
//     */
//    public void test_runStateMachine_activePrepareCommit() throws IOException {
//        
//        final Journal journal = new Journal(getProperties());
//        try {
//
//            /*
//             * Force a commit point on the journal.
//             */
//            journal.registerIndex(new IndexMetadata("foo", UUID.randomUUID()));
//            journal.commit();
//
//            final long ts0 = newTx(journal);
//            final ITx tx0 = journal.getTx(ts0);
//            assertEquals(ts0, tx0.getStartTimestamp());
//            assertTrue(tx0 == journal.getTx(ts0));
//            
//            assertTrue( tx0.isActive() );
//            assertFalse( tx0.isPrepared() );
//            assertFalse( tx0.isAborted() );
//            assertFalse( tx0.isCommitted() );
//            assertFalse( tx0.isComplete() );
//            
//            final long revisionTime = (tx0.isReadOnly()||tx0.isEmptyWriteSet() ? 0L : journal
//                .nextTimestamp());
//            
//            tx0.prepare(revisionTime);
//
//            assertFalse( tx0.isActive() );
//            assertTrue( tx0.isPrepared() );
//            assertFalse( tx0.isAborted() );
//            assertFalse( tx0.isCommitted() );
//            assertFalse( tx0.isComplete() );
//
////            assertEquals(commitTime,
//                    tx0.mergeDown(revisionTime);
////                    );
//
//            assertFalse( tx0.isActive() );
//            assertFalse( tx0.isPrepared() );
//            assertFalse( tx0.isAborted() );
//            assertTrue( tx0.isCommitted() );
//            assertTrue( tx0.isComplete() );
//
//        } finally {
//
//            journal.destroy();
//            
//        }
//
//    }

    /**
     * Simple test of the transaction run state machine. verifies that a 2nd
     * attempt to abort the same transaction results in an exception that does
     * not change the transaction run state.
     */
    public void test_runStateMachine_activeAbortAbort_correctRejection()
            throws IOException {

        final Journal journal = new Journal(getProperties());
        try {

            /*
             * Force a commit point on the journal.
             */
            journal.registerIndex(new IndexMetadata("foo", UUID.randomUUID()));
            journal.commit();

            final long ts0 = newTx(journal);
            final ITx tx0 = journal.getTx(ts0);
            assertEquals(ts0, tx0.getStartTimestamp());
            assertTrue(tx0 == journal.getTx(ts0));

            assertTrue(tx0.isActive());
            assertFalse(tx0.isPrepared());
            assertFalse(tx0.isAborted());
            assertFalse(tx0.isCommitted());
            assertFalse(tx0.isComplete());

            tx0.abort();

            assertFalse(tx0.isActive());
            assertFalse(tx0.isPrepared());
            assertTrue(tx0.isAborted());
            assertFalse(tx0.isCommitted());
            assertTrue(tx0.isComplete());

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

        } finally {

            journal.destroy();

        }

    }

//    /**
//     * Simple test of the transaction run state machine verifies that a 2nd
//     * attempt to prepare the same transaction results in an exception that
//     * changes the transaction run state to 'aborted'.
//     */
//    public void test_runStateMachine_activePreparePrepare_correctRejection() throws IOException {
//        
//        final Journal journal = new Journal(getProperties());
//        try {
//
//            /*
//             * Force a commit point on the journal.
//             */
//            journal.registerIndex(new IndexMetadata("foo", UUID.randomUUID()));
//            journal.commit();
//
//            final long ts0 = newTx(journal);
//            final ITx tx0 = journal.getTx(ts0);
//            assertEquals(ts0, tx0.getStartTimestamp());
//            assertTrue(tx0 == journal.getTx(ts0));
//
//            assertTrue(tx0.isActive());
//            assertFalse(tx0.isPrepared());
//            assertFalse(tx0.isAborted());
//            assertFalse(tx0.isCommitted());
//            assertFalse(tx0.isComplete());
//
//            final long revisionTime = (tx0.isReadOnly() || tx0.isEmptyWriteSet() ? 0L
//                    : journal.nextTimestamp());
//            tx0.prepare(revisionTime);
//
//            assertFalse(tx0.isActive());
//            assertTrue(tx0.isPrepared());
//            assertFalse(tx0.isAborted());
//            assertFalse(tx0.isCommitted());
//            assertFalse(tx0.isComplete());
//
//            try {
//                tx0.prepare(revisionTime);
//                fail("Expecting: " + IllegalStateException.class);
//            } catch (IllegalStateException ex) {
//                System.err.println("Ignoring expected exception: " + ex);
//            }
//
//            assertFalse(tx0.isActive());
//            assertFalse(tx0.isPrepared());
//            assertTrue(tx0.isAborted());
//            assertFalse(tx0.isCommitted());
//            assertTrue(tx0.isComplete());
//
//        } finally {
//
//            journal.destroy();
//
//        }
//
//    }

//    /**
//     * Simple test of the transaction run state machine verifies that a commit
//     * out of order results in an exception that changes the transaction run
//     * state to 'aborted'.
//     */
//    public void test_runStateMachine_activeCommit_correctRejection() throws IOException {
//        
//        final Journal journal = new Journal(getProperties());
//        try {
//
//            /*
//             * Force a commit point on the journal.
//             */
//            journal.registerIndex(new IndexMetadata("foo", UUID.randomUUID()));
//            journal.commit();
//
//            final long ts0 = newTx(journal);
//            final ITx tx0 = journal.getTx(ts0);
//            assertEquals(ts0, tx0.getStartTimestamp());
//            assertTrue(tx0 == journal.getTx(ts0));
//
//            assertTrue(tx0.isActive());
//            assertFalse(tx0.isPrepared());
//            assertFalse(tx0.isAborted());
//            assertFalse(tx0.isCommitted());
//            assertFalse(tx0.isComplete());
//
//            try {
//              final long commitTime = (tx0.isReadOnly() || tx0.isEmptyWriteSet() ? 0L
//              : journal.nextTimestamp());
//                tx0.mergeDown(commitTime);
//                fail("Expecting: " + IllegalStateException.class);
//            } catch (IllegalStateException ex) {
//                System.err.println("Ignoring expected exception: " + ex);
//            }
//
//            assertFalse(tx0.isActive());
//            assertFalse(tx0.isPrepared());
//            assertTrue(tx0.isAborted());
//            assertFalse(tx0.isCommitted());
//            assertTrue(tx0.isComplete());
//
//        } finally {
//
//            journal.destroy();
//
//        }
//
//    }

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

        final Journal journal = new Journal(getProperties());
        try {

            final String name = "abc";

            {

                final IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                md.setIsolatable(true);

                journal.registerIndex(md);

                journal.commit();

            }

            final long tx0 = newTx(journal);

            ITx tmp = journal.getTx(tx0);

            assertNotNull(tmp);

            IIndex ndx = journal.getIndex(name, tx0);

            assertNotNull(ndx);

            // commit the journal.
            journal.commit(tx0);

            /*
             * Verify that you can not access a named index after 'prepare'.
             */
            // try {
            assertNull(journal.getIndex(name, tx0));
            // fail("Expecting: " + IllegalStateException.class);
            // } catch (IllegalStateException ex) {
            // System.err.println("Ignoring expected exception: " + ex);
            // }

            /*
             * Verify that operations on an pre-existing index reference are now
             * denied.
             * 
             * @todo the existing index reference is not in fact disabled when
             * the transaction is no longer active. However, the nominal use
             * case for transactions is running inside of an AbstractTask so
             * this should not be a problem.
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

            assertNull(journal.getTx(tmp.getStartTimestamp()));

        } finally {

            journal.destroy();

        }

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
