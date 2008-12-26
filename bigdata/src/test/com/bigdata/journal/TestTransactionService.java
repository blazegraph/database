/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Dec 23, 2008
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase2;

import com.bigdata.service.AbstractFederation;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.CommitTimeIndex;
import com.bigdata.service.TxServiceRunState;

/**
 * Unit tests of the {@link AbstractTransactionService} using a mock client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTransactionService extends TestCase2 {

    /**
     * 
     */
    public TestTransactionService() {
    }

    /**
     * @param arg0
     */
    public TestTransactionService(String arg0) {
        super(arg0);
    }

    /**
     * Implementation uses a mock client.
     */
    protected MockTransactionService newFixture() {

        return new MockTransactionService(new Properties()).start();

    }

    /**
     * Implementation uses a mock client.
     */
    protected MockTransactionService newFixture(Properties p) {

        return new MockTransactionService(p).start();

    }

    protected static class MockTransactionService extends
            AbstractTransactionService {

        public MockTransactionService(Properties p) {

            super(p);

        }

        public MockTransactionService start() {
            
            super.start();
            
            return this;
            
        }
        
        @Override
        public AbstractFederation getFederation() {
            return null;
        }

        @Override
        protected void abortImpl(TxState state) {

            state.setRunState(RunState.Aborted);

        }

        @Override
        protected long commitImpl(TxState state) throws Exception {

            state.setRunState(RunState.Committed);

            final long commitTime = nextTimestamp();

            notifyCommit(commitTime);

            return commitTime;

        }

        /**
         * Note: We are not testing distributed commits here so this is not
         * implemented.
         */
        public long prepared(long tx, UUID dataService)
                throws InterruptedException, BrokenBarrierException {
            return 0;
        }

        /**
         * Note: We are not testing distributed commits here so this is not
         * implemented.
         */
        public boolean committed(long tx, UUID dataService) throws IOException,
                InterruptedException, BrokenBarrierException {
            return false;
        }

        @Override
        public long getLastCommitTime() {

            return lastCommitTime;

        }

        private long lastCommitTime = 0L;

        protected long findCommitTime(final long timestamp) {

            synchronized (commitTimeIndex) {

                return commitTimeIndex.find(timestamp);

            }

        }

        protected long findNextCommitTime(long commitTime) {

            synchronized (commitTimeIndex) {

                return commitTimeIndex.findNext(commitTime);

            }

        }

        private final CommitTimeIndex commitTimeIndex = CommitTimeIndex
                .createTransient();

        public void notifyCommit(long commitTime) {

            synchronized (commitTimeIndex) {

                /*
                 * Add all commit times
                 */
                commitTimeIndex.add(commitTime);

                /*
                 * Note: commit time notifications can be overlap such that they
                 * appear out of sequence with respect to their values. This is
                 * Ok. We just ignore any older commit times. However we do need
                 * to be synchronized here such that the commit time notices
                 * themselves are serialized so that we do not miss any.
                 */

                if (lastCommitTime < commitTime) {

                    lastCommitTime = commitTime;

                }

            }

        }

        /**
         * Awaits the specified run state.
         * 
         * @param service
         *            The service.
         * @param expectedRunState
         *            The expected run state.
         * 
         * @throws InterruptedException
         * @throws AssertionError
         */
        public void awaitRunState(final TxServiceRunState expectedRunState)
                throws InterruptedException {

            if (expectedRunState == null)
                throw new IllegalArgumentException();

            lock.lock();
            try {

                int i = 0;
                while (i < 100) {

                    if (expectedRunState == getRunState()) {

                        return;

                    }

                    txDeactivate.await(10/* ms */, TimeUnit.MILLISECONDS);

                    i++;

                }

                /*
                 * Note: This will generally fail since we did not achieve the
                 * desired run state in the loop above.
                 */

                assertEquals(expectedRunState, getRunState());

            } finally {

                lock.unlock();
            
            }

        }

    }
       
    /**
     * Create a new read-write tx and then abort it.
     * <p>
     * Note: New read-write transaction identifiers are assigned using
     * {@link ITimestampService#nextTimestamp()}. Therefore they are
     * monotonically increasing. New read-write transactions may be created at
     * any time - there are no preconditions other than that the transaction
     * service is running. Likewise there is no contention other than for the
     * next distinct timestamp.
     */
    public void test_newTx_readWrite_01() {

        final MockTransactionService service = newFixture();

        try {

            assertEquals(0, service.getActiveCount());

            final long t0 = service.nextTimestamp();

            final long tx = service.newTx(ITx.UNISOLATED);

            final long t1 = service.nextTimestamp();

            // read-write transactions use negative timestamps.
            assertTrue(TimestampUtility.isReadWriteTx(tx));

            // must be greater than a timestamp obtained before the tx.
            assertTrue(Math.abs(tx) > t0);

            // must be less than a timestamp obtained after the tx.
            assertTrue(Math.abs(tx) < t1);

            assertEquals(1, service.getActiveCount());
            
            assertEquals(1, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

            service.abort(tx);

            assertEquals(0, service.getActiveCount());

            assertEquals(1, service.getStartCount());

            assertEquals(1, service.getAbortCount());
            
            assertEquals(0, service.getCommitCount());

            assertEquals(0, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

        } finally {

            service.destroy();

        }

    }

    /**
     * Create a new read-write tx and then commit it.
     */
    public void test_newTx_readWrite_02() {

        final MockTransactionService service = newFixture();

        try {

            assertEquals(0, service.getActiveCount());

            final long t0 = service.nextTimestamp();

            final long tx = service.newTx(ITx.UNISOLATED);

            final long t1 = service.nextTimestamp();

            // read-write transactions use negative timestamps.
            assertTrue(TimestampUtility.isReadWriteTx(tx));

            // must be greater than a timestamp obtained before the tx.
            assertTrue(Math.abs(tx) > t0);

            // must be less than a timestamp obtained after the tx.
            assertTrue(Math.abs(tx) < t1);

            assertEquals(1, service.getActiveCount());

            assertEquals(1, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

            service.commit(tx);

            assertEquals(0, service.getActiveCount());

            assertEquals(1, service.getStartCount());
            assertEquals(0, service.getAbortCount());
            assertEquals(1, service.getCommitCount());

            assertEquals(0, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

        } finally {

            service.destroy();

        }

    }

    /**
     * Create two read-write transactions and commit both.
     */
    public void test_newTx_readWrite_03() {
        
        final MockTransactionService service = newFixture();

        try {

            assertEquals(0, service.getActiveCount());

            final long tx1 = service.newTx(ITx.UNISOLATED);

            final long tx2 = service.newTx(ITx.UNISOLATED);

            assertTrue(Math.abs(tx1) < Math.abs(tx2));

            assertEquals(2, service.getActiveCount());

            assertEquals(2, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

            service.commit(tx2);

            assertEquals(1, service.getActiveCount());

            assertEquals(1, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

            service.commit(tx1);

            assertEquals(0, service.getActiveCount());

            assertEquals(2, service.getStartCount());
            assertEquals(0, service.getAbortCount());
            assertEquals(2, service.getCommitCount());

            assertEquals(0, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

        } finally {

            service.destroy();

        }

    }
    
    /**
     * Create a read-write transaction, commit it, and then attempt to re-commit
     * it and to abort it - those operations should fail with an
     * {@link IllegalStateException}.
     */
    public void test_newTx_readWrite_txComplete_postConditions() {
        
        final MockTransactionService service = newFixture();

        try {

            final long tx = service.newTx(ITx.UNISOLATED);

            service.commit(tx);
            
            try {
                service.commit(tx);
                fail("Expecting: "+IllegalStateException.class);
            } catch(IllegalStateException ex) {
                log.info("Ignoring expected exception: "+ex);
            }

            try {
                service.abort(tx);
                fail("Expecting: "+IllegalStateException.class);
            } catch(IllegalStateException ex) {
                log.info("Ignoring expected exception: "+ex);
            }

        } finally {

            service.destroy();

        }

    }
    
    /**
     * Test ability to start a read-committed tx when [lastCommitTime] is
     * non-zero.
     * <p>
     * Note: A "read-committed" transactions is just a shorthand for a read-only
     * transaction whose start time is the last commit time on the database. As
     * such the abort and commit procedure are the same as for a read-only
     * transaction. The only difference is in how the start time of the
     * transaction is generated, so that is all we test here.
     * 
     * @throws IOException 
     */
    public void test_newTx_readCommitted01() throws IOException {

        final MockTransactionService service = newFixture();

        try {

            assertEquals(0, service.getActiveCount());

            service.notifyCommit(service.nextTimestamp());
            
            final long lastCommitTime = service.getLastCommitTime();

            final long t0 = service.nextTimestamp();

            final long tx = service.newTx(ITx.READ_COMMITTED);

            final long t1 = service.nextTimestamp();

            // verify read-only tx.
            assertFalse(TimestampUtility.isReadWriteTx(tx));

            // must be GT the lastCommitTime.
            assertTrue(Math.abs(tx) > lastCommitTime);

            // must be greater than a timestamp obtained before the tx.
            assertTrue(Math.abs(tx) > t0);

            // must be less than a timestamp obtained after the tx.
            assertTrue(Math.abs(tx) < t1);

            assertEquals(1, service.getActiveCount());

            assertEquals(0, service.getReadWriteActiveCount());

            assertEquals(1, service.getReadOnlyActiveCount());
            
            service.commit(tx);

            assertEquals(0, service.getActiveCount());

            assertEquals(1, service.getStartCount());
            assertEquals(0, service.getAbortCount());
            assertEquals(1, service.getCommitCount());

            assertEquals(0, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

        } finally {

            service.destroy();

        }

    }
    
    /**
     * Unit test when [lastCommitTime] is zero.
     */
    public void test_newTx_readCommitted02() {

        final MockTransactionService service = newFixture();

        try {

            assertEquals(0, service.getActiveCount());

            final long lastCommitTime = service.getLastCommitTime();

            final long t0 = service.nextTimestamp();

            final long tx = service.newTx(ITx.READ_COMMITTED);

            final long t1 = service.nextTimestamp();

            // verify read-only tx.
            assertFalse(TimestampUtility.isReadWriteTx(tx));

            // must be GT the lastCommitTime.
            assertTrue(Math.abs(tx) > lastCommitTime);

            // must be greater than a timestamp obtained before the tx.
            assertTrue(Math.abs(tx) > t0);

            // must be less than a timestamp obtained after the tx.
            assertTrue(Math.abs(tx) < t1);

            assertEquals(1, service.getActiveCount());

            assertEquals(0, service.getReadWriteActiveCount());

            assertEquals(1, service.getReadOnlyActiveCount());
            
            service.commit(tx);

            assertEquals(0, service.getActiveCount());

            assertEquals(1, service.getStartCount());
            assertEquals(0, service.getAbortCount());
            assertEquals(1, service.getCommitCount());

            assertEquals(0, service.getReadWriteActiveCount());

            assertEquals(0, service.getReadOnlyActiveCount());

        } finally {

            service.destroy();

        }

    }

    /**
     * Unit test for a new read-only transaction.
     * <p>
     * Read-only transactions are allowed to read on historical commit points of
     * the database. The edge case is allowed where the callers timestamp
     * exactly corresponds to the lastCommitTime, but it is not permitted to be
     * GT the lastCommitTime since that could allow data not yet committed to
     * become visible during the transaction (breaking isolation).
     * <p>
     * A commitTime is identifed by looking up the callers timestamp in a log of
     * the historical commit times and returning the first historical commit
     * time LTE the callers timestamp.
     * <p>
     * The transaction start time is then choosen from the half-open interval
     * <i>commitTime</i> (inclusive lower bound) : <i>nextCommitTime</i>
     * (exclusive upper bound).
     * 
     * @throws IOException 
     */
    public void test_newTx_readOnly() throws IOException {

        final Properties properties = new Properties();
        
        // setup as an immortal database.
        properties.setProperty(
                AbstractTransactionService.Options.MIN_RELEASE_AGE,
                ""+Long.MAX_VALUE);
        
        final MockTransactionService service = newFixture(properties);

        try {

            // populate the commit log on the service.

            final long commitTime = service.nextTimestamp();
            
            final long nextCommitTime = service.nextTimestamp();
            
            service.notifyCommit(commitTime);
            
            assertEquals(commitTime,service.getLastCommitTime());
            
            service.notifyCommit(nextCommitTime);
            
            assertEquals(nextCommitTime,service.getLastCommitTime());

            // a tx for the commit point whose commitTime is 10.
            final long tx1 = service.newTx(commitTime);
            
            System.err.println("tx1="+tx1);
            
            assertFalse(TimestampUtility.isReadWriteTx(tx1));

            assertTrue(tx1 >= commitTime && tx1 < nextCommitTime);

            // another tx for the same commit point.
            final long tx2 = service.newTx(commitTime);

            System.err.println("tx2="+tx2);
            
            assertFalse(TimestampUtility.isReadWriteTx(tx2));

            assertTrue(tx2 >= commitTime && tx2 < nextCommitTime);
            
            assertNotSame(tx1, tx2);

            // commit tx1 (releases its start time so that it may be reused).
            service.commit(tx1);
            
            // another tx for the same commit point.
            final long tx3 = service.newTx(commitTime);

            System.err.println("tx3=" + tx3);

            assertFalse(TimestampUtility.isReadWriteTx(tx3));

            assertTrue(tx3 >= commitTime && tx3 < nextCommitTime);

            // tx3 must be distinct from any active tx.
            assertNotSame(tx3, tx2);

            // but in fact we should have recycled tx1!
            assertEquals(tx1, tx3);

        } finally {

            service.destroy();

        }

    }

    /**
     * Unit test in which all possible start times for a read-only transaction
     * are used, forcing the caller to block.
     * 
     * @throws IOException
     */
    public void test_newTx_readOnly_contention() throws IOException {

        final MockTransactionService service = newFixture();

        try {

            // populate the commit log on the service.

            final long commitTime = 10;
            final long nextCommitTime = 12;
            
            service.notifyCommit(commitTime);
            
            assertEquals(commitTime,service.getLastCommitTime());
            
            service.notifyCommit(nextCommitTime);
            
            assertEquals(nextCommitTime,service.getLastCommitTime());

            // a tx for the commit point whose commitTime is 10.
            final long tx1 = service.newTx(commitTime);
            
            System.err.println("tx1="+tx1);
            
            assertTrue(tx1 >= commitTime && tx1 < nextCommitTime);

            // another tx for the same commit point.
            final long tx2 = service.newTx(commitTime);

            System.err.println("tx2="+tx2);
            
            assertTrue(tx2 >= commitTime && tx2 < nextCommitTime);
            
            assertNotSame(tx1, tx2);

            {
                
                /*
                 * First try to obtain a new tx for the same commit point in a
                 * thread. This should block. We wait for a bit (in the main
                 * thread) to make sure that the thread is not progressing and
                 * then interrupt this thread. This is to prove to ourselves
                 * that the txService can not grant a tx for this commit point
                 * right now.
                 */
                final Thread t = new Thread() {
                    
                    public void run() {
                        
                        final long tx3 = service.newTx(commitTime);

                        fail("Not expecting service to create tx: " + tx3);
                        
                    }
                    
                };
                
                t.start();
                
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                
                // interrupt thread so that the test will continue.
                t.interrupt();
                
            }
            {
            
                /*
                 * Run a thread that sleeps for a moment and then terminates one
                 * of the transactions that is keeping us from being able to
                 * allocate a newTx for the desired commit point. Once [tx2] is
                 * terminated, the main thread should be granted a new tx.
                 */
                new Thread() {

                    public void run() {

                        try {
                            log.info("sleeping in 2nd thread.");
                            Thread.sleep(250/* ms */);
                            log.info("woke up in 2nd thread.");
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                        /*
                         * Terminate a running tx for the desired commit point,
                         * freeing a timestamp that may be used for the newTx()
                         * request in the main thread.
                         */
                        
                        log.info("will terminate tx2: " + tx2);
                        
                        service.commit(tx2);
                        
                        log.info("did terminate tx2: " + tx2);

                    }

                }.start();

                /*
                 * This should block for a moment while the thread is sleeping
                 * and then succeed.
                 * 
                 * Note: The assigned transaction identifier will be the same as
                 * the transaction identifier for [tx2]. This is because we are
                 * in fact waiting on that transaction identifier to become free
                 * so that we can continue.
                 */

                log.info("requesting another tx for the same commit point");
                
                assertEquals(tx2,service.newTx(commitTime));
                
                log.info("have another tx for that commit point.");

            }
            
        } finally {

            service.destroy();

        }

    }

    /**
     * Verify that you can create a read-only transaction by providing the
     * lastCommitTime as the timestamp.
     * 
     * @throws IOException
     */
    public void test_newTx_readOnly_timestamp_is_lastCommitTime() throws IOException {
        
        final MockTransactionService service = newFixture();

        try {

            final long lastCommitTime = 10;

            // make this a valid commit time.
            service.notifyCommit(lastCommitTime);

            assertEquals(lastCommitTime, service.getLastCommitTime());

            final long t0 = service.nextTimestamp();

            // should be legal.
            final long tx = service.newTx(lastCommitTime);

            final long t1 = service.nextTimestamp();

            assertTrue(tx >= lastCommitTime);

            assertTrue(tx > t0 && tx < t1);

            // and assign another read-only tx for lastCommitTime.
            final long tx1 = service.newTx(lastCommitTime);

            final long t2 = service.nextTimestamp();

            assertTrue(tx1 >= lastCommitTime);

            assertTrue(tx1 > tx);

            assertTrue(tx1 > t1 && tx1 < t2);

        } finally {

            service.destroy();

        }

    }
        
    /**
     * Verify that you can not create a read-only transaction using a timestamp
     * that is in the future.
     */
    public void test_newTx_readOnly_timestampInFuture() {
        
        final MockTransactionService service = newFixture();

        try {

            /*
             * Note: The commit time log is empty so anything is in the future.
             */

            try {
                service.newTx(10);
                fail("Expecting: "+IllegalStateException.class);
            } catch(IllegalStateException ex) {
                log.info("Ignoring expected exception: "+ex);
            }
            
        } finally {

            service.destroy();

        }

    }

    /**
     * Verify that a request for an historical state that is no longer available
     * will be rejected.
     * 
     * @todo verify illegal to advance the release time while there are active
     *       tx LTE that the specified release time (or if legal, then force the
     *       abort of those tx and verify that).
     * 
     * @throws IOException
     */
    public void test_newTx_readOnly_historyGone() throws IOException {
        
        final MockTransactionService service = newFixture();

        try {

            /*
             * Verify that the service is not retaining history beyond the last
             * commit point.
             */
            assertEquals(0L, service.getMinReleaseAge());

            final long oldReleaseTime = service.getReleaseTime();
            
            assertEquals(0L,oldReleaseTime);
            
            // this will be the earliest running tx until it completes.
            final long tx0 = service.newTx(ITx.UNISOLATED);

            // timestamp GT [tx0] and LT [tx1].
            final long ts = service.nextTimestamp();
            
            // this will become the earliest running tx if tx0 completes first.
            final long tx1 = service.newTx(ITx.UNISOLATED);

            // commit the tx0 - this will update the release time.
//            final long commitTime0 = 
                service.commit(tx0);
            
            final long newReleaseTime = service.getReleaseTime();
            
            // verify release time was updated.
            if (oldReleaseTime == newReleaseTime) {

                fail("releaseTime not updated: releaseTime=" + newReleaseTime);
                
            }
            
            /*
             * Should have advanced the release time right up to (but LT) the
             * transaction which is now the earliest running tx (that will be
             * tx1). (This assumes [minReleaseAge==0].)
             */
            assertEquals(Math.abs(tx1) - 1, newReleaseTime);
            
            try {
                /*
                 * Try to read from [ts]. Since [minReleaseAge==0] the history
                 * should be gone and this should fail.
                 */
                service.newTx(ts);
                fail("Expecting: "+IllegalStateException.class);
            } catch(IllegalStateException ex) {
                log.info("Ignoring expected exception: "+ex);
            }
            
        } finally {

            service.destroy();

        }

    }

    /**
     * Test verifies the advance of the release time when the earliest running
     * transaction completes. This version focuses on when there are no active
     * transactions once the earliest transaction completes. In this case the
     * [lastCommitTime] is the exclusive upper bound for the new releaseTime.
     * 
     * @throws IOException
     */
    public void test_updateReleaseTime_noTxRemaining() throws IOException {

        final MockTransactionService service = newFixture();

        try {

            /*
             * Note: We have to force a commit time on the log.
             * 
             * Note that if [lastCommitTime==0] the releaseTime can not be
             * advanced if there are also no running transactions.
             */
            service.notifyCommit(service.nextTimestamp());
            
            assertEquals(0L, service.getReleaseTime());

            final long tx0 = service.newTx(ITx.UNISOLATED);

            final long tx1 = service.newTx(ITx.UNISOLATED);

            // unchanged.
            assertEquals(0L, service.getReleaseTime());

            // Abort the 2nd tx.
            service.abort(tx1);

            // unchanged.
            assertEquals(0L, service.getReleaseTime());

            // terminate the 1st tx.
            service.abort(tx0);

            // verify that releaseTime was updated.
            final long releaseTime = service.getReleaseTime();
            final long lastCommitTime = service.getLastCommitTime();
            System.err.println("tx0=" + tx0);
            System.err.println("tx1=" + tx1);
            System.err.println("releaseTime=" + releaseTime);
            System.err.println("lastCommitTime=" + lastCommitTime);
            assertNotSame(0L, releaseTime);

            /*
             * Note: The release time MUST NOT be advanced to the last commit
             * time!!! That would cause ALL commit points to be released.
             * 
             * Note: Both tx1 and tx0 are GT lastCommitTime so we can not test
             * against those here, but see the other test.
             */
            assertTrue(releaseTime < lastCommitTime);

        } finally {

            service.destroy();

        }

    }

    /**
     * A unit test of advancing the last release time for the case where there
     * are still active transactions running once the earliest active
     * transaction commits.
     * 
     * @throws IOException
     */
    public void test_updateReleaseTime_otherTxStillActive() throws IOException {

        final MockTransactionService service = newFixture();

        try {

//            /*
//             * Note: We have to force a commit time on the log since we are not
//             * really integrated with a database and if [lastCommitTime==0] the
//             * releaseTime can not be advanced if there are also no running
//             * transactions.
//             */
//            service.notifyCommit(service.nextTimestamp());
            
            assertEquals(0L, service.getReleaseTime());

            final long tx0 = service.newTx(ITx.UNISOLATED);

            final long tx1 = service.newTx(ITx.UNISOLATED);

            final long tx2 = service.newTx(ITx.UNISOLATED);

            // unchanged.
            assertEquals(0L, service.getReleaseTime());

            // Commit the 2nd tx
            service.commit(tx1);

            // unchanged.
            assertEquals(0L, service.getReleaseTime());

            // terminate the 1st tx.
            service.abort(tx0);

            // verify that releaseTime was updated.
            final long releaseTime = service.getReleaseTime();
            final long lastCommitTime = service.getLastCommitTime();
            System.err.println("tx0           =" + tx0);
            System.err.println("tx1           =" + tx1);
            System.err.println("tx2           =" + tx2);
            System.err.println("releaseTime   = " + releaseTime);
            System.err.println("lastCommitTime= " + lastCommitTime);
            assertNotSame(0L, releaseTime);

            /*
             * Note: The release time MUST NOT be advanced to the last commit
             * time!!!
             * 
             * That would cause ALL commit points to be released.
             */
            assertTrue(releaseTime < lastCommitTime);
            
            // releaseTime GTE [tx0].
            assertTrue(releaseTime >= Math.abs(tx0));

            // releaseTime is LT [tx2].
            assertTrue(releaseTime < Math.abs(tx2));
            
        } finally {

            service.destroy();

        }
        
    }

    /**
     * Create a read-only transaction, commit it, and then attempt to re-commit
     * it and to abort it - those operations should fail with an
     * {@link IllegalStateException}.
     * 
     * @throws IOException 
     */
    public void test_newTx_readOnly_txComplete_postConditions() throws IOException {
        
        final MockTransactionService service = newFixture();

        try {

            // make this a valid commit time.
            service.notifyCommit(10);
            
            final long tx = service.newTx(10);

            service.commit(tx);
            
            try {
                service.commit(tx);
                fail("Expecting: "+IllegalStateException.class);
            } catch(IllegalStateException ex) {
                log.info("Ignoring expected exception: "+ex);
            }

            try {
                service.abort(tx);
                fail("Expecting: "+IllegalStateException.class);
            } catch(IllegalStateException ex) {
                log.info("Ignoring expected exception: "+ex);
            }

        } finally {

            service.destroy();

        }

    }

    /**
     * Verifies that we can shutdown() the service when there are no
     * active transactions.
     */
    public void test_shutdown_nothingRunning() {

        final MockTransactionService service = newFixture();

        try {
            
            service.shutdown();
            
        } finally {

            service.destroy();
            
        }
        
    }

    /**
     * Test that the service will wait for a read-write tx to commit.
     * 
     * @throws InterruptedException
     */
    public void test_shutdown_waitsForReadWriteTx_commits()
            throws InterruptedException {

        final MockTransactionService service = newFixture();

        try {

            final long tx = service.newTx(ITx.UNISOLATED);

            final Thread t = new Thread() {
            
                public void run() {

                    service.shutdown();
                    
                }
                
            };
            
            t.setDaemon(true);
            
            t.start();

            service.awaitRunState( TxServiceRunState.Shutdown);

            // commit the running tx.
            service.commit(tx);

            service.awaitRunState( TxServiceRunState.Halted);

        } finally {

            service.destroy();

        }

    }

    /**
     * Test that the service will wait for a read-write tx to abort.
     * 
     * @throws InterruptedException
     */
    public void test_shutdown_waitsForReadWriteTx_aborts()
            throws InterruptedException {

        final MockTransactionService service = newFixture();

        try {

            final long tx = service.newTx(ITx.UNISOLATED);

            final Thread t = new Thread() {
            
                public void run() {

                    service.shutdown();
                    
                }
                
            };
            
            t.setDaemon(true);
            
            t.start();

            service.awaitRunState( TxServiceRunState.Shutdown);

            // abort the running tx.
            service.abort(tx);

            service.awaitRunState( TxServiceRunState.Halted);

        } finally {

            service.destroy();
            
        }
        
    }

    /**
     * Test that shutdown() does not permit new tx to start (a variety of things
     * are not permitted during shutdown).
     * 
     * @throws InterruptedException
     */
    public void test_shutdown_newTxNotAllowed() throws InterruptedException {

        final MockTransactionService service = newFixture();

        try {

            final long tx = service.newTx(ITx.UNISOLATED);

            final Thread t = new Thread() {
            
                public void run() {
                    
                    service.shutdown();
                    
                }
                
            };
            
            t.setDaemon(true);
            
            t.start();

            // verify service is shutting down.
            service.awaitRunState( TxServiceRunState.Shutdown);

            // verify newTx() is disallowed during shutdown.
            try {

                service.newTx(ITx.UNISOLATED);
                
                fail("Expecting: " + IllegalStateException.class);
                
            } catch (IllegalStateException ex) {
                
                log.info("Ignoring expected exception: " + ex);
                
            }

            // one tx left.
            assertEquals(1,service.getActiveCount());

            // abort the last running tx.
            service.abort(tx);

            // no tx left.
            assertEquals(0,service.getActiveCount());
            
            // wait until the service halts.
            service.awaitRunState(TxServiceRunState.Halted);

        } finally {

            service.destroy();
            
        }
        
    }

    /**
     * Test that the service will wait for a read-only tx to commit.
     */
    public void test_shutdown_waitsForReadOnlyTx_commits()
            throws InterruptedException {

        final MockTransactionService service = newFixture();

        try {

            final long tx = service.newTx(ITx.UNISOLATED);

            final Thread t = new Thread() {

                public void run() {

                    service.shutdown();

                }

            };

            t.setDaemon(true);

            t.start();

            service.awaitRunState( TxServiceRunState.Shutdown);

            // commit the running tx.
            service.commit(tx);

            service.awaitRunState( TxServiceRunState.Halted);

        } finally {

            service.destroy();

        }

    }

    /**
     * Test that the service will wait for a read-only tx to abort.
     */
    public void test_shutdown_waitsForReadOnlyTx_aborts()
            throws InterruptedException {

        final MockTransactionService service = newFixture();

        try {

            final long tx = service.newTx(ITx.UNISOLATED);

            final Thread t = new Thread() {

                public void run() {

                    service.shutdown();

                }

            };

            t.setDaemon(true);

            t.start();

            service.awaitRunState( TxServiceRunState.Shutdown);

            // abort the running tx.
            service.abort(tx);

            service.awaitRunState( TxServiceRunState.Halted);

        } finally {

            service.destroy();

        }

    }

    /**
     * Test that shutdown() may be interrupted while waiting for a tx to
     * complete and that it will convert to shutdownNow() which does not wait.
     * 
     * @throws InterruptedException
     */
    public void test_shutdown_interrupted() throws InterruptedException {

        final MockTransactionService service = newFixture();

        try {

//            final long tx = 
                service.newTx(ITx.UNISOLATED);

            final Thread t = new Thread() {

                public void run() {

                    service.shutdown();
                    
                }

            };

            t.setDaemon(true);

            t.start();

            service.awaitRunState( TxServiceRunState.Shutdown);

            // interrupt the thread running shutdown().
            t.interrupt();

            service.awaitRunState( TxServiceRunState.Halted);

        } finally {

            service.destroy();

        }

    }

}
