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

import junit.framework.TestCase2;

import com.bigdata.service.AbstractFederation;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.CommitTimeIndex;

/**
 * Unit tests of the {@link AbstractTransactionService} using a mock client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStandaloneTransactionService extends TestCase2 {

    /**
     * 
     */
    public TestStandaloneTransactionService() {
    }

    /**
     * @param arg0
     */
    public TestStandaloneTransactionService(String arg0) {
        super(arg0);
    }

    /**
     * Default implementation uses a mock client.
     * 
     * @param indexManagerIsIgnored 
     * 
     * @return
     */
    protected AbstractTransactionService newFixture() {
        
        return new AbstractTransactionService(new Properties()) {

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
                
                return nextTimestamp();
            }

            public long prepared(long tx, UUID dataService) throws 
                    InterruptedException, BrokenBarrierException {
                return 0;
            }

            public boolean committed(long tx, UUID dataService)
                    throws IOException, InterruptedException,
                    BrokenBarrierException {
                return false;
            }

            @Override
            public long lastCommitTime() {

                return lastCommitTime;
                
            }

            private long lastCommitTime = 0L;

            public void setReleaseTime(long releaseTime) {

                if (releaseTime < this.releaseTime) {

                    throw new IllegalStateException();
                    
                }
                
                this.releaseTime = releaseTime;

            }

            protected long getReleaseTime() {
                
                return releaseTime;
                
            }
            
            private long releaseTime = 0L;

            protected long findCommitTime(final long timestamp) {
                
                synchronized(commitTimeIndex) {
                    
                    return commitTimeIndex.find(timestamp);
                    
                }
                
            }

            protected long findNextCommitTime(long commitTime) {
                
                synchronized(commitTimeIndex) {
                    
                    return commitTimeIndex.findNext(commitTime);
                    
                }

            }
            
            private final CommitTimeIndex commitTimeIndex = CommitTimeIndex.createTransient();

            public void notifyCommit(long commitTime) {

                synchronized(commitTimeIndex) {

                    /*
                     * Add all commit times
                     */
                    commitTimeIndex.add(commitTime);
                    
                }
                
                synchronized(lastCommitTimeLock) {
                    
                    /*
                     * Note: commit time notifications can be overlap such that they
                     * appear out of sequence with respect to their values. This is Ok.
                     * We just ignore any older commit times. However we do need to be
                     * synchronized here such that the commit time notices themselves
                     * are serialized so that we do not miss any.
                     */
                    
                    if (lastCommitTime < commitTime) {

                        lastCommitTime = commitTime;
                        
                    }
                    
                }
                
            }
            
            private final Object lastCommitTimeLock = new Object();

        };
        
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

        final AbstractTransactionService service = newFixture();

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

        final AbstractTransactionService service = newFixture();

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
        
        final AbstractTransactionService service = newFixture();

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
        
        final AbstractTransactionService service = newFixture();

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

        final AbstractTransactionService service = newFixture();

        try {

            assertEquals(0, service.getActiveCount());

            service.notifyCommit(service.nextTimestamp());
            
            final long lastCommitTime = service.lastCommitTime();

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

        final AbstractTransactionService service = newFixture();

        try {

            assertEquals(0, service.getActiveCount());

            final long lastCommitTime = service.lastCommitTime();

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

        final AbstractTransactionService service = newFixture();

        try {

            // populate the commit log on the service.

            final long commitTime = 10;
            final long nextCommitTime = 20;
            
            service.notifyCommit(commitTime);
            
            assertEquals(commitTime,service.lastCommitTime());
            
            service.notifyCommit(nextCommitTime);
            
            assertEquals(nextCommitTime,service.lastCommitTime());

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

        final AbstractTransactionService service = newFixture();

        try {

            // populate the commit log on the service.

            final long commitTime = 10;
            final long nextCommitTime = 12;
            
            service.notifyCommit(commitTime);
            
            assertEquals(commitTime,service.lastCommitTime());
            
            service.notifyCommit(nextCommitTime);
            
            assertEquals(nextCommitTime,service.lastCommitTime());

            // a tx for the commit point whose commitTime is 10.
            final long tx1 = service.newTx(commitTime);
            
            System.err.println("tx1="+tx1);
            
            assertTrue(tx1 >= commitTime && tx1 < nextCommitTime);

            // another tx for the same commit point.
            final long tx2 = service.newTx(commitTime);

            System.err.println("tx2="+tx2);
            
            assertTrue(tx2 >= commitTime && tx2 < nextCommitTime);
            
            assertNotSame(tx1, tx2);

            /*
             * Another tx for the same commit point - this should block.
             * 
             * @todo this unit test setups a contention but it is not written
             * with the threading necessary to test the ability of
             * {@link AbstractTransactionService#newTx(long)} to block when
             * there is no start time available. However, the implementation
             * itself does not handle this case yet either. So, fix both the
             * test and the impl.
             */
            final long tx3 = service.newTx(commitTime);

//            System.err.println("tx3=" + tx3);
//
//            assertTrue(tx3 >= commitTime && tx3 < nextCommitTime);
//
//            // tx3 must be distinct from any active tx.
//            assertNotSame(tx3, tx2);

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
        
        final AbstractTransactionService service = newFixture();

        try {

            final long lastCommitTime = 10;

            // make this a valid commit time.
            service.notifyCommit(lastCommitTime);

            assertEquals(lastCommitTime, service.lastCommitTime());

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
        
        final AbstractTransactionService service = newFixture();

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
        
        final AbstractTransactionService service = newFixture();

        try {

            service.notifyCommit(10);

            service.notifyCommit(20);

            service.setReleaseTime(15);
            
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
     * Create a read-only transaction, commit it, and then attempt to re-commit
     * it and to abort it - those operations should fail with an
     * {@link IllegalStateException}.
     * 
     * @throws IOException 
     */
    public void test_newTx_readOnly_txComplete_postConditions() throws IOException {
        
        final AbstractTransactionService service = newFixture();

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
    
}
