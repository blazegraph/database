/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.jini.config.Configuration;
import net.jini.core.lookup.ServiceID;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.IndexManagerCallable;
import com.bigdata.ha.msg.HADigestRequest;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.util.InnerCause;

/**
 * Life cycle and related tests for a single remote {@link HAJournalServer} out
 * of a quorum of 3. The quorum will not meet for these unit tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Add test to verify that we do not permit a double-start of a
 *         service (correctly fails, reporting that the service is already
 *         running).
 */
public class TestHAJournalServer extends AbstractHA3JournalServerTestCase {

    public TestHAJournalServer() {
    }

    public TestHAJournalServer(final String name) {
        super(name);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: This overrides some {@link Configuration} values for the
     * {@link HAJournalServer} in order to establish conditions suitable for
     * testing the {@link ISnapshotPolicy} and {@link IRestorePolicy}.
     */
    @Override
    protected String[] getOverrides() {
        
        return new String[]{
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()"
        };
        
    }
    
    /**
     * One service starts, quorum does not meet (replication factor is 3). This
     * also serves to verify the <code>HAJournal-A.config</code> file.
     */
    public void testStartA() throws Exception {
        
        final HAGlue serverA = startA();
        
        try {

            quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            
            fail("Not expecting quorum meet");

        } catch (TimeoutException ex) {
        
            // ignore.
            
        }

        // Service is not met in any role around a quorum.
        try {
            serverA.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            // Ignore expected exception.
        }

        // Verify can access the REST API "status" page.
        doNSSStatusRequest(serverA);

        // Verify self-reports as NotReady.
        awaitHAStatus(serverA, HAStatusEnum.NotReady);
        
        // Verify that service self-reports as NotReady via the REST API.
        assertEquals(HAStatusEnum.NotReady, getNSSHAStatus(serverA));

        // Verify can not read on service.
        assertReadRejected(serverA);

        // Verify can not write on service.
        assertWriteRejected(serverA);

        assertTrue(getHAJournalFileA().exists());
        assertTrue(getHALogDirA().exists());
        assertTrue(getSnapshotDirA().exists());
        
//        serverA.enterErrorState().get();
//        
//        Thread.sleep(10000/*ms*/);
        
    }

    /**
     * Verify Server B will start - this helps to proof the configuration
     * files.
     */
    public void testStartB() throws Exception {
        
        final HAGlue serverB = startB();
        
        try {

            quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            
            fail("Not expecting quorum meet");

        } catch (TimeoutException ex) {
        
            // ignore.
            
        }
        
        doNSSStatusRequest(serverB);

        // Verify self-reports as NotReady.
        awaitHAStatus(serverB, HAStatusEnum.NotReady);

        // Verify can not read on service.
        assertReadRejected(serverB);

        // Verify can not write on service.
        assertWriteRejected(serverB);

        assertTrue(getHAJournalFileB().exists());
        assertTrue(getHALogDirB().exists());
        assertTrue(getSnapshotDirB().exists());
        
    }

    /**
     * Verify Server C will start - this helps to proof the configuration
     * files.
     */
    public void testStartC() throws Exception {
        
        final HAGlue serverC = startC();
        
        try {

            quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            
            fail("Not expecting quorum meet");

        } catch (TimeoutException ex) {
        
            // ignore.
            
        }
        
        doNSSStatusRequest(serverC);

        // Verify self-reports as NotReady.
        awaitHAStatus(serverC, HAStatusEnum.NotReady);

        // Verify can not read on service.
        assertReadRejected(serverC);

        // Verify can not write on service.
        assertWriteRejected(serverC);

        assertTrue(getHAJournalFileC().exists());
        assertTrue(getHALogDirC().exists());
        assertTrue(getSnapshotDirC().exists());
        
    }

    /**
     * Verify that the various bits of state are removed from the file system
     * when the service is destroyed.
     * 
     * @throws Exception
     */
    public void testStartA_Destroy() throws Exception {
        
        final HAGlue serverA = startA();
        
        try {

            quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            
            fail("Not expecting quorum meet");

        } catch (TimeoutException ex) {
        
            // ignore.
            
        }
        
        doNSSStatusRequest(serverA);

        assertTrue(getHAJournalFileA().exists());
        assertTrue(getHALogDirA().exists());
        assertTrue(getSnapshotDirA().exists());

        destroyA();

        assertFalse(getHAJournalFileA().exists());
        assertFalse(getHALogDirA().exists());
        assertFalse(getSnapshotDirA().exists());

    }
    
    /**
     * One service starts, quorum does not meet (replication factor is 3).
     * Shutdown and restart the service. Verify that it comes back up with the
     * same {@link ServiceID}. Again, quorum does not meet.
     * <p>
     * 
     * Note: I have observed an improper shutdown for this test. The stack
     * traces below show that the shutdown code was blocked awaiting the return
     * from clearToken(), which was triggered by serviceLeave() and
     * conditionalWithdrawVote(). This does not always result in a hung service.
     * The problem would appear to be a failure to correctly identify when the
     * quorum will break. If the code gets this wrong, then it can sping in
     * QuorumActorBase.conditionalClearToken(). That seems pretty dangerous.
     * 
     * <pre>
     * "shutdownThread" daemon prio=5 tid=7fa36f243800 nid=0x115c1c000 waiting on condition [115c1b000]
     *    java.lang.Thread.State: WAITING (parking)
     *     at sun.misc.Unsafe.park(Native Method)
     *     - parking to wait for  <7c021a100> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
     *     at java.util.concurrent.locks.LockSupport.park(LockSupport.java:156)
     *     at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1987)
     *     at com.bigdata.quorum.AbstractQuorum$QuorumActorBase.conditionalWithdrawVoteImpl(AbstractQuorum.java:1579)
     *     at com.bigdata.quorum.AbstractQuorum$QuorumActorBase.serviceLeave(AbstractQuorum.java:1500)
     *     at com.bigdata.journal.jini.ha.HAJournalServer.beforeShutdownHook(HAJournalServer.java:524)
     *     at com.bigdata.journal.jini.ha.AbstractServer.shutdownNow(AbstractServer.java:1457)
     *     at com.bigdata.journal.jini.ha.AbstractServer$ShutdownThread.run(AbstractServer.java:1892)     *
     *     
     * "WatcherActionService1" daemon prio=5 tid=7fa36dc63800 nid=0x114f50000 waiting on condition [114f4f000]
     *    java.lang.Thread.State: WAITING (parking)
     *     at sun.misc.Unsafe.park(Native Method)
     *     - parking to wait for  <7c021a0b8> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
     *     at java.util.concurrent.locks.LockSupport.park(LockSupport.java:156)
     *     at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:1987)
     *     at com.bigdata.quorum.AbstractQuorum$QuorumActorBase.conditionalClearToken(AbstractQuorum.java:1690)
     *     at com.bigdata.quorum.AbstractQuorum$QuorumActorBase.clearToken(AbstractQuorum.java:1437)
     *     at com.bigdata.quorum.AbstractQuorum$QuorumWatcherBase$8.run(AbstractQuorum.java:2959)
     *     at com.bigdata.quorum.AbstractQuorum$QuorumWatcherBase$1.run(AbstractQuorum.java:2048)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
     *     at java.lang.Thread.run(Thread.java:680)
     * </pre>
     */
    public void testRestartA() throws Exception {
        
        final UUID serviceUUID;
        final byte[] digestA;
        {

            final HAGlue serverA = startA();

            serviceUUID = serverA.getServiceUUID();
            
            try {

                quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);

                fail("Not expecting quorum meet");

            } catch (TimeoutException ex) {

                // ignore.

            }

            doNSSStatusRequest(serverA);

            // Verify self-reports as NotReady.
            awaitHAStatus(serverA, HAStatusEnum.NotReady);

            digestA = serverA.computeDigest(
                    new HADigestRequest(null/* storeId */)).getDigest();

        }

        /*
         * Restart the service.
         */
        {

            // Restart.
            final HAGlue serverA = restartA();

            // Make sure that the service came back up with the same UUID.
            assertEquals(serviceUUID, serverA.getServiceUUID());

            // Verify quorum does not meet.
            try {

                quorum.awaitQuorum(1000, TimeUnit.MILLISECONDS);

                fail("Not expecting quorum meet");

            } catch (TimeoutException ex) {

                // ignore.

            }

            // Verify NSS is running.
            doNSSStatusRequest(serverA);

            // Verify self-reports as NotReady.
            awaitHAStatus(serverA, HAStatusEnum.NotReady);

            /*
             * Verify no changes in digest on restart?
             */
            final byte[] digestA1 = serverA.computeDigest(
                    new HADigestRequest(null/* storeId */)).getDigest();

            assertEquals(digestA, digestA1);

        }

    }

    /**
     * This test is used to characterize what happens when we interrupt an RMI.
     * Most methods on the {@link HAGlue} interface are synchronous - they block
     * while some behavior is executed. This is even true for some methods that
     * return a {@link Future} in order to avoid overhead associated with the
     * export of a proxy and DGC thread leaks (since fixed in River).
     * <p>
     * This unit test setups up a service and then issues an RMI that invokes a
     * {@link Thread#sleep(long)} method on the service. The thread that issues
     * the RMI is then interrupted during the sleep.
     * <p>
     * Note: The EXPECTED behavior is that the remote task is NOT interrupted!
     * See these comments from the dev@river.apache.org mailing list:
     * 
     * <pre>
     * Hi Bryan:
     * 
     * I would expect the remote task to complete and return, despite the caller being interrupted.  The JERI subsystem (I�m guessing the invocation layer, but it might be the transport layer) might log an exception when it tried to return, but there was nobody on the calling end.
     * 
     * That�s consistent with a worst-case failure, where the caller drops off the network.  How is the called service supposed to know what happened to the caller?
     * 
     * In the case of a typical short operation, I wouldn�t see that as a big issue, as the wasted computational effort on the service side won�t be consequential.
     * 
     * In the case of a long-running operation where it becomes more likely that the caller wants to stop or cancel an operation (in addition to the possibility of having it interrupted), I�d try to break it into a series of operations (chunks), or setup an ongoing notification or update protocol.  You probably want to do that anyway, because clients probably would like to see interim updates while a long operation is in process.
     * 
     * Unfortunately, I don�t think these kinds of things can be reasonably handled in a communication layer - the application almost always needs to be involved in the resolution of a problem when we have a service-oriented system.
     * 
     * Cheers,
     * 
     * Greg.
     * </pre>
     * 
     * and
     * 
     * <pre>
     * Hi Bryan,
     * 
     * A number of years ago Ann Wollrath (the inventor of RMI) wrote some example code
     * related to RMI call cancellation; providing both a JRMP and a JERI
     * configuration.
     * Although the example is old and hasn't been maintained, it may provide
     * the sort of
     * guidance and patterns you're looking for.
     * 
     * You can find the source code and related collateral at the following link:
     * 
     * <https://java.net/projects/bodega/sources/svn/show/trunk/src/archive/starterkit-examples/src/com/sun/jini/example/cancellation?rev-219>
     * 
     * I hope this helps,
     * Brian
     * </pre>
     * <pre>
     * This is one of the places where a lease could help. An extension of the
     * existing JERI details could add a lease into the dispatcher layer so that
     * a constant �I am here� message would come through to the service. If the
     * client thread is interrupted it would no longer be pinging/notifying of
     * it�s interest in the results. That would allow the service end, to take
     * appropriate actions. I think that I�d want the export operation or
     * exporter creation, to include the setup of a call back that would occur
     * when an client wants something to stop. I would make the API include a
     * �correlation-ID�, and I�d have that passed into the call to do work, and
     * passed into the call back for cancellation.
     * 
     * Gregg
     * </pre>
     */
    public void test_interruptRMI() throws Exception {
        
        // Start a service.
        final HAGlue serverA = startA();

        final AtomicReference<Throwable> localCause = new AtomicReference<Throwable>();

        final ExecutorService executorService = Executors
                .newSingleThreadScheduledExecutor(DaemonThreadFactory
                        .defaultThreadFactory());
        
        try {

            final FutureTask<Void> localFuture = new FutureTask<Void>(
                    new Callable<Void>() {

                        @Override
                        public Void call() throws Exception {

                            try {
                                final Future<Void> ft = ((HAGlueTest) serverA)
                                        .submit(new SleepTask(6000/* ms */),
                                                false/* asyncFuture */);

                                return ft.get();
                            } catch (Throwable t) {
                                localCause.set(t);
                                log.error(t, t);
                                throw new RuntimeException(t);
                            } finally {
                                log.warn("Local submit of remote task is done.");
                            }
                        }
                    });
            /*
             * Submit task that will execute sleep on A. This task will block
             * until A finishes its sleep. When we cancel this task, the RMI to
             * A will be interrupted.
             */
            executorService.execute(localFuture);
            
            // Wait a bit to ensure that the task was started on A.
            Thread.sleep(2000/* ms */);

            // interrupt the local future. will cause interrupt of the RMI.
            localFuture.cancel(true/*mayInterruptIfRunning*/);

        } finally {
        
            executorService.shutdownNow();
            
        }

        /*
         * The local root cause of the RMI failure is an InterruptedException.
         * 
         * Note: There is a data race between when the [localCause] is set and
         * when we exit the code block above. This is because we are
         * interrupting the local task and have no means to await the completion
         * of its error handling routine which sets the [localCause].
         */
        {
            assertCondition(new Runnable() {
                @Override
                public void run() {
                    final Throwable tmp = localCause.get();
                    assertNotNull(tmp);
                    assertTrue(InnerCause.isInnerCause(tmp,
                            InterruptedException.class));
                }
            }, 10000/*timeout*/, TimeUnit.MILLISECONDS);
        }

        /*
         * Verify that A does NOT observe the interrupt. Instead, the
         * Thread.sleep() should complete normally on A. See the comments at the
         * head of this test method.
         * 
         * Note: Again, there is a data race.
         * 
         * Note: Because we might retry this, we do NOT use the getAndClearXXX()
         * method to recover the remote exception.
         */
        {
            assertCondition(new Runnable() {
                @Override
                public void run() {
                    Throwable tmp;
                    try {
                        tmp = ((HAGlueTest) serverA).getLastRootCause();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    assertNull(tmp);
//                    log.warn("Received non-null lastRootCause=" + tmp, tmp);
//                    assertTrue(InnerCause.isInnerCause(tmp,
//                            InterruptedException.class));
                }
            }, 10000/* timeout */, TimeUnit.MILLISECONDS);
        }
        
    }

    /**
     * Task sleeps for a specified duration.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class SleepTask extends IndexManagerCallable<Void> {

        private static final long serialVersionUID = 1L;

        private long millis;

        SleepTask(final long millis) {
            this.millis = millis;
        }

        @Override
        public Void call() throws Exception {
            log.warn("Will sleep: millis=" + millis);
            try {
                Thread.sleep(millis);
                log.warn("Sleep finished normally.");
            } catch (Throwable t) {
                log.error("Exception during sleep: "+t, t);
                ((HAJournalTest) getIndexManager()).getRemoteImpl()
                        .setLastRootCause(t);
                throw new RuntimeException(t);
            } finally {
                log.warn("Did sleep: millis=" + millis);
            }
            return null;
        }

    }

}
