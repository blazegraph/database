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

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.quorum.Quorum;
import com.bigdata.service.jini.JiniClientConfig;
import com.bigdata.util.InnerCause;
import com.bigdata.util.NV;

/**
 * Stress test suite for an {@link HAJournalServer} quorum with a replication
 * factor of THREE (3) and a fully met {@link Quorum} (refactored from
 * {@link TestHA3JournalServer}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class StressTestHA3JournalServer extends AbstractHA3JournalServerTestCase {

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
//        		"com.bigdata.journal.HAJournal.properties=" +TestHA3JournalServer.getTestHAJournalProperties(com.bigdata.journal.HAJournal.properties),
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\"",
                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
        };
        
    }
    
    public StressTestHA3JournalServer() {
    }

    public StressTestHA3JournalServer(String name) {
        super(name);
    }

    @Override
    protected int replicationFactor() {

        return 3;
        
    }

    /**
     * Complex hack to override the {@link HAJournal} properties.
     * 
     * @param in
     *            The {@link NV}[] from the configuration (if you can get it).
     *            
     * @return The {@link NV}[] from which the {@link Properties} will be
     *         constructed by {@link JiniClientConfig}
     */
    public static NV[] getTestHAJournalProperties(final NV[] in) {

        return in;
        
    }
    
//    protected BufferMode getDiskMode() {
//    	return BufferMode.DiskRW;
//    }

    /**
     * Atomicity stress test.
     * 
     * @throws Exception
     */
    public void testABCMultiLoadFollowerReads() throws Exception {
        
        doABCMultiLoadFollowerReads2(10/*nTransactions*/, false/*largeLoad*/);
        
    }
    
    /**
     * Atomicity stress test.
     * 
     * @throws Exception
     */
    public void testABCMultiLoadFollowerReadsLargeLoad() throws Exception {

        doABCMultiLoadFollowerReads2(5/*nTransactions*/, true/*largeLoad*/);
        
    }
    
    /**
     * Atomicity stress test based on <code>DROP ALL + LOAD</code> pattern. This
     * is similar to multitransaction but rather than a number of updates
     * following a load it is simply a number of loads followed by queries on
     * the folowers that are checkd for consistency. One load is performed up
     * front. We then read on the leader to identify the #of triples loaded and
     * verify that the same triple count is visible on the followers. We then
     * perform a series of <code>DROP ALL + LOAD</code> operations while running
     * concurrent <code>SELECT (COUNT(*) AS ?C) {?s ?p ?o}</code> queries. If
     * the platform is ACID, the queries should always report the same #of
     * triples (that is, the <code>DROP + LOAD</code> operation will never
     * expose an intermediate state in which just the DROP is visible or in
     * which the LOAD is only partly visible. After the initial LOAD and
     * verification, the COUNT(*) queries are asynchronous with respect to the
     * DROP + LOAD operations. This allows us to test many potential points
     * where atomicity might be validated.
     * 
     * @param loads
     * @param transactionDelay
     * @throws Exception
     * 
     *             TODO Port to the BigdataSail test suite and run for a single
     *             RWS, WORM, and MemStore backend.
     */
    protected void doABCMultiLoadFollowerReads2(final int nTransactions,
            final boolean largeLoad) throws Exception {

        try {

            // Start all services.
            final ABC services = new ABC(true/* sequential */);

            // Wait for a quorum meet.
            final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                    TimeUnit.MILLISECONDS);

            assertEquals(token, awaitFullyMetQuorum());

            final HAGlue leader = quorum.getClient().getLeader(token);

            // Verify assumption in this test.
            assertEquals(leader, services.serverA);

            // Wait until all services are "HA" ready.
            leader.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            services.serverB.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);
            services.serverC.awaitHAReady(awaitQuorumTimeout, TimeUnit.MILLISECONDS);

            // Do the initial DROPALL + LOAD and wait for the commit.
            new LargeLoadTask(token, largeLoad/* reallyLargeLoad */).call();

            // verify COUNT(*) is the same on all services.
            final long count = getCountStar(leader);
            assertEquals(count, getCountStar(services.serverB));
            assertEquals(count, getCountStar(services.serverC));
            assertTrue(count > 0); // non-zero.

            /**
             * Run until interrupted.  Will fail if 
             * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
             */
            class RunCountStarQueries implements Callable<Void> {

                public Void call() throws Exception {

                    try {

                        while (true) {

                            final long count0 = getCountStar(services.serverA);
                            final long count1 = getCountStar(services.serverB);
                            final long count2 = getCountStar(services.serverC);

                            if (log.isInfoEnabled())
                               log.info("StatementsA: " + count0
                                        + ", StatementsB: " + count1
                                        + ", StatementsC: " + count2
                                        );
                            
                            assertEquals(count, count0);
                            assertEquals(count, count1);
                            assertEquals(count, count2);

                            // Pause between query batches.
                            Thread.sleep(50/* ms */);
                            
                        }
                        
                    } catch (Throwable t) {

                        if (InnerCause.isInnerCause(t,
                                InterruptedException.class)) {

                            // Normal termination.
                            return null;
                            
                        }
                        
                        fail("Not expecting: " + t, t);
                        
                    }

                    // Keep the compiler happy.
                    return null;
                }
            };

            /*
             * Start the tasks to run the UPDATES (against the leader) and the
             * QUERIES (against a follower).
             */

            // Task to execute COUNT(*) queries against each service.
            final FutureTask<Void> queryTaskFuture = new FutureTask<Void>(
                    new RunCountStarQueries());
            
            try {

                // Run that task.
                executorService.submit(queryTaskFuture);
                
                // Now run a number of DROP ALL + LOAD tasks (in sequence).
                for (int t = 0 ; t < nTransactions; t++) {

                    final FutureTask<Void> loadTaskFuture = new FutureTask<Void>(
                            new LargeLoadTask(token, largeLoad/* reallyLargeLoad */));
                    
                    try {

                        executorService.submit(loadTaskFuture);

                        loadTaskFuture.get(); // wait on load!

                        if (log.isInfoEnabled())
                            log.info("Done with " + (t + 1) + " out of "
                                    + nTransactions + " loads");

                        if (queryTaskFuture.isDone()) {

                            /*
                             * Should run until cancelled. Check Future. Will probably
                             * thrown an exception.
                             */
                            
                            queryTaskFuture.get();
                            
                            // Should not be done unless cancelled.
                            fail("queryTask is done.");
                            
                        }

                    } finally {
                        
                        loadTaskFuture.cancel(true/* mayInterruptIfRunning */);
                        
                    }
                    
                }

            } finally {

                // Ensure cancelled.
                queryTaskFuture.cancel(true/*mayInterruptIfRunning*/);

            }

            assertDigestsEquals(new HAGlue[] { services.serverA,
                    services.serverB, services.serverC });

        } finally {
        
            destroyAll();
            
        }
            
    }
    
}
