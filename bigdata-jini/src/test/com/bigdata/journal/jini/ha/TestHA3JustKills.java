/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reservesuper.

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
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.jini.ha.HAJournalServer.HAQuorumService.IHAProgressListener;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;

/**
 * Test suite focused on sure kills (kill -9) of processes and correct handling
 * of failover in the face of such sudden process kills.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA3JustKills extends AbstractHA3JournalServerTestCase {

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
//	        		"com.bigdata.journal.HAJournal.properties=" +TestHA3JournalServer.getTestHAJournalProperties(com.bigdata.journal.HAJournal.properties),
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//	                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\"",
                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
        };
        
    }
    
    public TestHA3JustKills() {
    }

    public TestHA3JustKills(String name) {
        super(name);
    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, sure kill C (the last
     * follower). Verify that the LOAD completes successfully with the remaining
     * services (A+B).
     */
    public void testABC_LiveLoadRemainsMet_kill_C() throws Exception {

        // enforce join order
		final ABC startup = new ABC(true /*sequential*/);
		
		final long token = awaitFullyMetQuorum();
		
        // await the initial KB commit on all services.
        awaitCommitCounter(1L, new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(2000/* ms */);

		// Verify load is still running.
		assertFalse(ft.isDone());
		
		// Dump Zookeeper
		log.warn("ZOOKEEPER\n" + dumpZoo());
		
		kill(startup.serverC);

		// Note: Automatic.
//		startup.serverA.submit(new ForceRemoveService(getServiceCId()), true).get();

		awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] {startup.serverA, startup.serverB});

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        awaitMembers(new HAGlue[] {startup.serverA, startup.serverB});
		awaitJoined(new HAGlue[] {startup.serverA, startup.serverB});

		// token must remain unchanged to indicate same quorum
		assertEquals(token, awaitMetQuorum());
		
        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

    }
    
    public void _testStressABC_LiveLoadRemainsMet_kill_C() throws Exception {
        for (int i = 0; i < 5; i++) {
            try {
            	testABC_LiveLoadRemainsMet_kill_C();
            } catch (Throwable t) {
                fail("Run " + i, t);
            } finally {
            	Thread.sleep(1000);
                destroyAll();
            }
        }
    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, sure kill B (the first
     * follower). Verify that the LOAD completes successfully with the remaining
     * services (A+C), after the leader re-orders the pipeline.
     */
    public void testABC_LiveLoadRemainsMet_kill_B() throws Exception {

        // enforce join order
		final ABC startup = new ABC(true /*sequential*/);
		
		final long token = awaitFullyMetQuorum();
		
        // await the initial KB commit on all services.
        awaitCommitCounter(1L, new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // allow load head start
        Thread.sleep(2000/* ms */);

		// Verify load is still running.
		assertFalse(ft.isDone());
		
		// Dump Zookeeper
		log.warn("ZOOKEEPER\n" + dumpZoo());
		
		kill(startup.serverB);
		
		// Note: automatic.
//		startup.serverA.submit(new ForceRemoveService(getServiceBId()), true).get();

		awaitPipeline(10, TimeUnit.SECONDS, new HAGlue[] {startup.serverA, startup.serverC});
		
		// also check members and joined
		awaitMembers(new HAGlue[] {startup.serverA, startup.serverC});
		awaitJoined(new HAGlue[] {startup.serverA, startup.serverC});

		// token must remain unchanged to indicate same quorum
		assertEquals(token, awaitMetQuorum());
		
        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

    }

    /**
     * Base class for sure kill of a process when write replication reaches a
     * specific trigger condition.
     * <p>
     * Note: A concrete static instance of the {@link IHAProgressListener} MUST
     * be used since this class MUST be {@link Serializable}. An inner class
     * WILL NOT work since it would attempt to serialize the outer test class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    abstract static private class MyHAProgressListenerKillPID implements
            IHAProgressListener, Serializable {

        private static final long serialVersionUID = 1L;
        
        final private int pid;

        /**
         * 
         * @param pid
         *            The process to be killed.
         */
        public MyHAProgressListenerKillPID(final int pid) {
            this.pid = pid;
        }
        
        /** Sure kill of {@link #pid}. */
        protected void sureKill() {
            log.fatal("Will sure kill process: pid=" + pid);
            trySignal(SignalEnum.KILL, pid);
        }

    }

    private static class HAProgressListenerKillPID_1 extends
            MyHAProgressListenerKillPID {

        private static final long serialVersionUID = 1L;

        public HAProgressListenerKillPID_1(int pid) {
            super(pid);
        }

        @Override
        public void incReceive(final IHASyncRequest req,
                final IHAWriteMessage msg, final int nreads, final int rdlen,
                final int rem) throws Exception {
            
            log.error("msg=" + msg + ", nreads=" + nreads + ", rdlen=" + rdlen
                    + ", rem=" + rem);
            
            /*
             * Note: This is the *opening* root block counter. The process will
             * be killed as soon as it has received the first chunk of data for
             * the payload of the first replicated write cache block.
             */
  
            if (msg.getCommitCounter() == 1L) {

                sureKill();

            }

        }

    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, sure kill B (the first
     * follower) when it has received a specified number of bytes of data and is
     * attempting to incrementally replicate that data to C (the 2nd follower).
     * Verify that the LOAD completes successfully with the remaining services
     * (A+C), after the leader re-orders the pipeline.
     */
    public void testABC_LiveLoadRemainsMet_kill_B_duringIncrementalReplication()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /* sequential */);

        final long token = awaitFullyMetQuorum();

        // await the initial KB commit on all services.
        awaitCommitCounter(1L, new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        // Set a trigger to sure kill B once it reaches the specified
        // replication point.
        ((HAGlueTest) startup.serverB)
                .failWriteReplication(new HAProgressListenerKillPID_1(
                        ((HAGlueTest) startup.serverB).getPID()));

        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // Wait until B is killed.
        assertCondition(new Runnable() {
            public void run() {
                try {
                    startup.serverB.getWritePipelineAddr();
                    fail("B is still running.");
                } catch (IOException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: B is no longer responding: "
                                + ex);
                    return;
                }
            }
        }, 20000, TimeUnit.MILLISECONDS);

        // Verify load is still running.
        assertFalse(ft.isDone());

        // Dump Zookeeper
        log.warn("ZOOKEEPER\n" + dumpZoo());

        // Note: automatic.
//        startup.serverA.submit(new ForceRemoveService(getServiceBId()), true)
//                .get();

        awaitPipeline(10, TimeUnit.SECONDS, new HAGlue[] { startup.serverA,
                startup.serverC });

        // also check members and joined
        awaitMembers(new HAGlue[] { startup.serverA, startup.serverC });
        awaitJoined(new HAGlue[] { startup.serverA, startup.serverC });

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, sure kill C (the 2nd
     * follower) when it has received a specified number of bytes of data.
     * Verify that the LOAD completes successfully with the remaining services
     * (A+B).
     */
    public void testABC_LiveLoadRemainsMet_kill_C_duringIncrementalReplication()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /* sequential */);

        final long token = awaitFullyMetQuorum();

        // await the initial KB commit on all services.
        awaitCommitCounter(1L, new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        // Set a trigger to sure kill C once it reaches the specified
        // replication point.
        ((HAGlueTest) startup.serverC)
                .failWriteReplication(new HAProgressListenerKillPID_1(
                        ((HAGlueTest) startup.serverC).getPID()));

        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // Wait until C is killed.
        assertCondition(new Runnable() {
            public void run() {
                try {
                    startup.serverC.getWritePipelineAddr();
                    fail("C is still running.");
                } catch (IOException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: C is no longer responding: "
                                + ex);
                    return;
                }
            }
        }, 20000, TimeUnit.MILLISECONDS);

        // Verify load is still running.
        assertFalse(ft.isDone());

        // Dump Zookeeper
        log.warn("ZOOKEEPER\n" + dumpZoo());

        // Note: automatic.
//        startup.serverA.submit(new ForceRemoveService(getServiceCId()), true)
//                .get();

        awaitPipeline(10, TimeUnit.SECONDS, new HAGlue[] { startup.serverA,
                startup.serverB });

        // also check members and joined
        awaitMembers(new HAGlue[] { startup.serverA, startup.serverB });
        awaitJoined(new HAGlue[] { startup.serverA, startup.serverB });

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

    }

    /**
     * Start A+B+C in strict sequence. Wait until the quorum fully meets. Start
     * a long running LOAD. While the LOAD is running, C will issue a sure kill
     * of B (the 1st follower) when it has received a specified number of bytes
     * of data from B. Verify that the LOAD completes successfully with the
     * remaining services (A+C).
     */
    public void testABC_LiveLoadRemainsMet_C_kills_B_duringIncrementalReplication()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /* sequential */);

        final long token = awaitFullyMetQuorum();

        // await the initial KB commit on all services.
        awaitCommitCounter(1L, new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        // Set a trigger to sure kill B once C reaches the specified
        // replication point.
        ((HAGlueTest) startup.serverC)
                .failWriteReplication(new HAProgressListenerKillPID_1(
                        ((HAGlueTest) startup.serverB).getPID()));

        // start large load.
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // Wait until B is killed.
        assertCondition(new Runnable() {
            public void run() {
                try {
                    startup.serverB.getWritePipelineAddr();
                    fail("B is still running.");
                } catch (IOException ex) {
                    if (log.isInfoEnabled())
                        log.info("Expected exception: B is no longer responding: "
                                + ex);
                    return;
                }
            }
        }, 20000, TimeUnit.MILLISECONDS);

        // Verify load is still running.
        assertFalse(ft.isDone());

        // Dump Zookeeper
        log.warn("ZOOKEEPER\n" + dumpZoo());

        // Note: automatic.
//        startup.serverA.submit(new ForceRemoveService(getServiceCId()), true)
//                .get();

        awaitPipeline(10, TimeUnit.SECONDS, new HAGlue[] { startup.serverA,
                startup.serverC });

        // also check members and joined
        awaitMembers(new HAGlue[] { startup.serverA, startup.serverC });
        awaitJoined(new HAGlue[] { startup.serverA, startup.serverC });

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

    }

    /**
     * Test where we start A+B+C in strict sequence. Once we observe that all
     * services have gone through the initial KB create, we do a sudden kill of
     * B. We then start the live load. This test explores what happens when A is
     * not yet aware that B is dead when the UPDATE operation starts.
     * 
     * @throws Exception
     */
    public void testABC_awaitKBCreate_killB_LiveLoadRemainsMet()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /* sequential */);

        final long token = awaitFullyMetQuorum();

        // await the initial KB commit on all services.
        awaitCommitCounter(1L, new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        kill(startup.serverB);

        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        awaitPipeline(getZKSessionTimeout() + 5000, TimeUnit.MILLISECONDS,
                new HAGlue[] { startup.serverA, startup.serverC });

        // also check members and joined
        awaitMembers(new HAGlue[] { startup.serverA, startup.serverC });
        awaitJoined(new HAGlue[] { startup.serverA, startup.serverC });

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());
        
    }

    /**
     * Test where we start A+B+C in strict sequence. Once we observe that all
     * services have gone through the initial KB create, we do a sudden kill of
     * C. We then start the live load. This test explores what happens when A
     * and B are not yet aware that C is dead when the UPDATE operation starts.
     * 
     * @throws Exception
     */
    public void testABC_awaitKBCreate_killC_LiveLoadRemainsMet()
            throws Exception {

        // enforce join order
        final ABC startup = new ABC(true /* sequential */);

        final long token = awaitFullyMetQuorum();

        // await the initial KB commit on all services.
        awaitCommitCounter(1L, new HAGlue[] { startup.serverA, startup.serverB,
                startup.serverC });

        kill(startup.serverC);

        // start concurrent task loads that continue until fully met
        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
                token));

        executorService.submit(ft);

        // Note: Automatic.
//        startup.serverA.submit(new ForceRemoveService(getServiceCId()), true)
//                .get();

        awaitPipeline(getZKSessionTimeout() + 5000, TimeUnit.MILLISECONDS,
                new HAGlue[] { startup.serverA, startup.serverB });

        // also check members and joined
        awaitMembers(new HAGlue[] { startup.serverA, startup.serverB });
        awaitJoined(new HAGlue[] { startup.serverA, startup.serverB });

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());

        // Await LOAD, but with a timeout.
        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);

        // token must remain unchanged to indicate same quorum
        assertEquals(token, awaitMetQuorum());
        
    }

}
