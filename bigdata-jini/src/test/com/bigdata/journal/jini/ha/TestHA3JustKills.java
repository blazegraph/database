package com.bigdata.journal.jini.ha;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;

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
			
	        // start concurrent task loads that continue until fully met
	        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
	                token));

	        executorService.submit(ft);

	        // allow load head start
	        Thread.sleep(300/* ms */);

			// Verify load is still running.
			assertFalse(ft.isDone());
			
			// Dump Zookeeper
			log.warn("ZOOKEEPER\n" + dumpZoo());
			
			kill(startup.serverC);
			
			// FIXME: in the face of no implemented error propagation we can explicitly
			//	tell the leader to remove the killed service!
			startup.serverA.submit(new ForceRemoveService(getServiceCId()), true).get();

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
	    
	    public void testStressABC_LiveLoadRemainsMet_kill_C() throws Exception {
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
			
	        // start concurrent task loads that continue until fully met
	        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
	                token));

	        executorService.submit(ft);

	        // allow load head start
	        Thread.sleep(300/* ms */);

			// Verify load is still running.
			assertFalse(ft.isDone());
			
			// Dump Zookeeper
			log.warn("ZOOKEEPER\n" + dumpZoo());
			
			kill(startup.serverB);
			
			// FIXME: temporary call to explicitly remove the service prior to correct protocol
			startup.serverA.submit(new ForceRemoveService(getServiceBId()), true).get();

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
}
