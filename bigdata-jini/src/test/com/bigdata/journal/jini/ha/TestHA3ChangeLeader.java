package com.bigdata.journal.jini.ha;

import java.util.Random;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.bigdata.ha.HAGlue;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;

public class TestHA3ChangeLeader extends AbstractHA3JournalServerTestCase  {
	
    public TestHA3ChangeLeader() {
    }

    public TestHA3ChangeLeader(String name) {
        super(name);
    }

    /**
     * We have seen problems with updates when the leader changes, this test reconstructs
     * this simple scenario, with and update transaction, change of leader and then a 
     * second update transaction.
     * 
     * @throws Exception
     */
    public void testStartABC_ChangeLeader() throws Exception {

        doStartABC_ChangeLeader(1);
    }

    public void testStartABC_ChangeLeader_2Trans() throws Exception {

        doStartABC_ChangeLeader(2);
    }
    
    public void testStartABC_ChangeLeader_3Trans() throws Exception {

        doStartABC_ChangeLeader(3);
    }
    
    public void testStartABC_ChangeLeader_RandomTrans() throws Exception {
        final Random r = new Random();
        final int ntrans = r.nextInt(900);
        try {
            doStartABC_ChangeLeader(ntrans);
        } catch (Exception e) {
            log.error("Problem with " + ntrans + " transactions");
            throw e;
        }
    }
    
    public void doStartABC_ChangeLeader(final int ntrans) throws Exception {

        // Start 3 services
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        // Wait for a quorum meet.
        final long token1 = awaitFullyMetQuorum();

        // await pipeline
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] { serverA, serverB,
                serverC });

        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

		/*
         * Now go through sevearl commit points with a met quorum. The HALog
         * files should be retained at the final commit point.
         */
        for (int t = 0; t < ntrans; t++) {
        	simpleTransaction();
        }
        
        shutdownA();

        final long token2 = awaitNextQuorumMeet(token1);
       	
        // let's commit several transactions with the new leader
        for (int t = 0; t < 20; t++) {
            simpleTransaction();
            // Check store states
            assertStoreStates(new HAGlue[] { serverB, serverC });
        }

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverB, serverC });
       
        assertTrue(token2 == quorum.token());
    }
    
    public void testStartABC_KillLeader_RandomTrans() throws Exception {
        fail("Test disabled pending reconcilation of socket ticket");
        final Random r = new Random();
        final int ntrans = r.nextInt(900);
        try {
            doStartABC_KillLeader(ntrans);
        } catch (Exception e) {
            log.error("Problem with " + ntrans + " transactions");
            throw e;
        }
    }
    
    private void doStartABC_KillLeader(final int ntrans) throws Exception {

        // Start 3 services
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        // Wait for a quorum meet.
        final long token1 = awaitFullyMetQuorum();

        // await pipeline
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] { serverA, serverB,
                serverC });

        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

		/*
         * Now go through sevearl commit points with a met quorum. The HALog
         * files should be retained at the final commit point.
         */
        for (int t = 0; t < ntrans; t++) {
        	simpleTransaction();
        	
        	// Check store states
        	assertStoreStates(new HAGlueTest[] { (HAGlueTest) serverA, (HAGlueTest) serverB, (HAGlueTest) serverC });
        }
        
		kill(serverA);

        final long token2 = awaitNextQuorumMeet(token1);
       	        
        // let's commit several transactions with the new leader
        for (int t = 0; t < 20; t++) {
        	simpleTransaction();
        }

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverB, serverC });
       
    	assertStoreStates(new HAGlueTest[] { (HAGlueTest) serverB, (HAGlueTest) serverC });
        assertTrue(token2 == quorum.token());
    }
    /**
     * Similar to ChangeLeader but with a LargeLoad
     */
    public void _testStartABC_StressChangeLeader() throws Exception {

        // Start 3 services
        final HAGlue serverA = startA();
        final HAGlue serverB = startB();
        final HAGlue serverC = startC();

        // Wait for a quorum meet.
        final long token1 = awaitFullyMetQuorum();

        // await pipeline
        awaitPipeline(20, TimeUnit.SECONDS, new HAGlue[] { serverA, serverB,
                serverC });

        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        /*
         * LOAD data on leader.
         */
        for (int i = 0; i < 100; i++) {
	        final FutureTask<Void> ft = new FutureTask<Void>(new LargeLoadTask(
	                token1, true/* reallyLargeLoad */, false/*dropAll*/));
	
	        // Start LOAD.
	        executorService.submit(ft);
	        
	        // Await LOAD, but with a timeout.
	        ft.get(longLoadTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        
    	assertStoreStates(new HAGlue[] { serverA, serverB, serverC });

    	shutdownA();

        final long token2 = awaitNextQuorumMeet(token1);
       	        
        simpleTransaction();

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverB, serverC });
        
    	assertStoreStates(new HAGlue[] { serverB, serverC });
    	
        assertTrue(token2 == quorum.token());
       
    }
}
