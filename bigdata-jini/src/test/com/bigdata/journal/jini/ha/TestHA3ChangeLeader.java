package com.bigdata.journal.jini.ha;

import java.util.concurrent.TimeUnit;

import com.bigdata.ha.HAGlue;

public class TestHA3ChangeLeader extends AbstractHA3JournalServerTestCase  {
	
    /**
     * We have seen problems with updates when the leader changes, this test reconstructs
     * this simple scenario, with and update transaction, change of leader and then a 
     * second update transaction.
     * 
     * @throws Exception
     */
    public void testStartABC_ChangeLeader() throws Exception {

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
         * Now go through a commit point with a met quorum. The HALog
         * files should be retained at that commit point.
         */
        simpleTransaction();
        
        shutdownA();

        final long token2 = awaitNextQuorumMeet(token1);
       	        
        simpleTransaction();

        // And again verify binary equality of ALL journals.
        assertDigestsEquals(new HAGlue[] { serverB, serverC });
       
    }
}
