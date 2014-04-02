package com.bigdata.journal.jini.ha;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;

import net.jini.config.Configuration;


public class TestHA1JournalServer extends AbstractHA3JournalServerTestCase {

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
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor=1",
        };
        
    }
    
    protected String getZKConfigFile() {
    	return "zkClient1.config"; // 1 stage pipeline
    }
    
    public TestHA1JournalServer() {
    }

    public TestHA1JournalServer(String name) {
        super(name);
    }

    public void testStartA() throws Exception {
    	doStartA();
    }
    
    protected void doStartA() throws Exception {

        try {
        	quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        	
        	fail("HA1 requires quorum of 1!");
        } catch (TimeoutException te) {
        	// expected
        }

        // Start 1 service.
        final HAGlue serverA = startA();
        
        // this should succeed
        final long token = quorum.awaitQuorum(awaitQuorumTimeout,
                TimeUnit.MILLISECONDS);
        
        assertEquals(token, awaitFullyMetQuorum());
        
        final HAGlue leader = quorum.getClient().getLeader(token);
        
        assertEquals(serverA, leader);
    }
    
    public void testSimpleTransaction() throws Exception {
    	doStartA();
    	
        serverA.awaitHAReady(2, TimeUnit.SECONDS);
        
        /*
         * Awaiting HAReady is not sufficient since the service may still
         * writing the initial transaction.
         * 
         * So it seems that the problem is not so much with HA1 as rather the
         * status of a new journal being ready too soon to process an NSS
         * request
         */
        
        awaitCommitCounter(1, new HAGlue[] { serverA});
        
        // Thread.sleep(100);
        
        // serverA.
    	
    	log.warn("Calling SimpleTransaction");
    	simpleTransaction();
    	
        awaitCommitCounter(2, new HAGlue[] { serverA});
    }
    
    public void testMultiTransaction() throws Exception {
    	doStartA();
    	
        awaitCommitCounter(1, new HAGlue[] { serverA});
        // Thread.sleep(1000);
    	
        final int NTRANS = 10;
    	for (int t = 0; t < NTRANS; t++) {
    		simpleTransaction();
    	}

        awaitCommitCounter(NTRANS+1, new HAGlue[] { serverA});
}
}
