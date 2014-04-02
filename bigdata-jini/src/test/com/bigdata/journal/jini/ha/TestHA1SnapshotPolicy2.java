package com.bigdata.journal.jini.ha;

import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.msg.HARootBlockRequest;

public class TestHA1SnapshotPolicy2 extends AbstractHA3BackupTestCase {

    public TestHA1SnapshotPolicy2() {
    }

    public TestHA1SnapshotPolicy2(String name) {
        super(name);
    }
	
    /** How long to wait for snapshots to appear. */
    private final long awaitSnapshotMillis = 5000;
    
    protected String getZKConfigFile() {
    	return "zkClient1.config"; // 1 stage pipeline
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

        /*
         * We need to set the time at which the DefaultSnapshotPolicy runs to
         * some point in the Future in order to avoid test failures due to
         * violated assumptions when the policy runs up self-triggering (based
         * on the specified run time) during a CI run.
         */
        final String neverRun = getNeverRunSnapshotTime();
        
        return new String[]{
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.DefaultSnapshotPolicy("+neverRun+",0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor=1"
        };
        
    }

    /**
     * Verify that A takes a snapshot of the empty journal once the quorum
     * meets (but not before). 
     */
    public void test_A_snapshotOnQuorumMeet() throws Exception {

        // Start A
        final HAGlue serverA = startA();

         // Await quorum meet.
        awaitMetQuorum();
        
        // Expected commit point on A, B.
        final long commitCounter = 1;
        
        // Verify/await expected commit point.
        awaitCommitCounter(commitCounter, serverA);

        // Wait until both services are ready.
        awaitNSSAndHAReady(serverA);
        
         // Verify/await snapshot on A.
        awaitSnapshotExists(serverA, commitCounter);
    }

}
