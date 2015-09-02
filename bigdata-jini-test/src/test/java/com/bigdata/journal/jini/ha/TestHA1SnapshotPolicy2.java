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
package com.bigdata.journal.jini.ha;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;

/**
 * Test suite for HA1 online backups and point in time restore.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA1SnapshotPolicy2 extends AbstractHA3BackupTestCase {

    public TestHA1SnapshotPolicy2() {
    }

    public TestHA1SnapshotPolicy2(String name) {
        super(name);
    }
	
    @Override
    protected int replicationFactor() {

        return 1;
        
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
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor="+replicationFactor()
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
