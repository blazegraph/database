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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;

/**
 * Test suite for HA1.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA1JournalServerStandalone extends AbstractHA3JournalServerTestCase {

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
                "com.bigdata.journal.jini.ha.HAJournalServer.replicationFactor="+replicationFactor(),
        };
        
    }
    
    public TestHA1JournalServerStandalone() {
    }

    public TestHA1JournalServerStandalone(String name) {
        super(name);
    }

    @Override
    protected int replicationFactor() {

        return 1;
        
    }


    protected void doStartA() throws Exception {

        try {
            quorum.awaitQuorum(awaitQuorumTimeout, TimeUnit.MILLISECONDS);

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

    /*
    public void testSimpleTransaction() throws Exception {

        doStartA();

        serverA.awaitHAReady(2, TimeUnit.SECONDS);

        awaitCommitCounter(1, new HAGlue[] { serverA });

        simpleTransaction();

        awaitCommitCounter(2, new HAGlue[] { serverA });

    }
    */
    
    public void testSimpleTransactionLBS() throws Exception {

        doStartA();

        serverA.awaitHAReady(2, TimeUnit.SECONDS);

        awaitCommitCounter(1, new HAGlue[] { serverA });

        simpleTransactionLBS();

        awaitCommitCounter(2, new HAGlue[] { serverA });
        
    }
}
