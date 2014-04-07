/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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

import java.util.UUID;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

/**
 * Test suites for an {@link HAJournalServer} quorum with a replication factor
 * of THREE (3) and a fully met {@link Quorum}.
 * 
 * TODO Do we have any guards against rolling back a service in RESYNC if the
 * other services are more than 2 commit points before it? We probably should
 * not automatically roll it back to the other services in this case, but that
 * could also reduce the ergonomics of the HA3 configuration.
 * 
 * TODO All of these live load remains met tests could also be done with BOUNCE
 * rather than SHUTDOWN/RESTART. BOUNCE exercises different code paths and
 * corresponds to a zookeeper timeout, e.g., as might occur during a full GC
 * pause.
 * 
 * TODO Update the existing tests to verify that the quorum token is properly
 * set on C when C resyncs with A+B and that
 * {@link AbstractJournal#getHAReady()} reports the correct token. This tests
 * for a problem where we did not call setQuorumToken() again when we resync and
 * transition into the met quorum. This meant that the HAReady token is not set
 * for a service unless it is part of the initial quorum meet. One of the HA3
 * backup tests covers this, but we should be checking the HAReadyToken in this
 * test suite as well.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA3CancelQuery extends AbstractHA3JournalServerTestCase {

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
//                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
        };
        
    }
    
    public TestHA3CancelQuery() {
    }

    public TestHA3CancelQuery(String name) {
        super(name);
    }

    /**
     * Starts 3 services in a known order. The leader will be A. The pipeline
     * order will be A, B, C. Issues cancel request to each of the services and
     * verifies that all services are willing to accept a POST of the CANCEL
     * request.
     */
    public void test_ABC_CancelQuery() throws Exception {
    	
        final ABC abc = new ABC(true/*sequential*/); 

        final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        // get RemoteRepository for each service.
        final RemoteRepository[] repo = new RemoteRepository[3];

        repo[0] = getRemoteRepository(serverA);
        repo[1] = getRemoteRepository(serverB);
        repo[2] = getRemoteRepository(serverC);

        // Verify leader vs followers.
        assertEquals(HAStatusEnum.Leader, serverA.getHAStatus());
        assertEquals(HAStatusEnum.Follower, serverB.getHAStatus());
        assertEquals(HAStatusEnum.Follower, serverC.getHAStatus());

        repo[0].cancel(UUID.randomUUID());
        repo[1].cancel(UUID.randomUUID());
        repo[2].cancel(UUID.randomUUID());
        
    }

}
