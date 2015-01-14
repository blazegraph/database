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

import java.io.IOException;
import java.util.UUID;

import org.eclipse.jetty.client.HttpClient;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.rdf.sail.webapp.client.AutoCloseHttpClient;
import com.bigdata.rdf.sail.webapp.client.DefaultClientConnectionManagerFactory;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepository;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;

/**
 * Test suite for the SPARQL query and SPARQL update request cancellation
 * protocol for an {@link HAJournalServer} quorum with a replication factor of
 * THREE (3).
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
     * 
     * @see <a href="http://trac.bigdata.com/ticket/883">CANCEL Query fails on
     *      non-default kb namespace on HA follower</a>
     */
    public void test_ABC_CancelQuery() throws Exception {
    	
        final ABC abc = new ABC(true/*sequential*/); 

        final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // Verify leader vs followers.
        awaitHAStatus(serverA, HAStatusEnum.Leader);
        awaitHAStatus(serverB, HAStatusEnum.Follower);
        awaitHAStatus(serverC, HAStatusEnum.Follower);

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, new HAGlue[] { serverA, serverB, serverC });

        /*
         * Do CANCEL for each service using the default namespace.
         */
        final JettyRemoteRepositoryManager[] rpms = new JettyRemoteRepositoryManager[3];
       	final HttpClient client = DefaultClientConnectionManagerFactory.getInstance().newInstance();
        
        {
            // Get RemoteRepository for each service.

            rpms[0] = getRemoteRepository(serverA, client);
            rpms[1] = getRemoteRepository(serverB, client);
            rpms[2] = getRemoteRepository(serverC, client);

            rpms[0].cancel(UUID.randomUUID());
            rpms[1].cancel(UUID.randomUUID());
            rpms[2].cancel(UUID.randomUUID());

        }
        
        /*
         * Do CANCEL for each service using the SPARQL end point associated with
         * a non-default namespace:
         * 
         * /namespace/NAMESPACE/sparql
         */
        {
            final String namespace = "kb";

            // Get RemoteRepository for each service.
            final JettyRemoteRepository[] repo = new JettyRemoteRepository[3];
            
            repo[0] = rpms[0].getRepositoryForNamespace(namespace);
            repo[1] = rpms[1].getRepositoryForNamespace(namespace);
            repo[2] = rpms[2].getRepositoryForNamespace(namespace);

            repo[0].cancel(UUID.randomUUID());
            repo[1].cancel(UUID.randomUUID());
            repo[2].cancel(UUID.randomUUID());
        }
        
        // Close down the rpms and stop the client
        for (JettyRemoteRepositoryManager rpm : rpms) {
        	rpm.close();
        }
        
        client.stop();

    }

}
