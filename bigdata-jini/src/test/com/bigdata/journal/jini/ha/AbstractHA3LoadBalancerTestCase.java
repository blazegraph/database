/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.io.Serializable;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.jini.ha.HAJournalServer.HAQuorumService;
import com.bigdata.journal.jini.ha.HAJournalTest.HAGlueTest;
import com.bigdata.rdf.sail.webapp.HALoadBalancerServlet;
import com.bigdata.rdf.sail.webapp.client.JettyHttpClient;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepositoryManager;
import com.bigdata.rdf.sail.webapp.client.JettyRemoteRepository.RemoveOp;
import com.bigdata.rdf.sail.webapp.lbs.IHALoadBalancerPolicy;

/**
 * Test suite for the HA load balancer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/624"> HA Load Balancer </a>
 * 
 *      FIXME Test query using GET and POST to ensure that we have coverage in
 *      the load balancer servlet for proper forwarding (to the local service)
 *      and proper proxying (to a remote service) for both GET and POST. Also
 *      test PUT and DELETE methods. Tests need to be written to the leader (for
 *      forward to the local machine) and the follower (for proxying to the
 *      leader).
 *      <p>
 *      Test coverage should also extend to a service that is running but not
 *      joined with the met quorum. E.g., in an error state. Right now that will
 *      not work because the error state terminates the zk client connection and
 *      the HA load balancer is reusing the same zk client as the embedded
 *      HAJournalServer.
 * 
 *      FIXME Make sure that each concrete implementation of this class sets the
 *      specific load balancer policy to be tested *and* verifies that the
 *      specific load balancer policy is in effect.
 * 
 *      FIXME Write tests of handling when a service is up but not joined with
 *      the met quorum and when a service is not running. There are going to be
 *      some cases where the {@link HALoadBalancerServlet} proxies a request
 *      where the target service goes asynchronously and is unable to respond.
 *      What kinds of exceptions does this generate and how can we handle those
 *      exceptions?
 * 
 *      FIXME Write tests of handling when the target service of the original
 *      request goes into an error state concurrent with the request. The
 *      {@link HALoadBalancerServlet} should be robust even when the
 *      {@link HAQuorumService} associated with the {@link HAJournalServer} is
 *      not running. We do not want to be unable to proxy to another service
 *      just because this one is going through an error state. Would it make
 *      more sense to have a 2nd Quorum object for this purpose - one that is
 *      not started and stopped by the HAJournalServer?
 * 
 *      FIXME Verify that the bigdata remote service call works when the client
 *      is a bigdata HA replication cluster. This should work just fine if the
 *      sparqlEndpointURL is <code>.../bigdata/LBS/sparql</code>.
 *      
 *      FIXME Verify correct load balancing when addressing a non-default
 *      namespace (.../bigdata/LBS/namespace/NAMESPACE/sparql).
 */
abstract public class AbstractHA3LoadBalancerTestCase extends
        AbstractHA3JournalServerTestCase {

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
//              "com.bigdata.journal.HAJournal.properties=" +TestHA3JournalServer.getTestHAJournalProperties(com.bigdata.journal.HAJournal.properties),
                "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
                "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//                "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\"",
//                "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
//                /*
//                 * Setup configuration that supports the LBS. Note: This does not work. We would need to override (replace) HAJournal.properties to do this.
//                 */
//                "com.bigdata.journal.PlatformStatsPlugIn.Options.COLLECT_PLATFORM_STATISTICS=true",
//                "com.bigdata.journal.GangliaPlugIn.Options.GANGLIA_LISTEN=true",
//                "com.bigdata.journal.GangliaPlugIn.Options.GANGLIA_REPORT=true",
//                "com.bigdata.journal.GangliaPlugIn.Options.REPORT_DELAY=2000", // NB: short delay is used to develop the HALBS.
//                "com.bigdata.rdf.sail.webapp.HALoadBalancerServlet.policy=com.bigdata.rdf.sail.webapp.HALoadBalancerServlet.NOPLBSPolicy"
        };
        
    }
    
    public AbstractHA3LoadBalancerTestCase() {
    }

    public AbstractHA3LoadBalancerTestCase(final String name) {

        super(name);
        
    }

    protected void setPolicy(final IHALoadBalancerPolicy policy,
            final HAGlue... services) throws IOException {

        for(HAGlue service : services) {
            
            ((HAGlueTest) service).setHALoadBalancerPolicy(policy);

        }
        
    }
    
    /**
     * Return a {@link Serializable} instance of the
     * {@link IHALoadBalancerPolicy} to be tested.
     * <p>
     * Note: The {@link ServletContext} on the target server will govern
     * the initialization of the policy.
     * 
     * @see IHALoadBalancerPolicy#init(ServletConfig, IIndexManager)
     */
    abstract protected IHALoadBalancerPolicy newTestPolicy();
    
    /**
     * Simple tests with the load balancer enabled. This test verifies that we
     * can issue both read and write requests to the load balancer. Since read
     * requests are silently forwarded to the local service if the load balancer
     * is disabled, this is really only giving us good information about whether
     * or not the update requests have been proxied.
     */
    public void test_HA3LoadBalancer_01() throws Exception {

        final ABC abc = new ABC(true/* sequential */);

        final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

        final HAGlue[] services = new HAGlue[] { serverA, serverB, serverC };
        
        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, services);

        // Verify binary equality of ALL journals.
        assertDigestsEquals(services);

        // Impose the desired LBS policy.
        setPolicy(newTestPolicy(), services);
        
        // Shared JettyHttpCLient
        final JettyHttpClient client = new JettyHttpClient();
        client.start();
        
        // Repositories without the LBS.
        final JettyRemoteRepositoryManager[] repos = new JettyRemoteRepositoryManager[3];
        // Repositories with the LBS.
        final JettyRemoteRepositoryManager[] reposLBS = new JettyRemoteRepositoryManager[3];
        try {
	        repos[0] = getRemoteRepository(serverA, client);
	        repos[1] = getRemoteRepository(serverB, client);
	        repos[2] = getRemoteRepository(serverC, client);
	
	        /*
	         * Verify that query on all nodes is allowed.
	         */
	        for (JettyRemoteRepositoryManager r : repos) {
	
	            // Should be empty.
	            assertEquals(0L,
	                    countResults(r.prepareTupleQuery("SELECT * {?a ?b ?c}")
	                            .evaluate()));
	
	        }
	
	        reposLBS[0] = getRemoteRepository(serverA, true/* useLBS */, client);
	        reposLBS[1] = getRemoteRepository(serverB, true/* useLBS */, client);
	        reposLBS[2] = getRemoteRepository(serverC, true/* useLBS */, client);
	
	        /*
	         * Verify that query on all nodes is allowed using the LBS.
	         */
	        for (JettyRemoteRepositoryManager r : reposLBS) {
	
	            // Should be empty.
	            assertEquals(0L,
	                    countResults(r.prepareTupleQuery("SELECT * {?a ?b ?c}")
	                            .evaluate()));
	
	        }
	
	        /*
	         * Send an update request to the LBS on each service. The request should
	         * be proxied to the leader if it is directed to a follower. This is how
	         * we know whether the LBS is active or not. If it is not active, then
	         * the follower will refuse to process the update.
	         */
	
	        simpleTransaction_noQuorumCheck(serverA, true/* useLBS */); // leader.
	        simpleTransaction_noQuorumCheck(serverB, true/* useLBS */); // follower.
	        simpleTransaction_noQuorumCheck(serverC, true/* useLBS */); // follower.
	
	        // await the KB create commit point to become visible on each service.
	        awaitCommitCounter(4L, new HAGlue[] { serverA, serverB, serverC });
	
	        // Verify binary equality of ALL journals.
	        assertDigestsEquals(new HAGlue[] { serverA, serverB, serverC });
        } finally {
	        for (JettyRemoteRepositoryManager r : repos) {
	        	if (r != null)
	        		r.close();
	        }
	        for (JettyRemoteRepositoryManager r : reposLBS) {
	        	if (r != null)
	        		r.close();
	        }
	        
	        client.stop();
        }

    }

    /**
     * Test of DELETE on the leader.
     */
    public void test_delete_leader() throws Exception {

        final ABC abc = new ABC(true/* sequential */);

        final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

        // The expected HStatus for each of the services (A,B,C).
        final HAStatusEnum[] expectedHAStatusArray = new HAStatusEnum[] { //
                HAStatusEnum.Leader, //
                HAStatusEnum.Follower,//
                HAStatusEnum.Follower //
                };

        // The services in their join order.
        final HAGlue[] services = new HAGlue[] { serverA, serverB, serverC };
        
        // Verify quorum is FULLY met.
        awaitFullyMetQuorum();

        // await the KB create commit point to become visible on each service.
        awaitCommitCounter(1L, services);

        // Verify binary equality of ALL journals.
        assertDigestsEquals(services);

        // Verify the HAStatus of each service.
        awaitHAStatus(expectedHAStatusArray, services);

        // Impose the desired LBS policy.
        setPolicy(newTestPolicy(), services);
        
        // Shared HttpClient
        final JettyHttpClient client = new JettyHttpClient();
        client.start();
        
        // Repositories without the LBS.
        final JettyRemoteRepositoryManager[] repos = new JettyRemoteRepositoryManager[3];
        // Repositories with the LBS.
        final JettyRemoteRepositoryManager[] reposLBS = new JettyRemoteRepositoryManager[3];
        
		try {

			repos[0] = getRemoteRepository(serverA, client);
			repos[1] = getRemoteRepository(serverB, client);
			repos[2] = getRemoteRepository(serverC, client);

			reposLBS[0] = getRemoteRepository(serverA, true/* useLBS */, client);
			reposLBS[1] = getRemoteRepository(serverB, true/* useLBS */, client);
			reposLBS[2] = getRemoteRepository(serverC, true/* useLBS */, client);

			// Add some data using leader.
			simpleTransaction_noQuorumCheck(serverA, false/* useLBS */); // leader.

			// Should be non-zero.
			assertNotSame(0L, getCountStar(serverA, false/* useLBS */));

			// delete everything using leader.
			reposLBS[0].remove(new RemoveOp(null, null, null));

			// Verify everything is gone on the leader.
			awaitHAStatus(serverA, HAStatusEnum.Leader);

			// Should be zero.
			assertEquals(0L, getCountStar(serverA, false/* useLBS */));

			// await the commit point to become visible on each service.
			awaitCommitCounter(3L, services);

			// Verify binary equality of ALL journals.
			assertDigestsEquals(services);

		} finally {
	        for (JettyRemoteRepositoryManager r : repos) {
	        	if (r != null)
	        		r.close();
	        }
	        for (JettyRemoteRepositoryManager r : reposLBS) {
	        	if (r != null)
	        		r.close();
	        }
	        
	        client.stop();
        }

    }
    
    /**
     * Test of DELETE on the follower.
     */
	public void test_delete_follower() throws Exception {

		final ABC abc = new ABC(true/* sequential */);

		final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

		// The expected HStatus for each of the services (A,B,C).
		final HAStatusEnum[] expectedHAStatusArray = new HAStatusEnum[] { //
		HAStatusEnum.Leader, //
				HAStatusEnum.Follower,//
				HAStatusEnum.Follower //
		};

		// The services in their join order.
		final HAGlue[] services = new HAGlue[] { serverA, serverB, serverC };

		// Verify quorum is FULLY met.
		awaitFullyMetQuorum();

		// await the KB create commit point to become visible on each service.
		awaitCommitCounter(1L, services);

		// Verify binary equality of ALL journals.
		assertDigestsEquals(services);

		// Verify the HAStatus of each service.
		awaitHAStatus(expectedHAStatusArray, services);

		// Impose the desired LBS policy.
		setPolicy(newTestPolicy(), services);

		final JettyHttpClient client = new JettyHttpClient();
		client.start();
		// Repositories without the LBS.
		final JettyRemoteRepositoryManager[] repos = new JettyRemoteRepositoryManager[3];
		// Repositories with the LBS.
		final JettyRemoteRepositoryManager[] reposLBS = new JettyRemoteRepositoryManager[3];
		try {
			repos[0] = getRemoteRepository(serverA, client);
			repos[1] = getRemoteRepository(serverB, client);
			repos[2] = getRemoteRepository(serverC, client);

			reposLBS[0] = getRemoteRepository(serverA, true/* useLBS */, client);
			reposLBS[1] = getRemoteRepository(serverB, true/* useLBS */, client);
			reposLBS[2] = getRemoteRepository(serverC, true/* useLBS */, client);

			// Add some data using leader.
			simpleTransaction_noQuorumCheck(serverA, false/* useLBS */); // leader.

			// Should be non-zero.
			assertNotSame(0L, getCountStar(serverA, false/* useLBS */));

			// delete everything using 1st follower
			reposLBS[1].remove(new RemoveOp(null, null, null));

			// Verify everything is gone on the leader.
			awaitHAStatus(serverA, HAStatusEnum.Leader);

			// Should be zero.
			assertEquals(0L, getCountStar(serverA, false/* useLBS */));

			// await the commit point to become visible on each service.
			awaitCommitCounter(3L, services);

			// Verify binary equality of ALL journals.
			assertDigestsEquals(services);
		} finally {
			for (JettyRemoteRepositoryManager r : repos) {
				if (r != null)
					r.close();
			}
			for (JettyRemoteRepositoryManager r : reposLBS) {
				if (r != null)
					r.close();
			}
			
			client.stop();
		}

	}
    
    /**
     * Test of a simple sequence of events that I often test through a web
     * browser. This verifies that we can read on A and B using
     * 
     * <pre>
     * SELECT COUNT (*)
     * </pre>
     * 
     * and that we can write on A and B using
     * 
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * INSERT DATA
     * { 
     *   <http://example/book1> dc:title "A new book" ;
     *                          dc:creator "A.N.Other" .
     * }
     * </pre>
     * 
     * and
     * 
     * <pre>
     * PREFIX dc: <http://purl.org/dc/elements/1.1/>
     * DELETE DATA
     * { 
     *   <http://example/book1> dc:title "A new book" ;
     *                          dc:creator "A.N.Other" .
     * }
     * </pre>
     */
	public void test_simple_sequence() throws Exception {

		final ABC abc = new ABC(true/* sequential */);

		final HAGlue serverA = abc.serverA, serverB = abc.serverB, serverC = abc.serverC;

		// The expected HStatus for each of the services (A,B,C).
		final HAStatusEnum[] expectedHAStatusArray = new HAStatusEnum[] { //
		HAStatusEnum.Leader, //
				HAStatusEnum.Follower,//
				HAStatusEnum.Follower //
		};

		// The services in their join order.
		final HAGlue[] services = new HAGlue[] { serverA, serverB, serverC };

		// Verify quorum is FULLY met.
		awaitFullyMetQuorum();

		// await the KB create commit point to become visible on each service.
		awaitCommitCounter(1L, services);

		// Verify binary equality of ALL journals.
		assertDigestsEquals(services);

		// Verify the HAStatus of each service.
		awaitHAStatus(expectedHAStatusArray, services);

		// Impose the desired LBS policy.
		setPolicy(newTestPolicy(), services);

		final JettyHttpClient client = new JettyHttpClient();
		client.start();
		
		// Repositories without the LBS.
		final JettyRemoteRepositoryManager[] repos = new JettyRemoteRepositoryManager[3];
		// Repositories with the LBS.
		final JettyRemoteRepositoryManager[] reposLBS = new JettyRemoteRepositoryManager[3];
		try {
			repos[0] = getRemoteRepository(serverA, client);
			repos[1] = getRemoteRepository(serverB, client);
			repos[2] = getRemoteRepository(serverC, client);

			reposLBS[0] = getRemoteRepository(serverA, true/* useLBS */, client);
			reposLBS[1] = getRemoteRepository(serverB, true/* useLBS */, client);
			reposLBS[2] = getRemoteRepository(serverC, true/* useLBS */, client);

			/*
			 * Verify read on each service.
			 */
			assertEquals(0L, getCountStar(serverA));
			assertEquals(0L, getCountStar(serverB));
			assertEquals(0L, getCountStar(serverC));

			/*
			 * Verify insert on A, read back on all.
			 */
			simpleInsertTransaction_noQuorumCheck(serverA, true/* useLoadBalancer */);

			assertEquals(2L, getCountStar(serverA));
			assertEquals(2L, getCountStar(serverB));
			assertEquals(2L, getCountStar(serverC));

			/*
			 * Verify delete on B, read back on all.
			 */
			simpleDeleteTransaction_noQuorumCheck(serverB, true/* useLoadBalancer */);

			assertEquals(0L, getCountStar(serverA));
			assertEquals(0L, getCountStar(serverB));
			assertEquals(0L, getCountStar(serverC));
		} finally {
			for (JettyRemoteRepositoryManager r : repos) {
				if (r != null)
					r.close();
			}
			for (JettyRemoteRepositoryManager r : reposLBS) {
				if (r != null)
					r.close();
			}
			
			client.stop();
		}

	}

    protected void simpleInsertTransaction_noQuorumCheck(final HAGlue haGlue,
            final boolean useLoadBalancer) throws IOException, Exception {

        final StringBuilder sb = new StringBuilder();
        sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
        sb.append("INSERT DATA {\n");
        sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
        sb.append("  dc:creator \"A.N.Other\" .\n");
        sb.append("}\n");

        final String updateStr = sb.toString();

        final JettyHttpClient client = new JettyHttpClient();
        client.start();
        
        final JettyRemoteRepositoryManager repo = getRemoteRepository(haGlue,
                useLoadBalancer, client);
        try {
        	repo.prepareUpdate(updateStr).evaluate();
        } finally {
        	repo.close();
        	client.stop();
        }

    }

    protected void simpleDeleteTransaction_noQuorumCheck(final HAGlue haGlue,
			final boolean useLoadBalancer) throws IOException, Exception {

		final StringBuilder sb = new StringBuilder();
		sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
		sb.append("DELETE DATA {\n");
		sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
		sb.append("  dc:creator \"A.N.Other\" .\n");
		sb.append("}\n");

		final String updateStr = sb.toString();

        final JettyHttpClient client = new JettyHttpClient();
        client.start();
        
		final JettyRemoteRepositoryManager repo = getRemoteRepository(haGlue,
				useLoadBalancer, client);
		try {
			repo.prepareUpdate(updateStr).evaluate();
		} finally {
			repo.close();
			client.stop();
		}

	}

}
