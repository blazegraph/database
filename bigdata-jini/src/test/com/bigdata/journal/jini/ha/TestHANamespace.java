/**

Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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

import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Test case for concurrent list namespace and create namespace operations.
 * <p>
 * Note: The underlying issue is NOT HA specific. This test SHOULD be ported
 * to the standard NSS test suite.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/867"> NSS concurrency
 *      problem with list namespaces and create namespace </a>
 */
public class TestHANamespace extends AbstractHA3JournalServerTestCase {

	public TestHANamespace() {
	}

	public TestHANamespace(String name) {
		super(name);
	}

    /**
     * Test case for concurrent list namespace and create namespace operations.
     * <p>
     * Note: The underlying issue is NOT HA specific. This test SHOULD be ported
     * to the standard NSS test suite.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/867"> NSS concurrency
     *      problem with list namespaces and create namespace </a>
     */
	public void test_ticket_867() throws Throwable {

        /*
         * Controls the #of create/drop namespace operations. This many permits
         * are obtained, and a permit is released each time we do a create
         * namespace or drop namespace operation.
         */
        final int NPERMITS = 50;

        /*
         * Controls the #of queries that are executed in the main thread
         * concurrent with those create/drop namespace operations.
         */
		final int NQUERIES = 10; 
		
        final String NAMESPACE_PREFIX = getName() + "-";

		final ABC abc = new ABC(false/* simultaneous */);

		// Await quorum meet.
		final long token = quorum.awaitQuorum(awaitQuorumTimeout,
				TimeUnit.MILLISECONDS);

		// Figure out which service is the leader.
		final HAGlue leader = quorum.getClient().getLeader(token);

		// Wait until up and running as the leader.
		awaitHAStatus(leader, HAStatusEnum.Leader);
		
        final RemoteRepositoryManager repositoryManager = getRemoteRepositoryManager(
                leader, false/* useLBS */);

		final Semaphore awaitDone = new Semaphore(0);
		
		final AtomicReference<Exception> failure = new AtomicReference<Exception>(null);

		try {

			final Thread getNamespacesThread = new Thread(new Runnable() {

				@Override
				public void run() {

					try {

                        /*
                         * Create-delete namespaces with incrementing number in
                         * name.
                         */
						int n = 0;
						while (true) {

							final String namespace = NAMESPACE_PREFIX + n;

							final Properties props = new Properties();

							props.put(BigdataSail.Options.NAMESPACE,
									namespace);

                            if (log.isInfoEnabled())
                                log.info("Creating namespace " + namespace);

							repositoryManager
									.createRepository(namespace, props);

                            awaitDone.release(); // release a permit.

                            if (n % 2 == 0) {

                                if (log.isInfoEnabled())
                                    log.info("Removing namespace " + namespace);

                                repositoryManager.deleteRepository(namespace);
                                
                            }
							
							n++;

						}

					} catch (Exception e) {
						failure.set(e);
					} finally {
					    // release all permits.
						awaitDone.release(NPERMITS);
					}

				}

			});

			// Start running the create/drop namespace thread.
			getNamespacesThread.start();

			try {
                /*
                 * Run list namespace requests concurrent with the create/drop
                 * namespace requests.
                 * 
                 * FIXME Martyn: The list namespace requests should be running
                 * fully asynchronously with respect to the create/drop
                 * namespace requests, not getting a new set of permits and then
                 * just running the list namespace once for those NPERMITS
                 * create/drop requests. The way this is setup is missing too
                 * many opportunities for a concurrency issue with only one list
                 * namespace request per 50 create/drop requests.
                 */
				for (int n = 0; n < NQUERIES; n++) {
					awaitDone.acquire(NPERMITS);
					
					if (failure.get() != null)
						fail("Thread failure", failure.get());
		
		            if (log.isInfoEnabled())
		                log.info("Get namespace list...");
		
					try {
		
						final GraphQueryResult gqres = repositoryManager
								.getRepositoryDescriptions();
						int count = 0;
						while (gqres.hasNext()) {
						    final Statement st = gqres.next();
		                    if (log.isInfoEnabled())
		                        log.info("Statement: " + st);
							count++;
						}
						log.warn("Processed " + count + " statements");
						assertTrue(count > 0);
					} catch (Exception e) {
						fail("Unable to retrieve namespaces", e);
		
					}
				}
			} finally {				
				getNamespacesThread.interrupt();				
			}

		} finally {

			repositoryManager.close();

		}

	}
}
