/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.rdf.sail.webapp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.QueryEvaluationException;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.spo.NoAxiomFilter;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.util.DaemonThreadFactory;

import junit.framework.Test;

/**
 * Proxied test suite providing a stress test of the multi-tenancy API.
 * 
 * @param <S>
 * 
 * @see {@link StressTestConcurrentRestApiRequests} which provides full coverage of
 *      the REST API within a parameterized workload.
 */
public class StressTest_REST_MultiTenancy<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

	public StressTest_REST_MultiTenancy() {

	}

	public StressTest_REST_MultiTenancy(final String name) {

		super(name);

	}

	public static Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(StressTest_REST_MultiTenancy.class,
                "test.*", 
                Collections.singleton(BufferMode.DiskRW),
                TestMode.quads
//                , TestMode.sids
//                , TestMode.triples
                );
       
	}
	
   /**
    * This is based on a customer stress test that was able to reliably
    * replicate the issue with the apache http components library. The test
    * combines concurrent processes that
    * <ul>
    * <li>create a namespace</li>
    * <li>load data into a namespace</li>
    * <li>list the namespaces</li>
    * <li>run SPARQL against a namespace</li>
    * </ul>
    * 
    * Note: This test is intended to run for an hour or more to identify a
    * problem. It is run for a shorter period in CI.
    * 
    * @see <a href="http://trac.bigdata.com/ticket/967"> Replace Apache Http
    *      Components with jetty http client (was Roll forward the apache http
    *      components dependency to 4.3.3) </a>
    * 
    * @throws Exception
    */
	public void test_multiTenancy_967() throws Exception {

//	   doMultiTenancyStressTest(TimeUnit.SECONDS.toMillis(20));
	   
	}

   /**
     * We are seeing a problem where multiple namespaces are used and an aborted
     * operation on one namespace causes problems with another.
     * 
     * @throws Exception
     * 
     * @see BLZG-2023
     */
    public void test_multiTenancy_2023() throws Exception {
        final String ns1 = "namespace1";
        final String ns2 = "namespace2";
        final String ns3 = "namespace3";

        // Create 2 namespaces
        createNamespace(ns1);
        createNamespace(ns2);
        createNamespace(ns3);

        // Load them up
        loadStatements(ns1, 10000);
        loadStatements(ns2, 10000);
        loadStatements(ns3, 10000);

        // Run simple queries
        simpleQuery(ns1);
        simpleQuery(ns2);
        simpleQuery(ns3);

        // Update first with abort
        try {
            forceAbort(ns1);
        } catch (Throwable t) {
            // ignore
            t.printStackTrace();
        }

        // Update second
        loadStatements(ns2, 1000);

        // Update second with abort
        try {
            forceAbort(ns2);
        } catch (Throwable t) {
            // ignore
            t.printStackTrace();
        }

        // Drop Graph
        dropGraph(ns2);
        dropGraph(ns1);

        // Re-run simple queries
        simpleQuery(ns1);
        simpleQuery(ns2);
        simpleQuery(ns3);

        // Update second
        loadStatements(ns2, 1000);
    }

    private void createNamespace(final String namespace) throws Exception {
//        final Properties properties = new Properties();
//        final Properties properties = getTestMode().getProperties(); // FIXME BLZG-2023: Use the indicated test mode, but also test for triplesPlusTM.
        final Properties properties = TestMode.triplesPlusTruthMaintenance.getProperties();
        properties.put(BigdataSail.Options.NAMESPACE, namespace);
        log.warn(String.format("Create namespace %s...", namespace));
        m_mgr.createRepository(namespace, properties);
        log.warn(String.format("Create namespace %s done", namespace));
    }

    private void loadStatements(final String namespace, final int nstatements) throws Exception {
        final Collection<Statement> stmts = new ArrayList<>(nstatements);
        for (int i = 0; i < nstatements; i++) {
            stmts.add(generateTriple());
        }
        log.warn(String.format("Loading package into %s namespace...", namespace));
        m_mgr.getRepositoryForNamespace(namespace).add(new RemoteRepository.AddOp(stmts));
        log.warn(String.format("Loading package into %s namespace done", namespace));
    }

    private void forceAbort(final String namespace) throws Exception {
        final RemoteRepository rr = m_mgr.getRepositoryForNamespace(namespace);

        // force an abort by preparing an invalid update
        rr.prepareUpdate("FORCE ABORT").evaluate();
    }

    private void dropGraph(final String namespace) throws Exception {
        final RemoteRepository rr = m_mgr.getRepositoryForNamespace(namespace);

        // force an abort by preparing an invalid update
        rr.prepareUpdate("DROP GRAPH <" + namespace + ">").evaluate();
    }

    private void simpleQuery(final String namespace) throws QueryEvaluationException, Exception {
        log.warn(String.format("Execute SPARQL on %s namespace...", namespace));
        m_mgr.getRepositoryForNamespace(namespace).prepareTupleQuery("SELECT * {?s ?p ?o} LIMIT 100").evaluate()
                .close();
        log.warn(String.format("Execute SPARQL on %s namespace done", namespace));
    }

    /**
    * Runs the stress test for an hour. This is the minimum required to have
    * confidence that the problem is not demonstrated. Multiple hour runs are
    * better.
    * 
    * @throws Exception
    * 
     * @see {@link StressTestConcurrentRestApiRequests} which provides full
     *      coverage of the REST API within a parameterized workload.
    */
   public void stressTest_multiTenancy_967() throws Exception {

      doMultiTenancyStressTest(TimeUnit.HOURS.toMillis(1));
      
   }

   /**
    * Note: I have reduced the intervals between operations by 1/2 (10s => 5s;
    * 2s => 1s). This makes it possible to get something interesting done in a
    * shorter run (15s or so).
    * 
    * @param timeoutMillis
    *           The test duration. This needs to be at least 15 seconds to allow
    *           multiple namespaces to be created and some queries submitted.
    * 
    * @throws Exception
    */
   private void doMultiTenancyStressTest(final long timeoutMillis)
         throws Exception {

      final AtomicInteger namespaceCount = new AtomicInteger(0);
      final CountDownLatch latch = new CountDownLatch(2);
      final AtomicBoolean testSucceeding = new AtomicBoolean(true);

      final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
            20/* corePoolSize */, DaemonThreadFactory.defaultThreadFactory());

      try {

         // 1. Create namespace
         executor.submit(new Runnable() {
            @Override
            public void run() {
               final String namespace = "n" + namespaceCount.getAndIncrement();
               final Properties properties = new Properties();
               properties.put("com.bigdata.rdf.sail.namespace", namespace);
               try {
                  log.warn(String.format("Create namespace %s...", namespace));
                  m_mgr.createRepository(namespace, properties);
                  log.warn(String.format("Create namespace %s done", namespace));
                  latch.countDown();
               } catch (final Exception e) {
                  log.error(String.format("Failed to create namespace %s:",
                        namespace), e);
                  testSucceeding.set(false);
               }
               if (testSucceeding.get())
                  executor.schedule(this, 5, TimeUnit.SECONDS);
            }
         });

         // 2. Data load
         executor.submit(new Runnable() {
            @Override
            public void run() {
               String namespace = null;
               try {
                  latch.await(); // Wait at least 2 created namespaces
                  namespace = "n"
                        + ThreadLocalRandom.current().nextInt(
                              namespaceCount.get() - 1);
                  final Collection<Statement> stmts = new ArrayList<>(100000);
                  for (int i = 0; i < 100000; i++) {
                     stmts.add(generateTriple());
                  }
                  log.warn(String.format(
                        "Loading package into %s namespace...", namespace));
                  m_mgr.getRepositoryForNamespace(namespace).add(
                        new RemoteRepository.AddOp(stmts));
                  log.warn(String.format(
                        "Loading package into %s namespace done", namespace));
               } catch (final Exception e) {
                  log.error(
                        String.format(
                              "Failed to load package into namespace %s:",
                              namespace), e);
                  testSucceeding.set(false);
               }
               if (testSucceeding.get())
                  executor.schedule(this, 5, TimeUnit.SECONDS);
            }
         });

         // 3. Get namespace list
         executor.submit(new Runnable() {
            @Override
            public void run() {
               try {
                  log.warn("Get namespace list...");
                  m_mgr.getRepositoryDescriptions().close();
                  log.warn("Get namespace list done");
               } catch (final Exception e) {
                  log.error("Failed to get namespace list:", e);
                  testSucceeding.set(false);
               }
               if (testSucceeding.get())
                  executor.schedule(this, 1, TimeUnit.SECONDS);
            }
         });

         // 4. Execute SPARQL
         executor.submit(new Runnable() {
            @Override
            public void run() {
               String namespace = null;
               try {
                  latch.await(); // Wait at least 2 created namespaces
                  namespace = "n"
                        + ThreadLocalRandom.current().nextInt(
                              namespaceCount.get() - 1);
                  log.warn(String.format("Execute SPARQL on %s namespace...",
                        namespace));
                  m_mgr.getRepositoryForNamespace(namespace)
                        .prepareTupleQuery("SELECT * {?s ?p ?o} LIMIT 100")
                        .evaluate().close();
                  log.warn(String.format("Execute SPARQL on %s namespace done",
                        namespace));
               } catch (final Exception e) {
                  log.error(
                        String.format(
                              "Failed to execute SPARQL on %s namespace:",
                              namespace), e);
                  testSucceeding.set(false);
               }
               if (testSucceeding.get())
                  executor.schedule(this, 1, TimeUnit.SECONDS);
            }
         });

         // count down the seconds.
         final long beginNanos = System.nanoTime();
         final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
         while ((System.nanoTime() - beginNanos/* elapsed */) < timeoutNanos
               && testSucceeding.get()) {
            Thread.sleep(1000/* ms */); // Wait a while
         }

         log.warn("Stopping...");

      } finally {
         executor.shutdownNow();
         executor.awaitTermination(5, TimeUnit.MINUTES);
      }

      log.info("Cleanup namespaces...");
      for (int i = 0; i < namespaceCount.get(); i++) {
         m_mgr.deleteRepository("n" + i);
      }

   }

   private static final String URI_PREFIX = "http://bigdata.test/";

   private static Statement generateTriple() {

      return new StatementImpl(new URIImpl(URI_PREFIX + UUID.randomUUID()),
            new URIImpl(URI_PREFIX + UUID.randomUUID()), new URIImpl(URI_PREFIX
                  + UUID.randomUUID()));

   }

}
