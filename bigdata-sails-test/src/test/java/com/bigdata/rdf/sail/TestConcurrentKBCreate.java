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
/*
 * Created on Sep 4, 2008
 */
package com.bigdata.rdf.sail;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.bigdata.BigdataStatics;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sail.webapp.DatasetNotFoundException;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.rdf.vocab.BSBMVocabulary;
import com.bigdata.rdf.vocab.RDFSVocabulary;
import com.bigdata.util.InnerCause;

/**
 * Test suite for the concurrent create and discovery of a KB instance.
 * <p>
 * Note: There is a data race when creating the a KB (especially the default KB)
 * and verifying that the KB exists. If we find the KB in the row store cache
 * but we do not find the axioms, then the subsequent attempts to resolve the KB
 * fail - probably due to an issue with the default resource locator cache. This
 * is observed in the HA test suite where we wait for a quorum meet and then
 * spin in a loop looking for the KB to be concurrently created by the NSS. This
 * test suite was written to replicate and diagnose this problem. A
 * representative stack trace is below. Once this trace is generated, it is
 * produced repeatedly. Presumably, a restart of the service would cure the
 * stack trace since it is a cache side effect.
 * 
 * <pre>
 * INFO : 41211 2012-11-06 08:38:41,874 : WARN : 8542 2012-11-06 08:38:41,873      qtp877533177-45 org.eclipse.jetty.util.log.Slf4jLog.warn(Slf4jLog.java:50): /sparql
 * INFO : 41211 2012-11-06 08:38:41,874 : java.lang.RuntimeException: java.lang.RuntimeException: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
 * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1120)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doGet(QueryServlet.java:178)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at com.bigdata.rdf.sail.webapp.RESTServlet.doGet(RESTServlet.java:175)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at javax.servlet.http.HttpServlet.service(HttpServlet.java:707)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at javax.servlet.http.HttpServlet.service(HttpServlet.java:820)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHolder.handle(ServletHolder.java:534)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHandler.doHandle(ServletHandler.java:475)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:929)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.servlet.ServletHandler.doScope(ServletHandler.java:403)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ContextHandler.doScope(ContextHandler.java:864)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:117)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.HandlerList.handle(HandlerList.java:47)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:114)
 * INFO : 41211 2012-11-06 08:38:41,874 :    at org.eclipse.jetty.server.Server.handle(Server.java:352)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection.handleRequest(HttpConnection.java:596)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection$RequestHandler.headerComplete(HttpConnection.java:1051)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.http.HttpParser.parseNext(HttpParser.java:590)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.http.HttpParser.parseAvailable(HttpParser.java:212)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.server.HttpConnection.handle(HttpConnection.java:426)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint.handle(SelectChannelEndPoint.java:508)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint.access$000(SelectChannelEndPoint.java:34)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.io.nio.SelectChannelEndPoint$1.run(SelectChannelEndPoint.java:40)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at org.eclipse.jetty.util.thread.QueuedThreadPool$2.run(QueuedThreadPool.java:451)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at java.lang.Thread.run(Thread.java:680)
 * INFO : 41212 2012-11-06 08:38:41,875 : Caused by: java.lang.RuntimeException: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
 * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1102)
 * INFO : 41212 2012-11-06 08:38:41,875 :    ... 23 more
 * INFO : 41212 2012-11-06 08:38:41,875 : Caused by: java.lang.RuntimeException: No axioms defined? : LocalTripleStore{timestamp=-1, namespace=kb, container=null, indexManager=com.bigdata.journal.jini.ha.HAJournal@4d092447}
 * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.store.AbstractTripleStore.getAxioms(AbstractTripleStore.java:1787)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.BigdataSail.<init>(BigdataSail.java:934)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.BigdataSail.<init>(BigdataSail.java:891)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.BigdataRDFContext.getQueryConnection(BigdataRDFContext.java:1858)
 * INFO : 41212 2012-11-06 08:38:41,875 :    at com.bigdata.rdf.sail.webapp.QueryServlet.doEstCard(QueryServlet.java:1074)
 * INFO : 41212 2012-11-06 08:38:41,875 :    ... 23 more
 * </pre>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/617">
 *      Concurrent KB create fails with "No axioms defined?" </a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class TestConcurrentKBCreate extends ProxyBigdataSailTestCase {

    public TestConcurrentKBCreate() {
    }

    public TestConcurrentKBCreate(String name) {
        super(name);
    }

    public static class TestWithGroupCommit extends TestConcurrentKBCreate {

       public TestWithGroupCommit() {
       }

       public TestWithGroupCommit(String name) {
           super(name);
       }

       @Override
       public Properties getProperties() {
          
          // Inherit the properties from the delegate.
          final Properties properties = new Properties(super.getProperties());
          
          properties.setProperty(Journal.Options.GROUP_COMMIT,"true");

          return properties;
          
       }

    }
    
    public static class TestWithoutGroupCommit extends TestConcurrentKBCreate {

       public TestWithoutGroupCommit() {
       }

       public TestWithoutGroupCommit(String name) {
           super(name);
       }

       @Override
       public Properties getProperties() {
          
          // Inherit the properties from the delegate.
          final Properties properties = new Properties(super.getProperties());
          
          properties.setProperty(Journal.Options.GROUP_COMMIT,"false");

          return properties;
          
       }
       
    }
    
    /**
     * A non-concurrent version, just to make sure that the basic logic works.
     */
    public void test_KBCreateAndDiscovery() throws Exception {
     
       final String namespace = getName();

        // Inherit the properties from the delegate.
        final Properties properties = getProperties();
        
        Journal jnl = null;
        try {

            // New Journal instance.
            jnl = new Journal(properties);
            
            // KB does not exist.
            assertKBNotFound(jnl, namespace);

            // Create the KB instance.
            AbstractApiTask.submitApiTask(jnl, new CreateKBTask(namespace, properties)).get();

            // Attempt to discover the KB instance.
            assertKBExists(jnl, namespace);

        } finally {

            if (jnl != null)
                jnl.destroy();

        }
        
    }
    
    /**
     * A non-concurrent version testing both create and destroy.
     */
    public void test_KBCreateAndDestroy() throws Exception {
     
       final String namespace = getName();

        // Inherit the properties from the delegate.
        final Properties properties = getProperties();
        
        Journal jnl = null;
        try {

            // New Journal instance.
            jnl = new Journal(properties);
            
            // KB does not exist.
            assertKBNotFound(jnl, namespace);

            // Create the KB instance.
            AbstractApiTask.submitApiTask(jnl, new CreateKBTask(namespace, properties)).get();

            // Attempt to discover the KB instance.
            assertKBExists(jnl, namespace);

            // Destroy the KB instance.
            AbstractApiTask.submitApiTask(jnl, new DestroyKBTask(namespace)).get();

            // Verify gone.
            assertKBNotFound(jnl, namespace);

        } finally {

            if (jnl != null)
                jnl.destroy();

        }
        
    }

    /**
     * Verify that the named kb exists.
     * 
     * @throws SailException
     */
   private void assertKBExists(final Journal jnl, final String namespace)
         throws RepositoryException, SailException {
      // Attempt to discover the KB instance.
      BigdataSailRepositoryConnection conn = null;
      try {
         // Request query connection.
         conn = getQueryConnection(jnl, namespace, ITx.READ_COMMITTED);
         // Verify KB exists.
         assertNotNull(namespace, conn);
      } finally {
         // Ensure connection is closed.
         if (conn != null)
            conn.close();
      }
   }

    /**
     * Verify that the named kb does not exist.
     * 
     * @throws SailException
     */
   private void assertKBNotFound(final Journal jnl, final String namespace)
         throws RepositoryException, SailException {
      // Attempt to discover the KB instance.
      BigdataSailRepositoryConnection conn = null;
      try {
         // Request query connection.
         conn = getQueryConnection(jnl, namespace, ITx.READ_COMMITTED);
         // Verify KB does not exist.
         assertNull(namespace, conn);
      } finally {
         // Ensure connection is closed.
         if (conn != null)
            conn.close();
      }
   }

    /**
     * A non-concurrent version with two KBs.
     */
   public void test_2KBCreateAndDiscovery() throws Exception {

      final String namespace = getName();
      final String namespace2 = getName()+"2";

      // Inherit the properties from the delegate.
      final Properties properties = getProperties();

      Journal jnl = null;
      try {

         // New Journal instance.
         jnl = new Journal(properties);

         // KB does not exist.
         assertKBNotFound(jnl, namespace);

         // Create the KB instance.
         AbstractApiTask.submitApiTask(jnl,
               new CreateKBTask(namespace, properties)).get();

         assertKBExists(jnl, namespace);
         
         // Create the KB instance.
         AbstractApiTask.submitApiTask(jnl,
               new CreateKBTask(namespace2, properties)).get();

         assertKBExists(jnl, namespace2);

      } finally {

         if (jnl != null)
            jnl.destroy();

      }
       
   }
   
   /**
    * A non-concurrent version testing both create, destroy, and then re-create
    * where the KB properties have NOT been changed.
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/948"> Create/Destroy of KB
    *      followed by Create with different Vocabulary causes runtime exception
    *      </a>
    */
   public void test_CreateDestroy_ticket_948_00() throws Exception {
    
       final String namespace = getName();

       // Inherit the properties from the delegate.
      final Properties properties1 = new Properties(getProperties());
      properties1.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
            BSBMVocabulary.class.getName());

       Journal jnl = null;
       try {

           // New Journal instance.
           jnl = new Journal(getProperties());
           
           // KB does not exist.
           assertKBNotFound(jnl, namespace);

           // Create the KB instance.
           AbstractApiTask.submitApiTask(jnl, new CreateKBTask(namespace, properties1)).get();

           // Attempt to discover the KB instance.
           assertKBExists(jnl, namespace);

           // Destroy the KB instance.
           AbstractApiTask.submitApiTask(jnl, new DestroyKBTask(namespace)).get();

           // Verify gone.
           assertKBNotFound(jnl, namespace);

           // Re-create the KB instance with the same properties.
           AbstractApiTask.submitApiTask(jnl, new CreateKBTask(namespace, properties1)).get();

           // Verify gone.
           assertKBExists(jnl, namespace);

       } finally {

           if (jnl != null)
               jnl.destroy();

       }
       
   }
   /**
    * A non-concurrent version testing both create, destroy, and then re-create
    * where the KB properties have been changed (especially the Vocabulary class).
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/948"> Create/Destroy of KB
    *      followed by Create with different Vocabulary causes runtime exception
    *      </a>
    */
   public void test_CreateDestroy_ticket_948_01() throws Exception {
    
      if(!BigdataStatics.runKnownBadTests) {
         return;
      }
      
      final String namespace = getName();

       // Inherit the properties from the delegate.
      final Properties properties1 = new Properties(getProperties());
      properties1.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
            BSBMVocabulary.class.getName());

      final Properties properties2 = new Properties(getProperties());
      properties2.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
            RDFSVocabulary.class.getName());

       Journal jnl = null;
       try {

           // New Journal instance.
           jnl = new Journal(getProperties());
           
           // KB does not exist.
           assertKBNotFound(jnl, namespace);

           // Create the KB instance.
           AbstractApiTask.submitApiTask(jnl, new CreateKBTask(namespace, properties1)).get();

           // Attempt to discover the KB instance.
           assertKBExists(jnl, namespace);

           // Destroy the KB instance.
           AbstractApiTask.submitApiTask(jnl, new DestroyKBTask(namespace)).get();

           // Verify gone.
           assertKBNotFound(jnl, namespace);

           // Re-create the KB instance with a different Vocabulary class.
           AbstractApiTask.submitApiTask(jnl, new CreateKBTask(namespace, properties2)).get();

           // Verify gone.
           assertKBExists(jnl, namespace);

       } finally {

           if (jnl != null)
               jnl.destroy();

       }
       
   }
   
   /**
    * A non-concurrent version testing both create, destroy, and then re-create
    * where the KB properties have been changed (especially the Vocabulary class).
    * <p>
    * In this variant, we shutdown the Journal and then re-open it.
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/948"> Create/Destroy of KB
    *      followed by Create with different Vocabulary causes runtime exception
    *      </a>
    */
   public void test_CreateDestroy_ticket_948_02() throws Exception {
    
      if(!BigdataStatics.runKnownBadTests) {
         return;
      }
      
      final String namespace = getName();

       // Inherit the properties from the delegate.
      final Properties properties1 = new Properties(getProperties());
      properties1.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
            BSBMVocabulary.class.getName());

      final Properties properties2 = new Properties(getProperties());
      properties2.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
            RDFSVocabulary.class.getName());

       Journal jnl = null;
       try {

           // New Journal instance.
           jnl = new Journal(getProperties());
           
           // KB does not exist.
           assertKBNotFound(jnl, namespace);

           // Create the KB instance.
           AbstractApiTask.submitApiTask(jnl, new CreateKBTask(namespace, properties1)).get();

           // Attempt to discover the KB instance.
           assertKBExists(jnl, namespace);

           // Destroy the KB instance.
           AbstractApiTask.submitApiTask(jnl, new DestroyKBTask(namespace)).get();

           // Verify gone.
           assertKBNotFound(jnl, namespace);
           
           // Shutdown and reopen the journal.
           jnl.shutdown();
           
           jnl = new Journal(getProperties());
           

           // Re-create the KB instance with a different Vocabulary class.
           AbstractApiTask.submitApiTask(jnl, new CreateKBTask(namespace, properties2)).get();

           // Verify gone.
           assertKBExists(jnl, namespace);

       } finally {

           if (jnl != null)
               jnl.destroy();

       }
       
   }
   
   /**
     * Basic test of the concurrent create and discovery of a KB.
     */
    public void test_concurrentKBCreateAndDiscovery() throws Exception {

       final String namespace = getName();

        // Inherit the properties from the delegate.
        final Properties properties = getProperties();
        
        Journal jnl = null;
        try {

            // New Journal instance.
            jnl = new Journal(properties);
            
            doConcurrentCreateAndDiscoveryTest(jnl, namespace);

        } finally {

            if (jnl != null)
                jnl.destroy();

        }

    }

    /**
     * Runs the concurrent KB Create and Discovery test multiple times against a
     * single journal, but using a distinct KB namespace for each test.
     * <p>
     * Note: This does not attempt to create the distinct KBs concurrently.
     */
    public void test_concurrentKBCreateAndDiscoveryStressTestOnSharedJournal()
            throws Exception {

        // Inherit the properties from the delegate.
        final Properties properties = getProperties();

        Journal jnl = null;
        try {

            // New Journal instance.
            jnl = new Journal(properties);

            for (int i = 0; i < 100; i++) {

                final String namespace = getName() + "-" + i;

                doConcurrentCreateAndDiscoveryTest(jnl, namespace);

            }

        } finally {

            if (jnl != null)
                jnl.destroy();

        }

    }

    /**
     * Runs {@link #test_concurrentKBCreateAndDiscovery()} multiple times. Each
     * time that test runs, it uses a distinct {@link Journal} instance.
     */
    public void test_concurrentKBCreateAndDiscoveryStressTestOnDistinctJournals()
            throws Exception {

        for (int i = 0; i < 100; i++) {

            test_concurrentKBCreateAndDiscovery();
            
        }

    }

    /**
     * Run several concurrent {@link DiscoveryTask}s and one
     * {@link CreateKBTask}.
     */
    private void doConcurrentCreateAndDiscoveryTest(
            final IIndexManager indexManager, final String namespace)
            throws Exception {

        // Set iff the create succeeds.
        final AtomicBoolean created = new AtomicBoolean(false);

        // Set iff the discovery succeeds.
        final AtomicBoolean discovered = new AtomicBoolean(false);

        // The #of times discovery failed with a thrown exception
        final AtomicInteger errorCount = new AtomicInteger(0);

        // The tasks to be executed.
        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

        /*
         * The #of discovery tasks to schedule before/after the CreateKB task.
         * 
         * Note: the actual order of concurrent evaluation is arbitrary - it is
         * NOT strongly constrained by this.
         */
        final int numBefore = 10, numAfter = 10;

        for (int i = 0; i < numBefore; i++) {
 
            tasks.add(new DiscoveryTask(indexManager, namespace, discovered,
                    errorCount));
        
        }
        
        tasks.add(new Callable<Void>() {

           @Override
            public Void call() throws Exception {

                try {

                  AbstractApiTask.submitApiTask(
                        indexManager,
                        new CreateKBTask(namespace, ((Journal) indexManager)
                              .getProperties())).get();

                    created.set(true);

                    return null; // Done.

                } catch (Throwable t) {

                    log.error(t, t);

                    throw new RuntimeException(t);

                }

            }

        });

        for (int i = 0; i < numAfter; i++) {
            
            tasks.add(new DiscoveryTask(indexManager, namespace, discovered,
                    errorCount));
        
        }

        // Basis towards more concurrency in startup.
        ((ThreadPoolExecutor) indexManager.getExecutorService())
                .setCorePoolSize(tasks.size());
        ((ThreadPoolExecutor) indexManager.getExecutorService())
                .prestartAllCoreThreads();

        // Invoke all tasks in parallel.
        final List<Future<Void>> futures = indexManager.getExecutorService()
                .invokeAll(tasks);

        // Verify no thrown exceptions.
        for (Future<Void> f : futures) {

            f.get();

        }

        // Verify KB was created.
        assertTrue("KB was not created.", created.get());

        if (!discovered.get()) {

            // Try one last time.
            new DiscoveryTask(indexManager, namespace, discovered, errorCount).call();
            
        }

        // Verify KB was discovered.
        assertTrue("KB was not discovered.", created.get());

        assertEquals("Error count", 0, errorCount.get());

    }

    private class DiscoveryTask implements Callable<Void> {

        private final IIndexManager indexManager;
        private final String namespace;
        private final AtomicBoolean discovered;
        private final AtomicInteger errorCount;

        public DiscoveryTask(final IIndexManager indexManager,
                final String namespace, final AtomicBoolean discovered,
                final AtomicInteger errorCount) {

            this.indexManager = indexManager;
            this.namespace = namespace;
            this.discovered = discovered;
            this.errorCount = errorCount;
            
        }

        @Override
        public Void call() throws Exception {

            BigdataSailRepositoryConnection conn = null;

            try {
                
                conn = getQueryConnection(indexManager, namespace,
                        ITx.READ_COMMITTED);

                if (conn != null) {

                    discovered.set(true);

                }
                
            } catch(Throwable t) {

                errorCount.incrementAndGet();

                log.error(t);
                // Added delegate to trace to see which proxy version of this test fails in CI.
                throw new RuntimeException("delegate="+getOurDelegate()+", t=" + t, t);
                
            } finally {
                
                if (conn != null) {
                
                    conn.close();
                    
                }
                
            }
            
            return null;

        }

    }

    /**
     * Return a connection transaction. When the timestamp is associated with a
     * historical commit point, this will be a read-only connection. When it is
     * associated with the {@link ITx#UNISOLATED} view or a read-write
     * transaction, this will be a mutable connection.
     * 
     * @param namespace
     *            The namespace.
     * @param timestamp
     *            The timestamp.
     * 
     * @return The connection -or- <code>null</code> if the triple store
     *         instance was not found.
     * @throws SailException 
     */
    private BigdataSailRepositoryConnection getQueryConnection(
            final IIndexManager indexManager, final String namespace,
            final long timestamp) throws RepositoryException, SailException {

//        boolean ok = false;
//        final BigdataSail sail = new BigdataSail(namespace, indexManager);
//        try {
//            final BigdataSailRepository repo = new BigdataSailRepository(sail);
//            repo.initialize();
//            try {
//                final BigdataSailRepositoryConnection con;
//                if (TimestampUtility.isReadOnly(timestamp)) {
//                    con = repo.getReadOnlyConnection(timestamp);
//                } else {
//                    con = repo.getConnection();
//                }
//                con.setAutoCommit(false);
//                ok = true;
//                return con;
//            } catch (Throwable t) {
//                if(InnerCause.isInnerCause(t, DatasetNotFoundException.class)) {
//                    return null;
//                }
//                throw new RuntimeException(t);
//            }
//        } finally {
//            if(!ok)
//                sail.shutDown();
//        }
        
        /*
         * Note: [timestamp] will be a read-only tx view of the triple store if
         * a READ_LOCK was specified when the NanoSparqlServer was started
         * (unless the query explicitly overrides the timestamp of the view on
         * which it will operate).
         */
        final AbstractTripleStore tripleStore = getTripleStore(indexManager,
                namespace, timestamp);

        if (tripleStore == null) {

            return null;

      }

      try {

         // Wrap with SAIL.
         final BigdataSail sail = new BigdataSail(tripleStore);

         final BigdataSailRepository repo = new BigdataSailRepository(sail);

         repo.initialize();

         if (TimestampUtility.isReadOnly(timestamp)) {

            return (BigdataSailRepositoryConnection) repo
                  .getReadOnlyConnection(timestamp);

         }

         // Read-write connection.
         final BigdataSailRepositoryConnection conn = repo.getConnection();

         conn.setAutoCommit(false);

         return conn;
      } catch (Throwable t) {
         if (InnerCause.isInnerCause(t, DatasetNotFoundException.class)) {
            // Not found.
            return null;
         }
         throw new RuntimeException(t);
      }

   }

    /**
     * Return a read-only view of the {@link AbstractTripleStore} for the given
     * namespace will read from the commit point associated with the given
     * timestamp.
     * 
     * @param namespace
     *            The namespace.
     * @param timestamp
     *            The timestamp.
     * 
     * @return The {@link AbstractTripleStore} -or- <code>null</code> if none is
     *         found for that namespace and timestamp.
     */
    private AbstractTripleStore getTripleStore(
            final IIndexManager indexManager, final String namespace,
            final long timestamp) {

        // resolve the default namespace.
        final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
                .getResourceLocator().locate(namespace, timestamp);

        return tripleStore;
        
    }

//    /**
//     * Return an UNISOLATED connection.
//     * 
//     * @param namespace
//     *            The namespace.
//     * 
//     * @return The UNISOLATED connection.
//     * 
//     * @throws SailException
//     * 
//     * @throws RepositoryException
//     */
//    private BigdataSailRepositoryConnection getUnisolatedConnection(
//            final IIndexManager indexManager, final String namespace)
//            throws SailException, RepositoryException {
//
//        // resolve the default namespace.
//        final AbstractTripleStore tripleStore = (AbstractTripleStore) indexManager
//                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
//
//        if (tripleStore == null) {
//
//            throw new RuntimeException("Not found: namespace=" + namespace);
//
//        }
//
//        // Wrap with SAIL.
//        final BigdataSail sail = new BigdataSail(tripleStore);
//
//        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//
//        repo.initialize();
//
//        final BigdataSailRepositoryConnection conn = (BigdataSailRepositoryConnection) repo
//                .getUnisolatedConnection();
//
//        conn.setAutoCommit(false);
//
//        return conn;
//
//    }

}
