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

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Test suite for HA with concurrent writers and group commit.
 * <p>
 * Note: A different code path is used for commit for HA1 than HA3 (no call to
 * postCommit() or postHACommit(). Thus some kinds of errors will only be
 * observable in HA3. See #1136.
 * 
 * TODO Do concurrent writer use cases for concurrent writers that eventually
 * cause leader or follower fails to make sure that error recovery is Ok with
 * concurrent writers (we should be testing this in our release QA for HA as
 * well).
 * 
 * @author bryan
 */
public class AbstractHAGroupCommitTestCase extends AbstractHA3JournalServerTestCase {

   public AbstractHAGroupCommitTestCase() {
   }

   public AbstractHAGroupCommitTestCase(String name) {
      super(name);
   }

   /**
    * Create 2 namespaces and run concurrent writers that LOAD data into each
    * namespace.
    * 
    * @throws Exception
    */
   protected void doGroupCommit_2Namespaces_ConcurrentWriters(
         final boolean reallyLargeLoad) throws Exception {

      final String namespace1 = getName() + "-1";

      final String namespace2 = getName() + "-2";

      final Properties properties1 = new Properties();
      {
         properties1.setProperty(RemoteRepository.OPTION_CREATE_KB_NAMESPACE,
               namespace1);
      }
      final Properties properties2 = new Properties();
      {
         properties2.setProperty(RemoteRepository.OPTION_CREATE_KB_NAMESPACE,
               namespace2);
      }

      // Verify quorum is FULLY met.
      final long token = awaitFullyMetQuorum();

      final HAGlue leader = quorum.getClient().getLeader(token);

      final RemoteRepositoryManager repo = getRemoteRepository(leader, httpClient);

      try {

         // Create both repositories in a single thread.
         repo.createRepository(namespace1, properties1);
         repo.createRepository(namespace2, properties2);

         // Load data into both repositories using parallel threads.

         final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

         tasks.add(new LargeLoadTask(token, reallyLargeLoad,
               false/* dropAll */, namespace1));

         tasks.add(new LargeLoadTask(token, reallyLargeLoad,
               false/* dropAll */, namespace2));

         final List<Future<Void>> futures = executorService.invokeAll(tasks,
               TimeUnit.MINUTES.toMillis(4), TimeUnit.MILLISECONDS);

         for (Future<Void> f : futures) {

            f.get();

         }

         // Count the #of statements in each repo.

         final long count1 = countResults(repo
               .getRepositoryForNamespace(namespace1)
               .prepareTupleQuery("select * {?a ?b ?c}").evaluate());

         final long count2 = countResults(repo
               .getRepositoryForNamespace(namespace2)
               .prepareTupleQuery("select * {?a ?b ?c}").evaluate());

         assertEquals(count1, count2);

         assertTrue(count1 > 0);

         // Verify still met on token.
         assertEquals(token, quorum.token());

         // Verify still fully met on token.
         assertTrue(quorum.isQuorumFullyMet(token));

         repo.deleteRepository(namespace1);
         repo.deleteRepository(namespace2);

         // Verify still met on token.
         assertEquals(token, quorum.token());

         // Verify still fully met on token.
         assertTrue(quorum.isQuorumFullyMet(token));

         final long commitCounter = leader
               .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
               .getRootBlock().getCommitCounter();
         
         if (log.isInfoEnabled())
            log.info("There were " + commitCounter + " commits.");

      } finally {

         repo.close();

      }

   }

   /**
    * Test helper creates N namespaces and then loads the data into those
    * namespaces in parallel using a "DROP ALL; LOAD" pattern.
    * 
    * 
    * @param nnamespaces
    *           The #of namespaces to use.
    * @param nruns
    *           The #of tasks to submit for each namespace.
    * @param reallyLargeLoad
    *           a larger data set is loaded when this is <code>true</code>.
    */
   protected void doManyNamespacesConcurrentWritersTest(final int nnamespaces,
        final int nruns, final boolean reallyLargeLoad) throws Exception {

     final String[] namespaces = new String[nnamespaces];
     {

        for (int i = 0; i < nnamespaces; i++) {

           namespaces[i] = getName() + "-" + i;

        }

     }

      // Verify quorum is FULLY met.
      final long token = awaitFullyMetQuorum();

      final HAGlue leader = quorum.getClient().getLeader(token);

      final RemoteRepositoryManager repo = getRemoteRepository(leader,
            httpClient);

      try {

         /*
          * Create each repository (synchronous). Each repo will be its own
          * commit point since this is in a single thread in the local code.
          */
         for (int i = 0; i < nnamespaces; i++) {

            final Properties properties = new Properties();

            properties.setProperty(RemoteRepository.OPTION_CREATE_KB_NAMESPACE,
                  namespaces[i]);

            repo.createRepository(namespaces[i], properties);

         }

         /*
          * Load data into all repositories using parallel threads. The loads
          * into distinct namespaces will be parallelized (up to the #of write
          * executor threads). The loads against any given repository will be
          * serialized. Since each load is run in its own client thread, it is
          * possible (but very unlikely) that two loads for the same namespace
          * could be melded into the same commit point.
          */

         final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

         for (int i = 0; i < nruns; i++) {

            for (int j = 0; j < nnamespaces; j++) {

               tasks.add(new LargeLoadTask(token, reallyLargeLoad,
                     true/* dropAll */, namespaces[j]));

            }

         }

         final List<Future<Void>> futures = executorService.invokeAll(tasks,
               TimeUnit.MINUTES.toMillis(4), TimeUnit.MILLISECONDS);

         for (Future<Void> f : futures) {

            f.get();

         }

         // Count the #of statements in each repo.

         final long count0 = countResults(repo
               .getRepositoryForNamespace(namespaces[0])
               .prepareTupleQuery("select * {?a ?b ?c}").evaluate());

         assertTrue(count0 > 0);

         for (int i = 1; i < nnamespaces; i++) {

            final long count2 = countResults(repo
                  .getRepositoryForNamespace(namespaces[i])
                  .prepareTupleQuery("select * {?a ?b ?c}").evaluate());

            assertEquals(count0, count2);

         }

         // Verify still met on token.
         assertEquals(token, quorum.token());

         // Verify still fully met on token.
         assertTrue(quorum.isQuorumFullyMet(token));

         final long commitCounter = leader
               .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
               .getRootBlock().getCommitCounter();
         
         if (log.isInfoEnabled())
            log.info("There were " + commitCounter + " commits with "
                  + nnamespaces + " namespaces and " + nruns + " runs.");

      } finally {

         repo.close();

      }

   }

   /**
    * Test helper creates N namespaces and then loads the data into those
    * namespaces in parallel using a "DROP ALL; LOAD" pattern and a small
    * payload for the updates. Due to the small payload, it is reasonable to
    * expect that some commit groups will be melded that have more than one
    * update for a given namespace.
    * 
    * @param nnamespaces
    *           The #of namespaces to use.
    * @param nruns
    *           The #of tasks to submit for each namespace.
    */
   protected void doManyNamespacesConcurrentWritersSmallUpdatesTest(
         final int nnamespaces, final int nruns) throws Exception {

      final String[] namespaces = new String[nnamespaces];
      {

         for (int i = 0; i < nnamespaces; i++) {

            namespaces[i] = getName() + "-" + i;

         }

      }

      // Verify quorum is FULLY met.
      final long token = awaitFullyMetQuorum();

      final HAGlue leader = quorum.getClient().getLeader(token);

      final RemoteRepositoryManager repo = getRemoteRepository(leader,
            httpClient);

      try {

         /*
          * Create each repository (synchronous). Each repo will be its own
          * commit point since this is in a single thread in the local code.
          */
         for (int i = 0; i < nnamespaces; i++) {

            final Properties properties = new Properties();

            properties.setProperty(RemoteRepository.OPTION_CREATE_KB_NAMESPACE,
                  namespaces[i]);

            repo.createRepository(namespaces[i], properties);

         }

         /*
          * Load data into all repositories using parallel threads. The loads
          * into distinct namespaces will be parallelized (up to the #of write
          * executor threads). The loads against any given repository will be
          * serialized. Since each load is run in its own client thread, it is
          * possible (but very unlikely) that two loads for the same namespace
          * could be melded into the same commit point.
          */

         final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

         for (int i = 0; i < nruns; i++) {

            for (int j = 0; j < nnamespaces; j++) {

               tasks.add(new SmallUpdateTask(token, namespaces[j]));

            }

         }

         final List<Future<Void>> futures = executorService.invokeAll(tasks,
               TimeUnit.MINUTES.toMillis(4), TimeUnit.MILLISECONDS);

         for (Future<Void> f : futures) {

            f.get();

         }

         // Count the #of statements in each repo.

         final long count0 = countResults(repo
               .getRepositoryForNamespace(namespaces[0])
               .prepareTupleQuery("select * {?a ?b ?c}").evaluate());

         assertTrue(count0 > 0);

         for (int i = 1; i < nnamespaces; i++) {

            final long count2 = countResults(repo
                  .getRepositoryForNamespace(namespaces[i])
                  .prepareTupleQuery("select * {?a ?b ?c}").evaluate());

            assertEquals(count0, count2);

         }

         // Verify still met on token.
         assertEquals(token, quorum.token());

         // Verify still fully met on token.
         assertTrue(quorum.isQuorumFullyMet(token));

         final long commitCounter = leader
               .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
               .getRootBlock().getCommitCounter();
         
         if (log.isInfoEnabled())
            log.info("There were " + commitCounter + " commits with "
                  + nnamespaces + " namespaces and " + nruns + " runs.");
         
      } finally {

         repo.close();

      }

   }

   /**
    * Task submits a small "DROP ALL; INSERT DATA" update request to the leader.
    * 
    * @author bryan
    */
   protected class SmallUpdateTask implements Callable<Void> {

      private final long token;
      private final String namespace;

      public SmallUpdateTask(final long token, final String namespace) {
         this.token = token;
         this.namespace = namespace;
      }

      @Override
      public Void call() throws Exception {

         final StringBuilder sb = new StringBuilder();
         sb.append("DROP ALL;\n");
         sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
         sb.append("INSERT DATA {\n");
         sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
         sb.append("  dc:creator \"A.N.Other\" .\n");
         sb.append("}\n");

         final String updateStr = sb.toString();
         // Verify quorum is still valid.
         quorum.assertQuorum(token);

         final long elapsed;
         final HAGlue leader = quorum.getClient().getLeader(token);
         final RemoteRepositoryManager mgr = getRemoteRepository(leader, httpClient);
         try {
            final RemoteRepository repo = mgr
                  .getRepositoryForNamespace(namespace);
            final long begin = System.nanoTime();
//            if (log.isInfoEnabled())
//               log.info("load begin: " + repo);
            repo.prepareUpdate(updateStr).evaluate();
            elapsed = System.nanoTime() - begin;
         } finally {
            mgr.close();
         }

         // Verify quorum is still valid.
         quorum.assertQuorum(token);

         if (log.isInfoEnabled())
            log.info("load done: " + this + ", elapsed="
                  + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");

         return null;

      }

   }

   /**
    * A unit test of concurrent create/destroy of namespaces.
    * <p>
    * Note: Namespace create/destroy tasks contend for the GSR index. This
    * limits the potential parallelism since at most one create/destroy task can
    * run at a time regardless of the namespace it addresses.  However, multiple
    * create and/or destroy operations can still be melded into a single commit
    * group.
    * 
    * @param nnamespaces
    *           The #of namespaces to use.
    * @param nruns
    *           The #of tasks to submit for each namespace.
    * 
    * @throws Exception
    * 
    * FIXME Chase down why this does not work!  I am hitting errors with the 
    */
   protected void doGroupCommit_createDestroy_ManyNamespacesTest(
         final int nnamespaces, final int nruns) throws Exception {
      
      final String[] namespaces = new String[nnamespaces];
      {

         for (int i = 0; i < nnamespaces; i++) {

            namespaces[i] = getName() + "-" + i;

         }

      }

      // Verify quorum is FULLY met.
      final long token = awaitFullyMetQuorum();

      final HAGlue leader = quorum.getClient().getLeader(token);

      final RemoteRepositoryManager repo = getRemoteRepository(leader,
            httpClient);

      try {

         final StringBuilder sb = new StringBuilder();
         sb.append("DROP ALL;\n");
         sb.append("PREFIX dc: <http://purl.org/dc/elements/1.1/>\n");
         sb.append("INSERT DATA {\n");
         sb.append("  <http://example/book1> dc:title \"A new book\" ;\n");
         sb.append("  dc:creator \"A.N.Other\" .\n");
         sb.append("}\n");
         final String updateStr = sb.toString();

         /*
          * Create a set of tasks organized as a series of iterations. For each
          * iteration, we will either create or destroy all namespaces.
          */

         final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();

         for (int i = 0; i < nruns; i++) {

//            final boolean create = (i % 2 == 0);

            for (int j = 0; j < nnamespaces; j++) {

               // FIXME Why is it a problem to create/destroy the same namespaces?!?
               final String theNamespace = namespaces[j];
               final String theNamespaceDisplay = theNamespace
                     +"::round="+i
                     ;

               tasks.add(new Callable<Void>() {

                  @Override
                  public Void call() throws Exception {

                     try {

//                        if (create) {

                           final Properties properties = new Properties();

                           properties.setProperty(
                                 RemoteRepository.OPTION_CREATE_KB_NAMESPACE,
                                 theNamespace);

                           log.warn("Creating: " + theNamespaceDisplay);
                           repo.createRepository(theNamespace, properties);

                           if(true) {
                              // send an update request.
                              repo.getRepositoryForNamespace(theNamespace)
                                 .prepareUpdate(updateStr).evaluate();
                           }
                           
//                        } else {

                           log.warn("Destroying: " + theNamespaceDisplay);
                           repo.deleteRepository(theNamespace);

//                        }
                        return null;
                     } catch (Throwable t) {
                        log.error("namespace=" + theNamespace + " :: " + t);
                        throw new RuntimeException(t);
                     }
                  }
               });

            }

         }

         final List<Future<Void>> futures = executorService.invokeAll(tasks,
               TimeUnit.MINUTES.toMillis(4), TimeUnit.MILLISECONDS);

         for (Future<Void> f : futures) {

            f.get();

         }

         // Count the #of statements in each repo.

         final long count0 = countResults(repo
               .getRepositoryForNamespace(namespaces[0])
               .prepareTupleQuery("select * {?a ?b ?c}").evaluate());

         assertTrue(count0 > 0);

         for (int i = 1; i < nnamespaces; i++) {

            final long count2 = countResults(repo
                  .getRepositoryForNamespace(namespaces[i])
                  .prepareTupleQuery("select * {?a ?b ?c}").evaluate());

            assertEquals(count0, count2);

         }

         // Verify still met on token.
         assertEquals(token, quorum.token());

         // Verify still fully met on token.
         assertTrue(quorum.isQuorumFullyMet(token));

         final long commitCounter = leader
               .getRootBlock(new HARootBlockRequest(null/* storeUUID */))
               .getRootBlock().getCommitCounter();
         
         if (log.isInfoEnabled())
            log.info("There were " + commitCounter + " commits with "
                  + nnamespaces + " namespaces and " + nruns + " runs.");
         
      } finally {

         repo.close();

      }

   }
   
}
