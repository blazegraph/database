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
package com.bigdata.journal.jini.ha;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.client.HttpClient;

import com.bigdata.ha.HAGlue;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

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

      final HttpClient client = HttpClientConfigurator.getInstance()
            .newInstance();
      try {

         // Verify quorum is FULLY met.
         final long token = awaitFullyMetQuorum();

         final HAGlue leader = quorum.getClient().getLeader(token);

         final RemoteRepositoryManager repo = getRemoteRepository(leader,
               client);

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

         } finally {

            repo.close();

         }

      } finally {

         client.stop();

      }

   }

   /**
   * Test helper creates N namespaces and then loads the data into those
   * namespaces in parallel using a "DROP ALL; LOAD" pattern.
   * 
   * @param nnamespaces
   * @param nruns
   * @param reallyLargeLoad
   * @throws Exception
   */
   protected void doManyNamespacesConcurrentWritersTest(final int nnamespaces,
        final int nruns, final boolean reallyLargeLoad) throws Exception {

     final String[] namespaces = new String[nnamespaces];
     {

        for (int i = 0; i < nnamespaces; i++) {

           namespaces[i] = getName() + "-" + i;

        }

     }

     final HttpClient client = HttpClientConfigurator.getInstance()
           .newInstance();

     try {

        // Verify quorum is FULLY met.
        final long token = awaitFullyMetQuorum();

        final HAGlue leader = quorum.getClient().getLeader(token);
        
        final RemoteRepositoryManager repo = getRemoteRepository(leader,
              client);

        try {

           /*
            * Create each repository (synchronous). Each repo will be its own
            * commit point since this is in a single thread in the local code.
            */
           for (int i = 0; i < nnamespaces; i++) {

              final Properties properties = new Properties();

              properties
                    .setProperty(RemoteRepository.OPTION_CREATE_KB_NAMESPACE,
                          namespaces[i]);

              repo.createRepository(namespaces[i], properties);

           }

           /*
            * Load data into all repositories using parallel threads. The loads
            * into distinct namespaces will be parallelized (up to the #of
            * write executor threads). The loads against any given repository
            * will be serialized. Since each load is run in its own client
            * thread, it is possible (but very unlikely) that two loads for the
            * same namespace could be melded into the same commit point.
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
           
        } finally {

           repo.close();
           
        }

     } finally {
        
        client.stop();

     }

  }

}
