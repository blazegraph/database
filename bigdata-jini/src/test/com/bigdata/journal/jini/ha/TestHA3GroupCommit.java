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

import net.jini.config.Configuration;

import org.eclipse.jetty.client.HttpClient;

import com.bigdata.ha.HAGlue;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Test suite for HA3 with concurrent writers.
 * <p>
 * Note: A different code path is used for commit for HA1 than HA3 (no call to
 * postCommit() or postHACommit(). Thus some kinds of errors will only be
 * observable in HA3. See #1136.
 * 
 * TODO Do concurrent create / drop KB stress test with small loads in each KB.
 * 
 * TODO Do sequence of DROP ALL; LOAD concurent operations on multiple KBs.
 * 
 * TODO Do concurrent writers on the same KB. The operations should be
 * serialized.
 * 
 * TODO Do concurrent writer use cases for concurrent writers that eventually
 * cause leader or follower fails to make sure that error recovery is Ok with
 * concurrent writers.
 * 
 * @see TestHA1GroupCommit
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestHA3GroupCommit extends AbstractHA3JournalServerTestCase {

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
//            "com.bigdata.journal.HAJournal.properties=" +TestHA3JournalServer.getTestHAJournalProperties(com.bigdata.journal.HAJournal.properties),
               "com.bigdata.journal.jini.ha.HAJournalServer.restorePolicy=new com.bigdata.journal.jini.ha.DefaultRestorePolicy(0L,1,0)",
               "com.bigdata.journal.jini.ha.HAJournalServer.snapshotPolicy=new com.bigdata.journal.jini.ha.NoSnapshotPolicy()",
//               "com.bigdata.journal.jini.ha.HAJournalServer.HAJournalClass=\""+HAJournalTest.class.getName()+"\"",
               "com.bigdata.journal.jini.ha.HAJournalServer.onlineDisasterRecovery=true",
       };
       
   }
    
    public TestHA3GroupCommit() {
    }

    public TestHA3GroupCommit(String name) {
        super(name);
    }

    /**
     * Create 2 namespaces and then load data into those namespaces in parallel.
     * 
     * @throws Exception
     */
    public void test_HA3_groupCommit_create2Namespaces_concurrentLoad() throws Exception {

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

         new ABC(false/*sequential*/); // simultaneous start.

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

            tasks.add(new LargeLoadTask(token, false/* reallyLargeLoad */,
                  false/* dropAll */, namespace1));

            tasks.add(new LargeLoadTask(token, false/* reallyLargeLoad */,
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
     * Create 2 namespaces and then load data into those namespaces in parallel.
     * <p>
     * Note: This variant loads more data.
     * 
     * @throws Exception
     */
    public void test_HA3_groupCommit_create2Namespaces_concurrentLargeLoad() throws Exception {

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

         new ABC(false/*sequential*/); // simultaneous start.

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

            tasks.add(new LargeLoadTask(token, true/* reallyLargeLoad */,
                  false/* dropAll */, namespace1));

            tasks.add(new LargeLoadTask(token, true/* reallyLargeLoad */,
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
    * Create 2 namespaces and then load data into those namespaces in parallel
    * using a "DROP ALL; LOAD" pattern. A set of such tasks are generated and
    * the submitted in parallel. LOADs into the same namespace will be
    * serialized by the backend. Loads into different namespaces will be
    * parallelized.
    * 
    * @throws Exception
    */
    public void test_HA3_groupCommit_create2Namespaces_manyConcurrentLoadWithDropAll() throws Exception {

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

         new ABC(false/*sequential*/); // simultaneous start.

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

            for (int i = 0; i < 20; i++) {

               tasks.add(new LargeLoadTask(token, false/* reallyLargeLoad */,
                     true/* dropAll */, namespace1));

               tasks.add(new LargeLoadTask(token, false/* reallyLargeLoad */,
                     true/* dropAll */, namespace2));

            }
            
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

}
