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
package com.bigdata.rdf.sail.webapp;

import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;

import org.openrdf.query.parser.sparql.SPARQLUpdateTest;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Proxied test suite.
 * <p>
 * Note: Also see {@link SPARQLUpdateTest}. These two test suites SHOULD be kept
 * synchronized. {@link SPARQLUpdateTest} runs against a local kb instance while
 * this class runs against the NSS. The two test suites are not exactly the same
 * because one uses the {@link RemoteRepository} to communicate with the NSS
 * while the other uses the local API.
 * 
 * @param <S>
 * 
 * @see SPARQLUpdateTest
 */
public class TestConcurrentRestApiRequests<S extends IIndexManager> extends
		AbstractTestNanoSparqlClient<S> {

    public TestConcurrentRestApiRequests() {

    }

	public TestConcurrentRestApiRequests(final String name) {

		super(name);

	}

   /*
    * Note: We need to be running this test suite for each of the BufferModes
    * that we want to support. This is because there are subtle interactions
    * between the BufferMode, the AbstractTask, and the execution of mutation
    * operations. One approach might be to pass in a collection of BufferMode
    * values rather than a singleton and then generate the test suite for each
    * BufferMode value in that collection [I've tried this, but I am missing
    * something in the proxy test pattern with the outcome that the tests are
    * not properly distinct.]
    */
	static public Test suite() {

		return ProxySuiteHelper.suiteWhenStandalone(TestConcurrentRestApiRequests.class,
				"test.*",
				new LinkedHashSet<BufferMode>(Arrays.asList(new BufferMode[]{
				BufferMode.Transient, 
				BufferMode.DiskWORM, 
				BufferMode.MemStore,
				BufferMode.DiskRW, 
				})),
				TestMode.quads
				);
	}

	private static final String EX_NS = "http://example.org/";

//    private ValueFactory f = new ValueFactoryImpl();
//    private URI bob, alice, graph1, graph2;
//    protected RemoteRepository m_repo;

	@Override
	public void setUp() throws Exception {

		super.setUp();
	  
//        bob = f.createURI(EX_NS, "bob");
//        alice = f.createURI(EX_NS, "alice");
//
//        graph1 = f.createURI(EX_NS, "graph1");
//        graph2 = f.createURI(EX_NS, "graph2");
	}
	
//	/**
//	 * Load the test data set.
//	 * 
//	 * @throws Exception
//	 */
//	private void doLoadFile() throws Exception {
//        /*
//		 * Only for testing. Clients should use AddOp(File, RDFFormat) or SPARQL
//		 * UPDATE "LOAD".
//		 */
//        loadFile(
//                "bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/dataset-update.trig",
//                RDFFormat.TRIG);
//	}
	
	@Override
	public void tearDown() throws Exception {
	    
//	    bob = alice = graph1 = graph2 = null;
//	    
//	    f = null;
	    
	    super.tearDown();
	    
	}

    /**
     * Get a set of useful namespace prefix declarations.
     * 
     * @return namespace prefix declarations for rdf, rdfs, dc, foaf and ex.
     */
    static private String getNamespaceDeclarations() {
        final StringBuilder declarations = new StringBuilder();
//        declarations.append("PREFIX rdf: <" + RDF.NAMESPACE + "> \n");
//        declarations.append("PREFIX rdfs: <" + RDFS.NAMESPACE + "> \n");
//        declarations.append("PREFIX dc: <" + DC.NAMESPACE + "> \n");
//        declarations.append("PREFIX foaf: <" + FOAF.NAMESPACE + "> \n");
        declarations.append("PREFIX ex: <" + EX_NS + "> \n");
//        declarations.append("PREFIX xsd: <" +  XMLSchema.NAMESPACE + "> \n");
        declarations.append("\n");

        return declarations.toString();
    }
    
    /**
     * A stress test of concurrent SPARQL UPDATE requests against multiple 
     * namespaces.
     * 
     * @throws Exception 
     */
	public void test_concurrentClients() throws Exception {

		/*
		 * Note: Using a timeout will cause any tasks still running when the
		 * timeout expires to be interrupted.
		 * 
		 * See TestConcurrentJournal for the original version of this code.
		 */
		doConcurrentClientTest(//
				m_repo.getRemoteRepositoryManager(),// MultiTenancy API client
				30, TimeUnit.SECONDS,// timeout
				5, // #of concurrent requests
				20, // #of namespaces
				100 // #of trials
		);

	}

	/**
	 * A stress test of concurrent operations on multiple namespaces.
	 * 
	 * @param rmgr
	 *            The {@link RemoteRepositoryManager} providing access to the
	 *            end point(s) under test.
	 * 
	 * @param timeout
	 *            The timeout before the test will terminate.
	 * @param unit
	 *            The unit for that timeout.
	 * 
	 * @param nthreads
	 *            The #of concurrent client requests to issue (GTE 1).
	 *            Concurrent requests against the same namespace are
	 *            non-blocking. A mutation request against a namespace will be
	 *            serialized against other mutations on the same namespace (but
	 *            should be concurrent against mutations on other namespaces
	 *            when group commit is enabled).
	 * 
	 * @param nnamespaces
	 *            The number of namespaces against which concurrent requests
	 *            will be issued (GTE 1).
	 * 
	 * @param ntrials
	 *            The #of requests to issue (GTE 1).
	 * @throws Exception 
	 */
	private Result doConcurrentClientTest(
			final RemoteRepositoryManager rmgr, //
			final long timeout, final TimeUnit unit, //
			final int nthreads,//
			final int nnamespaces,//
			final int ntrials//
			)
			throws Exception {

		if (rmgr == null)
			throw new IllegalArgumentException();
				
		if (timeout <= 0)
			throw new IllegalArgumentException();

		if (unit == null)
			throw new IllegalArgumentException();
		
		if (nthreads <= 0)
			throw new IllegalArgumentException();

		if (nnamespaces <= 0)
			throw new IllegalArgumentException();

		if (ntrials <= 0)
			throw new IllegalArgumentException();

		final Random r = new Random();

		/*
		 * Setup the namespaces.
		 * 
		 * TODO Could create/destroy the namespaces during the test as well.
		 * That would introduce another source of contention - one that has
		 * proven to be an important kind of contention for provoking failure.
		 */
		final String[] namespaces = new String[nnamespaces];
		{

			for (int i = 0; i < nnamespaces; i++) {

				final String namespace = namespaces[i] = "kb#" + i;

				final Properties properties = getTestMode().getProperties();

				properties.setProperty(
						RemoteRepository.OPTION_CREATE_KB_NAMESPACE, namespace);

				rmgr.createRepository(namespace, properties);

			}

		}

		if (log.isInfoEnabled())
			log.info("Created " + nnamespaces + " namespaces: "
					+ Arrays.toString(namespaces));

		/*
		 * Setup the tasks that we will submit.
		 */

		final Collection<Callable<Void>> tasks = new LinkedHashSet<Callable<Void>>();

		{

			final MutationOp[] ops = MutationOp.values();

			for (int i = 0; i < ntrials; i++) {

//				final Collection<String> tmp = new LinkedHashSet<String>(
//						nnamespaces);

				final String namespace = namespaces[r.nextInt(nnamespaces)];

				final MutationOp op = MutationOp.values()[r.nextInt(ops.length)];

				tasks.add(new WriteTask(rmgr
						.getRepositoryForNamespace(namespace), i, op));

			}
		}

		/*
		 * Run all tasks and wait for up to the timeout for them to complete.
		 */

		if (log.isInfoEnabled())
			log.info("Submitting " + tasks.size() + " tasks");

		final long begin = System.nanoTime();

		final ExecutorService executorService = Executors.newFixedThreadPool(
				nthreads, DaemonThreadFactory.defaultThreadFactory());
		
		int nfailed = 0; // #of tasks that failed.
		// int nretry = 0; // #of tasks that threw RetryException
		int ninterrupt = 0; // #of interrupted tasks.
		int ncommitted = 0; // #of tasks that successfully committed.
		int nuncommitted = 0; // #of tasks that did not complete in time.
		final long elapsed;
		
		try {
		
		final List<Future<Void>> results = executorService
				.invokeAll(tasks, timeout, TimeUnit.SECONDS);

		elapsed = System.nanoTime() - begin;

		/*
		 * Examine the futures to see how things went.
		 */
		final Iterator<Future<Void>> itr = results.iterator();

		while (itr.hasNext()) {

			final Future<?> future = itr.next();

			if (future.isCancelled()) {

				nuncommitted++;

				continue;

			}

			try {

				future.get();

				ncommitted++;

			} catch (ExecutionException ex) {

				if (isInnerCause(ex, InterruptedException.class)
						|| isInnerCause(ex, ClosedByInterruptException.class)) {

					/*
					 * Note: Tasks will be interrupted if a timeout occurs when
					 * attempting to run the submitted tasks - this is normal.
					 */

					log.warn("Interrupted: " + ex);

					ninterrupt++;

//				} else if (isInnerCause(ex, SpuriousException.class)) {
//
//					nfailed++;
//
//					// } else if(isInnerCause(ex, RetryException.class)) {
//					//
//					// nretry++;

				} else {

					// Other kinds of exceptions are errors.

					fail("Not expecting: " + ex, ex);

				}

			}

		}

		} finally {
		executorService.shutdownNow();
		}

		/*
		 * Compute bytes written per second.
		 */

//		final long seconds = TimeUnit.NANOSECONDS.toSeconds(elapsed));

//		final long bytesWrittenPerSecond = indexManager.getRootBlockView()
//				.getNextOffset() / (seconds == 0 ? 1 : seconds);

		final Result ret = new Result();

		// these are the results.
		ret.put("nfailed", "" + nfailed);
		// ret.put("nretry",""+nretry);
		ret.put("ncommitted", "" + ncommitted);
		ret.put("ninterrupt", "" + ninterrupt);
		ret.put("nuncommitted", "" + nuncommitted);
		ret.put("elapsed(ms)", "" + TimeUnit.NANOSECONDS.toMillis(elapsed));
//		ret.put("bytesWrittenPerSec", "" + bytesWrittenPerSecond);
		ret.put("tasks/sec", "" + (ncommitted * 1000 / elapsed));
		// ret.put("maxRunning",
		// ""+journal.getConcurrencyManager().writeService.getMaxRunning());
		// ret.put("maxPoolSize",
		// ""+journal.getConcurrencyManager().writeService.getMaxPoolSize());
		// ret.put("maxLatencyUntilCommit",
		// ""+journal.getConcurrencyManager().writeService.getMaxCommitWaitingTime());
		// ret.put("maxCommitLatency",
		// ""+journal.getConcurrencyManager().writeService.getMaxCommitServiceTime());

		System.err.println(ret.toString(true/* newline */));

//		journal.deleteResources();

		return ret;

	}

	static private final Random r = new Random();

//	public static class ReadTask implements Callable<Long> {
//		
//	}
	
	/*
	 * TODO It would be nice to have pre-/post- condition checks for these. That
	 * would be possible with Jeremy's suggestion (ABORT IF/UNLESS ASK). Those
	 * pre-/post- conditions could be developed by analyzing the original tests
	 * for SPARQL UPDATE operations in TestSparqlUpdate.
	 * 
	 * TODO Could turn this enum into a class and push down the evaluate()
	 * method onto that class so we could handle query and update or other rest
	 * api methods through the same abstraction within this stress test.
	 */
	static enum MutationOp {
		SparqlUpdateDropAll("DROP ALL"), //
		SparqlUpdateLoad(
				"LOAD <file:bigdata-sails/src/test/com/bigdata/rdf/sail/webapp/dataset-update.trig>"), //
		SparqlUpdateInsertWhere(
				"INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }"), //
		SparqlUpdateInsertWhereGraph(
				"INSERT {GRAPH ?g {?x rdfs:label ?y . }} WHERE {GRAPH ?g {?x foaf:name ?y }}");
		MutationOp(final String updateStr) {
			this.updateStr = getNamespaceDeclarations() + updateStr;
		}

		public final String updateStr;
	}

	/**
	 * A task that issues mutation requests using the REST API.
	 */
	static private class WriteTask implements Callable<Void> {

		private final RemoteRepository repo;
		private final int trial;
		private final MutationOp op;

		public WriteTask(final RemoteRepository repo, final int trial,
				final MutationOp op) {

			if (repo == null)
				throw new IllegalArgumentException();

			if (op == null)
				throw new IllegalArgumentException();

			this.repo = repo;

			this.trial = trial;

			this.op = op;

		}

		@Override
		public Void call() throws Exception {

			repo.prepareUpdate(op.updateStr).evaluate();

			return null;

		}

	} // class WriteTask

}
