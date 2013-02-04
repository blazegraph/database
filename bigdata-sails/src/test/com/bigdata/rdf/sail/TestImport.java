/**
Copyright (C) SYSTAP, LLC 2011.  All rights reserved.

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

package com.bigdata.rdf.sail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.ntriples.NTriplesParser;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Unit test template for use in submission of bugs.
 * <p>
 * This test case will delegate to an underlying backing store. You can specify
 * this store via a JVM property as follows:
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code>
 * <p>
 * There are three possible configurations for the testClass:
 * <ul>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithQuads (quads mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithoutSids (triples mode)</li>
 * <li>com.bigdata.rdf.sail.TestBigdataSailWithSids (SIDs mode)</li>
 * </ul>
 * <p>
 * The default for triples and SIDs mode is for inference with truth maintenance
 * to be on. If you would like to turn off inference, make sure to do so in
 * {@link #getProperties()}.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestImport extends QuadsTestCase {
	//Set file to import:
	private static File file = new File("/Volumes/SSDData/bigdata/inforbix/test2.n3");
	//pollInterval in ms. Using 30000 works fine on my PC
	private static long pollInterval = 3000;
	
	private Thread pollingTask;
	private volatile Throwable exception = null;

	public TestImport() {
	}

	public TestImport(String arg0) {
		super(arg0);
	}

	/**
	 * Please set your database properties here, except for your journal file,
	 * please DO NOT SPECIFY A JOURNAL FILE.
	 */
	@Override
	public Properties getProperties() {
		Properties props = super.getProperties();

		/*
		 * For example, here is a set of five properties that turns off
		 * inference, truth maintenance, and the free text index.
		 */
		props.setProperty(BigdataSail.Options.AXIOMS_CLASS,
				NoAxioms.class.getName());
		props.setProperty(BigdataSail.Options.VOCABULARY_CLASS,
				NoVocabulary.class.getName());
		props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		props.setProperty(BigdataSail.Options.JUSTIFY, "false");
		props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "false");
		props.setProperty(BigdataSail.Options.BUFFER_MODE,
				BufferMode.DiskRW.toString());

		return props;
	}

	public void testBug() throws Exception {
		BigdataSail sail = getSail();
		try {
			BigdataSailRepository repo = new BigdataSailRepository(sail);
			repo.initialize();
			startPollingTask(repo);
			doImport(repo);
			assertNull(exception);
			stopPollingTask();
		} finally {
			final IIndexManager db = sail.getDatabase().getIndexManager();
			if (sail.isOpen())
				sail.shutDown();
			db.destroy();
		}
	}

	private void doImport(final BigdataSailRepository repo)
			throws Exception, InterruptedException {
		final BigdataSailRepositoryConnection conn = repo
				.getUnisolatedConnection();
		try {
			conn.setAutoCommit(false);
			final long time = System.currentTimeMillis();
			NTriplesParser parser = new NTriplesParser(conn.getValueFactory());
			parser.setStopAtFirstError(false);
			parser.setRDFHandler(new RDFInserter(conn) {
				private int count = 0;

				public void handleStatement(Statement st)
						throws RDFHandlerException {
					super.handleStatement(st);
					if ((++count % 50000) == 0) {
						try {
							conn.commit();
						} catch (RepositoryException e) {
							throw new RDFHandlerException(e);
						}
						System.out.println(count + " "
								+ (System.currentTimeMillis() - time) + "ms");
					}
					assertNull(exception);
				}
			});
			InputStreamReader reader = new InputStreamReader(
					new BufferedInputStream(new FileInputStream(file), 67108864),
					"UTF-8");
			try {
				parser.parse(reader, "http://example.com/");
			} finally {
				reader.close();
			}
			conn.commit();
			System.out.println("Done: " + conn.size());

		} finally {
			conn.close();
			stopPollingTask();
		}
	}

	/**
	 * The problem only shows when the sleep is within the pollTask.
	 * 
	 * In other words when there is a fast connection close() and reopen().
	 * 
	 * Sounds like a locking issue around closeSessionProtection and
	 * startSession with getReadOnlyConnection().
	 */
	private void startPollingTask(final BigdataSailRepository repo)
			throws RepositoryException {
		pollingTask = new Thread(new Runnable() {
			@Override
			public void run() {
				boolean interrupted = false;
				while (!Thread.currentThread().isInterrupted() && !interrupted)
					try {
						interrupted = runPollTask(repo);
						// Thread.sleep(pollInterval); // wait 30seconds for next poll
					} catch (RepositoryException e) {
						log.error("exception", e);
						interrupted = true;
						exception = e;
//					} catch (InterruptedException e) {
//						interrupted = true;
					}
				System.out.println("polling stopped");
			}
		});
		pollingTask.start();
	}

	private boolean runPollTask(BigdataSailRepository repo)
			throws RepositoryException {
		BigdataSailRepositoryConnection conn = repo.getReadOnlyConnection();
		try {
			conn.setAutoCommit(false);
			System.out.println("Polling now");
			Value[] res = query(
					conn,
					"res",
					new QueryBindingSet(),
					"SELECT ?res WHERE { ?subj a <os:class/AnalysisContext> .FILTER sameTerm(?subj, <os:elem/analysisContext/rss>) }");
			if (res.length != 0) {
				QueryBindingSet bs = new QueryBindingSet();
				bs.addBinding("ctx", res[0]);
				bs.addBinding("queued",
						conn.getValueFactory().createLiteral(true));
				query(conn,
						"ar",
						bs,
						"SELECT ?ar WHERE {?ar a <os:class/AnalysisResults>. ?ar <os:prop/analysis/isQueuedForAnalysis> ?queued.?ar <os:prop/analysis/context> ?ctx} LIMIT 5");
			}
			Thread.sleep(pollInterval); // wait 30seconds for next poll
			return false;
		} catch (InterruptedException e) {
			System.out.println("polltask interrupted");
			exception = e;
			return true;
		} catch (Throwable t) {
			log.error("Exception thrown, testcase is going to fail", t);
			exception = t;
			return true;
		} finally {
			conn.close();
		}
	}

	private Value[] query(RepositoryConnection conn, String result,
			QueryBindingSet input, String query)
			throws QueryEvaluationException, RepositoryException,
			MalformedQueryException {
		TupleQuery tq = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		ArrayList<Value> matches = new ArrayList<Value>();
		TupleQueryResult results = tq.evaluate();
		try {
			while (results.hasNext())
				matches.add(results.next().getValue(result));
			return matches.toArray(new Value[matches.size()]);
		} finally {
			results.close();
		}
	}

	private void stopPollingTask() {
		if (pollingTask != null) {
			pollingTask.interrupt();
			pollingTask = null;
		}
	}
}
