/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2011.  All rights reserved.

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

package com.bigdata.rdf.sail;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.openrdf.OpenRDFException;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * This is a stress test for abort/rollback semantics.
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
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/278
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:gerdev@users.sourceforge.net">Gerjon</a>
 * @version $Id$
 */
public class TestRollbacks extends QuadsTestCase {

    private static final Logger log = Logger.getLogger(TestRollbacks.class);

	public TestRollbacks() {
    }

    public TestRollbacks(String arg0) {
        super(arg0);
    }

    @Override
    public Properties getProperties() {
        
    	final Properties props = super.getProperties();

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

        // transactions are off in the base version of this class.
        props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "false");

//		props.setProperty(BigdataSail.Options.CREATE_TEMP_FILE, "true");
//		props.setProperty(BigdataSail.Options.BUFFER_MODE, BufferMode.DiskRW
//				.toString());
        
//        props.setProperty(BigdataSail.Options.EXACT_SIZE, "true");

        return props;
    }


    /** The thrown exception which is the first cause of failure. */
    private AtomicReference<Throwable> firstCause;

    /**
     * Service used to run the individual tasks. This makes it possible to
     * interrupt them as soon as one of the tasks fails.
     */
    private ExecutorService executorService = null;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        firstCause = new AtomicReference<Throwable>(null);
        executorService = Executors.newFixedThreadPool(3/*nthreads*/); 
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (executorService != null) {
            // interrupt any running tasks.
            executorService.shutdownNow();
        }
        // clear references so that junit does not hold onto them.
        executorService = null;
        firstCause = null;
        super.tearDown();
    }

    /**
     * Stress test for abort/rollback semantics consisting of many short
     * runs of the basic test.
     * 
     * @throws Exception
     */
	public void testManyShortRuns() throws Exception {
		
		for (int i = 0; i < 20; i++) {
		
			doTest(10);
			
		}
		
    }

    /**
     * Stress test for abort/rollback semantics consisting of one moderate
     * duration run of the basic test.
     * 
     * @throws Exception
     */
    public void testModerateDuration() throws Exception {

    	doTest(100);
    	
    }

    static private final AtomicInteger runCount = new AtomicInteger();
    
    private void doTest(final int maxCounter) throws InterruptedException, Exception {

        /*
         * Note: Each run needs to be in a distinct namespace since we otherwise
         * can have side-effects through the BigdataValueFactoryImpl for a given
         * namespace.
         */
        
        final Properties properties = new Properties(getProperties());
        
        properties.setProperty(BigdataSail.Options.NAMESPACE,
                "kb" + runCount.incrementAndGet());
        
        final BigdataSail sail = getSail(properties);
        
        try {
        	// Note: Modified to use the BigdataSailRepository rather than the base SailRepository class.
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();
            runConcurrentStuff(repo,maxCounter);
        } finally {
			final IIndexManager db = sail.getIndexManager();
			try {
				if (sail.isOpen()) {
					try {
						sail.shutDown();
					} catch (Throwable t) {
						log.error(t, t);
					}
				}
			} finally {
				db.destroy();
			}
        }
    }
    
    private void runConcurrentStuff(final SailRepository repo,final int maxCounter)
            throws Exception,
            InterruptedException {
        try {
            final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();
            tasks.add(new DoStuff(repo, true/*writer*/, maxCounter));
            tasks.add(new DoStuff(repo, false/*reader*/, maxCounter));
            tasks.add(new DoStuff(repo, false/*reader*/, maxCounter));
            final List<Future<Void>> futures = executorService.invokeAll(tasks);
            // Look for the first cause.
            final Throwable t = firstCause.get();
            if (t != null) {
                // Found it.
                throw new RuntimeException(t);
            }
            // test each future.
            for (Future<Void> f : futures) {
                f.get();
            }
        } finally {
            repo.shutDown();
        }
    }

	private class DoStuff implements Callable<Void> {

		private SailRepository repo;
		private boolean writer;
		private final int maxCounter;
		int counter = 0;

		/**
		 * @param repo
		 *            The repository.
		 * @param writer
		 *            <code>true</code> iff this is a writer.
		 * @param maxCounter
		 *            Sets a limit on the length of the stress test. A value of
		 *            1000 results in a 26 second run. A value of 100-200 is
		 *            more reasonable and is sufficient to readily identify any
		 *            problems during CI.
		 */
		private DoStuff(final SailRepository repo, final boolean writer,
				final int maxCounter) throws OpenRDFException {
			this.repo = repo;
			this.writer = writer;
			this.maxCounter = maxCounter;
		}

        public Void call() throws Exception {
//            if (writer) {
//                // Initial sleep on the writer.
//                Thread.sleep(500);
//            }
            RepositoryConnection conn = null;
            try {
				int counter2 = 0;
                conn = repo.getConnection();
                conn.setAutoCommit(false);
                while (firstCause.get() == null && counter < maxCounter) {
                    if (writer)
                        writer(conn);
                    else
                        reader(conn);
                    /*
                     * Note: If connection obtained/closed within the loop then
                     * the query is more likely to have some data to visit
                     * within its tx view.
                     */
					if (++counter2 % 4 == 0) {
						conn.close();
						conn = repo.getConnection();
						conn.setAutoCommit(false);
					}
//					conn = repo.getConnection();
//                    conn.setAutoCommit(false);
//                    conn.close();
                }
                return (Void) null;
            } catch (Throwable t) {
                firstCause.compareAndSet(null/* expect */, t);
                throw new RuntimeException(t);
            } finally {
                if (conn != null)
                    conn.close();
            }
        }

        private void reader(final RepositoryConnection conn)
                throws RepositoryException, MalformedQueryException,
                QueryEvaluationException, InterruptedException {
            query(conn);
//            Thread.sleep(100);
            query(conn);
            ++counter;

            if (counter % 3 == 0)
                conn.commit();
            else
                conn.rollback();

            // if (counter % 7 == 0) {
            // conn.close();
            // conn = repo.getConnection();
            // conn.setAutoCommit(false);
            // }
        }

        private void writer(final RepositoryConnection conn) throws RepositoryException,
                MalformedQueryException, QueryEvaluationException,
                InterruptedException {

            final URI subj = conn.getValueFactory().createURI(
                    "u:s" + (counter++));
            final Value value = conn.getValueFactory().createLiteral(
                    "literal" + counter);
            query(conn);
//            Thread.sleep(200);
            conn.add(subj, conn.getValueFactory().createURI("u:p"), subj);
            conn.add(subj, conn.getValueFactory().createURI("u:p"), value);
            conn.commit();

            if(log.isInfoEnabled())
                log.info("Added statements: size="+conn.size());
            
            // if (counter % 12 == 0) {
            // conn.close();
            // conn = repo.getConnection();
            // conn.setAutoCommit(false);
            // }
        }

        private void query(final RepositoryConnection conn) throws RepositoryException,
                MalformedQueryException, QueryEvaluationException {
            final long begin = System.currentTimeMillis();
            /*
             * Note: This query will do an access path scan rather than a join.
             * There are different code paths involved with a join, so there
             * might be problems on those code paths as well.
             */
            final boolean useJoin = counter % 2 == 0;
            final String query = !useJoin//
            // access path scan
            ? "SELECT ?b { ?a ?b ?c } LIMIT 20"//
            // join
            : "SELECT ?b { ?a ?b ?c . ?d ?b ?e} LIMIT 20"//
            ;
            final TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            q.setBinding("b", conn.getValueFactory().createURI("u:p"));
            if (useJoin)
                q.setBinding("d", conn.getValueFactory().createLiteral(
                        "literal1"));
            final TupleQueryResult tqr = q.evaluate();
            int n = 0;
            try {
                while (tqr.hasNext()) {
                    tqr.next();
                    n++;
                }
            } finally {
                tqr.close();
            }
            if (log.isInfoEnabled())
                log.info("Query: writer=" + writer + ", counter=" + counter
                        + ", nresults=" + n + ", elapsed="
                        + (System.currentTimeMillis() - begin));
        }
    }

}
