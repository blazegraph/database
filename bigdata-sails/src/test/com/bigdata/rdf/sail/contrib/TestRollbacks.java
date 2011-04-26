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

package com.bigdata.rdf.sail.contrib;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

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
import com.bigdata.rdf.sail.QuadsTestCase;
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
public class TestRollbacks extends QuadsTestCase {
    public TestRollbacks() {
    }

    public TestRollbacks(String arg0) {
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
        props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");
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
    
    public void testBug() throws Exception {
        BigdataSail sail = getSail();
        try {
            SailRepository repo = new SailRepository(sail);
            repo.initialize();
            runConcurrentStuff(repo);
        } finally {
            final IIndexManager db = sail.getDatabase().getIndexManager();
            if (sail.isOpen())
                sail.shutDown();
            db.destroy();
        }
    }
    
    private void runConcurrentStuff(final SailRepository repo)
            throws Exception,
            InterruptedException {
        try {
            final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();
            tasks.add(new DoStuff(repo, true));
            tasks.add(new DoStuff(repo, false));
            tasks.add(new DoStuff(repo, false));
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
        int counter = 0;

        private DoStuff(SailRepository repo, boolean writer)
                throws OpenRDFException {
          this.repo = repo;
            this.writer = writer;
        }

        public Void call() throws Exception {
//            if (writer) {
//                // Initial sleep on the writer.
//                Thread.sleep(500);
//            }
            RepositoryConnection conn = null;
            try {
                while (firstCause.get() == null) {
                    /*
                     * Note: If connection obtained/closed within the loop then
                     * the query is more likely to have some data to visit
                     * within its tx view.
                     */
                    conn = repo.getConnection();
                    conn.setAutoCommit(false);
                    if (writer)
                        writer(conn);
                    else
                        reader(conn);
                    conn.close();
                }
                return (Void) null;
            } catch (Throwable t) {
                firstCause.compareAndSet(null/* expect */, t);
                throw new RuntimeException(t);
            } finally {
                if (conn != null && conn.isOpen())
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
