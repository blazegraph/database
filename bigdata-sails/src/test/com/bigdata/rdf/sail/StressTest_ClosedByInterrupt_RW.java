package com.bigdata.rdf.sail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

public class StressTest_ClosedByInterrupt_RW extends TestCase {
    
    private static final Logger log = Logger
            .getLogger(StressTest_ClosedByInterrupt_RW.class);
    
    public StressTest_ClosedByInterrupt_RW() {
        super();
    }

    public StressTest_ClosedByInterrupt_RW(String name) {
        super(name);
    }
    
    private static final int NUM_INSERT_DELETE_LOOPS = 10;
    private static final int NUM_INSERTS_PER_LOOP = 200000;
    private static final int NUM_DELETES_PER_LOOP = 23000;
    private static final long MILLIS_BETWEEN_INSERTS = -1;
    private static final long MILLIS_BETWEEN_DELETES = -1;
    private static final int NUM_STATEMENTS_PER_INSERT = 50;

    private static final int NUM_SELECTS = 5000;
    private static final int NUM_STATEMENTS_PER_SELECT = 23000;
    private static final long MILLIS_BETWEEN_QUERY_BURSTS = 1000;

    private static boolean HALT_ON_ERROR = true;

    private volatile boolean stopRequested = false;

    private void snooze(final long millis) throws InterruptedException {
        if (millis > 0) {
            Thread.sleep(millis);
        }
    }

//    @Test
    public void test() throws RepositoryException, InterruptedException {
        final File jnlFile = new File("interrupted.jnl");

        if (jnlFile.exists()) {
            jnlFile.delete();
        }

        final Properties props = new Properties();
        props.setProperty("com.bigdata.rdf.sail.namespace", "emc.srm.topology.kb");
        props.setProperty("com.bigdata.journal.AbstractJournal.bufferMode", "DiskRW");
        props.setProperty("com.bigdata.btree.writeRetentionQueue.capacity", "4000");
        props.setProperty("com.bigdata.btree.BTree.branchingFactor", "128");
        props.setProperty("com.bigdata.service.AbstractTransactionService.minReleaseAge", "1");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.textIndex", "false");
        props.setProperty(
                "com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlTransitiveProperty", "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlSameAsClosure",
                "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlSameAsProperties",
                "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlInverseOf", "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlEquivalentClass",
                "false");
        props.setProperty(
                "com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlEquivalentProperty", "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainOwlHasValue", "false");
        props.setProperty("com.bigdata.rdf.rules.InferenceEngine.forwardChainRdfTypeRdfsResource",
                "false");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.axiomsClass",
                "com.bigdata.rdf.axioms.NoAxioms");
        props.setProperty("com.bigdata.rdf.sail.truthMaintenance", "false");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.justify", "false");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers", "false");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.quadsMode", "true");
        props.setProperty("com.bigdata.journal.AbstractJournal.maximumExtent", "209715200");
        props.setProperty("com.bigdata.service.IBigdataClient.collectPlatformStatistics", "false");
        props.setProperty("com.bigdata.service.IBigdataClient.httpdPort", "-1");
        props.setProperty("com.bigdata.rdf.sail.bufferCapacity", "100000");
        props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.bloomFilter", "false");

        props.setProperty(BigdataSail.Options.CREATE_TEMP_FILE, Boolean.FALSE.toString());
        props.setProperty(BigdataSail.Options.FILE, jnlFile.toString());

        final BigdataSail sail = new BigdataSail(props);
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        repo.initialize();

        final InsertDeleteRunner mapper = new InsertDeleteRunner(repo);
        final ReadOnlyRunner mdp = new ReadOnlyRunner(repo);

        final Thread mapperThread = new Thread(mapper);
        final Thread mdpThread = new Thread(mdp);

        mapperThread.start();
        mdpThread.start();

        mapperThread.join();
        System.out.println("Mapper is done");

        stopRequested = true;
        mdpThread.join();
        System.out.println("MDP is done");

        repo.shutDown();
        System.out.println("Repository has shut down");

    }

    private class InsertDeleteRunner implements Runnable {

        private final BigdataSailRepository repo;

        public InsertDeleteRunner(final BigdataSailRepository repo) {
            this.repo = repo;
        }

        @Override
        public void run() {

            for (int loop = 0; loop < NUM_INSERT_DELETE_LOOPS; ++loop) {

                System.out.println("[Read/Write] enter loop " + loop);
                RepositoryConnection conn = null;

                try {
                    System.out.println("[Read/Write] inserting ...");
                    conn = repo.getConnection();
                    conn.setAutoCommit(false);

                    for (int index = 0; index < NUM_INSERTS_PER_LOOP; ++index) {
                        doInsert(conn, loop, index);
                        snooze(MILLIS_BETWEEN_INSERTS);
                    }

                    conn.commit();
                    conn.close();
                    conn = null;

                } catch (Throwable t) {
                    printError("Read/Write threw on insert in loop " + loop, t);
                } finally {
                    closeNoException(conn);
                }

                try {
                    System.out.println("[Read/Write] deleting ...");
                    conn = repo.getConnection();
                    conn.setAutoCommit(false);

                    for (int index = 0; index < NUM_DELETES_PER_LOOP; ++index) {
                        doDelete(conn, loop, index);
                        snooze(MILLIS_BETWEEN_DELETES);
                    }

                    conn.commit();
                    conn.close();
                    conn = null;

                } catch (Throwable t) {
                    printError("Read/Write threw on delete in loop " + loop, t);
                } finally {
                    closeNoException(conn);
                }
                System.out.println("[Read/Write] leave loop " + loop);
            }
        }

        private void doInsert(final RepositoryConnection conn, final int loop, final int index)
                throws RepositoryException {
            final ValueFactory vf = conn.getValueFactory();
            final URI c = vf.createURI("context:loop:" + loop + ":item:" + index);
            final URI s = vf.createURI("subject:loop:" + loop + ":item:" + index);
            for (int x = 0; x < NUM_STATEMENTS_PER_INSERT; ++x) {
                final URI p = vf.createURI("predicate:" + x);
                final Literal o = vf.createLiteral("SomeValue");
                conn.add(s, p, o, c);
            }
        }

        private void doDelete(final RepositoryConnection conn, final int loop, final int index)
                throws RepositoryException {
            final ValueFactory vf = conn.getValueFactory();
            final URI context = vf.createURI("context:loop:" + loop + ":item:" + index);
            final Collection<Statement> statements = getStatementsForContext(conn, context);
            for (Statement statement : statements) {
                conn.remove(statement, context);
            }
        }

        private Collection<Statement> getStatementsForContext(final RepositoryConnection conn,
                final URI context) throws RepositoryException {
            RepositoryResult<Statement> res = null;
            final Collection<Statement> statements = new ArrayList<Statement>();
            try {
                res = conn.getStatements(null, null, null, false, context);
                while (res.hasNext()) {
                    statements.add(res.next());
                }
            } finally {
                res.close();
            }
            return statements;
        }
    }

    private class ReadOnlyRunner implements Runnable {

        private final BigdataSailRepository repo;

        public ReadOnlyRunner(final BigdataSailRepository repo) {
            this.repo = repo;
        }

        @Override
        public void run() {

            RepositoryConnection conn = null;
            TupleQueryResult result = null;
            int loop = 0;

            while (stopRequested == false) {

                try {

                    System.out.println("[Read      ] snooze");
                    snooze(MILLIS_BETWEEN_QUERY_BURSTS);
                    System.out.println("[Read      ] enter loop " + loop);

                    for (int invocation = 0; invocation < NUM_SELECTS; ++invocation) {

                        conn = repo.getReadOnlyConnection();
                        conn.setAutoCommit(false);

                        final String sparql = "SELECT ?s WHERE { ?s ?p ?o } LIMIT "
                                + NUM_STATEMENTS_PER_SELECT;
                        final TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
                        result = query.evaluate();

                        final List<String> duds = new ArrayList<String>();

                        while (result.hasNext()) {
                            final BindingSet bindingSet = result.next();
                            for (final Iterator<Binding> i = bindingSet.iterator(); i.hasNext();) {
                                final Binding b = i.next();
                                if (b.getValue() != null) {
                                    duds.add(b.getValue().stringValue());
                                }
                            }
                        }

                        result.close();
                        result = null;

                        conn.close();
                        conn = null;

                    }

                } catch (Throwable t) {
                    printError("Read Only threw in loop " + loop, t);
                } finally {
                    closeNoException(result);
                    closeNoException(conn);
                }

                System.out.println("[Read      ] leave loop " + loop);
                ++loop;

            }
        }

    }

    private void closeNoException(RepositoryConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (RepositoryException e) {
                log.error("closeNoException(conn)", e);
            }
        }
    }

    private void closeNoException(TupleQueryResult result) {
        if (result != null) {
            try {
                result.close();
            } catch (QueryEvaluationException e) {
                log.error("closeNoException(result)", e);
            }
        }
    }

    private void printError(final String message, final Throwable cause) {
        log.error(message, cause);
//        Exception e = new Exception(message, cause);
//        e.printStackTrace();
        if (HALT_ON_ERROR) {
            System.exit(123);
        }
    }

}
