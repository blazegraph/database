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

import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import junit.framework.Test;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.parser.sparql.SPARQLUpdateTest;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.engine.QueryTimeoutException;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.testutil.ExperimentDriver.Result;
import com.bigdata.util.Bytes;
import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.util.InnerCause;

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
 *            FIXME Add tests at the openrdf remote client interface layer as
 *            well. They are all flyweight wrappers around our own
 *            RemoteRepositoryManager (given the target namespace).
 * 
 *            FIXME Make sure that this test is running with GROUP_COMMIT
 *            enabled (it is, but GROUP_COMMIT should be parameterized in the
 *            test suite so we run both with and without unless we move the
 *            embedded use entirely over to that API).
 * 
 *            FIXME Also run this test with ISOLATABLE_INDICES enabled
 *            (potentially just for some of the namespaces since this is a
 *            per-namespace configuration property).
 * 
 *            FIXME Add Tx oriented tests. We might need to have a pool of
 *            (tx,namespace) pairs. Or just IRemoteTx objects. Could be an inner
 *            class in which isolation is known to be enabled. Make sure it is
 *            part of the test suite.
 *            
 *            TODO Refactor this test suite so we can use it against an NSS
 *            service end point rather than just integrated tests. That will
 *            make it easier to run as part of the long-lived testing leading
 *            up to a release.
 */
public class StressTestConcurrentRestApiRequests<S extends IIndexManager>
        extends AbstractTestNanoSparqlClient<S> {

    public StressTestConcurrentRestApiRequests() {

    }

    public StressTestConcurrentRestApiRequests(final String name) {

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

        if (true) {

            return ProxySuiteHelper.suiteWhenStandalone(
                    StressTestConcurrentRestApiRequests.class, //
                    "test.*",// normal
                    new LinkedHashSet<BufferMode>(Arrays
                            .asList(new BufferMode[] {
                            // BufferMode.Transient,
                            // BufferMode.DiskWORM,
                            BufferMode.MemStore,
//                             BufferMode.DiskRW,
                            })), TestMode.quads);

        } else if(true) {

            return ProxySuiteHelper.suiteWhenStandalone(
                    StressTestConcurrentRestApiRequests.class, //
//                    "stressTest_concurrentClients_2Hours",// 2 hours!
                    "stressTest_concurrentClients_10Minutes", // 10m
                    new LinkedHashSet<BufferMode>(Arrays
                            .asList(new BufferMode[] { BufferMode.DiskRW, })),
                    TestMode.quads);
        } else {

            return ProxySuiteHelper.suiteWhenStandalone(
                    StressTestConcurrentRestApiRequests.class, //
                    "stressTest_concurrentClients_24Hours",// 24 hours!
                    new LinkedHashSet<BufferMode>(Arrays
                            .asList(new BufferMode[] { BufferMode.DiskRW, })),
                    TestMode.quads);
        }

    }

    private static final String EX_NS = "http://example.org/";

    /** Bundles together future and operator. */
    private static class FutureAndTask {
        final Future<Void> f;
        final RestApiOp op;

        FutureAndTask(final Future<Void> f, final RestApiOp op) {
            if (f == null)
                throw new IllegalArgumentException();
            if (op == null)
                throw new IllegalArgumentException();
            this.f = f;
            this.op = op;
        }
    }

    /**
     * Shared state for the test harness that is used to coordinated across the
     * execution of the tasks.
     */
    private static class SharedTestState {
        /**
         * The {@link TestMode} (triples, RDR, quads, etc.).
         */
        private final TestMode testMode;
        /**
         * The #of tests that are currently executing (many of these may be
         * waiting on a namespace lock).
         */
        private final AtomicLong nrunning = new AtomicLong();
        /**
         * The #of tests that are actually doing something (they have their
         * namespace lock and are doing work).
         */
        private final AtomicLong nacting = new AtomicLong();
        /**
         * The #of operation instances that are actually doing work on a
         * namespace (they have their namespace lock and are doing work).
         * 
         * @see RandomNamespaceOp#begin(String, UUID, FutureTask)
         */
        private final ConcurrentHashMap<RestApiOp, String/* namespace */> activeTasks = new ConcurrentHashMap<RestApiOp, String>();
        /**
         * The {@link Future}s for the client operations - this collection
         * supports asynchronous cancellation of client operations in order to
         * provoke code paths associated with error handling in both the client
         * and (more vitally) the server.
         */
        private final ConcurrentHashMap<UUID, FutureAndTask> futures = new ConcurrentHashMap<UUID, FutureAndTask>();
        /**
         * The #of namespaces that have been created to date. The names of the
         * current namespaces (those that exist) are in {@link #namespaces}.
         */
        private final AtomicLong namespaceCreateCounter;
        /**
         * The #of namespaces that exist. This is maintained in a separate
         * counter to avoid invoking {@link ConcurrentHashMap#size()} all the
         * time.
         */
        private final AtomicLong namespaceExistCounter;
        /**
         * The minimum #of namespaces once the test harness is up and running.
         */
        private final AtomicLong minimumNamespaceCount;
        /**
         * A lock used to make sure that we do not violate the
         * {@link #minimumNamespaceCount}.
         */
        private final Lock destroyNamespaceLock;
        /**
         * This map is used to track the namespaces that exist on the server.
         * Operations choose the target namespace from among the current ones in
         * a pattern in which they also obtain either a readLock() or a
         * writeLock(). The writeLock() provides exclusive access (in the scope
         * of the test harness) to the associated namespace. The readLock()
         * allows concurrent access to the associated namespace.
         */
        private final ConcurrentHashMap<String, ReadWriteLock> namespaces;

        private final Random r;

        public SharedTestState(final TestMode testMode) {

            if (testMode == null)
                throw new IllegalArgumentException();

            this.testMode = testMode;

            r = new Random();

            namespaces = new ConcurrentHashMap<String, ReadWriteLock>();

            namespaceCreateCounter = new AtomicLong();

            namespaceExistCounter = new AtomicLong();

            minimumNamespaceCount = new AtomicLong();

            destroyNamespaceLock = new ReentrantLock();

        }

        /**
         * Return a namespace at random from the set of known to exist
         * namespaces. The caller will hold the appropriate lock.
         */
        private String lockRandomNamespace(final boolean readOnly) {
            final int k = r.nextInt((int) namespaceExistCounter.get());
            int i = -1;
            while (true) {
                for (Map.Entry<String, ReadWriteLock> e : namespaces.entrySet()) {
                    if (namespaceExistCounter.get() == 0) {
                        /*
                         * Either we need to run some operations that create new
                         * namespaces or we need to NOT run operations that
                         * destroy the last namespace.
                         */
                        throw new RuntimeException("No namespaces? readOnly="
                                + readOnly);
                    }
                    i++;
                    if (i < k) {
                        // log.info("Ignoring: i=" + i + "<k=" + k +
                        // ", namespace="
                        // + e.getKey());
                        continue;
                    }
                    // We can accept this namespace.
                    final String namespace = e.getKey();
                    final ReadWriteLock lock = e.getValue();
                    // Take the lock.
                    final Lock takenLock;
                    {
                        if (readOnly)
                            takenLock = lock.readLock();// read lock.
                        else
                            takenLock = lock.writeLock(); // write lock.
                        // acquire the lock.
                        takenLock.lock();
                    }
                    if (namespaces.get(namespace) != lock) {
                        /*
                         * The lock object was changed. Release the lock and
                         * look some more.
                         */
                        takenLock.unlock();
                        continue; // continue looking.
                    }
                    /*
                     * Namespace still exists in the map and is governed by the
                     * same lock.
                     */
                    return namespace;
                }
            }
            // We should never get here.
        }

        /**
         * Release a lock on a namespace.
         * 
         * @param namespace
         *            Then namespace (required).
         * @param readOnly
         *            When <code>true</code> the caller is holding the
         *            readLock(). When <code>false</code> they are holding the
         *            writeLock().
         * @param remove
         *            when <code>true</code> the <i>namespace</i> will be
         *            removed from the map.
         * 
         * @throws IllegalArgumentException
         *             if the <i>namespace</i> is <code>null</code>.
         * @throws IllegalStateException
         *             if <code>remove:=true</code> and
         *             <code>readOnly:=false</code> since removal from the map
         *             requires an exclusive lock to avoid violating locks
         *             obtained by other callers.
         * @throws IllegalStateException
         *             if <i>namespace</i> is not found in the map.
         * @throws RuntimeException
         *             An unchecked exception is thrown if the caller does not
         *             hold the appropriate lock.
         */
        private void unlockNamespace(final String namespace,
                final boolean readOnly, final boolean remove) {
            if (namespace == null)
                throw new IllegalArgumentException();
            if (remove && readOnly) {
                /*
                 * Note: We might need to relax this constraint if we allow
                 * DESTROY NAMESPACE operation without regard to concurrent
                 * client API requests. (Or perhaps we will just not use this
                 * map when we do that since the whole point is to violate the
                 * client's management of the namespaces and provoke failures
                 * that arise from those violated assumptions).
                 */
                throw new IllegalStateException(
                        "Removal from map requires exclusive lock: namespace="
                                + namespace);
            }
            // resolve entry in map.
            final ReadWriteLock lock = namespaces.get(namespace);
            if (lock == null)
                throw new IllegalStateException("Not locked: namespace="
                        + namespace);
            if (remove) {
                // remove entry from map.
                if (!namespaces.remove(namespace, lock)) {
                    throw new AssertionError("Entry not found in map: "
                            + namespace);
                }
            }
            // release lock.
            if (readOnly)
                lock.readLock().unlock();
            else
                lock.writeLock().unlock();
        }

    }

    private Collection<RestApiOp> restApiOps;

    private SharedTestState sharedTestState;

    @Override
    public void setUp() throws Exception {

        super.setUp();

        restApiOps = new LinkedList<RestApiOp>();

        sharedTestState = new SharedTestState(getTestMode());

        setupOperationMixture(getTestMode());

    }

    @Override
    public void tearDown() throws Exception {

        restApiOps = null;

        sharedTestState = null;

        super.tearDown();

    }

    /**
     * Create a bunch of operations to run against the REST API.
     * 
     * TODO Assign weights to each and then normalize the weights as
     * probabilities. Choose based on the normalized probabilities.
     * 
     * TODO Conditionally include quads or RDR specific operations based on the
     * {@link TestMode}.
     */
    private void setupOperationMixture(final TestMode testMode) {

        // SPARQL QUERY
        {

            // tuple query
            restApiOps.add(new SparqlTupleQueryOp(sharedTestState,
                    "SELECT (COUNT(*) as ?count) WHERE {?x foaf:name ?y }"));

            // graph query
            restApiOps.add(new SparqlGraphQueryOp(sharedTestState,
                    "CONSTRUCT WHERE {?x foaf:name ?y }"));

            // boolean query
            restApiOps.add(new SparqlBooleanQueryOp(sharedTestState,
                    "ASK WHERE {?x foaf:name ?y }"));

        }

        // SPARQL UPDATE
        {

            restApiOps.add(new DropAll(sharedTestState)
                    .setOperationFrequency(.01));

            restApiOps
                    .add(new SparqlUpdate(
                            sharedTestState,
                            "LOAD <file:src/test/java/com/bigdata/rdf/sail/webapp/dataset-update.trig>"));

            restApiOps.add(new SparqlUpdate(sharedTestState,
                    "INSERT {?x rdfs:label ?y . } WHERE {?x foaf:name ?y }"));

            if (testMode == TestMode.quads) {
                restApiOps
                        .add(new SparqlUpdate(sharedTestState,
                                "INSERT {GRAPH ?g {?x rdfs:label ?y . }} WHERE {GRAPH ?g {?x foaf:name ?y }}"));
            }

        }

        // TODO ESTCARD
        {        
            
        }
        
        /**
         * NSS Mutation API
         * 
         * FIXME Finish mutation API.
         * 
         * <pre>
         * 2.8 INSERT
         * 2.8.1 INSERT RDF (POST with Body) - done.
         * 2.8.2 INSERT RDF (POST with URLs)
         * 2.9 DELETE
         * 2.9.1 DELETE with Query
         * 2.9.2 DELETE with Body (using POST) - done.
         * 2.9.3 DELETE with Access Path
         * 2.11 UPDATE (DELETE + INSERT)
         * 2.11.1 UPDATE (DELETE statements selected by a QUERY plus INSERT statements from Request Body using PUT)
         * 2.11.2 UPDATE (POST with Multi-Part Request Body)
         * </pre>
         */
        {

            restApiOps
                    .add(new InsertWithBody(sharedTestState, 1000/* batchSize */)
                            .setOperationFrequency(.2));

            restApiOps
                    .add(new InsertWithBody(sharedTestState, 10000/* batchSize */)
                            .setOperationFrequency(.01));

            restApiOps
                    .add(new DeleteWithBody(sharedTestState, 1000/* batchSize */)
                            .setOperationFrequency(.2));

            restApiOps
                    .add(new DeleteWithBody(sharedTestState, 10000/* batchSize */)
                            .setOperationFrequency(.01));

        }

        // Multitenancy API.
        {

            restApiOps.add(new DESCRIBE_DATA_SETS(sharedTestState)
                    .setOperationFrequency(.05));

            restApiOps.add(new CREATE_DATA_SET(sharedTestState)
                    .setOperationFrequency(.03));

            restApiOps.add(new LIST_PROPERTIES(sharedTestState)
                    .setOperationFrequency(.20));

            restApiOps.add(new DESTROY_DATA_SET(sharedTestState)
                    .setOperationFrequency(.01));

        }

    }

    /**
     * Get a set of useful namespace prefix declarations.
     * 
     * @return namespace prefix declarations for rdf, rdfs, dc, foaf and ex.
     */
    static private String getNamespaceDeclarations() {
        final StringBuilder declarations = new StringBuilder();
        // declarations.append("PREFIX rdf: <" + RDF.NAMESPACE + "> \n");
        // declarations.append("PREFIX rdfs: <" + RDFS.NAMESPACE + "> \n");
        // declarations.append("PREFIX dc: <" + DC.NAMESPACE + "> \n");
        // declarations.append("PREFIX foaf: <" + FOAF.NAMESPACE + "> \n");
        declarations.append("PREFIX ex: <" + EX_NS + "> \n");
        // declarations.append("PREFIX xsd: <" + XMLSchema.NAMESPACE + "> \n");
        declarations.append("\n");

        return declarations.toString();
    }

    /**
     * A stress test of concurrent SPARQL UPDATE requests against multiple
     * namespaces that is intended to run in CI.
     */
    public void test_concurrentClients() throws Exception {
if(!BigdataStatics.runKnownBadTests)return; // FIXME Conditionally disabled in CI due to "namespace: EXISTS" test harness failures.
        /*
         * Note: Using a timeout will cause any tasks still running when the
         * timeout expires to be interrupted.
         * 
         * See TestConcurrentJournal for the original version of this code.
         */
        doConcurrentClientTest(//
                m_mgr,// remote repository manager
                10, TimeUnit.SECONDS,// timeout
                16, // #of concurrent requests
                20, // minimum #of namespaces
                4000, // #of trials
                1000L, 1000L, TimeUnit.MILLISECONDS // task interruption rate.
        );

    }

    /**
     * A 10 minute stress test.
     */
    public void stressTest_concurrentClients_10Minutes() throws Exception {

        doConcurrentClientTest(//
                m_mgr,// remote repository manager
                10, TimeUnit.MINUTES, // long run.
                16, // #of concurrent requests
                20, // minimum #of namespaces
                Bytes.megabyte32, // #of trials
                5000L, 5000L, TimeUnit.MILLISECONDS // task interruption rate.
        );

    }

    /**
     * A 2 hour stress test.
     */
    public void stressTest_concurrentClients_2Hours() throws Exception {

        doConcurrentClientTest(//
                m_mgr,// remote repository manager
                2, TimeUnit.HOURS, // long run.
                16, // #of concurrent requests
                20, // minimum #of namespaces
                Bytes.megabyte32, // #of trials
                5000L, 5000L, TimeUnit.MILLISECONDS // task interruption rate.
        );

    }

    /**
     * A 24 hour stress test.
     */
    public void stressTest_concurrentClients_24Hours() throws Exception {

        doConcurrentClientTest(//
                m_mgr,// remote repository manager
                24, TimeUnit.HOURS, // long run.
                16, // #of concurrent requests
                20, // minimum #of namespaces
                Bytes.megabyte32, // #of trials
                5000L, 5000L, TimeUnit.MILLISECONDS // task interruption rate.
        );

    }

    /**
     * A stress test of concurrent operations on multiple namespaces.
     * 
     * @param rmgr
     *            The {@link RemoteRepositoryManager} providing access to the
     *            end point(s) under test.
     * @param timeout
     *            The timeout before the test will terminate.
     * @param unit
     *            The unit for that timeout.
     * @param nthreads
     *            The #of concurrent client requests to issue (GTE 1).
     * @param initialNamespacesCount
     *            The initial number of namespaces (GTE 0).
     * @param ntrials
     *            The #of requests to issue (GTE 1).
     * @param initialDelayInterrupts
     *            The initial delay before client tasks are cancelled.
     * @param periodInterrupts
     *            client tasks will be CANCELed every period unit (IFF GT ZERO
     *            (0)).
     * @param unitInterrupts
     *            The units between cancellation of client tasks.
     * 
     * @throws Exception
     *             if there are any unexpected problems.
     * 
     *             TODO Handle {@link DatasetNotFoundException} as propagated
     *             back up to the HTTP layer IFF we allow a namespace to be
     *             deleted by a concurrent operation (this is prevented by
     *             locking right now).
     */
    private Result doConcurrentClientTest(final RemoteRepositoryManager rmgr, //
            final long timeout, final TimeUnit unit, //
            final int nthreads,//
            final int initialNamespacesCount,//
            final int ntrials,//
            final long initialDelayInterrupts,//
            final long periodInterrupts,//
            final TimeUnit unitInterrupts//
    ) throws Exception {

        if (rmgr == null)
            throw new IllegalArgumentException();

        if (timeout <= 0)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();

        if (nthreads <= 0)
            throw new IllegalArgumentException();

        if (initialNamespacesCount <= 0)
            throw new IllegalArgumentException();

        if (ntrials <= 0)
            throw new IllegalArgumentException();

        final Random r = new Random();

        /*
         * Setup the initial namespaces.
         */
        {

            // Make sure that we do not reduce the #of namespaces below this
            // limit.
            sharedTestState.minimumNamespaceCount.set(initialNamespacesCount);
            
            for (int i = 0; i < initialNamespacesCount; i++) {

                final RestApiOp op = new CREATE_DATA_SET(sharedTestState);

                op.newInstance(rmgr).call();

            }

            if (log.isInfoEnabled())
                log.info("Created " + initialNamespacesCount
                        + " initial nammespaces");

        }

        /*
         * Setup the tasks that we will submit. This is done by creating a
         * distribution of tasks based on their normalized probability as
         * declared in the test setup.
         * 
         * TODO It would be good to generate the actual task instances on demand
         * over time while respecting the distribution so we can run a very long
         * running workload based on this test harness rather than having to
         * generate all tasks up front.
         */
        final Collection<Callable<Void>> tasks = new LinkedHashSet<Callable<Void>>();

        {

            // the templates for the tasks (w/ unnormalized probabilities).
            final RestApiOp[] ops = restApiOps.toArray(new RestApiOp[0]);

            // compute normalized probability for each operation template.
            final double[] p = new double[ops.length];
            {
                {
                    double sum = 0d;
                    for (int i = 0; i < ops.length; i++) {
                        sum += p[i] = ops[i].operationProbability;
                    }
                    if (sum == 0d)
                        throw new AssertionError("No assigned probabilities");
                    for (int i = 0; i < ops.length; i++) {
                        p[i] /= sum;
                    }
                }
                // verify probabilities sum to ~ 1.0
                {
                    double sum = 0d;
                    for (int i = 0; i < ops.length; i++) {
                        sum += p[i];
                    }
                    assert sum <= 1.01d : "sum=" + sum + "::"
                            + Arrays.toString(p);
                    assert sum >= 0.99d : "sum=" + sum + "::"
                            + Arrays.toString(p);
                }
            }
            
            // Create population of randomly selected operations.
            for (int trial = 0; trial < ntrials; trial++) {

                // Random selection of operation.
                final RestApiOp op;
                {
                    final double d = r.nextDouble();
                    double sum = 0d;
                    int indexOf = -1;
                    for (int i = 0; i < ops.length; i++) {
                        sum += p[i];
                        if (sum >= d) {
                            indexOf = i;
                            break;
                        }
                    }
                    if (indexOf == -1) {
                        // Handles d ~ 1.0.
                        indexOf = ops.length - 1;
                    }
                    op = ops[indexOf];
                }

                tasks.add(op.newInstance(rmgr));

            }

        }

        /*
         * Run all tasks and wait for up to the timeout for them to complete.
         */

        if (log.isInfoEnabled())
            log.info("Submitting " + tasks.size() + " tasks");

        final long beginNanos = System.nanoTime();

        // Used to submit client requests.
        final ExecutorService executorService = Executors.newFixedThreadPool(
                nthreads, DaemonThreadFactory.defaultThreadFactory());

        // Used to interrupt client requests.
        final ScheduledExecutorService scheduledExecutor = Executors
                .newSingleThreadScheduledExecutor(DaemonThreadFactory
                        .defaultThreadFactory());

        int nfailed = 0; // #of tasks that failed.
        // int nretry = 0; // #of tasks that threw RetryException
        int ninterrupt = 0; // #of interrupted tasks.
        int ncommitted = 0; // #of tasks that successfully committed.
        int nuncommitted = 0; // #of tasks that did not complete in time.
        final long elapsedNanos;

        if (periodInterrupts > 0) {
            /*
             * Setup a scheduled future that will periodically interrupt the
             * thread executing the client operation *and* POST CANCEL request
             * for that operation to the server.
             */
            scheduledExecutor.scheduleWithFixedDelay(
                    new InterruptTask(rmgr, r), initialDelayInterrupts,
                    periodInterrupts, unitInterrupts);
        }

        try {

            /*
             * Note: As soon as this timeout is reached the remaining client API
             * tasks will be interrupted. This will cause CANCEL operations to
             * be posted to the server. These actions (CANCEL and client
             * initiating the termination of the HTTP request) will cause the
             * server to report errors out of launderThrowable(). THIS IS THE
             * EXPECTED BEHAVIOR.
             * 
             * If such errors show up before the timeout then they are real
             * errors and need to be diagnosed. This test driver will report any
             * such errors as test failures.
             */
            final List<Future<Void>> results = executorService.invokeAll(tasks,
                    timeout, unit);

            elapsedNanos = System.nanoTime() - beginNanos;

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
                            || isInnerCause(ex,
                                    ClosedByInterruptException.class)) {

                        /*
                         * Note: Tasks will be interrupted if a timeout occurs
                         * when attempting to run the submitted tasks - this is
                         * normal.
                         */

                        log.warn("Interrupted: " + ex);

                        ninterrupt++;

                    } else {

                        // Other kinds of exceptions are errors.

                        fail("Not expecting: " + ex, ex);

                    }

                }

            }

        } finally {
            executorService.shutdownNow();
            scheduledExecutor.shutdownNow();
//            scheduledFuture.cancel(true/* mayInterruptIfRunning */);
        }

        final Result ret = new Result();

        // these are the results.
        ret.put("nfailed", "" + nfailed);
        ret.put("ncommitted", "" + ncommitted);
        ret.put("ninterrupt", "" + ninterrupt);
        ret.put("nuncommitted", "" + nuncommitted);
        ret.put("elapsed(ms)", "" + TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
        ret.put("tasks/sec",
                ""
                        + (ncommitted * 1000 / TimeUnit.NANOSECONDS
                                .toMillis(elapsedNanos)));

        log.warn(ret.toString(true/* newline */));

        /*
         * FIXME Chase down all callers of Result<init>. Are the tests correctly
         * designed to detect failures? (nfailed==0)?
         */
        if (nfailed > 0)
            fail("Test failed: " + ret.toString());
        
        return ret;

    }

    /**
     * Task interrupts a running client operation and POSTs a CANCEL request to
     * the server. The operation to be cancelled is chosen at random.
     * 
     * TODO This random selection means that long running tasks tend to be
     * cancelled rather than complete.
     */
    private class InterruptTask implements Runnable {
        private final RemoteRepositoryManager rmgr;
        private final Random r;

        public InterruptTask(final RemoteRepositoryManager rmgr, final Random r) {
            if (rmgr == null)
                throw new IllegalArgumentException();
            if (r == null)
                throw new IllegalArgumentException();
            this.rmgr = rmgr;
            this.r = r;
        }
        @Override
        public void run() {
            /*
             * Note: Do NOT throw exceptions out of this code. That will prevent
             * the future from being re-scheduled.
             */
            // while (true)
            {
                final UUID[] uuids = sharedTestState.futures.keySet().toArray(
                        new UUID[] {});
                if (uuids.length == 0) {
                    // Nothing running.
                    return;
                }
                final UUID uuid = uuids[r.nextInt(uuids.length)];
                final FutureAndTask tmp = sharedTestState.futures.get(uuid);
                final Future<Void> ft = tmp.f;
                ft.cancel(true/* mayInterruptIfRunning */);
                if (ft.isCancelled()) {
                    log.warn("Cancelled task: op=" + tmp.op + ", uuid=" + uuid);
                    // POST CANCEL request.
                    try {
                        rmgr.cancel(uuid);
                    } catch (Throwable t) {
                        log.warn(t, t);
                    }
                }
                return;
            }
        }
    }

    /**
     * Abstract base class for REST API operation templates. An instance of the
     * template is obtained using {@link #newInstance(RemoteRepositoryManager)}.
     * Those template instances can then execute against the server using the
     * provisioned client API.
     */
    static abstract private class RestApiOp {

        /**
         * Shared state used to coordinate the individual operations.
         */
        protected final SharedTestState sharedTestState;

        /**
         * True iff this is a read-only task.
         */
        @SuppressWarnings("unused")
        private final boolean readOnly;

//        /**
//         * The probability that the task will be interrupted while it is
//         * executing (this probability is independent for each task so it does
//         * not need to be normalized) (default is ZERO).
//         */
//        private double interruptProbability = 0.0d;
        /**
         * The un-normalized likelihood that the task will be executed relative
         * to the other tasks in the workload mixture (default is ONE).
         */
        private double operationProbability = 1.0d;

        @Override
        public String toString() {

            return getClass().getSimpleName();

        }

        RestApiOp(final SharedTestState sharedTestState, final boolean readOnly) {
            if (sharedTestState == null)
                throw new IllegalArgumentException();
            this.sharedTestState = sharedTestState;
            this.readOnly = readOnly;
        }

//        /**
//         * Return true iff this is a read-only task.
//         */
//        public final boolean isReadOnly() {
//            return readOnly;
//        }

//        /**
//         * The probability that the task will be interrupted while it is
//         * executing (this probability is independent for each task so it does
//         * not need to be normalized) (default is ZERO).
//         */
//        public RestApiOp setInterruptProbability(double newValue) {
//            if (newValue < 0 || newValue >= 1)
//                throw new IllegalArgumentException();
//            this.interruptProbability = newValue;
//            return this;
//        }

        /**
         * Set the relative likelihood that this operation will be executed
         * compared to the other operations in the workload mixture (default is
         * ONE).
         * <p>
         * Note: These likelihoods must be normalized by the test harness to
         * ensure that they sum to ONE.
         */
        public RestApiOp setOperationFrequency(final double newValue) {
            if (newValue < 0 || newValue >= 1)
                throw new IllegalArgumentException();
            this.operationProbability = newValue;
            return this;
        }

        /**
         * Return an instance of this operation template that will run using the
         * specified client API.
         * 
         * @param rmgr
         *            The client API.
         * 
         * @return An instance of the operation template for execution by the
         *         test harness.
         */
        public Callable<Void> newInstance(final RemoteRepositoryManager rmgr) {

            if (rmgr == null)
                throw new IllegalArgumentException();

            return new InnerCallable(rmgr);

        }

        private class InnerCallable implements Callable<Void> {

            private final RemoteRepositoryManager rmgr;

            InnerCallable(final RemoteRepositoryManager rmgr) {
                if (rmgr == null)
                    throw new IllegalArgumentException();
                this.rmgr = rmgr;
            }

            /**
             * Execute the client operation.
             * 
             * @throws Exception
             *             whatever is thrown back to the client (or by the
             *             client) for the task.
             */
            @Override
            final public Void call() throws Exception {
                /*
                 * The UUID that will be assigned to a REST API request that we
                 * can cancel from the test harness.
                 */
                final UUID uuid = UUID.randomUUID();
                try {
                    sharedTestState.nrunning.incrementAndGet();
                    doApply(rmgr, uuid);
                    return (Void) null;
                } catch (Throwable t) {
                    /*
                     * Note: The stack traces here are from the *client*. Any
                     * stack trace from the server shows up as the response
                     * entity and we DO NOT have visibility into that trace (no
                     * RMI style stack frame from the server).
                     * 
                     * TODO There may also be HTTP status codes that we need to
                     * explicitly look for here and report back as
                     * success/failure along appropriate dimensions. This could
                     * all be returned as side-effects on the Task, but the
                     * FutureTask will not have access to that so maybe as a
                     * return type?
                     */
                    if (isTerminationByInterrupt(t)) {
                        if (log.isInfoEnabled())
                            log.info(t);
                        /*
                         * The test harness is either being torn down or has
                         * explicitly interrupted the execution of this
                         * RestApiOp. We will wrap (a) and re-throw the
                         * exception; and (b) CANCEL the operation on the
                         * server.
                         */
                        final InterruptedException ex = new InterruptedException(
                                toString());
                        ex.initCause(t);
                        // try {
                        // /*
                        // * This relies on (a) the client setting the UUID on
                        // the REST
                        // * operation; and (b) the server recognizing and using
                        // that
                        // * UUID; and (c) the server implementing the logic to
                        // locate
                        // * and interrupt the server-side task for that UUID.
                        // Without
                        // * all of those pieces this will fail to actually
                        // cancel the
                        // * operation on the server.
                        // */
                        // rmgr.cancel(queryId);
                        // } catch (Throwable t2) {
                        // throw new RuntimeException(
                        // "Error cancelling client operation: " + this, t2);
                        // }
                        throw ex;
                    } // end interrupt of client logic.
                      // launder anything else.
                    if (t instanceof Exception)
                        throw ((Exception) t);
                    throw new RuntimeException(toString(), t);
                } finally {
                    sharedTestState.nrunning.decrementAndGet();
                }

            }

            private boolean isTerminationByInterrupt(final Throwable cause) {

                if (InnerCause.isInnerCause(cause, InterruptedException.class))
                    return true;
                if (InnerCause.isInnerCause(cause, CancellationException.class))
                    return true;
                if (InnerCause.isInnerCause(cause,
                        ClosedByInterruptException.class))
                    return true;
                if (InnerCause.isInnerCause(cause, BufferClosedException.class))
                    return true;
                if (InnerCause.isInnerCause(cause, QueryTimeoutException.class))
                    return true;

                return false;

            }

        }

        /** When the client API operation begins to execute. */
        private long beginNanos = -1L;

        /**
         * Log begin client API operation and note timestamp.
         * 
         * @param namespace
         *            The namespace.
         */
        protected UUID begin(final String namespace, final UUID uuid,
                final FutureTask<Void> ft) {

            beginNanos = System.nanoTime();

            sharedTestState.activeTasks.put(this, namespace);

            // insert into cancellable map (UUID => FutureTask)
            sharedTestState.futures.put(uuid, new FutureAndTask(ft, this));
            
            if (log.isInfoEnabled())
                log.info("Call"//
                    + ": nactive="
                    + sharedTestState.nacting.incrementAndGet()
                    + ", namespace="
                    + namespace
                    + ", op="
                    + toString()
                    + ", active="
                    + sharedTestState.activeTasks.entrySet().toString());

            return uuid;
            
        }

        /**
         * Log end client API operation.
         * 
         * @param namespace
         *            The namespace.
         */
        protected void done(final String namespace, final UUID uuid) {

            final long elapsedNanos = System.nanoTime() - beginNanos;

            sharedTestState.activeTasks.remove(this, namespace);

            // Remove from collection of cancellable Futures.
            sharedTestState.futures.remove(uuid);
            
            if (log.isInfoEnabled())
                log.info("Done"//
                    + ": nactive="
                    + sharedTestState.nacting.decrementAndGet()
                    + ", namespace="
                    + namespace
                    + ", op="
                    + toString()
                    + ", elapsed="
                    + TimeUnit.NANOSECONDS.toMillis(elapsedNanos) + "ms"
                    + ", active="
                    + sharedTestState.activeTasks.entrySet().toString());

        }

        /**
         * Obtain an exclusive lock on a random existing namespace.
         * 
         * @return The name of that namespace.
         */
        protected String lockRandomNamespaceExclusive() {
            return sharedTestState.lockRandomNamespace(false/* readOnly */);
        }

        /**
         * Release an exclusive lock on the specified namespace.
         * 
         * @param namespace
         *            The name of that namespace.
         * @param remove
         *            <code>true</code> iff the namespace should be removed from
         *            the map.
         */
        protected void unlockNamespaceExclusive(final String namespace,
                final boolean remove) {
            sharedTestState.unlockNamespace(namespace, false/* readOnly */,
                    remove);
        }

        /**
         * Obtain an non-exclusive lock on a random existing namespace.
         * 
         * @return The name of that namespace.
         */
        protected String lockRandomNamespace() {
            return sharedTestState.lockRandomNamespace(true/* readOnly */);
        }

        /**
         * Release a non-exclusive lock on the specified namespace.
         * 
         * @param namespace
         *            The name of that namespace.
         */
        protected void unlockNamespace(final String namespace) {
            sharedTestState.unlockNamespace(namespace, true/* readOnly */,
                    false/* remove */);
        }

        /**
         * Implement this method for a concrete REST API operation.
         * 
         * @param rmgr
         *            The client API for the remote service.
         * @param uuid
         *            The UUID that the test harness may use to cancel the
         *            request.
         */
        protected abstract void doApply(final RemoteRepositoryManager rmgr,
                final UUID uuid) throws Exception;

    }

    /**
     * Abstract base class for tests that operate against a random namespace
     * that is known to exist on the server. The class builds in the pattern for
     * selecting the namespace at random and holding a <em>non-exclusive</em>
     * lock across the namespace.
     */
    private static abstract class RandomNamespaceOp extends RestApiOp {

        RandomNamespaceOp(final SharedTestState sharedTestState,
                final boolean readOnly) {

            super(sharedTestState, readOnly);

        }

        @Override
        final protected void doApply(final RemoteRepositoryManager rmgr,
                final UUID uuid) throws Exception {

            // obtain non-exclusive lock for random existing namespace.
            final String namespace = lockRandomNamespace();

            try {

                // client API for that namespace.
                final RemoteRepository repo = rmgr
                        .getRepositoryForNamespace(namespace);

                // Wrap as FutureTask.
                final FutureTask<Void> ft = new FutureTask<Void>(
                        
                        new Callable<Void>() {

                            @Override
                            public Void call() throws Exception {

                                // do operation.
                                doApplyToNamespace(repo, uuid);

                                return null;
                            }
                        });
                

                // register test.
                begin(namespace, uuid, ft);
                
                ft.run();// run in our thread.
                
                try {
                
                    ft.get(); // check future.
                    
                } finally {
                    
                    // done with test.
                    done(namespace, uuid);
                    
                }
                
            } finally {

                // release non-exclusive lock.
                unlockNamespace(namespace);

            }

        }

        /**
         * Invoked to execute an operation against a namespace.
         * 
         * @param repo
         *            The client API for that namespace.
         * @param uuid
         *            The uuid for the operation as assigned by the test
         *            harness.
         * @throws Exception
         */
        abstract protected void doApplyToNamespace(RemoteRepository repo,
                UUID uuid) throws Exception;

    }

    /*
     * SPARQL QUERY
     */

    private static class SparqlTupleQueryOp extends RandomNamespaceOp {

        SparqlTupleQueryOp(final SharedTestState sharedTestState,
                final String queryStr) {

            super(sharedTestState, true/* readOnly */);

            this.queryStr = getNamespaceDeclarations() + queryStr;

        }

        private final String queryStr;

        @Override
        protected void doApplyToNamespace(final RemoteRepository repo,
                final UUID uuid) throws Exception {

            repo.prepareTupleQuery(queryStr, uuid).evaluate();

        }

    }

    private static class SparqlGraphQueryOp extends RandomNamespaceOp {

        SparqlGraphQueryOp(final SharedTestState sharedTestState,
                final String queryStr) {

            super(sharedTestState, true/* readOnly */);

            this.queryStr = getNamespaceDeclarations() + queryStr;

        }

        private final String queryStr;

        @Override
        protected void doApplyToNamespace(final RemoteRepository repo,
                final UUID uuid) throws Exception {

            repo.prepareGraphQuery(queryStr, uuid).evaluate();

        }

    }

    private static class SparqlBooleanQueryOp extends RandomNamespaceOp {

        SparqlBooleanQueryOp(final SharedTestState sharedTestState,
                final String queryStr) {

            super(sharedTestState, true/* readOnly */);

            this.queryStr = getNamespaceDeclarations() + queryStr;

        }

        private final String queryStr;

        @Override
        protected void doApplyToNamespace(final RemoteRepository repo,
                final UUID uuid) throws Exception {

            repo.prepareBooleanQuery(queryStr, uuid).evaluate();

        }

    }

    /*
     * SPARQL UPDATE
     */

    private static class SparqlUpdate extends RandomNamespaceOp {

        SparqlUpdate(final SharedTestState sharedTestState,
                final String updateStr) {

            super(sharedTestState, false/* readOnly */);

            this.updateStr = getNamespaceDeclarations() + updateStr;

        }

        private final String updateStr;

        @Override
        protected void doApplyToNamespace(final RemoteRepository repo,
                final UUID uuid) throws Exception {

            repo.prepareUpdate(updateStr, uuid).evaluate();

        }

    }

    private static class DropAll extends SparqlUpdate {

        DropAll(SharedTestState sharedTestState) {
            super(sharedTestState, "DROP ALL");
        }

    }

    /*
     * Non-SPARQL API
     * 
     * FIXME Provide coverage for both mutation and read-only operations
     * (estcard, etc.)
     * 
     * FIXME (***) Non-SPARQL API MUST SUPPORT CANCEL and WE MUST PASS IN THE DESIRED UUID SO WE CAN CANCEL FROM THE TEST HARNESS.
     */

    private static class InsertWithBody extends RandomNamespaceOp {

        private final int batchSize;

        InsertWithBody(final SharedTestState sharedTestState,
                final int batchSize) {

            super(sharedTestState, false/* readOnly */);
            
            if (batchSize <= 0)
                throw new IllegalArgumentException();
            
            this.batchSize = batchSize;
            
        }

        @Override
        protected void doApplyToNamespace(final RemoteRepository repo,
                final UUID uuid) throws Exception {

            // Setup data.
            final RemoteRepository.AddOp op;
            {
                final Collection<Statement> stmts = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    stmts.add(generateTriple());
                }
                op = new RemoteRepository.AddOp(stmts);
            }

            // do mutation.
            repo.add(op, uuid);

        }

        private static Statement generateTriple() {

            return new StatementImpl(new URIImpl(EX_NS + UUID.randomUUID()),
                    new URIImpl(EX_NS + UUID.randomUUID()), new URIImpl(EX_NS
                            + UUID.randomUUID()));

        }

    }

    /**
     * DELETE WITH BODY
     * <p>
     * Note: This implementation uses a SELECT query with a LIMIT to identify
     * a set of statements that exist in the namespace. This is done because we
     * are using a lot of UUIDs when writing on the namespaces so we can not
     * expect a randomly generated set of UUID-based statements to match
     * anything on DELETE. Hence we delete out statements that are known to
     * exist.
     * <p>
     * Note: This does not wrap the SELECT query and the DELETE operation
     * with a transaction so the operation is not guaranteed to be atomic.
     * Therefore the #of statements actually removed is not guaranteed to be the
     * same as the #of statements that we POST in the DELETE request.
     */
    private static class DeleteWithBody extends RandomNamespaceOp {

        private final boolean quads;
        private final int batchSize;

        DeleteWithBody(final SharedTestState sharedTestState,
                final int batchSize) {

            super(sharedTestState, false/* readOnly */);
            
            if (batchSize <= 0)
                throw new IllegalArgumentException();
            
            this.batchSize = batchSize;
            
            quads = sharedTestState.testMode == TestMode.quads;
            
        }

        @Override
        protected void doApplyToNamespace(final RemoteRepository repo,
                final UUID uuid) throws Exception {

            /*
             * Setup data to be deleted.
             * 
             * Note: This uses SELECT rather than CONSTRUCT since we need to get
             * the graph as well when addressing a QUADS mode namespace.
             */
            final ValueFactory vf = ValueFactoryImpl.getInstance();
            final Collection<Statement> stmts = new ArrayList<>(batchSize);
            TupleQueryResult result = null;
            try {
                if (quads) {
                    result = repo.prepareTupleQuery(
                            "SELECT * WHERE {GRAPH ?g {?s ?p ?o} }")
                            .evaluate();
                } else {
                    result = repo.prepareTupleQuery(
                            "SELECT * WHERE {?s ?p ?o}").evaluate();
                }
                while (result.hasNext() && stmts.size() < batchSize) {
                    final BindingSet bset = result.next();
                    final Resource s = (Resource) bset.getBinding("s")
                            .getValue();
                    final URI p = (URI) bset.getBinding("p").getValue();
                    final Value o = (Value) bset.getBinding("o").getValue();
                    final Resource g = (Resource) (quads ? bset.getBinding("g")
                            .getValue() : null);
                    final Statement stmt = quads ? vf.createStatement(s, p, o,
                            g) : vf.createStatement(s, p, o);
                    stmts.add(stmt);
                }
            } finally {
                if (result != null)
                    result.close();
            }

            // do mutation.
            repo.remove(new RemoteRepository.RemoveOp(stmts), uuid);

        }

    }


    /*
     * MULTITENANCY API
     * 
     * TODO LIST NAMESPACES, GET PROPERTIES
     * 
     * FIXME (***) Multi-Tenancy API must support CANCEL.
     */

    abstract private static class RepositoryManagerOp extends RestApiOp {

        protected final String namespace;
        
        RepositoryManagerOp(final SharedTestState sharedTestState,
                final boolean readOnly, final String namespace) {
            super(sharedTestState, readOnly);
            if (namespace == null)
                throw new IllegalArgumentException();
            this.namespace = namespace;
        }
        
        @Override
        final protected void doApply(final RemoteRepositoryManager rmgr,
                final UUID uuid) throws Exception {
            
            // Obtain task and wrap as FutureTask.
            final FutureTask<Void> ft = new FutureTask<Void>(
                    getTask(rmgr, uuid));

            begin(namespace, uuid, ft);
            
            ft.run();// run in our thread.

            try {
            
                ft.get(); // check future.
                
            } finally {
                
                done(namespace, uuid);
                
            }
            
        }
        
        /**
         * Return the task.
         * 
         * @param uuid
         *            The UUID that the task must assign to the REST API
         *            operation that it is under test. The task MAY be cancelled
         *            by the test harness using this UUID.
         */
        protected abstract Callable<Void> getTask(
                final RemoteRepositoryManager rmgr, final UUID uuid);

    }
    
//    private static class CREATE_DATA_SET extends RepositoryManagerOp {
//
//        CREATE_DATA_SET(final SharedTestState sharedTestState) {
//            super(sharedTestState, false/* readOnly */, //
//                    "n"
//                            + sharedTestState.namespaceCreateCounter
//                                    .incrementAndGet()//
//            );
//        }
//
//        @Override
//        protected Callable<Void> getTask(final RemoteRepositoryManager rmgr,
//                final UUID uuid) {
//
//            return new Callable<Void>() {
//
//                @Override
//                public Void call() throws Exception {
//
//                    // Note: Wrap properties to avoid modification!
//                    final Properties properties = new Properties(
//                            sharedTestState.testMode.getProperties());
//
//                    // create namespace.
//                    rmgr.createRepository(namespace, properties, uuid);
//
//                    // add entry IFF created.
//                    if (sharedTestState.namespaces.putIfAbsent(namespace,
//                            new ReentrantReadWriteLock()) != null) {
//                        // Should not exist! Each namespace name is distinct!!!
//                        throw new AssertionError("namespace=" + namespace);
//                    }
//
//                    // Track #of namespaces that exist in the service.
//                    sharedTestState.namespaceExistCounter.incrementAndGet();
//
//                    return null;
//                }
//                
//            };
//
//        }
//
//    }
    
    private static class CREATE_DATA_SET extends RestApiOp {

        CREATE_DATA_SET(final SharedTestState sharedTestState) {
            super(sharedTestState, false/* readOnly */);
        }

        @Override
        protected void doApply(final RemoteRepositoryManager rmgr,
                final UUID uuid) throws Exception {

            // Note: Do NOT assign the namespace until the task executes!!!
            final String namespace = "n"
                    + sharedTestState.namespaceCreateCounter.incrementAndGet();

            // Wrap as FutureTask.
            final FutureTask<Void> ft = new FutureTask<Void>(

            new Callable<Void>() {

                @Override
                public Void call() throws Exception {

                    // Note: Wrap properties to avoid modification!
                    final Properties properties = new Properties(
                            sharedTestState.testMode.getProperties());

                    // create namespace.
                    rmgr.createRepository(namespace, properties);

                    // add entry IFF created.
                    if (sharedTestState.namespaces.putIfAbsent(namespace,
                            new ReentrantReadWriteLock()) != null) {
                        // Should not exist! Each namespace name is distinct!!!
                        throw new AssertionError("namespace=" + namespace);
                    }

                    // Track #of namespaces that exist in the service.
                    sharedTestState.namespaceExistCounter.incrementAndGet();

                    return null;
                }
                
            });

            begin(namespace, uuid, ft);

            ft.run(); // run in our thread.

            try {

                ft.get(); // check future.

            } finally {

                done(namespace, uuid);

            }

        }

    }

    /**
     * Drop a namespace.
     * 
     * FIXME Implement variant operation in which we drop the namespace even
     * though other operations are already queued against that namespace (which
     * could be namespace drops, queries, or mutations). This will provide some
     * coverage for error handling when a namespace disappears concurrent with
     * client requests against that namespace.
     */
    private static class DESTROY_DATA_SET extends RestApiOp {

        DESTROY_DATA_SET(final SharedTestState sharedTestState) {
            super(sharedTestState, false/* readOnly */);
        }

        @Override
        protected void doApply(final RemoteRepositoryManager rmgr,
                final UUID uuid) throws Exception {

            final AtomicBoolean success = new AtomicBoolean(false);
            
            // exclusive lock on random namespace.
            final String namespace = lockRandomNamespaceExclusive();

            try {

                // Wrap as FutureTask.
                final FutureTask<Void> ft = new FutureTask<Void>(

                new Callable<Void>() {

                    @Override
                    public Void call() throws Exception {

                        /*
                         * Atomic decision whether to destroy the namespace
                         * (MUTEX).
                         * 
                         * TODO This MUTEX section means that at most one
                         * DESTORY NAMESPACE operation can run at a time. This
                         * means that we are never running those operations
                         * concurrently and that means that we could be missing
                         * some interesting edge cases in the concurrency
                         * control. Modify this code to support more
                         * concurrency.
                         */
                        sharedTestState.destroyNamespaceLock.lock();
                        try {

                            // Track #of namespaces that exist in the service.
                            if (sharedTestState.namespaceExistCounter.get() <= sharedTestState.minimumNamespaceCount
                                    .get()) {

                                log.warn("AT NAMESPACE MINIMUM: min="
                                        + sharedTestState.minimumNamespaceCount
                                        + ", cur="
                                        + sharedTestState.namespaceExistCounter);

                                return null;

                            }

                            // destroy the namespace.
                            rmgr.deleteRepository(namespace, uuid);
                            
                            success.set(true);

                        } finally {

                            sharedTestState.destroyNamespaceLock.unlock();

                        }

                        return null;
                    }
                });

                begin(namespace, uuid, ft);

                ft.run();// run in our thread.

                try {

                    ft.get(); // check future.

                } finally {

                    done(namespace, uuid);

                }

            } finally {

                // Unlock. Remove IFF successful.
                unlockNamespaceExclusive(namespace, success.get()/* remove */);

            }

        }

    }
    
    /**
     * Operator to obtain a SERVICE DESCRIPTION of all known data sets (aka
     * namespaces).
     */
    private static class DESCRIBE_DATA_SETS extends RepositoryManagerOp {

        DESCRIBE_DATA_SETS(final SharedTestState sharedTestState) {
            /*
             * Note: A mock namespace is used to make sure that this operation
             * is not serialized on any real namespace.
             */
            super(sharedTestState, true/* readOnly */, //
                    "mock-namespace-" + UUID.randomUUID()//
            );
        }

        @Override
        protected Callable<Void> getTask(final RemoteRepositoryManager rmgr,
                final UUID uuid) {
 
            return new Callable<Void>() {
            
                public Void call() throws Exception {
                
                    rmgr.getRepositoryDescriptions(uuid);
                    
                    return null;
                    
                }

            };

        }

    }

    /**
     * Operator to obtain the properties for a specific namespace.
     * 
     * FIXME THIS TASK IS NOT CANCELLED YET.
     */
    private static class LIST_PROPERTIES extends RestApiOp {

        LIST_PROPERTIES(final SharedTestState sharedTestState) {
            super(sharedTestState, true/* readOnly */);
        }

        @Override
        protected void doApply(final RemoteRepositoryManager rmgr,
                final UUID uuid) throws Exception {
            
            // obtain non-exclusive lock for random existing namespace.
            final String namespace = lockRandomNamespace();

            try {
                
                rmgr.getRepositoryProperties(namespace, uuid);
                
            } finally {

                // release non-exclusive lock.
                unlockNamespace(namespace);

            }

        }

    }

}
/**
 * FIXME Verify that we are observing CANCEL requests for non-SPARQL QUERY and
 * non-SPARQL UPDATE operations (specifically CREATE NAMESPACE and DROP
 * NAMESPACE) on the server. Go ahead and test out the CANCEL DROP NAMESPACE
 * manually using a large load into one namespace + commit. Then DROP NAMESPACE
 * and look for the operation running in the status window. Finally cancel that
 * operation and make sure that it is terminated (non-success). This might
 * require the DROP NAMESPACE operation to be hacked to pause for 60s rather
 * than continuing to give me enough time after it has queued the delete blocks
 * to actually interrupt the task.
 * 
 * FIXME Try a test that writes a lot of data into a namespace and then (in a
 * second operation) deletes the namespace and cancels the DELETE after 200ms.
 * Verify that the namespace is NOT deleted. Do again. Then write another large
 * namespace and commit. Verify that a series of small commits may then be
 * taken. The goal is to drive recycling of deferred frees from a partial
 * release of slots.
 * 
 * FIXME This test might be passing with the RWStore.reset() changes. Roll those
 * back and see if we can provoke a test failure. Until we can get to a failure
 * we do not really know if we have a test for the problem.
 */
