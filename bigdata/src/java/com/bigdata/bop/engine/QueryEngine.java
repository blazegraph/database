/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
/*
 * Created on Aug 21, 2010
 */

package com.bigdata.bop.engine;

import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import alice.tuprolog.Prolog;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.bset.Union;
import com.bigdata.bop.fed.FederatedQueryEngine;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.view.FusedView;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.eval.pipeline.DistributedJoinTask;
import com.bigdata.resources.IndexManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ndx.IAsynchronousWriteBufferFactory;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * A class managing execution of concurrent queries against a local
 * {@link IIndexManager}.
 * <p>
 * <h2>Design notes</h2>
 * <p>
 * Much of the complexity of the current approach owes itself to having to run a
 * separate task for each join for each shard in order to have the appropriate
 * lock when running against the unisolated shard view. This also means that the
 * join task is running inside of the concurrency manager and hence has the
 * local view of the shard.
 * <p>
 * The main, and perhaps the only, reason why we run unisolated rules is during
 * closure, when we query against the unisolated indices and then write the
 * entailments back on the unisolated indices.
 * <p>
 * Supporting closure has always been complicated. This complexity is mostly
 * handled by ProgramTask#executeMutation() and
 * AbstractTripleStore#newJoinNexusFactory() which play games with the
 * timestamps used to read and write on the database, with commit points
 * designed to create visibility for tuples written by a mutation rule, and with
 * the automated advance of the read timestamp for the query in each closure
 * pass in order to make newly committed tuples visible to subsequent rounds of
 * closure. For scale-out, we do shard-wise auto commits so we always have a
 * commit point which makes each write visible and the read timestamp is
 * actually a read-only transaction which prevents the historical data we need
 * during a closure round from being released as we are driving updates onto the
 * federation. For the RWStore, we are having a similar problem (in the HA
 * branch since that is where we are working on the RWStore) where historically
 * allocated records were being released as writes drove updates on the indices.
 * Again, we "solved" the problem for the RWStore using a commit point followed
 * by a read-only transaction reading on that commit point to hold onto the view
 * on which the next closure round needs to read (this uncovered a problem with
 * the RWStore and transaction service interaction which Martyn is currently
 * working to resolve through a combination of shadow allocators and deferred
 * deletes which are processed once the release time is advanced by the
 * transaction service).
 * <p>
 * The WORM does not have some of these problems with closure because we never
 * delete history, so we do not need to create a commit point and a read-behind
 * transaction. However, the WORM would have problems with concurrent access to
 * the unisolated indices except that we hack that problem through the
 * transparent use of the UnisolatedReadWriteIndex, which allows multiple
 * threads to access the same unisolated index view using a read/write lock
 * pattern (concurrent readers are allowed, but there is only one writer and it
 * has exclusive access when it is running). This works out because we never run
 * closure operations against the WORM through the concurrency manager. If we
 * did, we would have to create a commit point after each mutation and use a
 * read-behind transaction to prevent concurrent access to the unisolated index.
 * <p>
 * The main advantage that I can see of the current complexity is that it allows
 * us to do load+closure as a single operation on the WORM, resulting in a
 * single commit point. This makes that operation ACID without having to use
 * full read/write transactions. This is how we gain the ACID contract for the
 * standalone Journal in the SAIL for the WORM. Of course, the SAIL does not
 * have that contract for the RWStore because we have to do the commit and
 * read-behind transaction in order to have visibility and avoid concurrent
 * access to the unisolated index (by reading behind on the last commit point).
 * <p>
 * I think that the reality is even one step more complicated. When doing truth
 * maintenance (incremental closure), we bring the temporary graph to a fixed
 * point (the rules write on the temp store) and then apply the delta in a
 * single write to the database. That suggests that incremental truth
 * maintenance would continue to be ACID, but that database-at-once-closure
 * would be round-wise ACID.
 * <p>
 * So, I would like to suggest that we break ACID for database-at-once-closure
 * and always follow the pattern of (1) do a commit before each round of
 * closure; and (2) create a read-behind transaction to prevent the release of
 * that commit point as we drive writes onto the indices. If we follow this
 * pattern then we can write on the unisolated indices without conflict and read
 * on the historical views without conflict. Since there will be a commit point
 * before each mutation rule runs (which corresponds to a closure round),
 * database-at-once-closure will be atomic within a round, but will not be a
 * single atomic operation. Per above, I think that we would retain the ACID
 * property for incremental truth maintenance against a WORM or RW mode Journal.
 * 
 * <p>
 * ----
 * </p>
 * 
 * The advantage of this proposal (commit before each mutation rule and run
 * query against a read-behind transaction) is that this could enormously
 * simplify how we execute joins.
 * <p>
 * Right now, we use a factory pattern to create a join task on each node for
 * each shard for which that node receives binding sets for a query. The main
 * reason for doing this is to gain the appropriate lock for the unisolated
 * index. If we never run a query against the unisolated index then we can go
 * around the concurrency manager and run a single "query manager" task for all
 * joins for all shards for all queries. This has some great benefits which I
 * will go into below.
 * <p>
 * That "query manager" task would be responsible for accepting buffers
 * containing elements or binding sets from other nodes and scheduling
 * consumption of those data based on various criteria (order of arrival,
 * priority, buffer resource requirements, timeout, etc.). This manager task
 * could use a fork join pool to execute light weight operations (NIO,
 * formulation of access paths from binding sets, mapping of binding sets onto
 * shards, joining a chunk already read from an access path against a binding
 * set, etc). Operations which touch the disk need to run in their own thread
 * (until we get Java 7 async file IO, which is already available in a preview
 * library). We could handle that by queuing those operations against a fixed
 * size thread pool for reads.
 * <p>
 * This is a radical change in how we handle distributed query execution, but I
 * think that it could have a huge payoff by reducing the complexity of the join
 * logic, making it significantly easier to execute different kinds of join
 * operations, reducing the overhead for acquiring locks for the unisolated
 * index views, reducing the #of threads consumed by joins (from one per shard
 * per join per query to a fixed pool of N threads for reads), etc. It would
 * centralize the management of resources on each node and make it possible for
 * us to handle things like join termination by simply purging data from the
 * query manager task for the terminated join.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * 
 * FIXME Unit tests for non-distinct {@link IElementFilter}s on an
 * {@link IPredicate}, unit tests for distinct element filter on an
 * {@link IPredicate} which is capable of distributed operations. Do not use
 * distinct where not required (SPOC, only one graph, etc).
 * <p>
 * It seems like the right way to approach this is by unifying the stackable CTC
 * striterator pattern with the chunked iterator pattern and passing the query
 * engine (or the bop context) into the iterator construction process (or simply
 * requesting that the query engine construct the iterator stack).
 * <p>
 * In terms of harmonization, it is difficult to say which way would work
 * better. In the short term we could simply allow both and mask the differences
 * in how we construct the filters, but the conversion to/from striterators and
 * chunked iterators seems to waste a bit of effort.
 * <p>
 * The trickiest part of all of this is to allow a distributed filter pattern
 * where the filter gets created on a set of nodes identified by the operator
 * and the elements move among those nodes using the query engine's buffers.
 * <p>
 * To actually implement the distributed distinct filter we need to stack the
 * following:
 * 
 * <pre>
 * - ITupleIterator
 * - Resolve ITuple to Element (e.g., SPOC).
 * - Layer on optional IElementFilter associated with the IPredicate.
 * - Layer on SameVariableConstraint iff required (done by AccessPath) 
 * - Resolve SPO to SPO, stripping off the context position.
 * - Chunk SPOs (SPO[], IKeyOrder), where the key order is from the access path.
 * - Filter SPO[] using DHT constructed on specified nodes of the cluster.
 *   The SPO[] chunks should be packaged into NIO buffers and shipped to those
 *   nodes.  The results should be shipped back as a bit vectors packaged into
 *   a NIO buffers.
 * - Dechunk SPO[] to SPO since that is the current expectation for the filter
 *   stack.
 * - The result then gets wrapped as a {@link IChunkedOrderedIterator} by
 *   the AccessPath using a {@link ChunkedArrayIterator}.
 * </pre>
 * 
 * This stack is a bit complex(!). But it is certainly easy enough to generate
 * the necessary bits programmatically.
 * 
 * FIXME Handling the {@link Union} of binding sets. Consider whether the chunk
 * combiner logic from the {@link DistributedJoinTask} could be reused.
 * 
 * FIXME INSERT and DELETE which will construct elements using
 * {@link IRelation#newElement(java.util.List, IBindingSet)} from a binding set
 * and then use {@link IMutableRelation#insert(IChunkedOrderedIterator)} and
 * {@link IMutableRelation#delete(IChunkedOrderedIterator)}. For s/o, we first
 * need to move the bits into the right places so it makes sense to unpack the
 * processing of the loop over the elements and move the data around, writing on
 * each index as necessary. There could be eventually consistent approaches to
 * this as well. For justifications we need to update some additional indices,
 * in which case we are stuck going through {@link IRelation} rather than
 * routing data directly or using the {@link IAsynchronousWriteBufferFactory}.
 * For example, we could handle routing and writing in s/o as follows:
 * 
 * <pre>
 * INSERT(relation,bindingSets) 
 * 
 * expands to
 * 
 * SEQUENCE(
 * SELECT(s,p,o), // drop bindings that we do not need
 * PARALLEL(
 *   INSERT_INDEX(spo), // construct (s,p,o) elements and insert
 *   INSERT_INDEX(pos), // construct (p,o,s) elements and insert
 *   INSERT_INDEX(osp), // construct (o,s,p) elements and insert
 * ))
 * 
 * </pre>
 * 
 * The output of the SELECT operator would be automatically mapped against the
 * shards on which the next operators need to write. Since there is a nested
 * PARALLEL operator, the mapping will be against the shards of each of the
 * given indices. (A simpler operator would invoke
 * {@link SPORelation#insert(IChunkedOrderedIterator)}. Handling justifications
 * requires that we also formulate the justification chain from the pattern of
 * variable bindings in the rule).
 * 
 * FIXME Handle {@link Program}s. There are three flavors, which should probably
 * be broken into three operators: sequence(ops), set(ops), and closure(op). The
 * 'set' version would be parallelized, or at least have an annotation for
 * parallel evaluation. These things belong in the same broad category as the
 * join graph since they are operators which control the evaluation of other
 * operators (the current pipeline join also has that characteristic which it
 * uses to do the nested index subqueries).
 * 
 * FIXME SPARQL to BOP translation
 * <p>
 * The initial pass should translate from {@link IRule} to {@link BOp}s so we
 * can immediately begin running SPARQL queries against the {@link QueryEngine}.
 * A second pass should explore a rules base translation from the openrdf SPARQL
 * operator tree into {@link BOp}s, perhaps using an embedded {@link Prolog}
 * engine. What follows is a partial list of special considerations for that
 * translation:
 * <ul>
 * <li>Distinct can be trivially enforced for default graph queries against the
 * SPOC index.</li>
 * <li>Local distinct should wait until there is more than one tuple from the
 * index since a single tuple does not need to be made distinct using a hash
 * map.</li>
 * <li>Low volume distributed queries should use solution modifiers which
 * evaluate on the query controller node rather than using distributed sort,
 * distinct, slice, or aggregation operators.</li>
 * <li></li>
 * <li></li>
 * <li></li>
 * <li>High volume queries should use special operators (different
 * implementations of joins, use an external merge sort, etc).</li>
 * </ul>
 * 
 * FIXME SPARQL Coverage: Add native support for all SPARQL operators. A lot of
 * this can be picked up from Sesame. Some things, such as isIRI() can be done
 * natively against the {@link IV}. Likewise, there is already a set of
 * comparison methods for {@link IV}s which are inlined values. Add support for
 * <ul>
 * <li></li>
 * <li></li>
 * <li></li>
 * <li></li>
 * <li></li>
 * <li></li>
 * </ul>
 * 
 * @todo Expander patterns will continue to exist until we handle the standalone
 *       backchainers in a different manner for scale-out so add support for
 *       those for now.
 * 
 * @todo There is a going to be a relationship to recycling of intermediates
 *       (for individual {@link BOp}s or {@link BOp} tree fragments) and a
 *       distributed query cache which handles invalidation (for updates) and
 *       {@link BOp} aware reuse of result sets available in the cache. This
 *       sort of thing will have to be coordinated among the cache nodes.
 */
public class QueryEngine implements IQueryPeer, IQueryClient {
    
    private final static transient Logger log = Logger
            .getLogger(QueryEngine.class);

    /**
     * Error message used if a query is not running.
     */
    protected static final transient String ERR_QUERY_NOT_RUNNING = "Query is not running:";

    /**
     * Access to the indices.
     * <p>
     * Note: You MUST NOT use unisolated indices without obtaining the necessary
     * locks. The {@link QueryEngine} is intended to run only against committed
     * index views for which no locks are required.
     */
    private final IIndexManager localIndexManager;

//    /**
//     * A pool used to service IO requests (reads on access paths).
//     * <p>
//     * Note: An IO thread pool at this level must attach threads to operations
//     * (access path reads) rather than to individual IO requests. In order to do
//     * this at the level of individual IOs the pool would have to be integrated
//     * into a lower layer, probably wrapping {@link FileChannelUtility}.
//     */
//    private final Executor iopool;

//    /**
//     * A pool for executing fork/join tasks servicing light weight tasks which
//     * DO NOT block on IO. Examples of such tasks abound, including: NIO for
//     * sending/receiving direct {@link ByteBuffer}s containing binding sets,
//     * elements, solutions, etc; formulation of access paths from binding sets;
//     * mapping of binding sets onto shards; joining a chunk already read from an
//     * access path against a binding set; etc. What all of these tasks have in
//     * common is that they DO NOT touch the disk. Until we get Java7 and async
//     * I/O, operations which touch the disk CAN NOT be combined with the fork /
//     * join model since they will trap the thread in which they are running
//     * (this is not true for {@link Lock}s).
//     * <p>
//     * Note: In order to enable the {@link ForkJoinPool} using Java6, you MUST
//     * run java with <code>-Xbootclasspath/p:jsr166.jar</code>, where you
//     * specify the fully qualified path of the jsr166.jar file.
//     */
//    private final ForkJoinPool fjpool;

    /**
     * The {@link UUID} of the service in which this {@link QueryEngine} is
     * running.
     * 
     * @return The {@link UUID} of the service in which this {@link QueryEngine}
     *         is running -or- <code>null</code> if the {@link QueryEngine} is
     *         not running against an {@link IBigdataFederation}.
     */
    public UUID getServiceUUID() {

        return null;
        
    }

    /**
     * The {@link IBigdataFederation} iff running in scale-out.
     * <p>
     * Note: The {@link IBigdataFederation} is required in scale-out in order to
     * perform shard locator scans when mapping binding sets across the next
     * join in a query plan.
     */
    public IBigdataFederation<?> getFederation() {
        
        return null;
        
    }

    /**
     * The <em>local</em> index manager, which provides direct access to local
     * {@link BTree} and {@link IndexSegment} objects. In scale-out, this is the
     * {@link IndexManager} inside the {@link IDataService} and provides direct
     * access to {@link FusedView}s (aka shards).
     * <p>
     * Note: You MUST NOT use unisolated indices without obtaining the necessary
     * locks. The {@link QueryEngine} is intended to run only against committed
     * index views for which no locks are required.
     */
    public IIndexManager getIndexManager() {
        
        return localIndexManager;
        
    }

    /**
     * The currently executing queries.
     */
    final protected ConcurrentHashMap<Long/* queryId */, RunningQuery> runningQueries = new ConcurrentHashMap<Long, RunningQuery>();

    /**
     * A priority queue of {@link RunningQuery}s having binding set chunks
     * available for consumption.
     */
    final private PriorityBlockingQueue<RunningQuery> priorityQueue = new PriorityBlockingQueue<RunningQuery>();

    /**
     * 
     * @param indexManager
     *            The <em>local</em> index manager.
     */
    public QueryEngine(final IIndexManager indexManager) {

        if (indexManager == null)
            throw new IllegalArgumentException();

        this.localIndexManager = indexManager;

//        this.iopool = new LatchedExecutor(indexManager.getExecutorService(),
//                nThreads);
//        this.iopool = Executors.newFixedThreadPool(nThreads,
//                new DaemonThreadFactory(getClass().getName()));
        
//        this.fjpool = new ForkJoinPool();

    }

    /**
     * Initialize the {@link QueryEngine}. It will accept binding set chunks and
     * run them against running queries until it is shutdown.
     */
    public void init() {

        final FutureTask<Void> ft = new FutureTask<Void>(new QueryEngineTask(),
                (Void) null);
        
        if (engineFuture.compareAndSet(null/* expect */, ft)) {
        
            localIndexManager.getExecutorService().execute(ft);
            
        } else {
            
            throw new IllegalStateException("Already running");
            
        }

    }

    /**
     * The {@link Future} for the query engine.
     */
    private final AtomicReference<FutureTask<Void>> engineFuture = new AtomicReference<FutureTask<Void>>();

    /**
     * Volatile flag is set for normal termination.  When set, no new queries
     * will be accepted but existing queries will run to completion.
     */
    private volatile boolean shutdown = false;

    /**
     * Return if the query engine is running.
     * 
     * @throws IllegalStateException
     *             if the query engine is shutting down.
     */
    protected void assertRunning() {
       
        if (shutdown)
            throw new IllegalStateException("Shutting down.");

    }

    /**
     * Runnable submits chunks available for evaluation against running queries.
     * 
     * @todo Handle priority for selective queries based on the time remaining
     *       until the timeout.
     *       <p>
     *       Handle priority for unselective queries based on the order in which
     *       they are submitted?
     * 
     * @todo The approach taken by the {@link QueryEngine} executes one task per
     *       pipeline bop per chunk. Outside of how the tasks are scheduled,
     *       this corresponds closely to the historical pipeline query
     *       evaluation.
     *       <p>
     *       Chunk concatenation could be performed here if we (a) mark the
     *       {@link LocalChunkMessage} with a flag to indicate when it has been
     *       accepted; and (b) rip through the incoming chunks for the query for
     *       the target bop and combine them to feed the task. Chunks which have
     *       already been assigned would be dropped when take() discovers them.
     *       [The chunk combination could also be done when we output the chunk
     *       if the sink has not been taken, e.g., by combining the chunk into
     *       the same target ByteBuffer, or when we add the chunk to the
     *       RunningQuery.]
     */
    private class QueryEngineTask implements Runnable {
        public void run() {
            if(log.isInfoEnabled())
                log.info("running: " + this);
            while (true) {
                try {
                    final RunningQuery q = priorityQueue.take();
                    final long queryId = q.getQueryId();
                    if (q.isCancelled())
                        continue;
                    final IChunkMessage<IBindingSet> chunk = q.chunksIn.poll();
                    if (log.isTraceEnabled())
                        log.trace("Accepted chunk: " + chunk);
                    try {
                        // create task.
                        final FutureTask<?> ft = q.newChunkTask(chunk);
                        if (log.isDebugEnabled())
                            log.debug("Running chunk: " + chunk);
                        // execute task.
                        localIndexManager.getExecutorService().execute(ft);
                    } catch (RejectedExecutionException ex) {
                        // shutdown of the pool (should be an unbounded
                        // pool).
                        log.warn("Dropping chunk: queryId=" + queryId);
                        continue;
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted.");
                    return;
                } catch (Throwable ex) {
                    // log and continue
                    log.error(ex, ex);
                    continue;
                }
            }
        }
    } // QueryEngineTask

    /**
     * Add a chunk of intermediate results for consumption by some query. The
     * chunk will be attached to the query and the query will be scheduled for
     * execution.
     * 
     * @param chunk
     *            A chunk of intermediate results.
     * 
     * @throws IllegalArgumentException
     *             if the chunk is <code>null</code>.
     * @throws IllegalStateException
     *             if the chunk is not materialized.
     */
    void acceptChunk(final IChunkMessage<IBindingSet> chunk) {
        
        if (chunk == null)
            throw new IllegalArgumentException();

        if (!chunk.isMaterialized())
            throw new IllegalStateException();

        final RunningQuery q = runningQueries.get(chunk.getQueryId());
        
        if(q == null)
            throw new IllegalStateException();
        
        // add chunk to the query's input queue on this node.
        q.acceptChunk(chunk);
        
        // add query to the engine's task queue.
        priorityQueue.add(q);
        
    }

    /**
     * Shutdown the {@link QueryEngine} (blocking). The {@link QueryEngine} will
     * not accept new queries, but existing queries will run to completion.
     * 
     * @todo This sleeps until {@link #runningQueries} is empty. It could be
     *       signaled when that collection becomes empty if we protected the
     *       collection with a lock for mutation (or if we just notice each time
     *       a query terminates). However, that would restrict the concurrency
     *       for query start/stop.
     */
    public void shutdown() {

        // normal termination.
        shutdown = true;

        while(!runningQueries.isEmpty()) {
            
            try {
                Thread.sleep(100/*ms*/);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            
        }
        
        // hook for subclasses.
        didShutdown();
        
        // stop the query engine.
        final Future<?> f = engineFuture.get();
        if (f != null)
            f.cancel(true/* mayInterruptIfRunning */);
        
    }

    /**
     * Hook is notified by {@link #shutdown()} when all running queries have
     * terminated.
     */
    protected void didShutdown() {
        
    }
    
    /**
     * Do not accept new queries and halt any running binding set chunk tasks.
     */
    public void shutdownNow() {
        
        shutdown = true;
        
        // stop the query engine.
        final Future<?> f = engineFuture.get();
        if (f != null)
            f.cancel(true/* mayInterruptIfRunning */);

        // halt any running queries.
        for(RunningQuery q : runningQueries.values()) {
            
            q.cancel(true/*mayInterruptIfRunning*/);
            
        }

    }

    /**
     * The query is no longer running. Resources associated with the query
     * should be released.
     * 
     * @todo A race is possible where a query is cancelled on a node where the
     *       node receives notice to start the query after the cancelled message
     *       has arrived. To avoid having such queries linger, we should have a
     *       a concurrent hash set with an approximate LRU policy containing the
     *       identifiers for queries which have been cancelled, possibly paired
     *       with the cause (null if normal execution). That will let us handle
     *       any reasonable concurrent indeterminism between cancel and start
     *       notices for a query.
     *       <p>
     *       Another way in which this might be addressed is by involving the
     *       client each time a query start is propagated to a node. If we
     *       notify the client that the query will start on the node first, then
     *       the client can always issue the cancel notices [unless the client
     *       dies, in which case we still want to kill the query which could be
     *       done based on a service disappearing from a jini registry or
     *       zookeeper.]
     */
    protected void halt(final RunningQuery q) {

        // remove from the set of running queries.
        runningQueries.remove(q.getQueryId(), q);
        
        if (log.isInfoEnabled())
            log.info("Removed entry for query: " + q.getQueryId());

    }
    
    /*
     * IQueryPeer
     */
    
    public void declareQuery(final IQueryDecl queryDecl) {
        
        throw new UnsupportedOperationException();
        
    }
    
    public void bufferReady(IChunkMessage<IBindingSet> msg) {

        throw new UnsupportedOperationException();

    }
    
    /*
     * IQueryClient
     */

//    public BOp getQuery(final long queryId) throws RemoteException {
//        
//        final RunningQuery q = runningQueries.get(queryId);
//        
//        if (q != null) {
//            
//            return q.getQuery();
//        
//        }
//        
//        return null;
//        
//    }

    public void startOp(final StartOpMessage msg) throws RemoteException {
        
        final RunningQuery q = runningQueries.get(msg.queryId);
        
        if (q != null) {
        
            q.startOp(msg);
            
        }

    }

    public void haltOp(final HaltOpMessage msg) throws RemoteException {
        
        final RunningQuery q = runningQueries.get(msg.queryId);
        
        if (q != null) {
            
            q.haltOp(msg);
            
        }
        
    }

    /**
     * Evaluate a query which visits {@link IBindingSet}s, such as a join. This
     * node will serve as the controller for the query.
     * 
     * @param queryId
     *            The unique identifier for the query.
     * @param query
     *            The query to evaluate.
     * 
     * @return An iterator visiting {@link IBindingSet}s which result from
     *         evaluating the query.
     * @param msg
     *            A message providing access to the initial {@link IBindingSet
     *            binding set(s)} used to begin query evaluation.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     * @throws RemoteException
     * 
     *             FIXME The test suites need to be modified to create a local
     *             {@link FederatedQueryEngine} object which fronts for an
     *             {@link IIndexManager} which is local to the client - not on a
     *             data service at all. This is necessary in order for the unit
     *             test (or application code) to directly access the
     *             RunningQuery reference, which is needed to use get() (to wait
     *             for the query), iterator() (to drain the query), etc.
     *             <p>
     *             This will also give us a place to hang query-local resources
     *             on the client.
     *             <p>
     *             This has to be a {@link FederatedQueryEngine} because it
     *             needs to talk to a federation. There should be nothing DS
     *             specific about the {@link FederatedQueryEngine}.
     */
    public RunningQuery eval(final long queryId,
            final BindingSetPipelineOp query,
            final IChunkMessage<IBindingSet> msg) throws Exception {

        if (query == null)
            throw new IllegalArgumentException();

        if (msg == null)
            throw new IllegalArgumentException();

        if (queryId != msg.getQueryId()) // @todo use equals() to compare UUIDs.
            throw new IllegalArgumentException();

        final RunningQuery runningQuery = newRunningQuery(this, queryId,
                true/* controller */, this/* clientProxy */, query);

        final long timeout = query.getProperty(BOp.Annotations.TIMEOUT,
                BOp.Annotations.DEFAULT_TIMEOUT);

        if (timeout < 0)
            throw new IllegalArgumentException(BOp.Annotations.TIMEOUT);

        if (timeout != Long.MAX_VALUE) {

            // Compute the deadline (may overflow if timeout is very large).
            final long deadline = System.currentTimeMillis() + timeout;

            if (deadline > 0) {
                /*
                 * Impose a deadline on the query.
                 */
                runningQuery.setDeadline(deadline);

            }

        }

        assertRunning();

        putRunningQuery(queryId, runningQuery);

        runningQuery.startQuery(msg);
        
        return runningQuery;

    }

    /**
     * Return the {@link RunningQuery} associated with that query identifier.
     * 
     * @param queryId
     *            The query identifier.
     *            
     * @return The {@link RunningQuery} -or- <code>null</code> if there is no
     *         query associated with that query identifier.
     */
    protected RunningQuery getRunningQuery(final long queryId) {

        return runningQueries.get(queryId);

    }

    public BindingSetPipelineOp getQuery(final long queryId) {
     
        final RunningQuery q = getRunningQuery(queryId);
        
        if (q == null)
            throw new IllegalArgumentException();
        
        return q.getQuery();
        
    }
    
    /**
     * Places the {@link RunningQuery} object into the internal map.
     * 
     * @param queryId
     *            The query identifier.
     * @param runningQuery
     *            The {@link RunningQuery}.
     */
    protected void putRunningQuery(final long queryId,
            final RunningQuery runningQuery) {

        if (runningQuery == null)
            throw new IllegalArgumentException();
        
        runningQueries.put(queryId, runningQuery);

    }
    
    /**
     * Factory for {@link RunningQuery}s.
     */
    protected RunningQuery newRunningQuery(final QueryEngine queryEngine,
            final long queryId, final boolean controller,
            final IQueryClient clientProxy, final BindingSetPipelineOp query) {

        return new RunningQuery(this, queryId, true/* controller */,
                this/* clientProxy */, query);

    }

}
