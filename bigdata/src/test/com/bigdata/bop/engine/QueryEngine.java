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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.ICloseableIterator;

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
 * @todo Approach query evaluation piecemeal. Start with predicates (access
 *       paths), joins, and a DHT filter on a predicate. That is nearly
 *       everything anyway. The main remaining pieces for query are distinct
 *       solutions, sorting. Mutation (constructing and sending elements to
 *       shards) and closure (running a program to a fixed point or a limiting
 *       number of iterations) are the remaining key pieces.
 *       <p>
 *       Initially use {@link Callable}s for binding set chunks. That will still
 *       limit the #of threads. Once we prove out the approach we can begin to
 *       write new operators based on a fork/join decomposition.
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

//    public static class Config {
//        
//        public int nIOThreads = 10;
//
//        public int maxBuffers = (int) Runtime.getRuntime().maxMemory() / 2
//                / DirectBufferPool.INSTANCE.getBufferCapacity();
//        
//    }
  
    private final IBigdataFederation<?> fed;
    
    /**
     * Access to the indices.
     * <p>
     * Note: You MUST NOT use unisolated indices without obtaining the necessary
     * locks. The {@link QueryEngine} is intended to run only against committed
     * index views for which no locks are required.
     */
    private final IIndexManager indexManager;

    /**
     * A service used to expose {@link ByteBuffer}s and managed index resources
     * for transfer to remote services in support of distributed query
     * evaluation.
     * 
     * @todo Relayer the {@link QueryEngine} w/o a buffer service for standalone
     *       query and w/ for scale-out query.
     */
    private final ManagedBufferService bufferService;
    
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
     * A chunk of intermediate results which are ready to be consumed by some
     * {@link BOp} in a specific query.
     */
    public static class BindingSetChunk {

        /**
         * The target {@link BOp}.
         */
        final int bopId;

        /**
         * The index partition which is being targetted for that {@link BOp}.
         */
        final int partitionId;
        
        /**
         * The binding sets to be consumed by that {@link BOp}.
         */
        final IAsynchronousIterator<IBindingSet[]> source;

        public BindingSetChunk(final int bopId, final int partitionId,
                final IAsynchronousIterator<IBindingSet[]> source) {
            if (source == null)
                throw new IllegalArgumentException();
            this.bopId = bopId;
            this.partitionId = partitionId;
            this.source = source;
        }
       
    }

    /**
     * Metadata about running queries.
     * 
     * @todo Cache any resources materialized for the query on this node (e.g.,
     *       temporary graphs materialized from a peer or the client). A bop
     *       should be able to demand those data from the cache and otherwise
     *       have them be materialized.
     * 
     * @todo metadata for priority queues (e.g., time remaining or priority).
     *       [metadata about resource allocations is part of the query plan.]
     * 
     * @todo HA aspects of running queries?
     * 
     * @todo Cancelled queries must reject or drop new chunks, etc. Halted
     *       queries must release all of their resources.
     */
    public static class RunningQuery<V extends BOpStats> implements Future<V> {

        /**
         * The run state of the query and the result of the computation iff it
         * completes execution normally (without being interrupted, cancelled,
         * etc).
         */
        final private Haltable<V> future = new Haltable<V>();

        /**
         * The runtime statistics for the query.
         * 
         * @todo This has to be per-{@link BOp}.
         */
        @SuppressWarnings("unchecked")
        final private V stats = (V) new BOpStats();
        
        /**
         * The class executing the query on this node.
         */
        final QueryEngine queryEngine;
        
        /** The unique identifier for this query. */
        final long queryId;

        /**
         * The timestamp or transaction identifier against which the query 
         * is reading.
         */
        final long readTimestamp;

        /**
         * The timestamp or transaction identifier against which the query 
         * is writing.
         */
        final long writeTimestamp;
        
        /**
         * The timestamp when the query was accepted by this node (ns).
         * 
         * @todo add a [deadline] field, which is when the query is due.
         */
        final long begin;

        /**
         * The client executing this query.
         */
        final IQueryClient clientProxy;
        
        /** The query iff materialized on this node. */
        final AtomicReference<BOp> queryRef;

        /**
         * The buffer used for the overall output of the query pipeline.
         * 
         * @todo How does the pipeline get attached to this buffer? Via a
         *       special operator?  Or do we just target the coordinating
         *       {@link QueryEngine} as the sink of the last operator so
         *       we can use NIO transfers?
         */
        final IBlockingBuffer<IBindingSet[]> queryBuffer;
        
        /**
         * A map associating resources with running queries. When a query halts,
         * the resources listed in its resource map are released. Resources can
         * include {@link ByteBuffer}s backing either incoming or outgoing
         * {@link BindingSetChunk}s, temporary files associated with the query,
         * hash tables, etc.
         * 
         * @todo only use the values in the map for transient objects, such as a
         *       hash table which is not backed by the disk. For
         *       {@link ByteBuffer}s we want to make the references go through
         *       the {@link BufferService}. For files, through the
         *       {@link ResourceManager}.
         * 
         * @todo We need to track the resources in use by the query so they can
         *       be released when the query terminates. This includes: buffers;
         *       joins for which there is a chunk of binding sets that are
         *       currently being executed; downstream joins (they depend on the
         *       source joins to notify them when they are complete in order to
         *       decide their own termination condition); local hash tables
         *       which are part of a DHT (especially when they are persistent);
         *       buffers and disk resources allocated to N-way merge sorts, etc.
         * 
         * @todo The set of buffers having data which has been accepted for this
         *       query.
         * 
         * @todo The set of buffers having data which has been generated for
         *       this query.
         * 
         * @todo The counter for each open join of the #of active sources. This
         *       must be coordinated with the client to decide when a join is
         *       done. This decision determines when the sinks of the join will
         *       be notified that a given source is done and hence when the sink
         *       joins will decide that they are done. [Think this through more
         *       in terms of the client coordination for optional gotos.]
         */
        private final ConcurrentHashMap<UUID, Object> resourceMap = new ConcurrentHashMap<UUID, Object>();

        /**
         * The chunks available for immediate processing.
         */
        private final BlockingQueue<BindingSetChunk> chunksIn = new LinkedBlockingDeque<BindingSetChunk>();

        /**
         * The chunks generated by this query.
         * 
         * @todo remove chucks from this queue when they are consumed, whether
         *       by a local process or by transferring the data to a remote
         *       service. When the data are transferred from a managed
         *       {@link ByteBuffer} to a remote service, release the
         *       {@link ByteBuffer} back to the {@link BufferService}.
         */
        private final BlockingQueue<BindingSetChunk> chunksOut = new LinkedBlockingDeque<BindingSetChunk>();

        /**
         * An index from the {@link BOp.Annotations#BOP_ID} to the {@link BOp}.
         */
        private final Map<Integer,BOp> bopIndex;

        /**
         * A collection of the currently executing futures.  {@link Future}s are
         * added to this collection
         */
        private final ConcurrentHashMap<Future<?>,Future<?>> futures = new ConcurrentHashMap<Future<?>, Future<?>>();
        
        /**
         * 
         * @param queryId
         * @param begin
         * @param clientProxy 
         * @param query
         *            The query (optional).
         */
        public RunningQuery(final QueryEngine queryEngine, final long queryId,
                final long readTimestamp, final long writeTimestamp,
                final long begin, final IQueryClient clientProxy,
                final BOp query,
                final IBlockingBuffer<IBindingSet[]> queryBuffer) {
            this.queryEngine = queryEngine;
            this.queryId = queryId;
            this.readTimestamp = readTimestamp;
            this.writeTimestamp = writeTimestamp;
            this.begin = begin;
            this.clientProxy = clientProxy;
            this.queryRef = new AtomicReference<BOp>(query);
            this.queryBuffer = queryBuffer;
            this.bopIndex = BOpUtility.getIndex(query);
        }

        /**
         * Make a chunk of binding sets available for consumption by the query.
         * 
         * @param chunk
         *            The chunk.
         */
        public void add(final BindingSetChunk chunk) {
            if (chunk == null)
                throw new IllegalArgumentException();
            future.halted();
            chunksIn.add(chunk);
            queryEngine.priorityQueue.add(this);
        }

        /**
         * Return the current statistics for the query.
         */
        public BOpStats getStats() {
            return stats;
        }

        /**
         * Return a {@link FutureTask} which will consume the binding set chunk.
         * 
         * @param chunk
         * 
         *            FIXME The chunk task should notice if the {@link Haltable}
         *            on the {@link RunningQuery} is halted and should terminate
         *            eagerly. There can be more than one chunk task running at
         *            a time for the same query and even for the same
         *            {@link BOp} for a given query. We need to keep those
         *            {@link Future}s in a collection so we can cancel them if
         *            the query is halted.
         */
        @SuppressWarnings("unchecked")
        protected Future<Void> newChunkTask(final BindingSetChunk chunk) {
            /*
             * Look up the BOp in the index, create the BOpContext for that BOp,
             * and return the value returned by BOp.eval(context).
             * 
             * @todo We have to provide for the sink, which can be a backed by
             * one, or many, NIO buffers for high volume query and which will be
             * just a transient blocking buffer otherwise.
             * 
             * @todo When eval of that chunk is done, the sink gets wrapped as a
             * chunk for the next bop (by its bopId) and submitted back to the
             * query engine (in scale-out, it gets mapped over the shards or
             * nodes).
             * 
             * @todo If eval of the chunk fails, halt() the query.
             */
            final BOp bop = bopIndex.get(chunk.bopId);
            if (bop == null) {
                throw new NoSuchBOpException(chunk.bopId);
            }
            if (!(bop instanceof PipelineOp<?>)) {
                throw new UnsupportedOperationException(bop.getClass()
                        .getName());
            }
            final IBlockingBuffer<?> sink = ((PipelineOp<?>) bop).newBuffer();
            final Integer altSinkId = (Integer) bop
                    .getProperty(BindingSetPipelineOp.Annotations.ALT_SINK_REF);
            if (altSinkId != null && !bopIndex.containsKey(altSinkId)) {
                throw new NoSuchBOpException(altSinkId);
            }
            final IBlockingBuffer<?> sink2 = altSinkId == null ? null
                    : ((PipelineOp<?>) bop).newBuffer();
            final BOpContext context = new BOpContext(queryEngine.fed,
                    queryEngine.indexManager, readTimestamp, writeTimestamp,
                    chunk.partitionId, stats, chunk.source, sink, sink2);
            return ((PipelineOp)bop).eval(context);
        }
        
        /**
         * Return an iterator which will drain the solutions from the query. The
         * query will be cancelled if the iterator is
         * {@link ICloseableIterator#close() closed}.
         * 
         * @return
         * 
         * @todo Do all queries produce solutions (mutation operations might
         *       return a mutation count, but they do not return solutions).
         */
        public IChunkedIterator<IBindingSet> iterator() {
            throw new UnsupportedOperationException();
        }

        /*
         * Future
         * 
         * Note: This is implemented using delegation to the Haltable so we can
         * hook various methods in order to clean up the state of a completed
         * query.
         */

        final public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        final public V get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        final public V get(long arg0, TimeUnit arg1)
                throws InterruptedException, ExecutionException,
                TimeoutException {
            return future.get(arg0, arg1);
        }

        final public boolean isCancelled() {
            return future.isCancelled();
        }

        final public boolean isDone() {
            return future.isDone();
        }

    }

    /**
     * The currently executing queries.
     * <p>
     * Queries may be in any state, including queries whose {@link BOp} has not
     * been materialized, queries which have been materialized but for which no
     * binding sets have been submitted, queries for which binding sets have
     * been submitted but not yet retrieved, queries which have binding sets
     * awaiting execution, queries which are actively executing a {@link BOp}
     * for some binding set chunk, queries which have generated outputs which
     * have not yet been demanded. Once we receive notice that a query has been
     * cancelled it is removed from this collection.
     * 
     * @todo if a query is halted, it needs to be removed from this collection.
     * 
     * @todo a race is possible where a query is cancelled on a node where the
     *       node receives notice to start the query after the cancelled message
     *       has arrived. to avoid having such queries linger, we should have a
     *       a concurrent hash set with an approximate LRU policy containing the
     *       identifiers for queries which have been cancelled, possibly paired
     *       with the cause (null if normal execution). That will let us handle
     *       any reasonable concurrent indeterminism between cancel and start
     *       notices for a query.
     *       <p>
     *       another way in which this might be addressed in involving the
     *       client each time a query start is propagated to a node. if we
     *       notify the client that the query will start on the node first, then
     *       the client can always issue the cancel notices [unless the client
     *       dies, in which case we still want to kill the query which could be
     *       done based on a service disappearing from a jini registry or
     *       zookeeper.]
     *       <p>
     *       This collection should also be the basis for live reporting on the
     *       active queries (their statistics) and administrative operations to
     *       kill a query.
     */
    private final ConcurrentHashMap<Long/* queryId */, RunningQuery<?>> runningQueries = new ConcurrentHashMap<Long, RunningQuery<?>>();

    /**
     * A priority queue of {@link RunningQuery}s having binding set chunks
     * available for consumption.
     */
    private final PriorityBlockingQueue<RunningQuery<?>> priorityQueue = new PriorityBlockingQueue<RunningQuery<?>>();

    /**
     * 
     * @param fed
     *            The federation iff running in scale-out.
     * @param indexManager
     *            The <em>local</em> index manager.
     * @param bufferService
     * @param nThreads
     * 
     * @todo nThreads is not used right now since tasks are being run against
     *       the index manager's executor service.  
     */
    public QueryEngine(final IBigdataFederation<?> fed, final IIndexManager indexManager,
            final ManagedBufferService bufferService, final int nThreads) {

        if (indexManager == null)
            throw new IllegalArgumentException();
        if (bufferService== null)
            throw new IllegalArgumentException();
        
        this.fed = fed; // MAY be null.
        this.indexManager = indexManager;
        this.bufferService = bufferService;
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
        
            indexManager.getExecutorService().execute(ft);
            
        } else {
            
            throw new IllegalStateException("Already running");
            
        }

    }

    /**
     * The {@link Future} for the query engine.
     */
    private final AtomicReference<FutureTask<Void>> engineFuture = new AtomicReference<FutureTask<Void>>();

    /**
     * Volatile flag is set for normal termination.
     */
    private volatile boolean shutdown = false; 
    
    /**
     * Runnable submits chunks available for evaluation against running queries.
     */
    private class QueryEngineTask implements Runnable {
        public void run() {
            try {
                while (true) {
                    final RunningQuery<?> q = priorityQueue.take();
                    if (q.isCancelled())
                        continue;
                    final BindingSetChunk chunk = q.chunksIn.poll();
                    if (chunk == null) {
                        // not expected, but can't do anything without a chunk.
                        if (log.isDebugEnabled())
                            log.debug("Dropping chunk: queryId=" + q.queryId);
                        continue;
                    }
                    if (log.isTraceEnabled())
                        log.trace("Accepted chunk: queryId=" + q.queryId);
                    try {
                        /*
                         * @todo The approach taken by the {@link QueryEngine}
                         * executes one task per pipeline bop per chunk. Outside
                         * of how the tasks are scheduled, this corresponds
                         * closely to the historical pipeline query evaluation.
                         * The other difference is that there is less
                         * opportunity for concatenation of chunks. However,
                         * chunk concatenation could be performed here if we (a)
                         * mark the BindingSetChunk with a flag to indicate when
                         * it has been accepted; and (b) rip through the
                         * incoming chunks for the query for the target bop and
                         * combine them to feed the task. Chunks which have
                         * already been assigned would be dropped when take()
                         * discovers them above. [The chunk combination could
                         * also be done when we output the chunk if the sink has
                         * not been taken, e.g., by combining the chunk into the
                         * same target ByteBuffer, or when we add the chunk to
                         * the RunningQuery.]
                         * 
                         * Note: newChunkTask() returns a Future which is
                         * already executing against a thread pool.
                         * 
                         * @todo Do we need to watch the futures? [Yes, since we
                         * need to mark the RunningQuery as halted if the Future
                         * reports an error.  It would be best to do that using
                         * a hook on the Future.]
                         */
                        final Future<?> ft = q.newChunkTask(chunk);
//                        iopool.execute(ft);
                        if (log.isDebugEnabled())
                            log.debug("Running chunk: queryId=" + q.queryId);
                    } catch (RejectedExecutionException ex) {
                        // shutdown of the pool (should be an unbounded pool).
                        log.warn("Dropping chunk: queryId=" + q.queryId);
                        continue;
                    } catch (Throwable ex) {
                        // log and continue
                        log.error(ex, ex);
                        continue;
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted.");
                return;
            }
        }
    } // QueryEngineTask
    
    /**
     * Do not accept new queries, but run existing queries to completion.
     */
    public void shutdown() throws InterruptedException {
        // normal termination.
        shutdown = true;
    }

    /**
     * Wait until all running queries are done.
     * 
     * @throws InterruptedException
     * 
     * @todo implement awaitTermination
     */
    public void awaitTermination() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Do not accept new queries and halt any running binding set chunk tasks.
     */
    public void shutdownNow() {
        shutdown = true;
        final Future<?> f = engineFuture.get();
        if (f != null)
            f.cancel(true/* mayInterruptIfRunning */);
    }

    /*
     * IQueryPeer
     */
    
    public void bufferReady(IQueryClient clientProxy,
            InetSocketAddress serviceAddr, long queryId, int bopId) {
        // TODO Auto-generated method stub
        
    }
    
    /*
     * IQueryClient
     */

    public BOp getQuery(long queryId) throws RemoteException {
        // TODO Auto-generated method stub
        return null;
    }

    public void startOp(long queryId, int opId, UUID serviceId, int partitionId)
            throws RemoteException {
        // TODO Auto-generated method stub

    }

    public void haltOp(long queryId, int opId, UUID serviceId, int partitionId,
            Throwable cause) throws RemoteException {
        // TODO Auto-generated method stub
        
    }

//    public IChunkedIterator<?> eval(final long queryId, final long timestamp,
//            final BOp query) throws Exception {
//
//        runningQueries.put(queryId, new RunningQuery(queryId,
//                System.nanoTime()/* begin */, this/* clientProxy */, query,
//                null/* source */));
//
//        return null;
//
//    }

    /**
     * Evaluate a query which visits {@link IBindingSet}s, such as a join. This
     * node will serve as the controller for the query.
     * 
     * @param queryId
     *            The unique identifier for the query.
     * @param readTimestamp
     *            The timestamp or transaction against which the query will run.
     * @param writeTimestamp
     *            The timestamp or transaction against which the query will
     *            write.
     * @param query
     *            The query to evaluate.
     * 
     * @return An iterator visiting {@link IBindingSet}s which result from
     *         evaluating the query.
     * 
     * @throws Exception
     * 
     * @todo Consider elevating the read/write timestamps into the query plan as
     *       annotations. Closure would then rewrite the query plan for each
     *       pass, replacing the readTimestamp with the new read-behind
     *       timestamp.
     */
    public RunningQuery<?> eval(final long queryId, final long readTimestamp,
            final long writeTimestamp, final BindingSetPipelineOp query)
            throws Exception {

        if (query == null)
            throw new IllegalArgumentException();
        
        @SuppressWarnings("unchecked")
        final RunningQuery<?> runningQuery = new RunningQuery(this, queryId,
                readTimestamp, writeTimestamp, System.nanoTime()/* begin */,
                this/* clientProxy */, query, newQueryBuffer(query));

        runningQueries.put(queryId, runningQuery);

        return runningQuery;

    }

    /**
     * 
     * @todo if the top bop is an operation which writes on the database then it
     *       should swallow the binding sets from the pipeline and we should be
     *       able to pass along a <code>null</code> query buffer.
     * 
     * @todo in scale-out, must return a proxy for the query buffer either here
     *       or when the query is sent along to another node for evaluation.
     *       <p>
     *       Actually, it would be nice if we could reuse the same NIO transfer
     *       of {@link ByteBuffer}s to move the final results back to the client
     *       rather than using a proxy object for the query buffer.
     */
    protected IBlockingBuffer<IBindingSet[]> newQueryBuffer(
            final BindingSetPipelineOp query) {

        return query.newBuffer();

    }

}
