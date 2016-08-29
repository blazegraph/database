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
/*
 * Created on Aug 21, 2010
 */

package com.bigdata.bop.engine;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.fed.FederatedQueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.view.FusedView;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.sail.webapp.client.HttpClientConfigurator;
import com.bigdata.resources.IndexManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.service.geospatial.GeoSpatialCounters;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.IHaltable;

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
public class QueryEngine implements IQueryPeer, IQueryClient, ICounterSetAccess {

    private final static transient Logger log = Logger
            .getLogger(QueryEngine.class);

    /**
     * Error message used if a query is not running.
     */
    protected static final transient String ERR_QUERY_NOT_RUNNING = "Query is not running:";

    /**
     * Annotations understood by the {@link QueryEngine}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Annotations extends PipelineOp.Annotations {

        /**
         * Annotation may be used to impose a specific {@link UUID} for a query.
         * This may be used by an external process such that it can then use
         * {@link QueryEngine#getRunningQuery(UUID)} to gain access to the
         * running query instance. It is an error if there is a query already
         * running with the same {@link UUID}.
         */
        String QUERY_ID = QueryEngine.class.getName() + ".queryId";
        
        /**
         * The name of the {@link IRunningQuery} implementation class which will
         * be used to evaluate a query marked by this annotation (optional). The
         * specified class MUST implement {@link IRunningQuery} and MUST have a
         * constructor with the following signature:
         * 
         * <pre>
         * public MyRunningQuery(QueryEngine queryEngine, UUID queryId,
         *             boolean controller, IQueryClient clientProxy,
         *             PipelineOp query, IChunkMessage<IBindingSet> realSource)
         * </pre>
         * 
         * Note that classes derived from {@link QueryEngine} may override
         * {@link QueryEngine#newRunningQuery(QueryEngine, UUID, boolean, IQueryClient, PipelineOp, IChunkMessage, IRunningQuery)}
         * in which case they might not support this option.
         */
        String RUNNING_QUERY_CLASS = QueryEngine.class.getName()
                + ".runningQueryClass";

//        String DEFAULT_RUNNING_QUERY_CLASS = StandaloneChainedRunningQuery.class.getName();
        String DEFAULT_RUNNING_QUERY_CLASS = ChunkedRunningQuery.class.getName();

        /**
         * The class used to map binding sets across the federation or transition
         * them from IBindingSet[]s to {@link IChunkMessage}s stored on the native
         * heap.
         * 
         * @see BLZG-533 Vector query engine on native heap.
         */
        String CHUNK_HANDLER = QueryEngine.class.getName() + ".chunkHandler";
        
    }

    /**
     * Return a {@link CounterSet} which reports various statistics for the
     * {@link QueryEngine}.
     */
    @Override
    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        // Note: This counter is not otherwise tracked.
        counters.deadlineQueueSize.set(deadlineQueue.size());
        
        // global counters.
        root.attach(counters.getCounters());

        // geospatial counters
        final CounterSet geoSpatial = root.makePath("GeoSpatial");
        geoSpatial.attach(geoSpatialCounters.getCounters());
        
//        // counters per tagged query group.
//        {
//
//            final CounterSet groups = root.makePath("groups");
//
//            final Iterator<Map.Entry<String, Counters>> itr = groupCounters
//                    .entrySet().iterator();
//
//            while (itr.hasNext()) {
//
//                final Map.Entry<String, Counters> e = itr.next();
//
//                final String tag = e.getKey();
//
//                final Counters counters = e.getValue();
//
//                // Note: path component may not be empty!
//                groups.makePath(tag == null | tag.length() == 0 ? "None" : tag)
//                        .attach(counters.getCounters());
//
//            }
//
//        }

        return root;

    }
    
    /**
     * Counters at the global level.
     */
    final protected QueryEngineCounters counters = newCounters();
    
    /**
     * GeoSpatial counters
     */
    final protected GeoSpatialCounters geoSpatialCounters = newGeoSpatialCounters();

//    /**
//     * Statistics for queries which are "tagged" so we can recognize their
//     * instances as members of some group.
//     */
//    final protected ConcurrentHashMap<String/* groupId */, Counters> groupCounters = new ConcurrentHashMap<String, Counters>();

//    /**
//     * Factory for {@link Counters} instances associated with a query group. A
//     * query is marked as a member of a group using {@link QueryHints#TAG}. This
//     * is typically used to mark queries which are instances of the same query
//     * template.
//     * 
//     * @param tag
//     *            The tag identifying a query group.
//     * 
//     * @return The {@link Counters} for that query group.
//     * 
//     * @throws IllegalArgumentException
//     *             if the argument is <code>null</code>.
//     */
//    protected Counters getCounters(final String tag) {
//
//        if(tag == null)
//            throw new IllegalArgumentException();
//        
//        Counters c = groupCounters.get(tag);
//
//        if (c == null) {
//
//            c = new Counters();
//
//            final Counters tmp = groupCounters.putIfAbsent(tag, c);
//
//            if (tmp != null) {
//
//                // someone else won the data race.
//                c = tmp;
//
//            }
//
//        }
//
//        return c;
//
//    }

    /**
     * Extension hook for new {@link QueryEngineCounters} instances.
     */
    protected QueryEngineCounters newCounters() {
        
        return new QueryEngineCounters();
        
    }
    
    /**
     * Extension hook for new {@link GeoSpatialCounters} instances.
     */
    protected GeoSpatialCounters newGeoSpatialCounters() {
       
       return new GeoSpatialCounters();
    }
    
    /**
     * The {@link QueryEngineCounters} object for this {@link QueryEngine}.
     */
    protected QueryEngineCounters getQueryEngineCounters() {
        
        return counters;
        
    }
    
    /**
     * The {@link QueryEngineCounters} object for this {@link QueryEngine}.
     */
    public GeoSpatialCounters getGeoSpatialCounters() {
        
        return geoSpatialCounters;
        
    }
    
    /**
     * Access to the <strong>local</strong> indices.
     * <p>
     * Note: You MUST NOT use unisolated indices without obtaining the necessary
     * locks. The {@link QueryEngine} is intended to run only against committed
     * index views for which no locks are required.
     */
    private final IIndexManager localIndexManager;

    /**
	 * The {@link HttpClient} is used to make remote HTTP connections (SPARQL
	 * SERVICE call joins).
	 */
    private final AtomicReference<HttpClient> clientConnectionManagerRef = new AtomicReference<HttpClient>();
    
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

    @Override
    public UUID getServiceUUID() {

        return ((IRawStore) localIndexManager).getUUID();
        
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
     * Return the {@link ConcurrencyManager} for the {@link #getIndexManager()
     * local index manager}.
     */
    public ConcurrencyManager getConcurrencyManager() {
        
        return ((Journal) localIndexManager).getConcurrencyManager();

    }
    
    /**
     * The RMI proxy for this {@link QueryEngine} when used as a query controller.
     * The default implementation returns <i>this</i>.
     */
    public IQueryClient getProxy() {

        return this;
        
    }
    
    /**
	 * Return the {@link HttpClient} used to make remote SERVICE call requests.
	 */
    public HttpClient getClientConnectionManager() {

    	HttpClient cm = clientConnectionManagerRef.get();
        
        if (cm == null) {

            // Note: Deliberate use of the ref as a monitor object.
            synchronized (clientConnectionManagerRef) {
            
                cm = clientConnectionManagerRef.get();
                
                if (cm == null) {

                    if (!isRunning()) {
                    
                        /*
                         * Shutdown.
                         */
                        
                        throw new IllegalStateException();

                    }
                    
                    /*
                     * Lazy instantiation.
                     */
                    
                    clientConnectionManagerRef
                            .set(cm = HttpClientConfigurator
                                    .getInstance().newInstance());

                }
                
            }
            
        }
        
        return cm;
        
    }
    
    /**
     * Return <code>true</code> iff running against an
     * {@link IBigdataFederation}.
     */
    public boolean isScaleOut() {

        return false;

    }

    /**
     * Lock used to guard register / halt of a query.
     */
    private final ReentrantLock lock = new ReentrantLock();
    
    /**
     * Signaled when no queries are running.
     */
    private final Condition nothingRunning = lock.newCondition();
    
    /**
     * The currently executing queries.
     */
    private final ConcurrentHashMap<UUID/* queryId */, AbstractRunningQuery> runningQueries = new ConcurrentHashMap<UUID, AbstractRunningQuery>();

    /**
     * LRU cache used to handle problems with asynchronous termination of
     * running queries.
     * <p>
     * Note: Holding onto the query references here might pin memory retained by
     * those queries. However, all we really need is the Haltable (Future) of
     * that query in this map.
     * 
     * @todo This should not be much of a hot spot even though it is not thread
     *       safe but the synchronized() call could force cache stalls anyway. A
     *       concurrent hash map with an approximate LRU access policy might be
     *       a better choice.
     * 
     * @todo The maximum cache capacity here is a SWAG. It should be large
     *       enough that we can not have a false cache miss on a system which is
     *       heavily loaded by a bunch of light queries.
     */
    private final LinkedHashMap<UUID, IHaltable<Void>> doneQueries = new LinkedHashMap<UUID,IHaltable<Void>>(
            16/* initialCapacity */, .75f/* loadFactor */, true/* accessOrder */) {

        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<UUID, IHaltable<Void>> eldest) {

            return size() > 100/* maximumCacheCapacity */;

        }
    };

    /**
     * A high concurrency cache operating as an LRU designed to close a data
     * race between the asynchronous start of a submitted query or update
     * operation and the explicit asynchronous CANCEL of that operation using
     * its pre-assigned {@link UUID}.
     * <p>
     * When a CANCEL request is received, we probe both the
     * {@link #runningQueries} and the {@link #doneQueries}. If no operation is
     * associated with that request, then we probe the running UPDATE
     * operations. Finally, if no such operation was discovered, then the
     * {@link UUID} of the operation to be cancelled is entered into this
     * collection.
     * <p>
     * Before a query starts, we consult the {@link #pendingCancelLRU}. If the
     * {@link UUID} of the query is discovered, then the query is cancelled
     * rather than run.
     * <p>
     * Note: The capacity of the backing hard reference queue is quite small.
     * {@link UUID}s are only entered into this collection if a CANCEL request
     * is asynchronously received either (a) before; or (b) long enough after a
     * query or update is executed that is not not found in either the running
     * queries map or the recently done queries map.
     * 
     * TODO There are some cases that are not covered by this. First, we do not
     * have {@link UUID}s for all REST API methods and thus they can not all be
     * cancelled. If we allowed an HTTP header to specify the UUID of the
     * request, then we could associate a UUID with all requests. The ongoing
     * refactor to support clean interrupt of NSS requests (#753) and the
     * ongoing refactor to support concurrent unisolated operations against the
     * same journal (#566) will provide us with the mechanisms to identify all
     * such operations so we can check their assigned UUIDs and cancel them when
     * requested.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/899"> REST API Query
     *      Cancellation </a>
     * @see <a href="http://trac.blazegraph.com/ticket/753"> HA doLocalAbort()
     *      should interrupt NSS requests and AbstractTasks </a>
     * @see <a href="http://trac.blazegraph.com/ticket/566"> Concurrent unisolated
     *      operations against multiple KBs on the same Journal </a>
     * @see #startEval(UUID, PipelineOp, Map, IChunkMessage)
     */
    private final ConcurrentWeakValueCache<UUID, UUID> pendingCancelLRU = new ConcurrentWeakValueCache<>(
            50/* queueCapacity (SWAG, but see above) */);

    /**
     * Add a query {@link UUID} to the LRU of query identifiers for which we
     * have received a CANCEL request, but were unable to find a running QUERY,
     * recently done query, or running UPDATE request.
     * 
     * @param queryId
     *            The UUID of the operation to be cancelled.
     *            
     * @see <a href="http://trac.blazegraph.com/ticket/899"> REST API Query
     *      Cancellation </a>
     */
    public void addPendingCancel(final UUID queryId) {

        if (queryId == null)
            throw new IllegalArgumentException();

        pendingCancelLRU.putIfAbsent(queryId, queryId);

    }
    
    /**
     * Return <code>true</code> iff the {@link UUID} is the the collection of
     * {@link UUID}s for which we have already received a CANCEL request.
     * <p>
     * Note: The {@link UUID} is removed from the pending cancel collection as a
     * side-effect.
     * 
     * @param queryId
     *            The {@link UUID} of the operation.
     * 
     * @return <code>true</code> if that operation has already been marked for
     *         cancellation.
     */
    public boolean pendingCancel(final UUID queryId) {

        if (queryId == null)
            throw new IllegalArgumentException();
        
        return pendingCancelLRU.remove(queryId) != null;

    }
    
    /**
     * A queue of {@link ChunkedRunningQuery}s having binding set chunks available for
     * consumption.
     * 
     * @todo Handle priority for selective queries based on the time remaining
     *       until the timeout.
     *       <p>
     *       Handle priority for unselective queries based on the order in which
     *       they are submitted?
     *       <p>
     *       Be careful when testing out a {@link PriorityBlockingQueue} here.
     *       First, that collection is intrinsically bounded (it is backed by an
     *       array) so it will BLOCK under heavy load and could be expected to
     *       have some resize costs if the queue size becomes too large. Second,
     *       either {@link ChunkedRunningQuery} needs to implement an appropriate
     *       {@link Comparator} or we need to pass one into the constructor for
     *       the queue.
     */
    final private BlockingQueue<AbstractRunningQuery> priorityQueue = new LinkedBlockingQueue<AbstractRunningQuery>();
//    final private BlockingQueue<RunningQuery> priorityQueue = new PriorityBlockingQueue<RunningQuery>(
//            );

    /**
     * A queue arranged in order of increasing deadline times. Only queries with
     * an explicit deadline are added to this priority queue. The head of the
     * queue contains the query whose deadline will expire soonest. A thread can
     * thus poll the head of the queue to determine whether the deadline would
     * have passed. Such queries can be removed from the queue and their
     * {@link AbstractRunningQuery#checkDeadline()} method invoked to force
     * their timely termination.
     * <p>
     * {@link AbstractRunningQuery#startOp(IStartOpMessage)} and
     * {@link AbstractRunningQuery#haltOp(IHaltOpMessage)} check to see if the
     * deadline for a query has expired. However, those methods are only invoked
     * when a query plan operator starts and halts. In cases where the query is
     * compute bound within a single operator (e.g., ORDER BY or an unconstrained
     * cross-product JOIN), the query will not be checked for termination. This
     * priority queue is used to ensure that the query deadline is tested even
     * though it may be in a compute bound operator.
     * <p>
     * If the deadline has expired, {@link IRunningQuery#cancel(boolean)} will
     * be invoked. In order for a compute bound operator to terminate in a
     * timely fashion, it MUST periodically test {@link Thread#interrupted()}.
     * <p>
     * Note: The deadline of a query may be set at most once. Thus, a query
     * which is entered into the {@link #deadlineQueue} may not have its
     * deadline modified. This means that we do not have to search the priority
     * queue for an existing reference to the query. It also means that we are
     * able to store an object that wraps the query with a {@link WeakReference}
     * and thus can avoid pinning the query on the heap until its deadline
     * expires. That means that we do not need to remove an entry from the
     * deadline queue each time a query terminates, but we do need to
     * periodically trim the queue to ensure that queries with distant deadlines
     * do not hang around in the queue for long periods of time after their
     * deadline has expired. This can be done by scanning the queue and removing
     * all entries whose {@link WeakReference} has been cleared.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/772">
     *      Query timeout only checked at operator start/stop. </a>
     */
    final private PriorityBlockingQueue<QueryDeadline> deadlineQueue = new PriorityBlockingQueue<QueryDeadline>();

    /**
     * Queries with a deadline that lies significantly in the future can lie
     * around in the priority queue until that deadline is reached if there are
     * other queries in front of them that are not terminated and whose deadline
     * has not be reached. Therefore, periodically, we need to scan the queue
     * and clear out entries for terminated queries. This is done any time the
     * size of the queue is at least this many elements when we examine the
     * queue in {@link #checkDeadlines()}.
     */
    final static private int DEADLINE_QUEUE_SCAN_SIZE = 200;
    
    /**
     * The maximum granularity before we will check the deadline priority queue
     * for queries that need to be terminated because their deadline has
     * expired.
     */
    final static private long DEADLINE_CHECK_MILLIS = 100;
    
    /**
     * Add the query to the deadline priority queue
     * 
     * @exception IllegalArgumentException
     *                if the query deadline has not been set.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/772">
     *      Query timeout only checked at operator start/stop. </a>
     */
    void addQueryToDeadlineQueue(final AbstractRunningQuery query) {

        final long deadline = query.getDeadline();

        if (deadline == Long.MAX_VALUE) {
            /*
             * Do not allow queries with an unbounded deadline into the priority
             * queue.
             */
            throw new IllegalArgumentException();
        }

        final long deadlineNanos = TimeUnit.MILLISECONDS.toNanos(deadline);

        deadlineQueue.add(new QueryDeadline(deadlineNanos, query));

    }

    /**
     * Scan the priority queue of queries with a specified deadline, halting any
     * queries whose deadline has expired.
     */
    static private void checkDeadlines(final long nowNanos,
            final PriorityBlockingQueue<QueryDeadline> deadlineQueue) {
        
        /*
         * While the queue is thread safe, we want at most one thread at a time
         * to be inspecting the queue for queries whose deadlines have expired.
         */
        synchronized (deadlineQueue) {

            /*
             * Check the head of the deadline queue for any queries whose
             * deadline has expired.
             */
            checkHeadOfDeadlineQueue(nowNanos, deadlineQueue);

            if (deadlineQueue.size() > DEADLINE_QUEUE_SCAN_SIZE) {

                /*
                 * Scan the deadline queue, removing entries for expired
                 * queries.
                 */
                scanDeadlineQueue(nowNanos, deadlineQueue);

            }

        }
        
    }

    /**
     * Check the head of the deadline queue for any queries whose deadline has
     * expired.
     */
    static private void checkHeadOfDeadlineQueue(final long nowNanos,
            final PriorityBlockingQueue<QueryDeadline> deadlineQueue) {
        
        QueryDeadline x;

        // remove the element at the head of the queue.
        while ((x = deadlineQueue.poll()) != null) {

            // test for query done or deadline expired.
            if (x.checkDeadline(nowNanos) == null) {

                /*
                 * This query is known to be done. It was removed from the
                 * priority queue above. We need to check the next element in
                 * the priority order to see whether it is also done.
                 */

                continue;

            }

            if (x.deadlineNanos > nowNanos) {

                /*
                 * This query has not yet reached its deadline. That means that
                 * no other query in the deadline queue has reached its
                 * deadline. Therefore we are done for now.
                 */

                // Put the query back on the deadline queue.
                deadlineQueue.add(x);

                break;

            }

        }

    }
    
    /**
     * Queries with a deadline that lies significantly in the future can lie
     * around in the priority queue until that deadline is reached if there are
     * other queries in front of them that are not terminated and whose deadline
     * has not be reached. Therefore, periodically, we need to scan the queue
     * and clear out entries for terminated queries.
     */
    static private void scanDeadlineQueue(final long nowNanos,
            final PriorityBlockingQueue<QueryDeadline> deadlineQueue) {

        final List<QueryDeadline> c = new ArrayList<QueryDeadline>(
                DEADLINE_QUEUE_SCAN_SIZE);

        // drain up to that many elements.
        deadlineQueue.drainTo(c, DEADLINE_QUEUE_SCAN_SIZE);

        int ndropped = 0, nrunning = 0;
        
        for (QueryDeadline x : c) {

            if (x.checkDeadline(nowNanos) != null) {

                // return this query to the deadline queue.
                deadlineQueue.add(x);

                nrunning++;

            } else {
                
                ndropped++;
                
            }

        }
        
        if (log.isInfoEnabled())
            log.info("Scan: threadhold=" + DEADLINE_QUEUE_SCAN_SIZE
                    + ", ndropped=" + ndropped + ", nrunning=" + nrunning
                    + ", deadlineQueueSize=" + deadlineQueue.size());

    }
    
    /**
     * 
     * @param localIndexManager
     *            The <em>local</em> index manager.
     */
    public QueryEngine(final IIndexManager localIndexManager) {

        if (localIndexManager == null)
            throw new IllegalArgumentException();

        this.localIndexManager = localIndexManager;

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

        final FutureTask<Void> ft = new FutureTaskMon<Void>(new QueryEngineTask(
                priorityQueue, deadlineQueue), (Void) null);

        if (engineFuture.compareAndSet(null/* expect */, ft)) {
        
            engineService.set(Executors
                    .newSingleThreadExecutor(new DaemonThreadFactory(
                            QueryEngine.class + ".engineService")));

            engineService.get().execute(ft);

        } else {
            
            throw new IllegalStateException("Already running");
            
        }

    }

    /**
     * {@link QueryEngine}s are used with a singleton pattern managed by the
     * {@link QueryEngineFactory}. They are torn down automatically once they
     * are no longer reachable. This behavior depends on not having any hard
     * references back to the {@link QueryEngine}.
     */
    @Override
    protected void finalize() throws Throwable {
        
        shutdownNow();
        
        super.finalize();
        
    }
    
    /**
     * The service on which we run the query engine.  This is started by {@link #init()}.
     */
    private final AtomicReference<ExecutorService> engineService = new AtomicReference<ExecutorService>();

    /**
     * The {@link Future} for the query engine.  This is set by {@link #init()}.
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

        if (engineFuture.get() == null)
            throw new IllegalStateException("Not initialized.");
        
        if (shutdown)
            throw new IllegalStateException("Shutting down.");

    }

    protected boolean isRunning() {

        return engineFuture.get() != null && !shutdown;

    }
    
    /**
     * Executes the {@link Runnable} on the local {@link IIndexManager}'s
     * {@link ExecutorService}.
     * 
     * @param r
     *            The {@link Runnable}.
     */
    final protected void execute(final Runnable r) {
        
        localIndexManager.getExecutorService().execute(r);
        
    }
    
    /**
     * Runnable submits chunks available for evaluation against running queries.
     * <p>
     * Note: This is a static inner class in order to avoid a hard reference 
     * back to the outer {@link QueryEngine} object.  This makes it possible
     * for the JVM to finalize the {@link QueryEngine} if the application no
     * longer holds a hard reference to it.  The {@link QueryEngine} is then
     * automatically closed from within its finalizer method.
     */
    static private class QueryEngineTask implements Runnable {
        
        final private BlockingQueue<AbstractRunningQuery> priorityQueue;
        final private PriorityBlockingQueue<QueryDeadline> deadlineQueue;

        public QueryEngineTask(
                final BlockingQueue<AbstractRunningQuery> priorityQueue,
                final PriorityBlockingQueue<QueryDeadline> deadlineQueue) {

            if (priorityQueue == null)
                throw new IllegalArgumentException();

            if (deadlineQueue == null)
                throw new IllegalArgumentException();

            this.priorityQueue = priorityQueue;
            
            this.deadlineQueue = deadlineQueue;

        }
        
        @Override
        public void run() {
            if(log.isInfoEnabled())
                log.info("Running: " + this);
            try {
                final long deadline = TimeUnit.MILLISECONDS
                        .toNanos(DEADLINE_CHECK_MILLIS);
                long mark = System.nanoTime();
                long remaining = deadline;
                while (true) {
                    try {
                        //log.warn("Polling deadline queue: remaining="+remaining+", deadlinkCheckMillis="+DEADLINE_CHECK_MILLIS);
                        final AbstractRunningQuery q = priorityQueue.poll(
                                remaining, TimeUnit.NANOSECONDS);
                        final long now = System.nanoTime();
                        if ((remaining = deadline - (now - mark)) < 0) {
                            //log.error("Checking deadline queue");
                            /*
                             * Check for queries whose deadline is expired.
                             *
                             * Note: We only do this every DEADLINE_CHECK_MILLIS
                             * and then reset [mark] and [remaining].
                             *
                             * Note: In queue.pool(), we only wait only up to
                             * the [remaining] time before the next check in
                             * queue.poll().
                             */
                            checkDeadlines(now, deadlineQueue);
                            mark = now;
                            remaining = deadline;
                        }
                        // Consume chunk already on queue for this query.
                        if (q != null && !q.isDone())
                            q.consumeChunk();
                    } catch (InterruptedException e) {
                        /*
                         * Note: Uncomment the stack trace here if you want to
                         * find where the query was interrupted.
                         * 
                         * Note: If you want to find out who interrupted the
                         * query, then you can instrument BlockingBuffer#close()
                         * in PipelineOp#newBuffer(stats).
                         */
                        if (log.isInfoEnabled())
                            log.info("Interrupted."
    //                            ,e
                                );
                        return;
                    } catch (Throwable t) {
                        // log and continue
                        log.error(t, t);
                        continue;
                    }
                } // while(true)
            } finally {
                if (log.isInfoEnabled())
                    log.info("QueryEngineTask is done.");
            }
        }
    } // QueryEngineTask

    /**
     * Add a chunk of intermediate results for consumption by some query. The
     * chunk will be attached to the query and the query will be scheduled for
     * execution.
     * 
     * @param msg
     *            A chunk of intermediate results.
     * 
     * @return <code>true</code> if the chunk was accepted. This will return
     *         <code>false</code> if the query is done (including cancelled) or
     *         the query engine is shutdown. The {@link IChunkMessage} will have
     *         been {@link IChunkMessage#release() released} if it was not
     *         accepted.
     * 
     * @throws IllegalArgumentException
     *             if the chunk is <code>null</code>.
     * @throws IllegalStateException
     *             if the chunk is not materialized.
     */
    protected boolean acceptChunk(final IChunkMessage<IBindingSet> msg) {
        
        if (msg == null)
            throw new IllegalArgumentException();

        if (!msg.isMaterialized())
            throw new IllegalStateException();

        final AbstractRunningQuery q = getRunningQuery(msg.getQueryId());
        
        if(q == null) {
            /*
             * The query is not registered on this node.
             */
            throw new IllegalStateException();
        }
        
        // add chunk to the query's input queue on this node.
        if (!q.acceptChunk(msg)) {
            // query is no longer running.
            msg.release();
            return false;
            
        }

        if(!isRunning()) {
            // query engine is no longer running.
            msg.release();
            return false;
            
        }

        // add query to the engine's task queue.
        priorityQueue.add(q);

        return true;

    }

    /**
     * Shutdown the {@link QueryEngine} (blocking). The {@link QueryEngine} will
     * not accept new queries, but existing queries will run to completion.
     */
    public void shutdown() {

        // normal termination.
        shutdown = true;

        lock.lock();
        try {
            while (!runningQueries.isEmpty()) {
                try {
                    nothingRunning.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            lock.unlock();
        }
        
        // hook for subclasses.
        didShutdown();
        
        // stop the query engine.
        final Future<?> f = engineFuture.get();
        if (f != null) {
            if(log.isInfoEnabled())
                log.info("Cancelling engineFuture: "+this);
            f.cancel(true/* mayInterruptIfRunning */);
        }

        // stop the service on which we ran the query engine.
        final ExecutorService s = engineService.get();
        if (s != null) {
            if(log.isInfoEnabled())
                log.info("Terminating engineService: "+this);
            s.shutdownNow();
        }
        
        final HttpClient cm = clientConnectionManagerRef.get();
        if (cm != null) {
            if (log.isInfoEnabled())
                log.info("Terminating HttpClient: " + this);
            try {
				cm.stop();
			} catch (Exception e) {
				log.error("Problem shutting down HttpClient", e);
			}
        }
        
        // clear the queues
        priorityQueue.clear();
        deadlineQueue.clear();

        // clear references.
        engineFuture.set(null);
        engineService.set(null);
        clientConnectionManagerRef.set(null);
        
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
        
        /*
         * Stop the QueryEngineTask: this is the task that accepts chunks that
         * are available for evaluation and assigns them to the
         * AbstractRunningQuery.
         */
        final Future<?> f = engineFuture.get();
        if (f != null) {
            if (log.isInfoEnabled())
                log.info("Cancelling engineFuture: " + this);
            f.cancel(true/* mayInterruptIfRunning */);
        }
        
        // stop the service on which we ran the QueryEngineTask.
        final ExecutorService s = engineService.get();
        if (s != null) {
            if (log.isInfoEnabled())
                log.info("Terminating engineService: "+this);
            s.shutdownNow();
        }
        
        final HttpClient cm = clientConnectionManagerRef.get();
        if (cm != null) {
            if (log.isInfoEnabled())
                log.info("Terminating HttpClient: " + this);
            try {
				cm.stop();
			} catch (Exception e) {
				log.error("Problem stopping HttpClient", e);
			}
        }

        // halt any running queries.
        for(AbstractRunningQuery q : runningQueries.values()) {
            
            q.cancel(true/*mayInterruptIfRunning*/);
            
        }
        
        // clear the queues
        priorityQueue.clear();
        deadlineQueue.clear();

        // clear references.
        engineFuture.set(null);
        engineService.set(null);
        clientConnectionManagerRef.set(null);
        
    }

    /*
     * IQueryPeer
     */
    
    @Override
    @Deprecated // see IQueryClient
    public void declareQuery(final IQueryDecl queryDecl) throws RemoteException {
        
        throw new UnsupportedOperationException();
        
    }
    
    @Override
    public void bufferReady(final IChunkMessage<IBindingSet> msg) {

        throw new UnsupportedOperationException();

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * The default implementation is a NOP.
     */
    @Override
    public void cancelQuery(final UUID queryId, final Throwable cause) {
        // NOP
    }

    /*
     * IQueryClient
     */
    @Override
    public PipelineOp getQuery(final UUID queryId) {
        
        final AbstractRunningQuery q = getRunningQuery(queryId);
        
        if (q == null)
            throw new IllegalArgumentException();
        
        return q.getQuery();
        
    }

    @Override
    public void startOp(final IStartOpMessage msg) throws RemoteException {
        
        final AbstractRunningQuery q = getRunningQuery(msg.getQueryId());
        
        if (q != null) {
        
            q.startOp(msg);
            
        }

    }

    @Override
    public void haltOp(final IHaltOpMessage msg) throws RemoteException {
        
        final AbstractRunningQuery q = getRunningQuery(msg.getQueryId());
        
        if (q != null) {
            
            q.haltOp(msg);
            
        }
        
    }

//    /**
//     * Return an {@link IAsynchronousIterator} that will read a single, empty
//     * {@link IBindingSet}.
//     */
//    private static ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator() {
//
//        return newBindingSetIterator(new ListBindingSet());
//
//    }

//    /**
//     * Return an {@link IAsynchronousIterator} that will read a single
//     * {@link IBindingSet}.
//     * 
//     * @param bindingSet
//     *            the binding set.
//     */
//    private static ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
//            final IBindingSet bindingSet) {
//
//        return new ThickAsynchronousIterator<IBindingSet[]>(
//                new IBindingSet[][] { new IBindingSet[] { bindingSet } });
//
//    }

//    /**
//     * Return an {@link IAsynchronousIterator} that will read the source
//     * {@link IBindingSet}s.
//     * 
//     * @param bsets
//     *            The source binding sets.
//     */
//    private static ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
//            final IBindingSet[] bsets) {
//     
//        return new ThickAsynchronousIterator<IBindingSet[]>(
//                new IBindingSet[][] { bsets });
//        
//    }
    
    /** Use a random UUID unless the UUID was specified on the query. */
    private static UUID getQueryUUID(final BOp op) {

        return op.getProperty(QueryEngine.Annotations.QUERY_ID,
                UUID.randomUUID());
        
    }

    /**
     * Return the starting point for pipeline evaluation/
     */
    private int getStartId(final BOp op) {

        final BOp startOp = BOpUtility.getPipelineStart(op);

        final int startId = startOp.getId();

        return startId;
        
    }

    private LocalChunkMessage newLocalChunkMessage(final UUID queryId,
            final BOp op, final IBindingSet src) {

        return new LocalChunkMessage(this/* queryEngine */, queryId,
                getStartId(op), -1 /* partitionId */, src);

    }
    
    private LocalChunkMessage newLocalChunkMessage(final UUID queryId,
            final BOp op, final IBindingSet[] src) {

        return new LocalChunkMessage(this/* queryEngine */, queryId,
                getStartId(op), -1 /* partitionId */, src);

    }

    private LocalChunkMessage newLocalChunkMessage(final UUID queryId,
            final BOp op, final IBindingSet[][] src) {

        return new LocalChunkMessage(this/* queryEngine */, queryId,
                getStartId(op), -1 /* partitionId */, src);

    }

//    /**
//     * Return a {@link LocalChunkMessage} for the query wrapping the specified
//     * source.
//     * 
//     * @param queryId
//     *            The query's {@link UUID}.
//     * @param op
//     *            The query.
//     * @param solutionCount
//     *            The #of solutions which can be drained from that source.
//     * @param src
//     *            The source to be wrapped.
//     * 
//     * @return The message.
//     * 
//     * @deprecated We are trying to get the {@link IAsynchronousIterator} out
//     * of the API here.
//     */
//    private LocalChunkMessage newLocalChunkMessage(final UUID queryId,
//            final BOp op, final int solutionCount,
//            final IAsynchronousIterator<IBindingSet[]> src) {
//
//        return new LocalChunkMessage(this/* queryEngine */, queryId,
//                getStartId(op), -1 /* partitionId */, solutionCount, src);
//
//    }
    
    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * 
     * @param query
     *            The query to evaluate.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(final BOp op) throws Exception {
        
        return eval(op, new ListBindingSet());
        
    }

    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * 
     * @param query
     *            The query to evaluate.
     * @param bset
     *            The initial binding set to present.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(final BOp op, final IBindingSet bset)
            throws Exception {

        final UUID queryId = getQueryUUID(op);

        return eval(queryId, (PipelineOp) op, null/* attributes */,
                newLocalChunkMessage(queryId, op, bset));

    }

    /**
     * Note: Used only by the test suite.
     */
    public AbstractRunningQuery eval(final UUID queryId, final BOp op,
            final IBindingSet bset) throws Exception {

        return eval(queryId, (PipelineOp) op, null/* attributes */,
                newLocalChunkMessage(queryId, op, bset));

    }

    /**
     * Note: Used only by the test suite.
     */
    public AbstractRunningQuery eval(final UUID queryId, final BOp op,
            final Map<Object, Object> queryAttributes, final IBindingSet[] bset)
            throws Exception {

        return eval(queryId, (PipelineOp) op, queryAttributes,
                newLocalChunkMessage(queryId, op, bset));

    }

    /**
     * Note: Used only by the test suite.
     */
    public AbstractRunningQuery eval(final UUID queryId, final BOp op,
            final Map<Object, Object> queryAttributes,
            final IBindingSet[][] bset) throws Exception {

        return eval(queryId, (PipelineOp) op, queryAttributes,
                newLocalChunkMessage(queryId, op, bset));

    }

    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * 
     * @param query
     *            The query to evaluate.
     * @param bsets
     *            The initial binding sets to present.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(final BOp op, final IBindingSet[] bsets)
            throws Exception {

        return eval(op, bsets, null/* attributes */);

    }

    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * 
     * @param query
     *            The query to evaluate.
     * @param bsets
     *            The initial binding sets to present.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(final BOp op, final IBindingSet[] bsets,
            final Map<Object, Object> attribs) throws Exception {

        final UUID queryId = getQueryUUID(op);

        return eval(queryId, (PipelineOp) op, attribs,
                newLocalChunkMessage(queryId, op, bsets));

    }
    
    //    /**
//     * Evaluate a query. This node will serve as the controller for the query.
//     * 
//     * @param query
//     *            The query to evaluate.
//     * @param solutionCount
//     *            The #of solutions which can be drained from the iterator.
//     * @param bsets
//     *            The binding sets to be consumed by the query.
//     * 
//     * @return The {@link IRunningQuery}.
//     * 
//     * @throws IllegalStateException
//     *             if the {@link QueryEngine} has been {@link #shutdown()}.
//     * @throws Exception
//     * 
//     * @deprecated We are trying to get the {@link IAsynchronousIterator} out of
//     *             the API here.
//     */
//    public AbstractRunningQuery eval(final BOp op, final int solutionCount,
//            final IAsynchronousIterator<IBindingSet[]> bsets) throws Exception {
//
//        final UUID queryId = getQueryUUID(op);
//        
//        return eval(queryId, (PipelineOp) op,
//                newLocalChunkMessage(queryId, op, solutionCount, bsets));
//
//    }

//    /**
//     * Evaluate a query. This node will serve as the controller for the query.
//     * 
//     * @param queryId
//     *            The unique identifier for the query.
//     * @param query
//     *            The query to evaluate.
//     * @param solutionCount
//     *            The #of source solutions which are being provided to the
//     *            query.
//     * @param bsets
//     *            The binding sets to be consumed by the query.
//     * 
//     * @return The {@link IRunningQuery}.
//     * 
//     * @throws IllegalStateException
//     *             if the {@link QueryEngine} has been {@link #shutdown()}.
//     * @throws Exception
//     * 
//     * @deprecated We are trying to get the {@link IAsynchronousIterator} out of
//     *             the API here.
//     */
//    public AbstractRunningQuery eval(final UUID queryId, final BOp op,
//            final int solutionCount,
//            final IAsynchronousIterator<IBindingSet[]> bsets) throws Exception {
//
//        return eval(queryId, (PipelineOp) op,
//                newLocalChunkMessage(queryId, op, solutionCount, bsets));
//
//    }

    /**
     * Evaluate a query. This node will serve as the controller for the query.
     * The {@link IBindingSet}s made available by the {@link IChunkMessage} will
     * be pushed into the query.
     * 
     * @param queryId
     *            The unique identifier for the query.
     * @param query
     *            The query to evaluate.
     * @param attribs
     *            Attributes to be attached to the query before it begins to
     *            execute (optional).
     * @param msg
     *            A message providing access to the initial {@link IBindingSet
     *            binding set(s)} used to begin query evaluation.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    public AbstractRunningQuery eval(//
            final UUID queryId,//
            final PipelineOp query,//
            final Map<Object,Object> queryAttributes,//
            final IChunkMessage<IBindingSet> msg//
            ) throws Exception {

        return startEval(queryId, query, queryAttributes, msg);

    }
    
    /**
     * Begin to evaluate a query (core impl). This node will serve as the
     * controller for the query. The {@link IBindingSet}s made available by the
     * {@link IChunkMessage} will be pushed into the query.
     * 
     * @param queryId
     *            The unique identifier for the query.
     * @param query
     *            The query to evaluate.
     * @param queryAttributes
     *            Attributes to be attached to the query before it begins to
     *            execute (optional).
     * @param msg
     *            A message providing access to the initial {@link IBindingSet
     *            binding set(s)} used to begin query evaluation.
     * 
     * @return The {@link IRunningQuery}.
     * 
     * @throws IllegalStateException
     *             if the {@link QueryEngine} has been {@link #shutdown()}.
     * @throws Exception
     */
    private AbstractRunningQuery startEval(//
            final UUID queryId,//
            final PipelineOp query,//
            final Map<Object, Object> queryAttributes,//
            final IChunkMessage<IBindingSet> msg//
            ) throws Exception {
        
        if (queryId == null)
            throw new IllegalArgumentException();

        if (query == null)
            throw new IllegalArgumentException();

        if (msg == null)
            throw new IllegalArgumentException();

        if (!queryId.equals(msg.getQueryId()))
            throw new IllegalArgumentException();

        /*
         * We are the query controller. Our reference will be reported as the
         * proxy and our serviceUUID will be reported as the UUID of the query
         * controller.
         */
        final AbstractRunningQuery runningQuery = newRunningQuery(queryId,
                true/* controller */, getProxy()/* queryController */,
                getServiceUUID(), query, msg/* realSource */);

        if (queryAttributes != null) {

            /*
             * Propagate any initial attributes to the query.
             */
            
            final IQueryAttributes tmp = runningQuery.getAttributes();

            for (Map.Entry<Object, Object> e : queryAttributes.entrySet()) {

                tmp.put(e.getKey(), e.getValue());

            }

        }
        
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

        // Note: ChunkTask verifies this.
//        /*
//         * Verify that all bops from the identified bop to the root have an
//         * assigned bop. This is required in order for us to be able to target
//         * messages to those operators.
//         */
//        BOpUtility.verifyPipline(msg.getBOpId(), query);

        // verify query engine is running.
        assertRunning();

        // add to running query table.
        if (putIfAbsent(queryId, runningQuery) != runningQuery) {

            /*
             * UUIDs should not collide when assigned randomly. However, the
             * UUID may be imposed by an exterior process, such as a SPARQL end
             * point, so it can access metadata about the running query even
             * when it is not a direct client of the QueryEngine. This provides
             * a safety check against UUID collisions which might be non-random.
             */
            throw new RuntimeException("Query exists with that UUID: uuid="
                + runningQuery.getQueryId());
            
        }

//        final String tag = query.getProperty(QueryHints.TAG,
//                QueryHints.DEFAULT_TAG);
//
//        final Counters c = tag == null ? null : getCounters(tag);

        // track #of started queries.
        counters.queryStartCount.increment();

//        if (c != null)
//            c.startCount.increment();

        if (pendingCancelLRU.containsKey(runningQuery.getQueryId())) {
            /*
             * The query was asynchronously scheduled for cancellation.
             */

            // Cancel the query.
            runningQuery.cancel(true/* mayInterruptIfRunning */);
            
            // Remove from the CANCEL LRU.
            pendingCancelLRU.remove(runningQuery.getQueryId());
        
            // Return the query. It has already been cancelled.
            return runningQuery;
            
        }

        // notify query start
        runningQuery.startQuery(msg);
        
        // tell query to consume the initial chunk.
        acceptChunk(msg);
        
        return runningQuery;

    }

    /*
     * Management of running queries.
     */

    /**
     * Places the {@link AbstractRunningQuery} object into the internal map.
     * 
     * @param queryId
     *            The query identifier.
     * @param runningQuery
     *            The {@link AbstractRunningQuery}.
     * 
     * @return The {@link AbstractRunningQuery} -or- another
     *         {@link AbstractRunningQuery} iff one exists with the same
     *         {@link UUID}.
     */
    protected AbstractRunningQuery putIfAbsent(final UUID queryId,
            final AbstractRunningQuery runningQuery) {

        if (queryId == null)
            throw new IllegalArgumentException();

        if (runningQuery == null)
            throw new IllegalArgumentException();

        // First, check [runningQueries] w/o acquiring a lock.
        {
            final AbstractRunningQuery tmp = runningQueries.get(queryId);

            if (tmp != null) {
        
                // Found existing query.
                return tmp;
                
            }
            
        }

        /*
         * A lock is used to address a race condition here with the concurrent
         * registration and halt of a query.
         */

        lock.lock();
        
        try {
            
            // Test for a recently terminated query.
            final Future<Void> doneQueryFuture = doneQueries.get(queryId);

            if (doneQueryFuture != null) {

                // Throw out an appropriate exception for a halted query.
                handleDoneQuery(queryId, doneQueryFuture);

                // Should never get here.
                throw new AssertionError();
                
            }

            // Test again for an active query while holding the lock.
            final AbstractRunningQuery tmp = runningQueries.putIfAbsent(queryId,
                    runningQuery);
            
            if (tmp != null) {
                
                // Another thread won the race.
                return tmp;
                
            }

            /*
             * Query was newly registered.
             */
            try {
                
                // Verify QueryEngine is running.
                assertRunning();
                
            } catch (IllegalStateException ex) {
                
                /**
                 * The query engine either is not initialized or was shutdown
                 * concurrent with adding the new query to the running query
                 * table. We yank the query out of the running query table in
                 * order to have no net effect and then throw out the exception
                 * indicating that the QueryEngine has been shutdown.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/705">
                 *      Race condition in QueryEngine.putIfAbsent() </a>
                 */

                runningQueries.remove(queryId, runningQuery);

                throw ex;

            }
            
            return runningQuery;
            
        } finally {

            lock.unlock();
            
        }

    }

    /**
     * Return the {@link AbstractRunningQuery} associated with that query
     * identifier.
     * 
     * @param queryId
     *            The query identifier.
     * 
     * @return The {@link AbstractRunningQuery} -or- <code>null</code> if there
     *         is no query associated with that query identifier.
     * 
     * @throws RuntimeException
     *             if the query halted with an error (if the query halted
     *             normally this will wrap an {@link InterruptedException}).
     */
    public /*protected*/ AbstractRunningQuery getRunningQuery(final UUID queryId) {

        if(queryId == null)
            throw new IllegalArgumentException();

        AbstractRunningQuery q;

        /*
         * First, test the concurrent map w/o obtaining a lock. This handles
         * queries which are actively running.
         */
        if ((q = runningQueries.get(queryId)) != null) {

            // Found running query.
            return q;
            
        }

        /*
         * Since the query was not found in the set of actively running queries,
         * we now get the lock, re-verify that it is not an active query, and
         * verify that it is not a halted query.
         */
        
        lock.lock();
        
        try {

            if ((q = runningQueries.get(queryId)) != null) {
            
                // Unlikely concurrent register of the query.
                return q;
            
            }

            // Test to see if the query is halted.
            final Future<Void> doneQueryFuture = doneQueries.get(queryId);

            if (doneQueryFuture != null) {
                
                // Throw out an appropriate exception for a halted query.
                handleDoneQuery(queryId, doneQueryFuture);

                // Should never get here.
                throw new AssertionError();
                
            }
            
            // Not found in done queries either.
            return null;
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * The query is no longer running. Resources associated with the query
     * should be released.
     */
    protected void halt(final AbstractRunningQuery q) {

        boolean interrupted = false;
        lock.lock();

        try {

            // notify listener(s)
            try {
                fireEvent(q);
            } catch (Throwable t) {
                if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                    // Defer impact until outside of this critical section.
                    interrupted = true;
                }
            }
            
            // insert/touch the LRU of recently finished queries.
            doneQueries.put(q.getQueryId(), q.getFuture());

            // remove from the set of running queries.
            runningQueries.remove(q.getQueryId(), q);

            if(runningQueries.isEmpty()) {

                // Signal that no queries are running.
                nothingRunning.signalAll();
                
            }
            
        } finally {

            lock.unlock();
            
        }

        if (interrupted)
            Thread.currentThread().interrupt();
        
    }
    
    /**
     * Handle a recently halted query by throwing an appropriate exception.
     * 
     * @param queryId
     *            The query identifier.
     * @param doneQueryFuture
     *            The {@link Future} for that query from {@link #doneQueries}.
     * @throws InterruptedException
     *             if the query halted normally.
     * @throws RuntimeException
     *             if the query halted with an error.
     */
    private void handleDoneQuery(final UUID queryId,
            final Future<Void> doneQueryFuture) {
        try {
            // Check the Future.
            doneQueryFuture.get();
            // The query is done, so the caller can not access it any more.
            throw new InterruptedException();
        } catch (InterruptedException e) {
            /*
             * Interrupted awaiting the future (note that the Future should be
             * available immediately).
             */
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            /*
             * Some kind of an error when running the query (might have just
             * been interrupted, in which case the InterruptedException will be
             * wrapped here).
             */
            throw new RuntimeException(e);
        }
    }

    /**
     * Listener API for {@link IRunningQuery} life cycle events (start/halt).
     * <p>
     * Note: While this interface makes it possible to catch the start and halt
     * of an {@link IRunningQuery}, it imposes an overhead on the query engine
     * and the potential for significant latency and other problems depending on
     * the behavior of the {@link IRunningQueryListener}. This interface was
     * added to facilitate certain test suites which could not otherwise be
     * written. It should not be used for protection code.
     */
    public interface IRunningQueryListener {
        
        void notify(IRunningQuery q);
        
    }
    
    /** Registered listeners. */
    private final CopyOnWriteArraySet<IRunningQueryListener> listeners = new CopyOnWriteArraySet<IRunningQueryListener>();

    /** Add a query listener. */
    public void addListener(final IRunningQueryListener l) {

        if (l == null)
            throw new IllegalArgumentException();

        listeners.add(l);

    }

    /** Remove a query listener. */
    public void removeListener(final IRunningQueryListener l) {
        
        if (l == null)
            throw new IllegalArgumentException();
        
        listeners.remove(l);
        
    }

    /**
     * Send an event to all registered listeners.
     */
    private void fireEvent(final IRunningQuery q) {
        
        if (q == null)
            throw new IllegalArgumentException();
        
        if(listeners.isEmpty()) {
         
            // NOP
            return;

        }

        final IRunningQueryListener[] a = listeners
                .toArray(new IRunningQueryListener[0]);

        for (IRunningQueryListener l : a) {

            final IRunningQueryListener listener = l;

            try {

                // send event.
                listener.notify(q);

            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, InterruptedException.class)) {

                    // Propagate interrupt.
                    throw new RuntimeException(t);

                }

                // Log and ignore.
                log.error(t, t);

            }

        }

    }

    /*
     * RunningQuery factory.
     */
    
    /**
     * Factory for {@link IRunningQuery}s.
     * 
     * @see Annotations#RUNNING_QUERY_CLASS
     */
    @SuppressWarnings("unchecked")
    protected AbstractRunningQuery newRunningQuery(
            /*final QueryEngine queryEngine,*/ final UUID queryId,
            final boolean controller, final IQueryClient clientProxy,
            final UUID queryControllerId,
            final PipelineOp query, final IChunkMessage<IBindingSet> realSource) {

        final String className = query.getProperty(
                Annotations.RUNNING_QUERY_CLASS,
                Annotations.DEFAULT_RUNNING_QUERY_CLASS);

        final Class<IRunningQuery> cls;
        try {
            cls = (Class<IRunningQuery>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Bad option: "
                    + Annotations.RUNNING_QUERY_CLASS, e);
        }

        if (!IRunningQuery.class.isAssignableFrom(cls)) {
            throw new RuntimeException(Annotations.RUNNING_QUERY_CLASS
                    + ": Must extend: " + IRunningQuery.class.getName());
        }

        final IRunningQuery runningQuery;
        try {

            final Constructor<? extends IRunningQuery> ctor = cls
                    .getConstructor(new Class[] { QueryEngine.class,
                            UUID.class, Boolean.TYPE, IQueryClient.class,
                            PipelineOp.class, IChunkMessage.class});

            // save reference.
            runningQuery = ctor.newInstance(new Object[] { this, queryId,
                    controller, clientProxy, query, realSource });

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }
        
        /*
         * @todo either modify to allow IRunningQuery return or update the
         * javadoc to specify the AbstractRunningQuery base class. 
         * 
         * @todo Measure the runtime cost of this dynamic decision.  We could
         * always hardware the default.
         */
        return (AbstractRunningQuery) runningQuery;
        
//        return new ChunkedRunningQuery(this, queryId, true/* controller */,
//                this/* clientProxy */, query);

    }

    public UUID[] getRunningQueries() {

        return runningQueries.keySet().toArray(new UUID[0]);

    }

//  synchronized public void addListener(final IQueryEngineListener listener) {
//
//      if (m_listeners == null) {
//
//          m_listeners = new Vector<IQueryEngineListener>();
//
//          m_listeners.add(listener);
//
//      } else {
//
//          if (m_listeners.contains(listener)) {
//
//              throw new IllegalStateException("Already registered: listener="
//                      + listener);
//
//          }
//
//          m_listeners.add(listener);
//
//      }
//
//  }
//
//    synchronized public void removeListener(IQueryEngineListener listener) {
//
//        if( m_listeners == null ) {
//            
//            throw new IllegalStateException
//                ( "Not registered: listener="+listener
//                  );
//            
//        }
//        
//        if( ! m_listeners.remove( listener ) ) {
//            
//            throw new IllegalStateException
//                ( "Not registered: listener="+listener
//                  );
//            
//        }
//        
//        if(m_listeners.isEmpty()) {
//
//          /*
//           * Note: We can test whether or not listeners need to be notified
//           * simply by testing m_listeners != null.
//           */
//            
//            m_listeners = null;
//            
//        }
//
//    }
//
//    // Must deliver events in another thread!
//    // Must drop and drop any errors.
//    // Optimize with CopyOnWriteArray
//    // Note: Security hole if we allow notification for queries w/o queryId.
//  protected void fireQueryEndedEvent(final IRunningQuery query) {
//      
//      if (m_listeners == null)
//          return;
//
//      final IQueryEngineListener[] listeners = (IQueryEngineListener[]) m_listeners
//              .toArray(new IQueryEngineListener[] {});
//
//      for (int i = 0; i < listeners.length; i++) {
//
//          final IQueryEngineListener l = listeners[i];
//
//          l.queryEnded(query);
//
//      }
//      
//    }
//    
//    private Vector<IQueryEngineListener> m_listeners;
//
//    public interface IQueryEngineListener {
//
//      void queryEnded(final IRunningQuery q);
//      
//    }

}
