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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.fed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IQueryDecl;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.QueryEngineCounters;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;

/**
 * An {@link IBigdataFederation} aware {@link QueryEngine}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class FederatedQueryEngine extends QueryEngine {

    private final static transient Logger log = Logger
            .getLogger(FederatedQueryEngine.class);

    /**
     * Annotations understood by the {@link QueryEngine}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public interface Annotations extends QueryEngine.Annotations {

//        /**
//         * The class used to map binding sets across the federation.
//         */
//        String CHUNK_HANDLER = FederatedQueryEngine.class.getName()
//                + ".chunkHandler";
        
    }
    
    /**
     * The {@link UUID} associated with this service.
     */
    private final UUID serviceUUID;
    
    /**
     * The {@link IBigdataFederation} iff running in scale-out.
     * <p>
     * Note: The {@link IBigdataFederation} is required in scale-out in order to
     * perform shard locator scans when mapping binding sets across the next
     * join in a query plan.
     */
    private final IBigdataFederation<?> fed;

    /**
     * The service used to expose {@link ByteBuffer}s and managed index
     * resources for transfer to remote services in support of distributed query
     * evaluation.
     */
    private final ManagedResourceService resourceService;

    /**
     * <code>true</code> iff the query engine is hosted by a {@link DataService}
     * .
     */
    private final boolean isDataService;
    
    /**
     * The proxy for this query engine when used as a query controller.
     */
    private final IQueryClient clientProxy;

//    /**
//     * A queue of {@link IChunkMessage}s which needs to have their data
//     * materialized so an operator can consume those data on this node.
//     * This queue is drained by the {@link MaterializeChunksTask}.
//     */
//    final private BlockingQueue<IChunkMessage<?>> chunkMaterializationQueue = new LinkedBlockingQueue<IChunkMessage<?>>();

    /**
     * The service used to accept {@link IChunkMessage} for evaluation. This is
     * started by {@link #init()}.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/472">
     *      acceptTaskService pool size on cluster </a>
     */
    private final AtomicReference<ExecutorService> acceptTaskService = new AtomicReference<ExecutorService>();
    
//    /**
//     * The {@link Future} for the task draining the {@link #chunkMaterializationQueue}.
//     */
//    private final AtomicReference<FutureTask<Void>> acceptMessageTaskFuture = new AtomicReference<FutureTask<Void>>();

    @Override
    public UUID getServiceUUID() {

        return serviceUUID;

    }

    @Override
    public IBigdataFederation<?> getFederation() {

        return fed;

    }

//    public ConcurrencyManager getConcurrencyManager() {
//
//        if (isDataService) {
//
//            /*
//             * The concurrency manager for the DataService.
//             */
//
//            return ((DelegateIndexManager) getIndexManager())
//                    .getConcurrencyManager();
//        } else {
//            
//            return super.getConcurrencyManager();
// 
//        }
//
//    }

    /**
     * The service used to expose {@link ByteBuffer}s and managed index
     * resources for transfer to remote services in support of distributed query
     * evaluation.
     */
    public ManagedResourceService getResourceService() {
    
        return resourceService;
        
    }
    
    /**
     * Overridden to return an RMI proxy for this {@link FederatedQueryEngine}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public IQueryClient getProxy() {
        
        return clientProxy;
        
    }
    
    @Override
    final public boolean isScaleOut() {
        
        return true;
        
    }
    
    /**
     * Return <code>true</code> iff the query engine instance is hosted by a
     * {@link DataService}.
     */
    final public boolean isDataService() {
        
        return isDataService;
        
    }

    /**
     * Overridden to strengthen the return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public /*protected*/ FederatedRunningQuery getRunningQuery(final UUID queryId) {

        return (FederatedRunningQuery) super.getRunningQuery(queryId);

    }

    public String toString() {

        return getClass().getName() + "{serviceUUID=" + getServiceUUID() + "}";

    }

    /**
     * Constructor used on a {@link DataService} (a query engine peer).
     * 
     * @param dataService
     *            The data service.
     */
    public FederatedQueryEngine(final DataService dataService) {

        this(dataService.getServiceUUID(), dataService.getFederation(),
                new DelegateIndexManager(dataService), dataService
                        .getResourceManager().getResourceService(),
                        true/* isDataService */);

    }
    
    /**
     * Constructor used on a non-{@link DataService} node to expose a query
     * controller. Since the query controller is not embedded within a data
     * service it needs to provide its own {@link ResourceService} and local
     * {@link IIndexManager}.
     * 
     * @param fed
     * @param indexManager
     * @param resourceService
     */
    public FederatedQueryEngine(//
            final UUID thisService,
            final IBigdataFederation<?> fed,//
            final IIndexManager indexManager,//
            final ManagedResourceService resourceService//
    ) {

        this(thisService, fed, indexManager, resourceService, false/* isDataService */);

    }

    private FederatedQueryEngine(//
                final UUID thisService,
                final IBigdataFederation<?> fed,//
                final IIndexManager localIndexManager,//
                final ManagedResourceService resourceService,//
                final boolean isDataService
                ) {

        super(localIndexManager);

        if (fed == null)
            throw new IllegalArgumentException();

        if (resourceService == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.serviceUUID = thisService;
        
        this.resourceService = resourceService;

        this.isDataService = isDataService;
        
        if(fed.isJiniFederation()) {

            /*
             * The proxy for this query engine when used as a query controller.
             * 
             * Note: DGC is relied on to clean up the exported proxy when the
             * query engine dies.
             * 
             * @todo There should be an explicit "QueryEngineServer" which is
             * used as the front end for SPARQL queries. It should have an
             * explicitly configured Exporter for its proxy. 
             */

            this.clientProxy = (IQueryClient) ((AbstractDistributedFederation<?>) fed)
                    .getProxy(this, true/* enableDGC */);

        } else {
        
            // E.g., an EmbeddedFederation in the test suite. 
            this.clientProxy = this;
            
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to also initialize a thread which will materialize
     * {@link IChunkMessage} for consumption by this node.
     * 
     * TODO The {@link #acceptTaskService} is not used right now since we are
     * always running the {@link MaterializeMessageTask} in the caller's thread.
     * If it becomes used, then we should reconsider the pool size.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/472">
     *      acceptTaskService pool size on cluster</a>
     */
    @Override
    public void init() {

        super.init();

//        acceptTaskService.set(Executors.newFixedThreadPool(20/* nthreads */,
//                new DaemonThreadFactory(FederatedQueryEngine.class
//                        + ".acceptService")));

    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to stop materializing chunks once all running queries are done.
     */
    @Override
    protected void didShutdown() {

        // stop the service which is accepting messages.
        final ExecutorService s = acceptTaskService.get();
        if (s != null) {
            s.shutdownNow();
        }

        // Clear the references.
        acceptTaskService.set(null);

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to stop materializing chunks.
     */
    @Override
    public void shutdownNow() {
        
        // stop the service which is accepting messages.
        final ExecutorService s = acceptTaskService.get();
        if (s != null) {
            s.shutdownNow();
        }

        // Clear the references.
        acceptTaskService.set(null);

        super.shutdownNow();

    }
    
    /**
     * Materialize an {@link IChunkMessage} for processing and place it on the
     * queue of accepted messages.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class MaterializeMessageTask implements Runnable {

        private final IChunkMessage<IBindingSet> msg;

        private volatile FederatedRunningQuery q;
        
        public MaterializeMessageTask(final IChunkMessage<IBindingSet> msg) {
            
            if(msg == null)
                throw new IllegalArgumentException();
            
            this.msg = msg;
            
        }

        public void run() {
            
            try {
            
                /*
                 * Note: accept() sets [q] as a side-effect! It SHOULD NOT be
                 * resolved in the constructor since it may require an RMI back
                 * to the query controller to materialize the query on this
                 * node.
                 */
                
                if (!accept(msg)) {
                
                    if (log.isDebugEnabled())
                        log.debug("dropping: " + msg);
                    
                    return;
                    
                }

                if (log.isDebugEnabled())
                    log.debug("accepted: " + msg);
                
                /*
                 * Note: This can block if queue for each (operator,shard)
                 * bundle has a bounded capacity. If it blocks then the
                 * acceptQueue can stagnate as no new IChunkMessages can be
                 * materialized.
                 */

                FederatedQueryEngine.this.acceptChunk(msg);

                final FederatedQueryEngineCounters c = getQueryEngineCounters();
                c.chunksIn.increment();
                c.solutionsIn.add(msg.getSolutionCount());
                
            } catch (Throwable t) {

                if(q != null) {
                
                    /*
                     * Note: Since no one is watching the Future for this task,
                     * an error here needs to cause the query to abort.
                     */
                    
                    q.halt(t);
                    
                } else {
                    
                    log.error(t, t);
                    
                }
                
            }
            
        }

        /**
         * Accept and materialize a chunk for processing.
         * 
         * @param msg
         *            The message.
         * 
         * @return true if the message was accepted and materialized.
         * 
         * @throws RemoteException
         */
        private boolean accept(final IChunkMessage<?> msg)
                throws RemoteException {

            final UUID queryId = msg.getQueryId();

            if (queryId == null) {
                // Should never be null.
                throw new AssertionError();
            }
            
            // lookup query by id.
            q = getRunningQuery(queryId);

            if (q == null) {

                // true iff this is the query controller
                final boolean isController = getServiceUUID().equals(
                        msg.getQueryControllerId());

                if (isController) {
                    
                    /*
                     * @todo This would indicate that the query had been
                     * concurrently terminated and cleared from the set of
                     * runningQueries and that we were not retaining metadata
                     * about queries which had been terminated.
                     */

                    throw new AssertionError(
                            "Query not running on controller: thisService="
                                    + getServiceUUID() + ", msg=" + msg);
                }

                try {

                    /*
                     * Get the query declaration from the query controller.
                     */
                    
                    q = getDeclaredQuery(queryId);
                    
                } catch (IllegalArgumentException ex) {
                    
                    /*
                     * The query is no longer available on the query controller.
                     * Typically, it has been cancelled. For example, by
                     * satisifying a LIMIT.
                     */
                    
                    if (log.isInfoEnabled())
                        log.info("Query is gone: isDataService="
                                + isDataService() + ", message=" + msg, ex);
                    
                    return false;
                    
                }

                if (q == null) {

                    /*
                     * Note: Should never be null per getDeclaredQuery().
                     */
                    
                    throw new AssertionError();
                    
                }
                
            }

            if (!q.isCancelled() && !msg.isMaterialized()) {

                try {

                    // materialize the chunk for this message.
                    msg.materialize(q);
                    
                } catch (Throwable t) {

                    if (!AbstractRunningQuery.isRootCauseInterrupt(t)) {
                    
                        /*
						 * Note: This can be triggered by serialization errors.
						 * 
						 * Whatever is thrown out here, we want to log @ ERROR
						 * unless the root cause was an interrupt.
						 * 
						 * @see
						 * https://sourceforge.net/apps/trac/bigdata/ticket/380
						 * 
						 * @see
						 * https://sourceforge.net/apps/trac/bigdata/ticket/479
						 */
                        
                        log.error("Problem materializing message: " + msg, t);
                        
                    }

                    return false;
                    
                }

            }

            return !q.isCancelled();

        }

        /**
         * This code path handles the message the first time a chunk is observed
         * on a node for a query. Since we do not broadcast the query to all
         * nodes, the node has to resolve the query from the query controller.
         * 
         * FIXME This should use a memoizer pattern. We are probably resolving
         * the query multiple times when the first operation on a node is
         * scattered over more than one shard on that node.
         * <p>
         * Note: The operations performed here MUST NOT encounter any bounded
         * resource, such as the acceptTaskService. If they did, then this
         * request could deadlock.
         */
        private FederatedRunningQuery getDeclaredQuery(final UUID queryId)
                throws RemoteException {

            /*
             * Request the query from the query controller (RMI).
             * 
             * FIXME We can not discover the query controller using river
             * because it is not registered as a service. We can find the query
             * peers on the data service nodes easily enough because they are
             * all registered with river. However, the QueryEngine serving as
             * the query controller is not currently registered with river and
             * hence it can not be discovered using the UUID of the query
             * controller alone. Probably the right thing to do is to register
             * the query controller with river so it can be discovered. We could
             * then modify getQueryPeer() (or add getQueryClient(UUID)) which
             * would hit the discovery cache.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/475
             */
            
//            final IQueryClient queryController = (IQueryClient) getQueryPeer(msg
//                    .getQueryControllerId());

            final IQueryClient queryController = msg.getQueryController();
            
            final PipelineOp query = queryController.getQuery(msg.getQueryId());
            
            if (query == null) {
            
                /*
                 * Should never be null; getQuery() throws
                 * IllegalArgumentException.
                 */
                
                throw new AssertionError();
                
            }

            final FederatedRunningQuery q = newRunningQuery(queryId,
                    false/* controller */, queryController,
                    msg.getQueryControllerId(), query, msg);

            return (FederatedRunningQuery) putIfAbsent(queryId, q);

        }
        
    }
    
    @Deprecated // see IQueryClient
    public void declareQuery(final IQueryDecl queryDecl) throws RemoteException {

        final UUID queryId = queryDecl.getQueryId();
        
        final FederatedRunningQuery q = newRunningQuery(/*this, */queryId,
                false/* controller */, queryDecl.getQueryController(),
                queryDecl.getQueryController().getServiceUUID(), // TODO This is an RMI(!)
                queryDecl.getQuery(), null/*realSource*/);
        
        putIfAbsent(queryId, q);

    }

    /**
     * {@inheritDoc}
     * 
     * TODO The timing and responsibility for materializing chunks needs to be
     * examined further when the data are being moved around using NIO rather
     * than {@link ThickChunkMessage}. At stake is when the intermediate
     * solutions are materialized on the node where they will be consumed. We
     * can either do this synchronous or asynchronously when bufferReady() is
     * caller or we can defer the transfer until the target operator on this
     * node is ready to run.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/472">
     *      acceptTaskService pool size on cluster </a>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/131">
     *      unselective joins on cluster</a>
     */
    @Override
    public void bufferReady(final IChunkMessage<IBindingSet> msg) {

        if (msg == null)
            throw new IllegalArgumentException();
        
        if(log.isDebugEnabled())
            log.debug("msg=" + msg);

        assertRunning();
        
        /*
         * Schedule task to materialize or otherwise handle the message.
         * 
         * Note: We do not explicitly monitor the Future of this task. However,
         * The MaterializeMessageTask takes responsibility for any failures in
         * its own operation and will cause the query to be cancelled if
         * anything goes wrong.
         */

//        final Executor s = acceptTaskService.get();
//        
//        if (s == null)
//            throw new RuntimeException("Not running");
//        
//        s.execute(new MaterializeMessageTask(msg));
        
        /*
         * Run in the caller's thread.
         */
        new MaterializeMessageTask(msg).run();
        
    }

    /**
     * Overridden to cancel all running operators for the query on this node.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void cancelQuery(final UUID queryId, final Throwable cause) {

        // lookup query by id.
        final FederatedRunningQuery q = getRunningQuery(queryId);

        if (q == null)
            return;

        /*
         * Queue the cancellation notice for asynchronous processing and return
         * immediately.
         * 
         * @todo Optimization: When the controller sends a node a terminate
         * signal for an operator, it should not bother to RMI back to the
         * controller (unless this is done for the purposes of confirmation,
         * which is available from the RMI return in any case). We could do this
         * if we had local knowledge of whether the query was already cancelled
         * (or that the notice was received from the query controller).
         */

        try {

            execute(new CancelQuery(q, cause));

        } catch (RejectedExecutionException ex) {

            // ignore - the node is shutting down.

        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to always use a {@link FederatedRunningQuery}.
     */
    @Override
    protected FederatedRunningQuery newRunningQuery(final UUID queryId,
            final boolean controller, final IQueryClient clientProxy,
            final UUID queryControllerId, final PipelineOp query,
            final IChunkMessage<IBindingSet> realSource) {

        return new FederatedRunningQuery(this/* queryEngine */, queryId,
                controller, clientProxy, queryControllerId, query, realSource);

    }

    /**
     * Resolve an {@link IQueryPeer}.
     * <p>
     * Note: This only resolves the peers running on the {@link IDataService}s.
     * It will not resolve a query controller unless an {@link IDataService} is
     * being used as the query controller.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * 
     * @return The proxy for the query peer.
     */
    protected IQueryPeer getQueryPeer(final UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        IQueryPeer proxy = proxyMap.get(serviceUUID);

//      if(log.isTraceEnabled()) log.trace("serviceUUID=" + serviceUUID
//              + (proxy != null ? "cached=" + proxy : " not cached."));

        if (proxy == null) {

            final IDataService dataService = getFederation().getDataService(
                    serviceUUID);

            if (dataService == null)
                throw new RuntimeException("No such service: " + serviceUUID);

            try {
                proxy = dataService.getQueryEngine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (proxy == null) {

                /*
                 * Note: Presumably this is due to the concurrent tear down of
                 * the peer.
                 */
                
                throw new RuntimeException("No query engine on service: "
                        + serviceUUID);

            }

            IQueryPeer tmp = proxyMap.putIfAbsent(serviceUUID, proxy);
            
            if (tmp != null) {
                proxy = tmp;
            }

//          if(log.isTraceEnabled()) log.trace("serviceUUID=" + serviceUUID + ", addedToCache="
//                  + (tmp == null) + ", proxy=" + proxy);

        }

        return proxy;

    }

    /**
     * Cache for {@link #getQueryPeer(UUID)}.
     */
    private final ConcurrentHashMap<UUID, IQueryPeer> proxyMap = new ConcurrentHashMap<UUID, IQueryPeer>();

    /**
     * Extension hook for new {@link QueryEngineCounters} instances.
     */
    @Override
    protected FederatedQueryEngineCounters newCounters() {
        
        return new FederatedQueryEngineCounters();
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to strengthen the return type.
     */
    @Override
    protected FederatedQueryEngineCounters getQueryEngineCounters() {

        return (FederatedQueryEngineCounters) counters;
        
    }

}
