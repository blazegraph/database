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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.fed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IQueryDecl;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * An {@link IBigdataFederation} aware {@link QueryEngine}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: FederatedQueryEngine.java 3508 2010-09-05 17:02:34Z thompsonbry
 *          $
 */
public class FederatedQueryEngine extends QueryEngine {

    private final static transient Logger log = Logger
            .getLogger(FederatedQueryEngine.class);

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
                        .getResourceManager().getResourceService());

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

        super(indexManager);

        if (fed == null)
            throw new IllegalArgumentException();

        if (resourceService == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.serviceUUID = thisService;
        
        this.resourceService = resourceService;

        if(fed instanceof JiniFederation<?>) {

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

            this.clientProxy = (IQueryClient) ((JiniFederation<?>) fed)
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
     * @todo ANALYTIC_QUERY: {@link IChunkMessage} are dropped onto a queue and
     *       materialized in order of arrival. This works fine for low latency
     *       pipelined query evaluation.
     *       <p>
     *       For analytic query, we (a) manage the #of high volume operators
     *       which run concurrently, presumably based on their demands on
     *       memory; and (b) model the chunks available before they are
     *       materialized locally such that (c) they can be materialized on
     *       demand (flow control); and (d) we can run the operator when there
     *       are sufficient chunks available without taking on too much data.
     *       <p>
     *       This requires a separate queue for executing high volume operators
     *       and also separate consideration of when chunks available on remote
     *       nodes should be materialized.
     */
    @Override
    public void init() {

        super.init();

        acceptTaskService.set(Executors
                .newCachedThreadPool(new DaemonThreadFactory(
                        FederatedQueryEngine.class + ".acceptService")));

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

        private final IChunkMessage<?> msg;

        public MaterializeMessageTask(final IChunkMessage<?> msg) {
            this.msg = msg;
        }

        public void run() {
            try {
                if (!accept(msg)) {
                    if (log.isDebugEnabled())
                        log.debug("dropping: " + msg);
                    return;
                }
                if (log.isDebugEnabled())
                    log.debug("accepted: " + msg);
                FederatedQueryEngine.this.acceptChunk((IChunkMessage) msg);
            } catch (Throwable t) {
                if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                    log.warn("Interrupted.");
                    return;
                }
                throw new RuntimeException(t);
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
        private boolean accept(final IChunkMessage<?> msg) throws RemoteException {

            final UUID queryId = msg.getQueryId();
            
            // lookup query by id.
            FederatedRunningQuery q = getRunningQuery(queryId);

            if (q == null) {

                // true iff this is the query controller
                final boolean isController = getServiceUUID().equals(
                        msg.getQueryController().getServiceUUID());

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

                // Get the query declaration from the query controller.
                q = getDeclaredQuery(queryId);
                
            }
            
            if (!q.isCancelled() && !msg.isMaterialized()) {

                // materialize the chunk for this message.
                msg.materialize(q);

            }

            return !q.isCancelled();

        }

        /**
         * This code path handles the message the first time a chunk is observed
         * on a node for a query. Since we do not broadcast the query to all
         * nodes, the node has to resolve the query from the query controller.
         * 
         * @throws RemoteException
         */
        private FederatedRunningQuery getDeclaredQuery(final UUID queryId)
                throws RemoteException {

            /*
             * Request the query from the query controller (RMI).
             */
            final PipelineOp query = msg.getQueryController().getQuery(
                    msg.getQueryId());

            final FederatedRunningQuery q = newRunningQuery(
                    /*FederatedQueryEngine.this,*/ queryId, false/* controller */,
                    msg.getQueryController(), query);

            return (FederatedRunningQuery) putIfAbsent(queryId, q);

        }
        
    }
    
    public void declareQuery(final IQueryDecl queryDecl) {

        final UUID queryId = queryDecl.getQueryId();
        
        final FederatedRunningQuery q = newRunningQuery(/*this, */queryId,
                false/* controller */, queryDecl.getQueryController(),
                queryDecl.getQuery());
        
        putIfAbsent(queryId, q);

    }
    
    @Override
    public void bufferReady(final IChunkMessage<IBindingSet> msg) {

        if (msg == null)
            throw new IllegalArgumentException();
        
        if(log.isDebugEnabled())
            log.debug("msg=" + msg);

        assertRunning();
        
        /*
         * Schedule task to materialized or otherwise handle the message.
         */

        final Executor s = acceptTaskService.get();
        
        if (s == null)
            throw new RuntimeException("Not running");
        
        s.execute(new MaterializeMessageTask(msg));
        
    }

    /**
     * Overridden to cancel all running operators for the query on this node.
     * <p>
     * {@inheritDoc}
     */
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

            getIndexManager().getExecutorService().execute(
                    new CancelQuery(q, cause));

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
    protected FederatedRunningQuery newRunningQuery(
            /*final QueryEngine queryEngine,*/ final UUID queryId,
            final boolean controller, final IQueryClient clientProxy,
            final PipelineOp query) {

        return new FederatedRunningQuery(this/*queryEngine*/, queryId, controller,
                clientProxy, query);

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

//		if(log.isTraceEnabled()) log.trace("serviceUUID=" + serviceUUID
//				+ (proxy != null ? "cached=" + proxy : " not cached."));

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

            IQueryPeer tmp = proxyMap.putIfAbsent(serviceUUID, proxy);
            
			if (tmp != null) {
				proxy = tmp;
			}

//			if(log.isTraceEnabled()) log.trace("serviceUUID=" + serviceUUID + ", addedToCache="
//					+ (tmp == null) + ", proxy=" + proxy);

        }

        return proxy;

    }

    /**
     * Cache for {@link #getQueryPeer(UUID)}.
     */
    private final ConcurrentHashMap<UUID, IQueryPeer> proxyMap = new ConcurrentHashMap<UUID, IQueryPeer>();
    
}
