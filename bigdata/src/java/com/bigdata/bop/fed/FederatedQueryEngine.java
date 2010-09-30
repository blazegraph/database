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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
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
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.InnerCause;

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
    
    /**
     * A queue of {@link IChunkMessage}s which needs to have their data
     * materialized so an operator can consume those data on this node.
     */
    final private BlockingQueue<IChunkMessage<?>> chunkMaterializationQueue = new LinkedBlockingQueue<IChunkMessage<?>>();

    /**
     * The {@link Future} for the task draining the {@link #chunkMaterializationQueue}.
     */
    private final AtomicReference<FutureTask<Void>> materializeChunksFuture = new AtomicReference<FutureTask<Void>>();

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
    protected FederatedRunningQuery getRunningQuery(final UUID queryId) {

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
            // the proxy for this query engine when used as a query controller.
        	this.clientProxy = (IQueryClient) ((JiniFederation<?>)fed).getProxy(this, false/*enableDGC*/);
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
        
        final FutureTask<Void> ft = new FutureTask<Void>(
                new MaterializeChunksTask(), (Void) null);
        
        if (materializeChunksFuture.compareAndSet(null/* expect */, ft)) {
        
            getIndexManager().getExecutorService().execute(ft);
            
        } else {
            
            throw new IllegalStateException("Already running");
            
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Extended to stop materializing chunks once all running queries are done.
     */
    @Override
    protected void didShutdown() {
        
        // stop materializing chunks.
        final Future<?> f = materializeChunksFuture.get();
        if (f != null)
            f.cancel(true/* mayInterruptIfRunning */);

    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Extended to stop materializing chunks.
     */
    @Override
    public void shutdownNow() {
        
        // stop materializing chunks.
        final Future<?> f = materializeChunksFuture.get();
        if (f != null)
            f.cancel(true/* mayInterruptIfRunning */);

        super.shutdownNow();

    }
    
    /**
     * Runnable materializes chunks and makes them available for further
     * processing.
     * 
     * @todo multiple threads for materializing chunks, not just one. can
     * be multiple {@link MaterializeChunksTask}s running.
     */
    private class MaterializeChunksTask implements Runnable {

        public void run() {
            if(log.isInfoEnabled())
                log.info("running: " + this);
            while (true) {
                try {
                    final IChunkMessage<?> msg = chunkMaterializationQueue.take();
                    if(log.isDebugEnabled())
                        log.debug("msg=" + msg);
                    try {
                        if(!accept(msg)) {
                            if(log.isDebugEnabled())
                                log.debug("dropping: " + msg);
                            continue;
                        }
                        if(log.isDebugEnabled())
                            log.debug("accepted: " + msg);
                        /*
                         * @todo The type warning here is because the rest of
                         * the API does not know what to do with messages for
                         * chunks other than IBindingSet[], e.g., IElement[],
                         * etc.
                         */
                        FederatedQueryEngine.this
                                .acceptChunk((IChunkMessage) msg);
                    } catch(Throwable t) {
                        if(InnerCause.isInnerCause(t, InterruptedException.class)) {
                            log.warn("Interrupted.");
                            return;
                        }
                        throw new RuntimeException(t);
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

                /*
                 * This code path handles the message the first time a chunk is
                 * observed on a node for a query. Since we do not broadcast the
                 * query to all nodes, the node has to resolve the query from the
                 * query controller.
                 * 
                 * @todo Track recently terminated queries and do not recreate them.
                 */
                
                // true iff this is the query controller
                final boolean isController = getServiceUUID().equals(
                        msg.getQueryController().getServiceUUID());
                
                if(isController) {
                    /*
                     * @todo This would indicate that the query had been
                     * concurrently terminated and cleared from the set of
                     * runningQueries and that we were not retaining metadata about
                     * queries which had been terminated.
                     */
                    throw new AssertionError(
                            "Query not running on controller: thisService="
                                    + getServiceUUID() + ", msg=" + msg);
                }

                /*
                 * Request the query from the query controller (RMI).
                 * 
                 * @todo RMI is too expensive. Apply a memoizer pattern to avoid
                 * race conditions.
                 */
                final PipelineOp query = msg.getQueryController()
                        .getQuery(msg.getQueryId());

                q = newRunningQuery(FederatedQueryEngine.this, queryId,
                        false/* controller */, msg.getQueryController(), query);

                final RunningQuery tmp = runningQueries.putIfAbsent(queryId, q);
                
                if(tmp != null) {
                    
                    // another thread won this race.
                    q = (FederatedRunningQuery) tmp;
                    
                }

            }
            
//            if(q == null)
//                throw new RuntimeException(ERR_QUERY_NOT_RUNNING + queryId);
        
            if (!q.isCancelled() && !msg.isMaterialized()) {

                msg.materialize(q);

            }

            return !q.isCancelled();

        }
        
    } // MaterializeChunksTask

    public void declareQuery(final IQueryDecl queryDecl) {

        final UUID queryId = queryDecl.getQueryId();
        
        putRunningQuery(queryId, newRunningQuery(this, queryId,
                false/* controller */, queryDecl.getQueryController(),
                queryDecl.getQuery()));

    }
    
    @Override
    public void bufferReady(final IChunkMessage<IBindingSet> msg) {

        if (msg == null)
            throw new IllegalArgumentException();
        
        assertRunning();
        
        // queue up message to be materialized or otherwise handled later.
        chunkMaterializationQueue.add(msg);
        
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
     * Factory for {@link RunningQuery}s.
     */
    @Override
    protected FederatedRunningQuery newRunningQuery(
            final QueryEngine queryEngine, final UUID queryId,
            final boolean controller, final IQueryClient clientProxy,
            final PipelineOp query) {

        return new FederatedRunningQuery(this, queryId, controller,
                this.clientProxy, query);

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

        IQueryPeer proxy = proxyMap.get(serviceUUID);

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

            proxyMap.put(serviceUUID, proxy);

        }

        return proxy;

    }

    /**
     * Cache for {@link #getQueryPeer(UUID)}.
     */
    private final ConcurrentHashMap<UUID, IQueryPeer> proxyMap = new ConcurrentHashMap<UUID, IQueryPeer>();
    
}
