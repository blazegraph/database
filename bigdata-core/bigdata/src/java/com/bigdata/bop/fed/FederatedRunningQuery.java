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
 * Created on Sep 6, 2010
 */

package com.bigdata.bop.fed;

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.ChunkedRunningQuery;
import com.bigdata.bop.engine.IChunkHandler;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocationContext;
import com.bigdata.service.IBigdataFederation;

/**
 * Extends {@link ChunkedRunningQuery} to provide additional state and logic
 * required to support distributed query evaluation against an
 * {@link IBigdataFederation} .
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class FederatedRunningQuery extends ChunkedRunningQuery {

    private final static transient Logger log = Logger
            .getLogger(FederatedRunningQuery.class);

    /**
     * The {@link UUID} of the service which is the {@link IQueryClient} running
     * this query.
     */
    /*private*/ final UUID queryControllerUUID;
    
    /**
     * A map of all allocation contexts associated with this query.
     * 
     * FIXME Separate out the life cycle of the allocation from the allocation
     * context. This is necessary so we can send chunks to multiple receivers or
     * retry transmission if a receiver dies.
     * 
     * FIXME When a chunk is transferred to the receiver, its allocation(s) must
     * be released on the sender. That should be done based on the life cycle of
     * the allocation. This implies that we should maintain a list for each life
     * cycle so they can be cleared without scanning the set of all allocations.
     * 
     * FIXME When the query is done, those allocations MUST be released
     * (excepting only allocations associated with the solutions which need to
     * be released as those data are consumed).
     * 
     * <pre>
     * Query terminates: releaseAllocationContexts(queryId)
     * BOp terminates: releaseAllocationContexts(queryId,bopId)
     * BOp+shard terminates: releaseAllocationContexts(queryId,bopId,shardId)
     * receiver takes data: releaseAllocations(IChunkMessage)
     * </pre>
     * 
     * The allocation contexts which use a bopId MUST use the bopId of the
     * operator which will consume the chunk. That way the producer can release
     * outputs buffered for that consumer as they are transferred to the
     * consumer. [If we leave the allocations in place until the bop evaluates
     * the chunk then we could retry if the node running the bop fails.]
     * <p>
     * Allocations which are tied to the life cycle of a {@link BOp}, rather the
     * the chunks processed for that {@link BOp}, should use (queryId,bopId).
     * For example, a DHT imposing distinct on a default graph access path has a
     * life cycle linked to the join reading on that access path (not the
     * predicate since the predicate is not part of the pipeline and is
     * evaluated by the join rather than the query engine).
     * <p>
     * Allocations tied to the life cycle of a query will not be released until
     * the query terminates. For example, a temporary graph containing the
     * ontology for a parallel distributed closure operation.
     */
    private final ConcurrentHashMap<AllocationContextKey, IAllocationContext> allocationContexts = new ConcurrentHashMap<AllocationContextKey, IAllocationContext>();
    
    /**
     * Extended to release all allocations associated with the specified
     * operator.
     * <p>
     * {@inheritDoc}
     */
    @Override
    protected void releaseNativeMemoryForOperator(final int bopId) {

        final Iterator<Map.Entry<AllocationContextKey, IAllocationContext>> itr = allocationContexts
                .entrySet().iterator();
        
        while (itr.hasNext()) {
        
            final Map.Entry<AllocationContextKey, IAllocationContext> e = itr
                    .next();
            
            if (e.getKey().hasOperatorScope(bopId)) {
            
                e.getValue().release();
                
            }
            
        }

    }
    
    /**
     * Extended to release all {@link IAllocationContext}s associated with the
     * query when it terminates.
     * <p>
     * {@inheritDoc}
    */
    @Override
    protected void releaseNativeMemoryForQuery() {

        for(IAllocationContext ctx : allocationContexts.values()) {

            ctx.release();
            
        }
        
        allocationContexts.clear();
        
        super.releaseNativeMemoryForQuery();
        
    }

    public FederatedRunningQuery(final FederatedQueryEngine queryEngine,
            final UUID queryId, final boolean controller,
            final IQueryClient clientProxy, final UUID queryControllerId,
            final PipelineOp query, final IChunkMessage<IBindingSet> realSource) {

        super(queryEngine, queryId, /* begin, */controller, clientProxy, query,
                realSource);

        if (queryControllerId == null)
            throw new IllegalArgumentException();

        this.queryControllerUUID = queryControllerId;

        if (!getQuery().getEvaluationContext().equals(
                BOpEvaluationContext.CONTROLLER)) {

            /*
             * In scale-out, the top-level operator in the query plan must be
             * evaluated on the query controller in order for the solutions to
             * be transferred to the query controller where they may be consumed
             * by the client.
             */
            throw new RuntimeException(
                    "The top-level of a query must be evaluated on the query controller: query="
                            + getQuery());

        }
        
//        peers = isController() ? new ConcurrentHashMap<IQueryPeer, IQueryPeer>()
//                : null;

    }

    @Override
    public FederatedQueryEngine getQueryEngine() {

        return (FederatedQueryEngine) super.getQueryEngine();

    }

    /**
     * Overridden to make this visible to the {@link FederatedQueryEngine}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    protected boolean acceptChunk(final IChunkMessage<IBindingSet> msg) {

        return super.acceptChunk(msg);
        
    }

    /**
     * Resolve the proxy for an {@link IQueryPeer}. This is special cased for
     * both <i>this</i> service (the actual reference is returned) and the query
     * controller (we use an alternative path to discover the query controller
     * since it might not be registered against a lookup service if it is not a
     * data service).
     * 
     * @param serviceUUID
     *            The service identifier for the peer.
     * 
     * @return The proxy for the service, the actual {@link QueryEngine}
     *         reference if the identified service is <i>this</i> service, or
     *         <code>null</code> if the service could not be discovered.
     */
    public IQueryPeer getQueryPeer(final UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        final IQueryPeer queryPeer;

        final FederatedQueryEngine queryEngine = getQueryEngine();
        
        if(serviceUUID.equals(queryEngine.getServiceUUID())) {

            /*
             * Return a hard reference to this query engine (NOT a proxy).
             * 
             * Note: This is used to avoid RMI when the message will be consumed
             * by the service which produced that message. This is a deliberate
             * performance optimization which is supported by all of the data
             * structures involved.
             */
            queryPeer = queryEngine;
            
//            if(log.isTraceEnabled()) log.trace("Target is self: "+serviceUUID);
            
        } else if (serviceUUID.equals(queryControllerUUID)) {
        
            // The target is the query controller.
            queryPeer = getQueryController();

//            if(log.isTraceEnabled()) log.trace("Target is controller: "+serviceUUID);

        } else {
            
            // The target is some data service.
            
//            if(log.isTraceEnabled()) log.trace("Target is peer: "+serviceUUID);

            queryPeer = queryEngine.getQueryPeer(serviceUUID);
            
        }

        return queryPeer;

    }

    /**
     * Return the {@link IAllocationContext} for the given key.
     * 
     * @param key
     *            The key.
     *            
     * @return The allocation context.
     */
    public IAllocationContext getAllocationContext(
            final AllocationContextKey key) {

        final IAllocationContext ctx = getQueryEngine().getResourceService()
                .getAllocator().getAllocationContext(key);

        // note the allocation contexts associated with this running query.
        final IAllocationContext ctx2 = allocationContexts
                .putIfAbsent(key, ctx);

        if (ctx2 != null)
            return ctx2;

        return ctx;

    }

    /**
     * Overridden to notify each peer on which the query was started.
     * <p>
     * {@inheritDoc}
     */
    @Override
    protected boolean cancelQueryOnPeers(final Throwable cause,
            final Set<UUID/* ServiceId */> startedOn) {

        boolean cancelled = super.cancelQueryOnPeers(cause, startedOn);

        final UUID queryId = getQueryId();
        
        for (UUID serviceId : startedOn) {

            final IQueryPeer peer = getQueryPeer(serviceId);
            
            try {

                peer.cancelQuery(queryId, cause);
                
            } catch (RemoteException e) {

                /*
                 * If we do not manage to notify the peer then the peer will not
                 * release resources assigned to the query in a timely manner.
                 * However, assuming the peer eventually manages to send some
                 * data to another node which has been notified (or to the
                 * controller), then the peer will receive a notice that the
                 * query either has been cancelled or is unknown. Either way,
                 * the peer will eventually release its resources allocated to
                 * the query once it reconnects.
                 */
                log.error("Could not notify: " + peer, e);
                
            }
            
            cancelled = true;
            
        }
        
        return cancelled;

    }
 
    @Override
    protected IChunkHandler getChunkHandler(final QueryEngine queryEngine, final PipelineOp query) {
        
        return query.getProperty(
                QueryEngine.Annotations.CHUNK_HANDLER,
                FederationChunkHandler.INSTANCE);
        
    }

    /**
     * Overridden to broadcast to all nodes and/or shards on which the operator
     * has run in scale-out. Broadcast will be to nodes if the operator is hash
     * partitioned. Broadcast will be to shards if the operator is key-range
     * partitioned.
     * <p>
     * {@inheritDoc}
     * 
     * @see BOpEvaluationContext
     */
    @Override
    protected void doLastPass(final int bopId, final Set doneOn) {
        
        final BOp op = getBOp(bopId);
        
        switch(op.getEvaluationContext()) {
        case CONTROLLER:
            super.doLastPass(bopId, doneOn);
            return;
        case ANY:
        case HASHED: {
            /*
             * For both ANY and HASHED we want to send a last pass message to
             * each service on which the operator was evaluated. For ANY, the
             * set of such services will be discovered dynamically as the query
             * is evaluated. For HASHED, the set of such services will be
             * defined by the time we start evaluation of that operator.
             */
            for (UUID serviceId : ((Set<UUID>) doneOn)) {

                final IChunkMessage<IBindingSet> emptyMessage = new EmptyChunkMessage<IBindingSet>(
                        getQueryController(), getQueryId(), bopId,
                        -1/* shardId */, true/* lastInvocation */);

                try {

                    final IQueryPeer queryPeer = getQueryPeer(serviceId);

                    queryPeer.bufferReady(emptyMessage);

                } catch (RemoteException e) {

                    throw new RuntimeException(
                            "Could not send message: serviceId=" + serviceId
                                    + " : " + e, e);

                }

            }
            return;
        }
        case SHARDED: {
            /*
             * Generate a last pass message for each shard on which the operator
             * was invoked.
             */
            for (Integer shardId : ((Set<Integer>) doneOn)) {

                final IChunkMessage<IBindingSet> emptyMessage = new EmptyChunkMessage<IBindingSet>(
                        getQueryController(), getQueryId(), bopId, shardId,
                        true/* lastInvocation */);

                acceptChunk(emptyMessage);
                
            }
            return;
        }
        default:
            throw new AssertionError();
        }

    }

}
