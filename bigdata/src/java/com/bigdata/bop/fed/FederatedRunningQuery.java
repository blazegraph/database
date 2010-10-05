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
 * Created on Sep 6, 2010
 */

package com.bigdata.bop.fed;

import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IShardwisePipelineOp;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.fed.shards.MapBindingSetsOverShardsBuffer;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocationContext;
import com.bigdata.journal.TemporaryStoreFactory;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.eval.pipeline.DistributedJoinTask;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ResourceService;

/**
 * Extends {@link RunningQuery} to provide additional state and logic required
 * to support distributed query evaluation against an {@link IBigdataFederation}
 * .
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: FederatedRunningQuery.java 3511 2010-09-06 20:45:37Z
 *          thompsonbry $
 * 
 * @todo SCALEOUT: Life cycle management of the operators and the query implies
 *       both a per-query bop:NodeList map on the query coordinator identifying
 *       the nodes on which the query has been executed and a per-query
 *       bop:ResourceList map identifying the resources associated with the
 *       execution of that bop on that node. In fact, this could be the same
 *       {@link #resourceMap} except that we would lose type information about
 *       the nature of the resource so it is better to have distinct maps for
 *       this purpose.
 * 
 * @todo HA aspects of running queries? Checkpoints for long running queries?
 * */
public class FederatedRunningQuery extends RunningQuery {

    private final static transient Logger log = Logger
            .getLogger(FederatedRunningQuery.class);

    /**
     * The {@link UUID} of the service which is the {@link IQueryClient} running
     * this query.
     */
    private final UUID queryControllerUUID;

    /**
     * A map associating resources with running queries. When a query halts, the
     * resources listed in its resource map are released. Resources can include
     * {@link ByteBuffer}s backing either incoming or outgoing
     * {@link LocalChunkMessage}s, temporary files associated with the query,
     * hash tables, etc.
     * 
     * @todo The {@link IAllocationContext} allows us to automatically release
     *       native {@link ByteBuffer}s used by the query. Such buffers do not
     *       need to be part of this map. This means that the only real use for
     *       the map will be temporary persistent resources, such as graphs or
     *       hash tables backed by a local file or the intermediate outputs of a
     *       sort operator. We may be able to manage the local persistent data
     *       structures using the {@link TemporaryStoreFactory} and manage the
     *       life cycle of the intermediate results for sort within its operator
     *       implementation.
     * 
     * @todo This map will eventually need to be moved into {@link RunningQuery}
     *       in order to support temporary graphs or other disk-backed resources
     *       associated with the evaluation of a query against a standalone
     *       database. However, the main use case are the resources associated
     *       with query against an {@link IBigdataFederation} which it why it is
     *       being developed in the {@link FederatedRunningQuery} class.
     * 
     * @todo Cache any resources materialized for the query on this node (e.g.,
     *       temporary graphs materialized from a peer or the client). A bop
     *       should be able to demand those data from the cache and otherwise
     *       have them be materialized.
     * 
     * @todo Only use the values in the map for transient objects, such as a
     *       hash table which is not backed by the disk. For {@link ByteBuffer}s
     *       we want to make the references go through the
     *       {@link ResourceService} . For files, through the
     *       {@link ResourceManager}.
     * 
     * @todo We need to track the resources in use by the query so they can be
     *       released when the query terminates. This includes: buffers; joins
     *       for which there is a chunk of binding sets that are currently being
     *       executed; downstream joins (they depend on the source joins to
     *       notify them when they are complete in order to decide their own
     *       termination condition); local hash tables which are part of a DHT
     *       (especially when they are persistent); buffers and disk resources
     *       allocated to N-way merge sorts, etc.
     * 
     * @todo The set of buffers having data which has been accepted for this
     *       query.
     * 
     * @todo The set of buffers having data which has been generated for this
     *       query.
     */
    private final ConcurrentHashMap<UUID, Object> resourceMap = new ConcurrentHashMap<UUID, Object>();

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
     * The collection of {@link IQueryPeer}s which have accepted for processing
     * at least one {@link IChunkMessage} or one life cycle event for this
     * query. This collection is maintained only on the controller. The peers
     * are responsible for notifying the controller when they first begin to do
     * work for a query. This collection is used to notify peers participating
     * in the evaluation of the query when the query has been terminated.
     * 
     * FIXME All of the cases below could be trapped by noticing when a peer
     * requests the query from the controller using
     * {@link IQueryClient#getQuery(UUID)}.  We would have to send along the
     * proxy for the peer as well.
     * 
     * FIXME Notify the controller the first time a peer accepts a chunk. Note
     * that peers may receive chunks from other peers and that it is possible
     * for some peers to never receive a chunk directly from the query
     * controller.
     * 
     * FIXME {@link RunningQuery#lifeCycleSetUpQuery()} methods should be
     * registered here.
     * 
     * FIXME {@link RunningQuery#lifeCycleSetUpOperator(int)} methods should be
     * registered here.
     */
    private final ConcurrentHashMap<IQueryPeer, IQueryPeer> peers; 
    
    /**
     * Extended to release all allocations associated with the specified
     * operator.
     * <p>
     * {@inheritDoc}
     */
    @Override
    protected void lifeCycleTearDownOperator(final int bopId) {
        final Iterator<Map.Entry<AllocationContextKey, IAllocationContext>> itr = allocationContexts
                .entrySet().iterator();
        while (itr.hasNext()) {
            final Map.Entry<AllocationContextKey, IAllocationContext> e = itr
                    .next();
            if (e.getKey().hasOperatorScope(bopId)) {
                e.getValue().release();
            }
        }
        super.lifeCycleTearDownOperator(bopId);
    }

    /**
     * Extended to release all {@link IAllocationContext}s associated with the
     * query when it terminates.
     * <p>
     * {@inheritDoc}
     * 
     * @todo We need to have distinct events for the query evaluation life cycle
     *       and the query results life cycle.
     */
    @Override
    protected void lifeCycleTearDownQuery() {
        for(IAllocationContext ctx : allocationContexts.values()) {
            ctx.release();
        }
        allocationContexts.clear();
        super.lifeCycleTearDownQuery();
    }

    /**
     * @param queryEngine
     * @param queryId
     * @param controller
     * @param clientProxy
     * @param query
     */
    public FederatedRunningQuery(final FederatedQueryEngine queryEngine,
            final UUID queryId, final boolean controller,
            final IQueryClient clientProxy, final PipelineOp query) {

        super(queryEngine, queryId, /*begin, */controller, clientProxy, query);

        /*
         * Note: getServiceUUID() should be a smart proxy method and thus not
         * actually do RMI here.  However, it is resolved eagerly and cached
         * anyway.
         */
        try {
            this.queryControllerUUID = getQueryController().getServiceUUID();
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
        
        if (!getQuery().getEvaluationContext().equals(
                BOpEvaluationContext.CONTROLLER)) {

            /*
             * In scale-out, the top-level operator in the query plan must be
             * evaluated on the query controller in order for the solutions to
             * be transferred to the query controller where they may be consumed
             * by the client.
             */
            throw new RuntimeException(
                    "The top-level of a query must be evaluated on the query controller.");

        }
        
        peers = isController() ? new ConcurrentHashMap<IQueryPeer, IQueryPeer>()
                : null;

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
    protected void acceptChunk(final IChunkMessage<IBindingSet> msg) {

        super.acceptChunk(msg);
        
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
     * @return The proxy for the service or <code>null</code> if the service
     *         could not be discovered.
     */
    protected IQueryPeer getQueryPeer(final UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        final IQueryPeer queryPeer;
        
        if(serviceUUID.equals(getQueryEngine().getServiceUUID())) {
        
            // Return a hard reference to this query engine (NOT a proxy).
            return getQueryEngine();
            
        } else if (serviceUUID.equals(queryControllerUUID)) {
        
            // The target is the query controller.
            queryPeer = getQueryController();
            
        } else {
            
            // The target is some data service.
            queryPeer = getQueryEngine().getQueryPeer(serviceUUID);
            
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
     * 
     * @todo use typesafe enum for the types of allocation contexts?
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
     * {@inheritDoc}
     * <p>
     * This method is overridden to organize the output from one operator so in
     * order to make it available to another operator running on a different
     * node. There are several cases which have to be handled and which are
     * identified by the {@link BOp#getEvaluationContext()}. In addition, we
     * need to handle low latency and high data volume queries somewhat
     * differently. Except for {@link BOpEvaluationContext#ANY}, all of these
     * cases wind up writing the intermediate results onto a direct
     * {@link ByteBuffer} and notifying the receiving service that there are
     * intermediate results which it can pull when it is ready to process them.
     * This pattern allows the receiver to impose flow control on the producer.
     * 
     * @todo Figure out how (or if) we will combine binding set streams emerging
     *       from concurrent tasks executing on a given node destined for the
     *       same shard/node. (There is code in the {@link DistributedJoinTask}
     *       which does this for the same shard, but it does it on the receiver
     *       side.) Pay attention to the #of threads running in the join, the
     *       potential concurrency of threads targeting the same (bopId,shardId)
     *       and how to best combine their data together.
     */
    @Override
    protected <E> int handleOutputChunk(final BOp bop, final int sinkId,
            final IBlockingBuffer<IBindingSet[]> sink) {

        if (bop == null)
            throw new IllegalArgumentException();

        if (sink == null)
            throw new IllegalArgumentException();

        final BOp targetOp = bopIndex.get(sinkId);

        if (targetOp == null)
            throw new IllegalStateException("Not found: " + sinkId);

        if(log.isTraceEnabled())
            log.trace("queryId=" + getQueryId() + ", sink=" + sinkId);
        
        switch (targetOp.getEvaluationContext()) {
        case ANY: {
            /*
             * This operator may be evaluated anywhere.
             */
            return super.handleOutputChunk(bop, sinkId, sink);
        }
        case HASHED: {
            /*
             * @todo The sink must use annotations to describe the nodes over
             * which the binding sets will be mapped and the hash function to be
             * applied. Look up those annotations and apply them to distribute
             * the binding sets across the nodes.
             */
            throw new UnsupportedOperationException();
        }
        case SHARDED: {
            /*
             * The sink must read or write on a shard so we map the binding sets
             * across the access path for the sink.
             * 
             * @todo Set the capacity of the the "map" buffer to the size of the
             * data contained in the sink (in fact, we should just process the
             * sink data in place using an expanded IChunkAccessor interface).
             * 
             * @todo high volume operators will need different capacity
             * parameters.
             * 
             * FIXME the chunkSize will limit us to RMI w/ the payload inline
             * when it is the same as the threshold for NIO chuck transfers.
             * This needs to be adaptive and responsive to the actual data scale
             * of the operator's outputs
             */
            @SuppressWarnings("unchecked")
            final IPredicate<E> pred = ((IShardwisePipelineOp) targetOp).getPredicate();
            final long timestamp = pred.getTimestamp(); 
            final int capacity = 1000;// @todo
            final int chunkOfChunksCapacity = 10;// @todo small queue
            final int chunkSize = 100;// @todo modest chunks.
            final MapBindingSetsOverShardsBuffer<IBindingSet, E> mapper = new MapBindingSetsOverShardsBuffer<IBindingSet, E>(
                    getFederation(), pred, timestamp, capacity) {
                @Override
                protected IBuffer<IBindingSet[]> newBuffer(final PartitionLocator locator) {
                    return new BlockingBuffer<IBindingSet[]>(
                            chunkOfChunksCapacity,//
                            chunkSize,//
                            BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT,//
                            BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT//
                    );
                }
            };
            /*
             * Map the binding sets over shards.
             */
            {
                final IAsynchronousIterator<IBindingSet[]> itr = sink
                        .iterator();
                try {
                    while (itr.hasNext()) {
                        final IBindingSet[] chunk = itr.next();
                        for (IBindingSet bset : chunk) {
                            mapper.add(bset);
                        }
                    }
                } finally {
                    itr.close();
                    mapper.flush();
                }
            }
            /*
             * The allocation context.
             * 
             * @todo use (queryId, serviceId, sinkId) when the target bop is
             * high volume operator (this requires annotation by the query
             * planner of the operator tree).
             */
            final IAllocationContext allocationContext = getAllocationContext(new QueryContext(
                    getQueryId()));

            /*
             * Generate the output chunks and notify the receivers.
             * 
             * @todo This stage should probably be integrated with the stage
             * which maps the binding sets over the shards (immediately above)
             * to minimize copying or visiting in the data. This could be done
             * by hooking the method which outputs a chunk to instead directly
             * send the IChunkMessage.
             */
            int messageSendCount = 0;
            for (Map.Entry<PartitionLocator, IBuffer<IBindingSet[]>> e : mapper
                    .getSinks().entrySet()) {

                final PartitionLocator locator = e.getKey();
                
                /*
                 * Note: newBuffer() above creates an BlockingBuffer so this
                 * cast is safe.
                 */
                final IBlockingBuffer<IBindingSet[]> shardSink = (IBlockingBuffer<IBindingSet[]>) e
                        .getValue();

                // close buffer now that nothing is left to map onto it.
                shardSink.close();
                
                // send message.
                sendChunkMessage(locator.getDataServiceUUID(), sinkId, locator
                        .getPartitionId(), allocationContext, shardSink);
                
                messageSendCount++;

            }

            return messageSendCount;

        }
        case CONTROLLER: {

            /*
             * Format the binding sets onto a ByteBuffer and publish that
             * ByteBuffer as a manager resource for the query and notify the
             * query controller that data is available for it.
             */

            final IAllocationContext allocationContext = getAllocationContext(new QueryContext(
                    getQueryId()));

            sendChunkMessage(queryControllerUUID, sinkId, -1/* partitionId */,
                    allocationContext, sink);

            return 1;

        }
        default:
            throw new AssertionError(targetOp.getEvaluationContext());
        }
        
    }

    /**
     * Create and send an {@link IChunkMessage} from some intermediate results.
     * Various optimizations are employed depending on the amount of data to be
     * moved and whether or not the target is this service.
     * 
     * @param serviceUUID
     *            The {@link UUID} of the {@link IQueryPeer} who is the
     *            recipient.
     * @param sinkId
     *            The identifier of the target {@link BOp}.
     * @param allocationContext
     *            The allocation context within which the {@link ByteBuffer}s
     *            will be managed for this {@link NIOChunkMessage}.
     * @param source
     *            The binding sets to be formatted onto a buffer.
     * 
     * @return The {@link NIOChunkMessage}.
     * 
     * @todo This is basically a factory for creating {@link IChunkMessage}s.
     *       That factory pattern in combined with the logic to send the message
     *       so we can do within JVM handoffs. We could break these things apart
     *       using {@link IChunkMessage#isMaterialized()} to detect inline
     *       cases. That would let us send out the messages in parallel, which
     *       could help to cut latency when an operator has a large fan out (in
     *       scale-out when mapping over shards or nodes).
     * 
     * @todo Release the allocations associated with each output chunk once it
     *       is received by the remote service.
     *       <p>
     *       When the query terminates all output chunks targeting any node
     *       EXCEPT the query controller should be immediately dropped.
     *       <p>
     *       If there is an error during query evaluation, then the output
     *       chunks for the query controller should be immediately dropped.
     *       <p>
     *       If the iterator draining the results on the query controller is
     *       closed, then the output chunks for the query controller should be
     *       immediately dropped.
     * 
     * @todo There are a few things for which the resource must be made
     *       available to more than one operator evaluation phase. The best
     *       examples are temporary graphs for parallel closure and large
     *       collections of graphIds for SPARQL "NAMED FROM DATA SET"
     *       extensions.
     * 
     * @todo Rethink the multiplicity relationship between chunks output from an
     *       operator, chunks output from mapping the operator over shards or
     *       nodes, RMI messages concerning buffers available for the sink
     *       operator on the various nodes, and the #of allocations per RMI
     *       message on both the sender and the receiver.
     *       <p>
     *       I am pretty sure that none of these are strongly coupled, e.g.,
     *       they are not 1:1. Some stages can combine chunks. Multiple
     *       allocations could be required on either the sender or the receiver
     *       purely due to where the slices fall on the backing direct
     *       {@link ByteBuffer}s in the {@link DirectBufferPool} and the sender
     *       and receiver do not need to use the same allocation context or have
     *       the same projection of slices onto the backing buffers.
     *       <p>
     *       The one thing which is critical is that the query controller is
     *       properly informed of the #of chunks made available to an operator
     *       and consumed by that operator, that those reports must be in the
     *       same units, and that the reports must be delivered back to the
     *       query controller in a manner which does not transiently violate the
     *       termination conditions of the query.
     */
    protected void sendChunkMessage(
            final UUID serviceUUID,
            final int sinkId,
            final int partitionId,
            final IAllocationContext allocationContext,
            final IBlockingBuffer<IBindingSet[]> source) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (allocationContext == null)
            throw new IllegalArgumentException();

        if (source == null)
            throw new IllegalArgumentException();

        if (source.isEmpty())
            throw new RuntimeException();

        // The peer to whom we send the message.
        final IQueryPeer peerProxy = getQueryPeer(serviceUUID);

        if (peerProxy == null)
            throw new RuntimeException("Not found: serviceId=" + serviceUUID);
        
        // true iff the target is this service (no proxy, no RMI).
        final boolean thisService = peerProxy == getQueryEngine();
        
        if(thisService) {

            /*
             * Leave the chunk as Java objects and drop it directly onto the
             * query engine.
             */

            final IChunkMessage<IBindingSet> msg = new LocalChunkMessage<IBindingSet>(
                    getQueryController(), getQueryId(), sinkId, partitionId,
                    source.iterator());

            if (log.isDebugEnabled())
                log.debug("Sending local message: " + msg);

            getQueryEngine().bufferReady(msg);
         
            return;
            
        }

        /*
         * We will be notifying another service (RMI) that a chunk is available.
         * 
         * Note: Depending on how much data it involved, we may move it with the
         * RMI message or out of band using NIO. This decision effects how we
         * serialize the chunk.
         */
        final IChunkMessage<IBindingSet> msg;
        if (source.size() < 100) {

            msg = new ThickChunkMessage<IBindingSet>(getQueryController(),
                    getQueryId(), sinkId, partitionId, source);

        } else {

            /*
             * Marshall the data onto direct ByteBuffer(s) and send a thin
             * message by RMI. The receiver will retrieve the data using NIO
             * against the ResourceService.
             */
            msg = new NIOChunkMessage<IBindingSet>(getQueryController(),
                    getQueryId(), sinkId, partitionId, allocationContext,
                    source, getQueryEngine().getResourceService().getAddr());

        }

        if (log.isDebugEnabled())
            log.debug("Sending remote message: " + msg);

        try {

            peerProxy.bufferReady(msg);

        } catch (RemoteException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean cancelQueryOnPeers(final Throwable cause) {

        boolean cancelled = super.cancelQueryOnPeers(cause);

        final UUID queryId = getQueryId();
        
        for (IQueryPeer peer : peers.values()) {

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

}
