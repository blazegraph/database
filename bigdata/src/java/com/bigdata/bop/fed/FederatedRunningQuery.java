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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IShardwisePipelineOp;
import com.bigdata.bop.engine.BindingSetChunk;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.io.DirectBufferPoolAllocator;
import com.bigdata.io.SerializerUtil;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocation;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocationContext;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.resources.ResourceManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;
import com.bigdata.striterator.IKeyOrder;

/**
 * Extends {@link RunningQuery} to provide additional state and logic required
 * to support distributed query evaluation against an {@link IBigdataFederation}
 * .
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: FederatedRunningQuery.java 3511 2010-09-06 20:45:37Z
 *          thompsonbry $
 * 
 * @todo SCALEOUT: We need to model the chunks available before they are
 *       materialized locally such that (a) they can be materialized on demand
 *       (flow control); and (b) we can run the operator when there are
 *       sufficient chunks available without taking on too much data.
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

    /**
     * The {@link UUID} of the service which is the {@link IQueryClient} running
     * this query.
     */
    private final UUID queryControllerUUID;
    
    /**
     * A map associating resources with running queries. When a query halts, the
     * resources listed in its resource map are released. Resources can include
     * {@link ByteBuffer}s backing either incoming or outgoing
     * {@link BindingSetChunk}s, temporary files associated with the query, hash
     * tables, etc.
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
     *       we want to make the references go through the {@link ResourceService}
     *       . For files, through the {@link ResourceManager}.
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
     * @todo Maintain multiple allocation contexts. Some can be query wide.
     *       Others might be specific to a serviceId and/or sinkId.
     */
    private final ConcurrentHashMap<Object/* key */, IAllocationContext> allocationContexts = new ConcurrentHashMap<Object, IAllocationContext>();
    
    public FederatedRunningQuery(FederatedQueryEngine queryEngine,
            long queryId, long readTimestamp, long writeTimestamp, long begin,
            long timeout, boolean controller, IQueryClient clientProxy,
            BOp query, IBlockingBuffer<IBindingSet[]> queryBuffer) {

        super(queryEngine, queryId, readTimestamp, writeTimestamp, begin,
                timeout, controller, clientProxy, query, queryBuffer);

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
        
    }

    @Override
    public FederatedQueryEngine getQueryEngine() {

        return (FederatedQueryEngine) super.getQueryEngine();

    }

    /**
     * The allocation context key groups together allocations onto the same
     * direct {@link ByteBuffer}s. There are different implementations depending
     * on how it makes sense to group data data for a given query.
     */
    static abstract private class AllocationContextKey {

        /**
         * Must be overridden. The queryId must be a part of each hashCode() in
         * order to ensure that the hash codes are well distributed across
         * different queries on the same node.
         */
        @Override
        abstract public int hashCode();

        /**
         * Must be overridden.
         */
        @Override
        abstract public boolean equals(Object o);

    }

    /**
     * An allocation context which is shared by all operators running in the
     * same query.
     */
    static private class QueryContext extends AllocationContextKey {
        private final Long queryId;

        QueryContext(final Long queryId) {
            this.queryId = Long.valueOf(queryId);
        }

        public int hashCode() {
            return queryId.hashCode();
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof QueryContext))
                return false;
            if (!queryId.equals(((QueryContext) o).queryId))
                return false;
            return true;
        }
    }

    /**
     * An allocation context which is shared by all operators running in the
     * same query which target the same service.
     */
    static private class ServiceContext extends AllocationContextKey {
        private final Long queryId;

        private final UUID serviceUUID;

        ServiceContext(final Long queryId, final UUID serviceUUID) {
            this.queryId = queryId;
            this.serviceUUID = serviceUUID;
        }

        public int hashCode() {
            return queryId.hashCode() * 31 + serviceUUID.hashCode();
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof ServiceContext))
                return false;
            if (!queryId.equals(((ServiceContext) o).queryId))
                return false;
            if (!serviceUUID.equals(((ServiceContext) o).serviceUUID))
                return false;
            return true;
        }
    }

    /**
     * An allocation context which is shared by all operators running in the
     * same query which target the same shard (the same shard implies the same
     * service, at least until we have HA with shard affinity).
     */
    static private class ShardContext extends AllocationContextKey {

        private final Long queryId;

        private final int partitionId;

        ShardContext(final Long queryId, final int partitionId) {
            this.queryId = queryId;
            this.partitionId = partitionId;
        }

        public int hashCode() {
            return queryId.hashCode() * 31 + partitionId;
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof ShardContext))
                return false;
            if (!queryId.equals(((ShardContext) o).queryId))
                return false;
            if (partitionId != partitionId)
                return false;
            return true;
        }
    }

    /**
     * Return the {@link IAllocationContext} for the given key.
     * 
     * @param key
     *            The key.
     *            
     * @return The allocation context.
     */
    private IAllocationContext getAllocationContext(
            final AllocationContextKey key) {

        return getQueryEngine().getResourceService().getAllocator()
                .getAllocationContext(key);

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
     */
    @Override
    protected <E> int add(final int sinkId,
            final IBlockingBuffer<IBindingSet[]> sink) {

        if (sink == null)
            throw new IllegalArgumentException();

        final BOp bop = bopIndex.get(sinkId);

        if (bop == null)
            throw new IllegalArgumentException();

        switch (bop.getEvaluationContext()) {
        case ANY: {
            return super.add(sinkId, sink);
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
             * @todo For a pipeline join, the predicate is the right hand
             * operator of the sink. This might be true for INSERT and DELETE
             * operators as well. [It is not, but make it so and document this
             * pattern or have a common interface method which returns the
             * IPredicate regardless of whether it is an operand or an
             * annotation.]
             * 
             * Note: IKeyOrder tells us which index will be used and should be
             * set on the predicate by the join optimizer.
             * 
             * @todo Use the read or write timestamp depending on whether the
             * operator performs mutation [this must be part of the operator
             * metadata.]
             * 
             * @todo Set the capacity of the the "map" buffer to the size of the
             * data contained in the sink (in fact, we should just process the
             * sink data in place).
             */
            @SuppressWarnings("unchecked")
            final IPredicate<E> pred = ((IShardwisePipelineOp) bop).getPredicate();
            final IKeyOrder<E> keyOrder = pred.getKeyOrder();
            final long timestamp = getReadTimestamp(); // @todo read vs write timestamp.
            final int capacity = 1000;// @todo
            final int capacity2 = 1000;// @todo
            final MapBindingSetsOverShardsBuffer<IBindingSet, E> mapper = new MapBindingSetsOverShardsBuffer<IBindingSet, E>(
                    getFederation(), pred, keyOrder, timestamp, capacity) {
                @Override
                IBuffer<IBindingSet> newBuffer(PartitionLocator locator) {
                    return new BlockingBuffer<IBindingSet>(capacity2);
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
                    sink.close();
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
             * to minimize copying or visiting in the data.
             */
            for (Map.Entry<PartitionLocator, IBuffer<IBindingSet>> e : mapper
                    .getSinks().entrySet()) {

                final PartitionLocator locator = e.getKey();
                
                final IBuffer<IBindingSet> shardSink = e.getValue();

                // FIXME harmonize IBuffer<IBindingSet> vs IBuffer<IBindingSet[]>
//                sendOutputChunkReadyMessage(newOutputChunk(locator
//                        .getDataServiceUUID(), sinkId, allocationContext,
//                        shardSink));
                throw new UnsupportedOperationException();
            }
            
        }
        case CONTROLLER: {

            /*
             * Format the binding sets onto a ByteBuffer and publish that
             * ByteBuffer as a manager resource for the query and notify the
             * query controller that data is available for it.
             */

            final IAllocationContext allocationContext = getAllocationContext(new QueryContext(
                    getQueryId()));

            sendOutputChunkReadyMessage(newOutputChunk(queryControllerUUID,
                    sinkId, allocationContext, sink));

        }
        default:
            throw new AssertionError(bop.getEvaluationContext());
        }

    }

    /**
     * Create an {@link OutputChunk} from some intermediate results.
     * 
     * @param serviceUUID
     *            The {@link UUID} of the {@link IQueryPeer} who is the
     *            recipient.
     * @param sinkId
     *            The identifier of the target {@link BOp}.
     * @param allocationContext
     *            The allocation context within which the {@link ByteBuffer}s
     *            will be managed for this {@link OutputChunk}.
     * @param source
     *            The binding sets to be formatted onto a buffer.
     * 
     * @return The {@link OutputChunk}.
     */
    protected OutputChunk newOutputChunk(
            final UUID serviceUUID,
            final int sinkId,
            final IAllocationContext allocationContext,
            final IBlockingBuffer<IBindingSet[]> source) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (allocationContext == null)
            throw new IllegalArgumentException();

        if (source == null)
            throw new IllegalArgumentException();

        int nbytes = 0;
        
        final List<IAllocation> allocations = new LinkedList<IAllocation>();
        
        final IAsynchronousIterator<IBindingSet[]> itr = source.iterator();
        
        try {

            while (itr.hasNext()) {

                // Next chunk to be serialized.
                final IBindingSet[] chunk = itr.next();
                
                // serialize the chunk of binding sets.
                final byte[] data = SerializerUtil.serialize(chunk);
                
                // track size of the allocations.
                nbytes += data.length;

                // allocate enough space for those data.
                final IAllocation[] tmp;
                try {
                    tmp = allocationContext.alloc(data.length);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }

                // copy the data into the allocations.
                DirectBufferPoolAllocator.put(data, tmp);

                // append the new allocations.
                allocations.addAll(Arrays.asList(tmp));

            }

        } finally {

            itr.close();

        }

        return new OutputChunk(getQueryId(), serviceUUID, sinkId, nbytes,
                allocations);

    }
    
    protected IQueryPeer getQueryPeer(final UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();
        
        final IQueryPeer queryPeer;
        
        if (serviceUUID.equals(queryControllerUUID)) {
        
            // The target is the query controller.
            queryPeer = getQueryController();
            
        } else {
            
            // The target is some data service.
            queryPeer = getQueryEngine().getQueryPeer(serviceUUID);
            
        }

        return queryPeer;

    }

    /**
     * Notify a remote {@link IQueryPeer} that data is available for it.
     * 
     * @todo If the target for the {@link OutputChunk} is this node then just
     *       drop it onto the {@link QueryEngine}.
     * 
     * @todo Report the #of bytes available with this message. However, first
     *       figure out if that if the #of bytes in this {@link OutputChunk} or
     *       across all {@link OutputChunk}s available for the target service
     *       and sink.
     * 
     * @todo Consider a fast path with inline RMI based transfer for small sets
     *       of data. We might just serialize to a byte[] and send that directly
     *       using a different message to notify the {@link IQueryPeer}.
     */
    protected void sendOutputChunkReadyMessage(final OutputChunk outputChunk) {
       
        try {

            // The peer to be notified.
            final IQueryPeer peerProxy = getQueryPeer(outputChunk.serviceId);

            // The Internet address and port where the peer can read the data
            // from this node.
            final InetSocketAddress serviceAddr = getQueryEngine()
                    .getResourceService().getAddr();

            peerProxy.bufferReady(getQueryController(), serviceAddr,
                    getQueryId(), outputChunk.sinkId);

        } catch (RemoteException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * A chunk of outputs.
     * 
     * @todo We probably need to use the {@link DirectBufferPoolAllocator} to
     *       receive the chunks within the {@link ManagedResourceService} as
     *       well.
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
     * @todo There are a few things where the resource must be made available to
     *       more than one operator evaluation phase. The best examples are
     *       temporary graphs for parallel closure and large collections of
     *       graphIds for SPARQL "NAMED FROM DATA SET" extensions.
     */
    private static class OutputChunk {

        final long queryId;

        final UUID serviceId;

        final int sinkId;

        final int nbytes;

        final List<IAllocation> allocations;

        public OutputChunk(final long queryId, final UUID serviceId,
                final int sinkId, final int nbytes,
                final List<IAllocation> allocations) {

            this.queryId = queryId;
            this.serviceId = serviceId;
            this.sinkId = sinkId;
            this.nbytes = nbytes;
            this.allocations = allocations;

        }

    }
    
}
