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
 * Created on Oct 22, 2010
 */

package com.bigdata.bop.fed;

import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IShardwisePipelineOp;
import com.bigdata.bop.engine.IChunkHandler;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.LocalChunkMessage;
import com.bigdata.bop.engine.StandaloneChunkHandler;
import com.bigdata.bop.fed.shards.MapBindingSetsOverShardsBuffer;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocationContext;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.eval.pipeline.DistributedJoinTask;

/**
 * The base class is extended to organize the output from one operator so in
 * order to make it available to another operator running on a different node.
 * There are several cases which have to be handled and which are identified by
 * the {@link BOp#getEvaluationContext()}. In addition, we need to handle low
 * latency and high data volume queries somewhat differently. Except for
 * {@link BOpEvaluationContext#ANY}, all of these cases wind up writing the
 * intermediate results onto a direct {@link ByteBuffer} and notifying the
 * receiving service that there are intermediate results which it can pull when
 * it is ready to process them. This pattern allows the receiver to impose flow
 * control on the producer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: FederationChunkHandler.java 6038 2012-02-17 17:43:26Z
 *          thompsonbry $
 * @param <E>
 *            The generic type of the objects in the relation.
 * 
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/488">Vector
 *      query engine messages per node</a>
 */
public class FederationChunkHandler<E> extends StandaloneChunkHandler {

    private final static Logger log = Logger
            .getLogger(FederationChunkHandler.class); 
    
    /**
     * FIXME Debug the NIO chunk message materialization logic (it is currently
     * disabled by the setting of the nioThreshold parameter to the
     * constructor).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/160">
     *      ResourceService should use NIO for file and buffer transfers</a>
     * 
     * @see <a
     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/486">Support
     *      NIO solution set interchange on the cluster</a>
     */
    @SuppressWarnings("rawtypes")
    public static final IChunkHandler INSTANCE = new FederationChunkHandler(
            Integer.MAX_VALUE/* nioThreshold */, false/*usePOJO*/);

    /**
     * Instance used by some test suites to avoid a dependency on the RDF data
     * model. All messages will use {@link LocalChunkMessage} which uses POJO
     * serialization.
     */
    @SuppressWarnings("rawtypes")
    public static final IChunkHandler TEST_INSTANCE = new FederationChunkHandler(
            Integer.MAX_VALUE/* nioThreshold */, true/*usePOJO*/);

    /**
     * The threshold above which the intermediate solutions are shipped using
     * NIO rather than RMI.
     * 
     * @see ThickChunkMessage
     * @see NIOChunkMessage
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/462">
     *      Invalid byte: 3 in ResourceService (StatusEnum) on cluster </a>
     */
    private final int nioThreshold;
    
    /**
     * When <code>true</code>, the {@link LocalChunkMessage} will be used for
     * all messages.  This allows the test cases to avoid RDF specific logic
     * in the {@link IChunkMessage} serialization.
     */
    private final boolean usePOJO;

    /**
     * @param nioThreshold
     *            The threshold above which the intermediate solutions are
     *            shipped using NIO rather than RMI. This is ignored if
     *            <code>usePOJO:=true</code>.
     * @param usePOJO
     *            When <code>true</code>, the {@link LocalChunkMessage} will be
     *            used for all messages. This allows the test cases to avoid RDF
     *            specific logic in the {@link IChunkMessage} serialization.
     */
    public FederationChunkHandler(final int nioThreshold, final boolean usePOJO) {
        
        /*
         * Note: The use of the native heap storage of the solutions in
         * combination with scale-out is untested.
         */
        super(false/*nativeHeap*/);
        
        this.nioThreshold = nioThreshold;
        
        this.usePOJO = usePOJO;
        
    }
    
    /**
     * {@inheritDoc}
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
    public int handleChunk(final IRunningQuery query, final int bopId,
            final int sinkId, final IBindingSet[] chunk) {

        if (query == null)
            throw new IllegalArgumentException();

        if (chunk == null)
            throw new IllegalArgumentException();

        if (chunk.length == 0)
            return 0;

        final FederatedRunningQuery q = (FederatedRunningQuery) query; 
        
        final BOp targetOp = q.getBOpIndex().get(sinkId);

        if (targetOp == null)
            throw new IllegalStateException("Not found: " + sinkId);

        if(log.isTraceEnabled())
            log.trace("queryId=" + query.getQueryId() + ", sourceBopId="+bopId+", sink=" + sinkId);
        
        switch (targetOp.getEvaluationContext()) {
        case ANY: {
            /*
             * This operator may be evaluated anywhere.
             */
            return super.handleChunk(query, bopId, sinkId, chunk);
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
             * of the operator's outputs. [Actually, we wind up re-combining the
             * chunks into a single chunk per target shard below.]
             */
            @SuppressWarnings("unchecked")
            final IPredicate<E> pred = ((IShardwisePipelineOp<E>) targetOp).getPredicate();
            final long timestamp = pred.getTimestamp(); 
            final int capacity = 1000;// @todo
            // FIXME Capacity is unbounded to prevent deadlock. See the node below.
            final int chunkOfChunksCapacity = Integer.MAX_VALUE;
            final int chunkSize = 100;// @todo modest chunks.
            final MapBindingSetsOverShardsBuffer<IBindingSet, E> mapper = new MapBindingSetsOverShardsBuffer<IBindingSet, E>(
                    q.getFederation(), pred, timestamp, capacity) {
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
//                final IAsynchronousIterator<IBindingSet[]> itr = sink
//                        .iterator();
//                try {
//                    while (itr.hasNext()) {
//                        final IBindingSet[] chunk = itr.next();
//                        for (IBindingSet bset : chunk) {
//                            mapper.add(bset);
//                        }
//                    }
//                } finally {
//                    itr.close();
//                    mapper.flush();
//                }
                for (IBindingSet bset : chunk) {
                    mapper.add(bset);
                }
                mapper.flush();
            }
            /*
             * The allocation context.
             * 
             * @todo use (queryId, serviceId, sinkId) when the target bop is
             * high volume operator (this requires annotation by the query
             * planner of the operator tree).
             */
            final IAllocationContext allocationContext = q
                    .getAllocationContext(new QueryContext(q.getQueryId()));

            /*
             * Generate the output chunks and notify the receivers.
             * 
             * FIXME If the output buffer has a bounded capacity then this can
             * deadlock when the buffer fills up because we are not draining the
             * buffer until the chunk has been fully mapped. This stage should
             * probably be integrated with the stage which maps the binding sets
             * over the shards (immediately above) to minimize copying or
             * visiting in the data. This could be done by hooking the method
             * which outputs a chunk to instead directly send the IChunkMessage.
             * We could also simplify the API from IBlockingBuffer to something
             * much thinner, such as add(IBindingSet[] chunk).
             * 
             * TODO We should report the time spent mapping chunks out to the
             * QueryLog. That could be done through an extension of BOpStats.
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

                // drain buffer to a single chunk.
                final IBindingSet[] a;
                {
                    int n = 0;
                    final List<IBindingSet[]> lst = new LinkedList<IBindingSet[]>();
                    final IAsynchronousIterator<IBindingSet[]> itr = shardSink.iterator();
                    try {
                        while (itr.hasNext()) {
                            final IBindingSet[] t = itr.next();
                            lst.add(t);
                            n += t.length;
                        }
                    } finally {
                        itr.close();
                    }
                    a = new IBindingSet[n];
                    int i = 0;
                    for(IBindingSet[] t : lst) {
                        System
                                .arraycopy(t/* src */, 0/* srcPos */,
                                        a/* dest */, i/* destPos */, t.length/* length */);
                        i += t.length;
                    }
                }

                if (a.length > 0) {

                    /*
                     * Send message.
                     * 
                     * Note: This avoids sending empty chunks.
                     * 
                     * @see https://sourceforge.net/apps/trac/bigdata/ticket/492
                     * (Empty chunk in ThickChunkMessage (cluster))
                     */
                    
                    // send message.
                    sendChunkMessage(q, locator.getDataServiceUUID(), sinkId,
                            locator.getPartitionId(), allocationContext, a);

                    // #of messages sent.
                    messageSendCount++;
                    
                }

            }

            return messageSendCount;

        }
        case CONTROLLER: {

            /*
             * Format the binding sets onto a ByteBuffer and publish that
             * ByteBuffer as a manager resource for the query and notify the
             * query controller that data is available for it.
             */

            final IAllocationContext allocationContext = q.getAllocationContext(new QueryContext(
                    q.getQueryId()));

            sendChunkMessage(q, q.queryControllerUUID, sinkId,
                    -1/* partitionId */, allocationContext, chunk);

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
            final FederatedRunningQuery q,
            final UUID serviceUUID,
            final int sinkId,
            final int partitionId,
            final IAllocationContext allocationContext,
            final IBindingSet[] source) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (allocationContext == null)
            throw new IllegalArgumentException();

        if (source == null)
            throw new IllegalArgumentException();

//        if (source.isEmpty())
//            throw new RuntimeException();

        // The peer to whom we send the message.
        final IQueryPeer peerProxy = q.getQueryPeer(serviceUUID);

        if (peerProxy == null)
            throw new RuntimeException("Not found: serviceId=" + serviceUUID);
        
        // true iff the target is this service (no proxy, no RMI).
        final boolean thisService = peerProxy == q.getQueryEngine();
        
        if(thisService) {

            /*
             * Leave the chunk as Java objects and drop it directly onto the
             * query engine.
             */

            final IChunkMessage<IBindingSet> msg = new LocalChunkMessage(
                    q.getQueryController(), //
                    q.getQueryId(), //
                    sinkId, //
                    partitionId, //
                    source);

            if (log.isDebugEnabled())
                log.debug("Sending local message: " + msg);

            /*
             * The message is fully materialized and will not cross a machine
             * boundary. Drop it directly onto the work queue on this
             * QueryEngine rather than handing it off to the QueryEngine. This
             * is more efficient and also prevents counting messages which
             * target the same query controller as inter-controller messages.
             */
            // drop the message onto the IRunningQuery
            q.acceptChunk(msg);
            // drop the message onto the QueryEngine.
            // q.getQueryEngine().bufferReady(msg);
         
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
        if (usePOJO) {
        
            msg = new LocalChunkMessage(q.getQueryController(), q.getQueryId(),
                    sinkId, partitionId, source);
            
        } else {
            
            if (source.length <= nioThreshold) {

                msg = new ThickChunkMessage<IBindingSet>(
                        q.getQueryController(), q.getQueryId(), sinkId,
                        partitionId, source);

            } else {

                /*
                 * Marshall the data onto direct ByteBuffer(s) and send a thin
                 * message by RMI. The receiver will retrieve the data using NIO
                 * against the ResourceService.
                 */
                msg = new NIOChunkMessage<IBindingSet>(q.getQueryController(),
                        q.getQueryId(), sinkId, partitionId, allocationContext,
                        source, q.getQueryEngine().getResourceService()
                                .getAddr());

            }
        
        }

        if (log.isDebugEnabled())
            log.debug("Sending remote message: " + msg);

        // Update counters since message will cross machine boundary.
        final FederatedQueryEngineCounters c = q.getQueryEngine()
                .getQueryEngineCounters();
        c.chunksOut.increment();
        c.solutionsOut.add(source.length);

        try {

            peerProxy.bufferReady(msg);

        } catch (RemoteException e) {

            throw new RuntimeException(e);

        }

    }

}
