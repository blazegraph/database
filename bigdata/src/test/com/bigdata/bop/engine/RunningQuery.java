/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 31, 2010
 */
package com.bigdata.bop.engine;

import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.resources.ResourceManager;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Metadata about running queries.
 * 
 * @todo HA aspects of running queries?  Checkpoints for long running queries?
 */
public class RunningQuery implements Future<Map<Integer,BOpStats>> {

    private final static transient Logger log = Logger
            .getLogger(RunningQuery.class);

    /**
     * The run state of the query and the result of the computation iff it
     * completes execution normally (without being interrupted, cancelled, etc).
     */
    final private Haltable<Map<Integer,BOpStats>> future = new Haltable<Map<Integer,BOpStats>>();

    /**
     * The runtime statistics for each {@link BOp} in the query and
     * <code>null</code> unless this is the query controller.
     */
    final private ConcurrentHashMap<Integer/* bopId */, BOpStats> statsMap;

    /**
     * The class executing the query on this node.
     */
    final QueryEngine queryEngine;

    /** The unique identifier for this query. */
    final long queryId;

    /**
     * The timestamp or transaction identifier against which the query is
     * reading.
     */
    final long readTimestamp;

    /**
     * The timestamp or transaction identifier against which the query is
     * writing.
     */
    final long writeTimestamp;

    /**
     * The timestamp when the query was accepted by this node (ms).
     */
    final long begin;

    /**
     * How long the query is allowed to run (elapsed milliseconds) -or-
     * {@link Long#MAX_VALUE} if there is no deadline.
     */
    final long timeout;

    /**
     * <code>true</code> iff the outer {@link QueryEngine} is the controller for
     * this query.
     */
    final boolean controller;

    /**
     * The client executing this query.
     */
    final IQueryClient clientProxy;

    /** The query iff materialized on this node. */
    final AtomicReference<BOp> queryRef;

    /**
     * The buffer used for the overall output of the query pipeline.
     * 
     * @todo How does the pipeline get attached to this buffer? Via a special
     *       operator? Or do we just target the coordinating {@link QueryEngine}
     *       as the sink of the last operator so we can use NIO transfers?
     */
    final IBlockingBuffer<IBindingSet[]> queryBuffer;

    /**
     * A map associating resources with running queries. When a query halts, the
     * resources listed in its resource map are released. Resources can include
     * {@link ByteBuffer}s backing either incoming or outgoing
     * {@link BindingSetChunk}s, temporary files associated with the query, hash
     * tables, etc.
     * 
     * @todo Cache any resources materialized for the query on this node (e.g.,
     *       temporary graphs materialized from a peer or the client). A bop
     *       should be able to demand those data from the cache and otherwise
     *       have them be materialized.
     * 
     * @todo only use the values in the map for transient objects, such as a
     *       hash table which is not backed by the disk. For {@link ByteBuffer}s
     *       we want to make the references go through the {@link BufferService}
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
     * The chunks available for immediate processing.
     */
    final BlockingQueue<BindingSetChunk> chunksIn = new LinkedBlockingDeque<BindingSetChunk>();

    /**
     * An index from the {@link BOp.Annotations#BOP_ID} to the {@link BOp}.
     */
    private final Map<Integer, BOp> bopIndex;

    /**
     * A collection of the currently executing future for operators for this
     * query.
     */
    private final ConcurrentHashMap<Future<?>, Future<?>> operatorFutures = new ConcurrentHashMap<Future<?>, Future<?>>();

    /**
     * A lock guarding {@link #runningTaskCount} and
     * {@link #availableChunkCount}.
     */
    private final Lock runStateLock = new ReentrantLock();

    /**
     * The #of tasks for this query which have started but not yet halted and
     * ZERO (0) if this is not the query coordinator.
     */
    private long runningTaskCount = 0;

    /**
     * The #of chunks for this query of which a running task has made available
     * but which have not yet been accepted for processing by another task and
     * ZERO (0) if this is not the query coordinator.
     */
    private long availableChunkCount = 0;

    /**
     * Return <code>true</code> iff this is the query controller.
     */
    public boolean isController() {
    
        return controller;
        
    }

    /**
     * Return the current statistics for the query and <code>null</code> unless
     * this is the query controller.
     * 
     * @todo When the query is done, there will be one entry in this map for
     *       each operator in the pipeline. Non-pipeline operators such as
     *       {@link Predicate}s do not currently make it into this map.
     */
    public Map<Integer/*bopId*/,BOpStats> getStats() {
        
        return statsMap;
        
    }

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
            final long begin, final long timeout, final boolean controller,
            final IQueryClient clientProxy, final BOp query,
            final IBlockingBuffer<IBindingSet[]> queryBuffer) {
        this.queryEngine = queryEngine;
        this.queryId = queryId;
        this.readTimestamp = readTimestamp;
        this.writeTimestamp = writeTimestamp;
        this.begin = begin;
        this.timeout = timeout;
        this.controller = controller;
        this.clientProxy = clientProxy;
        this.queryRef = new AtomicReference<BOp>(query);
        if (controller && query == null)
            throw new IllegalArgumentException();
        this.queryBuffer = queryBuffer;
        this.bopIndex = BOpUtility.getIndex(query);
        this.statsMap = controller ? new ConcurrentHashMap<Integer, BOpStats>()
                : null;
    }

    /**
     * Create a {@link BindingSetChunk} from a sink and add it to the queue.
     * 
     * @param sinkId
     * @param sink
     * @return The #of chunks made available for consumption by the sink. This
     *         will always be ONE (1) for scale-up. For scale-out, there will be
     *         one chunk per index partition over which the intermediate results
     *         were mapped.
     * 
     * @todo In scale-out, this is where we need to map the binding sets over
     *       the shards for the target operator. Once they are mapped, write the
     *       binding sets onto an NIO buffer for the target node and then send
     *       an RMI message to the node telling it that there is a chunk
     *       available for the given (queryId,bopId,partitionId).
     * 
     * @todo If we are running standalone, then do not format the data into a
     *       ByteBuffer.
     *       <p>
     *       For selective queries in s/o, first format the data onto a list of
     *       byte[]s, one per target shard/node. Then, using a lock, obtain a
     *       ByteBuffer if there is none associated with the query yet.
     *       Otherwise, using the same lock, obtain a slice onto that ByteBuffer
     *       and put as much of the byte[] as will fit, continuing onto a newly
     *       recruited ByteBuffer if necessary. Release the lock and notify the
     *       target of the ByteBuffer slice (buffer#, off, len). Consider
     *       pushing the data proactively for selective queries.
     *       <p>
     *       For unselective queries in s/o, proceed as above but we need to get
     *       the data off the heap and onto the {@link ByteBuffer}s quickly
     *       (incrementally) and we want the consumers to impose flow control on
     *       the producers to bound the memory demand (this needs to be
     *       coordinated carefully to avoid deadlocks). Typically, large result
     *       sets should result in multiple passes over the consumer's shard
     *       rather than writing the intermediate results onto the disk.
     */
    private int add(final int sinkId, final IBlockingBuffer<IBindingSet[]> sink) {

        /*
         * Note: The partitionId will always be -1 in scale-up.
         */
        final BindingSetChunk chunk = new BindingSetChunk(queryId, sinkId,
                -1/* partitionId */, sink.iterator());

        queryEngine.add(chunk);
        
        return 1;
        
    }

    /**
     * Make a chunk of binding sets available for consumption by the query.
     * <p>
     * Note: this is invoked by {@link QueryEngine#add(BindingSetChunk)}.
     * 
     * @param chunk
     *            The chunk.
     */
    void add(final BindingSetChunk chunk) {

        if (chunk == null)
            throw new IllegalArgumentException();

        // verify still running.
        future.halted();

        // add chunk to be consumed.
        chunksIn.add(chunk);

        if (log.isDebugEnabled())
            log.debug("queryId=" + queryId + ", chunksIn.size()="
                    + chunksIn.size());

    }

    /**
     * Atomically update the #of available chunks for processing on the query
     * controller node (this is invoked by the query controller for the chunk(s)
     * which are the input to a query).
     */
    public void startQuery(final BindingSetChunk chunk) {
        if (!controller)
            throw new UnsupportedOperationException();
        if(chunk==null)
            throw new IllegalArgumentException();
        if (chunk.queryId != queryId) // @todo equals() if queryId is UUID.
            throw new IllegalArgumentException();
        runStateLock.lock();
        try {
            availableChunkCount++;
            if (log.isInfoEnabled())
                log.info("queryId=" + queryId + ", availableChunks="
                        + availableChunkCount);
            System.err.println("startQ : bopId="+chunk.bopId+",running="+runningTaskCount+",available="+availableChunkCount);
            queryEngine.add(chunk);
        } finally {
            runStateLock.unlock();
        }
    }

    /**
     * Message provides notice that the operator has started execution.
     * 
     * @param bopId
     *            The identifier of the operator.
     * @param partitionId
     *            The index partition identifier against which the operator is
     *            executing.
     * @param serviceId
     *            The identifier of the service on which the operator is
     *            executing.
     * @param fanIn
     *            The #of chunks that will be consumed by the operator
     *            execution.
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     */
    public void startOp(final int bopId, final int partitionId,
            final UUID serviceId, final int fanIn) {
        if (!controller)
            throw new UnsupportedOperationException();
        runStateLock.lock();
        try {
            runningTaskCount++;
            availableChunkCount -= fanIn;
            System.err.println("startOp: bopId="+bopId+",running="+runningTaskCount+",available="+availableChunkCount+",fanIn="+fanIn);
            final long elapsed = System.currentTimeMillis() - begin;
            if (log.isTraceEnabled())
                log.trace("bopId=" + bopId + ",partitionId=" + partitionId
                        + ",serviceId=" + serviceId + " : runningTaskCount="
                        + runningTaskCount + ", availableChunkCount="
                        + availableChunkCount + ", elapsed=" + elapsed);
            if (elapsed > timeout) {
                future.halt(new TimeoutException());
                cancel(true/* mayInterruptIfRunning */);
            }
        } finally {
            runStateLock.unlock();
        }
    }

    /**
     * Message provides notice that the operator has ended execution. The
     * termination conditions for the query are checked. (For scale-out, the
     * node node controlling the query needs to be involved for each operator
     * start/stop in order to make the termination decision atomic).
     * 
     * @param bopId
     *            The identifier of the operator.
     * @param partitionId
     *            The index partition identifier against which the operator was
     *            executing.
     * @param cause
     *            The cause and <code>null</code> if the operator halted
     *            normally.
     * @param serviceId
     *            The identifier of the service on which the operator was
     *            executing.
     * @param fanOut
     *            The #of chunks which were made available to the downstream
     *            sink(s).
     * @param taskStats
     *            The statistics for the execution of the bop against the
     *            partition on the service.
     * @throws UnsupportedOperationException
     *             If this node is not the query coordinator.
     * 
     * @todo Do not release buffer with query results until consumed by client.
     * 
     * @todo Clone the {@link BOpStats} before reporting to avoid concurrent
     *       modification?
     */
    public void haltOp(final int bopId, final int partitionId,
            final UUID serviceId, final Throwable cause,
            final int fanOut, final BOpStats taskStats) {
        if (!controller)
            throw new UnsupportedOperationException();
        runStateLock.lock();
        try {
            // update per-operator statistics.
            {
                final BOpStats stats = statsMap.get(bopId);
                if (stats == null) {
                    statsMap.put(bopId, taskStats);
                } else {
                    stats.add(taskStats);
                }
            }
            /*
             * Update termination criteria counters.
             */
            // chunks generated by this task.
            availableChunkCount += fanOut;
            // one less task is running.
            runningTaskCount--;
            System.err.println("haltOp : bopId="+bopId+",running="+runningTaskCount+",available="+availableChunkCount+",fanOut="+fanOut);
            assert runningTaskCount >= 0 : "runningTaskCount="
                    + runningTaskCount;
            assert availableChunkCount >= 0 : "availableChunkCount="
                    + availableChunkCount;
            final long elapsed = System.currentTimeMillis() - begin;
            if (log.isTraceEnabled())
                log.trace("bopId=" + bopId + ",partitionId=" + partitionId
                        + ",serviceId=" + queryEngine.getServiceId()
                        + ", nchunks=" + fanOut + " : runningTaskCount="
                        + runningTaskCount + ", availableChunkCount="
                        + availableChunkCount + ", elapsed=" + elapsed);
            // test termination criteria
            if (cause != null) {
                // operator failed on this chunk.
                log.error("Error: Canceling query: queryId=" + queryId
                        + ",bopId=" + bopId + ",partitionId="
                        + partitionId, cause);
                future.halt(cause);
                cancel(true/* mayInterruptIfRunning */);
            } else if (runningTaskCount == 0 && availableChunkCount == 0) {
                // success (all done).
                future.halt(getStats());
                cancel(true/* mayInterruptIfRunning */);
            } else if (elapsed > timeout) {
                // timeout
                future.halt(new TimeoutException());
                cancel(true/* mayInterruptIfRunning */);
            }
        } finally {
            runStateLock.unlock();
        }
    }

    /**
     * Return a {@link FutureTask} which will consume the binding set chunk. The
     * caller must run the {@link FutureTask}.
     * 
     * @param chunk
     *            A chunk to be consumed.
     */
    @SuppressWarnings("unchecked")
    protected FutureTask<Void> newChunkTask(final BindingSetChunk chunk) {
        /*
         * Look up the BOp in the index, create the BOpContext for that BOp, and
         * return the value returned by BOp.eval(context).
         */
        final BOp bop = bopIndex.get(chunk.bopId);
        if (bop == null) {
            throw new NoSuchBOpException(chunk.bopId);
        }
        if (!(bop instanceof BindingSetPipelineOp)) {
            /*
             * @todo evaluation of element[] pipelines needs to use pretty much
             * the same code, but it needs to be typed for E[] rather than
             * IBindingSet[].
             */
            throw new UnsupportedOperationException(bop.getClass().getName());
        }
        // self
        final BindingSetPipelineOp op = ((BindingSetPipelineOp) bop);
        // parent (null if this is the root of the operator tree).
        final BOp p = BOpUtility.getParent(queryRef.get(), op);
        // sink (null unless parent is defined)
        final Integer sinkId = p == null ? null : (Integer) p
                .getProperty(BindingSetPipelineOp.Annotations.BOP_ID);
        final IBlockingBuffer<IBindingSet[]> sink = (p == null ? queryBuffer
                : op.newBuffer());
        // altSink [@todo altSink=null or sink when not specified?]
        final Integer altSinkId = (Integer) op
                .getProperty(BindingSetPipelineOp.Annotations.ALT_SINK_REF);
        if (altSinkId != null && !bopIndex.containsKey(altSinkId)) {
            throw new NoSuchBOpException(altSinkId);
        }
        final IBlockingBuffer<IBindingSet[]> altSink = altSinkId == null ? null
                : op.newBuffer();
        // context
        final BOpContext context = new BOpContext(queryEngine.getFederation(),
                queryEngine.getLocalIndexManager(), readTimestamp,
                writeTimestamp, chunk.partitionId, op.newStats(), chunk.source,
                sink, altSink);
        // FutureTask for operator execution (not running yet).
        final FutureTask<Void> f = op.eval(context);
        // Hook the FutureTask.
        final Runnable r = new Runnable() {
            public void run() {
                final UUID serviceId = queryEngine.getServiceId();
                /*
                 * @todo SCALEOUT: Combine chunks available on the queue for the
                 * current bop. This is not exactly "fan in" since multiple
                 * chunks could be available in scaleup as well.
                 */
                int fanIn = 1;
                int fanOut = 0;
                try {
                    clientProxy.startOp(queryId, chunk.bopId,
                            chunk.partitionId, serviceId, fanIn);
                    if (log.isDebugEnabled())
                        log.debug("Running chunk: queryId=" + queryId
                                + ", bopId=" + chunk.bopId + ", bop=" + bop);
                    f.run(); // run
                    f.get(); // verify success
                    if (sink != queryBuffer && !sink.isEmpty()) {
                        // handle output chunk.
                        fanOut += add(sinkId, sink);
                    }
                    if (altSink != queryBuffer && altSink != null
                            && !altSink.isEmpty()) {
                        // handle alt sink output chunk.
                        fanOut += add(altSinkId, altSink);
                    }
                    clientProxy.haltOp(queryId, chunk.bopId, chunk.partitionId,
                            serviceId, null/* cause */, fanOut, context
                                    .getStats());
                } catch (Throwable t) {
                    try {
                        clientProxy.haltOp(queryId, chunk.bopId,
                                chunk.partitionId, serviceId, t/* cause */,
                                fanOut, context.getStats());
                    } catch (RemoteException e) {
                        cancel(true/* mayInterruptIfRunning */);
                        log.error("queryId=" + queryId, e);
                    }
                }
            }
        };
        // wrap runnable.
        final FutureTask<Void> f2 = new FutureTask(r, null/* result */);
        // add to list of active futures for this query.
        operatorFutures.put(f2, f2);
        // return : caller will execute.
        return f2;
    }

    /**
     * Return an iterator which will drain the solutions from the query. The
     * query will be cancelled if the iterator is
     * {@link ICloseableIterator#close() closed}.
     * 
     * @return
     * 
     * @todo Do all queries produce solutions (mutation operations might return
     *       a mutation count, but they do not return solutions).
     * 
     *       FIXME Track chunks consumed by the client.
     */
    public IAsynchronousIterator<IBindingSet[]> iterator() {

        return queryBuffer.iterator();
        
    }

    /*
     * Future
     * 
     * Note: This is implemented using delegation to the Haltable so we can hook
     * various methods in order to clean up the state of a completed query.
     */

    /**
     * @todo Cancelled queries must reject or drop new chunks, etc. Queries must
     *       release all of their resources when they are done().
     */
    final public boolean cancel(final boolean mayInterruptIfRunning) {
        // halt the query.
        boolean cancelled = future.cancel(mayInterruptIfRunning);
        // cancel any running operators for this query.
        for (Future<?> f : operatorFutures.keySet()) {
            if (f.cancel(mayInterruptIfRunning))
                cancelled = true;
        }
        if (queryBuffer != null) {
            // close the output sink.
            queryBuffer.close();
        }
        // remove from the set of running queries.
        queryEngine.runningQueries.remove(queryId, this);
        // release any resources for this query.
        queryEngine.releaseResources(this);
        // true iff we cancelled something.
        return cancelled;
    }

    final public Map<Integer, BOpStats> get() throws InterruptedException,
            ExecutionException {
        return future.get();
    }

    final public Map<Integer, BOpStats> get(long arg0, TimeUnit arg1)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(arg0, arg1);
    }

    final public boolean isCancelled() {
        return future.isCancelled();
    }

    final public boolean isDone() {
        return future.isDone();
    }

}
