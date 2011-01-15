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
 * Created on Dec 31, 2010
 */

package com.bigdata.bop.engine;

import java.nio.channels.ClosedByInterruptException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.BufferAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NoSuchBOpException;
import com.bigdata.bop.PipelineOp;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.MultiplexBlockingBuffer;
import com.bigdata.util.InnerCause;

/**
 * An {@link IRunningQuery} implementation for a standalone database in which a
 * {@link BlockingQueue} is arranged in front of each operator task and each
 * operator task runs exactly once. While individual operators can buffer their
 * inputs if they need to run "operator-at-once", the dominant paradigm for this
 * class is pipelined evaluation. The pipeline will contain chunks of binding
 * sets, but each operator task will run exactly once and will run until its
 * input(s) have been closed. Since binding sets can be routed through the
 * default and the alternative sink, it is possible for there to be more than
 * one source which targets a given operator as its sink. These sources are
 * identified in advance and a counter is established which reflects the #of
 * sources writing on a given sink. The sink will consider its sources closed
 * only when that counter has been decremented to zero, indicating that all
 * sources for the sink have been closed.
 * <p>
 * This implementation does not use {@link IChunkMessage}s, can not be used with
 * scale-out, and does not support sharded indices.
 * 
 * @todo Since each operator task runs exactly once there is less potential
 *       parallelism in the operator task execution when compared to
 *       {@link ChunkedRunningQuery}. Determine whether or not queries evaluated
 *       by this class benefit from increased access path parallelism in
 *       pipelined join operators as a means of compensating for the decreased
 *       operator task parallelism or if the queue-based evaluation provides
 *       better overall throughput regardless.
 * 
 * @todo Run all unit tests of the query engine against the appropriate
 *       strategies.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StandaloneChainedRunningQuery extends AbstractRunningQuery {

    private final static transient Logger log = Logger
            .getLogger(StandaloneChainedRunningQuery.class);

    /**
     * The input queue for each operator. The {@link MultiplexBlockingBuffer} is
     * configured when the query is setup with one "skin" for each source
     * operator which can write on a given target operator. The
     * {@link MultiplexBlockingBuffer} will automatically be flushed and closed
     * once all skins have been closed by their source operators.
     */
    private final ConcurrentHashMap<Integer/*bopId*/, MultiplexBlockingBuffer<IBindingSet[]>> operatorQueues;

    /**
     * The {@link Future} for the task responsible for evaluating each operator.
     * <p>
     * Note: For some reason, the query termination semantics appear to be
     * smoother when we do not set the {@link Future} on the shared
     * {@link BlockingBuffer}s. They are being tracked separately here to
     * facilitate {@link #cancel(boolean)}. If an {@link OperatorTask} fails,
     * the {@link OperatorTaskWrapper} will trap the thrown exception and halt
     * the query without requiring direct access to the {@link Future} (it will
     * of course be used by {@link #cancel(boolean)}).
     */
    private final ConcurrentHashMap<Integer/*bopId*/, Future<Void>> operatorFutures;

    /**
     * Used to verify that {@link #acceptChunk(IChunkMessage)} is not invoked
     * more than once.
     */
    private final AtomicBoolean didAcceptChunk = new AtomicBoolean(false);

    /**
     * @param queryEngine
     * @param queryId
     * @param controller
     * @param clientProxy
     * @param query
     */
    public StandaloneChainedRunningQuery(final QueryEngine queryEngine,
            final UUID queryId, final boolean controller,
            final IQueryClient clientProxy, final PipelineOp query) {

        super(queryEngine, queryId, controller, clientProxy, query);

        this.operatorQueues = new ConcurrentHashMap<Integer/* bopId */, MultiplexBlockingBuffer<IBindingSet[]>>();

        this.operatorFutures = new ConcurrentHashMap<Integer/* bopId */, Future<Void>>();

        /*
         * Setup the input queues for each operator. Those queues will be used
         * by each operator which targets a given operator. Each operator will
         * start once and will run until all of its source(s) are closed.
         * 
         * This allocates the buffers in a top-down manner (this is the reverse
         * of the pipeline evaluation order). Allocation halts if we reach an
         * operator without children (e.g., StartOp) or an operator which is a
         * CONTROLLER (Union). (If allocation does not halt at those boundaries
         * then we can allocate buffers which will not be used. On the one hand,
         * the StartOp receives a message containing the chunk to be evaluated.
         * On the other hand, the buffers are not shared between the parent and
         * a subquery so allocation within the subquery is wasted. This is also
         * true for the [statsMap].)
         */
        setupOperatorQueues(query);

    }

    /**
     * Ignored.
     */
    @Override
    protected void consumeChunk() {
        // NOP.
    }

    /**
     * Copies the data from the {@link IChunkMessage} into the target operator
     * (this is used to get the query moving but the query does not generate new
     * {@link IChunkMessage} once it is running).
     */
    @Override
    protected boolean acceptChunk(final IChunkMessage<IBindingSet> msg) {

        if (!didAcceptChunk.compareAndSet(false/* expect */, true/* update */)) {
            /*
             * The query has already been presented with the initial chunk.
             */
            throw new IllegalStateException();
        }

        if(msg.getBOpId() != BOpUtility.getPipelineStart(getQuery()).getId()) {
            /*
             * The initial message must be addressed to the operator at the head
             * of the query pipeline. 
             */
            throw new IllegalArgumentException(ERR_NOT_PIPELINE_START);
        }

        final MultiplexBlockingBuffer<IBindingSet[]> factory = operatorQueues
                .get(msg.getBOpId());
        
        if (factory == null)
            throw new IllegalArgumentException("No such sink: " + msg);

        // Start all downstream tasks
        startTasks(getQuery());

        /*
         * Copy the initial binding sets onto the 1st operator in the pipeline.
         * 
         * Note: This must be done in a separate thread in case there is enough
         * source data to cause the sink to block since the caller can not begin
         * to drain the query until we return from this method.
         */
        getQueryEngine().execute(
                new CopyChunkTask(msg, factory.newInstance()/* sink */));

        return true;
        
    }

    private class CopyChunkTask implements Runnable {

        private final IChunkMessage<IBindingSet> msg;

        final IBlockingBuffer<IBindingSet[]> sink;

        private CopyChunkTask(final IChunkMessage<IBindingSet> msg,
                final IBlockingBuffer<IBindingSet[]> sink) {
            this.msg = msg;
            this.sink = sink;
        }

        public void run() {

            try {

                BOpUtility.copy(msg.getChunkAccessor().iterator(), sink,
                        null/* sink2 */, null/* constraints */, //
                        null/* stats */
                );

                sink.flush();
                sink.close();

            } catch (Throwable t) {

                halt(t);
                
            }

        }

    }
    
    /**
     * Pre-populate a map with {@link MultiplexBlockingBuffer} objects for the
     * query. Operators in subqueries are not visited since they will be
     * assigned buffer objects when they are run as a subquery. Operators
     * without children are not visited since they can not be the targets of
     * some other operator and hence do not need to have an assigned input
     * buffer.
     */
    private void setupOperatorQueues(final BOp op) {

        if (!(op instanceof PipelineOp))
            return;

//        if (op.arity() == 0)
//            return;

        final PipelineOp bop = (PipelineOp) op;

        final int bopId = bop.getId();

        /*
         * Note: the BlockingBufferWithStats tracks the *output* statistics for
         * an operator. Therefore it needs to wrap the skins not the shared
         * backing buffer for the target. That is why the shared BlockingBuffer
         * backing the MultiplexBlockingBuffer does not track the BOpStats.
         */
        final IBlockingBuffer<IBindingSet[]> buffer = new BlockingBuffer<IBindingSet[]>(
                bop.getChunkOfChunksCapacity(), bop.getChunkCapacity(), bop
                        .getChunkTimeout(), BufferAnnotations.chunkTimeoutUnit);

        operatorQueues.put(bopId, new MultiplexBlockingBuffer<IBindingSet[]>(
                buffer));

        if (!op.getProperty(BOp.Annotations.CONTROLLER,
                BOp.Annotations.DEFAULT_CONTROLLER)) {
            /*
             * Visit children, but not if this is a CONTROLLER operator since
             * its children belong to a subquery.
             */
            for (BOp t : op.args()) {

                // visit children (recursion)
                setupOperatorQueues(t);

            }

        }

    }

    /**
     * Pre-start a task for each operator. The operators are started in
     * back-to-front order (reverse pipeline evaluation order). The input queues
     * for the operators were created in by {@link #setupOperatorQueues(BOp)}
     * and are found in {@link #operatorQueues}. The output queues for the
     * operators are skins over the {@link MultiplexBlockingBuffer} instance for
     * the target operator.
     * 
     * @param op
     *            The operator.
     * 
     * @see #inputBufferMap
     */
    private void startTasks(final BOp op) {

        if (!(op instanceof PipelineOp))
            return;

//        if (op.arity() == 0)
//            return;

        final PipelineOp bop = (PipelineOp) op;

        final int bopId = bop.getId();

        final MultiplexBlockingBuffer<IBindingSet[]> factory = operatorQueues
                .get(bopId);

        if (factory == null)
            throw new AssertionError("No input buffer? " + op);

        final IAsynchronousIterator<IBindingSet[]> src = factory
                .getBackingBuffer().iterator();

        // Create task to evaluation that operator.
        final OperatorTask opTask = new OperatorTask(bopId, src);

        // Wrap task with error handling and handshaking logic.
        final FutureTask<Void> ft = new FutureTask<Void>(
                new OperatorTaskWrapper(opTask), null/* result */);

        if (operatorFutures.putIfAbsent(bopId, ft) != null) {
            // There is already a future registered for this operator. 
            throw new RuntimeException(ERR_DUPLICATE_IDENTIFIER + bopId);
        }
        
//        // Set task future on the backing buffer.
//        factory.getBackingBuffer().setFuture(ft);
        
        // Start task for that operator.
        getQueryEngine().execute(ft);

        if (!op.getProperty(BOp.Annotations.CONTROLLER,
                BOp.Annotations.DEFAULT_CONTROLLER)) {
            /*
             * Visit children, but not if this is a CONTROLLER operator since
             * its children belong to a subquery.
             */
            for (BOp t : op.args()) {

                // visit children (recursion)
                startTasks(t);
                
            }

        }

    }

    @Override
    protected boolean cancelRunningOperators(final boolean mayInterruptIfRunning) {

        boolean cancelled = false;
        
//        for (MultiplexBlockingBuffer<IBindingSet[]> buffer : operatorQueues
//                .values()) {
//        
//            final Future<?> f = buffer.getBackingBuffer().getFuture();
//
//            if (f != null && f.cancel(mayInterruptIfRunning)) {
//
//                cancelled = true;
//            
//            }
//
//        }

        for (Future<Void> f : operatorFutures.values()) {

            if (f.cancel(mayInterruptIfRunning)) {

                cancelled = true;

            }

        }

        return cancelled;
        
    }

    /**
     * Handles various handshaking with the {@link AbstractRunningQuery} and the
     * {@link RunState} for the query.
     */
    private class OperatorTaskWrapper implements Runnable {

        private final OperatorTask t;

        public OperatorTaskWrapper(final OperatorTask opTask) {

            if (opTask == null)
                throw new IllegalArgumentException();
            
            this.t = opTask;
            
        }

        /*
         * Note: The #of messages in and the #of messages out for the primary
         * and default sink are used to drive RunState. That works with chunked
         * evaluation just fine. For chained evaluation the query should run
         * until cancelled or the sources for all operators have been closed. As
         * a bit of a hack, I am using ONE (1) for messagesIn and ONE (1) for
         * the default sink and ZERO (0) for the altSink. This ensures that the
         * input and output message counts are balanced. In fact, those counts
         * are not terribly relevant for the "chained" running query and it
         * might make sense to decouple the "chained" running query from from
         * RunState. That would change the API boundaries for
         * AbstractRunningQuery and the ChunkedRunningQuery.
         */
        public void run() {

            final UUID serviceId = getQueryEngine().getServiceUUID();
            try {

                /*
                 * Notify query controller that operator task will start (sync
                 * notification).
                 */
                StandaloneChainedRunningQuery.this.startOp(new StartOpMessage(
                        getQueryId(), t.bopId, -1/* partitionId */, serviceId,
                        1/* messagesIn */));

                /*
                 * Run the operator task.
                 */
                final long begin = System.currentTimeMillis();
                try {
                    t.call();
                } catch(Throwable t) {
					/*
					 * Note: SliceOp will cause other operators to be
					 * interrupted during normal evaluation. Therefore, while
					 * these exceptions should cause the query to terminate,
					 * they should not be reported as errors to the query
					 * controller.
					 */
					if (!InnerCause.isInnerCause(t, InterruptedException.class)
			 		 && !InnerCause.isInnerCause(t, BufferClosedException.class)
			 		 && !InnerCause.isInnerCause(t, ClosedByInterruptException.class)
			 		 ) {
						// Not an error that we should ignore.
						throw t;
	                }
                } finally {
                    t.context.getStats().elapsed.add(System.currentTimeMillis()
                            - begin);
                }

                /*
                 * Notify the query controller that operator task did run (sync
                 * notification).
                 */
                final HaltOpMessage msg = new HaltOpMessage(getQueryId(), t.bopId,
                        -1/*partitionId*/, serviceId, null/* cause */, t.sinkId,
                        1/*t.sinkMessagesOut.get()*/, t.altSinkId,
                        0/*t.altSinkMessagesOut.get()*/, t.context.getStats());

                StandaloneChainedRunningQuery.this.haltOp(msg);
                
            } catch (Throwable ex1) {

                // Log an error.
                log.error("queryId=" + getQueryId() + ", bopId=" + t.bopId
                        + ", bop=" + t.bop, ex1);
                
                /*
                 * Mark the query as halted on this node regardless of whether
                 * we are able to communicate with the query controller.
                 * 
                 * Note: Invoking halt(t) here will log an error. This logged
                 * error message is necessary in order to catch errors in
                 * clientProxy.haltOp() (above and below).
                 */
                final Throwable firstCause = halt(ex1);

                final HaltOpMessage msg = new HaltOpMessage(getQueryId(), t.bopId,
                        -1/*partitionId*/, serviceId, firstCause, t.sinkId,
                        0/*t.sinkMessagesOut.get()*/, t.altSinkId,
                        0/*t.altSinkMessagesOut.get()*/, t.context.getStats());
                
                StandaloneChainedRunningQuery.this.haltOp(msg);

            }
        
        } // runOnce()
        
    } // class OperatorTaskWrapper

    /**
     * Runnable evaluates an operator.
     */
    private class OperatorTask implements Callable<Void> {

//        /** Alias for the {@link ChunkTask}'s logger. */
//        private final Logger log = chunkTaskLog;

        /** The index of the bop which is being evaluated. */
        private final int bopId;

        /** The operator which is being evaluated. */
        private final BOp bop;

        /**
         * The index of the operator which is the default sink for outputs
         * generated by this evaluation. This is the
         * {@link BOp.Annotations#BOP_ID} of the parent of this operator. This
         * will be <code>null</code> if the operator does not have a parent and
         * is not a query since no outputs will be generated in that case.
         */
        private final Integer sinkId;

        /**
         * The index of the operator which is the alternative sink for outputs
         * generated by this evaluation. This is <code>null</code> unless the
         * operator explicitly specifies an alternative sink using either
         * {@link PipelineOp.Annotations#ALT_SINK_REF} or
         * {@link PipelineOp.Annotations#ALT_SINK_GROUP}.  
         */
        private final Integer altSinkId;

        /**
         * The sink on which outputs destined for the {@link #sinkId} operator
         * will be written and <code>null</code> if {@link #sinkId} is
         * <code>null</code>.
         */
        private final IBlockingBuffer<IBindingSet[]> sink;

        /**
         * The sink on which outputs destined for the {@link #altSinkId}
         * operator will be written and <code>null</code> if {@link #altSinkId}
         * is <code>null</code>.
         */
        private final IBlockingBuffer<IBindingSet[]> altSink;

        /**
         * The evaluation context for this operator.
         */
        private final BOpContext<IBindingSet> context;

        /**
         * {@link FutureTask} which evaluates the operator (evaluation is
         * delegated to this {@link FutureTask}).
         */
        private final FutureTask<Void> ft;

//        /**
//         * The #of input messages.
//         */
//        final int messagesIn;
//        
//        /** #of chunk messages out to sink. */
//        final AtomicInteger sinkMessagesOut = new AtomicInteger(0);
//
//        /** #of chunk messages out to altSink. */
//        final AtomicInteger altSinkMessagesOut = new AtomicInteger(0);

        /**
         * A human readable representation of the {@link OperatorTask}'s state.
         */
        public String toString() {
            return "OperatorTask" + //
                    "{query=" + getQueryId() + //
                    ",bopId=" + bopId + //
                    ",sinkId=" + sinkId + //
                    ",altSinkId=" + altSinkId + //
                    "}";
        }

        /**
         * Core implementation.
         * <p>
         * This looks up the {@link BOp} which is the target for the message in
         * the {@link IRunningQuery#getBOpIndex() BOp Index}, creates the
         * sink(s) for the {@link BOp}, creates the {@link BOpContext} for that
         * {@link BOp}, and wraps the value returned by
         * {@link PipelineOp#eval(BOpContext)} in order to handle the outputs
         * written on those sinks.
         * 
         * @param bopId
         *            The operator to which the message was addressed.
         * @param partitionId
         *            The partition identifier to which the message was
         *            addressed.
         * @param messagesIn
         *            The number of {@link IChunkMessage} to be consumed by this
         *            task.
         * @param source
         *            Where the task will read its inputs.
         */
        public OperatorTask(final int bopId,
                final IAsynchronousIterator<IBindingSet[]> src) {

            this.bopId = bopId;
                        
            bop = getBOpIndex().get(bopId);
            
            if (bop == null)
                throw new NoSuchBOpException(bopId);
            
            if (!(bop instanceof PipelineOp))
                throw new UnsupportedOperationException(bop.getClass()
                        .getName());

            // self
            final PipelineOp op = ((PipelineOp) bop);

            // parent (null if this is the root of the operator tree).
            final BOp p = BOpUtility.getParent(getQuery(), op);

            /*
             * The sink is the parent. The parent MUST have an id so we can
             * target it with a message. (The sink will be null iff there is no
             * parent for this operator.)
             */
            sinkId = BOpUtility.getEffectiveDefaultSink(bop, p);

            // altSink (null when not specified).
            altSinkId = (Integer) op
                    .getProperty(PipelineOp.Annotations.ALT_SINK_REF);
//            {
//                // altSink (null when not specified).
//                final Integer altSinkId = (Integer) op
//                        .getProperty(PipelineOp.Annotations.ALT_SINK_REF);
//                final Integer altSinkGroup = (Integer) op
//                        .getProperty(PipelineOp.Annotations.ALT_SINK_GROUP);
//                if (altSinkId != null && altSinkGroup != null)
//                    throw new RuntimeException(
//                            "Annotations are mutually exclusive: "
//                                    + PipelineOp.Annotations.ALT_SINK_REF
//                                    + " and "
//                                    + PipelineOp.Annotations.ALT_SINK_GROUP);
//                if (altSinkGroup != null) {
//                    /*
//                     * Lookup the first pipeline op in the conditional binding
//                     * group and use its bopId as the altSinkId.
//                     */
//                    this.altSinkId = BOpUtility.getFirstBOpIdForConditionalGroup(
//                            getQuery(), altSinkGroup);
//                } else {
//                    // MAY be null.
//                    this.altSinkId = altSinkId;
//                }
//            }

            if (altSinkId != null && !getBOpIndex().containsKey(altSinkId))
                throw new NoSuchBOpException(altSinkId);

            if (altSinkId != null && sinkId == null) {
                throw new RuntimeException(
                        "The primary sink must be defined if the altSink is defined: "
                                + bop);
            }

            /*
             * Setup the BOpStats object. For some operators, e.g., SliceOp,
             * this MUST be the same object across all invocations of that
             * instance of that operator for this query. This is marked by the
             * PipelineOp#isSharedState() method and is handled by a
             * putIfAbsent() pattern when that method returns true.
             * 
             * Note: RunState#haltOp() avoids adding a BOpStats object to itself
             * since that would cause double counting when the same object is
             * used for each invocation of the operator.
             * 
             * Note: It tends to be more useful to have distinct BOpStats
             * objects for each operator task instance that we run as this makes
             * it possible to see how much work was performed by that task
             * instance. The data are aggregated in the [statsMap] across the
             * entire run of the query.
             */
            final BOpStats stats;
            if (((PipelineOp) bop).isSharedState()) {//|| statsMap != null) {
                // shared stats object.
                stats = getStats(bopId);
            } else {
                // distinct stats objects, aggregated as each task finishes.
                stats = op.newStats();
            }
            assert stats != null;

//            // The groupId (if any) for this operator.
//            final Integer fromGroupId = (Integer) op
//                    .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);

            if (p == null) {
                sink = getQueryBuffer();
            } else {
//                final BOp targetOp = getBOpIndex().get(sinkId);
//                final Integer toGroupId = (Integer) targetOp
//                        .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);
//                final SinkTransitionMetadata stm = new SinkTransitionMetadata(
//                        fromGroupId, toGroupId, true/* isSink */);
                sink = newBuffer(op, sinkId, //null/* stm */,
                /* sinkMessagesOut, */stats);
            }

            if (altSinkId == null) {
                altSink = null;
                // } else if(altSinkId.equals(sinkId)){
                /*
                 * @todo Note: The optimization when altSink:=sink is now only
                 * possible when the groupId is not changing during the
                 * transition.
                 */ 
                // altSink = sink;
            } else {
//                final BOp targetOp = getBOpIndex().get(altSinkId);
//                final Integer toGroupId = (Integer) targetOp
//                        .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);
//                final SinkTransitionMetadata stm = new SinkTransitionMetadata(
//                        fromGroupId, toGroupId, false/* isSink */);
                altSink = newBuffer(op, altSinkId, //null/*stm*/,
                        /*altSinkMessagesOut,*/ stats);
            }

            context = new BOpContext<IBindingSet>(
                    StandaloneChainedRunningQuery.this, -1/* partitionId */,
                    stats, src, sink, altSink);

            // FutureTask for operator execution (not running yet).
            if ((ft = op.eval(context)) == null)
                throw new RuntimeException("No future: " + op);

        }

        /**
         * Factory returns the {@link IBlockingBuffer} on which the operator
         * should write its outputs which target the specified <i>sinkId</i>.
         * 
         * @param op
         *            The operator whose evaluation task is being constructed.
         * @param sinkId
         *            The identifier for an operator which which the task will
         *            write its solutions (either the primary or alternative
         *            sink).
         * @param stats
         *            The statistics object for the evaluation of the operator.
         * 
         * @return The buffer on which the operator should write outputs which
         *         target that sink.
         */
        private IBlockingBuffer<IBindingSet[]> newBuffer(final PipelineOp op,
                final int sinkId,
//                final SinkTransitionMetadata sinkTransitionMetadata,
                /* final AtomicInteger sinkMessagesOut, */final BOpStats stats) {

            final MultiplexBlockingBuffer<IBindingSet[]> factory = operatorQueues
                    .get(sinkId);

            if (factory == null) {
                // Unknown sinkId.
                throw new IllegalArgumentException(ERR_NO_SUCH_BOP + sinkId);
            }

            /*
             * Wrap with buffer which will track the output stats for the source
             * operator writing on this sink. Wrap that to handle the sink
             * transition metadata.
             */
            return //new SinkTransitionBuffer(
                    new OutputStatsBuffer<IBindingSet[]>(factory.newInstance(),
                            stats)
                            //, sinkTransitionMetadata)
                            ;

        }

        /**
         * Evaluate the {@link IChunkMessage}.
         */
        public Void call() throws Exception {
            if (log.isDebugEnabled())
                log.debug("Running chunk: " + this);
            ft.run(); // run
            ft.get(); // verify success
            // Done.
            return null;
        } // call()

    } // class OperatorTask
    
}
