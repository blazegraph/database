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
 * Created on Sep 2, 2010
 */

package com.bigdata.bop;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.ChunkedRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * Abstract base class for pipeline operators where the data moving along the
 * pipeline is chunks of {@link IBindingSet}s.
 * <p>
 * The top-level of a query plan is composed of a required
 * {@link Annotations#JOIN_GRAPH}s followed by a mixture of optional joins and
 * {@link Annotations#CONDITIONAL_GROUP}s. A
 * {@link Annotations#CONDITIONAL_GROUP} will have at least one required join
 * (in a {@link Annotations#JOIN_GRAPH}) followed by zero or more optional
 * joins.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class PipelineOp extends BOpBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

	private final static transient Logger log = Logger
			.getLogger(PipelineOp.class);

    public interface Annotations extends BOp.Annotations, BufferAnnotations {

        /**
         * The value of the annotation is the {@link BOp.Annotations#BOP_ID} of
         * the ancestor in the operator tree which serves as the default sink
         * for binding sets (optional, default is the parent).
         */
        String SINK_REF = PipelineOp.class.getName() + ".sinkRef";

        /**
         * The value of the annotation is the {@link BOp.Annotations#BOP_ID} of
         * the ancestor in the operator tree which serves as the alternative
         * sink for binding sets (default is no alternative sink).
         * 
         * @see #ALT_SINK_GROUP
         */
        String ALT_SINK_REF = PipelineOp.class.getName()
                + ".altSinkRef";

		/**
		 * The value reported by {@link PipelineOp#isSharedState()} (default
		 * {@value #DEFAULT_SHARED_STATE}). This may be overridden to
		 * <code>true</code> to have instances operators evaluated in the same
		 * query engine context share the same {@link BOpStats} instance.
		 * <p>
		 * Note: {@link BOp#getEvaluationContext()} MUST be overridden to return
		 * {@link BOpEvaluationContext#CONTROLLER} if this annotation is
		 * overridden to <code>true</code>.
		 * <p>
		 * When <code>true</code>, the {@link QueryEngine} will impose the
		 * necessary constraints when the operator is evaluated.
		 */
		String SHARED_STATE = PipelineOp.class.getName() + ".sharedState";

		boolean DEFAULT_SHARED_STATE = false;

		/**
		 * Annotation may be used to indicate operators which are not thread
		 * safe (default {@value #DEFAULT_THREAD_SAFE}). Concurrent invocations
		 * of the evaluation task will not be scheduled for a given shard for an
		 * operator which is not thread safe.
		 * 
		 * @todo Unit tests for {@link ChunkedRunningQuery} to verify that it
		 *       eventually schedules operator tasks which were deferred to
		 *       prevent concurrent evaluation.
		 */
		String THREAD_SAFE = PipelineOp.class.getName() + ".threadSafe";

		boolean DEFAULT_THREAD_SAFE = true;

		/**
		 * Annotation used to mark pipelined (aka vectored) operators. When
		 * <code>false</code> the operator will use either "at-once" or
		 * "blocked" evaluation depending on how it buffers its data for
		 * evaluation.
		 */
		String PIPELINED = PipelineOp.class.getName() + ".pipelined";

		boolean DEFAULT_PIPELINED = true;

		/**
		 * For non-{@link #PIPELINED} operators, this non-negative value
		 * specifies the maximum #of bytes which the operator may buffer on the
		 * native heap before evaluation of the operator is triggered -or- ZERO
		 * (0) if the operator buffers the data on the Java heap (default
		 * {@value #DEFAULT_MAX_MEMORY}). When non-zero, the #of bytes specified
		 * should be a multiple of 4k. For a shared operation, the value is the
		 * maximum #of bytes which may be buffered per shard.
		 * <p>
		 * Operator "at-once" evaluation will be used if either (a) the operator
		 * is buffering data on the Java heap; or (b) the operator is buffering
		 * data on the native heap and the amount of buffered data does not
		 * exceed the specified value for {@link #MAX_MEMORY}. For convenience,
		 * the value {@link Integer#MAX_VALUE} may be specified to indicate that
		 * "at-once" evaluation is required.
		 * <p>
		 * When data are buffered on the Java heap, "at-once" evaluation is
		 * implied and the data will be made available to the operator as a
		 * single {@link IAsynchronousIterator} when the operator is invoked.
		 * <p>
		 * When {@link #MAX_MEMORY} is positive, data are marshaled in
		 * {@link ByteBuffer}s and the operator will be invoked once either (a)
		 * its memory threshold for the buffered data has been exceeded; or (b)
		 * no predecessor of the operator is running (or can be triggered) -and-
		 * all inputs for the operator have been materialized on this node. Note
		 * that some operators DO NOT support multiple pass evaluation
		 * semantics. Such operators MUST throw an exception if the value of
		 * this annotation could result in multiple evaluation passes.
		 */
		String MAX_MEMORY = PipelineOp.class.getName() + ".maxMemory";

		int DEFAULT_MAX_MEMORY = 0;
		
//        /**
//         * Annotation used to mark a set of (non-optional) joins which may be
//         * freely reordered by the query optimizer in order to minimize the
//         * amount of work required to compute the solutions.
//         * <p>
//         * Note: Optional joins MAY NOT appear within a join graph. Optional
//         * joins SHOULD be evaluated as part of the "tail plan" following the
//         * join graph, but before operations such as SORT, DISTINCT, etc. When
//         * the query plan includes {@link #CONDITIONAL_GROUP}s, those groups
//         * include a leading {@link #JOIN_GRAPH} (required joins) followed by
//         * zero or more optional joins.
//         */
//		String JOIN_GRAPH = PipelineOp.class.getName() + ".joinGraph";

//        /**
//         * Annotation used to mark a set of operators belonging to a conditional
//         * binding group. Bindings within with the group will be discarded if
//         * any required operator in the group fails. For example, if a binding
//         * set exits via the alternative sink for a required join then any
//         * conditional bindings within the group will be discarded.
//         * <p>
//         * Together with {@link #ALT_SINK_GROUP}, the {@link #CONDITIONAL_GROUP}
//         * annotation provides the information necessary in order to decide the
//         * re-entry point in the query plan when a join within an conditional
//         * binding group fails.
//         * <p>
//         * The {@link #CONDITIONAL_GROUP} annotation controls the
//         * {@link IBindingSet#push()} and {@link IBindingSet#pop(boolean)} of
//         * individual solutions as they propagate through the pipeline. When a
//         * pipeline starts, the {@link IBindingSet} stack contains only the top
//         * level symbol table (i.e., name/value bindings). When an intermediate
//         * solution enters a {@link PipelineOp} marked as belonging to a
//         * {@link #CONDITIONAL_GROUP}, a new symbol table is
//         * {@link IBindingSet#push() pushed} onto the stack for that solution.
//         * If the solution leaves the optional join group via the default sink,
//         * then the symbol table is "saved" when it is
//         * {@link IBindingSet#pop(boolean) popped} off of the stack. If the
//         * solution leaves the join group via the alternative sink, then the
//         * symbol table is discarded when it is {@link IBindingSet#pop(boolean)
//         * popped} off of the stack. This provides for conditional binding of
//         * variables within the operators of the group.
//         * <p>
//         * The value of the {@link #CONDITIONAL_GROUP} is an {@link Integer}
//         * which uniquely identifies the group within the query.
//         * 
//         * @deprecated The binding set stack push/pop mechanisms are not
//         *             sufficient to support optional join groups. This
//         *             annotation will be removed unless it proves valuable for
//         *             marking the elements of a join group, in which case the
//         *             javadoc needs to be updated.
//         */
//		String CONDITIONAL_GROUP = PipelineOp.class.getName() + ".conditionalGroup";

//        /**
//         * Annotation used to designate the target when a required operator
//         * within an {@link #CONDITIONAL_GROUP} fails. The value of this
//         * annotation must be the {@link #CONDITIONAL_GROUP} identifier
//         * corresponding to the next conditional binding group in the query
//         * plan. If there is no such group, then the {@link #ALT_SINK_REF}
//         * should be used instead to specify the target operator in the
//         * pipeline, e.g., a {@link SliceOp}.
//         * <p>
//         * The target {@link #CONDITIONAL_GROUP} is specified (rather than the
//         * bopId of the target join) since the non-optional joins in the target
//         * {@link #CONDITIONAL_GROUP} be reordered by the query optimizer. The
//         * entry point for solutions redirected to the {@link #ALT_SINK_GROUP}
//         * is therefore the first operator in the target
//         * {@link #CONDITIONAL_GROUP}. This decouples the routing decisions from
//         * the join ordering decisions.
//         * 
//         * @see #CONDITIONAL_GROUP
//         * @see #ALT_SINK_REF
//         * 
//         * @deprecated The binding set stack push/pop mechanisms are not
//         *             sufficient to support optional join groups. This
//         *             annotation will be removed.
//         */
//		String ALT_SINK_GROUP = PipelineOp.class.getName() + ".altSinkGroup";

    }

    /**
     * Required deep copy constructor.
     * 
     * @param op
     */
    protected PipelineOp(final PipelineOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    protected PipelineOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    /**
     * @see BufferAnnotations#CHUNK_CAPACITY
     */
    public int getChunkCapacity() {
        
        return getProperty(Annotations.CHUNK_CAPACITY,
                Annotations.DEFAULT_CHUNK_CAPACITY);

    }

    /**
     * @see BufferAnnotations#CHUNK_OF_CHUNKS_CAPACITY
     */
    public int getChunkOfChunksCapacity() {

        return getProperty(Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

    }

    /**
     * @see BufferAnnotations#CHUNK_TIMEOUT
     */
    public long getChunkTimeout() {
        
        return getProperty(Annotations.CHUNK_TIMEOUT,
                Annotations.DEFAULT_CHUNK_TIMEOUT);
        
    }

	/**
	 * Return <code>true</code> if the operator is pipelined (versus using
	 * "at-once" or blocked evaluation as discussed below).
	 * <dl>
	 * <dt>Pipelined</dt>
	 * <dd>Pipelined operators stream chunks of intermediate results from one
	 * operator to the next using producer / consumer pattern. Each time a set
	 * of intermediate results is available for a pipelined operator, it is
	 * evaluated against those inputs producing another set of intermediate
	 * results for its target operator(s). Pipelined operators may be evaluated
	 * many times during a given query and often have excellent parallelism due
	 * to the concurrent evaluation of the different operators on different sets
	 * of intermediate results.</dd>
	 * <dt>At-Once</dt>
	 * <dd>
	 * An "at-once" operator will run exactly once and must wait for all of its
	 * inputs to be assembled before it runs. There are some operations for
	 * which "at-once" evaluation is always required, such as ORDER_BY. Other
	 * operations MAY use operator-at-once evaluation in order to benefit from a
	 * combination of more efficient IO patterns and simpler design. At-once
	 * operators may either buffer their data on the Java heap (which is not
	 * scalable due to the heap pressure exerted on the garbage collector) or
	 * buffer their data on the native heap (which does scale).</dd>
	 * <dt>Blocked</dt>
	 * <dd>Blocked operators buffer large amounts of data on the native heap and
	 * run each time they exceed some threshold #of bytes of buffered data. A
	 * blocked operator is basically an "at-once" operator which buffers its
	 * data on the native heap and which can be evaluated in multiple passes.
	 * For example, a hash join could use a blocked operator design while an
	 * ORDER_BY operator can not. By deferring their evaluation until some
	 * threshold amount of data has been materialized, they may be evaluated
	 * once or more than once, depending on the data scale, but still retain
	 * many of the benefits of "at-once" evaluation in terms of IO patterns.
	 * Whether or not an operator can be used as a "blocked" operator is a
	 * matter of the underlying operator implementation.</dd>
	 * </dl>
	 * 
	 * @see Annotations#PIPELINED
	 * @see Annotations#MAX_MEMORY
	 */
	public boolean isPipelined() {
		return getProperty(PipelineOp.Annotations.PIPELINED,
				PipelineOp.Annotations.DEFAULT_PIPELINED);
	}
    
    /**
     * Return <code>true</code> iff {@link #newStats()} must be shared across
     * all invocations of {@link #eval(BOpContext)} for this operator for a
     * given query.
     * 
     * @see Annotations#SHARED_STATE
     */
    public boolean isSharedState() {

		return getProperty(Annotations.SHARED_STATE,
				Annotations.DEFAULT_SHARED_STATE);
        
    }

	/**
	 * Return a new object which can be used to collect statistics on the
	 * operator evaluation. This may be overridden to return a more specific
	 * class depending on the operator.
	 * <p>
	 * Some operators may use this to share state across multiple invocations of
	 * the operator within a given query (e.g., {@link SliceOp}). Another
	 * mechanism for sharing state is to use the same named allocation context
	 * for the memory manager across the operator invocation instances.
	 * <p>
	 * Operator life cycle events support pre-/post-operator behaviors. Such
	 * events can be used to processed buffered solutions accumulated within
	 * some shared state across multiple operator invocations.
	 */
    public BOpStats newStats() {

        return new BOpStats();

    }

//    /**
//     * Instantiate a buffer suitable as a sink for this operator. The buffer
//     * will be provisioned based on the operator annotations.
//     * <p>
//     * Note: if the operation swallows binding sets from the pipeline (such as
//     * operators which write on the database) then the operator MAY return an
//     * immutable empty buffer.
//     * 
//     * @param stats
//     *            The statistics on this object will automatically be updated as
//     *            elements and chunks are output onto the returned buffer.
//     * 
//     * @return The buffer.
//     */
//    public IBlockingBuffer<IBindingSet[]> newBuffer(final BOpStats stats) {
//
//        if (stats == null)
//            throw new IllegalArgumentException();
//        
//        return new BlockingBufferWithStats<IBindingSet[]>(
//                getChunkOfChunksCapacity(), getChunkCapacity(),
//                getChunkTimeout(), Annotations.chunkTimeoutUnit, stats);
//
//    }
    
    /**
     * Return a {@link FutureTask} which computes the operator against the
     * evaluation context. The caller is responsible for executing the
     * {@link FutureTask} (this gives them the ability to hook the completion of
     * the computation).
     * 
     * @param context
     *            The evaluation context.
     * 
     * @return The {@link FutureTask} which will compute the operator's
     *         evaluation.
     * 
     * @todo Modify to return a {@link Callable}s for now since we must run each
     *       task in its own thread until Java7 gives us fork/join pools and
     *       asynchronous file I/O. For the fork/join model we will probably
     *       return the ForkJoinTask.
     */
    abstract public FutureTask<Void> eval(BOpContext<IBindingSet> context);

	/**
	 * Hook to setup any resources associated with the operator (temporary
	 * files, memory manager allocation contexts, etc.). This hook is invoked
	 * exactly once and before any instance task for the operator is evaluated.
	 */
    public void setUp() throws Exception {

    	if (log.isTraceEnabled())
			log.trace("bopId=" + getId());

    }

	/**
	 * Hook to tear down any resources associated with the operator (temporary
	 * files, memory manager allocation contexts, etc.). This hook is invoked
	 * exactly once no later than when the query is cancelled. If the operator
	 * is known to be done executing, then this hook will be invoked at that
	 * time.
	 */
    public void tearDown() throws Exception {

    	if (log.isTraceEnabled())
    		log.trace("bopId=" + getId());
    	
    }

}
