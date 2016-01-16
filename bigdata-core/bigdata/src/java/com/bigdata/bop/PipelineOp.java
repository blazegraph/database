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
 * Created on Sep 2, 2010
 */

package com.bigdata.bop;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.QueryEngine;

/**
 * Abstract base class for pipeline operators where the data moving along the
 * pipeline is chunks of {@link IBindingSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class PipelineOp extends BOpBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

//	private final static transient Logger log = Logger
//			.getLogger(PipelineOp.class);

    public interface Annotations extends BOp.Annotations, BufferAnnotations {

        /**
         * The value of the annotation is the {@link BOp.Annotations#BOP_ID} of
         * the ancestor in the operator tree which serves as the default sink
         * for binding sets (optional, default is the parent).
         * 
         * @see BOpUtility#getEffectiveDefaultSink(BOp, BOp)
         */
        String SINK_REF = PipelineOp.class.getName() + ".sinkRef";

        /**
         * The value of the annotation is the {@link BOp.Annotations#BOP_ID} of
         * the ancestor in the operator tree which serves as the alternative
         * sink for binding sets (default is no alternative sink).
         * <p>
         * Note: JOIN and SUBQUERY operators will route optional solutions to
         * the altSink if the altSink is specified and to the default sink
         * otherwise.
         */
		String ALT_SINK_REF = PipelineOp.class.getName() + ".altSinkRef";

        /**
         * The value reported by {@link PipelineOp#isSharedState()} (default
         * {@value #DEFAULT_SHARED_STATE}). This may be overridden to
         * <code>true</code> to have instances of operators evaluated in the
         * same query engine context share the same {@link BOpStats} instance.
         * <p>
         * Note: {@link BOp#getEvaluationContext()} MUST be overridden to return
         * {@link BOpEvaluationContext#CONTROLLER} if this annotation is
         * overridden to <code>true</code>.
         * <p>
         * When <code>true</code>, the {@link QueryEngine} will impose the
         * necessary constraints when the operator is evaluated.
         * 
         * @see IQueryContext
         */
		String SHARED_STATE = PipelineOp.class.getName() + ".sharedState";

		boolean DEFAULT_SHARED_STATE = false;

        /**
         * When <code>true</code>, the {@link QueryEngine} MAY reorder the
         * solutions as they flow through the query plan (this is done as a
         * throughput optimization). When <code>false</code>, the
         * {@link QueryEngine} MUST NOT reorder solutions.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/798" >
         *      Solution order not always preserved. </a>
         */
        String REORDER_SOLUTIONS = PipelineOp.class.getName()
                + ".reorderSolutions";

	    boolean DEFAULT_REORDER_SOLUTIONS = true;
	    
		/**
		 * This option may be used to place an optional limit on the #of
		 * concurrent tasks which may run for the same (bopId,shardId) for a
		 * given query (default {@value #DEFAULT_MAX_PARALLEL}). The query is
		 * guaranteed to make progress as long as this is some positive integer.
		 * While limiting this value can limit the concurrency with which
		 * certain operators are evaluated and that can have a negative effect
		 * on the throughput, it controls both the demand on the JVM heap and
		 * the #of threads consumed.
		 * <p>
		 * Note: {@link #MAX_PARALLEL} is the annotation for pipelined joins
		 * which has the strongest effect on performance. Changes to both
		 * {@link #MAX_MESSAGES_PER_TASK} and {@link #PIPELINE_QUEUE_CAPACITY}
		 * have less effect and performance tends to be best around a modest
		 * value (10) for those annotations.
		 * 
		 * @see ISingleThreadedOp
		 */
		String MAX_PARALLEL = PipelineOp.class.getName() + ".maxParallel";

		/**
		 * @see #MAX_PARALLEL
		 */
		int DEFAULT_MAX_PARALLEL = 5; 

		/**
		 * For a pipelined operator, this is the maximum number of messages that
		 * will be assigned to a single invocation of the evaluation task for
		 * that operator (default {@value #DEFAULT_MAX_MESSAGES_PER_TASK}). By
		 * default the {@link QueryEngine} MAY (and generally does) combine
		 * multiple {@link IChunkMessage}s from the work queue of an operator
		 * for each evaluation pass made for that operator. When ONE (1), each
		 * {@link IChunkMessage} will be assigned to a new evaluation task for
		 * the operator. The value of this annotation must be a positive
		 * integer. If the operator is not-pipelined, then the maximum amount of
		 * data to be assigned to an evaluation task is governed by
		 * {@link #MAX_MEMORY} instead.
		 */
	    String MAX_MESSAGES_PER_TASK = PipelineOp.class.getName()
	            + ".maxMessagesPerTask";

		/**
		 * @see #MAX_MESSAGES_PER_TASK
		 */
	    int DEFAULT_MAX_MESSAGES_PER_TASK = 10;

		/**
		 * For pipelined operators, this is the capacity of the input queue for
		 * that operator. Producers will block if the input queue for the target
		 * operator is at its capacity. This provides an important limit on the
		 * amount of data which can be buffered on the JVM heap during pipelined
		 * query evaluation.
		 */
		String PIPELINE_QUEUE_CAPACITY = PipelineOp.class.getName()
				+ ".pipelineQueueCapacity";

		/**
		 * @see #PIPELINE_QUEUE_CAPACITY
		 */
		int DEFAULT_PIPELINE_QUEUE_CAPACITY = 10;

		/**
		 * Annotation used to mark pipelined (aka vectored) operators. When
		 * <code>false</code> the operator will use either "at-once" or
		 * "blocked" evaluation depending on how it buffers its data for
		 * evaluation.
		 * 
		 * @see PipelineOp#isPipelinedEvaluation()
		 */
		String PIPELINED = PipelineOp.class.getName() + ".pipelined";

		/**
		 * @see #PIPELINED
		 */
		boolean DEFAULT_PIPELINED = true;

        /**
         * This annotation is only used for non-{@link #PIPELINED} operators and
         * specifies the maximum #of bytes which the operator may buffer on the
         * native heap before evaluation of the operator is triggered (default
         * {@value #DEFAULT_MAX_MEMORY}).
         * <p>
         * If an operator buffers its data on the Java heap then this MUST be
         * ZERO (0L). At-once evaluation is always used for non-pipelined
         * operators which buffer their data on the Java heap as there is no
         * ready mechanism to bound their heap demand.
         * <p>
         * If an operator buffers its data on the native heap, then this MUST be
         * some positive value which specifies the maximum #of bytes which may
         * be buffered before the operator is evaluated. At-once evaluation for
         * operators which buffer their data on the native heap is indicated
         * with the value {@link Long#MAX_VALUE}.
         * <p>
         * Note: For a sharded operation, the value is the maximum #of bytes
         * which may be buffered per shard.
         * 
         * @see PipelineOp#isPipelinedEvaluation()
         */
		String MAX_MEMORY = PipelineOp.class.getName() + ".maxMemory";

		/**
		 * @see #MAX_MEMORY
		 */
		long DEFAULT_MAX_MEMORY = 0L;
		
        /**
         * When <code>true</code> a final evaluation pass will be invoked once
         * it is know that the operator can not be re-triggered by another
         * {@link IChunkMessage}. The final evaluation pass will be associated
         * with an empty {@link IChunkMessage} as its source and
         * {@link BOpContext#isLastInvocation()} will report <code>true</code>.
         * <p>
         * Note: A final evaluation pass will be triggered even if the operator
         * was never triggered by a normal evaluation pass. This behavior is
         * necessary for several language constructs. Operators are free to do
         * nothing if they can ignore the final evaluation pass in this case.
         */
        String LAST_PASS = PipelineOp.class.getName() + ".lastPass";

		boolean DEFAULT_LAST_PASS = false;
		
//      /**
//      * For hash partitioned operators, this is the set of the member nodes
//      * for the operator.
//      * <p>
//      * This annotation is required for such operators since the set of known
//      * nodes of a given type (such as all data services) can otherwise
//      * change at runtime.
//      * 
//      * @todo Move onto an interface parallel to {@link IShardwisePipelineOp}
//      */
//     String MEMBER_SERVICES = "memberServices";

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
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

        if (getMaxParallel() < 1)
            throw new IllegalArgumentException(Annotations.MAX_PARALLEL + "="
                    + getMaxParallel());

        if (isLastPassRequested()) {

            if (getMaxParallel() != 1)
                throw new IllegalArgumentException(Annotations.MAX_PARALLEL + "="
                        + getMaxParallel());

            if (!isPipelinedEvaluation())
                throw new UnsupportedOperationException(Annotations.PIPELINED
                        + "=" + isPipelinedEvaluation());

        }
        
    }

    /**
     * @see BufferAnnotations#CHUNK_CAPACITY
     */
    final public int getChunkCapacity() {
        
        return getProperty(Annotations.CHUNK_CAPACITY,
                Annotations.DEFAULT_CHUNK_CAPACITY);

    }

    /**
     * @see BufferAnnotations#CHUNK_OF_CHUNKS_CAPACITY
     */
    final public int getChunkOfChunksCapacity() {

        return getProperty(Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

    }

    /**
     * @see BufferAnnotations#CHUNK_TIMEOUT
     */
    final public long getChunkTimeout() {
        
        return getProperty(Annotations.CHUNK_TIMEOUT,
                (long)Annotations.DEFAULT_CHUNK_TIMEOUT);
        
    }

    /**
     * @see Annotations#REORDER_SOLUTIONS
     */
    final public boolean isReorderSolutions() {

        return getProperty(Annotations.REORDER_SOLUTIONS,
                Annotations.DEFAULT_REORDER_SOLUTIONS);
        
    }
    
    /**
     * The maximum amount of memory which may be used to buffered inputs for
     * this operator on the native heap. When ZERO (0), the inputs will be
     * buffered on the JVM heap. When {@link Long#MAX_VALUE}, an essentially
     * unbounded amount of data may be buffered on the native heap. Together
     * with {@link Annotations#PIPELINED}, {@link Annotations#MAX_MEMORY} is
     * used to determine whether an operator uses <i>pipelined</i>,
     * <i>at-once</i>, or <i>blocked</i> evaluation.
     * 
     * @see Annotations#MAX_MEMORY
     */
    final public long getMaxMemory() {

        return getProperty(Annotations.MAX_MEMORY,
                Annotations.DEFAULT_MAX_MEMORY);

    }

    /**
     * Return <code>true</code> iff the operator uses pipelined evaluation
     * (versus "at-once" or "blocked" evaluation as discussed below).
     * <dl>
     * <dt>Pipelined</dt>
     * <dd>Pipelined operators stream chunks of intermediate results from one
     * operator to the next using a producer / consumer pattern. Each time a set
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
    final public boolean isPipelinedEvaluation() {
		
    	return getProperty(PipelineOp.Annotations.PIPELINED,
				PipelineOp.Annotations.DEFAULT_PIPELINED);
    	
	}

    /**
     * <code>true</code> iff the operator will use at-once evaluation (all
     * inputs for the operator will be buffered and the operator will run
     * exactly once to consume those inputs).
     * 
     * @see Annotations#PIPELINED
     * @see Annotations#MAX_MEMORY
     */
    final public boolean isAtOnceEvaluation() {

        if (isPipelinedEvaluation())
            return false;

        final long maxMemory = getMaxMemory();
        
        if (maxMemory == 0L) {
            /*
             * Operator will buffer an essentially unbounded amount of data on
             * the JVM object heap.
             */
            return true;
        }
        if(maxMemory == Long.MAX_VALUE) {
            /*
             * Operator will buffer an essentially unbounded amount of data on
             * the native heap.
             */
            return true;
        }
        
        return false;

    }

    /**
     * <code>true</code> iff the operator uses blocked evaluation (it buffers
     * data on the native heap up to a threshold and then evaluate that block of
     * data).
     * 
     * @see Annotations#PIPELINED
     * @see Annotations#MAX_MEMORY
     */
    final public boolean isBlockedEvaluation() {

        if (isPipelinedEvaluation())
            return false;

        final long maxMemory = getMaxMemory();
        
        if (maxMemory > 0L && maxMemory < Long.MAX_VALUE) {
            /*
             * Operator will buffer data up to some maximum #of bytes on the
             * native heap.
             */
            return true;
        }
        
        return false;

    }

    /**
     * Assert that this operator is annotated as an "at-once" operator which
     * buffers its data on the java heap. The requirements are:
     * 
     * <pre>
     * PIPELINED := false
     * MAX_MEMORY := 0
     * </pre>
     * 
     * When the operator is not pipelined then it is either "blocked" or
     * "at-once". When MAX_MEMORY is ZERO, the operator will buffer its data on
     * the Java heap. All operators which buffer data on the java heap will
     * buffer an unbounded amount of data and are therefore "at-once" rather
     * than "blocked". Operators which buffer their data on the native heap may
     * support either "blocked" and/or "at-once" evaluation, depending on the
     * operators. E.g., a hash join can be either "blocked" or "at-once" while
     * an ORDER-BY is always "at-once".
     */
    protected void assertAtOnceJavaHeapOp() {

        // operator is "at-once" (not pipelined).
        if (isPipelinedEvaluation()) {
            throw new UnsupportedOperationException(Annotations.PIPELINED + "="
                    + isPipelinedEvaluation());
        }

//        // operator may not be broken into multiple tasks.
//        if (getMaxParallel() != 1) {
//            throw new UnsupportedOperationException(Annotations.MAX_PARALLEL
//                    + "=" + getMaxParallel());
//        }

        // operator must buffer its data on the Java heap
        final long maxMemory = getMaxMemory();

        if (maxMemory != 0L)
            throw new UnsupportedOperationException(Annotations.MAX_MEMORY
                    + "=" + maxMemory);

    }

//	/**
//	 * Return <code>true</code> iff concurrent invocations of the operator are
//	 * permitted.
//	 * <p>
//	 * Note: Operators which are not thread-safe still permit concurrent
//	 * evaluation for <em>distinct</em> partitions. In order to ensure that all
//	 * invocations of the operator within a query are serialized (no more than
//	 * one concurrent invocation) you must also specify
//	 * {@link BOpEvaluationContext#CONTROLLER}.
//	 * 
//	 * @see Annotations#THREAD_SAFE
//	 * @see BOp.Annotations#EVALUATION_CONTEXT
//	 */
//    public boolean isThreadSafe() {
//
//		return getProperty(Annotations.THREAD_SAFE,
//				Annotations.DEFAULT_THREAD_SAFE);
//        
//    }

    /**
     * If parallel evaluation is not allowed, then throws
     * {@link IllegalArgumentException}.
     */
    final protected void assertMaxParallelOne() {

        /*
         * Note: Tests the annotation, not getMaxParallel(), since we want to
         * make sure the annotation is valid and getMaxParallel() also tests for
         * the ISingleThreadedOp interface.
         */
        if (getProperty(PipelineOp.Annotations.MAX_PARALLEL,
                PipelineOp.Annotations.DEFAULT_MAX_PARALLEL) != 1) {

            throw new IllegalArgumentException(
                    PipelineOp.Annotations.MAX_PARALLEL + "="
                            + getMaxParallel());

        }
        
    }
    
    /**
     * The maximum parallelism with which tasks may be evaluated for this
     * operator (this is a per-shard limit in scale-out). A value of ONE (1)
     * indicates that at most ONE (1) instance of this task may be executing in
     * parallel for a given shard and may be used to indicate that the operator
     * evaluation task is not thread-safe.
     * 
     * @see Annotations#MAX_PARALLEL
     * @see ISingleThreadedOp
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/1002"> </a>
     */
	final public int getMaxParallel() {

	    if(this instanceof ISingleThreadedOp) {
	        
	        // Ignore the annotation value.
	        return 1;
	        
	    }
	    
		return getProperty(PipelineOp.Annotations.MAX_PARALLEL,
				PipelineOp.Annotations.DEFAULT_MAX_PARALLEL);

	}

    /**
     * Return <code>true</code> iff a final evaluation pass is requested by the
     * operator. The final evaluation pass will be invoked once for each node or
     * shard on which the operator was evaluated once it has consumed all inputs
     * from upstream operators.
     * 
     * @see Annotations#LAST_PASS
     */
    final public boolean isLastPassRequested() {

        return getProperty(Annotations.LAST_PASS,
                Annotations.DEFAULT_LAST_PASS);
        
    }

    /**
     * Return <code>true</code> iff {@link #newStats(IQueryContext)} must be
     * shared across all invocations of {@link #eval(BOpContext)} for this
     * operator for a given query.
     * 
     * @see Annotations#SHARED_STATE
     */
    final public boolean isSharedState() {

		return getProperty(Annotations.SHARED_STATE,
				Annotations.DEFAULT_SHARED_STATE);
        
    }
    
    /**
     * Return a new object which can be used to collect statistics on the
     * operator evaluation. This may be overridden to return a more specific
     * class depending on the operator.
     */
    public BOpStats newStats() {

        return new BOpStats();

    }

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

}
