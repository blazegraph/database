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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IShardwisePipelineOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.QueryTimeoutException;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.ArrayAccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBindingSetAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.ThreadLocalBufferFactory;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStarJoin;
import com.bigdata.relation.rule.IStarJoin.IStarConstraint;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.service.DataService;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.concurrent.Haltable;
import com.bigdata.util.concurrent.LatchedExecutor;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Pipelined join operator for online (selective) queries. The pipeline join
 * accepts chunks of binding sets from its operand, combines each binding set in
 * turn with its {@link IPredicate} annotation to produce an "asBound"
 * predicate, and then executes a nested indexed subquery against that asBound
 * predicate, writing out a new binding set for each element returned by the
 * asBound predicate which satisfies the join constraint.
 * <p>
 * Note: In order to support pipelining, query plans need to be arranged in a
 * "left-deep" manner.
 * <p>
 * Note: In scale-out, the {@link PipelineJoin} is generally annotated as a
 * {@link BOpEvaluationContext#SHARDED} or {@link BOpEvaluationContext#HASHED}
 * operator and the {@link IPredicate} is annotated for local access paths. If
 * you need to use remote access paths, then the {@link PipelineJoin} should be
 * annotated as a {@link BOpEvaluationContext#ANY} operator since it will be
 * issuing a remote read from each node on which it has source solutions to be
 * joined.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Break the star join logic out into its own join operator and test suite
 *       (or just discard it).
 */
public class PipelineJoin<E> extends PipelineOp implements
		IShardwisePipelineOp<E> {

	static private final transient Logger log = Logger
			.getLogger(PipelineJoin.class);

	/**
     * 
     */
	private static final long serialVersionUID = 1L;

	public interface Annotations extends AccessPathJoinAnnotations {

//		/**
//		 * An optional {@link IVariable}[] identifying the variables to be
//		 * retained in the {@link IBindingSet}s written out by the operator. All
//		 * variables are retained unless this annotation is specified.
//		 */
//		String SELECT = (PipelineJoin.class.getName() + ".select").intern();

//
// Note: The OPTIONAL annotation is on the *predicate*.
//
//        /**
//         * Marks the join as "optional" in the SPARQL sense. Binding sets which
//         * fail the join will be routed to the alternative sink as specified by
//         * either {@link PipelineOp.Annotations#ALT_SINK_REF} or
//         * {@link PipelineOp.Annotations#ALT_SINK_GROUP}.
//         * 
//         * @see #DEFAULT_OPTIONAL
//         * 
//         * @deprecated We should just inspect
//         *             {@link IPredicate.Annotations#OPTIONAL}.
//         */
//		String OPTIONAL = PipelineJoin.class.getName() + ".optional";
//
//		boolean DEFAULT_OPTIONAL = false;

//        /**
//         * An {@link IConstraint}[] which places restrictions on the legal
//         * patterns in the variable bindings (optional).
//         */
//        String CONSTRAINTS = (PipelineJoin.class.getName() + ".constraints").intern();

		/**
		 * The maximum parallelism with which the pipeline will consume the
		 * source {@link IBindingSet}[] chunk.
		 * <p>
		 * Note: When ZERO (0), everything will run in the caller's
		 * {@link Thread}, but there will still be one thread per pipeline join
		 * task which is executing concurrently against different source chunks.
		 * When GT ZERO (0), tasks will run on an {@link ExecutorService} with
		 * the specified maximum parallelism.
		 * <p>
		 * Note: This is NOT the same as
		 * {@link PipelineOp.Annotations#MAX_PARALLEL}. This option (
		 * {@link #MAX_PARALLEL_CHUNKS} limits the #of chunks for a single task
		 * which may be processed concurrently.
		 * {@link PipelineOp.Annotations#MAX_PARALLEL} limits the #of task
		 * instances which may run concurrently.
		 * 
		 * @see #DEFAULT_MAX_PARALLEL_CHUNKS
		 * 
		 * @todo Chunk level parallelism does not appear to benefit us when
		 *       using the ChunkedRunningQuery class and the new QueryEngine, so
		 *       this option might well go away which would allow us to simplify
		 *       the PipelineJoin implementation. (All the thread local buffer
		 *       stuff would go away as would much of the complexity of the
		 *       haltable since we could directly interrupt the task if it all
		 *       runs in the caller's thread).
		 */
		String MAX_PARALLEL_CHUNKS = (PipelineJoin.class.getName() + ".maxParallelChunks").intern();

		int DEFAULT_MAX_PARALLEL_CHUNKS = 0;

		/**
		 * When <code>true</code>, binding sets observed in the same chunk which
		 * have the binding pattern on the variables for the access path will be
		 * coalesced into a single access path (default
		 * {@value #DEFAULT_COALESCE_DUPLICATE_ACCESS_PATHS}). This option
		 * increases the efficiency of the join since it reads the access path
		 * once per set of input binding sets which are coalesced. This option
		 * does NOT reduce the #of solutions generated.
		 * <p>
		 * This option can cause some error in the join hit ratio when it is
		 * estimated from a cutoff join.
		 * 
		 * @see PipelineJoinStats#getJoinHitRatio()
		 * 
		 * @todo unit tests when (en|dis)abled.
		 */
		String COALESCE_DUPLICATE_ACCESS_PATHS = (PipelineJoin.class.getName()
				+ ".coalesceDuplicateAccessPaths").intern();

        boolean DEFAULT_COALESCE_DUPLICATE_ACCESS_PATHS = true;

        /**
         * When <code>true</code>, access paths will be reordered to maximize
         * locality.
         * <p>
         * Note: This needs to be turned off when the RTO uses row identifiers
         * to correlate the input and output solutions for complex joins (those
         * which required materialization of RDF Values for FILTER evaluation).
         * 
         * @todo unit tests when (en|dis)abled.
         */
        String REORDER_ACCESS_PATHS = (PipelineJoin.class.getName() + ".reorderAccessPaths")
                .intern();

        boolean DEFAULT_REORDER_ACCESS_PATHS = true;

        /**
         * The minimum number of (estimated) data points assigned to a task. This basically
         * defines the threshold upon which parallelization starts to pay out. Currently only
         * implemented for the geospatial feature (see BLZG-1478), but this might be implemented
         * more generally for the pipelined join in the future
         */
        public String MIN_DATAPOINTS_PER_TASK = 
                (PipelineJoin.class.getName() + ".minDatapointsPerTask").intern();
             
        // note: the default might be a bit high, it is used for the GeoSpatial stuff only, where
        //       we effectively only investigate a small part of the range; we may decrease for
        //       normal access paths and, in turn, add an adjustment factor for usage in the geospatial
        //       context
        public int DEFAULT_MIN_DATAPOINTS_PER_TASK = 100000;

        
        /**
         * Desired number of access path tasks generated per thread in case the range is large enough. 
         * The default is one task per thread, which is a good choice if there's not much skew in
         * the data. If there's a lot of skew, it might be beneficial to increase this parameter to 
         * avoid a situation where a certain thread gets assigned longer-running tasks s.t. other
         * threads are finished earlier (leading to, effectively, decreased parallelism). On the downside,
         * increasing this parameter leads to overhead in setting up additional subtasks, 
         * so values >5 are generally discouraged.
         * 
         * Note that this is an upper bound only: we always consider the MIN_DATAPOINTS_PER_TASK
         * threshold in order to avoid generating threads with a low workload.
         * 
         * Currently only implemented for the geospatial feature (see BLZG-1478), but this might be 
         * implemented more generally for the pipelined join in the future.
         * 
         * Must be a value >= 1.
         */
        public String NUM_TASKS_PER_THREAD = 
           (PipelineJoin.class.getName() + ".numTasksPerThread").intern();
        
        public int DEFAULT_NUM_TASKS_PER_THREAD = 1;
        

	}

	/**
	 * Deep copy constructor.
	 * 
	 * @param op
	 */
	public PipelineJoin(final PipelineJoin<E> op) {
		super(op);
	}

	/**
	 * Shallow copy vararg constructor.
	 * 
	 * @param args
	 * @param annotations
	 */
	public PipelineJoin(final BOp[] args, final NV... annotations) {

		this(args, NV.asMap(annotations));

	}

	/**
	 * Shallow copy constructor.
	 * 
	 * @param args
	 * @param annotations
	 */
	public PipelineJoin(final BOp[] args, final Map<String, Object> annotations) {

		super(args, annotations);

        /*
         * TODO We should be checking this operator's required properties,
         * especially when used for the RTO.
         */
		
		// if (arity() != 1)
		// throw new IllegalArgumentException();

		// if (left() == null)
		// throw new IllegalArgumentException();

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see Annotations#PREDICATE
	 */
	@SuppressWarnings("unchecked")
	@Override
	public IPredicate<E> getPredicate() {

		return (IPredicate<E>) getRequiredProperty(Annotations.PREDICATE);

	}

    /**
     * Return the value of {@link IPredicate#isOptional()} for the
     * {@link IPredicate} associated with this join.
     * 
     * @see IPredicate.Annotations#OPTIONAL
     */
	private boolean isOptional() {

//		return getProperty(Annotations.OPTIONAL, Annotations.DEFAULT_OPTIONAL);
        return getPredicate().isOptional();

	}

	/**
	 * 
	 * @see Annotations#CONSTRAINTS
	 */
    public IConstraint[] constraints() {

        return getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

    }
    
	/**
	 * @see Annotations#MAX_PARALLEL_CHUNKS
	 */
	public int getMaxParallelChunks() {

		// return 5;
		return getProperty(Annotations.MAX_PARALLEL_CHUNKS,
				Annotations.DEFAULT_MAX_PARALLEL_CHUNKS);

	}

	/**
	 * @see Annotations#SELECT
	 */
	public IVariable<?>[] variablesToKeep() {

		return getProperty(Annotations.SELECT, null/* defaultValue */);

	}

	@Override
	public PipelineJoinStats newStats() {

		return new PipelineJoinStats();

	}

	@Override
	public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

		return new FutureTaskMon<Void>(new JoinTask<E>(this, context));

	}

	/**
	 * Pipeline join impl.
	 */
	private static class JoinTask<E> extends Haltable<Void> implements
			Callable<Void> {

		/**
		 * The join that is being executed.
		 */
		final private PipelineJoin<?> joinOp;

		/**
		 * The constraint (if any) specified for the join operator.
		 */
		final private IConstraint[] constraints;

		/**
		 * The maximum parallelism with which the {@link JoinTask} will consume
		 * the source {@link IBindingSet}s.
		 * 
		 * @see Annotations#MAX_PARALLEL_CHUNKS
		 */
		final private int maxParallelChunks;

		/**
		 * The service used for executing subtasks (optional).
		 * 
		 * @see #maxParallelChunks
		 */
		final private Executor service;

		/**
		 * True iff the {@link #predicate} operand is an optional pattern (aka
		 * if this is a SPARQL style left join).
		 */
		final private boolean optional;

		/**
		 * The variables to be retained by the join operator. Variables not
		 * appearing in this list will be stripped before writing out the
		 * binding set onto the output sink(s).
		 */
		final private IVariable<?>[] variablesToKeep;

		/**
		 * The source for the elements to be joined.
		 */
		final private IPredicate<E> predicate;

		/**
		 * The relation associated with the {@link #predicate} operand.
		 */
		final private IRelation<E> relation;

		/**
		 * The partition identifier -or- <code>-1</code> if we are not reading
		 * on an index partition.
		 */
		final private int partitionId;

		/**
		 * The evaluation context.
		 */
		final private BOpContext<IBindingSet> context;

//		/**
//		 * When <code>true</code>, the {@link #stats} will be tracked. This is
//		 * <code>false</code> unless logging is requested for {@link QueryLog}
//		 * or stats are explicitly request (e.g., to support cutoff joins).
//		 */
//		final private boolean trackStats;
		
		/**
		 * The statistics for this {@link JoinTask}.
		 */
		final private PipelineJoinStats stats;

		/**
		 * An optional limit on the #of solutions to be produced. The limit is
		 * ignored if it is {@link Long#MAX_VALUE}.
		 * <p>
		 * Note: Invoking {@link #halt(Object)} is necessary to enforce the
		 * limit.
		 * 
		 * @see Annotations#LIMIT
		 */
		final private long limit;

		/**
		 * When <code>true</code> an attempt will be made to coalesce as-bound
		 * predicates which result in the same access path.
		 * 
		 * @see Annotations#COALESCE_DUPLICATE_ACCESS_PATHS
		 */
		final private boolean coalesceAccessPaths;

        /**
         * When <code>true</code>, access paths will be reordered to maximize
         * locality.
         * 
         * @see Annotations#REORDER_ACCESS_PATHS
         */
		final private boolean reorderAccessPaths;

		/**
		 * Used to enforce the {@link Annotations#LIMIT} iff one is specified.
		 */
		final private AtomicLong exactOutputCount = new AtomicLong();

		/**
		 * The source from which we read the binding set chunks.
		 * <p>
		 * Note: In keeping with the top-down evaluation of the operator tree
		 * the source should not be set until we begin to execute the
		 * {@link #left} operand and that should not happen until we are in
		 * {@link #call()} in order to ensure that the producer will be
		 * terminated if there is a problem setting up this join. Given that, it
		 * might need to be an atomic reference or volatile or the like.
		 */
		final private ICloseableIterator<IBindingSet[]> source;

		/**
		 * Where the join results are written.
		 * <p>
		 * Chunks of bindingSets are written pre-Thread unsynchronized buffers
		 * by {@link ChunkTask}. Those unsynchronized buffers overflow onto the
		 * per-JoinTask {@link #sink}, which is a thread-safe
		 * {@link IBlockingBuffer}. The downstream pipeline operator drains that
		 * {@link IBlockingBuffer} using its iterator(). When the {@link #sink}
		 * is closed and everything in it has been drained, then the downstream
		 * operator will conclude that no more bindingSets are available and it
		 * will terminate.
		 */
		final private IBlockingBuffer<IBindingSet[]> sink;

		/**
		 * The alternative sink to use when the join is {@link #optional} AND
		 * {@link BOpContext#getSink2()} returns a distinct buffer for the
		 * alternative sink. The binding sets from the source are copied onto
		 * the alternative sink for an optional join if the join fails. Normally
		 * the {@link BOpContext#getSink()} can be used for both the joins which
		 * succeed and those which fail. The alternative sink is only necessary
		 * when the failed join needs to jump out of a join group rather than
		 * routing directly to the ancestor in the operator tree.
		 */
		final private IBlockingBuffer<IBindingSet[]> sink2;

		/**
		 * The thread-local buffer factory for the default sink.
		 */
		final private TLBFactory threadLocalBufferFactory;

		/**
		 * The thread-local buffer factory for the optional sink (iff the
		 * optional sink is defined).
		 */
		final private TLBFactory threadLocalBufferFactory2;

		/**
		 * Instances of this class MUST be created in the appropriate execution
		 * context of the target {@link DataService} so that the federation and
		 * the joinNexus references are both correct and so that it has access
		 * to the local index object for the specified index partition.
		 * 
		 * @param joinOp
		 * @param context
		 */
		public JoinTask(//
				final PipelineJoin<E> joinOp,//
				final BOpContext<IBindingSet> context) {

			if (joinOp == null)
				throw new IllegalArgumentException();
			if (context == null)
				throw new IllegalArgumentException();

			this.joinOp = joinOp;
			this.predicate = joinOp.getPredicate();
			this.constraints = joinOp.constraints();
			this.maxParallelChunks = joinOp.getMaxParallelChunks();
			if (maxParallelChunks < 0)
				throw new IllegalArgumentException(Annotations.MAX_PARALLEL_CHUNKS
						+ "=" + maxParallelChunks);
			if (maxParallelChunks > 0) {
				// shared service.
				service = new LatchedExecutor(context.getIndexManager()
						.getExecutorService(), maxParallelChunks);
			} else {
				// run in the caller's thread.
				service = null;
			}
			this.optional = joinOp.isOptional();
			this.variablesToKeep = joinOp.variablesToKeep();
			this.context = context;
			this.relation = context.getRelation(predicate);
			this.source = context.getSource();
			this.sink = context.getSink();
			this.sink2 = context.getSink2();
			this.partitionId = context.getPartitionId();
			this.stats = (PipelineJoinStats) context.getStats();
			this.limit = joinOp.getProperty(Annotations.LIMIT,
					Annotations.DEFAULT_LIMIT);
			this.coalesceAccessPaths = joinOp.getProperty(
					Annotations.COALESCE_DUPLICATE_ACCESS_PATHS,
					Annotations.DEFAULT_COALESCE_DUPLICATE_ACCESS_PATHS);
            this.reorderAccessPaths = joinOp.getProperty(
                    Annotations.REORDER_ACCESS_PATHS,
                    Annotations.DEFAULT_REORDER_ACCESS_PATHS);

			this.threadLocalBufferFactory = new TLBFactory(sink);

			this.threadLocalBufferFactory2 = sink2 == null ? null
					: new TLBFactory(sink2);

			if (log.isDebugEnabled())
				log.debug("joinOp=" + joinOp);

		}

		@Override
		public String toString() {

			return getClass().getName() + "{ joinOp=" + joinOp + "}";

		}

		/**
		 * Runs the {@link JoinTask}.
		 * 
		 * @return <code>null</code>.
		 */
		@Override
		public Void call() throws Exception {

			// final long begin = System.currentTimeMillis();

			if (log.isDebugEnabled())
				log.debug("joinOp=" + joinOp);

			try {

				/*
				 * Consume bindingSet chunks from the source JoinTask(s).
				 */
				consumeSource();

				/*
				 * Flush and close the thread-local output buffers.
				 */
				threadLocalBufferFactory.flush();
				if (threadLocalBufferFactory2 != null)
					threadLocalBufferFactory2.flush();

				// flush the sync buffer
				flushAndCloseBuffersAndAwaitSinks();

				if (log.isDebugEnabled())
					log.debug("JoinTask done: joinOp=" + joinOp);

				halted();

				return null;

			} catch (Throwable t) {

				/*
				 * This is used for processing errors and also if this task is
				 * interrupted (because the sink has been closed).
				 */
				// ensure query halts.
				halt(t);
                
				// reset the unsync buffers.
				try {
					// resetUnsyncBuffers();
					threadLocalBufferFactory.reset();
					if (threadLocalBufferFactory2 != null)
						threadLocalBufferFactory2.reset();
				} catch (Throwable t2) {
					log.error(t2.getLocalizedMessage(), t2);
				}

				// reset the sync buffer and cancel the sink JoinTasks.
				try {
					cancelSinks();
				} catch (Throwable t2) {
					log.error(t2.getLocalizedMessage(), t2);
				}

				/*
				 * Close source iterators, which will cause any source JoinTasks
				 * that are still executing to throw a CancellationException
				 * when the Future associated with the source iterator is
				 * cancelled.
				 */
				try {
					closeSources();
				} catch (Throwable t2) {
					log.error(t2.getLocalizedMessage(), t2);
				}

				if (getCause() != null) {
					// abnormal termination.
					throw new RuntimeException(t);
				}
				// normal termination - ignore exception.
				return null;

				// } finally {
				//            	
				// stats.elapsed.add(System.currentTimeMillis() - begin);

//			} finally {
//				System.err.println(joinOp.toString());
//				System.err.println(stats.toString());
			}

		}

		/**
		 * Consume {@link IBindingSet} chunks from the {@link #source}.
		 * 
		 * @throws Exception
		 */
		protected void consumeSource() throws Exception {

			IBindingSet[] chunk;

			while (!isDone() && (chunk = nextChunk()) != null) {

				if (chunk.length == 0) {
					/*
					 * This can happen if you feed in an empty IBindingSet[] to
					 * the join.
					 */
					continue;
				}

				/*
				 * Consume the chunk until done using either the caller's thread
				 * or the executor service as appropriate to run subtasks.
				 */
				if (chunk.length <= 1) {

					/*
					 * Run on the caller's thread anyway since there is just one
					 * binding set to be consumed.
					 */

					new BindingSetConsumerTask(null/* service */, chunk)
							.call();

				} else {

					/*
					 * Run subtasks on either the caller's thread or the shared
					 * executed service depending on the configured value of
					 * [maxParallel].
					 */

					new BindingSetConsumerTask(service, chunk).call();

				}

			}

		}

		/**
		 * Closes the {@link #source} specified to the ctor.
		 */
		protected void closeSources() {

			if (log.isInfoEnabled())
				log.info(toString());

			source.close();

		}

		/**
		 * Flush and close all output buffers and await sink {@link JoinTask}
		 * (s).
		 * <p>
		 * Note: You MUST close the {@link BlockingBuffer} from which each sink
		 * reads <em>before</em> invoking this method in order for those sinks
		 * to terminate. Otherwise the source {@link IAsynchronousIterator}(s)
		 * on which the sink is reading will remain open and the sink will never
		 * decide that it has exhausted its source(s).
		 * 
		 * @throws InterruptedException
		 * @throws ExecutionException
		 */
		protected void flushAndCloseBuffersAndAwaitSinks()
				throws InterruptedException, ExecutionException {

			if (log.isDebugEnabled())
				log.debug("joinOp=" + joinOp);

			/*
			 * Close the thread-safe output buffer. For any JOIN except the
			 * last, this buffer will be the source for one or more sink
			 * JoinTasks for the next join dimension. Once this buffer is
			 * closed, the asynchronous iterator draining the buffer will
			 * eventually report that there is nothing left for it to process.
			 * 
			 * Note: This is a BlockingBuffer. BlockingBuffer#flush() is a NOP.
			 */

			sink.flush();
			sink.close();

			if (sink2 != null) {
				sink2.flush();
				sink2.close();
			}

		}

        /**
         * {@inheritDoc}
         * <p>
         * Note: The {@link JoinTask} extends {@link Haltable}. We want to treat
         * the {@link QueryTimeoutException} as a normal termination cause for a
         * {@link JoinTask}. The {@link Haltable} for the
         * {@link AbstractRunningQuery} will have its root cause set to the
         * {@link QueryTimeoutException} and from there the exception will get
         * eventually converted back into the appropriate openrdf exception. We
         * do things differently here because the {@link JoinTask} termination
         * is not the same as the {@link AbstractRunningQuery} termination.
         * {@link JoinTask} is the only place right now where we extend haltable
         * and the only place where we have to make this specific override.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/772">
         *      Query timeout only checked at operator start/stop. </a>
         */
		@Override
	    protected boolean isNormalTerminationCause(final Throwable cause) {

		    return super.isNormalTerminationCause(cause)
                    || super.isDeadlineTerminationCause(cause);
		    
		}
		
		/**
		 * Cancel sink {@link JoinTask}(s).
		 */
		protected void cancelSinks() {

			if (log.isDebugEnabled())
				log.debug("joinOp=" + joinOp);

			sink.reset();

			if (sink.getFuture() != null) {

				sink.getFuture().cancel(true/* mayInterruptIfRunning */);

			}

			if (sink2 != null) {

				sink2.reset();

				if (sink2.getFuture() != null) {

					sink2.getFuture().cancel(true/* mayInterruptIfRunning */);

				}

			}

		}

		/**
		 * Return a chunk of {@link IBindingSet}s from source.
		 * 
		 * @return The next chunk -or- <code>null</code> iff the source is
		 *         exhausted.
		 */
		protected IBindingSet[] nextChunk() throws InterruptedException {

            if (log.isDebugEnabled())
                log.debug("joinOp=" + joinOp);

            /*
             * We no longer have chunks which are being read from a remote
             * process. All chunks are fully materialized before a PipelineOp
             * begins to execute. Therefore there is no longer any reason to use
             * the non-blocking API in PipelineJoin.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/475
             */
//			while (!source.isExhausted()) {
//
//				halted();
//
//				// note: uses timeout to avoid blocking w/o testing [halt].
//				if (source.hasNext(10, TimeUnit.MILLISECONDS)) {
//
//					// read the chunk.
//					final IBindingSet[] chunk = source.next();
//
//					 stats.chunksIn.increment(); 
//					 stats.unitsIn.add(chunk.length);
//
//					if (log.isDebugEnabled())
//						log.debug("Read chunk from source: chunkSize="
//								+ chunk.length + ", joinOp=" + joinOp);
//
//					return chunk;
//
//				}
//
            // }
            while (source.hasNext()) {

                halted();

                // read the chunk.
                final IBindingSet[] chunk = source.next();

                stats.chunksIn.increment();
                stats.unitsIn.add(chunk.length);

                if (log.isDebugEnabled())
                    log.debug("Read chunk from source: chunkSize="
                            + chunk.length + ", joinOp=" + joinOp);

                return chunk;

            }
            
			/*
			 * Termination condition: the source is exhausted.
			 */

			if (log.isDebugEnabled())
				log.debug("Source exhausted: joinOp=" + joinOp);

			return null;

		}

		/**
		 * Class consumes a chunk of binding set executing a nested indexed join
		 * until canceled, interrupted, or all the binding sets are exhausted.
		 * For each {@link IBindingSet} in the chunk, an {@link AccessPathTask}
		 * is created which will consume that {@link IBindingSet}. The
		 * {@link AccessPathTask}s are sorted based on their
		 * <code>fromKey</code> so as to order the execution of those tasks in a
		 * manner that will maximize the efficiency of index reads. The ordered
		 * {@link AccessPathTask}s are then submitted to the caller's
		 * {@link Executor} or run in the caller's thread if the executor is
		 * <code>null</code>.
		 * 
		 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
		 *         Thompson</a>
		 */
		protected class BindingSetConsumerTask implements Callable<Void> {

			private final Executor executor;
			private final IBindingSet[] chunk;

			/**
			 * 
			 * @param executor
			 *            The service that will execute the generated
			 *            {@link AccessPathTask}s -or- <code>null</code> IFF you
			 *            want the {@link AccessPathTask}s to be executed in the
			 *            caller's thread.
			 * @param chunk
			 *            A chunk of binding sets from the upstream producer.
			 */
			public BindingSetConsumerTask(final Executor executor,
					final IBindingSet[] chunk) {

				if (chunk == null)
					throw new IllegalArgumentException();

				this.executor = executor;

				this.chunk = chunk;

			}

			/**
			 * Read chunks from one or more sources until canceled, interrupted,
			 * or all sources are exhausted and submits {@link AccessPathTask}s
			 * to the caller's {@link ExecutorService} -or- executes those tasks
			 * in the caller's thread if no {@link ExecutorService} was provided
			 * to the ctor.
			 * <p>
			 * Note: When running with an {@link ExecutorService}, the caller is
			 * responsible for waiting on that {@link ExecutorService} until the
			 * {@link AccessPathTask}s to complete and must verify all tasks
			 * completed successfully.
			 * 
			 * @return <code>null</code>
			 * 
			 * @throws BufferClosedException
			 *             if there is an attempt to output a chunk of
			 *             {@link IBindingSet}s or {@link ISolution}s and the
			 *             output buffer is an {@link IBlockingBuffer} (true for
			 *             all join dimensions exception the lastJoin and also
			 *             true for query on the lastJoin) and that
			 *             {@link IBlockingBuffer} has been closed.
			 */
			@Override
			public Void call() throws Exception {

				try {

					if (chunk.length == 1) {

						// fast path if the chunk has a single binding set.
						runOneTask();

						return null;

					}

					/*
					 * Generate (and optionally coalesce) the access path tasks.
					 */
					final AccessPathTask[] tasks = generateAccessPaths(chunk);

                    /*
                     * Reorder those tasks for better index read performance.
                     */
                    if (reorderAccessPaths)
                        reorderTasks(tasks);

					/*
					 * Execute the tasks (either in the caller's thread or on
					 * the supplied service).
					 */
					executeTasks(tasks);

					return null;

				} catch (Throwable t) {
					// ensure query halts.
					halt(t);
					if (getCause() != null) {
						// abnormal termination.
//						log.error("Halting join (abnormal termination): t="+t+" : cause="+getCause());
//						throw new RuntimeException("Halting join: " + t, t);
					    throw new RuntimeException(t);
					}
					// normal termination - ignore exception.
					if (log.isDebugEnabled())
						log.debug("Caught and ignored exception: " + t);
					return null;

				}

			}

			/**
			 * There is exactly one {@link IBindingSet} in the chunk, so run
			 * exactly one {@link AccessPathTask}.
			 * 
			 * @throws Exception
			 */
			private void runOneTask() throws Exception {

				if (chunk.length != 1)
					throw new AssertionError();

				final IBindingSet bindingSet = chunk[0];

				// constrain the predicate to the given bindings.
				IPredicate<E> asBound = predicate.asBound(bindingSet);

                if (asBound == null) {

                    /*
                     * This can happen for a SIDS mode join if some of the
                     * (s,p,o,[c]) and SID are bound on entry and they can not
                     * be unified. For example, the s position might be
                     * inconsistent with the Subject that can be decoded from
                     * the SID binding.
                     * 
                     * @see #815 (RDR query does too much work)
                     */

                    return;

                }
				
				if (partitionId != -1) {

					/*
					 * Constrain the predicate to the desired index partition.
					 * 
					 * Note: we do this for scale-out joins since the access
					 * path will be evaluated by a JoinTask dedicated to this
					 * index partition, which is part of how we give the
					 * JoinTask to gain access to the local index object for an
					 * index partition.
					 */

					asBound = asBound.setPartitionId(partitionId);

				}

				new JoinTask.AccessPathTask(asBound, Arrays.asList(chunk))
						.call();

			}

			/**
			 * Generate (and optionally coalesce) the {@link AccessPathTask}s
			 * for the chunk.
			 * 
			 * @param chunk
			 *            The chunk.
			 * 
			 * @return The tasks to process that chunk.
			 */
			protected AccessPathTask[] generateAccessPaths(
					final IBindingSet[] chunk) {

				final AccessPathTask[] tasks;

				if (coalesceAccessPaths) {

                    /*
                     * Aggregate the source bindingSets that license the same
                     * asBound predicate. The predicates in the keys of this map
                     * as "as-bound".
                     */
					final Map<IPredicate<E>, Collection<IBindingSet>> map = combineBindingSets(chunk);

					/*
					 * Generate an AccessPathTask from each distinct asBound
					 * predicate that will consume all of the source bindingSets
					 * in the chunk which resulted in the same asBound
					 * predicate.
					 */
					tasks = getAccessPathTasks(map);

				} else {

					/*
					 * Do not coalesce access paths.
					 */

					final List<AccessPathTask> tmp = new LinkedList<AccessPathTask>();

					for (int i = 0; i < chunk.length; i++) {

						final IBindingSet bindingSet = chunk[i];

						// constrain the predicate to the given bindings.
						IPredicate<E> asBound = predicate.asBound(bindingSet);

	                    if (asBound == null) {

	                        /*
	                         * This can happen for a SIDS mode join if some of the
	                         * (s,p,o,[c]) and SID are bound on entry and they can not
	                         * be unified. For example, the s position might be
	                         * inconsistent with the Subject that can be decoded from
	                         * the SID binding.
	                         * 
	                         * @see #815 (RDR query does too much work)
	                         */

	                        continue;

	                    }

	                    if (partitionId != -1) {

							/*
							 * Constrain the predicate to the desired index
							 * partition.
							 * 
							 * Note: we do this for scale-out joins since the
							 * access path will be evaluated by a JoinTask
							 * dedicated to this index partition, which is part
							 * of how we give the JoinTask to gain access to the
							 * local index object for an index partition.
							 */

							asBound = asBound.setPartitionId(partitionId);

						}

						tmp.add(new AccessPathTask(asBound, Collections
								.singletonList(bindingSet)));

					}

                    // Exact fit array.
                    tasks = tmp
                            .toArray(new JoinTask.AccessPathTask[tmp.size()]);

				}

				return tasks;

			}

			/**
			 * Populates a map of asBound predicates paired to a set of
			 * bindingSets.
			 * <p>
			 * Note: The {@link AccessPathTask} will apply each bindingSet to
			 * each element visited by the {@link IAccessPath} obtained for the
			 * asBound {@link IPredicate}. This has the natural consequence of
			 * eliminating subqueries within the chunk.
			 * 
			 * @param chunk
			 *            A chunk of bindingSets from the source join dimension.
			 * 
			 * @return A map which pairs the distinct asBound predicates to the
			 *         bindingSets in the chunk from which the predicate was
			 *         generated.
			 */
			protected Map<IPredicate<E>, Collection<IBindingSet>> combineBindingSets(
					final IBindingSet[] chunk) {

				if (log.isDebugEnabled())
					log.debug("chunkSize=" + chunk.length);

                /*
                 * Note: HashMap is used in preference to LinkedHashMap for
                 * better speed. We do not need to maintain order in this
                 * collection.
                 */
				final Map<IPredicate<E>, Collection<IBindingSet>> map = new HashMap<IPredicate<E>, Collection<IBindingSet>>(
						chunk.length);

				for (IBindingSet bindingSet : chunk) {

					halted();

					// constrain the predicate to the given bindings.
					IPredicate<E> asBound = predicate.asBound(bindingSet);

	                if (asBound == null) {

	                    /*
	                     * This can happen for a SIDS mode join if some of the
	                     * (s,p,o,[c]) and SID are bound on entry and they can not
	                     * be unified. For example, the s position might be
	                     * inconsistent with the Subject that can be decoded from
	                     * the SID binding.
	                     * 
	                     * @see #815 (RDR query does too much work)
	                     */

	                    continue;

	                }

	                if (partitionId != -1) {

						/*
						 * Constrain the predicate to the desired index
						 * partition.
						 * 
						 * Note: we do this for scale-out joins since the access
						 * path will be evaluated by a JoinTask dedicated to
						 * this index partition, which is part of how we give
						 * the JoinTask to gain access to the local index object
						 * for an index partition.
						 */

						asBound = asBound.setPartitionId(partitionId);

					}

					// lookup the asBound predicate in the map.
//					final HashedPredicate<E> hashedPred = new HashedPredicate<E>(
//							asBound);
					final IPredicate<E> hashedPred = asBound;
					Collection<IBindingSet> values = map.get(hashedPred);

					if (values == null) {

						/*
						 * This is the first bindingSet for this asBound
						 * predicate. We create a collection of bindingSets to
						 * be paired with that predicate and put the collection
						 * into the map using that predicate as the key.
						 */

						values = new LinkedList<IBindingSet>();

						map.put(hashedPred, values);

					} else {

						// more than one bindingSet will use the same access
						// path.
						 stats.accessPathDups.increment(); 

					}

					/*
					 * Add the bindingSet to the collection of bindingSets
					 * paired with the asBound predicate.
					 */

					values.add(bindingSet);

				}

				if (log.isDebugEnabled())
					log.debug("chunkSize=" + chunk.length
							+ ", #distinct predicates=" + map.size());

				return map;

			}

			/**
			 * Creates an {@link AccessPathTask} for each {@link IBindingSet} in
			 * the given chunk.
			 * 
			 * @param chunk
			 *            A chunk of {@link IBindingSet}s from one or more
			 *            source {@link JoinTask}s.
			 * 
			 * @return A chunk of {@link AccessPathTask} in a desirable
			 *         execution order.
			 * 
			 * @throws Exception
			 */
			protected AccessPathTask[] getAccessPathTasks(
					final Map<IPredicate<E>, Collection<IBindingSet>> map) {

				final int n = map.size();

				if (log.isDebugEnabled())
					log.debug("#distinct predicates=" + n);

				final AccessPathTask[] tasks = new JoinTask.AccessPathTask[n];

				final Iterator<Map.Entry<IPredicate<E>, Collection<IBindingSet>>> itr = map
						.entrySet().iterator();

				int i = 0;

				while (itr.hasNext()) {

					halted();

					final Map.Entry<IPredicate<E>, Collection<IBindingSet>> entry = itr
							.next();

					tasks[i++] = new AccessPathTask(entry.getKey(), entry
							.getValue());

				}

				return tasks;

			}

			/**
			 * The tasks are ordered based on the <i>fromKey</i> for the
			 * associated {@link IAccessPath} as licensed by each
			 * {@link IBindingSet}. This order tends to focus the reads on the
			 * same parts of the index partitions with a steady progression in
			 * the <i>fromKey</i> as we process a chunk of {@link IBindingSet}s.
			 * 
			 * @param tasks
			 *            The tasks.
          * @see <a href="http://trac.bigdata.com/ticket/1192" >
          * RDR query fails with ArrayIndexOutOfBoundsException </a>
          */
         protected void reorderTasks(final AccessPathTask[] tasks) {

            if (tasks.length == 0) {
               // See #1192.
               return;
            }
            
            // @todo layered access paths do not expose a fromKey.
            if (tasks[0].accessPath instanceof AccessPath<?>) {

               // reorder the tasks.
               Arrays.sort(tasks);

            }

         }

			/**
			 * Either execute the tasks in the caller's thread or schedule them
			 * for execution on the supplied service.
			 * 
			 * @param tasks
			 *            The tasks.
			 * 
			 * @throws Exception
			 */
			protected void executeTasks(final AccessPathTask[] tasks)
					throws Exception {

				if (executor == null) {

					/*
					 * No Executor, so run each task in the caller's thread.
					 */

					for (AccessPathTask task : tasks) {

						task.call();

					}

					return;

				}

				/*
				 * Build list of FutureTasks. This list is used to check all
				 * tasks for errors and ensure that any running tasks are
				 * cancelled.
				 */

				final List<FutureTask<Void>> futureTasks = new LinkedList<FutureTask<Void>>();

				for (AccessPathTask task : tasks) {

					final FutureTask<Void> ft = new FutureTaskMon<Void>(task);

					futureTasks.add(ft);

				}

				try {

					/*
					 * Execute all tasks.
					 */
					for (FutureTask<Void> ft : futureTasks) {

						halted();

						// Queue for execution.
						executor.execute(ft);

					} // next task.

					/*
					 * Wait for each task. If any task throws an exception, then
					 * [halt] will become true and any running tasks will error
					 * out quickly. Once [halt := true], we do not wait for any
					 * more tasks, but proceed to cancel all tasks in the
					 * finally {} clause below.
					 */
					for (FutureTask<Void> ft : futureTasks) {

						// Wait for a task.
						if (!isDone())
							ft.get();

					}

				} finally {

					/*
					 * Ensure that all tasks are cancelled, regardless of
					 * whether they were started or have already finished.
					 */
					for (FutureTask<Void> ft : futureTasks) {

						ft.cancel(true/* mayInterruptIfRunning */);

					}

				}

			}

		}

		/**
		 * Accepts an asBound {@link IPredicate} and a (non-empty) collection of
		 * {@link IBindingSet}s each of which licenses the same asBound
		 * predicate for the current join dimension. The task obtains the
		 * corresponding {@link IAccessPath} and delegates each chunk visited on
		 * that {@link IAccessPath} to a {@link ChunkTask}. Note that optionals
		 * are also handled by this task.
		 * 
		 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
		 *         Thompson</a>
		 */
		protected class AccessPathTask implements Callable<Void>,
				Comparable<AccessPathTask> {

			/**
			 * The {@link IBindingSet}s from the source join dimension to be
			 * combined with each element visited on the {@link #accessPath}. If
			 * there is only a single source {@link IBindingSet} in a given
			 * chunk of source {@link IBindingSet}s that results in the same
			 * asBound {@link IPredicate} then this will be an array with a
			 * single element. However, if multiple source {@link IBindingSet}s
			 * result in the same asBound {@link IPredicate} within the same
			 * chunk then those are aggregated and appear together in this
			 * array.
			 * <p>
			 * Note: An array is used for thread-safe traversal.
			 */
			final private IBindingSet[] bindingSets;

			/**
			 * An array correlated with the {@link #bindingSets} whose values
			 * are the #of solutions generated for each of the source binding
			 * sets consumed by this {@link AccessPathTask}. This array is used
			 * to determine, whether or not any solutions were produced for a
			 * given {@link IBindingSet}. When the join is optional and no
			 * solutions were produced for a given {@link IBindingSet}, the
			 * {@link IBindingSet} is output anyway.
			 */
			final private int[] naccepted;

			/**
			 * The {@link IAccessPath} corresponding to the asBound
			 * {@link IPredicate} for this join dimension. The asBound
			 * {@link IPredicate} is {@link IAccessPath#getPredicate()}.
			 */
			final private IAccessPath<E> accessPath;

			            /**
             * Return the <em>fromKey</em> for the {@link IAccessPath} generated
             * from the {@link IBindingSet} for this task.
             * 
             * @todo Layered access paths do not expose a fromKey, but the
             *       information we need is available
             *       {@link IKeyOrder#getFromKey(IKeyBuilder, IPredicate)}.
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/631">
             *      ClassCastException in SIDs mode query </a>
             */
			protected byte[] getFromKey() {

                if (accessPath instanceof EmptyAccessPath) {
                 
                    // EmptyAccessPath does not extend AccessPath.
                    return BytesUtil.EMPTY;
                    
                }
                
                return ((AccessPath<E>) accessPath).getFromKey();

			}

			@Override
			public int hashCode() {
				return super.hashCode();
			}

			/**
			 * Return <code>true</code> iff the tasks are equivalent (same as
			 * bound predicate). This test may be used to eliminate duplicates
			 * that arise when different source {@link JoinTask}s generate the
			 * same {@link IBindingSet}.
			 * 
			 * @param o
			 *            Another task.
			 * 
			 * @return if the as bound predicate is equals().
			 */
			@Override
			public boolean equals(final Object o) {

				if (this == o)
					return true;

				if (!(o instanceof JoinTask.AccessPathTask))
					return false;

				return accessPath.getPredicate().equals(
						((AccessPathTask) o).accessPath.getPredicate());

			}

			/**
			 * Evaluate an {@link IBindingSet} for the join dimension. When the
			 * task runs, it will pair each element visited on the
			 * {@link IAccessPath} with the asBound {@link IPredicate}. For each
			 * element visited, if the binding is acceptable for the constraints
			 * on the asBound {@link IPredicate}, then the task will emit one
			 * {@link IBindingSet} for each source {@link IBindingSet}.
			 * 
			 * @param predicate
			 *            The asBound {@link IPredicate}.
			 * @param bindingSets
			 *            A collection of {@link IBindingSet}s from the source
			 *            join dimension that all result in the same asBound
			 *            {@link IPredicate}.
			 */
			public AccessPathTask(final IPredicate<E> predicate,
					final Collection<IBindingSet> bindingSets) {

				if (predicate == null)
					throw new IllegalArgumentException();

				if (bindingSets == null)
					throw new IllegalArgumentException();

				/*
				 * Note: this needs to be the access path for the local index
				 * partition. We handle this by (a) constraining the predicate
				 * to the desired index partition; (b) using an IJoinNexus that
				 * is initialized once the JoinTask starts to execute inside of
				 * the ConcurrencyManager; (c) declaring; and (d) using the
				 * index partition name NOT the scale-out index name.
				 */

				final int n = bindingSets.size();

				if (n == 0)
					throw new IllegalArgumentException();

				this.accessPath = context.getAccessPath(relation, predicate);

				if (log.isDebugEnabled()) {
					log.debug("joinOp=" + joinOp);
					log.debug("#bindingSets=" + n);
					log.debug("accessPath=" + accessPath);
				}

				// convert to array for thread-safe traversal.
				this.bindingSets = bindingSets.toArray(new IBindingSet[n]);

				this.naccepted = new int[n];

			}

		    @Override
			public String toString() {

				return JoinTask.this.getClass().getSimpleName() + "{ joinOp="
						+ joinOp + ", #bindingSets=" + bindingSets.length + "}";

			}

			/**
			 * Evaluate the {@link #accessPath} against the {@link #bindingSets}
			 * . If nothing is accepted and {@link IPredicate#isOptional()} then
			 * the {@link #bindingSets} is output anyway (this implements the
			 * semantics of OPTIONAL).
			 * 
			 * @return <code>null</code>.
			 * 
			 * @throws BufferClosedException
			 *             if there is an attempt to output a chunk of
			 *             {@link IBindingSet}s or {@link ISolution}s and the
			 *             output buffer is an {@link IBlockingBuffer} (true for
			 *             all join dimensions exception the lastJoin and also
			 *             true for query on the lastJoin) and that
			 *             {@link IBlockingBuffer} has been closed.
			 */
		    @Override
			public Void call() throws Exception {

				halted();

				if (limit != Long.MAX_VALUE && exactOutputCount.get() > limit) {
					// break query @ limit.
					if (log.isInfoEnabled())
						log.info("Breaking query @ limit: limit=" + limit
								+ ", exactOutputCount="
								+ exactOutputCount.get());
					halt((Void) null);
					return null;
				}

				// range count of the as-bound access path (should be cached).
				final long rangeCount = accessPath
						.rangeCount(false/* exact */);
				
				if (log.isDebugEnabled()) {
					log.debug("range count: " + rangeCount);
				}
				
				stats.accessPathCount.increment();

				stats.accessPathRangeCount.add(rangeCount);

                if (accessPath.getPredicate() instanceof IStarJoin<?>) {

                    /*
                     * Star join. This has not yet proven to be more efficient.
                     * Presumably we wind up with sufficiently good IO locality
                     * with a normal pipeline join that the star join does not
                     * improve matters further.
                     */
					
                    handleStarJoin();

				} else {

				    if(true && accessPath instanceof IBindingSetAccessPath) {
				        // Handle join in terms of an IBindingSet iterator.
                        handleJoin2();
				    } else {
				        // Handle join in terms of an IElement[] iterator.
				        handleJoin();
				    }

				}

				return null;

			}

            /**
             * A vectored pipeline join (chunk at a time processing) for
             * {@link IElement}s.
             */@Deprecated // by handleJoin2()
			protected void handleJoin() {

				final long cutoffLimit = predicate.getProperty(
						IPredicate.Annotations.CUTOFF_LIMIT,
						IPredicate.Annotations.DEFAULT_CUTOFF_LIMIT);

				// Obtain the iterator for the current join dimension.
				final IChunkedOrderedIterator<?> itr = accessPath.iterator(0,
						cutoffLimit, 0/* capacity */);

				try {

					// Each thread gets its own buffer.
					final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer = threadLocalBufferFactory
							.get();

					// #of input solutions consumed (pre-increment).
					stats.inputSolutions.add(bindingSets.length); 

					while (itr.hasNext()) {

						halted();
						
						final Object[] chunk = itr.nextChunk();

						stats.accessPathChunksIn.increment();

//						System.err.println("#chunks="+stats.accessPathChunksIn+", chunkSize="+chunk.length);
						
						// process the chunk in the caller's thread.
						new ChunkTask(bindingSets, naccepted, unsyncBuffer,
								chunk).call();

					} // next chunk.

					if (optional) {

                        /*
                         * Note: when NO binding sets were accepted AND the
                         * predicate is OPTIONAL then we output the _original_
                         * binding set(s) to the sink join task(s). The
                         * CONSTRAINT(s) are NOT applied for the optional
                         * solutions.
                         */

						// Thread-local buffer iff optional sink is in use.
						final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = threadLocalBufferFactory2 == null ? null
								: threadLocalBufferFactory2.get();

						for (int bindex = 0; bindex < bindingSets.length; bindex++) {

							if (naccepted[bindex] > 0)
								continue;

							final IBindingSet bs = bindingSets[bindex];

//							if (constraints != null) {
//								if(!BOpUtility.isConsistent(constraints, bs)) {
//								    // Failed by the constraint on the join.
//								    continue;
//								}
//							}
							
							if (log.isTraceEnabled())
								log
										.trace("Passing on solution which fails an optional join: "
												+ bs);

							if (limit != Long.MAX_VALUE
									&& exactOutputCount.incrementAndGet() > limit) {
								// break query @ limit.
								if (log.isInfoEnabled())
									log.info("Breaking query @ limit: limit=" + limit
											+ ", exactOutputCount="
											+ exactOutputCount.get());
								halt((Void) null);
								break;
							}

							if (unsyncBuffer2 == null) {
								// use the default sink.
								unsyncBuffer.add(bs);
							} else {
								// use the alternative sink.
								unsyncBuffer2.add(bs);
							}

							stats.outputSolutions.increment();

						}

					}
					return;

				} catch (Throwable t) {

					// ensure query halts.
					halt(t);
					if (getCause() != null) {
						// abnormal termination.
						throw new RuntimeException(t);
					}
					// normal termination - ignore exception.

				} finally {

					itr.close();

				}

			}

            /**
             * A vectored pipeline join (chunk at a time processing) based on
             * the visitation of {@link IBindingSet}s
             */
            protected void handleJoin2() {

            		final long cutoffLimit = predicate.getProperty(
            			IPredicate.Annotations.CUTOFF_LIMIT, 
            			IPredicate.Annotations.DEFAULT_CUTOFF_LIMIT);
            	
                // Obtain the iterator for the current join dimension.
                final ICloseableIterator<IBindingSet[]> itr = ((IBindingSetAccessPath<?>) accessPath)
                      .solutions(context, cutoffLimit, stats);

                try {

                    // Each thread gets its own buffer.
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer = threadLocalBufferFactory
                            .get();

                    // #of input solutions consumed (pre-increment).
                    stats.inputSolutions.add(bindingSets.length); 

                    while (itr.hasNext()) {

                        halted();
                        
                        int naccepted = 0;
                        
                        final IBindingSet[] rightChunk = itr.next(); // AP solution chunk.
                        
                        for(IBindingSet right : rightChunk) { // next solution from AP.
                        
                        int bindex = 0; // index 1:1 with bindingSets[].
                        for (IBindingSet left : bindingSets) { // upstream pipeline solutions.

                            // join solutions.
                            final IBindingSet bset = BOpContext.bind(left, right,
//                                    true/* leftIsPipeline */,
                                    constraints,
                                    variablesToKeep);
                            
                            if (bset != null) {
                                // solutions joined.
                                if (limit != Long.MAX_VALUE
                                        && exactOutputCount.incrementAndGet() > limit) {
                                    // break query @ limit.
                                    if (log.isInfoEnabled())
                                        log.info("Breaking query @ limit: limit=" + limit
                                                + ", exactOutputCount="
                                                + exactOutputCount.get());
                                    halt((Void) null);
                                    break;
                                }

                                // Accept this binding set.
                                unsyncBuffer.add(bset);

                                // #of binding sets accepted.
                                naccepted++;
                                
                                // #of elements accepted for this binding set.
                                this.naccepted[bindex]++;

                                // #of output solutions generated.
                                stats.outputSolutions.increment(); 

                            }

                            if (bindex++ % 50 == 0) {
                                // Periodically check for an interrupt.
                                if (Thread.interrupted())
                                    throw new InterruptedException();
                            }

                        }

                        if (log.isDebugEnabled())
                            if (naccepted == 0) {
                                log.debug("Rejected solution: " + right);
                            } else {
                                log.debug("Accepted solution for " + naccepted
                                        + " of " + bindingSets.length
                                        + " possible bindingSet combinations: "
                                        + right);
                            }

                    } // next left (upstream) solution from leftChunk (aka bindingSets).

                    } // next right (AP) solution from rightChunk
                    
                    if (optional) {

                        /*
                         * Note: when NO binding sets were accepted AND the
                         * predicate is OPTIONAL then we output the _original_
                         * binding set(s) to the sink join task(s). The
                         * CONSTRAINT(s) are NOT applied for the optional
                         * solutions.
                         */

                        // Thread-local buffer iff optional sink is in use.
                        final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = threadLocalBufferFactory2 == null ? null
                                : threadLocalBufferFactory2.get();

                        for (int bindex = 0; bindex < bindingSets.length; bindex++) {

                            if (naccepted[bindex] > 0)
                                continue;

                            final IBindingSet bs = bindingSets[bindex];

                            if (log.isTraceEnabled())
                                log
                                        .trace("Passing on solution which fails an optional join: "
                                                + bs);

                            if (limit != Long.MAX_VALUE
                                    && exactOutputCount.incrementAndGet() > limit) {
                                // break query @ limit.
                                if (log.isInfoEnabled())
                                    log.info("Breaking query @ limit: limit=" + limit
                                            + ", exactOutputCount="
                                            + exactOutputCount.get());
                                halt((Void) null);
                                break;
                            }

                            if (unsyncBuffer2 == null) {
                                // use the default sink.
                                unsyncBuffer.add(bs);
                            } else {
                                // use the alternative sink.
                                unsyncBuffer2.add(bs);
                            }

                            stats.outputSolutions.increment();

                        }

                    }
                    return;

                } catch (Throwable t) {

                    // ensure query halts.
                    halt(t);
                    if (getCause() != null) {
                        // abnormal termination.
                        throw new RuntimeException(t);
                    }
                    // normal termination - ignore exception.

                } finally {

                    itr.close();

                }

            }

			protected void handleStarJoin() {

				IBindingSet[] solutions = this.bindingSets;

				final IStarJoin starJoin = (IStarJoin) accessPath
						.getPredicate();

				/*
				 * FIXME The star join does not handle the alternative sink yet.
				 * See the ChunkTask for the normal join.
				 */
				final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer = threadLocalBufferFactory
						.get();

				// Obtain the iterator for the current join dimension.
				final IChunkedOrderedIterator<?> itr = accessPath.iterator();

				// The actual #of elements scanned.
				int numElements = 0;

				try {

					/*
					 * Note: The fast range count would give us an upper bound,
					 * unless expanders are used, in which case there can be
					 * more elements visited.
					 */
					final Object[] elements;
					{

						/*
						 * First, gather all chunks.
						 */
						int nchunks = 0;
						final List<Object[]> chunks = new LinkedList<Object[]>();
						while (itr.hasNext()) {

							final Object[] chunk = (Object[]) itr.nextChunk();

							// add to list of chunks.
							chunks.add(chunk);

							numElements += chunk.length;

							stats.accessPathChunksIn.increment(); 

							nchunks++;

						} // next chunk.

						/*
						 * Now flatten the chunks into a simple array.
						 */
						if (nchunks == 0) {
							// No match.
							return;
						}
						if (nchunks == 1) {
							// A single chunk.
							elements = chunks.get(0);
						} else {
							// Flatten the chunks.
							elements = new Object[numElements];
							{
								int n = 0;
								for (Object[] chunk : chunks) {

									System.arraycopy(chunk/* src */,
											0/* srcPos */, elements/* dst */,
											n/* dstPos */, chunk.length/* len */);

									n += chunk.length;
								}
							}
						}
						stats.accessPathUnitsIn.add(numElements); 

					}

					if (numElements > 0) {

						final Iterator<IStarConstraint<?>> it = starJoin
								.getStarConstraints();

						boolean constraintFailed = false;

						while (it.hasNext()) {

							final IStarConstraint constraint = it.next();

							Collection<IBindingSet> constraintSolutions = null;

							final int numVars = constraint.getNumVars();

							for (int i = 0; i < numElements; i++) {

								final Object e = elements[i];

								if (constraint.isMatch(e)) {

									/*
									 * For each match for the constraint, we
									 * clone the old solutions and create a new
									 * solutions that appends the variable
									 * bindings from this match.
									 * 
									 * At the end, we set the old solutions
									 * collection to the new solutions
									 * collection.
									 */

									if (constraintSolutions == null) {

										constraintSolutions = new LinkedList<IBindingSet>();

									}

									for (IBindingSet bs : solutions) {

										if (numVars > 0) {

											bs = bs.clone();

											constraint.bind(bs, e);

										}

										constraintSolutions.add(bs);

									}

									// no reason to keep testing SPOs, there can
									// be only one
									if (numVars == 0) {

										break;

									}

								}

							}

							if (constraintSolutions == null) {

								/*
								 * We did not find any matches to this
								 * constraint. That is ok, as long it's
								 * optional.
								 */
								if (constraint.isOptional() == false) {

									constraintFailed = true;

									break;

								}

							} else {

								/*
								 * Set the old solutions to the new solutions,
								 * and move on to the next constraint.
								 */
								solutions = constraintSolutions
										.toArray(new IBindingSet[constraintSolutions
												.size()]);

							}

						}

						if (!constraintFailed) {

							for (IBindingSet bs : solutions) {

								if (limit != Long.MAX_VALUE
										&& exactOutputCount.incrementAndGet() > limit) {
									// break query @ limit.
									if (log.isInfoEnabled())
										log.info("Breaking query @ limit: limit=" + limit
												+ ", exactOutputCount="
												+ exactOutputCount.get());
									halt((Void) null);
									break;
								}

								unsyncBuffer.add(bs);

								// #of output solutions generated
								stats.outputSolutions.increment();

							}

						}

					}

					return;

				} catch (Throwable t) {

					// ensure query halts.
					halt(t);
					if (getCause() != null) {
						// abnormal termination.
						throw new RuntimeException(t);
					}
					// normal termination - ignore exception.

				} finally {

					itr.close();

				}

			}

			/**
			 * Imposes an order based on the <em>fromKey</em> for the
			 * {@link IAccessPath} associated with the task.
			 * 
			 * @param o
			 * 
			 * @return
			 */
		    @Override
			public int compareTo(final AccessPathTask o) {

				/*
				 * Just go ahead and run the ArrayAccessPaths first.
				 */
				if (accessPath instanceof ArrayAccessPath)
					return -1;
				if (o.accessPath instanceof ArrayAccessPath)
					return 1;
				
				return BytesUtil.compareBytes(getFromKey(), o.getFromKey());

			}

		}

		/**
		 * Task processes a chunk of elements read from the {@link IAccessPath}
		 * for a join dimension. Each element in the chunk in paired with a copy
		 * of the given bindings. If that {@link IBindingSet} is accepted by the
		 * {@link IRule}, then the {@link IBindingSet} will be output. The
		 * {@link IBindingSet}s to be output are buffered into chunks and the
		 * chunks added to the {@link JoinPipelineTask#bindingSetBuffers} for
		 * the corresponding predicate.
		 * 
		 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
		 *         Thompson</a>
		 */
		protected class ChunkTask implements Callable<Void> {

			/**
			 * The {@link IBindingSet}s which the each element in the chunk will
			 * be paired to create {@link IBindingSet}s for the downstream join
			 * dimension.
			 */
			private final IBindingSet[] bindingSets;

			/**
			 * The #of solutions accepted for each of the {@link #bindingSets}.
			 */
			private final int[] naccepted;

			/**
			 * A per-{@link Thread} buffer that is used to collect
			 * {@link IBindingSet}s into chunks before handing them off to the
			 * next join dimension. The hand-off occurs no later than when the
			 * current join dimension finishes consuming its source(s).
			 */
			private final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer;

			/**
			 * A chunk of elements read from the {@link IAccessPath} for the
			 * current join dimension.
			 */
			private final Object[] chunk;

			/**
			 * 
			 * @param bindingSet
			 *            The bindings with which the each element in the chunk
			 *            will be paired to create the bindings for the
			 *            downstream join dimension.
			 * @param naccepted
			 *            An array used to indicate as a side-effect the #of
			 *            solutions accepted for each of the {@link IBindingSet}
			 *            s.
			 * @param unsyncBuffer
			 *            A per-{@link Thread} buffer used to accumulate chunks
			 *            of generated {@link IBindingSet}s.
			 * @param chunk
			 *            A chunk of elements read from the {@link IAccessPath}
			 *            for the current join dimension.
			 */
			public ChunkTask(
					final IBindingSet[] bindingSet,
					final int[] naccepted,
					final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer,
					final Object[] chunk) {

				if (bindingSet == null)
					throw new IllegalArgumentException();

				if (naccepted == null)
					throw new IllegalArgumentException();

				if (unsyncBuffer == null)
					throw new IllegalArgumentException();

				if (chunk == null)
					throw new IllegalArgumentException();

				this.bindingSets = bindingSet;

				this.naccepted = naccepted;

				this.chunk = chunk;

				this.unsyncBuffer = unsyncBuffer;

			}

			/**
			 * 
			 * @throws BufferClosedException
			 *             if there is an attempt to output a chunk of
			 *             {@link IBindingSet}s or {@link ISolution}s and the
			 *             output buffer is an {@link IBlockingBuffer} (true for
			 *             all join dimensions exception the lastJoin and also
			 *             true for query on the lastJoin) and that
			 *             {@link IBlockingBuffer} has been closed.
			 */
		    @Override
			public Void call() throws Exception {

				try {

					for (Object e : chunk) {

						if (isDone())
							return null;

						// naccepted for the current element (trace only).
						int naccepted = 0;

						stats.accessPathUnitsIn.increment(); 

						int bindex = 0;
						for (IBindingSet bset : bindingSets) {

                            // #of binding sets accepted.
//						    naccepted++;
						    
//                            /* #of elements accepted for this binding set.
//                             * 
//                             * Note: We count binding sets as accepted before we
//                             * apply the constraints. This has the effect that
//                             * an optional join which produces solutions that
//                             * are then rejected by a FILTER associated with the
//                             * optional predicate WILL NOT pass on the original
//                             * solution even if ALL solutions produced by the
//                             * join are rejected by the filter.
//                             */
//                            this.naccepted[bindex]++;

							/*
							 * Clone the binding set since it is tested for each
							 * element visited.
							 */
							bset = bset.clone();

							// propagate bindings from the visited element.
							if (BOpContext.bind(predicate, constraints, e, bset)) {

								// optionally strip off unnecessary variables.
								bset = variablesToKeep == null ? bset : bset
										.copy(variablesToKeep);

								if (limit != Long.MAX_VALUE
										&& exactOutputCount.incrementAndGet() > limit) {
									// break query @ limit.
									if (log.isInfoEnabled())
										log.info("Breaking query @ limit: limit=" + limit
												+ ", exactOutputCount="
												+ exactOutputCount.get());
									halt((Void) null);
									break;
								}

								// Accept this binding set.
								unsyncBuffer.add(bset);

								// #of binding sets accepted.
								naccepted++;
								
								// #of elements accepted for this binding set.
								this.naccepted[bindex]++;

								// #of output solutions generated.
								stats.outputSolutions.increment(); 

							}

							bindex++;

						}

						if (log.isDebugEnabled())
							if (naccepted == 0) {
								log.debug("Rejected element: " + e.toString());
							} else {
								log.debug("Accepted element for " + naccepted
										+ " of " + bindingSets.length
										+ " possible bindingSet combinations: "
										+ e.toString());
							}
					}

					// Done.
					return null;

				} catch (Throwable t) {

					// ensure query halts.
					halt(t);
					if (getCause() != null) {
						// abnormal termination.
						throw new RuntimeException(t);
					}
					// normal termination - ignore exception.
					return null;

				}

			}

		} // class ChunkTask

		/**
		 * Concrete implementation with hooks to halt a join.
		 * 
		 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
		 *         Thompson</a>
		 */
		private class TLBFactory
				extends
				ThreadLocalBufferFactory<AbstractUnsynchronizedArrayBuffer<IBindingSet>, IBindingSet> {

			final private IBlockingBuffer<IBindingSet[]> sink;

			/**
			 * 
			 * @param sink
			 *            The thread-safe buffer onto which the thread-local
			 *            buffer overflow.
			 */
			public TLBFactory(final IBlockingBuffer<IBindingSet[]> sink) {

				if (sink == null)
					throw new IllegalArgumentException();

				this.sink = sink;

			}

			@Override
			protected AbstractUnsynchronizedArrayBuffer<IBindingSet> initialValue() {

				/*
				 * Wrap the buffer provided to the constructor with a thread
				 * local buffer.
				 */

				return new UnsyncLocalOutputBuffer<IBindingSet>(
				/* stats, */joinOp.getChunkCapacity(), sink);

			}

			@Override
			protected void halted() {

				JoinTask.this.halted();

			}

		} // class TLBFactory

	}// class JoinTask

}
