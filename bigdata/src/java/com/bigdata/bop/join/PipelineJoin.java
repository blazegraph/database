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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.counters.CAT;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
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
import com.bigdata.util.concurrent.Haltable;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * Pipelined join operator for online (selective) queries. The pipeline join
 * accepts chunks of binding sets from its left operand, combines each binding
 * set in turn with the right operand to produce an "asBound" predicate, and
 * then executes a nested indexed subquery against that asBound predicate,
 * writing out a new binding set for each element returned by the asBound
 * predicate which satisfies the join constraint.
 * <p>
 * Note: In order to support pipelining, query plans need to be arranged in a
 * "left-deep" manner and there may not be intervening operators between the
 * pipeline join operator and the {@link IPredicate} on which it will read.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Break the star join logic out into its own join operator and test
 *       suite.
 */
public class PipelineJoin extends BindingSetPipelineOp {

    static private final Logger log = Logger.getLogger(PipelineJoin.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BindingSetPipelineOp.Annotations {

        /**
         * An optional {@link IVariable}[] identifying the variables to be
         * retained in the {@link IBindingSet}s written out by the operator.
         * All variables are retained unless this annotation is specified.
         */
        String SELECT = PipelineJoin.class.getName() + ".select";
        
        /**
         * An optional {@link IConstraint}[] which places restrictions on the
         * legal patterns in the variable bindings.
         */
        String CONSTRAINTS = PipelineJoin.class.getName() + ".constraints";

        /**
         * Marks the join as "optional" in the SPARQL sense. Binding sets which
         * fail the join will be routed to the alternative sink as specified by
         * {@link BindingSetPipelineOp.Annotations#ALT_SINK_REF}.
         * 
         * @see #DEFAULT_OPTIONAL
         */
        String OPTIONAL = PipelineJoin.class.getName() + ".optional";

        boolean DEFAULT_OPTIONAL = false;

        /**
         * The maximum parallelism with which the pipeline will consume the
         * source {@link IBindingSet}[] chunk.
         * <p>
         * Note: When ZERO (0), everything will run in the caller's
         * {@link Thread}, but there will still be one thread per pipeline join
         * task which is executing concurrently against different source chunks.
         * When GT ZERO (0), tasks will run on an {@link ExecutorService} with
         * the specified maximum parallelism.
         * 
         * @see #DEFAULT_MAX_PARALLEL
         */
        String MAX_PARALLEL = PipelineJoin.class.getName() + ".maxParallel";

        int DEFAULT_MAX_PARALLEL = 0;
        
    }

    /**
     * Extended statistics for the join operator.
     */
    public static class PipelineJoinStats extends BOpStats {

        private static final long serialVersionUID = 1L;
        
        /**
         * The #of duplicate access paths which were detected and filtered out.
         */
        public final CAT accessPathDups = new CAT();
        /**
         * The #of access paths which were evaluated.
         */
        public final CAT accessPathCount = new CAT();
        
        /**
         * The #of chunks read from an {@link IAccessPath}.
         */
        public final CAT chunkCount = new CAT();
        
        /**
         * The #of elements read from an {@link IAccessPath}.
         */
        public final CAT elementCount = new CAT();

        /**
         * The maximum observed fan in for this join dimension (maximum #of
         * sources observed writing on any join task for this join dimension).
         * Since join tasks may be closed and new join tasks re-opened for the
         * same query, join dimension and index partition, and since each join
         * task for the same join dimension could, in principle, have a
         * different fan in based on the actual binding sets propagated this is
         * not necessarily the "actual" fan in for the join dimension. You would
         * have to track the #of distinct partitionId values to track that.
         */
        public int fanIn;

        /**
         * The maximum observed fan out for this join dimension (maximum #of
         * sinks on which any join task is writing for this join dimension).
         * Since join tasks may be closed and new join tasks re-opened for the
         * same query, join dimension and index partition, and since each join
         * task for the same join dimension could, in principle, have a
         * different fan out based on the actual binding sets propagated this is
         * not necessarily the "actual" fan out for the join dimension.
         */
        public int fanOut;

        public void add(final BOpStats o) {

            super.add(o);
            
            if (o instanceof PipelineJoinStats) {

                final PipelineJoinStats t = (PipelineJoinStats) o;

                accessPathDups.add(t.accessPathDups.get());

                accessPathCount.add(t.accessPathCount.get());

                chunkCount.add(t.chunkCount.get());

                elementCount.add(t.elementCount.get());

                if (t.fanIn > this.fanIn) {
                    // maximum reported fanIn for this join dimension.
                    this.fanIn = t.fanIn;
                }
                if (t.fanOut > this.fanOut) {
                    // maximum reported fanOut for this join dimension.
                    this.fanOut += t.fanOut;
                }

            }
            
        }
        
        @Override
        protected void toString(final StringBuilder sb) {
            sb.append(",accessPathDups=" + accessPathDups.estimate_get());
            sb.append(",accessPathCount=" + accessPathCount.estimate_get());
            sb.append(",chunkCount=" + chunkCount.estimate_get());
            sb.append(",elementCount=" + elementCount.estimate_get());
        }
        
    }
    
    /**
     * @param left
     *            The left operand, which must be an {@link IBindingSet}
     *            pipeline operator, such as another {@link PipelineJoin}.
     * @param right
     *            The right operand, which must be an {@link IPredicate}.
     * 
     * @param annotations
     */
    public PipelineJoin(final BindingSetPipelineOp left,
            final IPredicate<?> right, final Map<String, Object> annotations) {

        super(new BOp[] { left, right }, annotations);

        if (left == null)
            throw new IllegalArgumentException();

        if (right == null)
            throw new IllegalArgumentException();

    }

    protected BindingSetPipelineOp left() {

        return (BindingSetPipelineOp) get(0);

    }

    protected IPredicate<?> right() {

        return (IPredicate<?>) get(1);

    }

    /**
     * @see Annotations#CONSTRAINTS
     */
    protected IConstraint[] constraints() {

        return getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

    }

    /**
     * @see Annotations#OPTIONAL
     */
    protected boolean isOptional() {

        return getProperty(Annotations.OPTIONAL, Annotations.DEFAULT_OPTIONAL);

    }

    /**
     * @see Annotations#MAX_PARALLEL
     */
    protected int getMaxParallel() {

        return getProperty(Annotations.MAX_PARALLEL, Annotations.DEFAULT_MAX_PARALLEL);

    }

    /**
     * @see Annotations#SELECT
     */
    protected IVariable<?>[] variablesToKeep() {

        return getProperty(Annotations.SELECT, null/* defaultValue */);

    }

    @Override
    public PipelineJoinStats newStats() {

        return new PipelineJoinStats();
        
    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new JoinTask(this, context));
        
    }

    /**
     * Pipeline join impl.
     */
    private static class JoinTask extends Haltable<Void> implements Callable<Void> {

        /**
         * The join that is being executed.
         */
        final private PipelineJoin joinOp;

        /**
         * The constraint (if any) specified for the join operator.
         */
        final private IConstraint[] constraints;

        /**
         * The maximum parallelism with which the {@link JoinTask} will
         * consume the source {@link IBindingSet}s.
         * 
         * @see Annotations#MAX_PARALLEL
         */
        final private int maxParallel;

        /**
         * The service used for executing subtasks (optional).
         * 
         * @see #maxParallel
         */
        final private Executor service;

        /**
         * True iff the {@link #right} operand is an optional pattern (aka if
         * this is a SPARQL style left join).
         */
        final private boolean optional;

        /**
         * The variables to be retained by the join operator. Variables not
         * appearing in this list will be stripped before writing out the
         * binding set onto the output sink(s).
         */
        final private IVariable<?>[] variablesToKeep;

//        /**
//         * The source for the binding sets.
//         */
//        final BindingSetPipelineOp left;

        /**
         * The source for the elements to be joined.
         */
        final private IPredicate<?> right;

        /**
         * The relation associated with the {@link #right} operand.
         */
        final private IRelation<?> relation;
        
        /**
         * The partition identifier -or- <code>-1</code> if we are not reading
         * on an index partition.
         */
        final private int partitionId;
        
        /**
         * The evaluation context.
         */
        final private BOpContext<IBindingSet> context;

        /**
         * The statistics for this {@link JoinTask}.
         */
        final private PipelineJoinStats stats;

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
        final private IAsynchronousIterator<IBindingSet[]> source;

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
         * alternative sink. The binding sets from the source are copied onto the
         * alternative sink for an optional join if the join fails. Normally the
         * {@link BOpContext#getSink()} can be used for both the joins which
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
                final PipelineJoin joinOp,//
                final BOpContext<IBindingSet> context
                ) {

            if (joinOp == null)
                throw new IllegalArgumentException();
            if (context == null)
                throw new IllegalArgumentException();

//            this.fed = context.getFederation();
            this.joinOp = joinOp;
//            this.left = joinOp.left();
            this.right = joinOp.right();
            this.constraints = joinOp.constraints();
            this.maxParallel = joinOp.getMaxParallel();
            if (maxParallel > 0) {
                // shared service.
                service = new LatchedExecutor(context.getIndexManager()
                        .getExecutorService(), maxParallel);
            } else {
                // run in the caller's thread.
                service = null;
            }
            this.optional = joinOp.isOptional();
            this.variablesToKeep = joinOp.variablesToKeep();
            this.context = context;
            this.relation = context.getReadRelation(right);
            this.source = context.getSource();
            this.sink = context.getSink();
            this.sink2 = context.getSink2();
            this.partitionId = context.getPartitionId();
            this.stats = (PipelineJoinStats) context.getStats();

            this.threadLocalBufferFactory = new TLBFactory(sink);
            
            this.threadLocalBufferFactory2 = sink2 == null ? null
                    : new TLBFactory(sink2);

            if (log.isDebugEnabled())
                log.debug("joinOp=" + joinOp);

        }

        public String toString() {

            return getClass().getName() + "{ joinOp=" + joinOp + "}";

        }

        /**
         * Runs the {@link JoinTask}.
         * 
         * @return <code>null</code>.
         */
        public Void call() throws Exception {

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

                throw new RuntimeException(t);

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

                /*
                 * Consume the chunk until done using either the caller's thread
                 * or the executor service as appropriate to run subtasks.
                 */
                if (chunk.length <= 1) {
              
                    /*
                     * Run on the caller's thread anyway since there is just one
                     * binding set to be consumed.
                     */
                    
                    new BindingSetConsumerTask(null/* service */, chunk).call();
                    
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
            
            if(sink2!=null) {
                sink2.flush();
                sink2.close();
            }
            
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

            while (!source.isExhausted()) {

                halted();

                // note: uses timeout to avoid blocking w/o testing [halt].
                if (source.hasNext(10, TimeUnit.MILLISECONDS)) {

                    // read the chunk.
                    final IBindingSet[] chunk = source.next();

                    stats.chunksIn.increment();
                    stats.unitsIn.add(chunk.length);

                    if (log.isDebugEnabled())
                        log.debug("Read chunk from source: chunkSize="
                                + chunk.length + ", joinOp=" + joinOp);

                    return chunk;

                }

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
         * @version $Id$
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
            public Void call() throws Exception {

                try {

                    /*
                     * Aggregate the source bindingSets that license the same
                     * asBound predicate.
                     */
                    final Map<IPredicate<?>, Collection<IBindingSet>> map = combineBindingSets(chunk);

                    /*
                     * Generate an AccessPathTask from each distinct asBound
                     * predicate that will consume all of the source bindingSets
                     * in the chunk which resulted in the same asBound
                     * predicate.
                     */
                    final AccessPathTask[] tasks = getAccessPathTasks(map);

                    /*
                     * Reorder those tasks for better index read performance.
                     */
                    reorderTasks(tasks);

                    /*
                     * Execute the tasks (either in the caller's thread or on
                     * the supplied service).
                     */
                    executeTasks(tasks);

                    return null;

                } catch (Throwable t) {

                    halt(t);

                    throw new RuntimeException(t);

                }

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
            protected Map<IPredicate<?>, Collection<IBindingSet>> combineBindingSets(
                    final IBindingSet[] chunk) {

                if (log.isDebugEnabled())
                    log.debug("chunkSize=" + chunk.length);

                final Map<IPredicate<?>, Collection<IBindingSet>> map = new LinkedHashMap<IPredicate<?>, Collection<IBindingSet>>(
                        chunk.length);

                for (IBindingSet bindingSet : chunk) {

                    halted();

                    // constrain the predicate to the given bindings.
                    IPredicate<?> predicate = right.asBound(bindingSet);

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

                        predicate = predicate.setPartitionId(partitionId);

                    }

                    // lookup the asBound predicate in the map.
                    Collection<IBindingSet> values = map.get(predicate);

                    if (values == null) {

                        /*
                         * This is the first bindingSet for this asBound
                         * predicate. We create a collection of bindingSets to
                         * be paired with that predicate and put the collection
                         * into the map using that predicate as the key.
                         */

                        values = new LinkedList<IBindingSet>();

                        map.put(predicate, values);

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
                    final Map<IPredicate<?>, Collection<IBindingSet>> map) {

                final int n = map.size();

                if (log.isDebugEnabled())
                    log.debug("#distinct predicates=" + n);

                final AccessPathTask[] tasks = new AccessPathTask[n];

                final Iterator<Map.Entry<IPredicate<?>, Collection<IBindingSet>>> itr = map
                        .entrySet().iterator();

                int i = 0;

                while (itr.hasNext()) {

                    halted();

                    final Map.Entry<IPredicate<?>, Collection<IBindingSet>> entry = itr
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
             */
            protected void reorderTasks(final AccessPathTask[] tasks) {

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

                    final FutureTask<Void> ft = new FutureTask<Void>(task);

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
         * @version $Id$
         */
        protected class AccessPathTask implements Callable<Void>,
                Comparable<AccessPathTask> {

            /**
             * The {@link IBindingSet}s from the source join dimension to be
             * combined with each element visited on the {@link #accessPath}. If
             * there is only a single source {@link IBindingSet} in a given
             * chunk of source {@link IBindingSet}s that results in the same
             * asBound {@link IPredicate} then this will be a collection with a
             * single member. However, if multiple source {@link IBindingSet}s
             * result in the same asBound {@link IPredicate} within the same
             * chunk then those are aggregated and appear together in this
             * collection.
             * <p>
             * Note: An array is used for thread-safe traversal.
             */
            final private IBindingSet[] bindingSets;

            /**
             * The {@link IAccessPath} corresponding to the asBound
             * {@link IPredicate} for this join dimension. The asBound
             * {@link IPredicate} is {@link IAccessPath#getPredicate()}.
             */
            final private IAccessPath<?> accessPath;

            /**
             * Return the <em>fromKey</em> for the {@link IAccessPath} generated
             * from the {@link IBindingSet} for this task.
             * 
             * @todo Layered access paths do not expose a fromKey, but the
             *       information we need is available
             *       {@link IKeyOrder#getFromKey(IKeyBuilder, IPredicate)}.
             */
            protected byte[] getFromKey() {

                return ((AccessPath<?>) accessPath).getFromKey();

            }
            
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
            public boolean equals(final Object o) {

                if (this == o)
                    return true;
                
                if (!(o instanceof AccessPathTask))
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
            public AccessPathTask(final IPredicate<?> predicate,
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

                this.accessPath = context.getAccessPath(relation,
                        predicate);

                if (log.isDebugEnabled())
                    log.debug("joinOp=" + joinOp + ", #bindingSets=" + n
                            + ", accessPath=" + accessPath);

                // convert to array for thread-safe traversal.
                this.bindingSets = bindingSets.toArray(new IBindingSet[n]);

            }

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
            public Void call() throws Exception {

                halted();

                stats.accessPathCount.increment();

                if (accessPath.getPredicate() instanceof IStarJoin<?>) {

                    handleStarJoin();

                } else {

                    handleJoin();

                }

                return null;

            }

            /**
             * A vectored pipeline join (chunk at a time processing).
             */
            protected void handleJoin() {

                boolean nothingAccepted = true;

                // Obtain the iterator for the current join dimension.
                final IChunkedOrderedIterator<?> itr = accessPath.iterator();

                try {

                    // Each thread gets its own buffer.
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer = threadLocalBufferFactory
                            .get();

                    // Thread-local buffer iff optional sink is in use.
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = threadLocalBufferFactory2 == null ? null
                            : threadLocalBufferFactory2.get();
                    
                    while (itr.hasNext()) {

                        final Object[] chunk = itr.nextChunk();

                        stats.chunkCount.increment();

                        // process the chunk in the caller's thread.
                        final boolean somethingAccepted = new ChunkTask(
                                bindingSets, unsyncBuffer, chunk).call();

                        if (somethingAccepted) {

                            // something in the chunk was accepted.
                            nothingAccepted = false;

                        }

                    } // next chunk.

                    if (nothingAccepted && optional) {

                        /*
                         * Note: when NO binding sets were accepted AND the
                         * predicate is OPTIONAL then we output the _original_
                         * binding set(s) to the sink join task(s).
                         */

                        for (IBindingSet bs : this.bindingSets) {

                            if (unsyncBuffer2 == null) {
                                // use the default sink.
                                unsyncBuffer.add(bs);
                            } else {
                                // use the alternative sink.
                                unsyncBuffer2.add(bs);
                            }

                        }

                    }

                    return;

                } catch (Throwable t) {

                    halt(t);

                    throw new RuntimeException(t);

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

                            stats.chunkCount.increment();

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

                                    System
                                            .arraycopy(chunk/* src */,
                                                    0/* srcPos */,
                                                    elements/* dst */,
                                                    n/* dstPos */, chunk.length/* len */);

                                    n += chunk.length;
                                }
                            }
                        }
                        stats.elementCount.add(numElements);

                    }

                    if (numElements > 0) {

                        final Iterator<IStarConstraint<?>> it = starJoin
                                .getStarConstraints();

                        boolean constraintFailed = false;

                        while (it.hasNext()) {

                            final IStarConstraint constraint = it.next();

                            Collection<IBindingSet> constraintSolutions = null;

                            int numVars = constraint.getNumVars();

                            for (int i = 0; i < numElements; i++) {

                                Object e = elements[i];

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

                                unsyncBuffer.add(bs);

                            }

                        }

                    }

                    return;

                } catch (Throwable t) {

                    halt(t);

                    throw new RuntimeException(t);

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
            public int compareTo(final AccessPathTask o) {

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
         * @version $Id$
         */
        protected class ChunkTask implements Callable<Boolean> {

            /**
             * The {@link IBindingSet}s which the each element in the chunk will
             * be paired to create {@link IBindingSet}s for the downstream join
             * dimension.
             */
            private final IBindingSet[] bindingSets;

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
             * @param unsyncBuffer
             *            A per-{@link Thread} buffer used to accumulate chunks
             *            of generated {@link IBindingSet}s (optional). When the
             *            {@link ChunkTask} will be run in its own thread, pass
             *            <code>null</code> and the buffer will be obtained in
             *            {@link #call()}.
             * @param chunk
             *            A chunk of elements read from the {@link IAccessPath}
             *            for the current join dimension.
             */
            public ChunkTask(
                    final IBindingSet[] bindingSet,
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer,
                    final Object[] chunk) {

                if (bindingSet == null)
                    throw new IllegalArgumentException();

                // Allow null!
                // if (unsyncBuffer == null)
                // throw new IllegalArgumentException();

                if (chunk == null)
                    throw new IllegalArgumentException();

//                this.tailIndex = getTailIndex(orderIndex);

                this.bindingSets = bindingSet;

                this.chunk = chunk;

                this.unsyncBuffer = unsyncBuffer;

            }

            /**
             * @return <code>true</code> iff NO elements in the chunk (as read
             *         from the access path by the caller) were accepted when
             *         combined with the {@link #bindingSets} from the source
             *         {@link JoinTask}.
             * 
             * @throws BufferClosedException
             *             if there is an attempt to output a chunk of
             *             {@link IBindingSet}s or {@link ISolution}s and the
             *             output buffer is an {@link IBlockingBuffer} (true for
             *             all join dimensions exception the lastJoin and also
             *             true for query on the lastJoin) and that
             *             {@link IBlockingBuffer} has been closed.
             */
            public Boolean call() throws Exception {

                try {

//                    ChunkTrace.chunk(orderIndex, chunk);

                    boolean nothingAccepted = true;

                    // Use caller's or obtain our own as necessary.
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer = (this.unsyncBuffer == null) ? threadLocalBufferFactory
                            .get()
                            : this.unsyncBuffer;

                    for (Object e : chunk) {

                        if (isDone())
                            return nothingAccepted;

                        // naccepted for the current element (trace only).
                        int naccepted = 0;

                        stats.elementCount.increment();

                        for (IBindingSet bset : bindingSets) {

                            /*
                             * Clone the binding set since it is tested for each
                             * element visited.
                             */
                            bset = bset.clone();

                            // propagate bindings from the visited element.
                            if (context.bind(right, constraints, e, bset)) {

                                // optionally strip off unnecessary variables.
                                bset = variablesToKeep == null ? bset : bset
                                        .copy(variablesToKeep);

                                // Accept this binding set.
                                unsyncBuffer.add(bset);

                                naccepted++;

                                nothingAccepted = false;

                            }

                        }

                        if (log.isDebugEnabled())
                            log.debug("Accepted element for " + naccepted
                                    + " of " + bindingSets.length
                                    + " possible bindingSet combinations: "
                                    + e.toString() + ", joinOp=" + joinOp);
                    }

                    // if something is accepted in the chunk return true.
                    return nothingAccepted ? Boolean.FALSE : Boolean.TRUE;

                } catch (Throwable t) {

                    halt(t);

                    throw new RuntimeException(t);

                }

            }

        }// class ChunkTask

        /**
         * Concrete implementation with hooks to halt a join.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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
                 * Wrap the buffer provider to the constructor with a thread
                 * local buffer.
                 */

                return new UnsyncLocalOutputBuffer<IBindingSet>(stats, joinOp
                        .getChunkCapacity(), sink);

            }

            @Override
            protected void halted() {

                JoinTask.this.halted();

            }

        } // class TLBFactory

    }// class JoinTask

    /**
     * This is a shard wise operator.
     */
    @Override
    public BOpEvaluationContext getEvaluationContext() {
        
        return BOpEvaluationContext.SHARDED;
        
    }

}
