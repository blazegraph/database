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

import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStarJoin;
import com.bigdata.relation.rule.IStarJoin.IStarConstraint;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.pipeline.DistributedJoinTask;
import com.bigdata.relation.rule.eval.pipeline.JoinMasterTask;
import com.bigdata.relation.rule.eval.pipeline.JoinStats;
import com.bigdata.relation.rule.eval.pipeline.UnsyncLocalOutputBuffer;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * Pipelined join operator. The pipeline join accepts chunks of binding sets
 * from its left operand, combines each binding set in turn with the right
 * operand to produce an "asBound" predicate, and then executes a nested indexed
 * subquery against that asBound predicate, writing out a new binding set for
 * each element returned by the asBound predicate which satisfies the join
 * constraint.
 * <p>
 * Note: In order to support pipelining, query plans need to be arranged in a
 * "left-deep" manner and there may not be intervening operators between the
 * pipeline join operator and the {@link IPredicate} on which it will read.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PipelineJoin2 extends AbstractPipelineOp<IBindingSet> implements
        BindingSetPipelineOp {

    static protected final Logger log = Logger.getLogger(PipelineJoin2.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @todo maxParallel, variablesToKeep (or null for all), join constraint (vs
     *       rule constraint), operatorId, optional flag and goto operatorId for
     *       routing failed joins.
     */
    public interface Annotations extends AbstractPipelineOp.Annotations {
        
    }
    
    /**
     * @param left
     *            The left operand, which must be an {@link IBindingSet}
     *            pipeline operator, such as another {@link PipelineJoin2}.
     * @param right
     *            The right operand, which must be an {@link IPredicate}.
     * 
     * @param annotations
     */
    protected PipelineJoin2(final BindingSetPipelineOp left,
            final IPredicate<?> right, final Map<String, Object> annotations) {

        super(new BOp[] { left, right }, annotations);

        if (left == null)
            throw new IllegalArgumentException();

        if (right == null)
            throw new IllegalArgumentException();

    }

    protected BindingSetPipelineOp left() {

        return (BindingSetPipelineOp) args[0];

    }

    protected IPredicate<?> right() {

        return (IPredicate<?>) args[1];

    }

    /**
     * @todo Declare annotations for joins, including an {@link IConstraint} and
     *       whether or not the join is optional. There will need to be an
     *       interface for join operators where we can declare this stuff.
     *       <p>
     *       An operator-at-a-time or mega-chunk join operator might have
     *       additional annotations.
     */
    protected IConstraint constraint() {

        throw new UnsupportedOperationException();

    }

    protected boolean isOptional() {

        throw new UnsupportedOperationException();

    }

    /**
     * @todo variablesToKeep by annotation. when not specified, keep all.
     */
    protected IVariable[] variablesToKeep() {

        throw new UnsupportedOperationException();

    }

    /**
     * {@inheritDoc}
     * 
     * @todo we may need to pass in the partitionId here (and -1 iff it is
     *       scaleup) so we can apply it to the predicate when we formulate the
     *       access path. In fact, it would have to be passed all the way down
     *       to where we actually execute the {@link IPredicate} since there
     *       could be intermediate operators.
     */
    public Future<Void> eval(final IBigdataFederation<?> fed,
            final IJoinNexus joinNexus,
            final IBlockingBuffer<IBindingSet[]> sink) {

        final FutureTask<Void> ft = new FutureTask<Void>(new JoinTask(fed,
                this, joinNexus, sink));

        joinNexus.getIndexManager().getExecutorService().execute(ft);

        return ft;

    }

    /**
     * Pipeline join impl.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class JoinTask implements Callable<Void> {

        /**
         * The federation reference is passed along when we evaluate the
         * {@link #left} operand.
         */
        final protected IBigdataFederation<?> fed;
        
        /**
         * The join that is being executed.
         */
        final protected PipelineJoin2 joinOp;

        /**
         * The constraint (if any) specified for the join operator.
         */
        final IConstraint constraint;

        /**
         * True iff the {@link #right} operand is an optional pattern (aka if
         * this is a SPARQL style left join).
         */
        final boolean optional;
        
        /**
         * The variables to be retained by the join operator. Variables not
         * appearing in this list will be stripped before writing out the
         * binding set onto the {@link #sink}.
         */
        final IVariable<?>[] variablesToKeep;

        /**
         * The source for the binding sets.
         */
        final BindingSetPipelineOp left;

        /**
         * The source for the elements to be joined.
         */
        final IPredicate<?> right;

        /**
         * The relation associated with the {@link #right} operand.
         */
        final IRelation<?> relation;
        
        /**
         * The partition identifier -or- <code>-1</code> if we are not reading
         * on an index partition.
         */
        final int partitionId;
        
        /**
         * The {@link IJoinNexus} for the local {@link IIndexManager}, which
         * will be the live {@link IJournal}. This {@link IJoinNexus} MUST have
         * access to the local index objects, which means that class MUST be run
         * inside of the {@link ConcurrencyManager}.
         */
        final protected IJoinNexus joinNexus;

        /**
         * Volatile flag is set <code>true</code> if the {@link JoinTask}
         * (including any tasks executing on its behalf) should halt. This flag
         * is monitored by the {@link BindingSetConsumerTask}, the
         * {@link AccessPathTask}, and the {@link ChunkTask}. It is set by any
         * of those tasks if they are interrupted or error out.
         * 
         * @todo review handling of this flag. Should an exception always be
         *       thrown if the flag is set wrapping the {@link #firstCause}? Are
         *       there any cases where the behavior should be different? If not,
         *       then replace tests with halt() and encapsulate the logic in
         *       that method.
         */
        volatile protected boolean halt = false;

        /**
         * Set by {@link BindingSetConsumerTask}, {@link AccessPathTask}, and
         * {@link ChunkTask} if they throw an error. Tasks are required to use
         * an {@link AtomicReference#compareAndSet(Object, Object)} and must
         * specify <code>null</code> as the expected value. This ensures that
         * only the first cause is recorded by this field.
         */
        final protected AtomicReference<Throwable> firstCause = new AtomicReference<Throwable>(
                null);

        /**
         * Indicate that join processing should halt. This method is written
         * defensively and will not throw anything.
         * 
         * @param cause
         *            The cause.
         */
        protected void halt(final Throwable cause) {

            halt = true;

            final boolean isFirstCause = firstCause.compareAndSet(
                    null/* expect */, cause);

            if (log.isEnabledFor(Level.WARN))

                try {

                    if (!InnerCause.isInnerCause(cause,
                            InterruptedException.class)
                            && !InnerCause.isInnerCause(cause,
                                    CancellationException.class)
                            && !InnerCause.isInnerCause(cause,
                                    ClosedByInterruptException.class)
                            && !InnerCause.isInnerCause(cause,
                                    RejectedExecutionException.class)
                            && !InnerCause.isInnerCause(cause,
                                    BufferClosedException.class)) {

                        /*
                         * This logs all unexpected causes, not just the first
                         * one to be reported for this join task.
                         * 
                         * Note: The master will log the firstCause that it
                         * receives as an error.
                         */

                        log.warn("joinOp=" + joinOp + ", isFirstCause="
                                + isFirstCause + " : "
                                + cause.getLocalizedMessage(), cause);

                    }

                } catch (Throwable ex) {

                    // error in logging system - ignore.

                }

        }

        /**
         * The statistics for this {@link JoinTask}.
         */
        final JoinStats stats;

        /**
         * A factory pattern for per-thread objects whose life cycle is tied to
         * some container. For example, there may be an instance of this pool
         * for a {@link JoinTask} or an {@link AbstractBTree}. The pool can be
         * torn down when the container is torn down, which prevents its
         * thread-local references from escaping.
         * 
         * @author thompsonbry@users.sourceforge.net
         * @param <T>
         *            The generic type of the thread-local object.
         * 
         * @todo There should be two implementations of a common interface or
         *       abstract base class: one based on a private
         *       {@link ConcurrentHashMap} and the other on striped locks. The
         *       advantage of the {@link ConcurrentHashMap} is approximately 3x
         *       higher concurrency. The advantage of striped locks is that you
         *       can directly manage the #of buffers when when the threads using
         *       those buffers is unbounded. However, doing so could lead to
         *       deadlock since two threads can be hashed onto the same buffer
         *       object.
         */
        abstract public class ThreadLocalFactory<T extends IBuffer<E>, E> {

            /**
             * The thread-local queues.
             */
            private final ConcurrentHashMap<Thread, T> map;

            /**
             * A list of all objects visible to the caller. This is used to
             * ensure that any objects allocated by the factory are visited.
             * 
             * <p>
             * Note: Since the collection is not thread-safe, synchronization is
             * required when adding to the collection and when visiting the
             * elements of the collection.
             */
            private final LinkedList<T> list = new LinkedList<T>();

            protected ThreadLocalFactory() {

                this(16/* initialCapacity */, .75f/* loadFactor */, 16/* concurrencyLevel */);

            }

            protected ThreadLocalFactory(final int initialCapacity,
                    final float loadFactor, final int concurrencyLevel) {

                map = new ConcurrentHashMap<Thread, T>(initialCapacity,
                        loadFactor, concurrencyLevel);

            }

            /**
             * Return the #of thread-local objects.
             */
            final public int size() {

                return map.size();

            }

            /**
             * Add the element to the thread-local buffer.
             * 
             * @param e
             *            An element.
             * 
             * @throws IllegalStateException
             *             if the factory is asynchronously closed.
             */
            public void add(final E e) {

                get().add(e);

            }

            /**
             * Return a thread-local buffer
             * 
             * @return The thread-local buffer.
             * 
             * @throws RuntimeException
             *             if the join is halted.
             */
            final private T get() {
                final Thread t = Thread.currentThread();
                T tmp = map.get(t);
                if (tmp == null) {
                    if (map.put(t, tmp = initialValue()) != null) {
                        /*
                         * Note: Since the key is the thread it is not possible
                         * for there to be a concurrent put of an entry under
                         * the same key so we do not have to use putIfAbsent().
                         */
                        throw new AssertionError();
                    }
                    // Add to list.
                    synchronized (list) {
                        list.add(tmp);
                    }
                }
                if (halt)
                    throw new RuntimeException(firstCause.get());
                return tmp;
            }

            /**
             * Flush each of the unsynchronized buffers onto their backing
             * synchronized buffer.
             * 
             * @throws RuntimeException
             *             if the join is halted.
             */
            public void flush() {
                synchronized (list) {
                    int n = 0;
                    long m = 0L;
                    for (T b : list) {
                        if (halt)
                            throw new RuntimeException(firstCause.get());
                        // #of elements to be flushed.
                        final int size = b.size();
                        // flush, returning total #of elements written onto this
                        // buffer.
                        final long counter = b.flush();
                        m += counter;
                        if (log.isDebugEnabled())
                            log.debug("Flushed buffer: size=" + size
                                    + ", counter=" + counter);
                    }
                    if (log.isInfoEnabled())
                        log.info("Flushed " + n
                                + " unsynchronized buffers totalling " + m
                                + " elements");
                }
            }

            /**
             * Reset each of the synchronized buffers, discarding their buffered
             * writes.
             * <p>
             * Note: This method is used during error processing, therefore it
             * DOES NOT check {@link JoinTask#halt}.
             */
            public void reset() {
                synchronized (list) {
                    int n = 0;
                    for (T b : list) {
                        // #of elements in the buffer before reset().
                        final int size = b.size();
                        // reset the buffer.
                        b.reset();
                        if (log.isDebugEnabled())
                            log.debug("Reset buffer: size=" + size);
                    }
                    if (log.isInfoEnabled())
                        log.info("Reset " + n + " unsynchronized buffers");
                }
            }

            // /**
            // * Reset the per-{@link Thread} unsynchronized output buffers
            // (used as
            // * part of error handling for the {@link JoinTask}).
            // */
            // final protected void resetUnsyncBuffers() throws Exception {
            //
            // final int n = threadLocalBufferFactory.reset();
            // .close(new
            // Visitor<AbstractUnsynchronizedArrayBuffer<IBindingSet>>() {
            //
            // @Override
            // public void meet(
            // final AbstractUnsynchronizedArrayBuffer<IBindingSet> b)
            // throws Exception {
            //
            //
            // }

            /**
             * Create and return a new object.
             */
            abstract protected T initialValue();

        }

        final private ThreadLocalFactory<AbstractUnsynchronizedArrayBuffer<IBindingSet>, IBindingSet> threadLocalBufferFactory = new ThreadLocalFactory<AbstractUnsynchronizedArrayBuffer<IBindingSet>, IBindingSet>() {

            @Override
            protected AbstractUnsynchronizedArrayBuffer<IBindingSet> initialValue() {

                // new buffer created by the concrete JoinClass impl.
                return newUnsyncOutputBuffer();

            }
        };

        public String toString() {

            return getClass().getName() + "{ joinOp=" + joinOp + "}";

        }

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
        volatile private IAsynchronousIterator<IBindingSet[]> source;

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
        final protected IBlockingBuffer<IBindingSet[]> sink;

        /**
         * Instances of this class MUST be created in the appropriate execution
         * context of the target {@link DataService} so that the federation and
         * the joinNexus references are both correct and so that it has access
         * to the local index object for the specified index partition.
         * 
         * @param joinOp
         * @param joinNexus
         * @param sink
         *            The sink on which the {@link IBindingSet} chunks are
         *            written.
         * @param requiredVars
         */
        public JoinTask(//
                final IBigdataFederation<?> fed,//
                final PipelineJoin2 joinOp,//
                final IJoinNexus joinNexus,//
                final IBlockingBuffer<IBindingSet[]> sink
                ) {

            if (joinOp == null)
                throw new IllegalArgumentException();
            if (joinNexus == null)
                throw new IllegalArgumentException();
            if (sink == null)
                throw new IllegalArgumentException();

            this.fed = fed;
            this.joinOp = joinOp;
            this.left = joinOp.left();
            this.right = joinOp.right();
            this.constraint = joinOp.constraint();
            this.optional = joinOp.isOptional();
            this.variablesToKeep = joinOp.variablesToKeep();
            this.joinNexus = joinNexus;
            this.relation = joinNexus.getTailRelationView(right);
            this.partitionId = -1; /*
                                    * FIXME The partition identifier probably
                                    * needs to be passed in at evaluation time
                                    */
            this.stats = new JoinStats(partitionId, -1/*
                                                       * @todo orderIndex does
                                                       * not exist. use joinOpId
                                                       * instead?
                                                       */);

            if (log.isDebugEnabled())
                log.debug("joinOp=" + joinOp);

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
                 * FIXME The historical pipeline join propagated evaluation from
                 * left to right. This needs to be revisited in some great depth
                 * now that we are dealing with operator trees. Operator trees
                 * lend themselves naturally to top-down evaluation. However,
                 * top-down evaluation might not be compatible with distributed
                 * evaluation.
                 * 
                 * In fact, the mental model that I had was that the query plan
                 * was broadcast to all nodes and that join tasks were created
                 * and applied as binding sets appeared for the corresponding
                 * joinOp. This means a complete rethink of the starting and
                 * terminating conditions and the logic surrounding the guts of
                 * the join.
                 * 
                 * For a given IBindingSet[] chunk we will continue to do
                 * exactly what we have been doing, but the surrounding logic
                 * needs to be replaced. Starting a "join" (at least for
                 * scale-out) needs to merely register a task factory that will
                 * handle binding sets if they arrive.
                 */
                final Future<Void> sourceFuture = left.eval(fed, joinNexus,
                        sink);
                
                /*
                 * Consume bindingSet chunks from the source JoinTask(s).
                 */
                consumeSources();

                /*
                 * Flush and close output buffers and wait for all sink
                 * JoinTasks to complete.
                 */

                // flush the unsync buffers.
                // flushUnsyncBuffers();
                threadLocalBufferFactory.flush();

                // flush the sync buffer and await the sink JoinTasks
                flushAndCloseBuffersAndAwaitSinks();

                if (log.isDebugEnabled())
                    log.debug("JoinTask done: joinOp=" + joinOp + ", halt="
                            + halt + "firstCause=" + firstCause.get());
                if (halt)
                    throw new RuntimeException(firstCause.get());

                return null;

            } catch (Throwable t) {

                try {
                    logCallError(t);
                } catch (Throwable t2) {
                    log.error(t2.getLocalizedMessage(), t2);
                }

                /*
                 * This is used for processing errors and also if this task is
                 * interrupted (because a SLICE has been satisfied).
                 * 
                 * @todo For a SLICE, consider that the query solution buffer
                 * proxy could return the #of solutions added so far so that we
                 * can halt each join task on the last join dimension in a
                 * relatively timely manner producing no more than one chunk too
                 * many (actually, it might not be that timely since some index
                 * partitions might not produce any solutions; this suggests
                 * that the master might need a fatter API than a Future for the
                 * JoinTask so that it can directly notify the JoinTasks for the
                 * first predicate and they can propagate that notice downstream
                 * to their sinks). This will be an issue when fanOut GT ONE.
                 */

                halt(t);

                // reset the unsync buffers.
                try {
                    // resetUnsyncBuffers();
                    threadLocalBufferFactory.reset();
                } catch (Throwable t2) {
                    log.error(t2.getLocalizedMessage(), t2);
                }

                // reset the sync buffer and cancel the sink JoinTasks.
                try {
                    cancelSinks();
                } catch (Throwable t2) {
                    log.error(t2.getLocalizedMessage(), t2);
                }

                // report join stats _before_ we close our source(s).
                try {
                    reportOnce();
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

            } finally {

                // report join stats iff they have not already been reported.
                reportOnce();

            }

        }

        /**
         * Method is used to log the primary exception thrown by {@link #call()}
         * . The default implementation does nothing and the exception will be
         * logged by the {@link JoinMasterTask}. However, this method is
         * overridden by {@link DistributedJoinTask} so that the exception can
         * be logged on the host and {@link DataService} where it originates.
         * This appears to be necessary in order to trace back the cause of an
         * exception which can otherwise be obscured (or even lost?) in a deeply
         * nested RMI stack trace.
         * 
         * @param o
         * @param t
         */
        protected void logCallError(Throwable t) {

        }

        /**
         * Method reports {@link JoinStats} to the {@link JoinMasterTask}, but
         * only if they have not already been reported. This "report once"
         * constraint is used to make it safe to invoke during error handling
         * before actions which could cause the source {@link JoinTask}s (and
         * hence the {@link JoinMasterTask}) to terminate.
         */
        protected void reportOnce() {

            if (didReport.compareAndSet(false/* expect */, true/* update */)) {

//                try {
//
////                     @todo report statistics to the master.
//                     masterProxy.report(stats);
//
//                } catch (IOException ex) {
//
//                    log.warn("Could not report statistics to the master", ex);
//
//                }

            }

        }

        private final AtomicBoolean didReport = new AtomicBoolean(false);

        /**
         * Consume {@link IBindingSet} chunks from source(s). The first join
         * dimension always has a single source - the initialBindingSet
         * established by the {@link JoinMasterTask}. Downstream join dimensions
         * read from {@link IAsynchronousIterator}(s) from the upstream join
         * dimension. When the {@link IIndexManager} allows key-range
         * partitions, then the fan-in for the sources may be larger than one as
         * there will be one {@link JoinTask} for each index partition touched
         * by each join dimension.
         * 
         * @throws Exception
         * @throws BufferClosedException
         *             if there is an attempt to output a chunk of
         *             {@link IBindingSet}s or {@link ISolution}s and the output
         *             buffer is an {@link IBlockingBuffer} (true for all join
         *             dimensions exception the lastJoin and also true for query
         *             on the lastJoin) and that {@link IBlockingBuffer} has
         *             been closed.
         */
        protected void consumeSources() throws Exception {

            if (log.isInfoEnabled())
                log.info(toString());

            IBindingSet[] chunk;

            /*
             * The maximum parallelism with which the {@link JoinTask} will
             * consume the source {@link IBindingSet}s.
             * 
             * Note: When ZERO (0), everything will run in the caller's {@link
             * Thread}. When GT ZERO (0), tasks will run on an {@link
             * ExecutorService} with the specified maximum parallelism.
             * 
             * Note: even when maxParallel is zero there will be one thread per
             * join dimension. For many queries that may be just fine.
             * 
             * FIXME parallel execution requires some thread-local
             * unsynchronized buffers -- see my notes elsewhere in this class
             * for what has to be done to support this (actually, it all appears
             * to work just fine).
             * 
             * @todo raise this into an annotation or simply change over to a
             * fork join pool with a queue for access path reads.
             */
            final int maxParallel = 0;
            // final int maxParallel = joinNexus.getMaxParallelSubqueries();
            // final int maxParallel = 10;

            /*
             * Note: There is no reason for parallelism in the first join
             * dimension as there will be only a single source bindingSet and
             * hence a single AccessPathTask so the Executor is just overhead.
             * 
             * @todo this will not be true when we support binding set joins as
             * the input could be a stream of binding sets (basically, when the
             * first join dimension is a subrule, it can have lots of access
             * path tasks).
             */
            final Executor service;
            if (/* orderIndex > 0 && */maxParallel > 0) {

                // the sharedService.
                final ExecutorService sharedService = joinNexus
                        .getIndexManager().getExecutorService();

                service = new LatchedExecutor(sharedService, maxParallel);

            } else {
                // run in the caller's thread.
                service = null;
            }

            while (!halt && (chunk = nextChunk()) != null) {

                /*
                 * consume chunks until done (using caller's thread to consume
                 * and service to run subtasks).
                 */
                new BindingSetConsumerTask(service, chunk).call();

                if (halt)
                    throw new RuntimeException(firstCause.get());

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
         * A method used by the {@link #threadLocalBufferFactory} to create new
         * output buffer as required. The output buffer will be used to
         * aggregate {@link IBindingSet}s generated by this {@link JoinTask}.
         */
        final protected AbstractUnsynchronizedArrayBuffer<IBindingSet> newUnsyncOutputBuffer() {

            /*
             * The index is not key-range partitioned. This means that there is
             * ONE (1) JoinTask per predicate in the rule. The bindingSets are
             * aggregated into chunks by this buffer. On overflow, the buffer
             * writes onto a BlockingBuffer. The sink JoinTask reads from that
             * BlockingBuffer's iterator.
             */

            // flushes to the syncBuffer.
            return new UnsyncLocalOutputBuffer<IBindingSet>(stats, joinNexus
                    .getChunkCapacity(), sink);

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

            if (halt)
                throw new RuntimeException(firstCause.get());

            /*
             * Close the thread-safe output buffer. For any JOIN except the
             * last, this buffer will be the source for one or more sink
             * JoinTasks for the next join dimension. Once this buffer is
             * closed, the asynchronous iterator draining the buffer will
             * eventually report that there is nothing left for it to process.
             * 
             * Note: This is a BlockingBuffer. BlockingBuffer#flush() is a NOP.
             */

            sink.close();

            assert !sink.isOpen();

            if (halt)
                throw new RuntimeException(firstCause.get());

            try {

                sinkFuture.get();

            } catch (Throwable t) {

                halt(t);

            }

        }

        /**
         * Cancel sink {@link JoinTask}(s).
         */
        protected void cancelSinks() {

            if (log.isDebugEnabled())
                log.debug("joinOp=" + joinOp);

            sink.reset();

            sinkFuture.cancel(true/* mayInterruptIfRunning */);

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

                if (halt)
                    throw new RuntimeException(firstCause.get());

                // note: uses timeout to avoid blocking w/o testing [halt].
                if (source.hasNext(10, TimeUnit.MILLISECONDS)) {

                    // read the chunk.
                    final IBindingSet[] chunk = source.next();

                    stats.bindingSetChunksIn++;
                    stats.bindingSetsIn += chunk.length;

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

                //                final int tailIndex = getTailIndex(orderIndex);

                final Map<IPredicate<?>, Collection<IBindingSet>> map = new LinkedHashMap<IPredicate<?>, Collection<IBindingSet>>(
                        chunk.length);

                for (IBindingSet bindingSet : chunk) {

                    if (halt)
                        throw new RuntimeException(firstCause.get());

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
                        stats.accessPathDups++;

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

                    if (halt)
                        throw new RuntimeException(firstCause.get());

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
                if (tasks[0].accessPath instanceof AbstractAccessPath<?>) {

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

                        if (halt)
                            throw new RuntimeException(firstCause.get());

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
                        if (!halt)
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
             * @todo layered access paths do not expose a fromKey. This
             *       information is always available from the
             *       {@link SPOKeyOrder} and that method will be raised into the
             *       {@link IKeyOrder}. Unfortunately, for RDF we also need to
             *       know if triples or quads are being used, which is a
             *       property on the container or the relation.
             */
            protected byte[] getFromKey() {

                return ((AbstractAccessPath<?>) accessPath).getFromKey();

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
            public boolean equals(final AccessPathTask o) {

                return accessPath.getPredicate().equals(
                        o.accessPath.getPredicate());

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

                this.accessPath = joinNexus.getTailAccessPath(relation,
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

                if (halt)
                    throw new RuntimeException(firstCause.get());

                stats.accessPathCount++;

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

                    /*
                     * @todo In order to run the chunks on a thread pool, pass
                     * in [null] for the unsyncBuffer and each chunk will get
                     * its own buffer.
                     */
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer = threadLocalBufferFactory
                            .get();

                    while (itr.hasNext()) {

                        final Object[] chunk = itr.nextChunk();

                        stats.chunkCount++;

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

                            unsyncBuffer.add(bs);

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

                            stats.chunkCount++;

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
                        stats.elementCount += numElements;

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

                                // we did not find any matches to this
                                // constraint
                                // that is ok, as long it's optional
                                if (constraint.isOptional() == false) {

                                    constraintFailed = true;

                                    break;

                                }

                            } else {

                                // set the old solutions to the new solutions,
                                // and
                                // move on to the next constraint
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

//            /**
//             * The index of the predicate for the access path that is being
//             * consumed.
//             */
//            private final int tailIndex;

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

                        if (halt)
                            return nothingAccepted;

                        // naccepted for the current element (trace only).
                        int naccepted = 0;

                        stats.elementCount++;

                        for (IBindingSet bset : bindingSets) {

                            /*
                             * Clone the binding set since it is tested for each
                             * element visited.
                             */
                            bset = bset.clone();

                            // propagate bindings from the visited element.
                            if (joinNexus.bind(right, constraint, e, bset)) {

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

    }// class JoinTask

}
