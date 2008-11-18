package com.bigdata.relation.rule.eval.pipeline;

import java.io.IOException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ChunkTrace;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Consumes {@link IBindingSet} chunks from the previous join dimension.
 * <p>
 * Note: Instances of this class MUST be created on the {@link IDataService}
 * that is host to the index partition on the task will read and they MUST
 * run inside of an {@link AbstractTask} on the {@link ConcurrencyManager}
 * in order to have access to the local index object for the index
 * partition.
 * <p>
 * This class is NOT serializable.
 * <p>
 * For a rule with 2 predicates, there will be two {@link JoinTask}s. The
 * {@link #orderIndex} is ZERO (0) for the first {@link JoinTask} and ONE
 * (1) for the second {@link JoinTask}. The first {@link JoinTask} will
 * have a single initialBinding from the {@link JoinMasterTask} and will
 * read on the {@link IAccessPath} for the first {@link IPredicate} in the
 * evaluation {@link #order}. The second {@link JoinTask} will read chunks
 * of {@link IBindingSet}s containing partial solutions from the first
 * {@link JoinTask} and will obtain and read on an {@link IAccessPath} for
 * the second predicate in the evaluation order for every partial solution.
 * Since there are only two {@link IPredicate}s in the {@link IRule}, the
 * second and last {@link JoinTask} will write on the {@link ISolution}
 * buffer obtained from {@link JoinMasterTask#getSolutionBuffer()}. Each
 * {@link JoinTask} will report its {@link JoinStats} to the master, which
 * aggregates those statistics.
 * <p>
 * Note: {@link ITx#UNISOLATED} requests will deadlock if the same query
 * uses the same access path for two predicates! This is because the first
 * such join dimension in the evaluation order will obtain an exclusive lock
 * on an index partition making it impossible for another {@link JoinTask}
 * to obtain an exclusive lock on the same index partition. This is not a
 * problem if you are using read-consistent timestamps!
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Allow the access paths to be consumed in parallel. this would let
 *       us use more threads for join dimensions that had to test more
 *       source binding sets.
 *       <p>
 *       Parallel {@link AccessPathTask} processing is useful when each
 *       {@link AccessPathTask} consumes only a small chunk and there are a
 *       large #of source binding sets to be processed. In this case,
 *       parallelism reduces the overall latency by allowing threads to
 *       progress as soon as the data can be materialized from the index.
 *       {@link AccessPathTask} parallelism is realized by submitting each
 *       {@link AccessPathTask} to a service imposing a parallelism limit on
 *       the shared {@link IIndexStore#getExecutorService()}. Since the
 *       {@link AccessPathTask}s are concurrent, each one requires its own
 *       {@link UnsynchronizedOutputBuffer} on which it will place any
 *       accepted {@link IBindingSet}s. Once an {@link AccessPathTask}
 *       completes, its buffer may be reused by the next
 *       {@link AccessPathTask} assigned to a worker thread (this reduces
 *       heap churn and allows us to assemble full chunks when each
 *       {@link IAccessPath} realizes only a few accepted
 *       {@link IBindingSet}s). For an {@link ExecutorService} with a
 *       parallelism limit of N, there are therefore N
 *       {@link UnsynchronizedOutputBuffer}s. Those buffers must be flushed
 *       when the {@link JoinTask} exhausts its source(s).
 *       <p>
 *       Parallel {@link ChunkTask} processing may be useful when an
 *       {@link AccessPathTask} will consume a large #of chunks. Since the
 *       {@link IAccessPath#iterator()} is NOT thread-safe, reads on the
 *       {@link IAccessPath} must be sequential, but the chunks read from
 *       the {@link IAccessPath} can be placed onto a queue and parallel
 *       {@link ChunkTask}s can drain that queue, consuming the chunks.
 *       This can help by reducing the latency to materialize any given
 *       chunk.
 *       <p>
 *       the required change is to make have a per-thread object
 *       {@link UnsynchronizedArrayBuffer} feeding a thread-safe
 *       {@link UnsyncDistributedOutputBuffer} (potentially via a queue)
 *       which maps each generated binding set across the index partition(s)
 *       for the sink {@link JoinTask}s.
 */
abstract public class JoinTask implements Callable<Void> {

    static protected final Logger log = Logger.getLogger(JoinTask.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    static final protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    static final protected boolean DEBUG = log.isDebugEnabled();

    /** The rule that is being evaluated. */
    final protected IRule rule;

    /**
     * The #of predicates in the tail of that rule.
     */
    final protected int tailCount;

    /**
     * The index partition on which this {@link JoinTask} is reading -or-
     * <code>-1</code> if the deployment does not support key-range
     * partitioned indices.
     */
    final protected int partitionId;

    /**
     * The tail index in the rule for the predicate on which we are reading
     * for this join dimension.
     */
    final protected int tailIndex;

    /**
     * The {@link IPredicate} on which we are reading for this join
     * dimension.
     */
    final protected IPredicate predicate;

    /**
     * The index into the evaluation {@link #order} for the predicate on
     * which we are reading for this join dimension.
     */
    final protected int orderIndex;

    /**
     * <code>true</code> iff this is the last join dimension in the
     * evaluation order.
     */
    final protected boolean lastJoin;

    /**
     * A proxy for the remote {@link JoinMasterTask}.
     */
    final protected IJoinMaster masterProxy;

    /**
     * The {@link IJoinNexus} for the local {@link IIndexManager}, which
     * will be the live {@link IJournal}. This {@link IJoinNexus} MUST have
     * access to the local index objects, which means that class MUST be run
     * inside of the {@link ConcurrencyManager}. The {@link #joinNexus} is
     * created from the {@link #joinNexusFactory} once the task begins to
     * execute.
     * 
     * @todo javadoc
     */
    protected IJoinNexus joinNexus;

    /**
     * Volatile flag is set <code>true</code> if the {@link JoinTask}
     * (including any tasks executing on its behalf) should halt. This flag
     * is monitored by the {@link BindingSetConsumerTask}, the
     * {@link AccessPathTask}, and the {@link ChunkTask}. It is set by any
     * of those tasks if they are interrupted or error out.
     * 
     * @todo review handling of this flag. Should an exception always be
     *       thrown if the flag is set wrapping the {@link #firstCause}?
     *       Are there any cases where the behavior should be different?
     *       If not, then replace tests with halt() and encapsulate the
     *       logic in that method.
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
     * Indicate that join processing should halt.  This method is written
     * defensively and will not throw anything.
     * 
     * @param cause
     *            The cause.
     */
    protected void halt(final Throwable cause) {

        halt = true;

        firstCause.compareAndSet(null/*expect*/, cause);

        if (INFO)

            try {

                if (!InnerCause.isInnerCause(cause, InterruptedException.class)
                        && !InnerCause.isInnerCause(cause,
                                CancellationException.class)
                        && !InnerCause.isInnerCause(cause,
                                ClosedByInterruptException.class)
                        && !InnerCause.isInnerCause(cause,
                                RejectedExecutionException.class)
                        && !InnerCause.isInnerCause(cause,
                                BufferClosedException.class)) {

                    log.info("" + cause, cause);

                }

            } catch (Throwable ex) {

                // error in logging system - ignore.

            }

    }

    /**
     * The evaluation order. {@link #orderIndex} is the index into this
     * array. The {@link #orderIndex} is zero (0) for the first join
     * dimension and is incremented by one for each subsequent join
     * dimension. The value at <code>order[orderIndex]</code> is the index
     * of the tail predicate that will be evaluated at a given
     * {@link #orderIndex}.
     */
    final int[] order;

    /**
     * The statistics for this {@link JoinTask}.
     */
    final JoinStats stats;

    /**
     * This list is used to accumulate the references to the per-{@link Thread}
     * unsynchronized output buffers. The list is processed by either
     * {@link #flushUnsyncBuffers()} or {@link #resetUnsyncBuffers()}
     * depending on whether the {@link JoinTask} completes successfully or
     * not.
     * 
     * FIXME Examine in the debugger to see whether we are accumulating a
     * bunch of {@link ThreadLocal}s with buffers per thread or if their
     * life cycle is in fact scoped to the {@link JoinTask}.
     * <p>
     * Note: The real danger is that old buffers might hang around with
     * partial results which would cause {@link IBindingSet}s from other
     * {@link JoinTask}s to be emitted by the current {@link JoinTask}.
     */
    final private List<AbstractUnsynchronizedArrayBuffer<IBindingSet>> unsyncBufferList = new LinkedList<AbstractUnsynchronizedArrayBuffer<IBindingSet>>();

    /**
     * A factory for the per-{@link Thread} buffers used to accumulate
     * chunks of output {@link IBindingSet}s across the
     * {@link AccessPathTask}s for this {@link JoinTask}.
     * <p>
     * Note: This is not static because access is required to
     * {@link JoinTask#newUnsyncOutputBuffer()}.
     */
    final protected ThreadLocal<AbstractUnsynchronizedArrayBuffer<IBindingSet>> threadLocalBufferFactory = new ThreadLocal<AbstractUnsynchronizedArrayBuffer<IBindingSet>>() {

        protected synchronized AbstractUnsynchronizedArrayBuffer<IBindingSet> initialValue() {

            // new buffer created by the concrete JoinClass impl.
            final AbstractUnsynchronizedArrayBuffer<IBindingSet> buffer = newUnsyncOutputBuffer();

            /*
             * Note: List#add() is safe since initialValue() is
             * synchronized.
             */

            unsyncBufferList.add(buffer);

            return buffer;

        }

    };

    /**
     * A method used by the {@link #threadLocalBufferFactory} to create new
     * output buffer as required. The output buffer will be used to
     * aggregate {@link IBindingSet}s generated by this {@link JoinTask}.
     * <p>
     * Note: A different implementation class must be used depending on
     * whether or not this is the last join dimension for the query (when it
     * is, then we write on the solution buffer) and whether or not the
     * target join index is key-range partitioned (when it is, each binding
     * set is mapped across the sink {@link JoinTask}(s)).
     */
    abstract protected AbstractUnsynchronizedArrayBuffer<IBindingSet> newUnsyncOutputBuffer();

    /**
     * The buffer on which the last predicate in the evaluation order will
     * write its {@link ISolution}s.
     * 
     * @return The buffer.
     * 
     * @throws IllegalStateException
     *             unless {@link #lastJoin} is <code>true</code>.
     */
    abstract protected IBuffer<ISolution[]> getSolutionBuffer();

    /**
     * Return the index of the tail predicate to be evaluated at the given
     * index in the evaluation order.
     * 
     * @param orderIndex
     *            The evaluation order index.
     * 
     * @return The tail index to be evaluated at that index in the
     *         evaluation order.
     */
    final protected int getTailIndex(final int orderIndex) {

        assert order != null;

        final int tailIndex = order[orderIndex];

        assert orderIndex >= 0 && orderIndex < tailCount : "orderIndex="
                + orderIndex + ", rule=" + rule;

        return tailIndex;

    }

    public String toString() {

        return getClass().getName() + "{ orderIndex=" + orderIndex
                + ", partitionId=" + partitionId + ", lastJoin=" + lastJoin
                + "}";

    }

    /**
     * Instances of this class MUST be created in the appropriate execution
     * context of the target {@link DataService} so that the federation and
     * the joinNexus references are both correct and so that it has access
     * to the local index object for the specified index partition.
     * 
     * @param concurrencyManager
     * @param indexName
     * @param rule
     * @param joinNexus
     * @param order
     * @param orderIndex
     * @param partitionId
     *            The index partition identifier and <code>-1</code> if
     *            the deployment does not support key-range partitioned
     *            indices.
     * @param master
     * 
     * @see JoinTaskFactoryTask
     */
    public JoinTask(final String indexName, final IRule rule,
            final IJoinNexus joinNexus, final int[] order,
            final int orderIndex, final int partitionId,
            final IJoinMaster master) {

        if (rule == null)
            throw new IllegalArgumentException();
        if (joinNexus == null)
            throw new IllegalArgumentException();
        final int tailCount = rule.getTailCount();
        if (order == null)
            throw new IllegalArgumentException();
        if (order.length != tailCount)
            throw new IllegalArgumentException();
        if (orderIndex < 0 || orderIndex >= tailCount)
            throw new IllegalArgumentException();
        if (master == null)
            throw new IllegalArgumentException();

        this.rule = rule;
        this.partitionId = partitionId;
        this.tailCount = tailCount;
        this.orderIndex = orderIndex;
        this.joinNexus = joinNexus;
        this.order = order; // note: assign before using getTailIndex()
        this.tailIndex = getTailIndex(orderIndex);
        this.lastJoin = ((orderIndex + 1) == tailCount);
        this.predicate = rule.getTail(tailIndex);
        this.stats = new JoinStats(partitionId, orderIndex);
        this.masterProxy = master;

        if (DEBUG)
            log.debug("orderIndex=" + orderIndex + ", partitionId="
                    + partitionId);

    }

    /**
     * Runs the {@link JoinTask}.
     * 
     * @return <code>null</code>.
     */
    public Void call() throws Exception {

        if (DEBUG)
            log.debug("orderIndex=" + orderIndex + ", partitionId="
                    + partitionId);

        try {

            /*
             * Consume bindingSet chunks from the source JoinTask(s).
             */
            consumeSources();

            /*
             * Flush and close output buffers and wait for all sink
             * JoinTasks to complete.
             */

            // flush the unsync buffers.
            flushUnsyncBuffers();

            // flush the sync buffer and await the sink JoinTasks
            flushAndCloseBuffersAndAwaitSinks();

            if (DEBUG)
                log.debug("JoinTask done: orderIndex=" + orderIndex
                        + ", partitionId=" + partitionId + ", halt=" + halt
                        + "firstCause=" + firstCause.get());
            if (halt)
                throw new RuntimeException(firstCause.get());

            return null;

        } catch (Throwable t) {

            /*
             * This is used for processing errors and also if this task is
             * interrupted (because a SLICE has been satisified).
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
            resetUnsyncBuffers();

            // reset the sync buffer and cancel the sink JoinTasks.
            cancelSinks();

            // report join stats _before_ we close our source(s).
            reportOnce();

            /*
             * Close source iterators, which will cause any source JoinTasks
             * that are still executing to throw a CancellationException
             * when the Future associated with the source iterator is
             * cancelled.
             */
            closeSources();

            throw new RuntimeException(t);

        } finally {

            // report join stats iff they have not already been reported.
            reportOnce();

        }

    }

    /**
     * Method reports {@link JoinStats} to the {@link JoinMasterTask}, but
     * only if they have not already been reported. This "report once"
     * constraint is used to make it safe to invoke during error handling
     * before actions which could cause the source {@link JoinTask}s (and
     * hence the {@link JoinMasterTask}) to terminate.
     */
    protected void reportOnce() {

        if (!didReport) {

            didReport = true;

            try {

                // report statistics to the master.
                masterProxy.report(stats);

            } catch (IOException ex) {

                log.warn("Could not report statistics to the master", ex);

            }

        }

    }

    private boolean didReport = false;

    /**
     * Consume {@link IBindingSet} chunks from source(s). The first join
     * dimension always has a single source - the initialBindingSet
     * established by the {@link JoinMasterTask}. Downstream join
     * dimensions read from {@link IAsynchronousIterator}(s) from the
     * upstream join dimension. When the {@link IIndexManager} allows
     * key-range partitions, then the fan-in for the sources may be larger
     * than one as there will be one {@link JoinTask} for each index
     * partition touched by each join dimension.
     * 
     * @throws Exception
     * @throws BufferClosedException
     *             if there is an attempt to output a chunk of
     *             {@link IBindingSet}s or {@link ISolution}s and the
     *             output buffer is an {@link IBlockingBuffer} (true for all
     *             join dimensions exception the lastJoin and also true for
     *             query on the lastJoin) and that {@link IBlockingBuffer}
     *             has been closed.
     */
    protected void consumeSources() throws Exception {

        if (INFO)
            log.info(toString());

        /*
         * The maximum parallelism with which the {@link JoinTask} will
         * consume the source {@link IBindingSet}s.
         * 
         * Note: When ZERO (0), everything will run in the caller's
         * {@link Thread}. When GT ZERO (0), tasks will run on an
         * {@link ExecutorService} with the specified maximum parallelism.
         * 
         * Note: even when maxParallel is zero there will be one thread per
         * join dimension. For many queries that may be just fine.
         */
        final int maxParallel;
        maxParallel = joinNexus.getMaxParallelSubqueries();
        //            maxParallel = 10;

        /*
         * Note: There is little reason for parallelism in the first join
         * dimension as there will be only a single source bindingSet so the
         * thread pool is just overhead.
         */
        if (orderIndex > 0 && maxParallel > 0) {

            /*
             * Setup parallelism limitedService that will be used to run the
             * access path tasks. Note that this is layered over the shared
             * ExecutorService.
             * 
             * FIXME The parallelism limited executor service is clearly
             * broken. When enabled here it will fail to progress. However
             * the code runs just fine if you use a standard executor
             * service in its place.
             */

            //                // the sharedService.
            //                final ExecutorService sharedService = joinNexus
            //                        .getIndexManager().getExecutorService();
            //
            //                final ParallelismLimitedExecutorService limitedService = new ParallelismLimitedExecutorService(//
            //                        sharedService, //
            //                        maxParallel, //
            //                        joinNexus.getChunkCapacity() * 2// workQueueCapacity
            //                );
            final ExecutorService limitedService = Executors
                    .newFixedThreadPool(maxParallel, DaemonThreadFactory
                            .defaultThreadFactory());

            try {

                /*
                 * consume chunks until done (using caller's thread to
                 * consume and service to run subtasks).
                 */
                new BindingSetConsumerTask(limitedService).call();

                // normal shutdown.
                limitedService.shutdown();

                // wait for AccessPathTasks to complete.
                limitedService.awaitTermination(Long.MAX_VALUE,
                        TimeUnit.SECONDS);

                //                    if (limitedService.getErrorCount() > 0) {
                //
                //                        // at least one AccessPathTask failed.
                //
                //                        if (INFO)
                //                            log.info("Task failure(s): " + limitedService);
                //
                //                        throw new RuntimeException(
                //                                "Join failure(s): errorCount="
                //                                        + limitedService.getErrorCount());
                //
                //                    }
                if (halt)
                    throw new RuntimeException(firstCause.get());

            } finally {

                if (!limitedService.isTerminated()) {

                    // shutdown the parallelism limitedService.
                    limitedService.shutdownNow();

                }

            }

        } else {

            /*
             * consume chunks until done using the caller's thread and run
             * subtasks in the caller's thread as well.
             */
            new BindingSetConsumerTask(null/* noService */).call();

        }

    }

    /**
     * Close any source {@link IAsynchronousIterator}(s). This method is
     * invoked when a {@link JoinTask} fails.
     */
    abstract void closeSources();

    /**
     * Flush the per-{@link Thread} unsynchronized output buffers (they
     * write onto the thread-safe output buffer).
     */
    protected void flushUnsyncBuffers() {

        if (INFO)
            log.info("Flushing " + unsyncBufferList.size()
                    + " unsynchronized buffers");

        for (AbstractUnsynchronizedArrayBuffer<IBindingSet> b : unsyncBufferList) {

            // unless halted
            if (halt)
                throw new RuntimeException(firstCause.get());

            // #of elements to be flushed.
            final int size = b.size();

            // flush, returning total #of elements written onto this buffer.
            final long counter = b.flush();

            if (DEBUG)
                log.debug("Flushed buffer: size=" + size + ", counter="
                        + counter);

        }

    }

    /**
     * Reset the per-{@link Thread} unsynchronized output buffers (used as
     * part of error handling for the {@link JoinTask}).
     */
    protected void resetUnsyncBuffers() {

        if (INFO)
            log.info("Resetting " + unsyncBufferList.size()
                    + " unsynchronized buffers");

        for (AbstractUnsynchronizedArrayBuffer<IBindingSet> b : unsyncBufferList) {

            // #of elements in the buffer before reset().
            final int size = b.size();

            // flush the buffer.
            b.reset();

            if (DEBUG)
                log.debug("Reset buffer: size=" + size);

        }

    }

    /**
     * Flush and close all output buffers and await sink {@link JoinTask}(s).
     * <p>
     * Note: You MUST close the {@link BlockingBuffer} from which each sink
     * reads <em>before</em> invoking thise method in order for those
     * sinks to terminate. Otherwise the source
     * {@link IAsynchronousIterator}(s) on which the sink is reading will
     * remain open and the sink will never decide that it has exhausted its
     * source(s).
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    abstract protected void flushAndCloseBuffersAndAwaitSinks()
            throws InterruptedException, ExecutionException;

    /**
     * Cancel sink {@link JoinTask}(s).
     */
    abstract protected void cancelSinks();

    /**
     * Return a chunk of {@link IBindingSet}s from the
     * {@link IAsynchronousIterator}s. The 1st join dimension is always fed
     * by the {@link JoinMasterTask}. The nth+1 join dimension is always
     * fed by the nth {@link JoinTask}(s).
     * 
     * @return The next available chunk of {@link IBindingSet}s -or-
     *         <code>null</code> IFF all known source(s) are exhausted.
     */
    abstract protected IBindingSet[] nextChunk();

    /**
     * Class consumes chunks from the source(s) until cancelled,
     * interrupted, or all source(s) are exhausted. For each
     * {@link IBindingSet} in each chunk, an {@link AccessPathTask} is
     * created which will consume that {@link IBindingSet}. The
     * {@link AccessPathTask} for a given source chunk are sorted based on
     * their <code>fromKey</code> so as to order the execution of those
     * tasks in a manner that will maximize the efficiency of index reads.
     * The ordered {@link AccessPathTask}s are then submitted to the
     * caller's {@link Executor}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    protected class BindingSetConsumerTask implements Callable {

        private final ExecutorService executor;

        /**
         * 
         * @param executor
         *            The service that will execute the generated
         *            {@link AccessPathTask}s -or- <code>null</code> IFF
         *            you want the {@link AccessPathTask}s to be executed
         *            in the caller's thread.
         */
        public BindingSetConsumerTask(final ExecutorService executor) {

            this.executor = executor;

        }

        /**
         * Read chunks from one or more sources until cancelled,
         * interrupted, or all sources are exhausted and submits
         * {@link AccessPathTask}s to the caller's {@link ExecutorService}
         * -or- executes those tasks in the caller's thread if no
         * {@link ExecutorService} was provided to the ctor.
         * <p>
         * Note: When running with an {@link ExecutorService}, the caller
         * is responsible for waiting on that {@link ExecutorService} until
         * the {@link AccessPathTask}s to complete and must verify all
         * tasks completed successfully.
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
        public Object call() throws Exception {

            try {

                if (DEBUG)
                    log.debug("begin: orderIndex=" + orderIndex
                            + ", partitionId=" + partitionId);

                IBindingSet[] chunk;

                while (!halt && (chunk = nextChunk()) != null) {
                    // @todo ChunkTrace for bindingSet chunks in as well as access path chunks consumed 
                    if (DEBUG)
                        log.debug("Read chunk of bindings: chunkSize="
                                + chunk.length + ", orderIndex=" + orderIndex
                                + ", partitionId=" + partitionId);

                    /*
                     * Aggregate the source bindingSets that license the
                     * same asBound predicate.
                     */
                    final Map<IPredicate, Collection<IBindingSet>> map = combineBindingSets(chunk);

                    /*
                     * Generate an AbstractPathTask from each distinct
                     * asBound predicate that will consume all of the source
                     * bindingSets in the chunk which resulted in the same
                     * asBound predicate.
                     */
                    final AccessPathTask[] tasks = getAccessPathTasks(map);

                    /*
                     * Reorder those tasks for better index read
                     * performance.
                     */
                    reorderTasks(tasks);

                    /*
                     * Execute the tasks (either in the caller's thread or
                     * on the supplied service).
                     */
                    executeTasks(tasks);

                }

                if (halt)
                    throw new RuntimeException(firstCause.get());

                if (DEBUG)
                    log.debug("done: orderIndex=" + orderIndex
                            + ", partitionId=" + partitionId);

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
        protected Map<IPredicate, Collection<IBindingSet>> combineBindingSets(
                final IBindingSet[] chunk) {

            if (DEBUG)
                log.debug("chunkSize=" + chunk.length);

            final int tailIndex = getTailIndex(orderIndex);

            final Map<IPredicate, Collection<IBindingSet>> map = new LinkedHashMap<IPredicate, Collection<IBindingSet>>(
                    chunk.length);

            for (IBindingSet bindingSet : chunk) {

                if (halt)
                    throw new RuntimeException(firstCause.get());

                // constrain the predicate to the given bindings.
                IPredicate predicate = rule.getTail(tailIndex).asBound(
                        bindingSet);

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

                    // more than one bindingSet will use the same access path.
                    stats.accessPathDups++;

                }

                /*
                 * Add the bindingSet to the collection of bindingSets
                 * paired with the asBound predicate.
                 */

                values.add(bindingSet);

            }

            if (DEBUG)
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
                final Map<IPredicate, Collection<IBindingSet>> map) {

            final int n = map.size();

            if (DEBUG)
                log.debug("#distinct predicates=" + n);

            final AccessPathTask[] tasks = new AccessPathTask[n];

            final Iterator<Map.Entry<IPredicate, Collection<IBindingSet>>> itr = map
                    .entrySet().iterator();

            int i = 0;

            while (itr.hasNext()) {

                if (halt)
                    throw new RuntimeException(firstCause.get());

                final Map.Entry<IPredicate, Collection<IBindingSet>> entry = itr
                        .next();

                final IPredicate predicate = entry.getKey();

                final Collection<IBindingSet> bindingSets = entry.getValue();

                tasks[i++] = new AccessPathTask(predicate, bindingSets);

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
            if (tasks[0].accessPath instanceof AbstractAccessPath) {

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

            int i = 0;
            for (AccessPathTask task : tasks) {

                if (halt)
                    throw new RuntimeException(firstCause.get());

                if (executor != null) {

                    /*
                     * Queue the AccessPathTask for execution.
                     * 
                     * Note: The caller MUST verify that no tasks submitted
                     * to the [executor] result in errors. This is done by
                     * checking the errorCount for that [executor], so you
                     * need to run the tasks on a service which exposes that
                     * information.
                     */

                    executor.submit(task);

                } else {

                    /*
                     * Execute the AccessPathTask in the caller's thread.
                     */

                    task.call();

                }

                i++;

            } // next task.

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
    protected class AccessPathTask implements Callable, Comparable<AccessPathTask> {

        /**
         * The {@link IBindingSet}s from the source join dimension to be
         * combined with each element visited on the {@link #accessPath}.
         * If there is only a single source {@link IBindingSet} in a given
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
        final private IAccessPath accessPath;

        /**
         * Return the <em>fromKey</em> for the {@link IAccessPath}
         * generated from the {@link IBindingSet} for this task.
         * 
         * @todo layered access paths do not expose a fromKey.
         */
        protected byte[] getFromKey() {

            return ((AbstractAccessPath) accessPath).getFromKey();

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
        public boolean equals(AccessPathTask o) {

            return accessPath.getPredicate()
                    .equals(o.accessPath.getPredicate());

        }

        /**
         * Evaluate an {@link IBindingSet} for the join dimension. When the
         * task runs, it will pair each element visited on the
         * {@link IAccessPath} with the asBound {@link IPredicate}. For
         * each element visited, if the binding is acceptable for the
         * constraints on the asBound {@link IPredicate}, then the task
         * will emit one {@link IBindingSet} for each source
         * {@link IBindingSet}.
         * 
         * @param predicate
         *            The asBound {@link IPredicate}.
         * @param bindingSets
         *            A collection of {@link IBindingSet}s from the source
         *            join dimension that all result in the same asBound
         *            {@link IPredicate}.
         */
        public AccessPathTask(final IPredicate predicate,
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

            this.accessPath = joinNexus.getTailAccessPath(predicate);

            if (DEBUG)
                log.debug("orderIndex=" + orderIndex + ", tailIndex="
                        + tailIndex + ", tail=" + rule.getTail(tailIndex)
                        + ", #bindingSets=" + n + ", accessPath=" + accessPath);

            // convert to array for thread-safe traversal.
            this.bindingSets = bindingSets.toArray(new IBindingSet[n]);

        }

        public String toString() {

            return JoinTask.this.getClass().getSimpleName() + "{ orderIndex="
                    + orderIndex + ", partitionId=" + partitionId
                    + ", #bindingSets=" + bindingSets.length + "}";

        }

        /**
         * Evaluate the {@link #accessPath} against the {@link #bindingSets}.
         * If nothing is accepted and {@link IPredicate#isOptional()} then
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
        public Object call() throws Exception {

            if (halt)
                throw new RuntimeException(firstCause.get());

            final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer = threadLocalBufferFactory
                    .get();

            boolean nothingAccepted = true;

            stats.accessPathCount++;

            // Obtain the iterator for the current join dimension.
            final IChunkedOrderedIterator itr = accessPath.iterator();

            try {

                while (itr.hasNext()) {

                    final Object[] chunk = itr.nextChunk();

                    stats.chunkCount++;

                    // process the chunk in the caller's thread.
                    if (new ChunkTask(bindingSets, unsyncBuffer, chunk).call()) {

                        nothingAccepted = false;

                    }

                } // next chunk.

                if (nothingAccepted && predicate.isOptional()) {

                    /*
                     * Note: when NO binding sets were accepted AND the
                     * predicate is OPTIONAL then we output the _original_
                     * binding set(s) to the sink join task(s).
                     */

                    for (IBindingSet bs : this.bindingSets) {

                        unsyncBuffer.add(bs);

                    }

                }

                return null;

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
        public int compareTo(AccessPathTask o) {

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
    protected class ChunkTask implements Callable {

        /**
         * The index of the predicate for the access path that is being
         * consumed.
         */
        private final int tailIndex;

        /**
         * The {@link IBindingSet}s which the each element in the chunk
         * will be paired to create {@link IBindingSet}s for the downstream
         * join dimension.
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
         *            of generated {@link IBindingSet}s.
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

            if (unsyncBuffer == null)
                throw new IllegalArgumentException();

            if (chunk == null)
                throw new IllegalArgumentException();

            this.tailIndex = getTailIndex(orderIndex);

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

                ChunkTrace.chunk(orderIndex, chunk);

                boolean nothingAccepted = true;

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
                        if (joinNexus.bind(rule, tailIndex, e, bset)) {

                            // Accept this binding set.
                            unsyncBuffer.add(bset);

                            naccepted++;

                            nothingAccepted = false;

                        }

                    }

                    if (DEBUG)
                        log.debug("Accepted element for " + naccepted + " of "
                                + bindingSets.length
                                + " possible bindingSet combinations: "
                                + e.toString() + ", orderIndex=" + orderIndex
                                + ", lastJoin=" + lastJoin + ", rule="
                                + rule.getName());
                }

                return nothingAccepted ? Boolean.TRUE : Boolean.FALSE;

            } catch (Throwable t) {

                halt(t);

                throw new RuntimeException(t);

            }

        }

    }

}
