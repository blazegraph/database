package com.bigdata.relation.rule.eval.pipeline;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.bigdata.concurrent.NamedLock;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IRuleState;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.striterator.IKeyOrder;

/**
 * Implementation used by scale-out deployments. There will be one instance
 * of this task per index partition on which the rule will read. Those
 * instances will be in-process on the {@link DataService} hosting that
 * index partition. Instances are created on the {@link DataService} using
 * the {@link JoinTaskFactoryTask} helper class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class DistributedJoinTask extends JoinTask {

    /**
     * When <code>true</code>, enables a trace on {@link System#err} of
     * the code polling the source {@link IAsynchronousIterator}s from
     * which this {@link DistributedJoinTask} draws its {@link IBindingSet}
     * chunks.
     */
    static private final boolean trace = false;

    /**
     * The federation is used to obtain locator scans for the access paths.
     */
    final protected AbstractScaleOutFederation fed;

    /**
     * The {@link IJoinNexus} for the {@link IBigdataFederation}. This is
     * mainly used to setup the {@link #solutionBuffer} since it needs to
     * write on the scale-out index while the {@link AccessPathTask} will
     * read on the local index partition view.
     */
    final protected IJoinNexus fedJoinNexus;

    /**
     * @see IRuleState#getKeyOrder()
     */
    final private IKeyOrder[] keyOrders;

    /**
     * The name of the scale-out index associated with the next
     * {@link IPredicate} in the evaluation order and <code>null</code>
     * iff this is the last {@link IPredicate} in the evaluation order.
     */
    final private String nextScaleOutIndexName;

    /**
     * Sources for {@link IBindingSet} chunks that will be processed by this
     * {@link JoinTask}. There will be one such source for each upstream
     * {@link JoinTask} that targets this {@link JoinTask}.
     * <p>
     * Note: This is a thread-safe collection since new sources may be added
     * asynchronously during processing.
     */
    final private Vector<IAsynchronousIterator<IBindingSet[]>> sources = new Vector<IAsynchronousIterator<IBindingSet[]>>();

    /**
     * The {@link JoinTaskSink}s for the downstream
     * {@link DistributedJoinTask}s onto which the generated
     * {@link IBindingSet}s will be written. This is <code>null</code>
     * for the last join since we will write solutions onto the
     * {@link #getSolutionBuffer()} instead.
     * 
     * @todo configure capacity based on expectations of index partition
     *       fan-out for this join dimension
     */
    final private Map<PartitionLocator, JoinTaskSink> sinkCache;

    public DistributedJoinTask(
            //final ConcurrencyManager concurrencyManager,
            final String scaleOutIndexName, final IRule rule,
            final IJoinNexus joinNexus, final int[] order,
            final int orderIndex, final int partitionId,
            final AbstractScaleOutFederation fed, final IJoinMaster master,
            final IAsynchronousIterator<IBindingSet[]> src,
            final IKeyOrder[] keyOrders) {

        super(
                //concurrencyManager,
                DataService.getIndexPartitionName(scaleOutIndexName,
                        partitionId), rule, joinNexus, order, orderIndex,
                partitionId, master);

        if (fed == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.keyOrders = keyOrders;

        this.fedJoinNexus = joinNexus.getJoinNexusFactory().newInstance(fed);

        if (lastJoin) {

            sinkCache = null;

            nextScaleOutIndexName = null;

            final ActionEnum action = fedJoinNexus.getAction();

            if (action.isMutation()) {

                /*
                 * Note: The solution buffer for mutation operations
                 * is obtained locally from a joinNexus that is
                 * backed by the federation NOT the local index
                 * manager. (This is because the solution buffer
                 * needs to write on the scale-out indices.)
                 */

                final IJoinNexus tmp = fedJoinNexus;

                /*
                 * The view of the mutable relation for the _head_ of the
                 * rule.
                 */

                final IMutableRelation relation = (IMutableRelation) tmp
                        .getHeadRelationView(rule.getHead());

                switch (action) {

                case Insert: {

                    solutionBuffer = tmp.newInsertBuffer(relation);

                    break;

                }

                case Delete: {

                    solutionBuffer = tmp.newDeleteBuffer(relation);

                    break;

                }

                default:
                    throw new AssertionError();

                }

            } else {

                /*
                 * The solution buffer for queries is obtained from the
                 * master.
                 */

                try {

                    solutionBuffer = masterProxy.getSolutionBuffer();

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

        } else {

            final IPredicate nextPredicate = rule
                    .getTail(order[orderIndex + 1]);

            final String namespace = nextPredicate.getOnlyRelationName();

            nextScaleOutIndexName = namespace
                    + keyOrders[order[orderIndex + 1]];

            solutionBuffer = null;

            sinkCache = new LinkedHashMap<PartitionLocator, JoinTaskSink>();

            //                System.err.println("orderIndex=" + orderIndex + ", resources="
            //                        + Arrays.toString(getResource()) + ", nextPredicate="
            //                        + nextPredicate + ", nextScaleOutIndexName="
            //                        + nextScaleOutIndexName);

        }

        addSource(src);

    }

    /**
     * Adds a source from which this {@link DistributedJoinTask} will read
     * {@link IBindingSet} chunks. 
     * 
     * @param source
     *            The source.
     * 
     * @throws IllegalArgumentException
     *             if the <i>source</i> is <code>null</code>.
     * @throws IllegalStateException
     *             if {@link #closeSources()} has already been invoked.
     */
    public void addSource(final IAsynchronousIterator<IBindingSet[]> source) {

        if (source == null)
            throw new IllegalArgumentException();

        if (closedSources) {

            // new source declarations are rejected.
            throw new IllegalStateException();

        }

        sources.add(source);

        stats.fanIn++;

        if (DEBUG)
            log.debug("orderIndex=" + orderIndex + ", partitionId="
                    + partitionId + ", fanIn=" + stats.fanIn + ", fanOut="
                    + stats.fanOut);

    }

    final protected IBuffer<ISolution[]> getSolutionBuffer() {

        return solutionBuffer;

    }

    private final IBuffer<ISolution[]> solutionBuffer;

    /**
     * Sets a flag preventing new sources from being declared and closes all
     * known {@link #sources}.
     */
    protected void closeSources() {

        if (INFO)
            log.info(toString());

        closedSources = true;

        final IAsynchronousIterator[] a = sources
                .toArray(new IAsynchronousIterator[] {});

        for (IAsynchronousIterator source : a) {

            source.close();

        }

    }

    private volatile boolean closedSources = false;

    /**
     * This lock is used to make {@link #nextChunk()} and
     * {@link #addSource(IAsynchronousIterator)} into mutally exclusive
     * operations. {@link #nextChunk()} is the reader.
     * {@link #addSource(IAsynchronousIterator)} is the writer. These
     * operations need to be exclusive and atomic so that the termination
     * condition of {@link #nextChunk()} is consistent -- it should
     * terminate when there are no sources remaining. The first source is
     * added when the {@link DistributedJoinTask} is created. Additional
     * sources are added (and can result in a fan-in greater than one) when
     * a {@link JoinTaskFactoryTask} identifies that there is an existing
     * {@link DistributedJoinTask} and is able to atomically assign a new
     * source to that {@link DistributedJoinTask}. If the atomic assignment
     * of the new source fails (because all sources are exhausted before the
     * assignment occurs) then a new {@link DistributedJoinTask} will be
     * created for the same {@link DistributedJoinMasterTask}, orderIndex,
     * and index partition identifier and the source will be assigned to
     * that {@link DistributedJoinTask} instead.
     * 
     * FIXME this implies that we replace one task with the other in the
     * data service session.
     */
    private ReadWriteLock lock = new ReentrantReadWriteLock(false/* fair */);

    /**
     * Returns a chunk of {@link IBindingSet}s by combining chunks from the
     * various source {@link JoinTask}s.
     * 
     * @return A chunk assembled from one or more chunks from one or more of
     *         the source {@link JoinTask}s.
     */
    protected IBindingSet[] nextChunk() {

        if (DEBUG)
            log.debug("Reading chunk of bindings from source(s): orderIndex="
                    + orderIndex + ", partitionId=" + partitionId);

        // #of elements in the combined chunk(s)
        int bindingSetCount = 0;

        // source chunks read so far.
        final List<IBindingSet[]> chunks = new LinkedList<IBindingSet[]>();

        /*
         * Assemble a chunk of suitable size
         * 
         * @todo don't wait too long. if we have some data then it is
         * probably better to process that data rather than waiting beyond a
         * timeout for a full chunk. also, make sure that we are neither
         * yielding nor spinning too long in this loop. However, the loop
         * must wait if there is nothing available and the sources are not
         * yet exhausted.
         * 
         * @todo config. we need a different capacity here than the one used
         * for batch index operations. on the order of 100 should work well.
         * 
         * FIXME If fanIn GT ONE then how do we know when we can terminate
         * since there may be new upstream tasks at any time? E.g., we map
         * the 1st join dimension over 4 index partitions. At any given
         * time, this will read from one, two, or all of the mapped join
         * tasks for the first join dimension. However, it is perfectly
         * possible for one of the source join tasks to be done before
         * another one starts, in which case this loop might terminate early
         * since [sources] would be empty in between.
         */

        final int chunkCapacity = 100;// joinNexus.getChunkCapacity();

        while (!halt && !sources.isEmpty() && bindingSetCount < chunkCapacity) {

            if (trace)
                System.err.print("\norderIndex=" + orderIndex);

            if (trace)
                System.err.print(": reading");

            //                if (DEBUG)
            //                    log.debug("Testing " + nsources + " sources: orderIndex="
            //                            + orderIndex + ", partitionId=" + partitionId);

            // clone to avoid concurrent modification of sources during traversal.
            final IAsynchronousIterator<IBindingSet[]>[] sources = (IAsynchronousIterator<IBindingSet[]>[]) this.sources
                    .toArray(new IAsynchronousIterator[] {});

            // #of sources that are exhausted.
            int nexhausted = 0;

            for (int i = 0; i < sources.length
                    && bindingSetCount < chunkCapacity; i++) {

                if (trace)
                    System.err.print(" <<(" + i + ":" + sources.length + ")");

                final IAsynchronousIterator<IBindingSet[]> src = sources[i];

                // if there is something to read on that source.
                if (src.hasNext(1L, TimeUnit.MILLISECONDS)) {

                    /*
                     * Read the chunk, waiting up to the timeout for
                     * additional chunks from this source which can be
                     * combined together by the iterator into a single
                     * chunk.
                     * 
                     * @todo config chunkCombiner timeout here and
                     * experiment with the value with varying fanIns.
                     */
                    final IBindingSet[] chunk = src.next(10L,
                            TimeUnit.MILLISECONDS);

                    /*
                     * Note: Since hasNext() returned [true] for this source
                     * we SHOULD get a chunk back since it is known to be
                     * there waiting for us. The timeout should only give
                     * the iterator an opportunity to combine multiple
                     * chunks together if they are already in the iterator's
                     * queue (or if they arrive in a timely manner).
                     */
                    assert chunk != null;

                    chunks.add(chunk);

                    bindingSetCount += chunk.length;

                    if (trace)
                        System.err.print("[" + chunk.length + "]");

                    if (DEBUG)
                        log.debug("Read chunk from source: sources[" + i
                                + "], chunkSize=" + chunk.length
                                + ", orderIndex=" + orderIndex
                                + ", partitionId=" + partitionId);

                } else if (src.isExhausted()) {

                    nexhausted++;

                    if (trace)
                        System.err.print("X{" + nexhausted + "}");

                    if (DEBUG)
                        log.debug("Source is exhausted: nexhausted="
                                + nexhausted);

                    // no longer consider an exhausted source.
                    if (!this.sources.remove(src)) {

                        // could happen if src.equals() is not defined.
                        throw new AssertionError("Could not find source: "
                                + src);

                    }

                }

            }

            if (nexhausted == sources.length) {

                /*
                 * All sources are exhausted, but we may have buffered some
                 * data, which is checked below.
                 */

                break;

            }

        }

        if (halt)
            throw new RuntimeException(firstCause.get());

        /*
         * Combine the chunks.
         */

        final int chunkCount = chunks.size();

        if (chunkCount == 0) {

            /*
             * Termination condition: we did not get any data from any
             * source.
             * 
             * Note: This implies that all sources are exhausted per the
             * logic above.
             */

            if (DEBUG)
                log.debug("Sources are exhausted: orderIndex=" + orderIndex
                        + ", partitionId=" + partitionId);

            if (trace)
                System.err.print(" exhausted");

            return null;

        }

        final IBindingSet[] chunk;

        if (chunkCount == 1) {

            // Only one chunk is available.

            chunk = chunks.get(0);

        } else {

            // Combine 2 or more chunks.

            chunk = new IBindingSet[bindingSetCount];

            final Iterator<IBindingSet[]> itr = chunks.iterator();

            int offset = 0;

            while (itr.hasNext()) {

                final IBindingSet[] a = itr.next();

                System.arraycopy(a, 0, chunk, offset, a.length);

                offset += a.length;

            }

        }

        if (halt)
            throw new RuntimeException(firstCause.get());

        if (DEBUG) {

            log.debug("Read chunk(s): nchunks=" + chunkCount
                    + ", #bindingSets=" + chunk.length + ", orderIndex="
                    + orderIndex + ", partitionId=" + partitionId);
        }

        stats.bindingSetChunksIn += chunkCount;
        stats.bindingSetsIn += bindingSetCount;

        if (trace)
            System.err.print(" chunk[" + chunk.length + "]");

        return chunk;

    }

    protected AbstractUnsynchronizedArrayBuffer<IBindingSet> newUnsyncOutputBuffer() {

        final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncOutputBuffer;

        /*
         * On overflow, the generated binding sets are mapped across the
         * JoinTaskSink(s) for the target index partition(s).
         */

        final int chunkCapacity = fedJoinNexus.getChunkCapacity();

        if (lastJoin) {

            /*
             * Accepted binding sets are flushed to the solution buffer.
             */

            unsyncOutputBuffer = new UnsynchronizedSolutionBuffer<IBindingSet>(
                    this, fedJoinNexus, chunkCapacity);

        } else {

            /*
             * Accepted binding sets are flushed to the next join dimension.
             * 
             * Note: The index is key-range partitioned. Each bindingSet
             * will be mapped across the index partition(s) on which the
             * generated access path for that bindingSet will have to read.
             * There will be a JoinTask associated with each such index
             * partition. That JoinTask will execute locally on the
             * DataService which hosts that index partition.
             */

            unsyncOutputBuffer = new UnsyncDistributedOutputBuffer<IBindingSet>(
                    fed, this, chunkCapacity);

        }

        return unsyncOutputBuffer;

    }

    /**
     * Notifies each sink that this {@link DistributedJoinTask} will no
     * longer generate new {@link IBindingSet} chunks and then waits for the
     * sink task(s) to complete.
     * <p>
     * Note: Closing the {@link BlockingBuffer} from which a sink
     * {@link JoinTask} is reading will cause the source iterator for that
     * sink task to eventually return <code>false</code> indicating that
     * it is exhausted (assuming that the sink keeps reading on the
     * iterator).
     * 
     * @throws InterruptedException
     *             if interrupted while awaiting the future for a sink.
     */
    @Override
    protected void flushAndCloseBuffersAndAwaitSinks()
            throws InterruptedException, ExecutionException {

        if (DEBUG)
            log.debug("orderIndex="
                    + orderIndex
                    + ", partitionId="
                    + partitionId
                    + (lastJoin ? ", lastJoin" : ", sinkCount="
                            + sinkCache.size()));

        /*
         * For the last join dimension the JoinTask instead writes onto the
         * [solutionBuffer]. For query, that is the shared solution buffer
         * and will be a proxied object. For mutation, that is a per
         * JoinTask buffer that writes onto the target relation. In the
         * latter case we MUST report the mutationCount returned by flushing
         * the solutionBuffer via JoinStats to the master.
         * 
         * Note: JoinTask#flushUnsyncBuffers() will already have been
         * invoked so all generated binding sets will already be in the sync
         * buffer ready for output.
         */
        if (lastJoin) {

            assert sinkCache == null;

            if (DEBUG)
                log.debug("\nWill flush buffer containing "
                        + getSolutionBuffer().size() + " solutions.");

            final long counter = getSolutionBuffer().flush();

            if (DEBUG)
                log.debug("\nFlushed buffer: mutationCount=" + counter);

            if (joinNexus.getAction().isMutation()) {

                /*
                 * Apply mutationCount to the JoinStats so that it will be
                 * reported back to the JoinMasterTask.
                 */

                stats.mutationCount.addAndGet(counter);

            }

        } else {

            /*
             * Close sinks.
             * 
             * For all but the lastJoin, the buffers are writing onto the
             * per-sink buffers. We flush and close those buffers now. The
             * sink JoinTasks drain those buffers. Once the buffers are
             * closed, the sink JoinTasks will eventually exhaust the
             * buffers.
             * 
             * FIXME This should flush the buffers using a thread pool with
             * one thread per sink. This will give better throughput when
             * the fanOut is GT ONE (1).
             */

            {

                final Iterator<JoinTaskSink> itr = sinkCache.values()
                        .iterator();

                while (itr.hasNext()) {

                    if (halt)
                        throw new RuntimeException(firstCause.get());

                    final JoinTaskSink sink = itr.next();

                    if (DEBUG)
                        log.debug("Closing sink: sink=" + sink
                                + ", unsyncBufferSize="
                                + sink.unsyncBuffer.size()
                                + ", blockingBufferSize="
                                + sink.blockingBuffer.size());

                    // flush to the blockingBuffer.
                    sink.unsyncBuffer.flush();

                    // close the blockingBuffer.
                    sink.blockingBuffer.close();

                }

            }

            // Await sinks.
            {

                final Iterator<JoinTaskSink> itr = sinkCache.values()
                        .iterator();

                while (itr.hasNext()) {

                    if (halt)
                        throw new RuntimeException(firstCause.get());

                    final JoinTaskSink sink = itr.next();

                    final Future f = sink.getFuture();

                    if (DEBUG)
                        log.debug("Waiting for Future: sink=" + sink);

                    // will throw any exception from the sink's Future.
                    f.get();

                }

            }

        } // else (lastJoin)

        if (DEBUG)
            log.debug("Done: orderIndex="
                    + orderIndex
                    + ", partitionId="
                    + partitionId
                    + (lastJoin ? "lastJoin" : ", sinkCount="
                            + sinkCache.size()));

    }

    /**
     * Cancel all {@link DistributedJoinTask}s that are sinks for this
     * {@link DistributedJoinTask}.
     */
    @Override
    protected void cancelSinks() {

        // no sinks.
        if (lastJoin)
            return;

        if (DEBUG)
            log.debug("orderIndex=" + orderIndex + ", partitionId="
                    + partitionId + ", sinkCount=" + sinkCache.size());

        final Iterator<JoinTaskSink> itr = sinkCache.values().iterator();

        while (itr.hasNext()) {

            final JoinTaskSink sink = itr.next();

            sink.unsyncBuffer.reset();

            sink.blockingBuffer.reset();

            sink.blockingBuffer.close();

            sink.getFuture().cancel(true/* mayInterruptIfRunning */);

        }

        if (DEBUG)
            log.debug("Done: orderIndex=" + orderIndex + ", partitionId="
                    + partitionId + ", sinkCount=" + sinkCache.size());

    }

    //        protected IAsynchronousIterator<ISolution[]> getSolutionIteratorProxy(
    //                IAsynchronousIterator<ISolution[]> itr) {
    //
    //            if (fed.isDistributed()) {
    //
    //                return ((AbstractDistributedFederation) fed).getProxy(itr,
    //                        joinNexus.getSolutionSerializer());
    //
    //            }
    //
    //            return itr;
    //
    //        }

    //        protected IAsynchronousIterator<IBindingSet[]> getBindingSetIteratorProxy(
    //                IAsynchronousIterator<IBindingSet[]> itr) {
    //
    //            if (fed.isDistributed()) {
    //
    //                return ((AbstractDistributedFederation) fed).getProxy(itr,
    //                        joinNexus.getBindingSetSerializer());
    //
    //            }
    //
    //            return itr;
    //
    //        }

    /**
     * Return the sink on which we will write {@link IBindingSet} for the
     * index partition associated with the specified locator. The sink will
     * be backed by a {@link DistributedJoinTask} running on the
     * {@link IDataService} that is host to that index partition. The
     * scale-out index will be the scale-out index for the next
     * {@link IPredicate} in the evaluation order.
     * 
     * @param locator
     *            The locator for the index partition.
     * 
     * @return The sink.
     * 
     * @throws ExecutionException
     *             If the {@link JoinTaskFactoryTask} fails.
     * @throws InterruptedException
     *             If the {@link JoinTaskFactoryTask} is interrupted.
     * 
     * @todo Review this as a possible concurrency bottleneck. The operation
     *       can have significant latency since RMI is required on a cache
     *       miss to lookup or create the {@link JoinTask} on the target
     *       dataService. Therefore we should probably allow concurrent
     *       callers and establish a {@link NamedLock} that serializes
     *       callers seeking the {@link JoinTaskSink} for the same index
     *       partition identifier. Note that the limit on the #of possible
     *       callers is the permitted parallelism for processing the source
     *       {@link IBindingSet}s, e.g., the #of {@link ChunkTask}s that
     *       can execute in parallel for a given {@link JoinTask}.
     */
    synchronized protected JoinTaskSink getSink(final PartitionLocator locator)
            throws InterruptedException, ExecutionException {

        JoinTaskSink sink = sinkCache.get(locator);

        if (sink == null) {

            /*
             * Allocate JoinTask on the target data service and obtain a
             * sink reference for its future and buffers.
             * 
             * Note: The JoinMasterTask uses very similar logic to setup the
             * first join dimension.
             */

            final int nextOrderIndex = orderIndex + 1;

            if (DEBUG)
                log.debug("Creating join task: nextOrderIndex="
                        + nextOrderIndex + ", indexName="
                        + nextScaleOutIndexName + ", partitionId="
                        + locator.getPartitionId());

            final IDataService dataService = fed.getDataService(locator
                    .getDataServices()[0]);

            sink = new JoinTaskSink(fed, locator, this);

            /*
             * Export async iterator proxy.
             * 
             * Note: This proxy is used by the sink to draw chunks from the
             * source JoinTask(s).
             */
            final IAsynchronousIterator<IBindingSet[]> sourceItrProxy;
            if (fed.isDistributed()) {

                sourceItrProxy = ((AbstractDistributedFederation) fed)
                        .getProxy(sink.blockingBuffer.iterator(), joinNexus
                                .getBindingSetSerializer(), joinNexus
                                .getChunkOfChunksCapacity());

            } else {

                sourceItrProxy = sink.blockingBuffer.iterator();

            }

            // the future for the factory task (not the JoinTask).
            final Future factoryFuture;
            try {

                final JoinTaskFactoryTask factoryTask = new JoinTaskFactoryTask(
                        nextScaleOutIndexName, rule, joinNexus
                                .getJoinNexusFactory(), order, nextOrderIndex,
                        locator.getPartitionId(), masterProxy, sourceItrProxy,
                        keyOrders);

                // submit the factory task, obtain its future.
                factoryFuture = dataService.submit(factoryTask);

            } catch (IOException ex) {

                // RMI problem.
                throw new RuntimeException(ex);

            }

            /*
             * Obtain the future for the JoinTask from the factory task's
             * Future.
             */

            sink.setFuture((Future) factoryFuture.get());

            stats.fanOut++;

            sinkCache.put(locator, sink);

        }

        return sink;

    }

}
