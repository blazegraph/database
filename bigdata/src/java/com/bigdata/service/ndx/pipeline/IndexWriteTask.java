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
 * Created on Apr 15, 2009
 */

package com.bigdata.service.ndx.pipeline;

import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IScaleOutClientIndex;
import com.bigdata.service.Split;
import com.bigdata.util.InnerCause;

/**
 * Task drains a {@link BlockingBuffer} containing {@link KVO}[] chunks, splits
 * the chunks based on the separator keys for the scale-out index, and then
 * assigns each chunk to per-index partition {@link BlockingBuffer} which is in
 * turned drained by an {@link IndexPartitionWriteTask} that writes onto a
 * specific index partition.
 * <p>
 * If the task is interrupted, it will refuse additional writes by closing its
 * {@link BlockingBuffer} and will cancel any sub-tasks and discard any buffered
 * writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <T>
 *            The generic type of the procedure used to write on the index.
 * @param <O>
 *            The generic type for unserialized value objects.
 * @param <R>
 *            The type of the result from applying the index procedure to a
 *            single {@link Split} of data.
 * @param <A>
 *            The type of the aggregated result.
 * 
 * @todo performance comparison of write buffers vs method calls or just of the
 *       size of the target chunk for the write buffer?
 */
public class IndexWriteTask<T extends IKeyArrayIndexProcedure, O, R, A> extends
        AbstractHaltableProcess implements Callable<IndexWriteStats> {

    static protected transient final Logger log = Logger
            .getLogger(IndexWriteTask.class);

    // from the ctor.
    private final IScaleOutClientIndex ndx;

    private final int indexPartitionWriteQueueCapacity;

    private final IResultHandler<R, A> resultHandler;

    private final IDuplicateRemover<O> duplicateRemover;

    private final AbstractKeyArrayIndexProcedureConstructor<T> ctor;

    /**
     * The top-level buffer on which the application is writing.
     */
    private final BlockingBuffer<KVO<O>[]> buffer;
    
    /**
     * The iterator draining the {@link #buffer}.
     */
    private final IAsynchronousIterator<KVO<O>[]> src;

    /**
     * Map from the index partition identifier to the open subtask handling
     * writes bound for that index partition.
     * <p>
     * Note: This map must be protected against several kinds of concurrent
     * access using the {@link #lock}.
     */
    private final Int2ObjectLinkedOpenHashMap<IndexPartitionWriteTask<T,O,R,A>> subtasks;

    /**
     * Lock used to ensure consistency of the overall operation. There are
     * several ways in which an inconsistency could arise. Some examples
     * include:
     * <ul>
     * 
     * <li>The client writes on the top-level {@link BlockingBuffer} while an
     * index partition write is asynchronously handling a
     * {@link StaleLocatorException}. This could cause a problem because we may
     * be required to (re-)open an {@link IndexPartitionWriteTask}.</li>
     * 
     * <li>The client has closed the top-level {@link BlockingBuffer} but there
     * are still writes buffered for the individual index partitions. This could
     * cause a problem since we must wait until those buffered writes have been
     * flushed. We can not simply monitor the remaining values in
     * {@link #subtasks} since {@link StaleLocatorException}s could cause new
     * {@link IndexPartitionWriteTask} to start.</li>
     * 
     * <li>...</li>
     * 
     * </ul>
     * 
     * The {@link #lock} is therefore used to make the following operations
     * mutually exclusive while allowing them to complete:
     * <dl>
     * <dt>{@link #addToOutputBuffer(Split, KVO[], boolean)}</dt>
     * <dd>Adding data to an output blocking buffer.</dd>
     * <dt>{@link #handleStaleLocator(IndexPartitionWriteTask, KVO[], StaleLocatorException)}</dt>
     * <dd>Handling a {@link StaleLocatorException}, which may require
     * (re-)opening an {@link IndexPartitionWriteTask} even during
     * {@link #awaitAll()}.</dd>
     * <dt>{@link #cancelAll()}</dt>
     * <dd>Cancelling the task and its subtask(s).</dd>
     * <dt>{@link #awaitAll()}</dt>
     * <dd>Awaiting the successful completion of the task and its subtask(s).</dd>
     * </ol>
     */
    private final ReentrantLock lock = new ReentrantLock();
    
    /**
     * Condition is signaled by a subtask when it is finished. This is used by
     * {@link #awaitAll()} to while waiting for subtasks to complete. If all
     * subtasks in {@link #subtasks} are complete when this signal is received
     * then the master may terminate.
     */
    private final Condition subtask = lock.newCondition();
    
    /**
     * The statistics for the index write operation.
     */
    public final IndexWriteStats stats;

    public IndexWriteTask(final IScaleOutClientIndex ndx,
            final int indexPartitionWriteQueueCapacity,
            final IResultHandler<R, A> resultHandler,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor,
            final IndexWriteStats stats,
            final BlockingBuffer<KVO<O>[]> buffer) {

        if (ndx == null)
            throw new IllegalArgumentException();

        if (indexPartitionWriteQueueCapacity <= 0)
            throw new IllegalArgumentException();

        if (resultHandler == null)
            throw new IllegalArgumentException();

        if (duplicateRemover == null)
            throw new IllegalArgumentException();

        if (ctor == null)
            throw new IllegalArgumentException();

        if (stats == null)
            throw new IllegalArgumentException();
        
        if (buffer == null)
            throw new IllegalArgumentException();

        this.ndx = ndx;

        this.indexPartitionWriteQueueCapacity = indexPartitionWriteQueueCapacity;

        this.resultHandler = resultHandler;

        this.duplicateRemover = duplicateRemover;

        this.ctor = ctor;

        this.buffer = buffer;
        
        this.src = buffer.iterator();

        this.subtasks = new Int2ObjectLinkedOpenHashMap<IndexPartitionWriteTask<T,O,R,A>>();

        this.stats = stats;
        
    }

    public IndexWriteStats call() throws Exception {

        try {

            while (src.hasNext()) {

                final KVO<O>[] a = src.next();
                stats.chunksIn++;
                stats.elementsIn += a.length;

                // Split the ordered chunk.
                final LinkedList<Split> splits = ndx.splitKeys(ndx
                        .getTimestamp(), 0/* fromIndex */,
                        a.length/* toIndex */, a);

                // Break the chunk into the splits
                for (Split split : splits) {

                    halted();

                    addToOutputBuffer(split, a, false/* reopen */);

                }

                if (Thread.interrupted()) {
                    
                    throw halt(new InterruptedException());
                    
                }

                if (log.isDebugEnabled())
                    log.debug(stats);

            }

            awaitAll();

            if (log.isInfoEnabled())
                log.info("Done: " + stats);

        } catch (Throwable t) {

            try {
                cancelAll();
            } catch (Throwable t2) {
                log.error(t2);
            }

            throw new RuntimeException( t );

        }

        // Done.
        return stats;

    }

    /**
     * Await the completion of the writes on each index partition.
     * <p>
     * Note: This is tricky because a new buffer may be created at any time in
     * response to a {@link StaleLocatorException}. Also, when we handle a
     * {@link StaleLocatorException}, it is possible that new writes will be
     * identified for an index partition whose buffer we already closed (this is
     * handled by re-opening of the output buffer for an index partition if it
     * is closed when we handle a {@link StaleLocatorException}).
     * 
     * @throws ExecutionException
     *             This will report the first cause.
     * @throws InterruptedException
     *             If interrupted while awaiting the {@link #lock} or the child
     *             tasks.
     * 
     * @todo unit tests for some of these subtle points.
     */
    private void awaitAll() throws InterruptedException, ExecutionException {

        lock.lockInterruptibly();
        try {

            // close the source.
            src.close();

            while (true) {

                halted();
                
                final BlockingBuffer[] sinks = subtasks.values().toArray(new BlockingBuffer[0]);

                if (sinks.length == 0) {

                    // Done.
                    return;

                }

                /*
                 * Wait for the sinks to complete.
                 */
                for (BlockingBuffer sink : sinks) {

                    final Future<IndexPartitionWriteStats> f = (Future<IndexPartitionWriteStats>) sink
                            .getFuture();

                    if (f.isDone()) {

                        // check the future (can throw exception).
                        f.get();

                    }
                    
                }

                /*
                 * Yield the lock and wait up to a timeout for a sink to
                 * complete.
                 */
                subtask.await(10, TimeUnit.MILLISECONDS);

            } // continue

        } finally {
            
            lock.unlock();
            
        }

    }

    /**
     * Cancel all running tasks, discarding any buffered data.
     * <p>
     * Note: This method does not wait on the cancelled tasks.
     * <p>
     * Note: The caller should have already invoked {@link #halt(Throwable)}.
     */
    private void cancelAll() {

        lock.lock();
        try {

            // close the source.
            src.close();

            for (IndexPartitionWriteTask<T, O, R, A> sink : subtasks.values()) {

                final Future f = sink.buffer.getFuture();

                if (!f.isDone()) {

                    f.cancel(true/* mayInterruptIfRunning */);

                }

            }
            
        } finally {

            lock.unlock();
            
        }
        
    }
    
    /**
     * Return a {@link BlockingBuffer} which will write onto the indicated index
     * partition. The buffer is created if it does not exist. The buffer will be
     * drained by a concurrent thread running on the
     * {@link IBigdataFederation#getExecutorService()}. Buffers returned via
     * this method are automatically closed when the source iterator for this
     * class is exhausted.
     * <p>
     * Note: The caller must own the {@link #lock}. This requirement arises
     * because this method is invoked not only from within the thread consuming
     * consuming the top-level buffer but also invoked concurrently from the
     * thread(s) consuming the output buffer(s) when handling a
     * {@link StaleLocatorException} for that output buffer.
     * 
     * @param locator
     *            The index partition locator.
     * @param reopen
     *            <code>true</code> IFF a closed buffer should be re-opened
     *            (in fact, this causes a new buffer to be created and the new
     *            buffer will be drained by a new
     *            {@link IndexPartitionWriteTask}).
     * 
     * @return The {@link BlockingBuffer} for that index partition.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalMonitorStateException
     *             unless the caller owns the {@link #lock}.
     * @throws RuntimeException
     *             if {@link #halted()}
     */
    private BlockingBuffer<KVO<O>[]> getOutputBuffer(
            final PartitionLocator locator, final boolean reopen) {

        if (locator == null)
            throw new IllegalArgumentException();

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();
        
        // operation not allowed if halted.
        halted();
        
        final int partitionId = locator.getPartitionId();

        IndexPartitionWriteTask<T,O,R,A> sink = subtasks.get(partitionId);

        if (sink == null || (reopen && !sink.buffer.isOpen())) {

            /*
             * Resolve the service UUID to a proxy for the data service.
             * 
             * Note: If the sink already exists then we use its reference for
             * the dataService. This avoids a small overhead for service lookup
             * but also helps to make the system more robust since we known that
             * the reference is still valid unless we get an RMI error when we
             * try to use it. However, this will still do a service lookup if a
             * sink completes its processing and is removed from the map before
             * we see another request for a sink writing on the same index
             * partition.
             */
            final IDataService dataService = (sink == null ? ndx
                    .getDataService(locator) : sink.dataService);

            if (dataService == null)
                throw new RuntimeException("DataService not found: " + locator);

            final BlockingBuffer<KVO<O>[]> out = new BlockingBuffer<KVO<O>[]>(
                    new ArrayBlockingQueue<KVO<O>[]>(
                            indexPartitionWriteQueueCapacity), //
                    BlockingBuffer.DEFAULT_CONSUMER_CHUNK_SIZE,// @todo config
                    BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT,// @todo config
                    BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT,//
                    true// ordered
            );

            new IndexPartitionWriteTask<T, O, R, A>(this, locator, dataService,
                    out);

            final Future<IndexPartitionWriteStats> future = ndx.getFederation()
                    .getExecutorService().submit(sink);

            out.setFuture(future);

            subtasks.put(partitionId, sink);
            
            stats.indexPartitionStartCount++;

        }

        return sink.buffer;

    }
    
    /**
     * Removes the output buffer (unless it has been replaced by another output
     * buffer associated with a different sink).
     * <p>
     * Note: The {@link IndexPartitionWriteTask} invokes this method to remove
     * its output buffer when it is done. However, it is possible for
     * {@link #getOutputBuffer(PartitionLocator)} to replace the sink in the
     * {@link #subtasks} map if <i>reopen</i> was true. When that occurs, the
     * request by the old sink will be ignored.
     * 
     * @param sink
     *            The sink.
     */
    protected void removeOutputBuffer(
            final IndexPartitionWriteTask<T, O, R, A> sink) {

        if (sink == null)
            throw new IllegalArgumentException();

        lock.lock();
        try {

            final IndexPartitionWriteTask<T, O, R, A> t = subtasks
                    .get(sink.partitionId);

            if (t == sink) {

                /*
                 * Remove map entry IFF it is for the same reference.
                 */

                subtasks.remove(sink.partitionId);

            }

            /*
             * Note: increment counter regardless of whether or not the
             * reference was the same since the specified sink is now done.
             */
            stats.indexPartitionDoneCount++;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Resolves the output buffer onto which the split must be written and adds
     * the data to that output buffer.
     * <p>
     * Note: This is <code>synchronized</code> in order to make it MUTEX with
     * {@link #handleStaleLocator(IndexPartitionWriteTask, KVO[], StaleLocatorException)}.
     * <p>
     * Note: <em>reopen</em> causes a new {@link BlockingBuffer} to be
     * allocated. Therefore the existing {@link BlockingBuffer} MUST be not only
     * closed but also completely drained before reopen is allowed. The is only
     * legitimate within
     * {@link #handleStaleLocator(IndexPartitionWriteTask, KVO[], StaleLocatorException)}
     * 
     * @param split
     *            The {@link Split} identifies both the tuples to be dispatched
     *            and the {@link PartitionLocator} on which they must be
     *            written.
     * @param a
     *            The array of tuples. Only those tuples addressed by the
     *            <i>split</i> will be written onto the output buffer.
     * @param reopen
     *            <code>true</code> IFF a closed buffer should be re-opened
     *            (in fact, this causes a new buffer to be created and the new
     *            buffer will be drained by a new
     *            {@link IndexPartitionWriteTask}).
     * 
     * @throws InterruptedException
     *             if the thread is interrupted.
     */
    @SuppressWarnings("unchecked")
    protected void addToOutputBuffer(final Split split, final KVO<O>[] a,
            final boolean reopen) throws InterruptedException {

        lock.lockInterruptibly();
        try {
            /*
             * Make a dense chunk for this split.
             */

            final KVO<O>[] b = new KVO[split.ntuples];

            for (int i = 0, j = split.fromIndex; i < split.ntuples; i++, j++) {

                b[i] = a[j];

            }

            // add the dense split to the appropriate output buffer.
            getOutputBuffer((PartitionLocator) split.pmd, reopen).add(b);

        } finally {

            lock.unlock();

        }

    }

    /**
     * The master has to: (a) update its locator cache such that no more work is
     * assigned to this output buffer; (b) re-split the chunk which failed with
     * the StaleLocatorException; and (c) re-split all the data remaining in the
     * output buffer since it all needs to go into different output buffer(s).
     * <p>
     * Note: This is synchronized in order to make the handling of a
     * {@link StaleLocatorException} MUTEX with respect to adding data to an
     * output buffer using {@link #addToOutputBuffer(Split, KVO[])}. This
     * provides a guarentee that no more data will be added to a given output
     * buffer once this method holds the monitor. Since the output buffer is
     * single threaded we will never observe more than one
     * {@link StaleLocatorException} for a given index partition within the
     * context of the same {@link IndexWriteTask}. Together this allows us to
     * decisively handle the {@link StaleLocatorException} and close out the
     * output buffer on which it was received.
     * 
     * @param sink
     *            The class draining the output buffer.
     * @param chunk
     *            The chunk which it was writing when it received the
     *            {@link StaleLocatorException}.
     * @param cause
     *            The {@link StaleLocatorException}.
     */
    protected void handleStaleLocator(
            final IndexPartitionWriteTask<T, O, R, A> sink,
            final KVO<O>[] chunk, final StaleLocatorException cause)
            throws InterruptedException {

        if (sink == null)
            throw new IllegalArgumentException();
        
        if (chunk == null)
            throw new IllegalArgumentException();
        
        if (cause == null)
            throw new IllegalArgumentException();

        lock.lockInterruptibly();
        try {

            stats.staleLocatorCount++;

            final long ts = ndx.getTimestamp();

            /*
             * Notify the client so it can refresh the information for this
             * locator.
             */
            ndx.staleLocator(ts, sink.locator, cause);

            /*
             * Close the output buffer for this sink - nothing more may written
             * onto it now that we have seen the StaleLocatorException.
             */
            sink.buffer.close();

            // Handle the chunk for which we got the stale locator exception.
            {

                final LinkedList<Split> splits = ndx.splitKeys(ts,
                        0/* fromIndex */, chunk.length/* toIndex */, chunk);

                for (Split split : splits) {

                    /*
                     * Note: In this case we may re-open an output buffer for
                     * the index partition. The circumstances under which this
                     * can occur are subtle. However, if data had already been
                     * assigned to the output buffer for the index partition and
                     * written through to the index partition and the output
                     * buffer closed because awaitAll() was invoked before we
                     * received the stale locator exception for an outstanding
                     * RMI, then it is possible for the desired output buffer to
                     * already be closed. In order for this condition to arise
                     * either the stale locator exception must have been
                     * received in response to a different index operation or
                     * the the client is not caching the index partition
                     * locators.
                     */

                    addToOutputBuffer(split, chunk, true/* reopen */);

                }

            }

            /*
             * Drain the rest of the buffered chunks from the sink, assigning
             * them to the sink(s).
             */
            {

                final IAsynchronousIterator<KVO<O>[]> itr = sink.src;

                while (itr.hasNext()) {

                    // next buffered chunk.
                    final KVO<O>[] a = itr.next();

                    // split the chunk.
                    final LinkedList<Split> splits = ndx.splitKeys(ts,
                            0/* fromIndex */, a.length/* toIndex */, a);

                    for (Split split : splits) {

                        /*
                         * Map onto the output buffers.
                         * 
                         * Again, note that we can re-open the output buffer in
                         * this case.
                         */

                        addToOutputBuffer(split, chunk, true/* reopen */);

                    }

                }

            }

            /*
             * Remove the buffer from the map
             * 
             * Note: This could cause a concurrent modification error if we are
             * awaiting the various output buffers to be closed. In order to
             * handle that code that modifies or traverses the [buffers] map
             * MUST be MUTEX or synchronized.
             */
            subtasks.remove(sink.locator.getPartitionId());

        } finally {

            lock.unlock();
            
        }

    }

    /**
     * Class drains a {@link BlockingBuffer} writing on a specific index
     * partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class IndexPartitionWriteTask<T extends IKeyArrayIndexProcedure, O, R, A>
            implements Callable<IndexPartitionWriteStats> {

        static protected transient final Logger log = Logger
                .getLogger(IndexPartitionWriteTask.class);

        /**
         * The master.
         */
        private final IndexWriteTask<T, O, R, A> master;

        /**
         * The index partition locator.
         */
        private final PartitionLocator locator;

        /**
         * The data service on which the index partition resides.
         */
        private final IDataService dataService;

        /**
         * The buffer on which the {@link IndexWriteTask} writes tuples to be
         * written onto the index partition associated with this task.
         */
        private final BlockingBuffer<KVO<O>[]> buffer;

        /**
         * The iterator draining the {@link #buffer}.
         */
        private final IAsynchronousIterator<KVO<O>[]> src;

        /**
         * The timestamp associated with the index view.
         */
        private final long ts;

        /**
         * The index partition identifier.
         */
        private final int partitionId;
        
        /**
         * The name of the index partition.
         */
        private final String indexPartitionName;

        /**
         * The statistics for writes on this index partition for the
         * {@link #master}.
         */
        private final IndexPartitionWriteStats stats;
        
        public IndexPartitionWriteTask(final IndexWriteTask<T, O, R, A> master,
                final PartitionLocator locator, final IDataService dataService,
                final BlockingBuffer<KVO<O>[]> buffer) {

            if (master == null)
                throw new IllegalArgumentException();

            if (locator == null)
                throw new IllegalArgumentException();

            if (dataService == null)
                throw new IllegalArgumentException();

            if (buffer == null)
                throw new IllegalArgumentException();

            this.master = master;

            this.locator = locator;

            this.dataService = dataService;

            this.buffer = buffer;
            
            this.src = buffer.iterator();

            this.ts = master.ndx.getTimestamp();

            this.partitionId = locator.getPartitionId();

            this.indexPartitionName = DataService.getIndexPartitionName(
                    master.ndx.getName(), partitionId);

            this.stats = master.stats.getStats(partitionId);

        }

        public IndexPartitionWriteStats call() throws Exception {

            try {

                /*
                 * Poll the iterator with a timeout to avoid deadlock with
                 * awaitAll().
                 * 
                 * Note: In order to ensure termination the subtask MUST poll
                 * the iterator with a timeout so that a subtask which was
                 * created in response to a StaleLocatorException during
                 * master.awaitAll() can close its own blocking buffer IF: (a)
                 * the top-level blocking buffer is closed; and (b) the
                 * subtask's blocking buffer is empty. This operation needs to
                 * be coordinated using the master's [lock], as does any
                 * operation which writes on the subtask's buffer. Otherwise we
                 * can wait forever for a subtask to complete. The subtask uses
                 * the [subtask] Condition signal the master when it is
                 * finished.
                 */
                while (true) {

                    master.halted();

                    // nothing available w/in timeout?
                    if (!src.hasNext(
                            // @todo config timeout
                            BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT,
                            BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT)) {

                        // are we done? should we close our buffer?
                        master.lock.lockInterruptibly();
                        try {
                            if (!buffer.isOpen() && !src.hasNext()) {
                                // We are done.
                                if (log.isInfoEnabled())
                                    log.info("No more data: " + this);
                                break;
                            }
                            if (master.src.isExhausted()) {
                                if (buffer.isEmpty()) {
                                    // close our buffer.
                                    buffer.close();
                                    if (log.isInfoEnabled())
                                        log.info("Closed buffer: " + this);
                                }
                            }
                        } finally {
                            master.lock.unlock();
                        }
                        continue;
                        
                    }

                    if(!nextChunk()) {
                        
                        // Done (handled a stale locator).
                        break;
                        
                    }
                    
                }

                // done.
                return stats;

            } finally {

                master.removeOutputBuffer(this);

                if (log.isInfoEnabled())
                    log.info("Done: " + stats);

                src.close();

            }

        }
        
        /**
         * Reads the next chunk from the buffer (there MUST be a chunk waiting
         * in the buffer).
         * 
         * @return <code>true</code> iff a {@link StaleLocatorException} was
         *         handled, in which case the task should exit immediately.
         * 
         * @throws IOException
         *             RMI error.
         * @throws ExecutionException
         * @throws InterruptedException
         */
        private boolean nextChunk() throws ExecutionException,
                InterruptedException, IOException {

            // A chunk in sorted order.
            final KVO<O>[] sourceChunk = src.next();

            /*
             * Remove duplicates in a caller specified manner (may be a NOP).
             */
            final KVO<O>[] chunk = master.duplicateRemover.filter(sourceChunk);

            if (chunk.length == 0) {

                // empty chunk after duplicate elimination (keep reading).
                return false;

            }

            // size of the chunk to be processed.
            final int chunkSize = chunk.length;

            /*
             * Change the shape of the data for RMI.
             */

            final boolean sendValues = master.ctor.sendValues();

            final byte[][] keys = new byte[chunkSize][];

            final byte[][] vals = sendValues ? new byte[chunkSize][] : null;

            for (int i = 0; i < chunkSize; i++) {

                keys[i] = chunk[i].key;

                if (sendValues)
                    vals[i] = chunk[i].val;

            }

            /*
             * Instantiate the procedure using the data from the chunk and
             * submit it to be executed on the DataService using an RMI call.
             */

            final long beginNanos = System.nanoTime();

            try {

                final T proc = master.ctor.newInstance(master.ndx,
                        0/* fromIndex */, chunkSize/* toIndex */, keys, vals);

                // submit and await Future
                final R result = ((Future<R>) dataService.submit(ts,
                        indexPartitionName, proc)).get();

                if (master.resultHandler != null) {

                    // aggregate results.
                    master.resultHandler.aggregate(result, new Split(locator,
                            0/* fromIndex */, chunkSize/* toIndex */));

                }

                if (log.isDebugEnabled())
                    log.debug(stats);

                // keep reading.
                return false;

            } catch (ExecutionException ex) {

                stats.elapsedNanos += System.nanoTime() - beginNanos;

                final StaleLocatorException cause = (StaleLocatorException) InnerCause
                        .getInnerCause(ex, StaleLocatorException.class);

                if (cause != null) {

                    /*
                     * Handle a stale locator.
                     * 
                     * Note: The master has to (a) close the output buffer for
                     * this subtask; (b) update its locator cache such that no
                     * more work is assigned to this output buffer; (c) re-split
                     * the chunk which failed with the StaleLocatorException;
                     * and (d) re-split all the data remaining in the output
                     * buffer since it all needs to go into different output
                     * buffer(s).
                     */
                    master.handleStaleLocator(this, chunk, cause);

                    // done.
                    return true;

                } else {

                    throw ex;

                }

            } finally {

                final long elapsedNanos = System.nanoTime() - beginNanos;

                // update the local statistics.
                stats.elementsOut += chunkSize;
                stats.chunksOut++;
                stats.elapsedNanos += elapsedNanos;

                // update the master's statistics.
                synchronized (master.stats) {
                    master.stats.chunksOut++;
                    master.stats.elementsOut += chunkSize;
                    master.stats.elapsedNanos += elapsedNanos;
                }

            }

        }

    } // IndexPartitionWriteTask

}
