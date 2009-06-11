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

import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.IDataService;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.IScaleOutClientIndex;

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
 * @param <H>
 *            The generic type of the value returned by {@link Callable#call()}
 *            for the master.
 * @param <O>
 *            The generic type for unserialized value objects.
 * @param <E>
 *            The generic type of the elements in the chunks stored in the
 *            {@link BlockingBuffer}.
 * @param <S>
 *            The generic type of the subtask implementation class.
 * @param <L>
 *            The generic type of the key used to lookup a subtask in the
 *            internal map (must be unique and must implement hashCode() and
 *            equals() per their contracts).
 * @param <HS>
 *            The generic type of the value returned by {@link Callable#call() }
 *            for the subtask.
 * @param <T>
 *            The generic type of the CTOR for the procedure used to write on
 *            the index.
 * @param <R>
 *            The type of the result from applying the index procedure to a
 *            single {@link Split} of data.
 * @param <A>
 *            The type of the aggregated result.
 */
abstract public class IndexWriteTask <//
H extends IndexAsyncWriteStats<L, HS>, //
O extends Object, //
E extends KVO<O>, //
S extends IndexPartitionWriteTask, //
L extends PartitionLocator, //
HS extends IndexPartitionWriteStats,//
T extends IKeyArrayIndexProcedure,//
R,//
A//
> extends AbstractMasterTask<H, E, S, L> {

    // from the ctor.
    protected final IScaleOutClientIndex ndx;

    protected final int sinkQueueCapacity;

    protected final int sinkChunkSize;

    protected final long sinkChunkTimeoutNanos;

    protected final IResultHandler<R, A> resultHandler;

    protected final IDuplicateRemover<O> duplicateRemover;

    protected final AbstractKeyArrayIndexProcedureConstructor<T> ctor;

    public String toString() {
        
        return getClass().getName() + "{index=" + ndx.getName() + ", open="
                + buffer.isOpen() + ", ctor=" + ctor + "}";
        
    }

    /**
     * {@inheritDoc}
     * 
     * @param ndx
     *            The client's view of the scale-out index.
     * @param sinkIdleTimeoutNanos
     *            The time in nanoseconds after which an idle sink will be
     *            closed. Any buffered writes are flushed when the sink is
     *            closed. This must be GTE the <i>sinkChunkTimeout</i>
     *            otherwise the sink will decide that it is idle when it was
     *            just waiting for enough data to prepare a full chunk.
     * @param sinkPollTimeoutNanos
     *            The time in nanoseconds that the {@link AbstractSubtask sink}
     *            will wait inside of the {@link IAsynchronousIterator} when it
     *            polls the iterator for a chunk. This value should be
     *            relatively small so that the sink remains responsible rather
     *            than blocking inside of the {@link IAsynchronousIterator} for
     *            long periods of time.
     * @param sinkQueueCapacity
     *            The capacity of the internal queue for the per-sink output
     *            buffer.
     * @param sinkChunkSize
     *            The desired size of the chunks written that will be written by
     *            the {@link AbstractSubtask sink}.
     * @param sinkChunkTimeoutNanos
     *            The maximum amount of time in nanoseconds that a sink will
     *            combine smaller chunks so that it can satisify the desired
     *            <i>sinkChunkSize</i>.
     * @param duplicateRemover
     *            Removes duplicate key-value pairs from the (optional).
     * @param ctor
     *            The ctor instantiates an {@link IIndexProcedure} for each
     *            chunk written on an index partition.
     * @param resultHandler
     *            Aggregates results across the individual index partition write
     *            operations (optional).
     * @param stats
     *            The index statistics object.
     * @param buffer
     *            The buffer on which the application will write.
     */
    public IndexWriteTask(final IScaleOutClientIndex ndx,
            final long sinkIdleTimeoutNanos,
            final long sinkPollTimeoutNanos,
            final int sinkQueueCapacity,
            final int sinkChunkSize,
            final long sinkChunkTimeoutNanos,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor,
            final IResultHandler<R, A> resultHandler,
            final H stats,
            final BlockingBuffer<E[]> buffer) {

        super(stats, buffer, sinkIdleTimeoutNanos, sinkPollTimeoutNanos);
        
        if (ndx == null)
            throw new IllegalArgumentException();

        if (sinkQueueCapacity <= 0)
            throw new IllegalArgumentException();

        if (sinkChunkSize <= 0)
            throw new IllegalArgumentException();
        
        if (sinkChunkTimeoutNanos <= 0)
            throw new IllegalArgumentException();
        
//        if (duplicateRemover == null)
//            throw new IllegalArgumentException();

        if (ctor == null)
            throw new IllegalArgumentException();

//      if (resultHandler == null)
//      throw new IllegalArgumentException();

        this.ndx = ndx;

        this.sinkQueueCapacity = sinkQueueCapacity;

        this.sinkChunkSize = sinkChunkSize;
        
        this.sinkChunkTimeoutNanos = sinkChunkTimeoutNanos;
        
        this.resultHandler = resultHandler;

        this.duplicateRemover = duplicateRemover;

        this.ctor = ctor;
        
    }

//    * This implementation grabs the {@link AbstractMasterTask#lock}.
//    * <p>
//    * Note: Locking is required here so that the splits are computed coherently
//    * with respect to the buffers to which they will be assigned. Otherwise,
//    * for example, a set of splits computed before a MOVE is noticed will
//    * continue to target the pre-MOVE index partition after sink for that index
//    * partition has noticed the MOVE, closed its buffer, and redirected its
//    * buffered output to another sink. If we did not hold the lock across the
//    * split/addToOutputBuffer operation then the master would discover that the
//    * output buffer assigned by the split had since been closed (due to a
//    * redirect).
    /**
     * 
     * @todo The test suite does not demonstrate this problem which makes
     *       detection difficult. See
     *       {@link TestMasterTaskWithRedirect#test_redirectStressTest()}.
     * 
     * @todo The test suite failed to demonstrate a problem where reopen was
     *       false and there had been an idle timeout for a sink and the master
     *       attempted to write another chunk onto that sink. The problem has
     *       since been repaired in the code, but the test suite still does not
     *       detect the issue.
     * 
     * @todo the requirement to hold the lock across the add of the splits could
     *       stifle throughput when writes on some locators are slower and their
     *       output buffers fill up.
     */
    protected void handleChunk(final E[] a, final boolean reopen)
            throws InterruptedException {

        final long begin = System.nanoTime();
        
//        lock.lockInterruptibly();
        try {

            final long beforeSplit = System.nanoTime();
            
            // Split the ordered chunk.
            final LinkedList<Split> splits = ndx.splitKeys(ndx.getTimestamp(),
                    0/* fromIndex */, a.length/* toIndex */, a);
            
            final long splitNanos = System.nanoTime() - beforeSplit;

            synchronized (stats) {

                stats.elapsedSplitChunkNanos += splitNanos;

            }
            
            // Break the chunk into the splits
            for (Split split : splits) {

                halted();

                addToOutputBuffer((L) split.pmd, a, split.fromIndex,
                        split.toIndex, reopen);

            }

        } finally {

//            lock.unlock();
            
            synchronized (stats) {
             
                stats.handledChunkCount++;
                
                stats.elapsedHandleChunkNanos += System.nanoTime() - begin;
                
            }
            
        }

    }

//    /**
//     * The master has to: (a) update its locator cache such that no more work is
//     * assigned to this output buffer; (b) re-split the chunk which failed with
//     * the StaleLocatorException; and (c) re-split all the data remaining in the
//     * output buffer since it all needs to go into different output buffer(s).
//     * <p>
//     * Note: The handling of a {@link StaleLocatorException} MUST be MUTEX with
//     * respect to adding data to an output buffer using
//     * {@link #addToOutputBuffer(Split, KVO[])}. This provides a guarantee that
//     * no more data will be added to a given output buffer once this method
//     * holds the monitor. Since a single thread drains any given output buffer
//     * we will never observe more than one {@link StaleLocatorException} for a
//     * given index partition within the context of the same
//     * {@link IndexWriteTask}. Together this allows us to decisively handle the
//     * {@link StaleLocatorException} and close out the output buffer on which it
//     * was received.
//     * 
//     * @param sink
//     *            The class draining the output buffer.
//     * @param chunk
//     *            The chunk which it was writing when it received the
//     *            {@link StaleLocatorException}.
//     * @param cause
//     *            The {@link StaleLocatorException}.
//     */
//    @SuppressWarnings("unchecked")
//    protected void handleStaleLocator(final S sink, final E[] chunk,
//            final StaleLocatorException cause) throws InterruptedException {
//
//        if (sink == null)
//            throw new IllegalArgumentException();
//        
//        if (chunk == null)
//            throw new IllegalArgumentException();
//        
//        if (cause == null)
//            throw new IllegalArgumentException();
//
//        lock.lockInterruptibly();
//        try {
//
//            stats.redirectCount++;
//
//            /*
//             * Notify the client so it can refresh the information for this
//             * locator.
//             */
//            ndx.staleLocator(ndx.getTimestamp(), (L) sink.locator, cause);
//
//            /*
//             * Redirect the chunk and anything in the buffer to the appropriate
//             * output sinks.
//             */
//            handleRedirect(sink, chunk);
//
//            /*
//             * Remove the buffer from the map
//             * 
//             * Note: This could cause a concurrent modification error if we are
//             * awaiting the various output buffers to be closed. In order to
//             * handle that code that modifies or traverses the [buffers] map
//             * MUST be MUTEX or synchronized.
//             */
//            removeOutputBuffer((L) sink.locator, sink);
//
//        } finally {
//
//            lock.unlock();
//            
//        }
//
//    }

    @SuppressWarnings("unchecked")
    @Override
    protected S newSubtask(final L locator, final BlockingBuffer<E[]> out) {

        final IDataService dataService = ndx.getDataService(locator);
        
        if (dataService == null)
            throw new RuntimeException("DataService not found: "
                    + locator.getDataServiceUUID());
        
        return (S) new IndexPartitionWriteTask(this, locator, dataService, out);
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The queue capacity, chunk size and chunk timeout are taken from the ctor
     * parameters.
     */
    @Override
    protected BlockingBuffer<E[]> newSubtaskBuffer() {
        
        return new BlockingBuffer<E[]>(//
                // @todo config deque vs queue (deque combines on add() as well)
                new LinkedBlockingDeque<E[]>(sinkQueueCapacity),//
//                new ArrayBlockingQueue<E[]>(sinkQueueCapacity), //
                sinkChunkSize,// 
                sinkChunkTimeoutNanos,//
                TimeUnit.NANOSECONDS,//
                true // ordered
        );
        
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Future<HS> submitSubtask(final S subtask) {

        return (Future<HS>) ndx.getFederation().getExecutorService().submit(
                subtask);
        
    }

    /**
     * Concrete master hides most of the generic types leaving you with only
     * those that are meaningfully parameterize for applications using the
     * streaming write API.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @param <T>
     *            The generic type of the CTOR for the procedure used to write
     *            on the index.
     * @param <O>
     *            The generic type for unserialized value objects.
     * @param <R>
     *            The type of the result from applying the index procedure to a
     *            single {@link Split} of data.
     * @param <A>
     *            The type of the aggregated result.
     */
    public static class M<T extends IKeyArrayIndexProcedure, O, R, A> extends
            IndexWriteTask<//
            IndexAsyncWriteStats<PartitionLocator, IndexPartitionWriteStats>, // H
            O, // O
            KVO<O>, // E
            IndexPartitionWriteTask, // S
            PartitionLocator, // L
            IndexPartitionWriteStats, // HS
            T, //
            R, //
            A  //
            > {

        /**
         * {@inheritDoc}
         */
        public M(
                final IScaleOutClientIndex ndx,
                final long sinkIdleTimeoutNanos,
                final long sinkPollTimeoutNanos,
                final int sinkQueueCapacity,
                final int sinkChunkSize,
                final long sinkChunkTimeoutNanos,
                final IDuplicateRemover<O> duplicateRemover,
                final AbstractKeyArrayIndexProcedureConstructor<T> ctor,
                final IResultHandler<R, A> resultHandler,
                final IndexAsyncWriteStats<PartitionLocator, IndexPartitionWriteStats> stats,
                final BlockingBuffer<KVO<O>[]> buffer) {
            
            super(ndx, sinkIdleTimeoutNanos, sinkPollTimeoutNanos,
                    sinkQueueCapacity, sinkChunkSize, sinkChunkTimeoutNanos,
                    duplicateRemover, ctor, resultHandler, stats, buffer);

        }

    }

}
