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

import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.IDataService;
import com.bigdata.service.IScaleOutClientIndex;
import com.bigdata.service.Split;

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
 * 
 * @todo throughput could probably be increased by submitting a sink to the data
 *       service which received chunks from client(s) [use a factory for this
 *       similar to the pipeline joins?], accumulating chunks, and merge sorted
 *       those chunks before performing a sustained index write. However, this
 *       might go too far and cause complications with periodic overflow.
 */
abstract public class IndexWriteTask <//
H extends IndexWriteStats<L, HS>, //
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

    protected final int subtaskQueueCapacity;

    protected final IResultHandler<R, A> resultHandler;

    protected final IDuplicateRemover<O> duplicateRemover;

    protected final AbstractKeyArrayIndexProcedureConstructor<T> ctor;

    public String toString() {
        
        return getClass().getName() + "{index=" + ndx.getName() + ", open="
                + buffer.isOpen() + ", ctor=" + ctor + "}";
        
    }

    public IndexWriteTask(final IScaleOutClientIndex ndx,
            final int subtaskQueueCapacity,
            final IResultHandler<R, A> resultHandler,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor,
            final H stats,
            final BlockingBuffer<E[]> buffer) {

        super(stats, buffer);
        
        if (ndx == null)
            throw new IllegalArgumentException();

        if (subtaskQueueCapacity <= 0)
            throw new IllegalArgumentException();

//        if (resultHandler == null)
//            throw new IllegalArgumentException();

//        if (duplicateRemover == null)
//            throw new IllegalArgumentException();

        if (ctor == null)
            throw new IllegalArgumentException();

        if (stats == null)
            throw new IllegalArgumentException();
        
        if (buffer == null)
            throw new IllegalArgumentException();

        this.ndx = ndx;

        this.subtaskQueueCapacity = subtaskQueueCapacity;

        this.resultHandler = resultHandler;

        this.duplicateRemover = duplicateRemover;

        this.ctor = ctor;
        
    }

    /**
     * This implementation grabs the {@link AbstractMasterTask#lock}.
     * <p>
     * Note: Locking is required here so that the splits are computed coherently
     * with respect to the buffers to which they will be assigned. Otherwise,
     * for example, a set of splits computed before a MOVE is noticed will
     * continue to target the pre-MOVE index partition after sink for that index
     * partition has noticed the MOVE, closed its buffer, and redirected its
     * buffered output to another sink. If we did not hold the lock across the
     * split/addToOutputBuffer operation then the master would discover that the
     * output buffer assigned by the split had since been closed (due to a
     * redirect).
     * 
     * @TODO The test suite does not demonstrate this problem which makes
     *       detection difficult. See
     *       {@link TestMasterTaskWithRedirect#test_redirectStressTest()}.
     * 
     * @todo the requirement to hold the lock across the add of the splits could
     *       stifle throughput when writes on some locators are slower and their
     *       output buffers fill up.
     */
    protected void nextChunk(final E[] a, final boolean reopen)
            throws InterruptedException {

        lock.lockInterruptibly();
        try {

            // Split the ordered chunk.
            final LinkedList<Split> splits = ndx.splitKeys(ndx.getTimestamp(),
                    0/* fromIndex */, a.length/* toIndex */, a);

            // Break the chunk into the splits
            for (Split split : splits) {

                halted();

                addToOutputBuffer((L) split.pmd, a, split.fromIndex,
                        split.toIndex, reopen);

            }

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
     * Note: The handling of a {@link StaleLocatorException} MUST be MUTEX with
     * respect to adding data to an output buffer using
     * {@link #addToOutputBuffer(Split, KVO[])}. This provides a guarentee that
     * no more data will be added to a given output buffer once this method
     * holds the monitor. Since the output buffer is single threaded we will
     * never observe more than one {@link StaleLocatorException} for a given
     * index partition within the context of the same {@link IndexWriteTask}.
     * Together this allows us to decisively handle the
     * {@link StaleLocatorException} and close out the output buffer on which it
     * was received.
     * 
     * @param sink
     *            The class draining the output buffer.
     * @param chunk
     *            The chunk which it was writing when it received the
     *            {@link StaleLocatorException}.
     * @param cause
     *            The {@link StaleLocatorException}.
     */
    @SuppressWarnings("unchecked")
    protected void handleStaleLocator(final S sink, final E[] chunk,
            final StaleLocatorException cause) throws InterruptedException {

        if (sink == null)
            throw new IllegalArgumentException();
        
        if (chunk == null)
            throw new IllegalArgumentException();
        
        if (cause == null)
            throw new IllegalArgumentException();

        lock.lockInterruptibly();
        try {

            stats.redirectCount++;

            /*
             * Notify the client so it can refresh the information for this
             * locator.
             */
            ndx.staleLocator(ndx.getTimestamp(), (L) sink.locator, cause);

            /*
             * Redirect the chunk and anything in the buffer to the appropriate
             * output sinks.
             */
            handleRedirect(sink, chunk);

            /*
             * Remove the buffer from the map
             * 
             * Note: This could cause a concurrent modification error if we are
             * awaiting the various output buffers to be closed. In order to
             * handle that code that modifies or traverses the [buffers] map
             * MUST be MUTEX or synchronized.
             */
            removeOutputBuffer((L) sink.locator, sink);

        } finally {

            lock.unlock();
            
        }

    }

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
     * @todo configure chunk size and timeout. could just take the values from
     *       {@link AbstractMasterTask#buffer}.
     */
    @Override
    protected BlockingBuffer<E[]> newSubtaskBuffer() {
        
        return new BlockingBuffer<E[]>(//
                new ArrayBlockingQueue<E[]>(subtaskQueueCapacity), //
                BlockingBuffer.DEFAULT_CONSUMER_CHUNK_SIZE,// 
                BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT,//
                BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT,//
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
            IndexWriteStats<PartitionLocator, IndexPartitionWriteStats>, // H
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
         * @param ndx
         * @param subtaskQueueCapacity
         * @param resultHandler
         * @param duplicateRemover
         * @param ctor
         * @param stats
         * @param buffer
         */
        public M(
                IScaleOutClientIndex ndx,
                int subtaskQueueCapacity,
                IResultHandler<R, A> resultHandler,
                IDuplicateRemover<O> duplicateRemover,
                AbstractKeyArrayIndexProcedureConstructor<T> ctor,
                IndexWriteStats<PartitionLocator, IndexPartitionWriteStats> stats,
                BlockingBuffer<KVO<O>[]> buffer) {
            
            super(ndx, subtaskQueueCapacity, resultHandler, duplicateRemover,
                    ctor, stats, buffer);

        }
        
    }

}
