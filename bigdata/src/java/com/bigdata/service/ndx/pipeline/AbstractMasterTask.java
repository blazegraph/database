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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.keys.KVO;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.Split;
import com.bigdata.util.concurrent.AbstractHaltableProcess;

/**
 * Abstract base class for a master task which consumes chunks of elements
 * written onto a {@link BlockingBuffer} and distributes those chunks to
 * subtasks according to some abstraction which is not defined by this class.
 * 
 * @param <H>
 *            The generic type of the value returned by {@link Callable#call()}
 *            for the master.
 * @param <E>
 *            The generic type of the elements in the chunks stored in the
 *            {@link BlockingBuffer}.
 * @param <S>
 *            The generic type of the subtask implementation class.
 * @param <L>
 *            The generic type of the key used to lookup a subtask in the
 *            internal map (must be unique and must implement hashCode() and
 *            equals() per their contracts).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractMasterTask<//
H extends AbstractMasterStats<L, ? extends AbstractSubtaskStats>,//
E,//
S extends AbstractSubtask,//
L>//
        extends AbstractHaltableProcess implements Callable<H> {

    static protected transient final Logger log = Logger
            .getLogger(AbstractMasterTask.class);

    /**
     * The top-level buffer on which the application is writing.
     */
    protected final BlockingBuffer<E[]> buffer;

    /**
     * The iterator draining the {@link #buffer}.
     * <p>
     * Note: DO NOT close this iterator from within {@link #call()} as that
     * would cause this task to interrupt itself!
     */
    protected final IAsynchronousIterator<E[]> src;

    /**
     * Map from the index partition identifier to the open subtask handling
     * writes bound for that index partition.
     * <p>
     * Note: This map must be protected against several kinds of concurrent
     * access using the {@link #lock}.
     */
    private final Map<L, S> subtasks;

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
    protected final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition is signaled by a subtask when it is finished. This is used by
     * {@link #awaitAll()} to while waiting for subtasks to complete. If all
     * subtasks in {@link #subtasks} are complete when this signal is received
     * then the master may terminate.
     */
    protected final Condition subtask = lock.newCondition();

    /**
     * Statistics for this (and perhaps other) masters.
     */
    protected final H stats;
    
    public AbstractMasterTask(final H stats,
            final BlockingBuffer<E[]> buffer) {

        if (stats == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        this.stats = stats;
        
        this.buffer = buffer;

        this.src = buffer.iterator();

        this.subtasks = new LinkedHashMap<L, S>();

    }

    public H call() throws Exception {

        try {

            while (src.hasNext()) {

                halted();
                
                final E[] a = src.next();

                // empty chunk?
                if (a.length == 0)
                    continue;
                
                synchronized (stats) {
                    // update the master stats.
                    stats.chunksIn++;
                    stats.elementsIn += a.length;
                }

                nextChunk(a, false/* reopen */);
                
            }

            awaitAll();

        } catch (Throwable t) {

            log.error("Cancelling: job=" + this + ", cause=" + t, t);

            try {
                cancelAll(true/* mayInterruptIfRunning */);
            } catch (Throwable t2) {
                log.error(t2);
            }

            throw new RuntimeException(t);

        }

        // Done.
        return stats;

    }

    /**
     * Handle the next chunk of elements from the {@link #buffer}.
     * 
     * @param chunk
     *            A chunk.
     * @param reopen
     *            When <code>false</code> it is an error if the output buffer
     *            has been closed. When <code>true</code> the output buffer
     *            will be (re-)opened as necessary. This will be
     *            <code>false</code> when invoked by {@link #call()} (since
     *            the output buffers are not closed until the master's buffer is
     *            closed) and should be <code>true</code> if you are handling
     *            redirects.
     */
    abstract protected void nextChunk(E[] chunk, boolean reopen)
            throws InterruptedException;
    
    /**
     * Redirects a chunk to the appropriate sink(s) and then drains the
     * sink, redirecting all chunks which can be read from that sink to the
     * appropriate sink(s). The <i>sink</i> is closed so that no further
     * data may be written on it.
     * 
     * @param sink
     *            The sink whose output needs to be redirected.
     * @param chunk
     *            The chunk which the sink was processing when it discovered
     *            that it need to redirect its outputs to a different sink
     *            (that is, a chunk which it had already read from its
     *            buffer and hence which needs to be redirected now).
     *            
     * @throws InterruptedException
     */
    protected void handleRedirect(final S sink, final E[] chunk)
            throws InterruptedException {

        synchronized (stats) {
            stats.redirectCount++;
        }
        
        /*
         * Close the output buffer for this sink - nothing more may written
         * onto it now that we have seen the StaleLocatorException.
         */
        sink.buffer.close();

        /*
         * Handle the chunk for which we got the stale locator exception.
         * 
         * Note: In this case we may re-open an output buffer for the index
         * partition. The circumstances under which this can occur are
         * subtle. However, if data had already been assigned to the output
         * buffer for the index partition and written through to the index
         * partition and the output buffer closed because awaitAll() was
         * invoked before we received the stale locator exception for an
         * outstanding RMI, then it is possible for the desired output
         * buffer to already be closed. In order for this condition to arise
         * either the stale locator exception must have been received in
         * response to a different index operation or the the client is not
         * caching the index partition locators.
         */
        nextChunk(chunk, true/* reopen */);

        /*
         * Drain the rest of the buffered chunks from the sink, assigning
         * them to the sink(s). Again, we will 'reopen' the output buffer if
         * it has been closed.
         */
        {

            final IAsynchronousIterator<E[]> itr = sink.src;

            while (itr.hasNext()) {

                nextChunk(itr.next(), true/* reopen */);

            }

        }

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
     */
    private void awaitAll() throws InterruptedException, ExecutionException {

        lock.lockInterruptibly();
        try {

            // close buffer - nothing more may be written on the master.
            buffer.close();

            while (true) {

                halted();

                final AbstractSubtask[] sinks = subtasks.values().toArray(
                        new AbstractSubtask[0]);

                if (sinks.length == 0) {

                    // Done.
                    break;

                }

                if (log.isDebugEnabled())
                    log.debug("Waiting for " + sinks.length + " subtasks : "
                            + this);

                /*
                 * Wait for the sinks to complete.
                 */
                for (AbstractSubtask sink : sinks) {

                    final Future f = sink.buffer.getFuture();

                    if (f.isDone()) {

                        // check the future (can throw exception).
                        f.get();

                    }

                }

                /*
                 * Yield the lock and wait up to a timeout for a sink to
                 * complete.
                 * 
                 * @todo config
                 */
                subtask.await(BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT,
                        BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT);

            } // continue

            if (log.isInfoEnabled())
                log.info("All subtasks are done: " + this);
            
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
    private void cancelAll(final boolean mayInterruptIfRunning) {

        lock.lock();
        try {

            log.warn("Cancelling job: " + this);

            /*
             * Close the buffer (nothing more may be written).
             * 
             * Note: We DO NOT close the [src] iterator since that would cause
             * this task to interrupt itself!
             */
            buffer.close();

            for (S sink : subtasks.values()) {

                final Future f = sink.buffer.getFuture();

                if (!f.isDone()) {

                    f.cancel(mayInterruptIfRunning);

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
     *            The locator (unique subtask key).
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
    private BlockingBuffer<E[]> getOutputBuffer(final L locator,
            final boolean reopen) {

        if (locator == null)
            throw new IllegalArgumentException();

        if (!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        // operation not allowed if halted.
        halted();

        S sink = subtasks.get(locator);

        if (reopen && sink != null && !sink.buffer.isOpen()) {

            // wait for the sink to terminate normally.
            awaitSink(sink);

        }

        if (sink == null || reopen) {

            if (log.isDebugEnabled())
                log.debug("Creating output buffer: " + locator);

            final BlockingBuffer<E[]> out = newSubtaskBuffer();

            sink = newSubtask(locator, out);

            final Future<? extends AbstractSubtaskStats> future = submitSubtask(sink);

            out.setFuture(future);

            subtasks.put(locator, sink);

            synchronized(stats) {

                stats.subtaskStartCount++;
                
            }

        }

        return sink.buffer;

    }

    /**
     * Factory for a new buffer for a subtask.
     */
    abstract protected BlockingBuffer<E[]> newSubtaskBuffer();
    
    /**
     * Factory for a new subtask.
     * 
     * @param locator
     *            The unique key for the subtask.
     * @param out
     *            The {@link BlockingBuffer} on which the master will write for
     *            that subtask.
     *            
     * @return The subtask.
     */
    abstract protected S newSubtask(L locator, BlockingBuffer<E[]> out);
    
    /**
     * Submit the subtask to an {@link Executor}.
     * 
     * @param subtask
     *            The subtask.
     * 
     * @return The {@link Future}.
     */
    abstract protected Future<? extends AbstractSubtaskStats> submitSubtask(S subtask);
    
    /**
     * This is invoked when there is already a sink for that index partition but
     * it has been closed. Poll the future until the existing sink is finished
     * before putting the new sink into play. This ensures that we can verify
     * the Future completes normally. Other sinks (except the one(s) that is
     * waiting on this Future) will continue to drain normally.
     * 
     * @throws IllegalMonitorStateException
     *             unless the caller holds the {@link #lock}
     * @throws IllegalStateException
     *             unless the {@link #buffer} is closed.
     */
    protected void awaitSink(final S sink) {

        if(!lock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        if(sink.buffer.isOpen())
            throw new IllegalStateException();

        final Future f = sink.buffer.getFuture();
        final long begin = System.nanoTime();
        long lastNotice = begin;
        try {

            while (!f.isDone()) {

                subtask.await(
                        // @todo config
                        BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT,
                        BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT);

                final long now = System.nanoTime();
                final long elapsed = now - lastNotice;

                if (elapsed >= 1000) {
                    log.warn("Waiting on sink: elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(elapsed)
                            + ", sink=" + sink);
                }

            }

            // test the future.
            f.get();

        } catch (Throwable t) {

            halt(t);

            throw new RuntimeException(t);

        }

    }

    /**
     * Removes the output buffer (unless it has been replaced by another output
     * buffer associated with a different sink).
     * 
     * @param sink
     *            The sink.
     */
    protected void removeOutputBuffer(final L locator,
            final AbstractSubtask sink) {

        if (sink == null)
            throw new IllegalArgumentException();

        lock.lock();
        try {

            final S t = subtasks.get(locator);

            if (t == sink) {

                /*
                 * Remove map entry IFF it is for the same reference.
                 */

                subtasks.remove(locator);

                if (log.isDebugEnabled())
                    log.debug("Removed output buffer: " + locator);

            }

            /*
             * Note: increment counter regardless of whether or not the
             * reference was the same since the specified sink is now done.
             */
            synchronized (stats) {

                stats.subtaskEndCount++;

            }

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
    protected void addToOutputBuffer(final L locator, final E[] a,
            final int fromIndex, final int toIndex, final boolean reopen)
            throws InterruptedException {

        final int n = (toIndex - fromIndex);

        if (n == 0)
            return;
        
        lock.lockInterruptibly();
        try {

            /*
             * Make a dense chunk for this split.
             */

            final E[] b = (E[]) java.lang.reflect.Array.newInstance(a
                    .getClass().getComponentType(), n);

            System.arraycopy(a, fromIndex, b, 0, n);

            // add the dense split to the appropriate output buffer.
            getOutputBuffer(locator, reopen).add(b);

        } finally {

            lock.unlock();

        }

    }

}
