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

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.keys.KVO;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.ISplitter;
import com.bigdata.util.concurrent.AbstractHaltableProcess;

/**
 * Abstract base class for a master task which consumes chunks of elements
 * written onto a {@link BlockingBuffer} and distributes those chunks to
 * subtasks according to some abstraction which is not defined by this class.
 * 
 * <h3>Design discussion</h3>
 * 
 * The asynchronous write API exposes a blocking buffer to the application which
 * accepts concurrent writes of {@link KVO}[] chunks, in which each {@link KVO}
 * represents a tuple to be written on a scale-out index. The buffer is drained
 * by a master task, which transfers chunks to sinks tasks, each of which writes
 * on a specific index partition.
 * <p>
 * The master is provisioned with a CTOR which is used to convert the
 * {@link KVO}s into <code>unsigned byte[]</code> <i>keys</i> and
 * <code>byte[]</code> <i>values</i> for writes on the scale-out index. The
 * master may be provisioned with a mechanism to filter out duplicate tuples.
 * <p>
 * Writes on the index partitions are asynchronous with respect to the
 * application. However, a {@link KVOC} / {@link KVOLatch} combination can be
 * used to coordinate notification when some set of tuples of interest have been
 * successfully written onto the scale-out index. This combination can also be
 * used to pass back values from the write operation if they are assigned by
 * side-effect onto the {@link KVO#obj} reference.
 * <p>
 * The asynchronous write implementation is divided into a master, with an input
 * queue and a redirect queue, and sinks, each of which has an input queue and
 * writes chunks onto a specific index partition for the scale-out index. The
 * input queues for the master and the sinks are bounded. The redirect input
 * queue is unbounded. The master and each sink is assigned its own worker
 * thread.
 * <p>
 * The master transfers chunks from its input queue to the sinks. It polls the
 * redirect queue for a chunk. If that queue was empty, then it polls a chunk
 * from the input queue. If no chunks are available, it needs to check again.
 * The master stops polling the input queue when the input queue is closed, but
 * it continues to drain the redirect queue until all sinks are done or the
 * master is canceled.
 * <p>
 * Note: The requirement for polling arises because: (a) we are not coordinating
 * signals for the arrival of a chunk on the input or redirect queues; and (b) a
 * chunk can be redirected at any time if there is an outstanding write by a
 * sink on an index partition.
 * <p>
 * The atomic decision to terminate the master is made using a {@link #lock}.
 * The lock is specific to the life cycle of the sinks. The lock is held when a
 * sink is created. When a sink terminates, its last action is to grab the lock
 * and signal the {@link #subtaskDone} {@link Condition}. The master terminates
 * when, while holding the lock, it observes that no sinks are running AND the
 * redirect queue is empty. Since chunks are placed onto the redirect queue by
 * sinks (and there are no sinks running) and by the master (which is not
 * issuing a redirect since it is running its termination logic) these criteria
 * are sufficient for termination. However, the sink must ensure that its buffer
 * is closed before it terminates, even if it terminates by exception, so that
 * an attempt to transfer a chunk to the sink will not block forever.
 * <p>
 * Once the master is holding a chunk, it splits the chunk into a set of dense
 * chunks correlated with the locators of the index partitions on which those
 * chunks will be written. The split operation is NOT atomic, but it is
 * consistent in the following sense. If a {@link Split} is identified based on
 * old information, then the chunk will be directed to an index partition which
 * no longer exists. An attempt to write on that index partition will result in
 * a stale locator exception, which is handled.
 * <p>
 * Once the chunk has been split, the split chunks are transferred to the
 * appropriate sink(s). Since the master is not holding any locks, a blocking
 * put() may be used to transfer the chunk to sink.
 * <p>
 * The sink drain chunks from its input queue. If the input queue is empty and
 * the idle timeout expires before a chunk is transferred to the input queue,
 * then the sink will be asynchronously closed and an
 * {@link IdleTimeoutException} will be set on the input queue. If the master
 * attempts to transfer a chunk to the sink's input queue after the idle
 * timeout, then an exception wrapping the idle timeout exception will be
 * thrown. The master handles the wrapped idle timeout exception by re-opening
 * the sink and will retry the transfer of the chunk to the (new) sink's input
 * queue. After the sink closes it's input queue by an idle timeout, it will
 * continue to drain the input queue until it is empty, at which point the sink
 * will terminate (this handles the case where the master concurrently
 * transferred a chunk to the sink's input queue before it was closed by the
 * idle time out).
 * <p>
 * The sink combines chunks drained from its input queue until the target chunk
 * size for a write is achieved or until the chunk timeout for the sink is
 * exceeded. The sink then writes on the index partition corresponding to its
 * locator. This write occurs in the thread assigned to the sink and the sink
 * will block during the write request.
 * <p>
 * If a stale locator exception is received by the sink in response to a write,
 * it will: (a) notify the client of the stale locator exception; (b) close the
 * input queue, setting the stale locator exception as the cause; (c) place the
 * chunk for that write onto the master's (unbounded) redirect queue; and (d)
 * drain its input queue and transfer the chunks to the master's redirect queue.
 * <p>
 * If the master attempts to transfer a chunk to the input queue for a sink
 * which has closed its input queue in response to a stale locator exception,
 * then an exception will be thrown with the stale locator exception as the
 * inner cause. The master will trap that exception and place the chunk on the
 * redirect queue instead.
 * <p>
 * If the sink RMI is successful, the sink will invoke the optional result
 * handler and touch each tuple in the chunk using KVO#done(). These protocols
 * can be used to pass results from asynchronous writes back to the application.
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
 * @see ISplitter
 * 
 */
public abstract class AbstractMasterTask<//
H extends AbstractMasterStats<L, ? extends AbstractSubtaskStats>, //
E, //
S extends AbstractSubtask,//
L>//
        extends AbstractHaltableProcess implements Callable<H>, IMasterTask<E,H> {

    static protected transient final Logger log = Logger
            .getLogger(AbstractMasterTask.class);

    /**
     * The top-level buffer on which the application is writing.
     */
    protected final BlockingBuffer<E[]> buffer;

    /**
     * A unbounded queue of chunks for which a {@link StaleLocatorException} was
     * received. When this buffer is not empty, it is drained by preference over
     * the {@link #buffer}.
     * <p>
     * This design allows us to avoid deadlocks when a sink is full and the
     * master is blocked attempting to add another chunk to that sink. If a
     * {@link StaleLocatorException} is thrown for the outstanding write by that
     * sink, then this situation would deadlock since the {@link #lock} is
     * already held and the sink is unable to drain.
     */
    protected final BlockingQueue<E[]> redirectQueue = new LinkedBlockingQueue<E[]>(/* unbounded */);

    public BlockingBuffer<E[]> getBuffer() {

        return buffer;
        
    }
    
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
    private final ConcurrentHashMap<L, S> sinks;

    /**
     * Maps an operation across the subtasks.
     * 
     * @param op
     *            The operation, which should be light weight
     * 
     * @throws InterruptedException
     */
    public void mapOperationOverSubtasks(final SubtaskOp<S> op)
            throws InterruptedException, Exception {

//        /*
//         * Note: by grabbing the values as an array we avoid the possibility
//         * that the operation might request any locks and therefore avoid the
//         * possibility that map() could cause a deadlock based on the choice of
//         * the op to be mapped.
//         */
//        final AbstractSubtask[] sinks;
//        lock.lockInterruptibly();
//        try {
//
//            sinks = subtasks.values().toArray(new AbstractSubtask[0]);
//
//        } finally {
//
//            lock.unlock();
//
//        }
//
//        for (S subtask : subtasks.values()) {
//
//            op.call(subtask);
//
//        }
        final Iterator<S> itr = sinks.values().iterator();
        while(itr.hasNext()) {
            op.call(itr.next());
        }

    }

    /**
     * Lock used for sink life cycle events <em>only</em>.
     */
    protected final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition is signaled by a subtask when it is finished. This is used by
     * {@link #awaitAll()} to while waiting for subtasks to complete. If all
     * subtasks in {@link #subtasks} are complete when this signal is received
     * then the master may terminate.
     */
    protected final Condition subtaskDone = lock.newCondition();

    /**
     * Statistics for this (and perhaps other) masters.
     */
    protected final H stats;
    
    public H getStats() {
        
        return stats;
        
    }

    /**
     * The timeout in nanoseconds before closing an idle output sink.
     */
    protected final long sinkIdleTimeoutNanos;

    /**
     * The time in nanoseconds that the {@link AbstractSubtask sink} will wait
     * inside of the {@link IAsynchronousIterator} when it polls the iterator
     * for a chunk. If this value is too large then the sink will block for
     * noticeable lengths of time and will be less responsive to interrupts.
     * Something in the 10s of milliseconds is appropriate.
     */
    protected final long sinkPollTimeoutNanos;

    /**
     * 
     * @param stats
     *            Statistics for the master.
     * @param buffer
     *            The buffer on which data is written by the application and
     *            from which it is drained by the master.
     * @param sinkIdleTimeoutNanos
     *            The time in nanoseconds after which an idle sink will be
     *            closed. Any buffered writes are flushed when the sink is
     *            closed. This must be GTE the <i>sinkChunkTimeout</i>
     *            otherwise the sink will decide that it is idle when it was
     *            just waiting for enough data to prepare a full chunk.
     * @param sinkPollTimeoutNanos
     *            The time in nanoseconds that the {@link AbstractSubtask sink}
     *            will wait inside of the {@link IAsynchronousIterator} when it
     *            polls the iterator for a chunk. If this value is too large
     *            then the sink will block for noticeable lengths of time and
     *            will be less responsive to interrupts. Something in the 10s of
     *            milliseconds is appropriate.
     */
    public AbstractMasterTask(final H stats, final BlockingBuffer<E[]> buffer,
            final long sinkIdleTimeoutNanos, final long sinkPollTimeoutNanos) {

        if (stats == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        if (sinkIdleTimeoutNanos <= 0)
            throw new IllegalArgumentException();

        if (sinkPollTimeoutNanos <= 0)
            throw new IllegalArgumentException();
        
        this.stats = stats;

        this.buffer = buffer;

        this.sinkIdleTimeoutNanos = sinkIdleTimeoutNanos;
        
        this.sinkPollTimeoutNanos = sinkPollTimeoutNanos;
        
        this.src = buffer.iterator();

        /*
         * The current access to this map is relatively limited. It is accessed
         * when assembling the performance counters and by the worker thread for
         * the master task. The size of the map is the #of index partitions. The
         * #of index partitions does not change rapidly, so this map will not be
         * resized frequently. The only time there is a rapid change in the #of
         * index partitions is when we scatter-split an index.
         */
        this.sinks = new ConcurrentHashMap<L, S>();
        
        stats.addMaster(this);

    }

    public H call() throws Exception {

        /*
         * Note: If idle timeouts are allowed then we need to reopen the buffer
         * if it has closed by a timeout.
         */

        final boolean reopen = sinkIdleTimeoutNanos != 0;
        
        try {

            /*
             * Note: This polls the master's input buffer so it can check the
             * redirectQueue in a timely manner.
             */
            while (true) {//src.hasNext() || !redirectQueue.isEmpty()) {

                halted();
                
                // drain the redirectQueue if not empty.
                E[] a = redirectQueue.poll();

                if (a == null) {

                    // poll the master's input queue.
                    if (src.hasNext(buffer.getChunkTimeout(),
                            TimeUnit.NANOSECONDS)) {
                        
                        // drain chunk from the master's input queue.
                        a = src.next();
                        
                    } else {

                        /*
                         * Nothing available right now.
                         */
                        if (!buffer.isOpen() && buffer.isEmpty()) {

                            /*
                             * If the master's input buffer is closed and has
                             * been drained then we stop polling here, but we
                             * will continue to drain the redirectQueue in
                             * awaitAll().
                             */
                            break;

                        } else {

                            // Nothing available - poll again.
                            continue;

                        }

                    }

                } else {

                    if (log.isInfoEnabled())
                        log.info("Read chunk from redirectQueue");

                }

                // empty chunk?
                if (a.length == 0)
                    continue;
                
                synchronized (stats) {
                    // update the master stats.
                    stats.chunksIn++;
                    stats.elementsIn += a.length;
                }

                handleChunk(a, reopen);
                
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
    abstract protected void handleChunk(E[] chunk, boolean reopen)
            throws InterruptedException;

    /**
     * Await the completion of the writes on each index partition. The master
     * will terminate when there are no active subtasks and the redirect queue
     * is empty. That condition is tested atomically.
     * 
     * @throws ExecutionException
     *             This will report the first cause.
     * @throws InterruptedException
     *             If interrupted while awaiting the {@link #lock} or the child
     *             tasks.
     * 
     * @todo The test suite should include a case where a set of redirects
     *       appear just as the sinks are processing their last chunk. This
     *       corresponds to the conditions for a deadlock, which has been
     *       resolved in the code. The deadlock would arise because the
     *       {@link #awaitAll()} was holding the {@link #lock} across
     *       {@link #handleChunk(Object[], boolean)} such that a
     *       {@link AbstractSubtask#call()} was unable to gain the {@link #lock}
     *       so that it could signal {@link #subtaskDone}.
     */
    private void awaitAll() throws InterruptedException, ExecutionException {

            // close buffer - nothing more may be written on the master.
        buffer.close();

        while (true) {

            halted();

            /*
             * Note: We need to hold this lock in order to make an decision
             * atomic concerning whether or not the master's termination
             * conditions have been satisfied.
             * 
             * Note: If we poll the redirectQueue and find that there is another
             * chunk to be processed, then we MUST NOT hold the lock when
             * processing that chunk. Doing so will lead to a deadlock in
             * AbstractSubtask#call() when it tries to grab the lock so that it
             * can signal subtaskDone.
             */
            final E[] a;
            lock.lockInterruptibly();
            try {

                a = redirectQueue.poll();

                if (a == null) {

                    /*
                     * There is nothing available from the redirect queue. 
                     */
                    
                    if (sinks.isEmpty() && redirectQueue.isEmpty()) {

                        /*
                         * We are done since there are no running sinks and the
                         * redirectQueue is empty.
                         * 
                         * Note: We MUST be holding the lock for this
                         * termination condition to be atomic.
                         */
                        break;

                    }

                    /*
                     * Check for sinks which have finished.
                     */
                    
                    if (log.isDebugEnabled())
                        log.debug("Waiting for " + sinks.size()
                                + " subtasks : " + this);

                    for (S sink : sinks.values()) {

                        final Future<?> f = sink.buffer.getFuture();

                        if (f.isDone()) {

                            // check the future (can throw exception).
                            f.get();

                            // clear from the map (but ONLY once we have checked
                            // the Future!)
                            removeOutputBuffer((L) sink.locator,
                                    (AbstractSubtask) sink);

                        }

                    }

                    /*
                     * Yield the lock and wait up to a timeout for a sink to
                     * complete. We can not wait that long because we are still
                     * polling the redirect queue!
                     * 
                     * @todo config timeout
                     */
                    subtaskDone.await(5, TimeUnit.MILLISECONDS);

                    continue;

                }

            } finally {

                lock.unlock();

            }

            if (a != null) {

                /*
                 * Handle a redirected chunk.
                 * 
                 * Note: We DO NOT hold the [lock] here!
                 */

                handleChunk(a, true/* reopen */);

                continue;

            }

        } // continue

        if (log.isInfoEnabled())
            log.info("All subtasks are done: " + this);

    }

    /**
     * Cancel all running tasks, discarding any buffered data.
     * <p>
     * Note: This method does not wait on the canceled tasks.
     * <p>
     * Note: The caller should have already invoked {@link #halt(Throwable)}.
     */
    private void cancelAll(final boolean mayInterruptIfRunning) {

        log.warn("Cancelling job: " + this);

        /*
         * Close the buffer (nothing more may be written).
         * 
         * Note: We DO NOT close the iterator draining the buffer since that
         * would cause this task to interrupt itself!
         */
        buffer.close();
        
        // Clear the backing queue.
        buffer.clear();
        
        // clear the redirect queue.
        redirectQueue.clear();
        
        // cancel the futures.
        for (S sink : sinks.values()) {

            final Future<?> f = sink.buffer.getFuture();

            if (!f.isDone()) {

                f.cancel(mayInterruptIfRunning);

            }

        }

    }

    /**
     * Return the sink for the locator. The sink is created if it does not exist
     * using {@link #newSubtaskBuffer()} and
     * {@link #newSubtask(Object, BlockingBuffer)}.
     * 
     * @param locator
     *            The locator (unique subtask key).
     * @param reopen
     *            <code>true</code> IFF a closed buffer should be re-opened (in
     *            fact, this causes a new buffer to be created and the new
     *            buffer will be drained by a new
     *            {@link IndexPartitionWriteTask}).
     * 
     * @return The sink for that locator.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws InterruptedException
     *             if interrupted.
     * @throws RuntimeException
     *             if {@link #halted()}
     */
    private S getSink(final L locator, final boolean reopen)
            throws InterruptedException {

        if (locator == null)
            throw new IllegalArgumentException();

        // operation not allowed if halted.
        halted();

        S sink = sinks.get(locator);

        if (reopen && sink != null && !sink.buffer.isOpen()) {

            if (log.isInfoEnabled())
                log.info("Reopening sink (was closed): " + this + ", locator="
                        + locator);

            // wait for the sink to terminate normally.
            awaitSink(sink);

            sink = null;

        }

        if (sink == null) {

            if (log.isInfoEnabled())
                log.info("Creating output buffer: " + this + ", locator="
                        + locator);

            /*
             * Obtain the lock used to guard sink life cycle events.
             * 
             * @todo double-check locking or is caller single threaded, in which
             * case do we need this lock? Document this better either way. [The
             * caller is single threaded since this is invoked from the master's
             * thread, at least for now.]
             */
            lock.lockInterruptibly();
            try {

                final BlockingBuffer<E[]> out = newSubtaskBuffer();

                sink = newSubtask(locator, out);

                final S oldval = sinks.put(locator, sink);

                if (oldval == null) {

                    // assign a worker thread to the sink.
                    final Future<? extends AbstractSubtaskStats> future = submitSubtask(sink);

                    out.setFuture(future);

                    synchronized (stats) {

                        stats.subtaskStartCount++;

                    }

                } else {

                    // concurrent create of the sink.
                    sink = oldval;

                }
                
            } finally {

                lock.unlock();
                
            }

        }

        return sink;

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
     * it has been closed. This checks the {@link Future} to verify that the
     * sink completed normally.
     * 
     * @throws IllegalStateException
     *             unless the {@link #buffer} is closed.
     */
    private void awaitSink(final S sink) {

        if (sink == null)
            throw new IllegalArgumentException();
        
        if(sink.buffer.isOpen())
            throw new IllegalStateException();

        final Future<?> f = sink.buffer.getFuture();
        final long begin = System.nanoTime();
        try {

            while (true) {

                try {

                    // test the future.
                    f.get(1, TimeUnit.SECONDS);

                    // clear from the map (but ONLY once we have checked the Future!)
                    removeOutputBuffer((L) sink.locator, (AbstractSubtask) sink);

                    // Done.
                    return;
                    
                } catch (TimeoutException ex) {

                    log.warn("Waiting on sink: elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
                                    - begin) + ", sink=" + sink);

                }

            }

        } catch (Throwable t) {

            halt(t);

            throw new RuntimeException(t);

        }

    }

    /**
     * Removes the output buffer (unless it has been replaced by another output
     * buffer associated with a different sink). DO NOT invoke this method until
     * you have checked the {@link Future} of the sink for errors.
     * 
     * @param locator
     *            The locator.
     * @param sink
     *            The sink.
     */
    protected void removeOutputBuffer(final L locator,
            final AbstractSubtask sink) {

        if (locator == null)
            throw new IllegalArgumentException();

        if (sink == null)
            throw new IllegalArgumentException();
        
        /*
         * Remove map entry IFF it is for the same reference.
         */
        if (sinks.remove(locator, sink)) {

            if (log.isDebugEnabled())
                log.debug("Removed output buffer: " + locator);

            /*
             * Note: This should not be invoked on a sink whose Future has not
             * been checked. That means that nobody is checking the Future for
             * that sink and that errors could go unreported.
             */
            
            assert sink.buffer.getFuture().isDone();

        }

    }

    /**
     * Resolves the output buffer onto which the split must be written and adds
     * the data to that output buffer.
     * 
     * @param split
     *            The {@link Split} identifies both the tuples to be dispatched
     *            and the {@link PartitionLocator} on which they must be
     *            written.
     * @param a
     *            The array of tuples. Only those tuples addressed by the
     *            <i>split</i> will be written onto the output buffer.
     * @param reopen
     *            <code>true</code> IFF a closed buffer should be re-opened (in
     *            fact, this causes a new buffer to be created and the new
     *            buffer will be drained by a new {@link AbstractSubtask}).
     * 
     * @throws InterruptedException
     *             if the thread is interrupted.
     */
    @SuppressWarnings("unchecked")
    protected void addToOutputBuffer(final L locator, final E[] a,
            final int fromIndex, final int toIndex, boolean reopen)
            throws InterruptedException {

        final int n = (toIndex - fromIndex);

        if (n == 0)
            return;

        /*
         * Make a dense chunk for this split.
         */

        final E[] b = (E[]) java.lang.reflect.Array.newInstance(a.getClass()
                .getComponentType(), n);

        System.arraycopy(a, fromIndex, b, 0, n);

        final long begin = System.nanoTime();

        boolean added = false;

        while (!added) {

            halted();

            // resolve the sink / create iff necessary.
            final S sink = getSink(locator, reopen);

            try {

                added = offerChunk(sink, b, reopen);

            } catch (BufferClosedException ex) {

                if (ex.getCause() instanceof IdleTimeoutException
                        || ex.getCause() instanceof MasterExhaustedException) {

                    /*
                     * Note: The sinks sets the exception if it closes the input
                     * queue by idle timeout or because the master was
                     * exhausted.
                     * 
                     * These exceptions are trapped here and cause the sink to
                     * be re-opened (simply by restarting the loop).
                     */

                    if (log.isInfoEnabled())
                        log.info("Sink closed asynchronously: cause="
                                + ex.getCause() + ", sink=" + sink);

                    // definitely re-open!
                    reopen = true;
                    
                    // retry
                    continue;

                } else {

                    // anything else is a problem.
                    throw ex;

                }

            }

            if (added) {

                /*
                 * Update timestamp of the last chunk written on that sink.
                 */
                sink.lastChunkNanos = System.nanoTime();

            }

        }

        synchronized (stats) {

            stats.chunksTransferred += 1;
            stats.elementsTransferred += b.length;
            stats.elementsOnSinkQueues += b.length;
            stats.elapsedSinkOfferNanos += (System.nanoTime() - begin);

        }

    }

    /**
     * Add a dense chunk to the sink's input queue. This is method deliberately
     * yields the {@link #lock} if it blocks while attempting to added the chunk
     * to the sink's input queue. This is done to prevent deadlocks from arising
     * where the caller owns the {@link #lock} but the sink's input queue is
     * full and the sink can not gain the {@link #lock} in order to drain a
     * chunk.
     * 
     * @param sink
     *            The sink.
     * @param dense
     *            A dense chunk to be transferred to the sink's input queue.
     * @param reopen
     * 
     * @return <code>true</code> iff the chunk was added to the sink's input
     *         queue.
     * 
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    private final boolean offerChunk(final S sink, E[] dense,
            final boolean reopen) throws InterruptedException {

        // track offer time.
//        final long begin = System.nanoTime();

        boolean added = false;

        while (!added) {

            halted();

            try {

                // stack trace through here if [reopen == true]
                added = sink.buffer.add(dense, offerTimeoutNanos,
                        TimeUnit.NANOSECONDS);
                
            } catch (BufferClosedException ex) {

                if (ex.getCause() instanceof StaleLocatorException) {

                    /*
                     * Note: The sinks sets the exception when closing the
                     * buffer when handling the stale locator exception and
                     * transfers the outstanding and all queued chunks to the
                     * redirectQueue.
                     * 
                     * When we trap the stale locator exception here we need to
                     * transfer the chunk to the redirectQueue since the buffer
                     * was closed (and drained) asynchronously.
                     */

                    if (log.isInfoEnabled())
                        log
                                .info("Sink closed asynchronously by stale locator exception: "
                                        + sink);

                    redirectQueue.put(dense);

                    added = true;

                } else {

                    // anything else is a problem.
                    throw ex;

                }

            }

        }

        return added;

    }
    
    /**
     * This is a fast timeout since we want to avoid the possibility that
     * another thread require's the master's {@link #lock} while we are waiting
     * on a sink's input queue. Whenever this timeout expires we will yield the
     * {@link #lock} and then retry in a bit.
     */
    private final static long offerTimeoutNanos = TimeUnit.MILLISECONDS
            .toNanos(1);

    /**
     * This is a much slower timeout - we log a warning when it expires.
     */
    private final static long offerWarningTimeoutNanos = TimeUnit.MILLISECONDS
            .toNanos(5000);

}
