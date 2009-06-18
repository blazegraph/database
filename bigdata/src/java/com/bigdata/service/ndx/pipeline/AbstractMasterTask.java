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
 * @todo Update javadoc to reflect that the master no longer waits for a closed
 *       sink in {@link #getSink(Object, boolean)} but instead places the sink
 *       onto the {@link #finishedSubtaskQueue}.
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
    private final BlockingQueue<E[]> redirectQueue = new LinkedBlockingQueue<E[]>(/* unbounded */);

    /**
     * The #of chunks on the master's redirectQueue.
     */
    public final int getRedirectQueueSize() {

        return redirectQueue.size();
        
    }

    /**
     * Places a chunk onto the master's redirectQueue.
     * 
     * @param chunk
     *            The chunk.
     * 
     * @throws InterruptedException
     */
    protected final void redirectChunk(final E[] chunk)
            throws InterruptedException {

        /*
         * @todo Acquiring the lock here should not be (and probably is not)
         * necessary for the master's atomic termination condition.
         */
        lock.lockInterruptibly();
        try {
            redirectQueue.put(chunk);
        } finally {
            lock.unlock();
        }

    }
    
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

        final Iterator<S> itr = sinks.values().iterator();

        while(itr.hasNext()) {
            
            op.call(itr.next());
            
        }

    }

    /**
     * Lock used for sink life cycle events <em>only</em>.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition is signaled by a subtask when it is finished. This is used by
     * {@link #awaitAll()} to while waiting for subtasks to complete. If all
     * subtasks in {@link #subtasks} are complete when this signal is received
     * then the master may terminate.
     */
    private final Condition subtaskDone = lock.newCondition();

    /**
     * A queue of subtasks which have finished running. The master polls this
     * queue in {@link #call()} in order to clear finished sinks from
     * {@link #sinks} in a timely manner during normal operations.
     * {@link #awaitAll()} and {@link #cancelAll(boolean)} both handle this in
     * their own way.
     */
    private final BlockingQueue<S> finishedSubtaskQueue = new LinkedBlockingQueue<S>();

    /**
     * Notify the master that a subtask is done. The subtask is placed onto the
     * {@link #finishedSubtaskQueue} queue. The master polls that queue in
     * {@link #call()} and {@link #awaitAll()} and checks the {@link Future} of
     * each finished subtask using {@link #drainFutures()}. If a {@link Future}
     * reports an error, then the master is halted. This is how we ensure that
     * all subtasks complete normally.
     * 
     * @param subtask
     *            The subtask.
     * 
     * @throws InterruptedException
     */
    protected void notifySubtaskDone(final AbstractSubtask subtask)
            throws InterruptedException {
        
        if (subtask == null)
            throw new IllegalArgumentException();
        
        if (subtask.buffer.isOpen())
            throw new IllegalStateException();

        lock.lockInterruptibly();
        
        try {
    
            // atomic transfer from [sinks] to [finishedSubtasks].
            moveSinkToFinishedQueueAtomically((L) subtask.locator,
                    (AbstractSubtask) subtask);

            // signal condition (used by awaitAll()).
            subtaskDone.signalAll();

        } finally {
        
            lock.unlock();
            
        }
    
    }

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
         * Note: If idle timeouts are allowed then we need to reopen the sink if
         * it has closed by a timeout.
         */

        final boolean reopen = sinkIdleTimeoutNanos != 0;
        
        try {

            /*
             * Note: This polls the master's input buffer so it can check the
             * redirectQueue in a timely manner.
             */
            while (true) {

                halted();

                drainFutures();
                
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
                    stats.chunksIn.incrementAndGet();
                    stats.elementsIn.addAndGet(a.length);
                }

                handleChunk(a, reopen);
                
            } // while(true)

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
     *       appear just as the sinks are processing their last chunk.
     */
    private void awaitAll() throws InterruptedException, ExecutionException {

        if(buffer.isOpen()) {

            /*
             * The buffer must be closed as a precondition otherwise the
             * application can continue to write data on the master's input
             * queue and the termination conditions can not be satisified.
             */

            throw new IllegalStateException();
            
        }

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
             * can signal subtaskDone. Therefore we process the chunks outside
             * of this code block, once we have released the lock.
             */
            final E[] a;
            lock.lockInterruptibly();
            try {

                a = redirectQueue.poll();

                if (a == null) {

                    /*
                     * There is nothing available from the redirect queue.
                     */

                    if (finishedSubtaskQueue.isEmpty() && sinks.isEmpty()
                            && redirectQueue.isEmpty()) {

                        /*
                         * We are done since there are no running sinks, and no
                         * sinks whose Future we still need to test, and the
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

                    drainFutures();
                    
                    if (!finishedSubtaskQueue.isEmpty()) {

                        /*
                         * Yield the lock and wait up to a timeout for a sink to
                         * complete. We can not wait that long because we are
                         * still polling the redirect queue!
                         * 
                         * @todo config timeout
                         */

                        subtaskDone.await(50, TimeUnit.MILLISECONDS);

                    }

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

            }

        } // while(true)
        
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
    private void cancelAll(final boolean mayInterruptIfRunning)
            throws InterruptedException {

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
        
        // cancel the futures.
        for (S sink : sinks.values()) {

            final Future<?> f = sink.buffer.getFuture();

            if (!f.isDone()) {

                f.cancel(mayInterruptIfRunning);

            }

        }

        // wait for all sinks to complete.
        for( S sink : sinks.values()) {
            final Future<?> f = sink.buffer.getFuture();
            try {
                f.get();
            } catch (InterruptedException ex) {
                throw ex;
            } catch (ExecutionException ex) {
                /*
                 * Ignore exceptions here since we are halting anyway and we can
                 * expect a bunch of canceled tasks because we just interrupted
                 * all of the subtasks.
                 */
                log.warn("sink=" + sink + " : " + ex);
            }
            
        }
        
        // clear references to the sinks.
        sinks.clear();
        
        // clear queue of finished sinks (we do not need to check their futures).
        finishedSubtaskQueue.clear();

        // clear the redirect queue.
        redirectQueue.clear();
        
    }

    /**
     * Return the sink for the locator. The sink is created if it does not exist
     * using {@link #newSubtaskBuffer()} and
     * {@link #newSubtask(Object, BlockingBuffer)}.
     * <p>
     * Note: The caller is single threaded since this is invoked from the
     * master's thread. This code depends on that assumption.
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

        if (sink != null && sink.buffer.isOpen()) {

            /*
             * The sink is good, so return it.
             * 
             * Note: the caller must handle the exception if the sink is
             * asynchronously closed before the caller can use it.
             */

            return sink;

        }

        /*
         * @todo This lock should not be necessary (and probably is not) since
         * the caller is single threaded.
         */
        lock.lockInterruptibly();
        try {

            if (reopen && sink != null && !sink.buffer.isOpen()) {

                if (log.isInfoEnabled())
                    log.info("Reopening sink (was closed): " + this
                            + ", locator=" + locator);

                /*
                 * Note: Instead of waiting for the sink here, the sink will
                 * notify the master when it is done and be placed onto the
                 * [finishedSinks] queue. When it's Future#isDone(), we will
                 * drain it from the queue and check it's Future for errors.
                 * 
                 * This allows the master to not block when a sink is closed but
                 * still awaiting an RMI. It also implies that there can be two
                 * active sinks for the same locator, but only one will be
                 * recorded in [sinks] at any given time.
                 */

                // remove the old sink from the map (IFF that is the reference
                // found).
                moveSinkToFinishedQueueAtomically(locator, sink);

                // clear reference so we can re-open the sink.
                sink = null;

            }

            if (sink == null) {

                if (log.isInfoEnabled())
                    log.info("Creating output buffer: " + this + ", locator="
                            + locator);

                final BlockingBuffer<E[]> out = newSubtaskBuffer();

                sink = newSubtask(locator, out);

                final S oldval = sinks.put(locator, sink);

                // should not be an entry in the map for that locator.
                assert oldval == null : "locator=" + locator;

                // if (oldval == null) {

                // assign a worker thread to the sink.
                final Future<? extends AbstractSubtaskStats> future = submitSubtask(sink);

                out.setFuture(future);

                stats.subtaskStartCount.incrementAndGet();

                // } else {
                //
                // // concurrent create of the sink.
                // sink = oldval;
                //
                // }

            }

            return sink;

        } finally {

            lock.unlock();

        }

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
     * Drains any {@link Future}s from {@link #finishedSubtaskQueue} which are done
     * and halts the master if there is an error for a {@link Future}.
     * 
     * @throws InterruptedException
     *             if interrupted.
     */
    private void drainFutures() throws InterruptedException, ExecutionException {

        while (true) {
        
            halted();
            
            /*
             * Poll the queue of sinks that have finished running
             * (non-blocking).
             * 
             * Note: We don't actually remove the subtask from the queue until
             * its Future is available.
             */
            final S sink = finishedSubtaskQueue.peek();
            
            if (sink == null) {
                
                // queue is empty.
                return;
                
            }

            if(sink.buffer.isOpen())
                throw new IllegalStateException(sink.toString());

            final Future<?> f = sink.buffer.getFuture();

            if(!sink.buffer.getFuture().isDone()) {
             
                // Future is not available for the next sink in the queue.
                return;
                
            }
            
            try {

                // check the future.
                f.get();

                // remove the head of the queue.
                if (sink != finishedSubtaskQueue.remove()) {

                    // The wrong sink is at the head of the queue.
                    throw new AssertionError();
                    
                }
                
            } catch(ExecutionException ex) {
                
                // halt on error.
                throw halt(ex);
                
            } finally {

                /*
                 * Increment this counter immediately when the subtask is done
                 * regardless of the outcome. This is used by some unit tests to
                 * verify idle timeouts and the like.
                 */
                stats.subtaskEndCount.incrementAndGet();
                
                if (log.isDebugEnabled())
                    log.debug("subtaskEndCount incremented: " + sink.locator);
            }

        }

    }

    /**
     * Transfer a sink from {@link #sinks} to {@link #finishedSubtaskQueue}. The
     * entry for the locator is removed from {@link #sinks} atomically IFF that
     * map the given <i>sink</i> is associated with the given <i>locator</i> in
     * that map.
     * <p>
     * This is done atomically using the {@link #lock}. This is invoked both by
     * {@link AbstractSubtask#call()} (when it is preparing to exit call()) and
     * by {@link #getSink(Object, boolean)} (when it discovers that the sink for
     * a locator is closed, but not yet finished with its work).
     * <p>
     * Note: The sink MUST be atomically transferred from {@link #sinks} to
     * {@link #finishedSubtaskQueue} and {@link #awaitAll()} MUST verify that
     * both are empty in order for the termination condition to be atomic.
     * 
     * @param locator
     *            The locator.
     * @param sink
     *            The sink.
     * 
     * @todo this method really should be private. It is exposed for one of the
     *       unit tests.
     */
    protected void moveSinkToFinishedQueueAtomically(final L locator,
            final AbstractSubtask sink) throws InterruptedException {

        if (locator == null)
            throw new IllegalArgumentException();

        if (sink == null)
            throw new IllegalArgumentException();

        lock.lockInterruptibly();
        try {

            /*
             * Place on queue (check by call(), awaitAll()). It is safe to do
             * this even if the sink has already been moved, in which case its
             * Future will be checked twice, which is not a problem.
             */
            finishedSubtaskQueue.put((S) sink);

            /*
             * Remove map entry IFF it is for the same reference.
             */
            if (sinks.remove(locator, sink)) {

                if (log.isDebugEnabled())
                    log.debug("Removed output buffer: " + locator);

            }

        } finally {
        
            lock.unlock();
            
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

                added = sink.buffer.add(b, offerWarningTimeoutNanos,
                        TimeUnit.NANOSECONDS);
                
                final long now = System.nanoTime();
                
                if (added) {

                    /*
                     * Update timestamp of the last chunk written on that sink.
                     */
                    sink.lastChunkNanos = now;

                } else {
                    
                    log.warn("Sink is slow: elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(now - begin)
                            + "ms, sink=" + sink);
                    
                }
                
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

                    redirectChunk( b );

                    added = true;
                    
                } else if (ex.getCause() instanceof IdleTimeoutException
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
                    
                } else {

                    // anything else is a problem.
                    throw ex;

                }

            }

        }

        synchronized (stats) {

            stats.chunksTransferred.incrementAndGet();
            stats.elementsTransferred.addAndGet(b.length);
            stats.elementsOnSinkQueues.addAndGet(b.length);
            stats.elapsedSinkOfferNanos += (System.nanoTime() - begin);

        }

    }

    /**
     * This timeout is used to log warning messages when a sink is slow.
     */
    private final static long offerWarningTimeoutNanos = TimeUnit.MILLISECONDS
            .toNanos(5000);

}
