/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Nov 11, 2007
 */

package com.bigdata.relation.accesspath;

import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.relation.rule.IQueryOptions;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.NestedSubqueryWithJoinThreadsTask;
import com.bigdata.striterator.ICloseableIterator;

/**
 * <p>
 * A buffer that will block when it is full. You write elements on the buffer
 * and they can be read using {@link #iterator()}. This class is safe for
 * concurrent writes (multiple threads can use {@link #add(Object)}) but the
 * {@link #iterator()} is not thread-safe (it assumes a single reader).
 * </p>
 * <p>
 * <strong>You MUST make sure that the thread that sets up the
 * {@link BlockingBuffer} and which submits a task that writes on the buffer
 * also sets the {@link Future} on the {@link BlockingBuffer} so that the
 * iterator can monitor the {@link Future}, detect if it has been cancelled,
 * and throw out the exception from the {@link Future} back to the client.
 * Failure to do this can lead to the iterator not terminating!</strong>
 * </p>
 * <p>
 * Note: {@link BlockingBuffer} is used (a) for {@link IAccessPath} iterators
 * that exceed the fully-buffered read threashold; (b) for high-level query with
 * at least one join; (c) by the BigdataStatementIteratorImpl, which is used by
 * the RDF DB for high-level query with no joins; and by the
 * BigdataSolutionResolverator, which is used by the RDF DB high-level query
 * that produces binding sets.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlockingBuffer<E> implements IBlockingBuffer<E> {
    
    protected static final Logger log = Logger.getLogger(BlockingBuffer.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * <code>true</code> until the buffer is {@link #close()}ed.
     */
    private volatile boolean open = true;

    private volatile Throwable cause = null;

    /**
     * Used to coordinate the reader and the writer.
     */
    private final BlockingQueue<E> queue;

    /**
     * The singleton for the iterator used to read from this buffer.
     */
    private final IAsynchronousIterator<E> iterator;
    
    /**
     * The default capacity for the internal buffer. Chunks can not be larger
     * than this.
     */
    protected static transient final int DEFAULT_CAPACITY = 5000;

    protected static transient final int DEFAULT_CHUNK_SIZE = 10000;

    protected static transient final long DEFAULT_CHUNK_TIMEOUT = 1000;

    protected static transient final TimeUnit DEFAULT_CHUNK_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    /**
     * The target chunk size for the chunk combiner.
     */
    private final int chunkCapacity;

    /**
     * The maximum time to wait in nanoseconds for another chunk to come along
     * so that we can combine it with the current chunk for {@link #next()}. A
     * value of ZERO(0) disables chunk combiner.
     */
    private final long chunkTimeout;

    private volatile Future future;
    
    public void setFuture(Future future) {
    
        synchronized (this) {
        
            if (future == null)
                throw new IllegalArgumentException();

            if (this.future != null)
                throw new IllegalStateException();

            this.future = future;

        }
        
    }
    
    /**
     *
     */
    public BlockingBuffer() {
        
        this(DEFAULT_CAPACITY);
        
    }
    
    /**
     * Ctor automatically provisions an appropriate {@link BlockingQueue}.
     * 
     * @param capacity
     *            The capacity of the buffer. When the generic type <i>&lt;E&gt;</i>
     *            is an array type, then this is the <i>chunkOfChunksCapacity</i>
     *            and small chunks will be automatically combined based on
     *            availability and latency.
     *            <p>
     *            When zero (0) a {@link SynchronousQueue} will be used.
     *            Otherwise an {@link ArrayBlockingQueue} of the given capacity
     *            is used.
     */
    public BlockingBuffer(int capacity) {

       this(capacity,
            DEFAULT_CHUNK_SIZE,
            DEFAULT_CHUNK_TIMEOUT,
            DEFAULT_CHUNK_TIMEOUT_UNIT
            ); 

    }
    
    public BlockingBuffer(final int capacity, final int chunkSize,
            final long chunkTimeout, final TimeUnit chunkTimeoutUnit) {

        /*
         * FIXME test w/ synchronous queue. The chunk combiner should still
         * work.
         */
        this(capacity == 0 ? new SynchronousQueue<E>()
                : new ArrayBlockingQueue<E>(capacity),
                chunkSize,
                chunkTimeout,
                chunkTimeoutUnit
                ); 

    }

    /**
     * Core ctor.
     * 
     * @param queue
     *            The queue on which elements will be buffered. Elements will be
     *            {@link #add(Object) added} to the queue and drained by the
     *            {@link #iterator()}.
     * @param chunkCapacity
     *            The target chunk size. When the elements stored in the buffer
     *            are chunks (ie., arrays of some component type), elements will
     *            be combined together to form larger chunks until this
     *            chunkSize is satisifed, the {@link #iterator()} is exhausted,
     *            or the <i>chunkTime</i> is reached.
     * @param chunkTimeout
     *            The maximum time to wait in nanoseconds for another chunk to
     *            come along so that we can combine it with the current chunk
     *            for {@link #next()}. A value of ZERO(0) disables chunk
     *            combiner.
     * @param chunkTimeoutUnit
     *            The units in which the <i>chunkTimeout</i> is expressed.
     * 
     */
    public BlockingBuffer(final BlockingQueue<E> queue, final int chunkCapacity,
            final long chunkTimeout, final TimeUnit chunkTimeoutUnit) {
    
        if (queue == null)
            throw new IllegalArgumentException();

        if (chunkCapacity < 0) {
            /*
             * Note: zero is allowed since the buffer may store individual
             * elements depending on the generic type <E>, not just chunks of
             * elements.
             */
            throw new IllegalArgumentException();
        }

        if (chunkTimeout < 0L) {
            /*
             * Note: zero is allowed and will disable the chunk combiner.
             */
            throw new IllegalArgumentException();
        }
        
        if (chunkTimeoutUnit == null)
            throw new IllegalArgumentException();
        
        this.queue = queue;
        
        this.chunkCapacity = chunkCapacity;
        
        // convert to nanoseconds.
        this.chunkTimeout = TimeUnit.NANOSECONDS.convert(chunkTimeout,
                chunkTimeoutUnit);
        
        this.iterator = new BlockingIterator();

    }

    /**
     * @throws IllegalStateException
     *             if the buffer is not open.
     */
    private void assertOpen() {
        
        if(!open) {
        
            if (cause != null) {

                throw new IllegalStateException(cause);
                
            }
            
            throw new IllegalStateException();
            
        }
        
    }

    public boolean isEmpty() {

        return queue.isEmpty();
        
    }

    public int size() {

        return queue.size();
        
    }

    /**
     * Closes the {@link BlockingBuffer} such that it will not accept new
     * elements. The {@link IAsynchronousIterator} will drain any elements
     * remaining in the {@link BlockingBuffer} (this does NOT close the
     * {@link BlockingIterator}).
     */
    public void close() {
    
        this.open = false;

        notifyIterator();
        
        if (INFO)
            log.info("closed.");
        
    }

    private final void notifyIterator() {

        /*
         * The iterator does not actually wait() on anything so this is not
         * necessary.
         */
//        synchronized(iterator) {
//            
//            iterator.notify();
//            
//        }

    }
    
    public void abort(Throwable cause) {

        if (cause == null)
            throw new IllegalArgumentException();

        log.warn("cause=" + cause, cause);

        synchronized (this) {

            if (this.cause != null) {

                log.warn("Already aborted with cause=" + this.cause);

                return;

            }

            this.cause = cause;

            this.open = false;
            
        }
        
    }
    
    public void add(E e) {

        assertOpen();

        final long begin = System.currentTimeMillis();
        
        // wait if the queue is full.
        int ntries = 0;
        // initial delay before the 1st log message.
        long timeout = 100;
        // Note: controls time between log messages NOT the wait time.
        final long maxTimeout = 10000;

        while (true) {

            try {

                if (!queue.offer(e, timeout, TimeUnit.MILLISECONDS)) {

                    /*
                     * Note: While not the only explanation, a timeout here can
                     * occur if you have nested JOINs. The outer JOIN can
                     * timeout waiting on the inner JOINs to consume the current
                     * tuple. If there are a lot of tuples being evaluated in
                     * the inner JOINs, then a timeout is becomes more likely.
                     */
                    
                    ntries++;
                    
                    final long elapsed = System.currentTimeMillis() - begin;

                    timeout = Math.min(maxTimeout, timeout *= 2);
                    
                    log.warn("waiting - queue is full: ntries=" + ntries
                            + ", elapsed=" + elapsed + ", timeout=" + timeout);

                    continue;
                    
                }
                
            } catch (InterruptedException ex) {

                close();

                throw new RuntimeException("Buffer closed by interrupt", ex);
                
            }
            
            // item now on the queue.

            if (DEBUG)
                log.debug("added: " + e.toString());
            
            notifyIterator();
            
            return;
            
        }
        
    }

    public long flush() {

        return 0L;
        
    }

    public void reset() {
        
        queue.clear();
        
        // clear nextE on the inner class?
        
    }
    
    /**
     * The iterator is NOT thread-safe and does NOT support remove().
     * <p>
     * Note: If the {@link IAsynchronousIterator} is
     * {@link ICloseableIterator#close()}d before the {@link Future} of the
     * process writing on the {@link BlockingBuffer} is done, then the
     * {@link Future} will be cancelled using {@link Thread#interrupt()}. Owing
     * to a feature of {@link FileChannel}, this will cause the backing store
     * to be asynchronously closed if the interupt is detected during an IO. T
     * backing store will be re-opened transparently, but there is overhead
     * associated with that (locks to be re-acquired, etc).
     * <p>
     * The most common reason to close an iterator early are that you want to
     * only visit a limited #of elements. However, if you use either
     * {@link IAccessPath#iterator(int, int)} or {@link IRule} with an
     * {@link IQueryOptions} to impose that limit, then most processes that
     * produce {@link IAsynchronousIterator}s will automatically terminate when
     * they reach the desired limit, thereby avoiding issuing interrupts. Those
     * processes include {@link IAccessPath} scans where the #of elements to be
     * visited exceeds the fully materialized chunk threashold and {@link IRule}
     * evaluation, e.g., by {@link NestedSubqueryWithJoinThreadsTask}.
     * 
     * @return The iterator (this is a singleton).
     * 
     * @see BlockingIterator#close()
     */
    public IAsynchronousIterator<E> iterator() {

        return iterator;
        
    }

    /**
     * An inner class that reads from the buffer. This is not thread-safe - it
     * makes no attempt to be atomic in its operations in {@link #hasNext()} or
     * {@link #next()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class BlockingIterator implements IAsynchronousIterator<E> {
        
        /**
         * <code>true</code> iff this iterator is open - it is closed when the
         * thread consuming the iterator decides that it is done with the
         * iterator.
         * <p>
         * Note: {@link BlockingBuffer#open} is <code>true</code> until the
         * thread WRITING on the buffer decides that it has nothing further to
         * write. Once {@link BlockingBuffer#open} becomes <code>false</code>
         * and there are no more elements in the buffer then the iterator is
         * exhausted since there is nothing left that it can visit and nothing
         * new will enter into the buffer.
         */
        private boolean open = true;

        /**
         * The next element to be returned (from the head of the queue).
         * <p>
         * Note: We use {@link #nextE} because {@link BlockingQueue} does not
         * offer a method in which we peek with a timeout, only in which we poll
         * with a timeout. Therefore we poll with a timeout and if poll()
         * returns an element, then we set it on {@link #nextE}.
         */
        private E nextE = null;

        /**
         * The #of chunks delivered so far.
         */
        private long nchunks = 0L;
        
        /**
         * The #of elements delivered so far.
         */
        private long nelements = 0L;
        
        /**
         * Create an iterator that reads from the buffer.
         */
        private BlockingIterator() {
       
            if (INFO)
                log.info("Starting iterator.");
            
        }
       
        /**
         * @throws IllegalStateException
         *             if the {@link BlockingBuffer#abort(Throwable)} was
         *             invoked asynchronously.
         */
        private void assertNotAborted() {

            if (cause != null) {

                throw new IllegalStateException(cause);

            }
                
        }

        /**
         * Notes that the iterator is closed and hence may no longer be read.
         * <p>
         * If the source is still running and we close the iterator then the
         * source will block once the iterator fills up and it will fail to
         * progress. To avoid this, and to have the source process terminate
         * eagerly if the client closes the iterator, we cancel the future if it
         * is not yet done.
         * <p>
         * Note: This means that processes writing on a BlockingBuffer MUST
         * treat an interrupt() as normal (but eager) termination. The best
         * example is rule execution. When the (nested) joins for a rule are
         * executed, each join-task can write on this buffer. However, if the
         * interrupt is noticed during an IO then the {@link FileChannel} for
         * the backing store will be closed asynchronously. While it will be
         * re-opened transparently, it is best to avoid that overhead. For most
         * cases you can do this using either
         * {@link IAccessPath#iterator(int, int)} or an {@link IRule} with an
         * {@link IQueryOptions} that specifies a LIMIT.
         * 
         * @see BlockingBuffer#iterator()
         */
        public void close() {

            if (DEBUG)
                log.debug("");
            
            if (!open)
                return;

            open = false;

            if (future != null && !future.isDone()) {

                if(DEBUG) {
                    
                    log.debug("will cancel future: "+future);
                    
                }
                
                future.cancel(true/* mayInterruptIfRunning */);
                
                if(DEBUG) {
                    
                    log.debug("did cancel future: "+future);
                    
                }

            }

        }

        /**
         * <code>true</code> iff the {@link Future} has been set and
         * {@link Future#isDone()} returns <code>true</code>.
         */
        public boolean isFutureDone() {
            
            return futureIsDone;
            
        }
        
        private volatile boolean futureIsDone = false;
        
        private final void checkFuture() {

            if (!futureIsDone && future != null && future.isDone()) {

                if (INFO)
                    log.info("Future is done");

                // don't re-execute this code.
                futureIsDone = true;

                /*
                 * Make sure the buffer is closed. In fact, the caller probably
                 * does not need to close the buffer since we do it here when
                 * their task completes.
                 */
                BlockingBuffer.this.close();

                try {

                    // look for an error from the Future.
                    future.get();

                } catch (InterruptedException e) {

                    if (INFO)
                        log.info(e.getMessage());

                    // itr will not deliver any more elements.
                    close();

                } catch (ExecutionException e) {

                    log.error(e, e);

                    // itr will not deliver any more elements.
                    close();

                    // rethrow exception.
                    throw new RuntimeException(e);

                }

                /*
                 * Fall through. If there is anything in the queue then we still
                 * need to drain it!
                 */

            }

        }

        /**
         * Return <code>true</code> iff there is at least one element that can
         * be visited (blocking). If the buffer is empty then this will block
         * until: (a) an element appears in the buffer; or (b) the buffer is
         * {@link BlockingBuffer#close()}ed.
         */
        public boolean hasNext() {

            /*
             * Note: We can safely interpret a [false] return as an indication
             * that the iterator is exhausted since we have set an infinite
             * timeout.
             */
            
            return hasNext(Long.MAX_VALUE, TimeUnit.SECONDS);
            
        }
        
        public boolean isExhausted() {
            
            if (!BlockingBuffer.this.open && nextE == null && queue.isEmpty()) {

                // iterator is known to be exhausted.
                return true;

            }

            // iterator might visit more elements (might not also).
            return false;
            
        }

        public boolean hasNext(final long timeout, final TimeUnit unit) {
            
            if (nextE != null) {

                // we already have the next element on hand.
                return true;
                
            }

            if (DEBUG)
                log.debug("begin");

            final long begin = System.nanoTime();

            // note: timeout when nanos <= 0.
            long nanos = unit.toNanos(timeout);

            long lastTime = System.nanoTime();

            /*
             * Loop while the BlockingBuffer is open -or- there is an element
             * available.
             * 
             * Note: hasNext() must wait until the buffer is closed and all
             * elements in the queue (and nextE) have been consumed before it
             * can conclude that there will be nothing more that it can visit.
             * This re-tests whether or not the buffer is open after a timeout
             * and continues to loop until the buffer is closed AND there are no
             * more elements in the queue.
             */
            int ntries = 0;
            // initial delay before the 1st log message.
            long initialLogDelay = 250;
            // maximum delay between log messages (NOT the wait time).
            final long maxLogDelay = 10000;
            while (BlockingBuffer.this.open || nextE != null
                    || !queue.isEmpty()) {

                /*
                 * Perform some checks to decide whether the iterator is still
                 * valid. These checks are tested before we check the timeout
                 * which is a preference for finding (a) a problem with the
                 * iterator; or (b) that we already have the next element.
                 */
                
                checkFuture();
                
                assertNotAborted();

                if(!open) {
                    
                    if (INFO)
                        log.info("iterator is closed");
                    
                    return false;
                    
                }
                
                /*
                 * Now that we have those checks out of the way, update the time
                 * remaining and see if we have a timeout.
                 */
                
                final long now = System.nanoTime();

                // decrement time remaining.
                nanos -= now - lastTime;

                lastTime = now;
                
                if (nanos <= 0) {

                    if (INFO)
                        log.info("Timeout");
                    
                    return false;
                    
                }
                
                try {

                    /*
                     * Poll the queue with a timeout. If there is nothing on the
                     * queue, then increase the timeout and retry from the top
                     * (this will re-check whether or not the buffer has been
                     * closed asynchronously).
                     * 
                     * Note: DO NOT sleep() here. Polling will use a Condition
                     * to wake up when there is something in the queue. If you
                     * sleep() here then ANY timeout can introduce an
                     * unacceptable latency since many queries can be evaluated
                     * in LT 10ms !
                     */

                    if ((nextE = queue.poll(initialLogDelay, TimeUnit.MILLISECONDS)) != null) {
                        
                        if (DEBUG)
                            log.debug("next: " + nextE);

                        return true;

                    }
                    
                } catch (InterruptedException ex) {

                    if (INFO)
                        log.info(ex.getMessage());

                    // itr will not deliver any more elements.
                    close();
                    
                    return false;
                    
                }
                
                /*
                 * Nothing available yet on the queue.
                 */

                // increase the timeout.
                initialLogDelay = Math.min(maxLogDelay, initialLogDelay *= 2);

                ntries++;

                /*
                 * This could be a variety of things such as waiting on a mutex
                 * that is already held, e.g., an index lock, that results in a
                 * deadlock between the process writing on the buffer and the
                 * process reading from the buffer. If you are careful with who
                 * gets the unisolated view then this should not be a problem.
                 */

                final long elapsed = TimeUnit.MILLISECONDS.convert(
                        (now - begin), TimeUnit.NANOSECONDS);

                if (elapsed > 2000/* ms */) {

                    if (future == null) {

                        /*
                         * This can arise if you fail to set the future on the
                         * buffer such that the iterator can not monitor it. If
                         * the future completes (esp. with an error) then the
                         * iterator can keep looking for another element but the
                         * source is no longer writing on the buffer and nothing
                         * will show up.
                         */

                        log.error("Future not set on buffer");

                    } else {

                        log.warn("Iterator is not progressing: ntries="
                                + ntries + ", elapsed=" + elapsed);

                    }

                }

                // loop again.
                continue;
                
            }

            if (INFO)
                log.info("Exhausted");

            assert !BlockingBuffer.this.open;

            assert nextE == null;
            
            return false;

        }

        public E next(long timeout, TimeUnit unit) {

            final long startTime = System.nanoTime();
            
            // timeout in nanoseconds.
            long nanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
            
            if (!hasNext(nanos, TimeUnit.NANOSECONDS)) {

                // nothing available within timeout.
                final long elapsed = System.nanoTime() - startTime;
                
                final boolean isTimeout = elapsed >= timeout;
                
                if (isTimeout) {

                    /*
                     * Since the timeout was exceeded we can not know whether
                     * or not the buffer was exhausted,
                     */ 
                    
                    return null;
                    
                }

                throw new NoSuchElementException();

            }

            final E e = _next();

            // remaining nanos until timeout.
            nanos = System.nanoTime() - startTime;
            
            if (nanos > 0 && e.getClass().getComponentType() != null) {
                
                /*
                 * Combine chunk(s).
                 */
                
                return combineChunks(e, 1/* nchunks */, System.nanoTime(), nanos);
                
            }

            return e;

        }
        
        public E next() {
            
            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            final E e = _next();
            
            if (chunkTimeout > 0 && e.getClass().getComponentType() != null) {
                
                /*
                 * Combine chunk(s).
                 */
                
                return combineChunks(e, 1/* nchunks */, System.nanoTime(), chunkTimeout);
                
            }

            return e;
            
        }
        
        /**
         * Returns the next element (non-blocking).
         * 
         * @throws NoSuchElementException
         *             if {@link #nextE} is <code>null</code> (there is no
         *             element waiting for us).
         */
        private E _next() {

            if (nextE == null)
                throw new NoSuchElementException();
            
            // we already have the next element.
            final E e = nextE;

            nextE = null;
            
            nelements++;
            
            return e;

        }
        
        /**
         * Method is invoked IFF <code>E</code> is an array type (a chunk).
         * The method examines the size of E compared to a desired minimum chunk
         * size. If we would rather have a larger chunk and the buffer is not
         * empty, then we immediately take the next chunk and combine it with
         * the current chunk. This proceeds recursively until either we have a
         * suitable chunk size or the queue is empty.
         * <p>
         * This method is intended to optimize cases where multiple threads
         * produce small chunks that were flushed onto the buffer. By combining
         * those chunks we can improve throughput.
         * 
         * @param e
         *            A chunk.
         * @param nchunks
         *            The #of chunks that have already been consumed. This is
         *            ONE (1) the first time this method is invoked by
         *            {@link #next()}.
         * @param startTime
         *            The start time in nanoseconds of the <i>depth==1</i>
         *            request.
         * @param timeout
         *            The timeout in nanoseconds.
         * 
         * @return Either the same chunk or another chunk with additional
         *         elements.
         */
        private E combineChunks(final E e, final int nchunks,
                final long startTime, final long timeout) {

            final Object[] chunk = (Object[]) e;
            
            final long elapsed = System.nanoTime() - startTime;
            
            final boolean isTimeout = elapsed >= timeout;
            
            // remaining nanos until timeout.
            final long nanos = chunkTimeout - elapsed;
            
            if (chunk.length < chunkCapacity && !isTimeout
                    && hasNext(nanos, TimeUnit.NANOSECONDS)) {
                
                /*
                 * Note: hasNext() will block until either we have another chunk
                 * or until the iterator is known to be exhausted.
                 */

                return combineChunks(combineNextChunk(e), nchunks + 1, startTime, timeout);
                
            }
            
            if (chunk.length < chunkCapacity && !isTimeout && !queue.isEmpty()) {
                
                /*
                 * Non-blocking case. Once we have satisified the minimum chunk
                 * size we will only combine chunks that are already waiting in
                 * the queue.
                 * 
                 * Note: required to make sure that _nextE is bound. This is
                 * non-blocking since we already know that the queue is not
                 * empty.
                 */
                hasNext();
                
                return combineChunks(combineNextChunk(e), nchunks + 1, startTime, timeout);

            }

            // Done.
            if (INFO) 
            log.info("done:\n" + ">>> #chunks=" + nchunks + ", #elements="
                    + chunk.length + ", chunkCapacity=" + chunkCapacity
                    + ", elapsed=" + elapsed + "ns, isTimeout=" + isTimeout
                    + ", queueEmpty=" + queue.isEmpty() + ", open="
                    + BlockingBuffer.this.open);
            
            return e;
            
        }
        
        /**
         * Combines this chunk with the {@link #_next()} one.
         * 
         * @param e
         *            A chunk that we already have on hand.
         * 
         * @return The given chunk combined with the {@link #_next()} chunk.
         */
        @SuppressWarnings("unchecked")
        private E combineNextChunk(final E e) {
            
            final Object[] e1 = (Object[]) e;
            
            final Object[] e2 = (Object[]) _next();
            
            final int chunkSize = e1.length + e2.length;
            
            // Dynamic instantiation of array of the same component type.
            final Object[] a = (E[]) java.lang.reflect.Array.newInstance(e1[0]
                    .getClass(), chunkSize);
            
            // copy first chunk onto the new array.
            System.arraycopy(e, 0, a, 0, e1.length);
            
            // copy second chunk onto the new array.
            System.arraycopy(e2, 0, a, e1.length, e2.length);
            
            // return the combined chunk.
            return (E) a;
            
        }
        
        /**
         * The operation is not supported.
         */
        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }

    }
    
}
