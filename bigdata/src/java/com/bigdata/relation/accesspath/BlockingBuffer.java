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
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

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
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * The initial delay before we will log a warning for {@link #add(Object)}
     * and {@link BlockingBuffer.BlockingIterator#hasNext(long, TimeUnit)}.
     */
    private static final long initialLogTimeout = TimeUnit.NANOSECONDS.convert(
            2000L, TimeUnit.MILLISECONDS);

    /**
     * The maximum timeout before we log the next warning for
     * {@link #add(Object)} and
     * {@link BlockingBuffer.BlockingIterator#hasNext(long, TimeUnit)}.
     * The log timeout will double after each warning logged up to this maximum.
     */
    private static final long maxLogTimeout = TimeUnit.NANOSECONDS
            .convert(10000L, TimeUnit.MILLISECONDS);
    
    /**
     * The #of times that we will use {@link BlockingQueue#offer(Object)} or
     * {@link Queue#poll()} before converting to the variants of those methods
     * which accept a timeout. The timeouts are used to reduce the contention
     * for the queue if either the producer or the consumer is lagging.
     */
    private static final int NSPIN = 100;
    
    /**
     * The timeout for offer() or poll() as a function of the #of tries that
     * have already been made to {@link #add(Object)} or read a chunk.
     * 
     * @param ntries
     *            The #of tries.
     *            
     * @return The timeout (milliseconds).
     */
    private static final long getTimeout(final int ntries) {
        
        return ntries < 500 ? 10L : ntries < 1000 ? 100L : 250L;
        
    }

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
     * The default capacity for the internal {@link Queue} on which elements (or
     * chunks of elements) are buffered.
     */
    protected static transient final int DEFAULT_CAPACITY = 5000;

    /**
     * The default target chunk size for
     * {@link BlockingBuffer.BlockingIterator#next()}.
     */
    protected static transient final int DEFAULT_CHUNK_SIZE = 10000;

    /**
     * The default timeout in milliseconds during which chunks of elements may
     * be combined by {@link BlockingBuffer.BlockingIterator#next()}.
     */
    protected static transient final long DEFAULT_CHUNK_TIMEOUT = 20;

    protected static transient final TimeUnit DEFAULT_CHUNK_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    /**
     * The target chunk size for the chunk combiner.
     * 
     * @see #DEFAULT_CHUNK_SIZE
     */
    private final int chunkCapacity;

    /**
     * The maximum time to wait in nanoseconds for another chunk to come along
     * so that we can combine it with the current chunk for {@link #next()}. A
     * value of ZERO(0) disables chunk combiner.
     * 
     * @see #DEFAULT_CHUNK_TIMEOUT
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

        /*
         * The stack frame in which the buffer was allocated. This gives you a
         * clue about who is the producer that will write on the buffer.
         * 
         * Note: This is only allocated for debugging purposes. Otherwise the
         * stack frame should not be taken.
         */
//        this.stackFrame = new RuntimeException("Buffer Allocation Stack Frame");
        
    }

    private RuntimeException stackFrame;

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append("BlockingBuffer");

        sb.append("{ open=" + open);

        sb.append(", hasFuture=" + future != null);

        sb.append(", elementCount=" + elementCount);

        sb.append(", chunkCount=" + chunkCount);

        if (cause != null)
            sb.append(", cause=" + cause);

        sb.append("}");

        return sb.toString();

    }

    /**
     * @throws BufferClosedException
     *             if the buffer is not open. The cause will be set if
     *             {@link #abort(Throwable)} was used.
     */
    private void assertOpen() {
        
        if(!open) {
        
            if (cause != null) {

                throw new BufferClosedException(cause);
                
            }
            
            throw new BufferClosedException();
            
        }
        
    }

    public boolean isEmpty() {

        return queue.isEmpty();
        
    }

    public int size() {

        return queue.size();
        
    }

    public boolean isOpen() {
        
        return open;
        
    }
    
    /**
     * Closes the {@link BlockingBuffer} such that it will not accept new
     * elements. The {@link IAsynchronousIterator} will drain any elements
     * remaining in the {@link BlockingBuffer} (this does NOT close the
     * {@link BlockingIterator}).
     */
    synchronized public void close() {
    
        if (open) {
         
            try {
         
                if (INFO)
                    log.info("closed.");

                if (stackFrame != null) {

                    /*
                     * Purely for debugging.
                     */

                    log.warn("closing buffer at", new RuntimeException());

                    log.warn("buffer allocated at", stackFrame);

                }
                
            } finally {

                this.open = false;

            }

        }
        
    }

    public void abort(final Throwable cause) {

        if (cause == null)
            throw new IllegalArgumentException();

        if (INFO)
            log.info("cause=" + cause, cause);

        synchronized (this) {

            if (this.cause != null) {

                log.warn("Already aborted with cause=" + this.cause);

                return;

            }

            // set the cause.
            this.cause = cause;

            // mark as closed.
            this.open = false;
            
        }
        
    }
    
    /**
     * The #of chunks {@link #add(Object)ed} to the buffer. This will be ZERO
     * unless the generic type of the buffer is an array type.
     */
    private long chunkCount = 0L;

    /**
     * The #of chunks {@link #add(Object)ed} to the buffer. This will be ZERO
     * unless the generic type of the buffer is an array type.
     */
    public long getChunkCount() {
        
        return chunkCount;
        
    }

    /**
     * The #of elements {@link #add(Object)ed} to the buffer. When the generic
     * type of the buffer is an array type, this will be the sum of the length
     * of the arrays {@link #add(Object)ed} to the buffer.
     */
    private long elementCount = 0L;

    /**
     * The #of elements {@link #add(Object)ed} to the buffer. When the generic
     * type of the buffer is an array type, this will be the sum of the length
     * of the arrays {@link #add(Object)ed} to the buffer.
     */
    public long getElementCount() {
        
        return elementCount;
        
    }

    /**
     * @throws BufferClosedException
     *             if the buffer has been {@link #close()}d.
     */
    public void add(E e) {

        assertOpen();

        final long begin = System.nanoTime();
        
        // wait if the queue is full.
        int ntries = 0;
        // initial delay before the 1st log message.
        long logTimeout = initialLogTimeout;

        while (true) {

            /*
             * Note: While not the only explanation, a timeout here can occur if
             * you have nested JOINs. The outer JOIN can timeout waiting on the
             * inner JOINs to consume the current tuple. If there are a lot of
             * tuples being evaluated in the inner JOINs, then a timeout is
             * becomes more likely.
             * 
             * Note: This is basically a spin lock, though offer(e) does acquire
             * a lock internally. If we have spun enough times, then we will use
             * staged timeouts for subsequent offer()s. This allows the consumer
             * to catch up with less contention for the queue.
             */

            final boolean added;

            if (ntries < NSPIN) {

                // offer (non-blocking).
                added = queue.offer(e);

            } else {

                final long timeout = getTimeout(ntries);

                try {

                    // offer (staged timeout).
                    added = queue.offer(e, timeout, TimeUnit.MILLISECONDS);

                } catch (InterruptedException ex) {

                    abort(ex);

                    throw new RuntimeException("Buffer closed by interrupt", ex);

                }

            }

            if (!added) {

                ntries++;

                final long elapsed = System.nanoTime() - begin;

                if (elapsed >= logTimeout) {

                    logTimeout += Math.min(maxLogTimeout, logTimeout);

                    log.warn("waiting - queue is full: ntries="
                            + ntries
                            + ", elapsed="
                            + TimeUnit.MILLISECONDS.convert(elapsed,
                                    TimeUnit.NANOSECONDS)
                            + ", timeout="
                            + TimeUnit.MILLISECONDS.convert(logTimeout,
                                    TimeUnit.NANOSECONDS));

                }

                // try to add the element again.
                continue;

            }

            // item now on the queue.

            if (e.getClass().getComponentType() != null) {

                chunkCount++;
                elementCount += ((Object[]) e).length;

            } else {

                elementCount++;

            }
            if (DEBUG)
                log.debug("added: " + e.toString());

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
         * <p>
         * Note: This is NOT <code>volatile</code> since it is set by the
         * consumer using {@link #close()} and the consumer is supposed to be
         * single-threaded.
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
         * The #of chunks read so far.
         */
        private long nchunks = 0L;
        
        /**
         * The #of elements read so far.
         */
        private long nelements = 0L;
        
        /**
         * Create an iterator that reads from the buffer.
         */
        private BlockingIterator() {
       
            if (INFO)
                log.info("Starting iterator.");
            
        }
       
//        /**
//         * @throws IllegalStateException
//         *             if the {@link BlockingBuffer#abort(Throwable)} was
//         *             invoked asynchronously.
//         */
//        final private void assertNotAborted() {
//
//            if (cause != null) {
//
//                throw new IllegalStateException(cause);
//
//            }
//                
//        }

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

            if (future == null) {

                log.warn("Future not set");

            } else if (!future.isDone()) {

                if (DEBUG)
                    log.debug("will cancel future: " + future);

                future.cancel(true/* mayInterruptIfRunning */);

                if (DEBUG)
                    log.debug("did cancel future: " + future);

            }

        }

//        /**
//         * <code>true</code> iff the {@link Future} has been set and
//         * {@link Future#isDone()} returns <code>true</code>.
//         */
//        public boolean isFutureDone() {
//            
//            return futureIsDone;
//            
//        }
        
        /**
         * Checks the {@link BlockingBuffer#future} to see if it (a) is done;
         * and (b) has completed successfully. The first time this method checks
         * the {@link Future} and discovers that {@link Future#isDone()} returns
         * <code>true</code> it {@link BlockingBuffer#close()}s the
         * {@link BlockingBuffer}. This is done in case the producer fails to
         * close the {@link BlockingBuffer} themselves. If this method tests the
         * {@link Future} and discovers an exception, then it will wrap and
         * re-throw that exception such that it becomes visible to the consumer.
         * <p>
         * Note: {@link #hasNext()} WILL NOT return <code>false</code> until
         * the {@link BlockingBuffer} is closed. Therefore, this method MUST be
         * invoked MUST be invoked periodically. We do this if the iterator
         * appears to not be progressing since failure by the producer to close
         * the {@link BlockingBuffer} will lead to an iterator that appears to
         * be stalled. However, we also must invoke {@link #checkFuture()} no
         * later than when {@link #hasNext(long, TimeUnit)} returns
         * <code>false</code> in order for the consumer to be guarenteed to
         * see any exception thrown by the producer.
         * <p>
         * Note: Once the {@link BlockingBuffer} is closed (except for aborts),
         * the consumer still needs to drain the {@link BlockingBuffer#queue}.
         */
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

            }

        }
        private boolean futureIsDone = false;

        public boolean isExhausted() {
            
            if (!BlockingBuffer.this.open && nextE == null && queue.isEmpty()) {

                // iterator is known to be exhausted.
                return true;

            }

            // iterator might visit more elements (might not also).
            return false;
            
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
        
        public boolean hasNext(final long timeout, final TimeUnit unit) {
            
            if (nextE != null) {

                // we already have the next element on hand.
                return true;
                
            }

            /*
             * core impl.
             */
            
            final long nanos = unit.toNanos(timeout);

//            final boolean hasNext = _hasNext(nanos);
//            
//            if(!hasNext) {
//                
//                /*
//                 * Note: We call checkFuture() no later than when hasNext()
//                 * would return false so that the consumer will see any
//                 * exception thrown by the producer's Future.
//                 */
//                
//                checkFuture();
//                
//            }
            
            return _hasNext(nanos);

        }
        
        /**
         * 
         * @param nanos
         *            The number of nanos seconds remaining until timeout. This
         *            value is decremented until LTE ZERO (0L), at which point
         *            {@link #_hasNext(long)} will timeout.
         * 
         * @return iff an element is available before the timeout has expired.
         */
        private boolean _hasNext(long nanos) {

            final long begin = System.nanoTime();

            long lastTime = begin;

            // #of times that we poll() the queue.
            int ntries = 0;

            // the timeout until the next log message.
            long logTimeout = initialLogTimeout;

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
             * 
             * Note: These tests are in order of _speed_. poll() and isEmpty()
             * both acquire a lock, so they are the slowest. BlockingBuffer.open
             * is volatile. nextE is just a local field.
             */
            while (nextE != null || BlockingBuffer.this.open
                    || ((nextE = queue.poll()) != null)) {

                if (nextE != null) {

                    /*
                     * Either nextE was already bound or the queue was not empty
                     * and we took the head of the queue using the non-blocking
                     * poll().
                     * 
                     * Note: poll() is no more expensive than size(). However,
                     * it both tests whether or not the queue is empty and takes
                     * the head of the queue iff it is not empty. Note that we
                     * DO NOT poll unless [nextE] is [null] since that would
                     * cause us to loose the value in [nextE].
                     */
                    return true;

                }
                
                /*
                 * Perform some checks to decide whether the iterator is still
                 * valid.
                 */
                
                if(!open) {
                    
                    if (INFO)
                        log.info("iterator is closed");

                    // check before returning a strong [false] (vs timeout).
                    checkFuture();
                    
                    return false;
                    
                }

                // Note: tests volatile field.
                if (cause != null)
                    throw new IllegalStateException(cause);

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

                    // weak false (timeout).
                    return false;
                    
                }
                
                /*
                 * Poll the queue.
                 * 
                 * Note: DO NOT sleep() here. Polling will use a Condition to
                 * wake up when there is something in the queue. If you sleep()
                 * here then ANY timeout can introduce an unacceptable latency
                 * since many queries can be evaluated in LT 10ms !
                 * 
                 * Note: Even with poll(timeout,unit), a modest timeout will
                 * result in the thread yielding and a drammatic performance
                 * hit. We are better off using a short timeout so that we can
                 * continue to test the other conditions, specifically whether
                 * the buffer was closed asynchronously. Otherwise a client that
                 * writes nothing on the buffer will wind up hitting the timeout
                 * inside of poll(timeout,unit).
                 */

                if (ntries < NSPIN) {

                    /*
                     * This is basically a spin lock (it can spin without
                     * yielding if there is no contention for the queue because
                     * the producer is lagging).
                     */
                    
                    if ((nextE = queue.poll()) != null) {
                        
                        if (DEBUG)
                            log.debug("next: " + nextE);

                        return true;

                    }
                    
                } else {
                    
                    /*
                     * Here we are willing to be put to sleep for a bit. This
                     * can free a CPU that was otherwise caught in a spin on
                     * poll() to run the producer so that we have something to
                     * read.
                     */
                    
                    try {

                        final long timeout = getTimeout(ntries);
                        
                        if ((nextE = queue.poll(timeout, TimeUnit.MILLISECONDS)) != null) {

                            if (DEBUG)
                                log.debug("next: " + nextE);

                            return true;

                        }
                    
                    } catch (InterruptedException ex) {

                        if (INFO)
                            log.info(ex.getMessage());

                        // itr will not deliver any more elements.
                        close();

                        // strong false (interrupted, so itr is closed).
                        checkFuture();
                        
                        return false;

                    }

                }
                
                /*
                 * Nothing available yet on the queue.
                 */

                assert nextE == null;
                
                ntries++;
                
                final long elapsedNanos = (now - begin);

                if (elapsedNanos >= logTimeout) {

                    /*
                     * This could be a variety of things such as waiting on a
                     * mutex that is already held, e.g., an index lock, that
                     * results in a deadlock between the process writing on the
                     * buffer and the process reading from the buffer. If you
                     * are careful with who gets the unisolated view then this
                     * should not be a problem.
                     */

                    // increase timeout until the next log message.
                    logTimeout += Math.min(maxLogTimeout, logTimeout);

                    if (future == null) {

                        /*
                         * This can arise if you fail to set the future on the
                         * buffer such that the iterator can not monitor it. If
                         * the future completes (esp. with an error) then the
                         * iterator can keep looking for another element but the
                         * source is no longer writing on the buffer and nothing
                         * will show up.
                         */

                        log.error("Future not set on buffer.");

                    } else {

                        /*
                         * check future : if there is an error in the producer
                         * task and the producer fails to close() the blocking
                         * buffer then the iterator will appear to be stalled.
                         */
                        
                        checkFuture();
                        
                        log.warn("Iterator is not progressing: ntries="
                                + ntries
                                + ", elapsed="
                                + TimeUnit.MILLISECONDS.convert(elapsedNanos,
                                        TimeUnit.NANOSECONDS) + "ms"
//                                + ", buffer.open=" + BlockingBuffer.this.open
//                                + ", itr.open=" + this.open
//                                        , stackFrame // shows where it was allocated.
//                                        , new RuntimeException() // shows where it is being consumed.
                                );
                        
                    }

                }
                
                // loop again.
                continue;
                
            }

            if (INFO)
                log.info("Exhausted");

            assert isExhausted();
            
            // strong false - the iterator is exhausted.
            checkFuture();
            
            return false;

        }

        public E next(final long timeout, final TimeUnit unit) {

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
            
            if (e.getClass().getComponentType() != null) {
                
                /*
                 * Combine chunk(s)?
                 */

                final long now = System.nanoTime();

                // remaining nanos until timeout.
                nanos = now - startTime;

                if (nanos > 0) {

                    // combine chunks.
                    return combineChunks(e, 1/* nchunks */, now, nanos);

                }
                
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
            
            if(e.getClass().getComponentType() != null) {

                nchunks++;
                nelements+= ((Object[])e).length;
                
            } else {
                
                nelements++;
                
            }
            
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
