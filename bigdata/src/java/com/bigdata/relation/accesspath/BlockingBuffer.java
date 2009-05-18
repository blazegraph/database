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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.rdf.store.BigdataSolutionResolverator;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;
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
 * at least one join; (c) by the {@link BigdataStatementIteratorImpl}, which is
 * used by the RDF DB for high-level query with no joins; and by the
 * {@link BigdataSolutionResolverator}, which is used by the RDF DB high-level
 * query that produces binding sets.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlockingBuffer<E> implements IBlockingBuffer<E> {

    /**
     * Warning messages are emitted if either the producer or the consumer is
     * stalled. When {@link Logger#isInfoEnabled()} is <code>true</code>,
     * those log messages will be include stack traces which can help to
     * identify the the consumer or producer.
     */
    protected static final Logger log = Logger.getLogger(BlockingBuffer.class);
    
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
    private static final long getTimeoutMillis(final int ntries) {
        
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
    public static transient final int DEFAULT_PRODUCER_QUEUE_CAPACITY = 5000;

    /**
     * The default target chunk size for
     * {@link BlockingBuffer.BlockingIterator#next()}.
     */
    public static transient final int DEFAULT_CONSUMER_CHUNK_SIZE = 10000;

    /**
     * The default timeout in milliseconds during which chunks of elements may
     * be combined by {@link BlockingBuffer.BlockingIterator#next()}.
     */
    public static transient final long DEFAULT_CONSUMER_CHUNK_TIMEOUT = 20;

    /**
     * The unit in which {@link #DEFAULT_CONSUMER_CHUNK_TIMEOUT} is expressed
     * (milliseconds).
     */
    public static transient final TimeUnit DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    /**
     * The target chunk size for the chunk combiner.
     * 
     * @see #DEFAULT_CONSUMER_CHUNK_SIZE
     */
    private final int chunkCapacity;

    /**
     * The target chunk size for the chunk combiner.
     * 
     * @see #DEFAULT_CONSUMER_CHUNK_SIZE
     */
    public final int getChunkSize() {

        return chunkCapacity;
        
    }
    
    /**
     * The maximum time to wait in nanoseconds for another chunk to come along
     * so that we can combine it with the current chunk for {@link #next()}. A
     * value of ZERO(0) disables chunk combiner.
     * 
     * @see #DEFAULT_CONSUMER_CHUNK_TIMEOUT
     */
    private final long chunkTimeout;

    /**
     * The maximum time to wait in nanoseconds for another chunk to come along
     * so that we can combine it with the current chunk for {@link #next()}. A
     * value of ZERO(0) disables chunk combiner.
     * 
     * @see #DEFAULT_CONSUMER_CHUNK_TIMEOUT
     */
    public final long getChunkTimeout() {

        return chunkTimeout;
        
    }
    
    /**
     * <code>true</code> iff the data in the buffered are chunks of elements
     * presented in their natural sort order. When <code>true</code>, the
     * {@link BlockingIterator} will apply a merge sort if it combines chunks so
     * that the combined chunk remains in the natural order for the element
     * type.
     */
    private final boolean ordered;
    
    private volatile Future future;
    
    public void setFuture(final Future future) {
    
        synchronized (this) {
        
            if (future == null)
                throw new IllegalArgumentException();

            if (this.future != null)
                throw new IllegalStateException();

            this.future = future;

        }
        
    }

    /**
     * The {@link Future} set by {@link #setFuture(Future)}.
     * 
     * @return The {@link Future} -or- <code>null</code> if no {@link Future}
     *         has been set.
     * 
     * @todo There should be a generic type for this.
     */
    public Future getFuture() {
        
        /*
         * Note: [future] is volatile so we do not need to be synchronized to
         * read the reference (changes to the reference will be visible anyway).
         */

        return future;
        
    }
    
    /**
     *
     */
    public BlockingBuffer() {
        
        this(DEFAULT_PRODUCER_QUEUE_CAPACITY);
        
    }
    
    /**
     * Ctor automatically provisions an appropriate {@link BlockingQueue}.
     * 
     * @param capacity
     *            The capacity of the buffer. When the generic type <i>&lt;E&gt;</i>
     *            is an array type, then this is the <i>chunkOfChunksCapacity</i>
     *            and small chunks will be automatically combined based on
     *            availability and latency. When zero (0) a
     *            {@link SynchronousQueue} will be used. Otherwise an
     *            {@link ArrayBlockingQueue} of the given capacity is used.
     */
    public BlockingBuffer(final int capacity) {

       this(capacity,
            DEFAULT_CONSUMER_CHUNK_SIZE,
            DEFAULT_CONSUMER_CHUNK_TIMEOUT,
            DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT
            ); 

    }
    
    /**
     * 
     * @param capacity
     *            The capacity of the buffer. When the generic type <i>&lt;E&gt;</i>
     *            is an array type, then this is the <i>chunkOfChunksCapacity</i>
     *            and small chunks will be automatically combined based on
     *            availability and latency. When zero (0) a
     *            {@link SynchronousQueue} will be used. Otherwise an
     *            {@link ArrayBlockingQueue} of the given capacity is used.
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
     */
    public BlockingBuffer(final int capacity, final int chunkSize,
            final long chunkTimeout, final TimeUnit chunkTimeoutUnit) {

        this(capacity == 0 ? new SynchronousQueue<E>()
                : new ArrayBlockingQueue<E>(capacity), //
                chunkSize,//
                chunkTimeout,//
                chunkTimeoutUnit,//
                false// ordered
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
     * @param ordered
     *            When <code>true</code> the data are asserted to be ordered
     *            and a merge sort will be applied if chunks are combined such
     *            that the combined chunks will also be ordered (this has no
     *            effect unless the generic type of the buffer is an array
     *            type).
     */
    public BlockingBuffer(final BlockingQueue<E> queue,
            final int chunkCapacity, final long chunkTimeout,
            final TimeUnit chunkTimeoutUnit, final boolean ordered) {
    
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

        this.ordered = ordered;
        
        this.iterator = new BlockingIterator();

        /*
         * The stack frame in which the buffer was allocated. This gives you a
         * clue about who is the producer that will write on the buffer.
         * 
         * Note: This is only allocated for debugging purposes. Otherwise the
         * stack frame should not be taken.
         */
        if (log.isInfoEnabled()) {

            this.openStackFrame = new RuntimeException(
                    MSG_ALLOCATION_STACK_FRAME);
            
        }
        
    }

    /**
     * Label for messages displaying the stack frame within which the
     * {@link BlockingBuffer} was allocated.
     * 
     * @see #openStackFrame
     */
    private static transient final String MSG_ALLOCATION_STACK_FRAME = "Buffer Allocation Stack Frame";

    /**
     * Label for messages displaying the stack frame within which the
     * {@link BlockingBuffer} was {@link #close() closed}.
     * 
     * @see #closeStackFrame
     */
    private static transient final String MSG_CLOSED_STACK_FRAME = "Buffer Closed Stack Frame";
    
    /**
     * Label for messages displaying the stack frame of the producer writing on
     * the {@link BlockingBuffer} using {@link #add(Object)}.
     */
    private static transient final String MSG_PRODUCER_STACK_FRAME = "Buffer Producer Stack Frame";

    /**
     * Label for messages displaying the stack frame of the consumer draining
     * the {@link BlockingBuffer} using the {@link BlockingIterator}.
     */
    private static transient final String MSG_CONSUMER_STACK_FRAME = "Buffer Consumer Stack Frame";

    /**
     * Stack frame where the buffer was allocated. This is allocated iff
     * {@link Logger#isInfoEnabled()} is <code>true</code> for {@link #log}.
     */
    private RuntimeException openStackFrame;
    
    /**
     * Stack frame where the buffer was closed. This is allocated iff
     * {@link Logger#isInfoEnabled()} is <code>true</code> for {@link #log}.
     */
    private RuntimeException closeStackFrame;

    public String toString() {

        final StringBuilder sb = new StringBuilder();

        sb.append("BlockingBuffer");

        sb.append("{ open=" + open);

        sb.append(", hasFuture=" + (future != null));

        sb.append(", elementCount=" + elementCount);

        sb.append(", chunkCount=" + chunkCount);

        if (true || log.isInfoEnabled()) {

            /*
             * Note: These data are only approximate and most BlockingQueue
             * implementations will obtain a when when you call size() or
             * remainingCapacity().
             */

            sb.append(", size~=" + queue.size());

            sb.append(", remainingCapacity~=" + queue.remainingCapacity());
            
        }
        
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

            if (openStackFrame != null) {

                /*
                 * Purely for debugging.
                 */

                log.warn(openStackFrame);

            }
            if (closeStackFrame != null) {

                /*
                 * Purely for debugging.
                 */

                log.warn(closeStackFrame);

            }

            if (cause != null) {

                throw new BufferClosedException(cause);
                
            }
            
            throw new BufferClosedException();
            
        }
        
    }

    public boolean isEmpty() {

        return queue.isEmpty();
        
    }

    /**
     * This reports the #of items in the queue. 
     * 
     * @see #getElementsOnQueueCount()
     */
    public int size() {

        return queue.size();
        
    }

    public boolean isOpen() {
        
        return open;
        
    }
    
    /**
     * Closes the {@link BlockingBuffer} such that it will not accept new
     * elements (this is a NOP if unless the buffer is open). Once the buffer is
     * closed, the {@link BlockingIterator} will drain any elements remaining in
     * the {@link BlockingBuffer} and then report <code>false</code> for
     * {@link BlockingIterator#hasNext()) (this does NOT close the
     * {@link BlockingIterator}).
     */
    synchronized public void close() {
    
        if (open) {
         
            try {

//                if (log.isInfoEnabled()) {
//
//                    // Note: Stack trace is to show WHO closed the buffer
//                    log.info("Closing buffer: ", new RuntimeException(
//                            "Closing buffer."));
//                    
//                }

                if (log.isInfoEnabled()) {
                
                    this.closeStackFrame = new RuntimeException(
                            MSG_CLOSED_STACK_FRAME);               
                
                }
                
//                if (openStackFrame != null) {
//
//                    /*
//                     * Purely for debugging.
//                     */
//
////                    log.warn("closing buffer at", new RuntimeException());
//
//                    log.warn("buffer allocated at", openStackFrame);
//
//                }
                
            } finally {

                this.open = false;

            }

        }
        
    }

    public void abort(final Throwable cause) {

        if (cause == null)
            throw new IllegalArgumentException();

        if (log.isInfoEnabled())
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
     * The #of elements on the queue.  When the queue uses a scalar element 
     * type this will track the {@link #size()} very closely.  However, when
     * the queue uses an array element type, this will be the sum of the #of
     * elements across all arrays on the queue.
     */
    private AtomicLong elementsOnQueueCount = new AtomicLong(0L);
    
    /**
     * The #of elements on the queue.  When the queue uses a scalar element 
     * type this will track the {@link #size()} very closely.  However, when
     * the queue uses an array element type, this will be the sum of the #of
     * elements across all arrays on the queue.
     */
    public long getElementsOnQueueCount() {
       
        return elementsOnQueueCount.get();
        
    }
    
    /**
     * @throws BufferClosedException
     *             if the buffer has been {@link #close()}d.
     * @throws RuntimeException
     *             if the caller's {@link Thread} is interrupted. The
     *             {@link RuntimeException} will wrap the
     *             {@link InterruptedException} as its cause.
     */
    public void add(final E e) {

        add(e, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        
    }

    /**
     * Add element to the buffer.
     * 
     * @param e
     *            The element.
     * @param timeout
     *            The timeout.
     * @param unit
     *            The unit in which the timeout is expressed.
     * 
     * @return <code>true</code> iff the element was added to the buffer (
     *         <code>false</code> indicates that the timout expired before the
     *         element could be added to the buffer).
     */
    public boolean add(final E e, final long timeout, final TimeUnit unit) {
        
        assertOpen();

        if (e == null)
            throw new IllegalArgumentException();

        if (e.getClass().getComponentType() != null) {

            if (((Object[]) e).length == 0) {

                if(log.isInfoEnabled())
                    log.info("Empty chunk.");
                
                // empty chunk.
                return true;

            }
            
        }

        final long begin = System.nanoTime();
        
        // wait if the queue is full.
        int ntries = 0;
        // initial delay before the 1st log message.
        long logTimeout = initialLogTimeout;
        
        // nanoseconds remaining until the timeout.
        long nanos;

        while ((nanos = (System.nanoTime() - begin)) > 0) {

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

                final long offerTimeoutNanos = Math
                        .min(nanos, TimeUnit.MILLISECONDS
                                .toNanos(getTimeoutMillis(ntries)));

                try {

                    // offer (staged timeout, not to exceed the time remaining).
                    added = queue.offer(e, offerTimeoutNanos,
                            TimeUnit.NANOSECONDS);

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

                    final String msg = "blocked: ntries="
                            + ntries
                            + ", elapsed="
                            + TimeUnit.MILLISECONDS.convert(elapsed,
                                    TimeUnit.NANOSECONDS)
                            + ", timeout="
                            + TimeUnit.MILLISECONDS.convert(logTimeout,
                                    TimeUnit.NANOSECONDS) + "ms : " + this;

                    if (log.isInfoEnabled() && logTimeout > maxLogTimeout) {
                        /*
                         * Issue warning with stack trace showing who is
                         * blocked.
                         */
                        log.warn(msg, new RuntimeException(
                                MSG_PRODUCER_STACK_FRAME));
                    } else {
                        // issue warning.
                        log.warn(msg);
                    }

                }

                // try to add the element again.
                continue;

            }

            // item now on the queue.

            if (e.getClass().getComponentType() != null) {

                chunkCount++;
                elementCount += ((Object[]) e).length;
                elementsOnQueueCount.addAndGet(((Object[]) e).length);

                if (log.isDebugEnabled())
                    log.debug("added chunk: len=" + ((Object[])e).length);

            } else {

                elementCount++;
                elementsOnQueueCount.incrementAndGet();
                
                if (log.isDebugEnabled())
                    log.debug("added: " + e.toString());

            }

            // success.
            return true;

        }

        // timeout
        return false;
        
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
         * Safe non-blocking representation of the iterator state.
         */
        public String toString() {

            final StringBuilder sb = new StringBuilder();

            sb.append("BlockingIterator");

            sb.append("{ open=" + open);

            sb.append(", futureIsDone=" + futureIsDone);

            sb.append(", bufferIsOpen=" + BlockingBuffer.this.open);

            sb.append(", nextE=" + (nextE != null));

            sb.append(", chunkCount=" + chunkCount);

            sb.append(", elementCount=" + elementCount);

            sb.append("}");

            return sb.toString();

        }

        /**
         * Create an iterator that reads from the buffer.
         */
        private BlockingIterator() {
       
            if (log.isInfoEnabled())
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
         * Mark the iterator as being closed but DO NOT cancel the
         * {@link Future}.
         */
        private void _close() {

            if (log.isDebugEnabled())
                log.debug("");
            
            if (!open)
                return;

            open = false;

        }
        
        /**
         * {@inheritDoc}
         * 
         * @see BlockingBuffer#iterator()
         */
        public void close() {

            _close();
            
            if (future == null) {

                /*
                 * Note: The future is not always set. For example, when a Join
                 * Task creates a BlockingBuffer for each sink there is an
                 * inversion of control. The JoinTask is populating the
                 * BlockingBuffer in its own thread(s) while the sink is
                 * draining the BlockingBuffer independely. However, the more
                 * common case has a task that is created to write on the
                 * BlockingBuffer and the iterator is then consumed by the
                 * caller (Query works this way).
                 */
                
                if (log.isInfoEnabled()) {

                    /*
                     * Purely for debugging.
                     */

                    final String msg = "Future not set: " + this;

                    log.warn(msg, new RuntimeException());

                    if (openStackFrame != null) {

                        log.warn(msg, openStackFrame);

                    }

                }
                
            } else if (!future.isDone()) {

                if (log.isInfoEnabled()) {
                    
                    /*
                     * Log @ WARN with a stack trace so that you can see who
                     * closed the iterator.
                     */
                    log.warn(this, new RuntimeException("Cancelling future: "
                            + future));
                } else {
                    
                    /*
                     * Otherwise log @ WARN w/o a stack trace.
                     */
                    log.warn("Cancelling future: " + future);
                    
                }

                future.cancel(true/* mayInterruptIfRunning */);

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

                if (log.isInfoEnabled())
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

                    if (log.isInfoEnabled())
                        log.info(e.getMessage());

                    // itr will not deliver any more elements.
                    _close();

                } catch (ExecutionException e) {

                    log.error(e, e);

                    // itr will not deliver any more elements.
                    _close();

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
         * 
         * @todo The if the iterator is interrupted the
         *       {@link IAsynchronousIterator} API hides the interrupt and
         *       closes the iterator. Consider an API change that does not clear
         *       the interrupted status so that callers can notice that they
         *       were interrupted or that simply throws out the
         *       {@link InterruptedException}? This API change would put us
         *       more in line with how Java APIs deal with interrupts might have
         *       consequences for the existing code.
         */
        private boolean _hasNext(long nanos) {

        	// set to true to log stack traces at most once per request.
        	final boolean logOnce = !log.isDebugEnabled();

            /*
             * This false until the producer/consumer stack traces have been
             * logged. The flag is used to prevent repeated logging of those
             * stack traces while the iterator is blocked in this method. The
             * traces will be logged at most once per invocation of this method.
             */
            boolean loggedStackTraces = false;
            
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

                if (nextE == null) {

                    /*
                     * Non-blocking test. This covers the case when the buffer
                     * is still open so we would not have polled the queue in
                     * the while() condition above. Since we did not poll above
                     * we will do it now before testing against the timeout. If
                     * we don't poll the queue before we examine the timeout
                     * then hasNext(timeout) will return false when there is
                     * something already buffered and the timeout is short
                     * (1ns).
                     */

                    nextE = queue.poll();
                
                }
                
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
                    
                    if (log.isDebugEnabled())
                        log.debug("iterator is closed");

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

                    if (log.isDebugEnabled())
                        log.debug("Timeout");

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
                        
                        if (log.isDebugEnabled())
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

                        final long timeout = getTimeoutMillis(ntries);
                        
                        if ((nextE = queue.poll(timeout, TimeUnit.MILLISECONDS)) != null) {

                            if (log.isDebugEnabled())
                                log.debug("next: " + nextE);

                            return true;

                        }
                    
                    } catch (InterruptedException ex) {

                        if (log.isDebugEnabled())
							log.info("Interrupted: " + this, ex);
						else if (log.isInfoEnabled())
							log.info("Interrupted: " + this);

                        // itr will not deliver any more elements.
                        _close();

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

                        final String msg = "Iterator is not progressing: ntries="
                                + ntries
                                + ", elapsed="
                                + TimeUnit.MILLISECONDS.convert(elapsedNanos,
                                        TimeUnit.NANOSECONDS) + "ms : " + this;

                        if (log.isInfoEnabled() && !loggedStackTraces) {
                            
                            // shows stack trace of the consumer.
                            log.warn(msg, new RuntimeException(
                                    MSG_CONSUMER_STACK_FRAME));
                            
                            if (openStackFrame != null) {
                            
                                // shows stack trace where allocated.
                                log.warn(msg, openStackFrame);

                            }
                            
                            if (logOnce) {
								/*
								 * Do not log the stack traces again during this
								 * invocation.
								 */
								loggedStackTraces = true;
							}
                            
                        } else {
                            
                            // log a warning w/o stack trace.
                            log.warn(msg);
                            
                        }
                        
                    }

                }
                
                // loop again.
                continue;
                
            }

            if (log.isInfoEnabled())
                log.info("Exhausted");

            assert isExhausted();
            
            // strong false - the iterator is exhausted.
            checkFuture();
            
            return false;

        }

        public E next(long timeout, final TimeUnit unit) {

        	// elapsed is measured against this timestamp.
            final long startTime = System.nanoTime();
            
        	// convert timeout to nanos seconds.
        	timeout = TimeUnit.NANOSECONDS.convert(timeout, unit);
        	
            // time remaining in nanoseconds.
            long nanos = timeout;
            
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
                    return combineChunks(e, 1/* nchunks */, startTime, timeout);

                }
                
            }

            return e;

        }
        
        /**
         * Return the next element. If the element is an array type, then the
         * optional chunk combiner may be applied to aggregate chunks. When the
         * chunk combiner is applied, this method will block until either a
         * chunk of sufficient size has been accumulated -or- the chunk combiner
         * timeout has been exceeded. In either case, it will then return the
         * chunk which was accumulated.
         */
        public E next() {
            
            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            final E e = _next();
            
            if (chunkTimeout > 0 && e.getClass().getComponentType() != null) {
                
                /*
                 * Combine chunk(s).
                 */

                final E chunk = combineChunks(e, 1/* nchunks */, System
                        .nanoTime(), chunkTimeout);

                if (ordered) {

                    /*
                     * The data in the source chunk(s) are ordered so we apply a
                     * merge sort IFF two or more chunks were combined together
                     * such that the resulting chunk will remain ordered.
                     * 
                     * @todo Verify that this provides us with an efficient
                     * sort. We are merging N arrays of sorted elements.
                     * However, this merge sort does not directly take advantage
                     * of the source chunk boundaries. It would be easy enough
                     * to consider the current element for each source chunk in
                     * turn and take the smallest, writing it onto an output
                     * array. Since we know that we need to combine the chunks
                     * anyway, this might be faster. However, the
                     * combineChunks() method would have to be written
                     * differently so that it returned an array of chunks to
                     * which we could then apply the merge sort IFF the array
                     * had more than one element and the data were known to be
                     * ordered.
                     */
                    
                    final int len0 = ((Object[]) e).length;

                    final int len1 = ((Object[]) chunk).length;

                    if (len1 != len0) {

                        // in place merge sort.
                        ChunkMergeSortHelper.mergeSort((Object[]) chunk);

                    }

                }
                
                return chunk;
                
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
            
            if (e.getClass().getComponentType() != null) {

                nchunks++;
                nelements += ((Object[]) e).length;
                elementsOnQueueCount.addAndGet(-((Object[]) e).length);
                
            } else {
                
                nelements++;
                elementsOnQueueCount.decrementAndGet();
                
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

                return combineChunks(combineNextChunk(e), nchunks + 1,
                        startTime, timeout);

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

                return combineChunks(combineNextChunk(e), nchunks + 1,
                        startTime, timeout);

            }

            // Done.
            if (log.isInfoEnabled())
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
            
            if (e2.length == 0) {
            
                // empty chunk.
                return e;
                
            }
            
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
