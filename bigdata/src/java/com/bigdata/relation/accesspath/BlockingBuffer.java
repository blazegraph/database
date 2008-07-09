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

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * A buffer that will block when it is full. You write elements on the buffer
 * and they can be read using {@link #iterator()}. This class is safe for
 * concurrent writes (multiple threads can use {@link #add(Object)}) but the
 * {@link #iterator()} is not thread-safe (it assumes a single reader).
 * 
 * <p>
 * <strong>Make sure that the thread that sets up the {@link BlockingBuffer} and
 * submits a task that writes on the buffer sets the {@link Future} on the
 * {@link BlockingBuffer} so that the iterator can monitor the future, detect if
 * it has been cancelled, and throw out the exception from the future back to
 * the client. Failure to do this can lead to the iterator not terminating!</strong>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlockingBuffer<E> implements IBlockingBuffer<E> {
    
    protected static Logger log = Logger.getLogger(BlockingBuffer.class);
    
    /**
     * <code>true</code> until the buffer is {@link #close()}ed.
     */
    private volatile boolean open = true;

    private volatile Throwable cause = null;
    
    /**
     * Used to coordinate the reader and the writer.
     */
    private final ArrayBlockingQueue<E> queue;

    /**
     * The singleton for the iterator used to read from this buffer.
     */
    private final IChunkedOrderedIterator<E> iterator;

    /**
     * The element write order IFF known.
     */
    private final IKeyOrder<E> keyOrder;

    /** Optional filter to keep elements out of the buffer. */
    private final IElementFilter<E> filter;
    
    private final int minChunkSize;
    
    private final long chunkTimeout;
    
    /**
     * The default capacity for the internal buffer. Chunks can not be larger
     * than this.
     */
    public static transient final int DEFAULT_CAPACITY = 5000;
    
    /**
     * The default minimum chunk size. If the buffer has fewer than this many
     * elements and the buffer has not been {@link #close() closed} then it will
     * wait up to {@link #DEFAULT_CHUNK_TIMEOUT} milliseconds before returning
     * the next chunk based on what is already in the buffer.
     */
    public static transient final int DEFAULT_MIN_CHUNK_SIZE = 100;
    
    /**
     * The maximum amount of time to wait in
     * {@link BlockingIterator#nextChunk()}.
     */
    public static transient final int DEFAULT_CHUNK_TIMEOUT = 1000/*ms*/;

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
     * 
     * @param capacity
     *            The capacity of the buffer.
     */
    public BlockingBuffer(int capacity) {

        this(capacity, null/* keyOrder */, null/*filter*/);

    }
    
    /**
     * 
     * @param capacity
     *            The capacity of the buffer.
     * @param keyOrder
     *            The visitation order in which the elements will be
     *            <em>written</em> onto the buffer and <code>null</code> if
     *            you do not have a <em>strong</em> guarentee for the write
     *            order.
     * @param filter
     *            An optional filter for elements to be kept out of the buffer.
     */
    public BlockingBuffer(int capacity, IKeyOrder<E> keyOrder, IElementFilter filter) {
       
        this(capacity, keyOrder, filter, DEFAULT_MIN_CHUNK_SIZE, DEFAULT_CHUNK_TIMEOUT);
        
    }

    /**
     * 
     * @param capacity
     *            The capacity for the internal buffer. Chunks can not be larger
     *            than this.
     * @param keyOrder
     *            The visitation order in which the elements will be
     *            <em>written</em> onto the buffer and <code>null</code> if
     *            you do not have a <em>strong</em> guarentee for the write
     *            order.
     * @param filter
     *            An optional filter for elements to be kept out of the buffer.
     * @param minChunkSize
     *            The minimum chunk size. If the buffer has fewer than this many
     *            elements and the buffer has not been {@link #close() closed}
     *            then {@link #iterator()} will wait up to
     *            {@link #DEFAULT_CHUNK_TIMEOUT} milliseconds before returning
     *            the next chunk based on what is already in the buffer.
     * @param chunkTimeout
     *            The maximum amount of time the {@link #iterator()} will wait
     *            to satisify the minimum chunk size.
     */
    public BlockingBuffer(int capacity, IKeyOrder<E> keyOrder,
            IElementFilter filter, int minChunkSize, long chunkTimeout) {

        if (capacity <= 0)
            throw new IllegalArgumentException();

        if (minChunkSize < 0)
            throw new IllegalArgumentException();

        if (minChunkSize > capacity)
            throw new IllegalArgumentException();
        
        if (chunkTimeout < 0)
            throw new IllegalArgumentException();

        this.queue = new ArrayBlockingQueue<E>(capacity);
        
        this.iterator = new BlockingIterator();

        this.keyOrder = keyOrder;
        
        this.filter = filter;
        
        this.minChunkSize = minChunkSize;
        
        this.chunkTimeout = chunkTimeout;
        
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

    public void close() {
    
        this.open = false;

        log.info("closed.");
        
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
    
    /**
     * May be overriden to filter elements allowed into the buffer. The default
     * implementation allows all elements into the buffer.
     * 
     * @param e
     *            A element.
     * 
     * @return true if the element is allowed into the buffer.
     */
    protected boolean accept(E e) {

        if (filter != null) {

            return filter.accept(e);

        }
        
        return true;
        
    }
    
    public boolean add(E e) {

        assertOpen();

        if (!accept(e)) {

            if (log.isDebugEnabled())
                log.debug("reject: " + e.toString());

            return false;

        }

//        if (log.isInfoEnabled())
//            log.info("add: " + e.toString());

        // wait if the queue is full.
        while (true) {

            try {

                if (queue.offer(e, 100, TimeUnit.MILLISECONDS)) {

                    // item now on the queue.

                    if (log.isDebugEnabled())
                        log.debug("added: " + e.toString());
                    
                    return true;
                    
                }
                
            } catch (InterruptedException ex) {

                close();

                throw new RuntimeException("Buffer closed by interrupt", ex);
                
            }
            
        }
        
    }

    public long flush() {

        return 0L;
        
    }

    public void reset() {
        
        queue.clear();
        
    }
    
    /**
     * The returned iterator is NOT thread-safe and does NOT support remove().
     * 
     * @return The iterator (this is a singleton).
     */
    public IChunkedOrderedIterator<E> iterator() {

        return iterator;
        
    }

    /**
     * An inner class that reads from the buffer. This is not thread-safe - it
     * makes no attempt to be atomic in its operations in {@link #next()} or
     * {@link #nextChunk()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class BlockingIterator implements IChunkedOrderedIterator<E> {
        
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
         * Create an iterator that reads from the buffer.
         */
        private BlockingIterator() {
       
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
         */
        public void close() {

            log.debug("");
            
            if (!open)
                return;

            open = false;

            if (future != null && !future.isDone()) {

                /*
                 * If the source is still running and we close the iterator then
                 * the source will block once the iterator fills up and it will
                 * fail to progress. To avoid this, and to have the source
                 * process terminate eagerly if the client closes the iterator,
                 * we cancel the future if it is not yet done.
                 */
                
                if(log.isDebugEnabled()) {
                    
                    log.debug("will cancel future: "+future);
                    
                }
                
                future.cancel(true/* mayInterruptIfRunning */);
                
                if(log.isDebugEnabled()) {
                    
                    log.debug("did cancel future: "+future);
                    
                }

            }

        }

        private volatile boolean didCheckFuture = false;

        /**
         * Return <code>true</code> if there are elements in the buffer that
         * can be visited and blocks when the buffer is empty. Returns false iff
         * the buffer is {@link BlockingBuffer#close()}ed.
         * 
         * @throws RuntimeException
         *             if the current thread is interrupted while waiting for
         *             the buffer to be {@link BlockingBuffer#flush()}ed.
         */
        public boolean hasNext() {

            log.debug("begin");

            assertNotAborted();

            if(!open) {
                
                log.info("iterator is closed");
                
                return false;
                
            }

            /*
             * Note: hasNext must wait until the buffer is closed and all
             * elements in the queue have been consumed before it can conclude
             * that there will be nothing more that it can visit. This re-tests
             * whether or not the buffer is open after a timeout and continues
             * to loop until the buffer is closed AND there are no more elements
             * in the queue.
             */
            
            final long begin = System.currentTimeMillis();
            
            while (BlockingBuffer.this.open || !queue.isEmpty()) {

                if (!didCheckFuture && future != null && future.isDone()) {

                    log.info("Future is done");

                    // don't re-execute this code.
                    didCheckFuture = true;
                    
                    /*
                     * Make sure the buffer is closed. In fact, the caller
                     * probably does not need to close the buffer since we do it
                     * here when their task completes.
                     */
                    BlockingBuffer.this.close();

                    try {

                        // look for an error from the Future.
                        future.get();
                        
                    } catch (InterruptedException e) {
                        
                        log.info("Interrupted");
                        
                    } catch (ExecutionException e) {

                        log.error(e,e);
                        
                        throw new RuntimeException(e);
                        
                    }

                    /*
                     * Fall through. If there is anything in the queue then we
                     * still need to drain it!
                     */
                    
                }

                assertNotAborted();

                /*
                 * Use a set limit on wait and recheck whether or not the
                 * buffer has been closed asynchronously.
                 */

                final E spo = queue.peek();

                if (spo == null) {
                    
                    try {
                        Thread.sleep(100/*millis*/);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    
                    final long now = System.currentTimeMillis();
                    
                    final long elapsed = now - begin;

                    // @todo use a movable threshold to avoid too frequent retriggering
                    if (elapsed > 2000) {

                        if (future == null) {

                            /*
                             * This can arise if you fail to set the future on
                             * the buffer such that the iterator can not monitor
                             * it. If the future completes (esp. with an error)
                             * then the iterator can keep looking for another
                             * element but the source is no longer writing on
                             * the buffer and nothing will show up.
                             */
                            
                            log.error("Future not set on buffer");
                            
                        } else {
                            
                            /*
                             * This could be a variety of things such as waiting
                             * on a mutex that is already held, e.g., an index
                             * lock, that results in a deadlock between the
                             * process writing on the buffer and the process
                             * reading from the buffer. If you are careful with
                             * who gets the unisolated view then this should not
                             * be a problem.
                             */
                            
                            log.warn("Iterator is not progressing...");

                        }
                        
                    }
                    
                    continue;
                    
                }
                
                if (log.isDebugEnabled())
                    log.debug("next: " + spo.toString());
                
                return true;
                
            }

            if (log.isInfoEnabled())
                log.info("Exhausted: bufferOpen=" + BlockingBuffer.this.open
                        + ", size=" + queue.size());
            
            return false;

        }

        public E next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            log.debug("");

            assert !queue.isEmpty();
            
            final E spo;

            try {

                spo = queue.take();
                
            } catch(InterruptedException ex) {
                
                close();
                
                throw new RuntimeException("Closed by interrupt", ex);
                
            }
            
            if (log.isDebugEnabled())
                log.debug("next: " + spo.toString());

            return spo;

        }

        @SuppressWarnings("unchecked")
        public E[] nextChunk() {

            final long begin = System.currentTimeMillis();
            
            if (!hasNext()) {

                throw new NoSuchElementException();

            }
            
            /*
             * This is thee current size of the buffer. The buffer size MAY grow
             * asynchronously but will not shrink outside of the effect of this
             * method unless it is asynchronously reset().
             * 
             * @todo handle asynchronous reset() safely.
             * 
             * @todo if the buffer size LT MIN_CHUNK_SIZE then await
             * minChunkSize with a timeout of ~250ms - ~1s (config param). This
             * will give the buffer a chance to build up some data and make the
             * chunk-at-a-time processing more efficient.
             */
            final int chunkSize = queue.size();

            E[] chunk = null;

            int n = 0;

            while (n < chunkSize) {

                final E e = next();
                
                if (chunk == null) {

                    chunk = (E[]) java.lang.reflect.Array.newInstance(e
                            .getClass(), chunkSize);

                }
                
                // add to this chunk.
                chunk[n++] = e;
                
            }
            
            if(log.isInfoEnabled()) {
            
                final long elapsed = System.currentTimeMillis() - begin;
                
                log.info("obtained chunk: size="+n+", elapsed="+elapsed+" ms");
                
            }
            
            return chunk;
            
        }

        public IKeyOrder<E> getKeyOrder() {
            
            return keyOrder;
            
        }
        
        public E[] nextChunk(IKeyOrder<E> keyOrder) {

            if (keyOrder == null)
                throw new IllegalArgumentException();

            final E[] chunk = nextChunk();

            if (!keyOrder.equals(getKeyOrder())) {

                // sort into the required order.

                Arrays.sort(chunk, 0, chunk.length, keyOrder.getComparator());

            }

            return chunk;

        }
        
        /**
         * The operation is not supported.
         */
        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }
        
    }
    
}
