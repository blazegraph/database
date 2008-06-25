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

package com.bigdata.join;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * A buffer that will block when it is full. You write elements on the buffer
 * and they can be read using {@link #iterator()}. This class is safe for
 * concurrent writes (multiple threads can use {@link #add(Object)}) but the
 * {@link #iterator()} is not thread-safe (it assumes a single reader).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlockingBuffer<E> implements IBuffer<E> {
    
    protected static Logger log = Logger.getLogger(BlockingBuffer.class);
    
    /**
     * <code>true</code> until the buffer is {@link #close()}ed.
     */
    private volatile boolean open = true;

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

    private final int minChunkSize;
    
    private final long chunkTimeout;
    
    /**
     * The default capacity for the internal buffer. Chunks can not be larger
     * than this.
     */
    protected static transient final int DEFAULT_CAPACITY = 1000;
    
    /**
     * The default minimum chunk size. If the buffer has fewer than this many
     * elements and the buffer has not been {@link #close() closed} then it will
     * wait up to {@link #DEFAULT_CHUNK_TIMEOUT} milliseconds before returning
     * the next chunk based on what is already in the buffer.
     */
    protected static transient final int DEFAULT_MIN_CHUNK_SIZE = 100;
    
    /**
     * The maximum amount of time to wait in
     * {@link BlockingIterator#nextChunk()}.
     */
    protected static transient final int DEFAULT_CHUNK_TIMEOUT = 1000/*ms*/;

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

        this(capacity, null/* keyOrder */);

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
     */
    public BlockingBuffer(int capacity, IKeyOrder<E> keyOrder) {
       
        this(capacity, keyOrder, DEFAULT_MIN_CHUNK_SIZE, DEFAULT_CHUNK_TIMEOUT);
        
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
    public BlockingBuffer(int capacity, IKeyOrder<E> keyOrder, int minChunkSize, long chunkTimeout) {
        
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
        
        this.minChunkSize = minChunkSize;
        
        this.chunkTimeout = chunkTimeout;
        
    }

    /**
     * Always returns ZERO (0).
     */
    public int getJustificationCount() {
        
        return 0;
        
    }

    private void assertOpen() {
        
        if(!open) {
            
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
     * Signal that no more data will be written on this buffer (this is required
     * in order for the {@link #iterator()} to know when no more data will be
     * made available).
     */
    public void close() {
        
        this.open = false;

        log.info("closed.");
        
    }
    
    /**
     * May be overriden to filter elements allowed into the buffer. The default
     * implementation allows all elements into the buffer.
     * 
     * @param e
     *            A element.
     * @return true if the element is allowed into the buffer.
     */
    protected boolean isValid(E e) {
        
        return true;
        
    }
    
    /**
     * Adds the elements to the buffer.
     */
    public boolean add(E spo) {

        assertOpen();

        if (!isValid(spo)) {

            if (log.isInfoEnabled())
                log.info("reject: " + spo.toString());

            return false;

        }

        if (log.isInfoEnabled())
            log.info("add: " + spo.toString());

        // wait if the queue is full.
        while (true) {

            try {

                if (queue.offer(spo, 100, TimeUnit.MILLISECONDS)) {

                    // item now on the queue.

                    if (log.isInfoEnabled())
                        log.info("added: " + spo.toString());
                    
                    return true;
                    
                }
                
            } catch (InterruptedException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }
        
    }

    /**
     * This is a NOP since the {@link #iterator()} is the only way to consume
     * data written on the buffer.
     * 
     * @return ZERO (0L)
     */
    public long flush() {

        return 0L;
        
    }

    public void reset() {
        
        queue.clear();
        
    }
    
    /**
     * Return an iterator reading from the buffer. The elements will be visited
     * in the order in which they were written on the buffer. The returned
     * iterator is NOT thread-safe and does NOT support remove().
     * 
     * @return The iterator (this is a singleton).
     */
    public IChunkedOrderedIterator<E> iterator() {

        return iterator;
        
    }

    /**
     * An inner class that reads from the buffer. This is not thread-safe
     * because it makes to attempt to be atomic in its operations in
     * {@link #next()} or {@link #nextChunk()}.
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
         * Notes that the iterator is closed and hence may no longer be read.
         */
        public void close() {

            if (!open)
                return;

            open = false;

        }

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
            while (BlockingBuffer.this.open || !queue.isEmpty()) {

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
                    
                    continue;
                    
                }
                
//                if (filter != null && filter.isMatch(spo)) {
//
//                    // rejected by the filter.
//
//                    if (log.isInfoEnabled())
//                        log.info("reject: " + spo.toString(store));
//
//                    // consume the head of the queue.
//                    try {
//                        buffer.take();
//                    } catch(InterruptedException ex) {
//                        throw new RuntimeException(ex);
//                    }
//                    
//                    continue;
//                    
//                }
                
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

            assert !queue.isEmpty();
            
            final E spo;

            try {

                spo = queue.take();
                
            } catch(InterruptedException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            if (log.isInfoEnabled())
                log.info("next: " + spo.toString());

            return spo;

        }

        @SuppressWarnings("unchecked")
        public E[] nextChunk() {

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
