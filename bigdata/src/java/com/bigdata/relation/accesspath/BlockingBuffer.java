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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.striterator.IAsynchronousIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

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
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlockingBuffer<E> implements IBlockingBuffer<E> {
    
    protected static Logger log = Logger.getLogger(BlockingBuffer.class);
    
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
    private final IChunkedOrderedIterator<E> iterator;

    /**
     * The element write order IFF known.
     */
    private final IKeyOrder<E> keyOrder;

    /**
     * Optional filter to keep elements out of the buffer.
     */
    private final IElementFilter<E> filter;
    
    /**
     * The default capacity for the internal buffer. Chunks can not be larger
     * than this.
     */
    protected static transient final int DEFAULT_CAPACITY = 5000;
    
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

        this(capacity, null/* keyOrder */, null/* filter */);

    }
    
    /**
     * 
     * @param capacity
     *            The capacity of the buffer.
     * @param keyOrder
     *            The visitation order in which the elements will be
     *            <em>written</em> onto the buffer and <code>null</code>
     *            unless you have a <em>strong</em> guarentee for the
     *            visitation order or <code>null</code> if there is no
     *            inherent order.
     * @param filter
     *            A filter for elements to be kept out of the buffer (optional).
     */
    public BlockingBuffer(int capacity, IKeyOrder<E> keyOrder,
            IElementFilter<E> filter) {

        this(new ArrayBlockingQueue<E>(capacity), keyOrder, filter);

    }
    
    /**
     * Core ctor.
     * 
     * @param queue
     *            The queue on which elements will be buffered. Elements will be
     *            {@link #add(Object) added} to the queue and drained by the
     *            {@link #iterator()}.
     * @param keyOrder
     *            The visitation order in which the elements will be
     *            <em>written</em> onto the buffer and <code>null</code>
     *            unless you have a <em>strong</em> guarentee for the
     *            visitation order or <code>null</code> if there is no
     *            inherent order.
     * @param filter
     *            A filter for elements to be kept out of the buffer (optional).
     */
    public BlockingBuffer(BlockingQueue<E> queue, IKeyOrder<E> keyOrder,
            IElementFilter<E> filter) {
    
        if (queue == null)
            throw new IllegalArgumentException();
        
        this.queue = queue;
        
        this.iterator = new BlockingIterator();

        this.keyOrder = keyOrder;
        
        this.filter = filter;
        
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

        notifyIterator();
        
        if (INFO)
            log.info("closed.");
        
    }
    
    private final void notifyIterator() {

        synchronized(iterator) {
            
            iterator.notify();
            
        }

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

    /**
     * @todo this is not optimized. see if query is faster when single threaded.
     *       if so, then this will need to be optimized and we will need to use
     *       rule-local unsynchronized buffers that flush onto the solution
     *       buffer.
     */
    public void add(int n, E[] a) {

        for (int i = 0; i < n; i++) {

            add(a[i]);

        }

    }
    
    public void add(E e) {

        assertOpen();

        if (!accept(e)) {

            if (DEBUG)
                log.debug("reject: " + e.toString());

            return;

        }

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
     * The returned iterator is NOT thread-safe and does NOT support remove().
     * It will implement {@link IAsynchronousIterator}.
     * 
     * @return The iterator (this is a singleton).
     */
    public IChunkedOrderedIterator<E> iterator() {

        return iterator;
        
    }

    /**
     * An inner class that reads from the buffer. This is not thread-safe - it
     * makes no attempt to be atomic in its operations in {@link #hasNext()},
     * {@link #next()} or {@link #nextChunk()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class BlockingIterator implements IChunkedOrderedIterator<E>, IAsynchronousIterator<E> {
        
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
         */
        public void close() {

            if (DEBUG)
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
                
                if(DEBUG) {
                    
                    log.debug("will cancel future: "+future);
                    
                }
                
                future.cancel(true/* mayInterruptIfRunning */);
                
                if(DEBUG) {
                    
                    log.debug("did cancel future: "+future);
                    
                }

            }

        }

        private volatile boolean didCheckFuture = false;
        
        private final void checkFuture() {

            if (!didCheckFuture && future != null && future.isDone()) {

                if (INFO)
                    log.info("Future is done");

                // don't re-execute this code.
                didCheckFuture = true;

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
         * Return <code>true</code> if there are elements in the buffer that
         * can be visited and blocks when the buffer is empty. Returns false iff
         * the buffer is {@link BlockingBuffer#close()}ed.
         * 
         * @throws RuntimeException
         *             if the current thread is interrupted while waiting for
         *             the buffer to be {@link BlockingBuffer#flush()}ed.
         */
        public boolean hasNext() {

            if (DEBUG)
                log.debug("begin");

            final long begin = System.currentTimeMillis();
            
            int ntries = 0;
            // initial delay before the 1st log message.
            long timeout = 250;
            // maximum delay between log messages (NOT the wait time).
            final long maxTimeout = 10000;

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
            
            while (BlockingBuffer.this.open || nextE != null || !queue.isEmpty()) {

                checkFuture();
                
                assertNotAborted();

                if(!open) {
                    
                    if (INFO)
                        log.info("iterator is closed");
                    
                    return false;
                    
                }
                
                if (nextE != null) {

                    // we already have the next element on hand.
                    return true;
                    
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
                    
                    return false;
                    
                }
                
                /*
                 * Nothing available yet on the queue.
                 */

                // increase the timeout.
                timeout = Math.min(maxTimeout, timeout *= 2);

                ntries++;

                final long now = System.currentTimeMillis();

                final long elapsed = now - begin;

                /*
                 * This could be a variety of things such as waiting on a mutex
                 * that is already held, e.g., an index lock, that results in a
                 * deadlock between the process writing on the buffer and the
                 * process reading from the buffer. If you are careful with who
                 * gets the unisolated view then this should not be a problem.
                 */

                log.warn("Iterator is not progressing: ntries=" + ntries
                        + ", elapsed=" + elapsed);

                if (elapsed > 2000 && future == null) {

                    /*
                     * This can arise if you fail to set the future on the
                     * buffer such that the iterator can not monitor it. If the
                     * future completes (esp. with an error) then the iterator
                     * can keep looking for another element but the source is no
                     * longer writing on the buffer and nothing will show up.
                     */

                    log.error("Future not set on buffer");

                }

                continue;
                
            }

            if (INFO)
                log.info("Exhausted: bufferOpen=" + BlockingBuffer.this.open
                        + ", nextE=" + nextE != null);

            return false;

        }

        public E next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            assert nextE != null;
            
            // we already have the next element.
            final E e = nextE;

            nextE = null;
 
            nelements++;
            
            if (DEBUG)
                log.debug("next: nchunks=" + nchunks + ", nelements="
                        + nelements + ", " + e);

            return e;

        }

        public E[] nextChunk() {

            return nextChunk(//
//                    Math.min(1000, capacity), // minChunkSize
                    1000, // minChunkSize
                    500L, // timeout
                    TimeUnit.MILLISECONDS // units.
                    );
            
        }
        
//        @SuppressWarnings("unchecked")
        public E[] nextChunk(int minChunkSize, long timeout, TimeUnit unit) {

            if (INFO)
                log.info("minChunkSize=" + minChunkSize + ", timeout="
                        + timeout + ", unit=" + unit);
            
            final long begin = System.currentTimeMillis();

            // @todo hasNext() does not respect the timeout.
            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            int chunkSize;
            synchronized(this) {
                
            // note: timeout when nanos<=0.
            long nanos = unit.toNanos(timeout);
            
            long lastTime = System.nanoTime();

            /*
             * [chunkSize] is the current size of the buffer. The buffer size
             * MAY grow asynchronously but will not shrink outside of the effect
             * of this method unless it is asynchronously reset().
             * 
             * Note: chunkSize = queue.size() + 1 since [nextE] is already on
             * hand to be read.
             * 
             * We stop looking as soon as the BlockingBuffer is closed.
             * 
             * @todo handle asynchronous reset() safely.
             */
            while (((chunkSize = queue.size() + 1/* nextE */) < minChunkSize)
                    && nanos > 0 && BlockingBuffer.this.open) {
                
                long now = System.nanoTime();
                
                nanos -= now - lastTime;
                
                lastTime = now;
                
                try {

//                    /*
//                     * FIXME sleeping here is not the best way to handle this,
//                     * but we lack a means to be notified when another element
//                     * is added to the buffer.
//                     * 
//                     * Perhaps use an explicit Lock and Condition on the
//                     * BlockingBuffer so that we will be awakened if another
//                     * element is added? Or even if the buffer reaches a desired
//                     * size?!?
//                     */
//                    Thread.sleep(1/* ms */);

                    wait();
                    
                } catch (InterruptedException ex) {
                    
                    throw new RuntimeException(ex.getMessage());
                    
                }
                
            }
            
            }
            
//            final int chunkSize = queue.size() + 1/* nextE */;

            final E[] chunk;
            {

                /*
                 * Drain everything in the queue.
                 */
                
                final List<E> drained = new ArrayList<E>(chunkSize << 2);

                final int drainCount = queue.drainTo(drained);

//                final E e = (E) drained.get(0);

                chunk = (E[]) java.lang.reflect.Array.newInstance(nextE
                        .getClass(), drainCount + 1/* nextE */);
                
                chunk[0] = nextE;
                
                nextE = null;

                for (int i = 0; i < drainCount; i++) {

                    chunk[i + 1] = drained.get(i);

                }
                
            }
            
// E[] chunk = null;
//
//            int n = 0;
//
//            while (n < chunkSize) {
//
//                final E e = next();
//                
//                assert e != null;
//                
//                if (chunk == null) {
//
//                    chunk = (E[]) java.lang.reflect.Array.newInstance(e
//                            .getClass(), chunkSize);
//
//                }
//                
//                // add to this chunk.
//                chunk[n++] = e;
//                
//            }
            
            nchunks++;
            
            nelements += chunk.length;
            
            if (INFO) {

                final long elapsed = System.currentTimeMillis() - begin;

                log.info("#chunks=" + nchunks + ", #elements=" + nelements
                        + ", chunkSize=" + chunk.length + ", elapsed="
                        + elapsed + " ms, bufferOpen="
                        + BlockingBuffer.this.open);

            }
            
            assert chunk != null;
            
            return chunk;
            
        }

        final public IKeyOrder<E> getKeyOrder() {
            
            return keyOrder;
            
        }
        
        final public E[] nextChunk(IKeyOrder<E> keyOrder) {

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
