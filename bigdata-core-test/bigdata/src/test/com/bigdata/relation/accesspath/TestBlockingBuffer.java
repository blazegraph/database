/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 10, 2008
 */

package com.bigdata.relation.accesspath;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase2;

import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.util.InnerCause;

/**
 * Test suite for {@link BlockingBuffer} and its {@link IAsynchronousIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBlockingBuffer extends TestCase2 {

    /**
     * 
     */
    public TestBlockingBuffer() {
    }

    /**
     * @param arg0
     */
    public TestBlockingBuffer(String arg0) {
        super(arg0);
    }

    private final ExecutorService service = Executors
            .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

    protected void tearDown() throws Exception {

        service.shutdownNow();
        
        super.tearDown();
        
    }
    
    /**
     * Basic test of the ability to add to a buffer with a fixed capacity queue
     * and to drain the elements from the queue including tests of the
     * non-blocking aspects of the API.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_blockingBuffer() throws InterruptedException,
            ExecutionException, TimeoutException {

        final Object e0 = new Object();
        final Object e1 = new Object();
        final Object e2 = new Object();

        final BlockingBuffer<Object> buffer = new BlockingBuffer<Object>(3/* capacity */);

        // buffer is empty.
        assertTrue(buffer.isOpen());
        assertTrue(buffer.isEmpty());
        assertEquals("chunkCount", 0L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 0L, buffer.getElementsAddedCount());

        final IAsynchronousIterator<Object> itr = buffer.iterator();

        // nothing available from the iterator (non-blocking test).
        assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
        assertNull(itr.next(1, TimeUnit.NANOSECONDS));

        // add an element to the buffer - should not block.
        buffer.add(e0);

        // should be one element in the buffer and zero chunks since not array type.
        assertTrue(buffer.isOpen());
        assertFalse(buffer.isEmpty());
        assertEquals("chunkCount", 0L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 1L, buffer.getElementsAddedCount());

        // something should be available now (non-blocking).
        assertTrue(itr.hasNext(1, TimeUnit.NANOSECONDS));

        // something should be available now (blocking).
        assertTrue(itr.hasNext());

        // add another element to the buffer - should not block.
        buffer.add(e1);

        // should be two elements in the buffer and zero chunks
        assertTrue(buffer.isOpen());
        assertFalse(buffer.isEmpty());
        assertEquals("chunkCount", 0L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 2L, buffer.getElementsAddedCount());

        // future of task writing a 3rd element on the buffer.
        final Future<?> producerFuture = service.submit(new Runnable() {
            public void run() {
                
                /*
                 * add another element - should block until we take an element
                 * using the iterator.
                 */
                buffer.add(e2);
                
                /*
                 * itr.hasNext() will block until the buffer is closed. 
                 */
                buffer.close();

            }
        });

        // future of task draining the buffer.
        final Future<?> consumerFuture = service.submit(new Runnable() {
            public void run() {

                assertEquals(e0, itr.next());
                assertEquals(e1, itr.next());
                assertEquals(e2, itr.next());

                // nothing available from the iterator (non-blocking test).
                try {
                    assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
                    assertNull(itr.next(1, TimeUnit.NANOSECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                // will block until we close the buffer.
                assertFalse(itr.hasNext());
                
            }
        });
        
        // wait a little bit for the producer future.
        producerFuture.get(100, TimeUnit.MILLISECONDS);

        // wait a little bit for the consumer future.
        consumerFuture.get(100, TimeUnit.MILLISECONDS);

    }

    /**
     * Test that a thread blocked on add() will be unblocked by
     * {@link BlockingBuffer#close()}. For this test, there is no consumer and
     * there is a single producer. We just fill up the buffer and when it is
     * full, we verify that the producer is blocked. We then use
     * {@link BlockingBuffer#close()} to close the buffer and verify that the
     * producer was woken up.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/707" >
     *      BlockingBuffer.close() does not unblock threads </a>
     */
    public void test_blockingBuffer_close_noConsumer() throws Exception {
      
        // object added to the queue by the producer threads.
        final Object e0 = new Object();

        // the buffer
        final BlockingBuffer<Object> buffer = new BlockingBuffer<Object>(2/* capacity */);

        // Set to the first cause for the producer.
        final AtomicReference<Throwable> producerCause = new AtomicReference<Throwable>();

        // the producer - just drops an object onto the buffer.
        final class Producer implements Runnable {
            @Override
            public void run() {
                try {
                buffer.add(e0);
                } catch(Throwable t) {
                    // set the first cause.
                    if (producerCause.compareAndSet(null/* expect */, t)) {
                        log.warn("First producer cause: " + t);
                    }
                    // ensure that producer does not re-run by throwing out
                    // cause.
                    throw new RuntimeException(t);
                }
            }
        }

        final Producer p = new Producer();
        
        p.run(); // should not block.
        p.run(); // should not block.
        
        final ExecutorService service = Executors
                .newSingleThreadExecutor(DaemonThreadFactory
                        .defaultThreadFactory());
        Future<?> f = null;
        try {

            f = service.submit(new Producer());
            
            Thread.sleep(200/*ms*/);
            
            // The producer should be blocked.
            assertFalse(f.isDone());
            
            // Closed the buffer.
            buffer.close();

            // Verify producer was woken up.
            try {

                f.get(1/* timeout */, TimeUnit.SECONDS);
                
            } catch (ExecutionException ex) {
                
                if (!InnerCause.isInnerCause(ex, BufferClosedException.class)) {
                
                    /*
                     * Should have thrown a BufferClosedException.
                     */
                    
                    fail("Unexpected cause: " + ex, ex);
                    
                }
                
            }
            
        } finally {

            service.shutdownNow();
            
            service.awaitTermination(1/* timeout */, TimeUnit.SECONDS);
            
        }
        
    }
    

    /**
     * Test that a thread blocked on add() will be unblocked by
     * {@link BlockingBuffer#close()}. For this test, there is no consumer and
     * there is a single producer. We just fill up the buffer and when it is
     * full, we verify that the producer is blocked. We then use the object
     * returned by {@link BlockingBuffer#iterator()} to
     * {@link IAsynchronousIterator#close()} to close the buffer and verify that
     * the producer was woken up.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/707" >
     *      BlockingBuffer.close() does not unblock threads </a>
     */
    public void test_blockingBuffer_close_noConsumer_closedThroughIterator() throws Exception {
      
        // object added to the queue by the producer threads.
        final Object e0 = new Object();

        // the buffer
        final BlockingBuffer<Object> buffer = new BlockingBuffer<Object>(2/* capacity */);

        // Set to the first cause for the producer.
        final AtomicReference<Throwable> producerCause = new AtomicReference<Throwable>();

        // the producer - just drops an object onto the buffer.
        final class Producer implements Runnable {
            @Override
            public void run() {
                try {
                buffer.add(e0);
                } catch(Throwable t) {
                    // set the first cause.
                    if (producerCause.compareAndSet(null/* expect */, t)) {
                        log.warn("First producer cause: " + t);
                    }
                    // ensure that producer does not re-run by throwing out
                    // cause.
                    throw new RuntimeException(t);
                }
            }
        }

        final Producer p = new Producer();
        
        p.run(); // should not block.
        p.run(); // should not block.
        
        final ExecutorService service = Executors
                .newSingleThreadExecutor(DaemonThreadFactory
                        .defaultThreadFactory());

        FutureTask<Void> ft = null;
        try {

            // Wrap computation as FutureTask.
            ft = new FutureTask<Void>(new Producer(), (Void) null/* result */);

            /*
             * Set the Future on the BlockingBuffer. This is how it will notice
             * when the iterator is closed.
             */
            buffer.setFuture(ft);

            // Submit computation for evaluation.
            service.submit(ft);
            
            Thread.sleep(200/*ms*/);
            
            // The producer should be blocked.
            assertFalse(ft.isDone());
            
            // Closed the buffer using the iterator.
            buffer.iterator().close();

            // Verify producer was woken up.
            try {

                ft.get(1/* timeout */, TimeUnit.SECONDS);
                
            } catch(CancellationException ex) {
                
                if(log.isInfoEnabled())
                    log.info("Ignoring expected exception: "+ex);
                
            }
//            } catch (ExecutionException ex) {
//                
//                if (!InnerCause.isInnerCause(ex, BufferClosedException.class)) {
//                
//                    /*
//                     * Should have thrown a BufferClosedException.
//                     */
//                    
//                    fail("Unexpected cause: " + ex, ex);
//                    
//                }
//                
//            }
            
        } finally {

            service.shutdownNow();
            
            service.awaitTermination(1/* timeout */, TimeUnit.SECONDS);
            
        }
        
    }
    
    /**
     * Test that threads blocked on add() will be unblocked by
     * {@link BlockingBuffer#close()}. A pool of producer threads run. A
     * (smaller) pool of (slower) consumer thread(s) also runs. The
     * {@link BlockingBuffer} is closed once the producer threads begin to
     * block. We then verify that all producer threads are appropriately
     * notified of the asynchronous cancellation of their request.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/707" >
     *      BlockingBuffer.close() does not unblock threads </a>
     */
    public void test_blockingBuffer_close() throws InterruptedException,
            ExecutionException, TimeoutException {

        // capacity of the target buffer.
        final int BUFFER_CAPACITY = 1000;
        
        // max #of concurrent consumer/producer threads.
        final int POOL_SIZE = 10;
        
        // producer rate.
        final long PRODUCER_RATE_MILLIS = 1;

        // consumer rate (must be slower than the producer!)
        final long CONSUMER_RATE_MILLIS = 2;
        
        assertTrue(CONSUMER_RATE_MILLIS > PRODUCER_RATE_MILLIS);

        // object added to the queue by the producer threads.
        final Object e0 = new Object();

        // the buffer
        final BlockingBuffer<Object> buffer = new BlockingBuffer<Object>(BUFFER_CAPACITY);

        // the iterator draining that buffer.
        final IAsynchronousIterator<Object> itr = buffer.iterator();

        // Set to the first cause for the producer.
        final AtomicReference<Throwable> producerCause = new AtomicReference<Throwable>();

        // Set to the first cause for the consumer.
        final AtomicReference<Throwable> consumerCause = new AtomicReference<Throwable>();

        // the producer - just drops an object onto the buffer.
        final class Producer implements Runnable {
            @Override
            public void run() {
                try {
                buffer.add(e0);
                } catch(Throwable t) {
                    // set the first cause.
                    if (producerCause.compareAndSet(null/* expect */, t)) {
                        log.warn("First producer cause: " + t);
                    }
                    // ensure that producer does not re-run by throwing out
                    // cause.
                    throw new RuntimeException(t);
                }
            }
        }

        // the consumer - drains an object from the buffer iff available.
        final class Consumer implements Runnable {
            @Override
            public void run() {
                try {
                    if (itr.hasNext()) {
                        itr.next();
                    }
                } catch (Throwable t) {
                    // set the first cause.
                    if (consumerCause.compareAndSet(null/* expect */, t)) {
                        log.warn("First consumer cause: " + t);
                    }
                    // ensure that consumer does not re-run by throwing out
                    // cause.
                    throw new RuntimeException(t);
                }
            }
        }

        /*
         * Setup scheduled thread pool. One thread will be the producer. The
         * other is the consumer. We will schedule producer tasks at a higher
         * rate than consumer tasks. Eventually the blocking buffer will block
         * in the producer. Once we notice this condition, we will close() the
         * blocking buffer and verify that the producer thread was appropriately
         * notified.
         */
        final ScheduledExecutorService service = Executors
                .newScheduledThreadPool(POOL_SIZE,
                        DaemonThreadFactory.defaultThreadFactory());

        final ScheduledFuture<?> producerFuture = service.scheduleAtFixedRate(
                new Producer(), 0/* initialDelay */, PRODUCER_RATE_MILLIS,
                TimeUnit.MILLISECONDS);

        final ScheduledFuture<?> consumerFuture = service.scheduleAtFixedRate(
                new Consumer(), 0/* initialDelay */, CONSUMER_RATE_MILLIS,
                TimeUnit.MILLISECONDS);

        try {

            // Wait for the buffer to become full.
            while (true) {
// FIXME Variant where the consumer is terminated before we test the buffer for being full (or simply never run) - this test might even exist already.
                if (buffer.size() == BUFFER_CAPACITY) {
                    
                    // log out message. includes buffer stats.
                    log.warn("Buffer is full: " + buffer.toString());
                    
                    buffer.close();
                    
                    break;
                    
                }

                Thread.sleep(PRODUCER_RATE_MILLIS/* ms */);

            }
            
            // Wait for the consumer to drain the buffer.
            while (!buffer.isEmpty()) {

                // fail if consumer is done. should run until cancelled.
                assertFalse(consumerFuture.isDone());

                Thread.sleep(CONSUMER_RATE_MILLIS/* ms */);

            }
            
            log.info("Buffer has been drained.");
            
            /*
             * The producer should no longer be running. It should have failed
             * when the blocking buffer was closed. Once failed, it should not
             * have been re-scheduled for execution.
             */
            try {

                // wait just a bit to be safe, but should be done.
                producerFuture.get(50/* timeout */, TimeUnit.MILLISECONDS);

            } catch (TimeoutException ex) {
            
                fail("Producer is still running (blocked): " + ex, ex);

            } catch (ExecutionException ex) {
            
                if (InnerCause.isInnerCause(ex, BufferClosedException.class)) {
                    // This is an acceptable first cause for the producer.
                    return;
                }
                
                // otherwise throw out the 1st cause for the producer.
                throw ex;
                
            }

            /*
             * The consumer should still be running. It will not do anything
             * since there is nothing to be drained anymore.
             */
            assertFalse(consumerFuture.isDone());

        } catch(Throwable t) {
            
            /*
             * Ensure worker threads are cancelled if the test fails.
             */

            consumerFuture.cancel(true/* mayInterruptIfRunning */);

            producerFuture.cancel(true/* mayInterruptIfRunning */);

            fail("Test failure: rootCause=" + t, t);

        } finally {

            service.shutdownNow();

            if (!consumerFuture.isDone() || !producerFuture.isDone()) {
                /*
                 * Note: If the service does not terminate after a timeout then
                 * we have a hung thread on add().
                 */
                service.awaitTermination(5/* timeout */, TimeUnit.SECONDS);
            }

        }
        
    }

    /**
     * Stress test version of {@link #test_blockingBuffer_close()}.
     */
    public void _testStress_blockingBuffer_close() throws InterruptedException,
            ExecutionException, TimeoutException {

        for (int i = 0; i < 100; i++) {
         
            try {

                test_blockingBuffer_close();

            } catch (Throwable t) {
                
                fail("Failed @ i=" + i + " : " + t, t);
                
            }
            
        }

    }

}
