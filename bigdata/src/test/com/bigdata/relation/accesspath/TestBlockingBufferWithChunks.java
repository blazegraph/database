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
 * Created on Sep 10, 2008
 */

package com.bigdata.relation.accesspath;

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase2;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for {@link BlockingBuffer} and its {@link IAsynchronousIterator}
 * when using an array type for the elements (chunk processing).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test ordered chunk process also.
 */
public class TestBlockingBufferWithChunks extends TestCase2 {

	/**
	 * 
	 */
	public TestBlockingBufferWithChunks() {
	}

	/**
	 * @param arg0
	 */
	public TestBlockingBufferWithChunks(String arg0) {
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

        final Integer e0 = new Integer(0);
        final Integer e1 = new Integer(1);
        final Integer e2 = new Integer(2);

        final int queueCapacity = 3;
		final BlockingQueue<Integer[]> queue = new ArrayBlockingQueue<Integer[]>(
				queueCapacity);
		final int chunkSize = 4;
		final long chunkTimeout = 1000;
		final TimeUnit chunkTimeoutUnit = TimeUnit.MILLISECONDS;
		final boolean ordered = false;

		final BlockingBuffer<Integer[]> buffer = new BlockingBuffer<Integer[]>(
				queue, chunkSize, chunkTimeout, chunkTimeoutUnit, ordered);

        // buffer is empty.
        assertTrue(buffer.isOpen());
        assertTrue(buffer.isEmpty());
        assertEquals("chunkCount", 0L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 0L, buffer.getElementsAddedCount());

        final IAsynchronousIterator<Integer[]> itr = buffer.iterator();

        // nothing available from the iterator (non-blocking test).
        assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
        assertNull(itr.next(1, TimeUnit.NANOSECONDS));

        // add an element to the buffer - should not block.
        buffer.add(new Integer[]{e0});

        // should be one element and one chunk accepted by the buffer.
        assertTrue(buffer.isOpen());
        assertFalse(buffer.isEmpty());
        assertEquals("chunkCount", 1L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 1L, buffer.getElementsAddedCount());

        // something should be available now (non-blocking).
        assertTrue(itr.hasNext(1, TimeUnit.NANOSECONDS));

        // something should be available now (blocking).
        assertTrue(itr.hasNext());

        // add another element to the buffer - should not block.
        buffer.add(new Integer[]{e1});

        // should be two elements and two chunks accepted into the buffer
        assertTrue(buffer.isOpen());
        assertFalse(buffer.isEmpty());
        assertEquals("chunkCount", 2L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 2L, buffer.getElementsAddedCount());

        final ReentrantLock lock = new ReentrantLock();
        final Condition cond = lock.newCondition();
        final AtomicBoolean proceedFlag = new AtomicBoolean(false);
        
        // future of task writing a 3rd element on the buffer.
        final Future producerFuture = service.submit(new Callable<Void>() {
            public Void call() throws Exception {
                
                lock.lockInterruptibly();
				try {
					if (!proceedFlag.get()) {
						cond.await();
					}
					/*
					 * add another element - should block until we take an
					 * element using the iterator.
					 */
					buffer.add(new Integer[] { e2 });

					/*
					 * itr.hasNext() will block until the buffer is closed.
					 */
					buffer.close();
				} finally {
					lock.unlock();
				}
				// done.
				return null;

            }
		});

		// future of task draining the buffer.
		final Future consumerFuture = service.submit(new Callable<Void>() {
			public Void call() throws Exception {

				try {
					lock.lockInterruptibly();
					try {

						assertTrue(itr.hasNext());

						// take the first chunk - two elements.
						if (log.isInfoEnabled())
							log.info("Awaiting first chunk");
						assertSameArray(new Integer[] { e0, e1 }, itr.next(50,
								TimeUnit.MILLISECONDS));
						if (log.isInfoEnabled())
							log.info("Have first chunk");

						/*
						 * Verify that we obtained the first chunk before the
						 * buffer was closed. Otherwise next() blocked
						 * attempting to compile a full chunk until the producer
						 * timeout, at which point the producer closed the
						 * buffer and next() noticed the closed buffer and
						 * returned.
						 */
						assertTrue(buffer.isOpen());
						assertFalse("buffer was closed.", itr.isExhausted());

						/*
						 * Verify that nothing is available from the iterator
						 * (non-blocking test).
						 */
						assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
						assertNull(itr.next(1, TimeUnit.NANOSECONDS));

						// Signal the producer that it should continue.
						proceedFlag.set(true);
						cond.signal();

					} finally {

						lock.unlock();

					}

					// should block until we close the buffer.
					assertTrue(itr.hasNext());

					// last chunk
					assertSameArray(new Integer[] { e2 }, itr.next());

					// should be immediately false.
					assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
					// should be immediately null.
					assertNull(itr.next(1, TimeUnit.NANOSECONDS));

					// The synchronous API should also report an exhausted
					// itr.
					assertFalse(itr.hasNext());
					try {
						itr.next();
						fail("Expecting: " + NoSuchElementException.class);
					} catch (NoSuchElementException ex) {
						if (log.isInfoEnabled())
							log.info("Ignoring expected exception: " + ex);
					}

					return null;

				} catch (Throwable t) {
					log.error("Consumer failed or blocked: " + t, t);
					throw new Exception(t);
				}

			}
		});

		final long testTimeout = 1000;

		// wait a little bit for the producer future.
		producerFuture.get(testTimeout, TimeUnit.MILLISECONDS);

		// wait a little bit for the consumer future.
		consumerFuture.get(testTimeout, TimeUnit.MILLISECONDS);

	}

    /*
	 * This is commented out since the only difference between next() with an
	 * infinite chunk timeout and next(timeout := Long.MAX_VALUE) is that the
	 * latter will check to see if the timeout has expired after invoking
	 * hasNext(timeout). With an infinite timeout (Long.MAX_VALUE) this will
	 * never be true so the two cases are in fact equivalent.
	 */
//    /**
//	 * Test liveness with an infinite chunk timeout. The buffer is setup such
//	 * that it contains one or two chunks, which could be combined into a single
//	 * chunk. next(timeout) should notice as soon as the buffer is closed (a few
//	 * ms of latency at most) and combine whatever is remaining in the buffer
//	 * into a chunk and return that to the consumer.
//	 * 
//	 * @throws TimeoutException
//	 * @throws ExecutionException
//	 * @throws InterruptedException
//	 */
//	public void test_chunkCombinerNoticesCloseWithInfiniteTimeout()
//			throws InterruptedException, ExecutionException, TimeoutException {
//
//        final Integer e0 = new Integer(0);
//        final Integer e1 = new Integer(1);
//        final Integer e2 = new Integer(2);
//
//        final int queueCapacity = 3;
//		final BlockingQueue<Integer[]> queue = new ArrayBlockingQueue<Integer[]>(
//				queueCapacity);
//		final int chunkSize = 4;
//		final long chunkTimeout = Long.MAX_VALUE;
//		final TimeUnit chunkTimeoutUnit = TimeUnit.MILLISECONDS;
//		final boolean ordered = false;
//
//		final BlockingBuffer<Integer[]> buffer = new BlockingBuffer<Integer[]>(
//				queue, chunkSize, chunkTimeout, chunkTimeoutUnit, ordered);
//
//        // buffer is empty.
//        assertTrue(buffer.isOpen());
//        assertTrue(buffer.isEmpty());
//        assertEquals("chunkCount", 0L, buffer.getChunkCount());
//        assertEquals("elementCount", 0L, buffer.getElementCount());
//
//        final IAsynchronousIterator<Integer[]> itr = buffer.iterator();
//
//        // nothing available from the iterator (non-blocking test).
//        assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
//        assertNull(itr.next(1, TimeUnit.NANOSECONDS));
//
//        // add an element to the buffer - should not block.
//        buffer.add(new Integer[]{e0});
//
//        // should be one element and one chunk accepted by the buffer.
//        assertTrue(buffer.isOpen());
//        assertFalse(buffer.isEmpty());
//        assertEquals("chunkCount", 1L, buffer.getChunkCount());
//        assertEquals("elementCount", 1L, buffer.getElementCount());
//
//        // something should be available now (non-blocking).
//        assertTrue(itr.hasNext(1, TimeUnit.NANOSECONDS));
//
//        // something should be available now (blocking).
//        assertTrue(itr.hasNext());
//
//        // add another element to the buffer - should not block.
//        buffer.add(new Integer[]{e1});
//
//        // should be two elements and two chunks accepted into the buffer
//        assertTrue(buffer.isOpen());
//        assertFalse(buffer.isEmpty());
//        assertEquals("chunkCount", 2L, buffer.getChunkCount());
//        assertEquals("elementCount", 2L, buffer.getElementCount());
//
//        final ReentrantLock lock = new ReentrantLock();
//        final Condition cond = lock.newCondition();
//        final AtomicBoolean proceedFlag = new AtomicBoolean(false);
//        
//        // future of task writing a 3rd element on the buffer.
//        final Future producerFuture = service.submit(new Callable<Void>() {
//            public Void call() throws Exception {
//                
//                lock.lockInterruptibly();
//				try {
//					if (!proceedFlag.get()) {
//						cond.await();
//					}
//					/*
//					 * add another element - should block until we take an
//					 * element using the iterator.
//					 */
//					buffer.add(new Integer[] { e2 });
//
//					/*
//					 * itr.hasNext() will block until the buffer is closed.
//					 */
//					buffer.close();
//				} finally {
//					lock.unlock();
//				}
//				// done.
//				return null;
//
//            }
//		});
//
//		// future of task draining the buffer.
//		final Future consumerFuture = service.submit(new Callable<Void>() {
//			public Void call() throws Exception {
//
//				try {
//					lock.lockInterruptibly();
//					try {
//
//						assertTrue(itr.hasNext());
//
//						// take the first chunk - two elements.
//						if (log.isInfoEnabled())
//							log.info("Awaiting first chunk");
//						assertSameArray(new Integer[] { e0, e1 }, itr.next(50,
//								TimeUnit.MILLISECONDS));
//						if (log.isInfoEnabled())
//							log.info("Have first chunk");
//
//						/*
//						 * Verify that we obtained the first chunk before the
//						 * buffer was closed. Otherwise next() blocked
//						 * attempting to compile a full chunk until the producer
//						 * timeout, at which point the producer closed the
//						 * buffer and next() noticed the closed buffer and
//						 * returned.
//						 */
//						assertTrue(buffer.isOpen());
//						assertFalse("buffer was closed.", itr.isExhausted());
//
//						/*
//						 * Verify that nothing is available from the iterator
//						 * (non-blocking test).
//						 */
//						assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
//						assertNull(itr.next(1, TimeUnit.NANOSECONDS));
//
//						// Signal the producer that it should continue.
//						proceedFlag.set(true);
//						cond.signal();
//
//					} finally {
//
//						lock.unlock();
//
//					}
//
//					// last chunk
//					assertSameArray(new Integer[] { e2 }, itr.next());
//
//					// should be immediately false.
//					assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
//					// should be immediately null.
//					assertNull(itr.next(1, TimeUnit.NANOSECONDS));
//
//					// The synchronous API should also report an exhausted
//					// itr.
//					assertFalse(itr.hasNext());
//					try {
//						itr.next();
//						fail("Expecting: " + NoSuchElementException.class);
//					} catch (NoSuchElementException ex) {
//						if (log.isInfoEnabled())
//							log.info("Ignoring expected exception: " + ex);
//					}
//
//					return null;
//
//				} catch (Throwable t) {
//					log.error("Consumer failed or blocked: " + t, t);
//					throw new Exception(t);
//				}
//
//			}
//		});
//
//		final long testTimeout = 1000;
//
//		// wait a little bit for the producer future.
//		producerFuture.get(testTimeout, TimeUnit.MILLISECONDS);
//
//		// wait a little bit for the consumer future.
//		consumerFuture.get(testTimeout, TimeUnit.MILLISECONDS);
//
//	}

}
