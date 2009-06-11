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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase2;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for {@link BlockingBuffer} and its {@link IAsynchronousIterator}
 * when using an array type for the elements (chunk processing) and a
 * {@link BlockingDeque}, which permits combination of chunks as they are added
 * to the buffer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test ordered chunk process also.
 */
public class TestBlockingBufferWithChunksDeque extends TestCase2 {

	/**
	 * 
	 */
	public TestBlockingBufferWithChunksDeque() {
	}

	/**
	 * @param arg0
	 */
	public TestBlockingBufferWithChunksDeque(String arg0) {
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
        final Integer e3 = new Integer(3);
        final Integer e4 = new Integer(4);
        final Integer e5 = new Integer(5);
        final Integer e6 = new Integer(6);

        final int queueCapacity = 3;
		final BlockingQueue<Integer[]> queue = new LinkedBlockingDeque<Integer[]>(
				queueCapacity);
		final int minimumChunkSize = 2;
		final long chunkTimeout = 1000;
		final TimeUnit chunkTimeoutUnit = TimeUnit.MILLISECONDS;
		/*
         * The test timeout is just a smidge longer than the chunk timeout.
         * 
         * Note: use Long.MAX_VALUE iff debugging.
         */
      final long testTimeout = Long.MAX_VALUE;
//        final long testTimeout = chunkTimeout + 20;
		final boolean ordered = false;

		final BlockingBuffer<Integer[]> buffer = new BlockingBuffer<Integer[]>(
				queue, minimumChunkSize, chunkTimeout, chunkTimeoutUnit, ordered);

        // buffer is empty.
        assertTrue("isOpen", buffer.isOpen());
        assertTrue("isEmpty", buffer.isEmpty());
        assertEquals("size", 0, buffer.size());
        assertEquals("chunkCount", 0L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 0L, buffer.getElementsAddedCount());

        final IAsynchronousIterator<Integer[]> itr = buffer.iterator();

        // nothing available from the iterator (non-blocking test).
        assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
        assertNull(itr.next(1, TimeUnit.NANOSECONDS));

        // add an element to the buffer - should not block.
        buffer.add(new Integer[]{e0});

        // should be one element and one chunk accepted by the buffer.
        assertTrue("isOpen", buffer.isOpen());
        assertFalse("isEmpty", buffer.isEmpty());
        assertEquals("size", 1, buffer.size());
        assertEquals("chunkCount", 1L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 1L, buffer.getElementsAddedCount());

        /*
         * Note: we can not test itr.hasNext() without removing the chunk from
         * the buffer, in which case the next chunk would not be combined with
         * the first.
         */
//        // something should be available now (non-blocking).
//        assertTrue(itr.hasNext(1, TimeUnit.NANOSECONDS));
//
//        // something should be available now (blocking).
//        assertTrue(itr.hasNext());

        // add another element to the buffer - should not block.
        buffer.add(new Integer[]{e1});

        /*
         * Should be two elements but only one chunks accepted into the buffer
         * since the 2nd chunk should have been combined with the first on add.
         */
        assertTrue("isOpen", buffer.isOpen());
        assertFalse("isEmpty", buffer.isEmpty());
        assertEquals("size", 1, buffer.size());
        assertEquals("chunkCount", 2L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 2L, buffer.getElementsAddedCount());

        /*
         * Add another element to the buffer. There is only one chunk in the
         * buffer and it has a length of 2. The new chunk has a length of 3 so
         * it can not be combined and will result in 2 chunks in the buffer.
         * The total element count is now 5 := (1 + 1) + 3.
         */
        buffer.add(new Integer[]{e2,e3,e4});
        assertTrue("isOpen", buffer.isOpen());
        assertFalse("isEmpty", buffer.isEmpty());
        assertEquals("size", 2, buffer.size());
        assertEquals("chunkCount", 3L, buffer.getChunksAddedCount());
        assertEquals("elementCount", 5L, buffer.getElementsAddedCount());
        
        final ReentrantLock lock = new ReentrantLock();
        final Condition cond = lock.newCondition();
        final AtomicBoolean proceedFlag = new AtomicBoolean(false);
        
        // future of task writing another element on the buffer.
        final Future producerFuture = service.submit(new Callable<Void>() {
            public Void call() throws Exception {
                
                lock.lockInterruptibly();
				try {
					if (!proceedFlag.get()) {
						cond.await();
					}
					
                    if(log.isInfoEnabled())
                        log.info("Producer resumed.");

					assertTrue("isOpen",buffer.isOpen());
					assertTrue("isEmpty",buffer.isEmpty());

                    /*
                     * Add another element - should not block since the buffer is
                     * empty.
                     */
                    if(log.isInfoEnabled())
                         log.info("Adding last chunk to buffer.");
                    buffer.add(new Integer[] { e5, e6 });
                    if(log.isInfoEnabled())
                        log.info("Added last chunk to buffer.");
                    assertTrue("isOpen", buffer.isOpen());
                    assertEquals("chunkCount", 4L, buffer.getChunksAddedCount());
                    assertEquals("elementCount", 7L, buffer.getElementsAddedCount());

                    /*
                     * itr.hasNext() (in the producer thread) will block until
                     * the buffer is closed.
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
						assertFalse("buffer closed?", itr.isExhausted());

						// take the 2nd chunk - 3 elements.
	                      if (log.isInfoEnabled())
	                            log.info("Awaiting second chunk");
	                        assertSameArray(new Integer[] { e2, e3, e4 }, itr.next());
	                        if (log.isInfoEnabled())
	                            log.info("Have second chunk");

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

                    /*
                     * Should block until we close the buffer. Will report
                     * [false] if the consumer was interrupted within hasNext()
                     * since that will cause the iterator to be asynchronously
                     * closed.
                     */
					assertTrue("Iterator exhausted?", itr.hasNext());

					// last chunk
					assertSameArray(new Integer[] { e5, e6 }, itr.next());

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

		Throwable cause = null;

		try {
	        // wait a little bit for the producer future.
		    producerFuture.get(testTimeout, chunkTimeoutUnit);
		} catch(Throwable t) {
		    cause = t;
		    log.error(t,t);
		}

		try {
		// wait a little bit for the consumer future.
		consumerFuture.get(testTimeout, chunkTimeoutUnit);
		} catch(Throwable t) {
		    if(cause==null) {
		        cause = t;
		    }
		    log.error(t,t);
		}

		if (cause != null) {

            fail(cause.getLocalizedMessage(), cause);
		    
		}
		
	}

}
