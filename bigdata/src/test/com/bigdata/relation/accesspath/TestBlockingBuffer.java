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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase2;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for {@link BlockingBuffer} and its {@link IAsynchronousIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo this should have a more extensive test suite since we use it all over.
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
     * 
     * FIXME Do a variant of this for chunks. I suspect that the problem emerges
     * when there is a partial chunk and next(timeout) fails to respect the
     * timeout.
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
        assertEquals("chunkCount", 0L, buffer.getChunkCount());
        assertEquals("elementCount", 0L, buffer.getElementCount());

        final IAsynchronousIterator<Object> itr = buffer.iterator();

        // nothing available from the iterator (non-blocking test).
        assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
        assertNull(itr.next(1, TimeUnit.NANOSECONDS));

        // add an element to the buffer - should not block.
        buffer.add(e0);

        // should be one element in the buffer and zero chunks since not array type.
        assertTrue(buffer.isOpen());
        assertFalse(buffer.isEmpty());
        assertEquals("chunkCount", 0L, buffer.getChunkCount());
        assertEquals("elementCount", 1L, buffer.getElementCount());

        // something should be available now (non-blocking).
        assertTrue(itr.hasNext(1, TimeUnit.NANOSECONDS));

        // something should be available now (blocking).
        assertTrue(itr.hasNext());

        // add another element to the buffer - should not block.
        buffer.add(e1);

        // should be two elements in the buffer and zero chunks
        assertTrue(buffer.isOpen());
        assertFalse(buffer.isEmpty());
        assertEquals("chunkCount", 0L, buffer.getChunkCount());
        assertEquals("elementCount", 2L, buffer.getElementCount());

        // future of task writing a 3rd element on the buffer.
        final Future producerFuture = service.submit(new Runnable() {
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
        final Future consumerFuture = service.submit(new Runnable() {
            public void run() {

                assertEquals(e0, itr.next());
                assertEquals(e1, itr.next());
                assertEquals(e2, itr.next());

                // nothing available from the iterator (non-blocking test).
                assertFalse(itr.hasNext(1, TimeUnit.NANOSECONDS));
                assertNull(itr.next(1, TimeUnit.NANOSECONDS));

                // will block until we close the buffer.
                assertFalse(itr.hasNext());
                
            }
        });
        
        // wait a little bit for the producer future.
        producerFuture.get(100, TimeUnit.MILLISECONDS);

        // wait a little bit for the consumer future.
        consumerFuture.get(100, TimeUnit.MILLISECONDS);

    }

}
