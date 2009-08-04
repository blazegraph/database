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
 * Created on Jul 11, 2009
 */

package com.bigdata.service.jini.master;

import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Abstract base class for the scanner for a mapped master job. The
 * {@link Callable} should return the #of resources which were accepted for
 * processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractResourceScanner<V> implements Callable<Long> {

    protected static final Logger log = Logger
            .getLogger(AbstractResourceScanner.class);
    
    /**
     * The master buffer onto which the scanner drops chunks of resources
     * for processing.
     */
    private final BlockingBuffer<V[]> buffer;

    /**
     * A queue used to combine the individual resources reported to
     * {@link #accept(Object)} into chunks before they are added to the
     * {@link #buffer}.
     */
    private final ArrayBlockingQueue<V> queue;

    /**
     * Lock used to serialize the decision to transfer a chunk from the queue to
     * the buffer.
     */
    private final ReentrantLock queueLock = new ReentrantLock();
    
    /**
     * The #of resources accepted by the scanner.
     */
    private final AtomicLong acceptCount = new AtomicLong();

    /**
     * The #of chunks of resources which have been added to the buffer.
     */
    private final AtomicLong chunkCount = new AtomicLong();

    /**
     * Return the #of accepted resources.
     */
    final public long getAcceptCount() {

        return acceptCount.get();

    }

    /**
     * @param buffer
     *            The buffer to which the resources should be added.
     */
    protected AbstractResourceScanner(final BlockingBuffer<V[]> buffer) {

        if (buffer == null)
            throw new IllegalArgumentException();

        this.buffer = buffer;

        this.queue = new ArrayBlockingQueue<V>(2 * buffer.getMinimumChunkSize());
        
    }

    /**
     * Invokes {@link #runScanner()}, queuing and transferring chunks of
     * resources to the {@link BlockingBuffer} specified to the ctor. When
     * {@link #runScanner()} completes normally, the remaining resources
     * are transferred from the internal queue to the {@link BlockingBuffer}.
     * 
     * @return The #of resources accepted by the scanner.
     */
    final public Long call() throws Exception {

        // run the scanner.
        runScanner();

        // flush the last chunk to the blocking buffer.
        flushQueue();
        
        // #of resources accepted by the scanner.
        return acceptCount.get();
        
    }
    
    /**
     * Run the scanner.
     * 
     * @throws Exception
     */
    protected abstract void runScanner() throws Exception;
    
    /**
     * Accept a resource for processing.
     * 
     * @param resource
     *            The resource.
     */
    public void accept(final V resource) throws InterruptedException {

        if (resource == null)
            throw new IllegalArgumentException();

        if (log.isDebugEnabled())
            log.debug("accept: " + resource);
        
        this.acceptCount.incrementAndGet();

        // add the resource to the queue.
        queue.add(resource);

        /*
         * Synchronize callers. If there are multiple threads accepting
         * resources then only one thread at a time will cause the chunk to be
         * drained from the queue and placed onto the buffer.
         */
        queueLock.lockInterruptibly();
        try {

            if (queue.size() >= buffer.getMinimumChunkSize()) {

                // drain a chunk, transferring it to the buffer.
                transferChunk();

            }

        } finally {
         
            queueLock.unlock();
            
        }

    }
    
    /**
     * Drain a chunk from the queue, transferring it to the buffer (blocks if
     * the buffer is full).
     */
    @SuppressWarnings("unchecked")
    private void transferChunk() {
        
        final LinkedList<V> c = new LinkedList<V>();

        // drain chunk containing up to the desired chunk size.
        queue.drainTo(c, buffer.getMinimumChunkSize());

        final int chunkSize = c.size();
        
        if (chunkSize == 0)
            return;
        
        // allocate array of the appropriate component type.
        final V[] a = (V[]) java.lang.reflect.Array.newInstance(c.getFirst()
                .getClass(), chunkSize);

        // copy the chunk onto the array.
        int i = 0;
        for (V v : c) {

            assert v != null : "null @ index=" + i;
            
            a[i++] = v;

        }
        assert i == chunkSize : "i=" + i + ", chunkSize=" + chunkSize;

        if(log.isInfoEnabled()) {
            
            log.info("chunkSize=" + chunkSize + ", naccepted=" + acceptCount
                    + ", chunkCount=" + chunkCount);
            
        }
        
        /*
         * Add the chunk to the buffer.
         * 
         * Note: this will block if the queue is full.
         */
        buffer.add(a);

        chunkCount.incrementAndGet();
        
    }
    
    /**
     * Drain anything left in the queue, transferring it in chunks to the buffer
     * (blocks if the buffer is full).
     */
    private void flushQueue() {

        if (log.isInfoEnabled())
            log.info("Flushing queue to buffer.");
        
        while(!queue.isEmpty()) {

            // transfer a chunk from the queue to the buffer.
            transferChunk();
        
        }

    }

}
