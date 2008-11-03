/*

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
package com.bigdata.striterator;

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * Wraps an {@link IChunkedIterator} and asynchronously resolves chunks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <E>
 *            The generic type of the elements visited by the source iterator.
 * @param <F>
 *            The generic type of the elements visited by this iterator (the
 *            resolved elements).
 * @param <S>
 *            The generic type of the application state object.
 */
abstract public class AbstractChunkedResolverator<E,F,S> implements ICloseableIterator<F> {

    final protected static Logger log = Logger.getLogger(AbstractChunkedResolverator.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();
    
    /**
     * The source iterator.
     */
    private final IChunkedOrderedIterator<E> src;
    
    /**
     * Application specified state object.
     */
    protected final S state;
    
    /**
     * The index of the last entry returned in the current {@link #chunk} and
     * <code>-1</code> until the first entry is returned.
     */
    private int lastIndex = -1;

    /**
     * The current chunk of resolved elements.
     */
    private F[] chunk = null;

//    /**
//     * Total elapsed time for the iterator instance.
//     */
//    private long elapsed = 0L;
    
    /**
     * 
     * @param state
     *            Application specified state (optional).
     * @param src
     *            The source iterator (will be closed exhausted or when this
     *            iterator is closed).
     * @param buffer
     *            The {@link BlockingBuffer} on which the resolved chunks will
     *            be written by the {@link ChunkConsumerTask} and from which
     *            this iterator will read those chunks.
     */
    public AbstractChunkedResolverator(final S state,
            final IChunkedOrderedIterator<E> src,
            final BlockingBuffer<F[]> buffer) {

        if (src == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        this.state = state;

        this.src = src;

        this.buffer = buffer;
        
    }

    /**
     * Creates and runs a task that will consume chunks from the source
     * iterator, resolve the elements in each chunk, and place the resolved
     * chunks onto the {@link BlockingBuffer}.
     * <p>
     * <strong>You MUST invoke this before consuming results from the iterator.</strong>
     * <p>
     * Implementations SHOULD strengthen the return type.
     * 
     * @param service
     *            The service on which the task will be executed.
     */
    public AbstractChunkedResolverator<E, F, S> start(ExecutorService service) {
        
        if (resolvedItr != null)
            throw new IllegalStateException();
        
        /*
         * Create and run a task which reads chunks from the source iterator and
         * writes resolved chunks on the buffer.
         */
        final Future f = service.submit(new ChunkConsumerTask());

        /*
         * Set the future for that task on the buffer.
         */
        buffer.setFuture(f);
        
        /*
         * This class will read resolved chunks from the [resolvedItr] and then
         * hand out BigdataStatements from the current [chunk].
         */
        resolvedItr = buffer.iterator();
        
        return this;
        
    }

    /**
     * Buffer containing chunks of resolved elements.
     */
    private final BlockingBuffer<F[]> buffer;
    
    /**
     * Iterator draining chunks of resolved elements from the {@link #buffer}.
     * This is <code>null</code> until {@link #start(ExecutorService)} is
     * invoked.
     */
    private IAsynchronousIterator<F[]> resolvedItr;

    /**
     * Consumes chunks from the source iterator, placing the converted chunks on
     * a queue.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ChunkConsumerTask implements Callable<Long> {

        public ChunkConsumerTask() {
            
        }
        
        /**
         * @return The #of elements (not chunks) that were converted.
         * 
         * @throws Exception
         */
        public Long call() throws Exception {

            try {

                if (DEBUG)
                    log.debug("Start");

                final long begin = System.currentTimeMillis();

                long nchunks = 0;
                long nelements = 0;
                
                while (src.hasNext()) {

                    // fetch the next chunk.
                    final E[] chunk = src.nextChunk();

                    final F[] converted = resolveChunk(chunk);
                    
                    assert converted.length == chunk.length;
                    
                    buffer.add(converted);

                    nchunks++;
                    nelements += chunk.length;
                    
                    if (DEBUG)
                        log.debug("nchunks="+nchunks+", chunkSize="+chunk.length);

                }

                final long elapsed = (System.currentTimeMillis() - begin);
                    
                if (INFO)
                    log.info("Finished: nchunks=" + nchunks + ", nelements="
                            + nelements + ", elapsed=" + elapsed + "ms");

                return nelements;
                
            } finally {

                src.close();
                
                buffer.close();

            }

        }
        
    }
    
    /**
     * Resolves the elements in a source chunk, returning a chunk of resolved
     * elements.
     * <p>
     * Note: This method is invoked by the {@link ChunkConsumerTask} which runs
     * asynchronously.
     * 
     * @param chunk
     *            The next chunk from the source iterator.
     * 
     * @return The resolved chunk.
     */
    abstract protected F[] resolveChunk(final E[] chunk);

    /**
     * @throws IllegalStateException
     *             unless {@link #start(ExecutorService)} has been invoked.
     */
    public boolean hasNext() {

        if (resolvedItr == null) {
            
            throw new IllegalStateException();

        }
        
        if (lastIndex != -1 && lastIndex + 1 < chunk.length) {

            return true;
            
        }

        // read a resolved chunk from the buffer's async iterator.
        return resolvedItr.hasNext();
        
    }

    public F next() {

//        final long begin = System.currentTimeMillis();
        
        if (!hasNext())
            throw new NoSuchElementException();

        if (lastIndex == -1 || lastIndex + 1 == chunk.length) {

            // get the next chunk of resolved BigdataStatements.
            chunk = resolvedItr.next();
            
            // reset the index.
            lastIndex = -1;

//            final long now = System.currentTimeMillis();
            
//            elapsed += (now - begin);

            if (DEBUG)
                log.debug("nextChunk ready");
            
        }

        // the next resolved element.
        final F f = chunk[++lastIndex];
            
        if (DEBUG)
            log.debug("lastIndex=" + lastIndex + ", chunk.length="
                    + chunk.length + ", stmt=" + f);

        return f;

    }

    /**
     * @throws UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

    public void close() {

        if (INFO)
            log.info("lastIndex=" + lastIndex + ", chunkSize="
                    + (chunk != null ? "" + chunk.length : "N/A"));

        // asynchronous close by the consumer of the producer's buffer.
        buffer.close();

        chunk = null;
        
    }

}
