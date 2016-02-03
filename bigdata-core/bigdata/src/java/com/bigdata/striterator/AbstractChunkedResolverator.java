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
package com.bigdata.striterator;

import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.ICloseableIterator;

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

    final private static Logger log = Logger
            .getLogger(AbstractChunkedResolverator.class);

//    /**
//     * True iff the {@link #log} level is DEBUG or less.
//     */
//    final protected static boolean DEBUG = log.isDebugEnabled();
    
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

    private volatile boolean open = true;
    
//    /**
//     * Total elapsed time for the iterator instance.
//     */
//    private long elapsed = 0L;
    
    /**
     * 
     * @param state
     *            Application specified state (optional).
     * @param src
     *            The source iterator (will be closed or exhausted when this
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
    // Note: Synchronized for atomic decision making and publication of [resolvedItr].
    synchronized 
    public AbstractChunkedResolverator<E, F, S> start(final ExecutorService service) {
    
        if (resolvedItr != null)
            throw new IllegalStateException();

        /*
         * Create a task which reads chunks from the source iterator and writes
         * resolved chunks on the buffer.
         */
        final FutureTask<Long> ft = new FutureTask<Long>(
                new ChunkConsumerTask());

        /*
         * Set the future for that task on the buffer.
         */
        buffer.setFuture(ft);

        /*
         * This class will read resolved chunks from the [resolvedItr] and then
         * hand out BigdataStatements from the current [chunk].
         */
        resolvedItr = buffer.iterator();
        
        // Submit the task for execution.
        service.execute(ft);

//        /*
//         * Create and run a task which reads chunks from the source iterator and
//         * writes resolved chunks on the buffer.
//         */
//        final Future<?> f = service.submit(new ChunkConsumerTask());
//
//        /*
//         * Set the future for that task on the buffer.
//         */
//        buffer.setFuture(f);
        
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
     */
    private class ChunkConsumerTask implements Callable<Long> {

        public ChunkConsumerTask() {
            
        }
        
        /**
         * @return The #of elements (not chunks) that were converted.
         * 
         * @throws Exception
         */
        @Override
        public Long call() throws Exception {

            try {

                if (log.isDebugEnabled())
                    log.debug("Start");

                final long begin = System.currentTimeMillis();

                long nchunks = 0;
                long nelements = 0;

                // while buffer (aka sink) is open and source has more data.
                while (buffer.isOpen() && src.hasNext()) {

                    // fetch the next chunk (already available).
                    final E[] chunk = src.nextChunk();

                    if (!buffer.isOpen()) {
                        /*
                         * Asynchronous close of the sink. By checking
                         * buffer.isOpen() here and in the while() clause, we
                         * will notice a closed sink more rapidly and close
                         * the source in a more timely manner.
                         * 
                         * @see https://sourceforge.net/apps/trac/bigdata/ticket/361
                         */
                        break;
                    }
                    
                    final F[] converted;
                    try {
                        converted = resolveChunk(chunk);
                    } catch (Throwable t) {
                        /*
                         * If the root cause of the throwable was an interrupt,
                         * then close the buffer (aka the sink) and break out of
                         * the loop (we will not read anything more from the
                         * source).
                         * 
                         * @see
                         * https://sourceforge.net/apps/trac/bigdata/ticket/460
                         */
                        if (InnerCause.isInnerCause(t,
                                InterruptedException.class)
                                || InnerCause.isInnerCause(t,
                                        ClosedByInterruptException.class)) {
//                            buffer.abort(t);
                            buffer.close();
                            break;
                        }
                        throw new RuntimeException(t);
                    }
                    
                    /**
                     * Note: This is no longer true. Some conversions can now
                     * expand or reduce the size of the chunk.
                     * 
                     * @see <a href="http://trac.blazegraph.com/ticket/866" >
                     *      Efficient batch remove of a collection of triple
                     *      patterns </a>
                     */
//                    assert converted.length == chunk.length;
                    
                    // Note: Throws BufferClosedException if closed.
                    buffer.add(converted);

                    nchunks++;
                    nelements += chunk.length;
                    
                    if (log.isDebugEnabled())
                        log.debug("nchunks=" + nchunks + ", chunkSize="
                                + chunk.length);

                }

                final long elapsed = (System.currentTimeMillis() - begin);
                    
                if (log.isInfoEnabled())
                    log.info("Finished: nchunks=" + nchunks + ", nelements="
                            + nelements + ", elapsed=" + elapsed
                            + "ms, sink.open=" + buffer.isOpen());

                return nelements;
                
            } finally {

                try {
                    src.close();
                } finally {
                    /*
                     * Note: Close the buffer since nothing more will be written
                     * on it, but DO NOT close the iterator draining the buffer
                     * (aka [resolvedItr]) since the consumer will use that to
                     * drain the buffer.
                     * 
                     * Note: Failure to close the buffer here will cause a
                     * severe performance penalty.
                     * 
                     * Note: Closing the [resolvedItr] here will cause data to
                     * be lost.
                     */
                    buffer.close();
                }

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
    @Override
    public boolean hasNext() {

        if(open && _hasNext())
            return true;
        
        close();
     
        return false;
        
    }
        
    private boolean _hasNext() {

        if (resolvedItr == null) {
            
            throw new IllegalStateException();

        }
        
        if (lastIndex != -1 && chunk != null && lastIndex + 1 < chunk.length) {

            return true;
            
        }

        // read a resolved chunk from the buffer's async iterator.
        return resolvedItr.hasNext();
        
    }

    @Override
    public F next() {

//        final long begin = System.currentTimeMillis();
        
        if (!hasNext())
            throw new NoSuchElementException();

        if (lastIndex == -1 || chunk != null && lastIndex + 1 == chunk.length) {

            // get the next chunk of resolved BigdataStatements.
            chunk = resolvedItr.next();
            
            // reset the index.
            lastIndex = -1;

//            final long now = System.currentTimeMillis();
            
//            elapsed += (now - begin);

            if (log.isDebugEnabled())
                log.debug("nextChunk ready: size=" + chunk.length + ", chunk="
                        + Arrays.toString(chunk));
            
        }

        // the next resolved element.
        final F f = chunk[++lastIndex];
            
        if (log.isDebugEnabled())
            log.debug("lastIndex=" + lastIndex + ", chunk.length="
                    + chunk.length + ", visting=" + f);

        return f;

    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void remove() {

        throw new UnsupportedOperationException();

    }

    @Override
    public void close() {

        if (open) {

            open = false;

            if (log.isInfoEnabled())
                log.info("lastIndex=" + lastIndex + ", chunkSize="
                        + (chunk != null ? "" + chunk.length : "N/A"));

            /*
             * Explicitly close the source since we will not be reading anything
             * more from it.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/361
             */
            src.close();

            /*
             * Close the sink since nothing more will be written on it. 
             */
            buffer.close();

            /*
             * Since the outer iterator is being closed, nothing more will be
             * read from the buffer so we also close the iterator draining the
             * buffer.
             */
            resolvedItr.close();
            
            chunk = null;

        }

    }

}
