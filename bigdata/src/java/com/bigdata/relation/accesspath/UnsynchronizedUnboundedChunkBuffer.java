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
 * Created on Jun 20, 2008
 */

package com.bigdata.relation.accesspath;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.bigdata.striterator.EmptyChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * An unsynchronized buffer backed by a fixed capacity array that migrates
 * references onto an internal {@link Queue}, which may be drained by an
 * {@link #iterator()}.
 * <p>
 * <strong>This implementation is NOT thread-safe.</strong>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see TestUnsynchronizedUnboundedChunkBuffer
 */
public class UnsynchronizedUnboundedChunkBuffer<E> extends
        AbstractUnsynchronizedArrayBuffer<E> {

    /**
     * The queue onto which chunks are evicted by {@link #overflow()}.
     */
    private final Queue<E[]> queue;
    
    /**
     * The order of the elements as they are added to the buffer (iff known).
     */
    private final IKeyOrder<E> keyOrder;
    
    /**
     * From the first element visited.
     */
    private Class<E[]> chunkClass = null;
    
    /**
     * @param capacity
     *            The capacity of the backing buffer.
     */
    public UnsynchronizedUnboundedChunkBuffer(final int capacity) {

        this(capacity, null/* filter */, null/* keyOrder */);

    }

    /**
     * @param capacity
     *            The capacity of the backing buffer.
     * @param filter
     *            Filter to keep elements out of the buffer (optional).
     * @param keyOrder
     *            The order of the elements in the chunks written onto the
     *            buffer or <code>null</code> iff not known.
     */
    public UnsynchronizedUnboundedChunkBuffer(final int capacity,
            final IElementFilter<E> filter, final IKeyOrder<E> keyOrder) {

        super(capacity, filter);

        this.keyOrder = keyOrder;

        queue = new LinkedBlockingQueue<E[]>(/* unbounded capacity */);

    }

    /** Add the chunk to the target buffer. */
    final protected void handleChunk(final E[] chunk) {

        if (chunkClass == null) {

            // Note the Class of the chunk for the snapshot iterator.
            chunkClass = (Class<E[]>) chunk.getClass();
            
        }
        
        queue.add(chunk);

    }

    /**
     * Iterator drains chunks from a snapshot of the queue (shallow copy).
     * Chunks are drained in a FIFO basis. The internal buffer is flushed onto a
     * chunk on the queue before the snapshot is taken so that any buffered
     * elements will appear in the snapshot.
     */
    public IChunkedOrderedIterator<E> iterator() {
        
        overflow();
        
        /*
         * Snapshot the queue state and wrap as an iterator visiting chunks of
         * elements.
         */
        synchronized (queue) {
            
            if (queue.isEmpty()) {

                // Note: handles case where chunkClass is not defined.
                return new EmptyChunkedIterator<E>(keyOrder);

            }

            assert chunkClass != null;
            
            final Iterator<E[]> src = Arrays.asList(
                    (E[][]) queue.toArray((E[][]) java.lang.reflect.Array
                            .newInstance(chunkClass, 0/* size */))).iterator();
            
            // wrap with the IChunkedOrderedIterator API.
            return new ListOfChunksIterator<E>(src, keyOrder);
            
        }
        
    }

    /**
     * Drains chunks from a queue. Chunks are drained in a FIFO basis.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class ListOfChunksIterator<E> implements
            IChunkedOrderedIterator<E> {

        private final Iterator<E[]> src;

        private final IKeyOrder<E> keyOrder;

        /**
         * 
         * @param src
         *            Iterator visiting dense chunks.
         * @param keyOrder
         *            The order of the elements in the buffer or
         *            <code>null</code> iff not known.
         */
        public ListOfChunksIterator(final Iterator<E[]> src,
                final IKeyOrder<E> keyOrder) {

            this.src = src;

            this.keyOrder = keyOrder;

        }

        /** <code>true</code> iff open. */
        private boolean open = true;
        
        /** Index of the next element to visit in the current chunk. */
        private int i = 0;

        /**
         * The current chunk or null if another chunk should be read from the
         * queue.
         */
        private E[] chunk;
        
        public boolean hasNext() {

            if (!open)
                return false;

            do {

                if (chunk != null && i >= chunk.length) {
                    
                    // chunk is exhausted.
                    chunk = null;
                    
                    i = 0;
                    
                }
                
                if (chunk == null) {

                    if(!src.hasNext()) {
                    
                        // nothing left.
                        return false;
                    
                    }

                    // another chunk.
                    chunk = src.next();

                    i = 0;

                }
                
            } while (chunk.length == 0); // skip empty chunks.
            
            return true;
            
        }
        
        public E next() {

            if (!hasNext())
                throw new NoSuchElementException();

            return chunk[i++];

        }

        public E[] nextChunk() {

            if (!hasNext())
                throw new NoSuchElementException();

            final E[] ret;
            
            if (i == 0) {
                
                ret = chunk;
                
            } else {

                /*
                 * Create and return a new chunk[] containing only the elements
                 * remaining in the current iterator.
                 */
                
                final int remaining = chunk.length - i;
                
                /*
                 * Dynamically instantiation an array of the same component type
                 * as the objects that we are visiting.
                 */

                ret = (E[]) java.lang.reflect.Array.newInstance(chunk.getClass()
                        .getComponentType(), remaining);

                
                System.arraycopy(chunk, i, ret, 0, remaining);
                
            }
            
            // indicate that all statements have been consumed.
            
            chunk = null;
            
            i = 0;
            
            return ret;
            
        }

        /**
         * @throws UnsupportedOperationException
         */
        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }

        public void close() {

            open = false;
            
        }

        public IKeyOrder<E> getKeyOrder() {
        
            return keyOrder;
            
        }

        public E[] nextChunk(final IKeyOrder<E> keyOrder) {

            if (keyOrder == null)
                throw new IllegalArgumentException();

            final E[] chunk = nextChunk();

            if (!keyOrder.equals(getKeyOrder())) {

                /*
                 * Sort into the required order.
                 * 
                 * Note: Since this iterator is supposed to have snapshot
                 * semantics the chunk is cloned so that the sort does not
                 * disturb the source data.
                 */

                Arrays.sort(chunk.clone(), 0, chunk.length, keyOrder.getComparator());

            }

            return chunk;

        }

    }

}
