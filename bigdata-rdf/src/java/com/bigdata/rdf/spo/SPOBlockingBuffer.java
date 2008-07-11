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

package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.bigdata.rdf.inf.Justification;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.accesspath.IKeyOrder;

/**
 * A buffer that will block when it is full.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated by {@link BlockingBuffer} - used only
 *             {@link LocalTripleStore#match(org.openrdf.model.Literal[], org.openrdf.model.URI[], org.openrdf.model.URI)}
 */
public class SPOBlockingBuffer implements ISPOAssertionBuffer {
    
    /**
     * <code>true</code> until the buffer is {@link #close()}ed.
     */
    private volatile boolean open = true;

    /**
     * Used to resolve term identifiers to terms in log statements (optional).
     */
    private final AbstractTripleStore store;
    
    /**
     * Optional filter used to reject {@link SPO}s such that they do
     * not enter into the buffer.
     */
    private final IElementFilter<SPO> filter;
    
    /**
     * Used to coordinate the reader and the writer.
     */
    private final ArrayBlockingQueue<SPO> queue;
    
    /**
     * 
     * @param store
     *            Used to resolve term identifiers to terms in log statements
     *            (optional).
     * @param filter
     *            Filter used to reject {@link SPO}s such that they do not
     *            enter into the buffer (optional).
     * @param capacity
     *            The capacity of the buffer.
     */
    public SPOBlockingBuffer(AbstractTripleStore store, IElementFilter<SPO> filter, int capacity) {
        
        this.store = store;
        
        this.filter = filter;
        
        this.queue = new ArrayBlockingQueue<SPO>(capacity);
        
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
     * Adds the {@link SPO} to the buffer.
     * 
     * @throws UnsupportedOperationException
     *             if <i>justification</i> is non-<code>null</code>.
     */
    public boolean add(SPO spo, Justification justification) {
       
        assertOpen();
        
        if (justification != null)
            throw new UnsupportedOperationException();

        return add(spo);
        
    }
    
    public boolean add(SPO spo) {
        
        if(filter != null && filter.accept(spo)) {
            
            if (log.isInfoEnabled())
                log.info("reject: " + spo.toString(store));

            return false;
            
        }

        if (log.isInfoEnabled())
            log.info("add: " + spo.toString(store));
        
        // wait if the queue is full.
        while(true) {

            try {
                
                if(queue.offer(spo,100,TimeUnit.MILLISECONDS)) {

                    // item now on the queue.

                    if (log.isInfoEnabled())
                        log.info("added: " + spo.toString(store));
                    
                    return true;
                    
                }
                
            } catch (InterruptedException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }
        
    }

    /**
     * NOP.
     * 
     * @return ZERO(0) since this does not write on statement indices.
     */
    public int flush(boolean reset) {

        return 0;
        
    }

    /**
     * NOP
     * 
     * @return ZERO(0) since this does not write on statement indices.
     */
    public int flush() {
        
        return queue.size();
        
    }

    /**
     * Return an iterator reading from the buffer. The {@link SPO}s will be
     * visited in the order in which they were written on the buffer.
     * 
     * @return The iterator.
     */
    public IChunkedOrderedIterator<SPO> iterator() {
        
        return iterator( null /*filter*/ );
        
    }

    /**
     * Return an iterator reading from the buffer. The {@link SPO}s will be
     * visited in the order in which they were written on the buffer.
     * 
     * @param filter
     *            {@link SPO}s which match the filter will not be visited by
     *            the iterator (optional).
     */
    public IChunkedOrderedIterator<SPO> iterator(IElementFilter<SPO> filter) {
        
        return new SPOBlockingIterator( filter );
        
    }

    /**
     * An inner class that reads from the buffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class SPOBlockingIterator implements IChunkedOrderedIterator<SPO> {
        
        /**
         * <code>true</code> iff this iterator is open - it is closed when the
         * thread consuming the iterator decides that it is done with the
         * iterator.
         * <p>
         * Note: {@link SPOBlockingBuffer#open} is <code>true</code> until the
         * thread WRITING on the buffer decides that it has nothing further to
         * write. Once {@link SPOBlockingBuffer#open} becomes <code>false</code>
         * and there are no more {@link SPO}s in the buffer then the iterator
         * is exhausted since there is nothing left that it can visit and
         * nothing new will enter into the buffer.
         */
        private boolean open = true;

        /**
         * Optional filter applied by the iterator as it reads from the buffer.
         */
        private final IElementFilter<SPO> filter;
        
        /**
         * Create an iterator that reads from the buffer.
         * 
         * @param filter
         *            {@link SPO}s which match the filter will not be visited
         *            by the iterator (optional).
         */
        SPOBlockingIterator(IElementFilter<SPO> filter) {
       
            this.filter = filter;

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
         * Returns <code>null</code> since the {@link SPO}s are not in any
         * specific order.
         */
        public IKeyOrder<SPO> getKeyOrder() {

            return null;
            
        }
        
        /**
         * Return <code>true</code> if there are {@link SPO}s in the buffer
         * that can be visited and blocks when the buffer is empty. Returns
         * false iff the buffer is {@link SPOBlockingBuffer#close()}ed.
         * 
         * @throws RuntimeException
         *             if the current thread is interrupted while waiting for
         *             the buffer to be {@link SPOBlockingBuffer#flush()}ed.
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
            while (SPOBlockingBuffer.this.open || !queue.isEmpty()) {

                /*
                 * Use a set limit on wait and recheck whether or not the
                 * buffer has been closed asynchronously.
                 */

                final SPO spo = queue.peek();

                if (spo == null) {
                    
                    try {
                        Thread.sleep(100/*millis*/);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    
                    continue;
                    
                }
                
                if (filter != null && filter.accept(spo)) {

                    // rejected by the filter.

                    if (log.isInfoEnabled())
                        log.info("reject: " + spo.toString(store));

                    // consume the head of the queue.
                    try {
                        queue.take();
                    } catch(InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    
                    continue;
                    
                }
                
                if(DEBUG) log.debug("next: "+spo.toString(store));
                
                return true;
                
            }

            if (log.isInfoEnabled())
                log.info("Exhausted: bufferOpen="+SPOBlockingBuffer.this.open+", size="+queue.size());
            
            return false;

        }

        public SPO next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            assert !queue.isEmpty();
            
            final SPO spo;

            try {

                spo = queue.take();
                
            } catch(InterruptedException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            if (log.isInfoEnabled())
                log.info("next: " + spo.toString(store));

            return spo;

        }

        public SPO[] nextChunk() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

//            /*
//             * Note: this attempt to return everything in the buffer at the
//             * moment we regard it in the next chunk.
//             */
//            
//            final int n = buffer.size();
//            
//            return buffer.toArray(new SPO[0]);
            
            /*
             * This is thee current size of the buffer. The buffer size MAY grow
             * asynchronously but will not shrink since we are the only class
             * that takes items from the buffer.
             */
            final int chunkSize = queue.size();

            final SPO[] chunk = new SPO[chunkSize];

            int n = 0;

            while (n < chunkSize) {

                // add to this chunk.
                chunk[n++] = next();
                
            }
            
            return chunk;
            
        }

        public SPO[] nextChunk(IKeyOrder keyOrder) {
            
            if (keyOrder == null)
                throw new IllegalArgumentException();

            SPO[] chunk = nextChunk();
            
            // sort into the required order.

            Arrays.sort(chunk, 0, chunk.length, keyOrder.getComparator());

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
