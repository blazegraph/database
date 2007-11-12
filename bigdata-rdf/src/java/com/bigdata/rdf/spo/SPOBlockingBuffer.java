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
import com.bigdata.rdf.util.KeyOrder;

/**
 * A buffer that will block when it is full.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOBlockingBuffer implements ISPOAssertionBuffer {
    
    /**
     * <code>true</code> until the buffer is {@link #close()}ed.
     */
    private boolean open = true;

    /**
     * Used to resolve term identifiers to terms in log statements (optional).
     */
    private final AbstractTripleStore store;
    
    /**
     * Optional filter used to reject {@link SPO}s such that they do
     * not enter into the buffer.
     */
    private final ISPOFilter filter;
    
    /**
     * Used to coordinate the reader and the writer.
     */
    private final ArrayBlockingQueue<SPO> buffer;
    
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
    public SPOBlockingBuffer(AbstractTripleStore store, ISPOFilter filter, int capacity) {
        
        this.store = store;
        
        this.filter = filter;
        
        this.buffer = new ArrayBlockingQueue<SPO>(capacity);
        
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

        return buffer.isEmpty();
        
    }

    public int size() {

        return buffer.size();
        
    }

    /**
     * Signal that no more data will be written on this buffer (this is required
     * in order for the {@link #iterator()} to know when no more data will be
     * made available).
     */
    public void close() {

        this.open = false;
        
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
        
        if(filter != null && filter.isMatch(spo)) {
            
            log.info("reject: "+spo.toString(store));

            return false;
            
        }

        log.info("add: "+spo.toString(store));
        
        // wait if the queue is full.
        while(true) {

            try {
                
                if(buffer.offer(spo,100,TimeUnit.MILLISECONDS)) {

                    // item now on the queue.
                    
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
        
        return buffer.size();
        
    }

    /**
     * Return an iterator reading from the buffer. The {@link SPO}s will be
     * visited in the order in which they were written on the buffer.
     * 
     * @return The iterator.
     */
    public ISPOIterator iterator() {
        
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
    public ISPOIterator iterator(ISPOFilter filter) {
        
        return new SPOBlockingIterator( filter );
        
    }

    /**
     * An inner class that reads from the buffer.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class SPOBlockingIterator implements ISPOIterator {
        
        private boolean open = true;

        /**
         * Optional filter applied by the iterator as it reads from the buffer.
         */
        private final ISPOFilter filter;
        
        /**
         * Create an iterator that reads from the buffer.
         * 
         * @param filter
         *            {@link SPO}s which match the filter will not be visited
         *            by the iterator (optional).
         */
        SPOBlockingIterator(ISPOFilter filter) {
       
            this.filter = filter;

            log.info("Starting iterator.");
            
        }

        /**
         * Does nothing.
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
        public KeyOrder getKeyOrder() {

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

            if(!open) return false;

            while (SPOBlockingBuffer.this.open || !buffer.isEmpty()) {

                /*
                 * Use a set limit on wait and recheck whether or not the
                 * buffer has been closed asynchronously.
                 */

                final SPO spo = buffer.peek();

                if (spo == null) {
                    
                    try {
                        Thread.sleep(100/*millis*/);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    
                    continue;
                    
                }
                
                if (filter != null && filter.isMatch(spo)) {

                    // rejected by the filter.

                    log.info("reject: "+spo.toString(store));

                    // consume the head of the queue.
                    try {
                        buffer.take();
                    } catch(InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    
                    continue;
                    
                }
                
                if(DEBUG) log.debug("next: "+spo.toString(store));
                
                return true;
                
            }

            log.info("Exhausted: bufferOpen="+SPOBlockingBuffer.this.open+", size="+buffer.size());
            
            return false;

        }

        public SPO next() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            assert !buffer.isEmpty();
            
            final SPO spo;

            try {

                spo = buffer.take();
                
            } catch(InterruptedException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            log.info("next: "+spo.toString(store));

            return spo;

        }

        public SPO[] nextChunk() {

            if (!hasNext()) {

                throw new NoSuchElementException();

            }

            /*
             * Note: this attempt to return everything in the buffer at the
             * moment we regard it in the next chunk.
             */
            
            return buffer.toArray(new SPO[0]);
            
//            // size to the #of SPOs waiting in the buffer.
//            final int chunkSize = buffer.size();
//
//            SPO[] chunk = new SPO[chunkSize];
//
//            int n = 0;
//
//            while (hasNext() && n < chunkSize) {
//
//                // add to this chunk.
//                chunk[n++] = next();
//                
//            }
//            
//            if (n != chunkSize) {
//
//                // make it dense.
//                
//                SPO[] tmp = new SPO[n];
//                
//                System.arraycopy(chunk, 0, tmp, 0, n);
//                
//                chunk = tmp;
//             
//            }
//            
//            return stmts;
            
        }

        public SPO[] nextChunk(KeyOrder keyOrder) {
            
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
