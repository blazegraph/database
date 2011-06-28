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
 * Created on Aug 27, 2008
 */

package com.bigdata.service.proxy;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Wraps an {@link IRemoteChunkedIterator} so that it looks like an
 * {@link IChunkedOrderedIterator} again.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WrappedRemoteChunkedIterator<E> implements
        IChunkedOrderedIterator<E> {

    protected static final Logger log = Logger
            .getLogger(WrappedRemoteChunkedIterator.class);
    
    /** The source - the reference is cleared when the iterator is closed. */
    private IRemoteChunkedIterator<E> src;

    /**
     * The #of {@link IRemoteChunk}s read from the source so far.
     */
    private long nchunks = 0L;
    
    /**
     * If the source will not return any _more_ chunks. There will still be data
     * in the current chunk if {@link #a} is non-null and {@link #index} is less
     * than the #of elements in {@link #a}.
     */
    private boolean exhausted;
    
    /** The elements in the current chunk. */
    private E[] a;
    
    /** The index into the current chunk. */
    private int index;
    
    /** The {@link IKeyOrder} for the source iterator (may be null). */
    private IKeyOrder<E> keyOrder;

    /**
     * 
     * @param src
     *            The source.
     */
    public WrappedRemoteChunkedIterator(final IRemoteChunkedIterator<E> src) {

        if (src == null)
            throw new IllegalArgumentException();

        // save reference.
        this.src = src;
        
        try {

            // pre-fetch the first chunk.
            readChunkFromSource();
            
        } catch (IOException ex) {

            /*
             * Attempt to close the source if there was a problem reading the
             * first chunk. However, only log a message if the src.close() fails
             * since we want the [ex] from above to get thrown out of this ctor.
             */
            try {

                src.close();
                
            } catch(IOException ex2) {
                
                log.warn("Could not close source", ex2);
                
            }
            
            // clear the reference.
            this.src = null;
            
            // wrap and throw the original exception.
            throw new RuntimeException(ex);
            
        }
        
    }

    public IKeyOrder<E> getKeyOrder() {

        return keyOrder;
        
    }

    /**
     * Reads a chunk from the source.
     * 
     * @throws IOException
     *             If there is an RMI problem.
     */
    private void readChunkFromSource() throws IOException {

        // read a chunk from the source.
        final IRemoteChunk<E> chunk = src.nextChunk();

        if (nchunks == 0) {

            // save once.
            keyOrder = chunk.getKeyOrder();

        }

        exhausted = chunk.isExhausted();

        a = chunk.getChunk();

        index = 0;

        if (log.isInfoEnabled()) {

            log.info("nchunks=" + nchunks + ", sourceExhausted=" + exhausted
                    + ", elementsInChunk=" + a.length);

        }
        
        nchunks++;

    }

    /**
     * Variant traps and masquerades the {@link IOException}.
     */
    private void readChunkFromSource2() {

        try {

            readChunkFromSource();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }
    
    public boolean hasNext() {
     
        if (exhausted && (a == null || index >= a.length)) {
            
            /*
             * The source is exhausted and either there are no elements in a[]
             * or we have exhausted all of the elements in a[].
             */
            
            return false;
            
        }

        /*
         * There is something remaining.
         */
        
        if (index >= a.length) {

            // pre-fetch the next chunk if we are done with the last one.

            readChunkFromSource2();
            
        }

        return true;
        
    }

    public E next() {
        
        if (!hasNext())
            throw new NoSuchElementException();
        
        final E e = a[index++];
        
        if (log.isDebugEnabled()) {

            log.debug("e=" + e + ", index=" + index + ", chunkSize=" + a.length);
            
        }
        
        return e;
    }

    @SuppressWarnings("unchecked")
    public E[] nextChunk() {
        
        if (!hasNext()) {

            throw new NoSuchElementException();
            
        }

        final E[] ret;
        
        if (index == 0) {
            
            /*
             * Nothing has been returned to the caller by next() so we can just
             * return the backing array in this case.
             */
            
            ret = a;

            if (log.isDebugEnabled()) {

                log.debug("returning entire chunk: chunkSize=" + a.length);
                
            }
            
        } else {

            /*
             * Create and return a new E[] containing only the elements
             * remaining in the current chunk.
             */
            
            final int remaining = a.length - index;
            
            /*
             * Dynamically instantiation an array of the same component type
             * as the objects that we are visiting.
             */

            ret = (E[]) java.lang.reflect.Array.newInstance(a.getClass()
                    .getComponentType(), remaining);

            
            System.arraycopy(a, index, ret, 0, remaining);
            
            if (log.isDebugEnabled()) {

                log.debug("returning remainder of chunk: remaining=" + remaining);
                
            }

        }
        
        // indicate that all elements in the current have been consumed.
        
        index = a.length;
        
        return ret;
        
    }

    public E[] nextChunk(IKeyOrder<E> keyOrder) {

        if (keyOrder == null)
            throw new IllegalArgumentException();

        final E[] chunk = nextChunk();

        if (!keyOrder.equals(getKeyOrder())) {

            // sort into the required order.

            Arrays.sort(chunk, 0, chunk.length, keyOrder.getComparator());

        }

        return chunk;

    }

    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public void remove() {
        
        throw new UnsupportedOperationException();

    }

    public void close() {

        if (src != null) {

            if(log.isInfoEnabled()) {
                
                log.info("Closing remote iterator");
                
            }
            
            try {

                src.close();

            } catch (IOException e) {

                throw new RuntimeException(e);

            } finally {

                // clear now that it's been closed.
                src = null;

            }

        }

    }

}
