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
package com.bigdata.striterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.IElementFilter;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Striterator;

/**
 * Converts an <code>Iterator</code> into chunked iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkedWrappedIterator<E> implements IChunkedOrderedIterator<E> {

    protected static transient final Logger log = Logger.getLogger(ChunkedWrappedIterator.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    private boolean open = true;

    private final Class<? extends E> elementClass;
    
    /**
     * The source iterator supplied by the caller. If this is an
     * {@link ICloseableIterator} then {@link #close()} will drill through and
     * close the source as well.
     */
    private final Iterator<E> realSource;

    /**
     * If {@link #filter} is non-<code>null</code> then this is an iterator
     * that filters the {@link #realSource}.
     */
    private final Iterator<E> src;

    private final int chunkSize;
    
    private final IKeyOrder<E> keyOrder;

    /** Optional filter applied to the source iterator. */
    private final IElementFilter<E> filter;
    
    private long nchunks = 0L;
    private long nelements = 0L;
    
    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     */
    public ChunkedWrappedIterator(final Iterator<E> src) {

        this(src, DEFAULT_CHUNK_SIZE, null/*elementClass*/);

    }

    public ChunkedWrappedIterator(final Iterator<E> src, final int chunkSize,
            Class<? extends E> elementClass) {

        this(src, chunkSize, elementClass, null/* keyOrder */, null/* filter */);

    }

    public ChunkedWrappedIterator(final Iterator<E> src, final int chunkSize,
            final IKeyOrder<E> keyOrder, final IElementFilter<E> filter) {

        this(src, chunkSize, null/* elementClass */, keyOrder, filter);
        
    }
    
    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     * @param chunkSize
     *            The desired chunk size.
     * @param elementClass
     *            The class for the array component type (optional, but the
     *            default will use the runtime type of the first element of the
     *            array which can be too restrictive).
     * @param keyOrder
     *            The order in which the elements will be visited by the source
     *            iterator if known and <code>null</code> otherwise.
     * @param filter
     *            Optional filter. When non-<code>null</code> only elements
     *            accepted by the filter will be visited by this iterator.
     *            <p>
     *            Note: This filter will be applied on the client side after
     *            tuples have already been resolved to relation elements. Where
     *            possible, you are better off passing the filter into the
     *            source iterator running on the server(s).
     */
    @SuppressWarnings("unchecked")
    public ChunkedWrappedIterator(final Iterator<E> src, final int chunkSize,
            final Class<? extends E> elementClass, final IKeyOrder<E> keyOrder,
            final IElementFilter<E> filter) {
        
        if (src == null)
            throw new IllegalArgumentException();

        if (chunkSize <= 0)
            throw new IllegalArgumentException();

        this.elementClass = elementClass;
        
        this.realSource = src;
        
        /*
         * If a filter was specified then use a Striterator to filter the source
         * iterator such that it includes only those SPOs that match the filter.
         */
        
        this.src = (filter == null //
                ? src //
                : new Striterator(src).addFilter(new Filter(){

                    private static final long serialVersionUID = 1L;

                    protected boolean isValid(final Object arg0) {
                        
                        return filter.accept((E) arg0);
                        
                    }}
                )
                );
        
        this.chunkSize = chunkSize;
        
        this.keyOrder = keyOrder;
        
        this.filter = filter;
        
    }
    
    public void close() {

        if (!open)
            return;
        
        open = false;

        if(realSource instanceof ICloseableIterator) {
            
            ((ICloseableIterator<E>)realSource).close();
            
        }
        
        if(INFO)
            log.info("#chunks="+nchunks+", #elements="+nelements);

    }

    /**
     * Return <code>true</code> if there are elements in the source iterator.
     */
    public boolean hasNext() {

        if(!open) return false;

        return src.hasNext();
        
    }

    /**
     * The next element from the source iterator.
     */
    public E next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        final E e = src.next();
        
        nelements++;
        
        return e;

    }

    /**
     * The next chunk of elements in whatever order the were visited by
     * {@link #next()}.
     */
    @SuppressWarnings("unchecked")
    public E[] nextChunk() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        int n = 0;

        E[] chunk = null;
        
        while (open && src.hasNext() && n < chunkSize) {

            final E t = src.next();

            if (chunk == null) {

                /*
                 * Dynamically instantiate an array of the same component type
                 * as the objects that we are visiting.
                 */

                chunk = (E[]) java.lang.reflect.Array.newInstance(
                        (elementClass != null ? elementClass : t.getClass()),
                        chunkSize);

            }

            // add to this chunk.
            chunk[n++] = t;
            
        }
        
        if (n != chunkSize) {

            // make it dense.
            
            final E[] tmp = (E[]) java.lang.reflect.Array
                    .newInstance((elementClass != null ? elementClass
                            : chunk[0].getClass()), n);
            
            System.arraycopy(chunk, 0, tmp, 0, n);
            
            chunk = tmp;
         
        }

        nchunks++;
        nelements += n;
        
        if (DEBUG)
            log.debug("#chunks=" + nchunks + ", chunkSize=" + chunk.length
                    + ", #elements=" + nelements);
        
        return chunk;
        
    }

    /**
     * Delegated to the source iterator.
     */
    public void remove() {
        
        src.remove();
        
    }

    public IKeyOrder<E> getKeyOrder() {

        return keyOrder;
        
    }

    public E[] nextChunk(final IKeyOrder<E> keyOrder) {

        if (keyOrder == null)
            throw new IllegalArgumentException();

        final E[] chunk = nextChunk();

        if (!keyOrder.equals(getKeyOrder())) {

            // sort into the required order.

            Arrays.sort(chunk, 0, chunk.length, keyOrder.getComparator());

        }

        return chunk;

    }
    
}
