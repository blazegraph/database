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

import org.apache.log4j.Logger;

/**
 * A thread-safe buffer backed by a fixed capacity array. Concrete
 * implementations must empty the buffer in {@link #flush(int, Object[])}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractArrayBuffer<E> implements IBuffer<E> {

    protected static final Logger log = Logger.getLogger(AbstractArrayBuffer.class);
    
    private final int capacity;
    private final IElementFilter<E> filter;
    
    private int size;
    private E[] buffer;
    
    /**
     * @param capacity
     *            The capacity of the backing buffer.
     * @param filter
     *            An optional filter for keeping elements out of the buffer.
     */
    protected AbstractArrayBuffer(int capacity, IElementFilter<E> filter) {
        
        if (capacity <= 0)
            throw new IllegalArgumentException();
        
        this.capacity = capacity;

        this.filter = filter;
        
        /*
         * Note: The backing array is allocated once we receive the first
         * element so we can get the array component type right.
         * 
         * @todo this could be a problem if the element type was in fact Object
         * since we should allocate an Object[] but we will in fact allocate an
         * array of whatever type that first object is. This could in turn cause
         * runtime errors. If it is then we need to pass in an object (or an
         * empty array) of the correct type.
         */
        
    }
    
    /**
     * If {@link #size()} reports zero(0).
     */
    public boolean isEmpty() {

        return size == 0;
        
    }

    /**
     * The approximate #of elements in the buffer.
     */
    public int size() {

        return size;
        
    }

    /**
     * Filters elements allowed into the buffer.
     * 
     * @param e
     *            Some element.
     * 
     * @return <code>true</code> iff the buffer accepts the element.
     */
    protected boolean accept(E e) {

        if (filter != null) {

            if(!filter.accept(e)) {
                
                // rejected by the filter.
                
                if(log.isDebugEnabled()) {
                    
                    log.debug("rejected: element="+e+", filter="+filter);
                    
                }

                return false;

            }
            
        }
        
        return true;

    }

    public void add(E e) {

        if (e == null)
            throw new IllegalArgumentException();

        if(log.isDebugEnabled()) {
            
            log.debug("element="+e);
            
        }
        
        if (accept(e)) {

            synchronized (this) {

                if (buffer == null) {

                    buffer = (E[]) java.lang.reflect.Array.newInstance(e
                            .getClass(), capacity);

                } else if (size == buffer.length) {

                    flush();

                }

                buffer[size++] = e;

            }

        }

    }

    public void add(final int n, final E[] a) {

        if (n == 0)
            return;

        if (a == null)
            throw new IllegalArgumentException();

        if (a.length < n)
            throw new IllegalArgumentException();

        if (log.isDebugEnabled()) {

            log.debug("n=" + n + ", a=" + a);

        }
        
        synchronized (this) {

            for (int i = 0; i < n; i++) {

                final E e = a[i];

                if (e == null)
                    throw new IllegalArgumentException("null @ index=" + i);

                if (accept(e)) {

                    if (buffer == null) {

                        buffer = (E[]) java.lang.reflect.Array.newInstance(e
                                .getClass(), capacity);

                    } else if (size == buffer.length) {

                        flush();

                    }

                    buffer[size++] = e;

                }
                
            }

        }

    }
    
    synchronized public long flush() {

        if (size > 0) {

            if (log.isInfoEnabled()) {

                log.info("flushing buffer with " + size + " elements");
                
            }
            
            final long nwritten = flush(size, buffer);
            
            counter += nwritten;
            
            if (log.isInfoEnabled()) {

                log.info("wrote " + nwritten + " elements, cumulative total="
                        + counter);
                
            }
            
            clearBuffer();
            
        }
        
        return counter;
    
    }
    
    private long counter = 0L;
    
    synchronized public void reset() {
        
        if(log.isInfoEnabled()) {
            
            log.info("Resetting buffer state and counter.");
            
        }
        
        clearBuffer();
        
        counter = 0L;
        
    }
    
    /** Clear hard references from the buffer for better GC. */
    private void clearBuffer() {

        for(int i=0; i<size; i++) {
            
            buffer[i] = null;
            
        }

        // the buffer is now empty.
        size = 0;

    }

    /**
     * This method is automatically invoked if the buffer is flushed and it is
     * non-empty. The implementation is required to dispose of the contents of
     * the buffer. The caller is already synchronized on <i>this</i> so no
     * further synchronization is necessary. It is assumed that the contents of
     * the buffer have been safely disposed of when this method returns.
     * 
     * @param n
     *            The #of elements in the array.
     * @param a
     *            The array of elements.
     * 
     * @return The #of elements that were modified in the backing relation when
     *         the buffer was flushed (unlike {@link #flush()}, this is not a
     *         cumulative counter, but the #of modified elements in the relation
     *         for this operation only).
     */
    abstract protected long flush(int n, E[] a);
    
}
