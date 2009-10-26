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
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * The capacity of the backing buffer.
     */
    protected final int capacity;
    
    /**
     * The component type of the backing byte, used when a new instance is
     * allocated.
     */
    protected final Class cls;
    
    /**
     * An optional filter for keeping elements out of the buffer.
     */
    protected final IElementFilter<E> filter;
    
    protected int size;
    protected E[] buffer;

    /**
     * @param capacity
     *            The capacity of the backing buffer.
     * @param cls
     *            Array instances of this component type will be allocated.
     * @param filter
     *            An optional filter for keeping elements out of the buffer.
     */
    protected AbstractArrayBuffer(final int capacity, final Class cls,
            final IElementFilter<E> filter) {
        
        if (capacity <= 0)
            throw new IllegalArgumentException();

        if (cls == null)
            throw new IllegalArgumentException();
        
        this.capacity = capacity;

        this.cls = cls;
        
        this.filter = filter;

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
    protected boolean accept(final E e) {

        if (filter != null) {

            if(!filter.accept(e)) {
                
                // rejected by the filter.
                
                if(DEBUG) {
                    
                    log.debug("rejected: element="+e+", filter="+filter);
                    
                }

                return false;

            }
            
        }
        
        return true;

    }

    @SuppressWarnings("unchecked")
    public void add(final E e) {

        if (e == null)
            throw new IllegalArgumentException();

        if(DEBUG) {
            
            log.debug("element="+e);
            
        }
        
        if (accept(e)) {

            synchronized (this) {

                if (buffer == null) {

                    buffer = (E[]) java.lang.reflect.Array.newInstance(cls,
                            capacity);

                } else if (size == buffer.length) {

                    flush();

                }

//                try {
                    buffer[size++] = e;
//                } catch (ArrayStoreException ex) {
//                    throw new RuntimeException("bufferClass="
//                            + buffer.getClass()
//                            + "["
//                            + buffer.getClass().getComponentType()
//                            + "]"
//                            + ", e.class="
//                            + e.getClass()
//                            + (e.getClass().getComponentType() != null ? "["
//                                    + e.getClass().getComponentType() + "]"
//                                    : ""));
//                }

            }

        }

    }

    synchronized public long flush() {

        if (size > 0) {

            if (INFO) {

                log.info("flushing buffer with " + size + " elements");
                
            }
            
            final long nwritten = flush(size, buffer);
            
            counter += nwritten;
            
            if (INFO) {

                log.info("wrote " + nwritten + " elements, cumulative total="
                        + counter);
                
            }
            
            clearBuffer();
            
        }
        
        return counter;
    
    }
    
    private long counter = 0L;
    
    synchronized public void reset() {
        
        if(INFO) {
            
            log.info("Resetting buffer state and counter.");
            
        }
        
        clearBuffer();
        
        counter = 0L;
        
    }
    
    /** Clear hard references from the buffer for better GC. */
    private void clearBuffer() {

        for (int i = 0; i < size; i++) {
            
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
