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
 * Created on Aug 8, 2008
 */

package com.bigdata.relation.accesspath;

import org.apache.log4j.Logger;

/**
 * Abstract class does not handle {@link IBuffer#flush()} or
 * {@link IBuffer#reset()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractUnsynchronizedArrayBuffer<E> implements IBuffer<E>{

    protected static final Logger log = Logger.getLogger(AbstractUnsynchronizedArrayBuffer.class);
    
    private final int capacity;
    
    /**
     * The #of elements in the internal {@link #buffer}.
     */
    protected int size;
    
    /**
     * The internal buffer.
     */
    protected E[] buffer;
    
    /**
     * @param capacity
     *            The capacity of the backing buffer.
     */
    protected AbstractUnsynchronizedArrayBuffer(int capacity) {
        
        if (capacity <= 0)
            throw new IllegalArgumentException();

        this.capacity = capacity;

        /*
         * Note: The backing array is allocated once we receive the first
         * element so we can get the array component type right.
         * 
         * @todo this could be a problem if the element type was in fact Object
         * since we should allocate an Object[] but we will in fact allocate an
         * array of whatever type that first object is. This could in turn cause
         * runtime errors. If it is a problem then we need to pass in an object
         * (or an empty array) of the correct type.
         */
        
    }
    
    /**
     * If {@link #size()} reports zero(0).
     */
    public boolean isEmpty() {

        return size == 0;
        
    }

    /**
     * The exact #of elements currently in the buffer.
     */
    public int size() {

        return size;
        
    }

    /**
     * <strong>Not thread-safe.</strong>
     */
    @SuppressWarnings("unchecked")
    public void add(E e) {

        if (e == null)
            throw new IllegalArgumentException();

        if(log.isDebugEnabled()) {
            
            log.debug("element="+e);
            
        }

        if (buffer == null) {

            buffer = (E[]) java.lang.reflect.Array.newInstance(e.getClass(),
                    capacity);

        } else if (size == buffer.length) {

            overflow();
            
            if (size >= buffer.length) {

                throw new RuntimeException("overflow");
                
            }

        }

        buffer[size++] = e;
        
    }

    /**
     * <strong>Not thread safe.</strong> 
     */
    @SuppressWarnings("unchecked")
    public void add(final int n, final E[] a) {

        if(n == 0) return;
        
        if (a == null)
            throw new IllegalArgumentException();

        if (a.length < n)
            throw new IllegalArgumentException();
        
        if(log.isDebugEnabled()) {
            
            log.debug("n=" + n + ", a=" + a);

        }

        for (int i = 0; i < n; i++) {

            final E e = a[i];

            if (e == null)
                throw new IllegalArgumentException("null @ index=" + i);

            if (buffer == null) {

                buffer = (E[]) java.lang.reflect.Array.newInstance(
                        e.getClass(), capacity);

            } else if (size == buffer.length) {

                if(log.isInfoEnabled()) {
                 
                    log.info("moving references to target buffer: size="+size);
                    
                }
                
                overflow();
                
            }

            buffer[size++] = e;

        }

    }
    
    /**
     * Invoked if the internal buffer is full. Implementation must reset
     * {@link #size} to the #of elements remaining in the interal buffer.
     */
    abstract protected void overflow(); 
    
}
