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
 * An unsynchronized buffer backed by a fixed capacity array that flushes onto a
 * (normally synchronized) buffer supplied by the caller using
 * {@link IBuffer#add(int, Object[])}. This class may be useful in reducing
 * contention for locks or synchronized blocks of code by reducing the frequency
 * with which those methods are invoked.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnsynchronizedArrayBuffer<E> implements IBuffer<E> {

    protected static final Logger log = Logger.getLogger(UnsynchronizedArrayBuffer.class);
    
    private final int capacity;
    private final IBuffer<E> target;
    
    private int size;
    private E[] buffer;
    
    /**
     * @param capacity
     *            The capacity of the backing buffer.
     * @param target
     *            The target buffer onto which the elements will be flushed.
     */
    public UnsynchronizedArrayBuffer(int capacity, IBuffer<E> target) {
        
        if (capacity <= 0)
            throw new IllegalArgumentException();

        if (target == null)
            throw new IllegalArgumentException();
        
        this.capacity = capacity;

        this.target = target;
        
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
     * The exact #of elements currently in the buffer.
     */
    public int size() {

        return size;
        
    }

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

            flush();

        }

        buffer[size++] = e;
        
    }

    public void add(int n, E[] a) {

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
                
                // add all references to the target buffer.
                target.add(size, buffer);
                
                // reset the #of references in this buffer.
                size = 0;

            }

            buffer[size++] = e;

        }

    }
    
    public long flush() {

        if (size > 0) {

            if (log.isInfoEnabled()) {

                log.info("flushing buffer with " + size + " elements");
                
            }

            // add all references to the target buffer.
            target.add(size, buffer);
            
            // reset the #of references in this buffer.
            size = 0;
            
        }
        
        // tell the target buffer to flush itself.
        final long nwritten = target.flush();
        
        counter += nwritten;
        
        if (log.isInfoEnabled()) {

            log.info("wrote " + nwritten + " elements, cumulative total="
                    + counter);
            
        }
            
        return counter;
    
    }
    
    private long counter = 0L;
    
    public void reset() {
        
        if(log.isInfoEnabled()) {
            
            log.info("Resetting buffer state and counter.");
            
        }
        
        this.size = 0;
        
        counter = 0L;
        
    }
    
}
