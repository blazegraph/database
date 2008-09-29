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
 * An unsynchronized buffer backed by a fixed capacity array that migrates
 * references onto the caller's buffer (which is normally threas-safe) using
 * {@link IBuffer#add(int)}.
 * <p>
 * <strong>This implementation is NOT thread-safe.</strong>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnsynchronizedArrayBuffer<E> implements IBuffer<E> {

    protected static final Logger log = Logger.getLogger(UnsynchronizedArrayBuffer.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * The buffer onto which chunks are evicted by {@link #overflow()}.
     */
    private final IBuffer<E[]> target;

    /**
     * The capacity of the internal buffer each time it is allocated.
     */
    private final int capacity;
    
    /**
     * The #of elements in the internal {@link #buffer}.  This is reset
     * each time we allocate the {@link #buffer}.
     */
    private int size = 0;
    
    /**
     * The internal buffer -or- <code>null</code> if we need to allocate one.
     */
    private E[] buffer = null;
    
    /**
     * Optional filter to keep elements out of the buffer.
     */
    private final IElementFilter<E> filter;
    
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
     * @param target
     *            The target buffer onto which the elements will be flushed.
     * @param capacity
     *            The capacity of the backing buffer.
     */
    public UnsynchronizedArrayBuffer(IBuffer<E[]> target, int capacity) {

        this(target, capacity, null/* filter */);

    }

    /**
     * @param target
     *            The target buffer onto which chunks of elements will be
     *            flushed.
     * @param capacity
     *            The capacity of the backing buffer.
     * @param filter
     *            Filter to keep elements out of the buffer (optional).
     */
    public UnsynchronizedArrayBuffer(IBuffer<E[]> target, int capacity,
            IElementFilter<E> filter) {

        if (target == null)
            throw new IllegalArgumentException();

        if (capacity <= 0)
            throw new IllegalArgumentException();

        this.target = target;
        
        this.capacity = capacity;

        this.filter = filter;
        
        /*
         * Note: The backing array is allocated dynamically to get the array
         * component type right.
         */
        
    }

    /**
     * Applies the optional filter to restrict elements allowed into the buffer.
     * When the optional filter was NOT specified all elements are allowed into
     * the buffer.
     * 
     * @param e
     *            A element.
     * 
     * @return true if the element is allowed into the buffer.
     */
    protected boolean accept(E e) {

        if (filter != null) {

            return filter.accept(e);

        }

        return true;

    }

    /**
     * <strong>Not thread-safe.</strong>
     */
    @SuppressWarnings("unchecked")
    public void add(E e) {

        if (e == null)
            throw new IllegalArgumentException();

        if (!accept(e)) {

            if (DEBUG)
                log.debug("reject: " + e.toString());

            return;

        }

        if (buffer == null) {

            // re-allocate on demand.
            buffer = (E[]) java.lang.reflect.Array.newInstance(e.getClass(),
                    capacity);

            size = 0;
            
        } else if (size == buffer.length) {

            overflow();
            
            assert size == 0;
            assert buffer == null;

            // re-allocate so that we can store the new element.
            buffer = (E[]) java.lang.reflect.Array.newInstance(e.getClass(),
                    capacity);
            
            size = 0;
            
        }

        if(DEBUG) {
            
            log.debug("accept: " + e);
            
        }

        buffer[size++] = e;
        
    }
    
    /**
     * Adds all references in the internal buffer to the target buffer, resets
     * the #of elements in the internal buffer to ZERO (0), and clears the
     * internal array - it will be reallocated again on demand.
     */
    @SuppressWarnings("unchecked")
    protected void overflow() {

        assert size >= 0;
        
        if (size > 0) {

            assert buffer != null;
            assert buffer.length > 0;
            
            final boolean dense = size == buffer.length; 
            
            if (INFO)
                log.info("size=" + size + ", dense=" + dense);

            final E[] a;
            if (dense) {

                a = buffer;

            } else {
                
                // make dense.

                assert buffer[0] != null;
                
                // allocate using dynamic type.
                a = (E[]) java.lang.reflect.Array.newInstance(buffer[0]
                        .getClass(), size);
                
                // copy data into new a[].
                System.arraycopy(buffer, 0, a, 0, size);
                
            }
            
            // add a chunk to the target buffer.
            target.add( a );

            // clear reference - will be reallocated on demand.
            buffer = null;
            size = 0;
            
        }
        
    }
    
    public long flush() {

        final int n = size;
        
        // move everthing onto the target buffer.
        overflow();
        
        counter += n;
        
//        // tell the target buffer to flush itself.
//        final long nwritten = target.flush();
        
        if (INFO) {

            log.info("wrote " + n + " elements, cumulative total=" + counter);
            
        }
            
        return counter;
    
    }
    
    private long counter = 0L;
    
    public void reset() {
        
        if(INFO) {
            
            log.info("Resetting buffer state and counter.");
            
        }

        // will be reallocated on demand.
        buffer = null;
        
        // reset size to zero.
        size = 0;
        
        // clear the cumulative counter.
        counter = 0L;
        
    }
    
}
