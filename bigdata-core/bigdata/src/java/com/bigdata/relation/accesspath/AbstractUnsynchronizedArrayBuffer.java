/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Oct 27, 2008
 */

package com.bigdata.relation.accesspath;

import org.apache.log4j.Logger;

/**
 * An abstract implementation of an unsynchronized buffer backed by a fixed
 * capacity array.
 * <p>
 * <strong>This implementation is NOT thread-safe.</strong>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class AbstractUnsynchronizedArrayBuffer<E> implements IBuffer<E> {

    private static final Logger log = Logger
            .getLogger(AbstractUnsynchronizedArrayBuffer.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static private boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static private boolean DEBUG = log.isDebugEnabled();

    /**
     * The capacity of the internal buffer each time it is allocated.
     */
    final private int capacity;
    
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
     * The component type for the backing array.
     */
    private final Class<? extends E> cls;
    
    /**
     * Optional filter to keep elements out of the buffer.
     */
    private final IElementFilter<? extends E> filter;
    
    /**
     * If {@link #size()} reports zero(0).
     */
    @Override
    final public boolean isEmpty() {

        return size == 0;
        
    }

    /**
     * The exact #of elements currently in the buffer.
     */
    @Override
    final public int size() {

        return size;
        
    }

    /**
     * @param capacity
     *            The capacity of the backing buffer.
     * @param cls
     *            The component type for the backing array.
     */
    public AbstractUnsynchronizedArrayBuffer(final int capacity,
            final Class<? extends E> cls) {

        this(capacity, cls, null/* filter */);

    }
    
    /**
     * @param capacity
     *            The capacity of the backing buffer.
     * @param cls
     *            The component type for the backing array.
     * @param filter
     *            Filter to keep elements out of the buffer (optional).
     */
    public AbstractUnsynchronizedArrayBuffer(final int capacity,
            final Class<? extends E> cls, final IElementFilter<E> filter) {

        if (capacity <= 0)
            throw new IllegalArgumentException();

        if (cls == null)
            throw new IllegalArgumentException();

        this.capacity = capacity;

        this.filter = filter;

        this.cls = cls;
        
//        /*
//         * Note: The backing array is allocated dynamically to get the array
//         * component type right.
//         */
        
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

        if (filter != null && filter.canAccept(e)) {

            return filter.isValid(e);

        }

        return true;

    }

    /**
     * <strong>Not thread-safe.</strong>
     */
    @Override
    final public void add(E e) {
        
        add2(e);
        
    }
    
    /**
     * Adds an element to the buffer.
     * 
     * <strong>Not thread-safe.</strong>
     * 
     * @param e An element.
     * 
     * @return <code>true</code> iff adding the element caused the buffer to
     *         {@link #overflow()}
     */
    @SuppressWarnings("unchecked")
    public boolean add2(final E e) {

        if (e == null)
            throw new IllegalArgumentException();

        if (!accept(e)) {

            if (DEBUG)
                log.debug("reject: " + e.toString());

            return false;

        }

        boolean overflow = false;
        
        if (buffer == null) {

            // re-allocate on demand.
            buffer = (E[]) java.lang.reflect.Array.newInstance(//
                    cls,//e.getClass(),
                    capacity);

            size = 0;
            
        } else if (size == buffer.length) {

            overflow();
            
            assert size == 0;
            assert buffer == null;

            // re-allocate so that we can store the new element.
            buffer = (E[]) java.lang.reflect.Array.newInstance(//
                    cls,//e.getClass(),
                    capacity);
            
            size = 0;
            
            overflow = true;
            
        }

        if(DEBUG)
            log.debug("accept: " + e);

        buffer[size++] = e;

        return overflow;
        
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
                a = (E[]) java.lang.reflect.Array.newInstance(
//                        buffer[0].getClass(),
                        cls,
                        size);
                
                // copy data into new a[].
                System.arraycopy(buffer, 0, a, 0, size);
                
            }
            
            // clear reference - will be reallocated on demand.
            buffer = null;
            size = 0;
            
            // add a chunk to the target buffer.
            handleChunk( a );

        }
        
    }
    
    /**
     * Dispatch a chunk.
     * 
     * @param chunk
     *            A chunk.
     */
    abstract protected void handleChunk(E[] chunk);
    
    @Override
    public long flush() {

        if (size == 0) {

            // nothing is buffered.
            return counter;
            
        }
        
        final int n = size;
        
        // move everything onto the target buffer.
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
    
    @Override
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
