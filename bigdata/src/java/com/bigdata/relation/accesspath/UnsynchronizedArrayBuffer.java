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


/**
 * An unsynchronized buffer backed by a fixed capacity array that migrates
 * references onto the caller's buffer (which may be synchronized) using
 * {@link IBuffer#add(int, Object[])}. This class may be useful when the code
 * is single threaded or and in reducing contention for locks or synchronized
 * blocks of code by reducing the frequency with which those methods are
 * invoked.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnsynchronizedArrayBuffer<E> extends AbstractUnsynchronizedArrayBuffer<E> {

    private final IBuffer<E> target;
    
    /**
     * @param capacity
     *            The capacity of the backing buffer.
     * @param target
     *            The target buffer onto which the elements will be flushed.
     */
    public UnsynchronizedArrayBuffer(int capacity, IBuffer<E> target) {
    
        super(capacity);
        
        if (target == null)
            throw new IllegalArgumentException();

        this.target = target;
        
    }
    
    /**
     * Adds all references in the internal buffer to the target buffer using
     * {@link IBuffer#add(int, Object[])} and sets the #of elements in the
     * internal buffer to ZERO (0).
     */
    protected void overflow() {

        if (size > 0) {

            if (log.isInfoEnabled())
                log.info("size=" + size);

            target.add(size, buffer);

            size = 0;

        }
        
    }
    
    public long flush() {

        // move everthing onto the target buffer.
        overflow();
        
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
