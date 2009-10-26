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
 * Created on Oct 28, 2008
 */

package com.bigdata.relation.accesspath;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * An {@link IAsynchronousIterator} that may be serialized and sent to a remote
 * JVM for consumption. Since all data to be visited is supplied to the ctor,
 * the client will be able consume the data without waiting.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThickAsynchronousIterator<E> implements IAsynchronousIterator<E>,
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -5166933016517242441L;

    private transient boolean open = true;

    /**
     * Index of the last element visited by {@link #next()} and <code>-1</code>
     * if NO elements have been visited.
     */
    private int lastIndex;
    
    /**
     * The array of elements to be visited by the iterator.
     */
    private final E[] a;
    
    /**
     * Create a thick iterator.
     * 
     * @param a
     *            The array of elements to be visited by the iterator (may be
     *            empty, but may not be <code>null</code>).
     * 
     * @throws IllegalArgumentException
     *             if <i>a</i> is <code>null</code>.
     */
    public ThickAsynchronousIterator(final E[] a) {

        if (a == null)
            throw new IllegalArgumentException();
        
        this.a = a;

        lastIndex = -1;
        
    }
    
    private final void assertOpen() {
        
        if (!open)
            throw new IllegalStateException();
        
    }

    public boolean hasNext() {
        
        return lastIndex + 1 < a.length;

    }

    public E next() {
        
        if (!hasNext())
            throw new NoSuchElementException();
        
        return a[++lastIndex];
        
    }

    public void remove() {

        throw new UnsupportedOperationException();
        
    }

    /*
     * ICloseableIterator.
     */

    public void close() {

        open = false;
        
    }

    /*
     * IAsynchronousIterator.
     */
    
    public boolean isExhausted() {

        return !hasNext();
        
    }

    /**
     * Delegates to {@link #hasNext()} since all data are local and timeouts can
     * not occur.
     */
    public boolean hasNext(long timeout, TimeUnit unit) {

        return hasNext();
        
    }

    /**
     * Delegates to {@link #next()} since all data are local and timeouts can
     * not occur.
     */
    public E next(long timeout, TimeUnit unit) {

        return next();
        
    }

}
