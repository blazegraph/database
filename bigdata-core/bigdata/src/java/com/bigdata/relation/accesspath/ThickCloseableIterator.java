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
 * Created on Oct 28, 2008
 */

package com.bigdata.relation.accesspath;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.NoSuchElementException;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * An {@link ICloseableIterator} that may be serialized and sent to a remote
 * JVM for consumption.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThickCloseableIterator<E> implements ICloseableIterator<E>,
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

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
     * The number of elements to be visited by the iterator.
     */
    private final int len;
    
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
    public ThickCloseableIterator(final E[] a) {

        if (a == null)
            throw new IllegalArgumentException();
        
        this.a = a;
        this.len = a.length;

        lastIndex = -1;
        
    }

    /**
     * Create a thick iterator.
     * 
     * @param a
     *            The array of elements to be visited by the iterator (may be
     *            empty, but may not be <code>null</code>).
     * @param len
     *            The number of elements to be visited by the iterator. Must be
     *            less than the length of the array.
     * 
     * @throws IllegalArgumentException
     *             if <i>a</i> is <code>null</code>.
     */
    public ThickCloseableIterator(final E[] a, final int len) {

        if (a == null)
            throw new IllegalArgumentException();
        
        if (len > a.length)
        	throw new IllegalArgumentException();
        
        this.a = a;
        this.len = len;

        lastIndex = -1;
        
    }

    public boolean hasNext() {
        
        if(open && lastIndex + 1 < len)
            return true;
        
        close();
        
        return false;

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

    private void readObject(ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        
        in.defaultReadObject();
        
        open = true;
        
   }
    
}
