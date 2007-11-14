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
package com.bigdata.rdf.spo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.rdf.util.KeyOrder;

import cutthecrap.utils.striterators.Striterator;

/**
 * Converts an <code>Iterator&lt;SPO&gt;</code> into chunked iterator.
 * <p>
 * Note: The visitation order is whatever the order was for the source iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo todo refactor to extend {@link ChunkedIterator}. The filter will have
 *       to be applied during the super() in the ctor.
 */
public class ChunkedSPOIterator implements ISPOIterator {
    
    private boolean open = true;
    
    private final Iterator<SPO> src;
    
    private final int chunkSize;
    
    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     */
    public ChunkedSPOIterator(Iterator<SPO>src) {
        
        this(src, 10000, null);
        
    }

    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     * @param chunkSize
     *            The desired chunk size.
     */
    public ChunkedSPOIterator(Iterator<SPO>src, int chunkSize) {
        
        this(src, chunkSize , null );
        
    }
    
    /**
     * Create an iterator that reads from the source.
     * 
     * @param src
     *            The source iterator.
     * @param chunkSize
     *            The desired chunk size.
     * @param filter
     *            An optional filter.
     */
    public ChunkedSPOIterator(Iterator<SPO>src, int chunkSize, ISPOFilter filter) {

        if (src == null)
            throw new IllegalArgumentException();

        if (chunkSize <= 0)
            throw new IllegalArgumentException();
        
        /*
         * If a filter was specified then use a Striterator to filter the source
         * iterator such that it includes only those SPOs that match the filter.
         */
        
        this.src = (filter == null //
                ? src //
                : new Striterator(src).addFilter(new SPOFilter(filter))
                );
     
        this.chunkSize = chunkSize;
        
    }
    
    public void close() {

        if (!open)
            return;
        
        open = false;

        if(src instanceof ISPOIterator) {
            
            ((ISPOIterator)src).close();
            
        }

    }

    /**
     * Returns <code>null</code> since the {@link SPO}s are not in any
     * specific order as visited by {@link #next()}.
     */
    public KeyOrder getKeyOrder() {

        return null;
        
    }
    
    /**
     * Return <code>true</code> if there are {@link SPO}s in the source
     * iterator.
     */
    public boolean hasNext() {

        if(!open) return false;

        return src.hasNext();
        
    }

    /**
     * The next {@link SPO} in the source iterator.
     */
    public SPO next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        return src.next();

    }

    /**
     * The next chunk of {@link SPO}s in whatever order the were visited by
     * {@link #next()}.
     */
    public SPO[] nextChunk() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        SPO[] chunk = new SPO[chunkSize];

        int n = 0;

        while (hasNext() && n < chunkSize) {

            // add to this chunk.
            chunk[n++] = next();
            
        }
        
        if (n != chunkSize) {

            // make it dense.
            
            SPO[] tmp = new SPO[n];
            
            System.arraycopy(chunk, 0, tmp, 0, n);
            
            chunk = tmp;
         
        }
        
        return chunk;
        
    }

    public SPO[] nextChunk(KeyOrder keyOrder) {
        
        if (keyOrder == null)
            throw new IllegalArgumentException();

        SPO[] chunk = nextChunk();
        
        // sort into the required order.

        Arrays.sort(chunk, 0, chunk.length, keyOrder.getComparator());

        return chunk;
        
    }

    /**
     * Delegated to the source iterator.
     */
    public void remove() {
        
        src.remove();
        
    }
    
}