/**

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
 * Created on Sep 6, 2010
 */

package com.bigdata.striterator;

import java.util.Arrays;
import java.util.Iterator;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.ICloseable;
import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Wrap an iterator visiting chunks as an iterator visiting the individual
 * elements in each chunk.
 * 
 * @param <E>
 *            The generic type of the elements in the chunks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see CloseableChunkedIteratorWrapperConverter
 */
public class Dechunkerator<E> implements ICloseableIterator<E> {

    /**
     * The iterator given to the constructor.
     */
    private final Iterator<E[]> src;
    
    /**
     * The iterator which visits the unchunked elements.
     */
    private final Iterator<E> itr;

    /**
     * Wrap an iterator visiting chunks as an iterator visiting the individual
     * elements in each chunk.
     * 
     * @param src
     *            An iterator visiting chunks of type <E>
     */
    @SuppressWarnings("unchecked")
    public Dechunkerator(final Iterator<E[]> src) {

        this.src = src;
        
        itr = new Striterator(src).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            @Override
            protected Iterator<E> expand(final Object e) {
             
                return Arrays.asList((E[]) e).iterator();
                
            }

        });

    }

    @Override
    public boolean hasNext() {

        return itr.hasNext();
        
    }

    @Override
    public E next() {
     
        return itr.next();
        
    }

    @Override
    public void remove() {

        throw new UnsupportedOperationException();

    }

    @Override
    public void close() {

        if (src instanceof ICloseable) {

            ((ICloseable) src).close();

        }

    }

}
