/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 6, 2010
 */

package com.bigdata.striterator;

import java.util.Arrays;
import java.util.Iterator;

import cutthecrap.utils.striterators.Expander;
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
            protected Iterator expand(Object arg0) {
                return Arrays.asList((E[]) arg0).iterator();
            }
        });

    }

    public boolean hasNext() {

        return itr.hasNext();
        
    }

    public E next() {
     
        return itr.next();
        
    }

    public void remove() {

        throw new UnsupportedOperationException();

    }

    public void close() {

        if (src instanceof ICloseableIterator<?>) {

            ((ICloseableIterator<?>) src).close();

        }

    }

}
