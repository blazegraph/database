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
 * Created on Sep 4, 2008
 */

package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import com.bigdata.striterator.ICloseableIterator;

/**
 * Class aligns a bigdata {@link ICloseableIterator} with a Sesame 2
 * {@link CloseableIteration}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the visited elements.
 * @param <E>
 *            The generic type of the exceptions thrown by the Sesame 2
 *            {@link CloseableIteration}.
 */
public class Bigdata2SesameIteration<T, E extends Exception> implements
        CloseableIteration<T, E> {

    private final ICloseableIterator<? extends T> src;
    
    public Bigdata2SesameIteration(final ICloseableIterator<? extends T> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public void close() throws E {
        
        src.close();
        
    }

    public boolean hasNext() throws E {
        
        return src.hasNext();
        
    }

    public T next() throws E {
        
        return src.next();
        
    }

    public void remove() throws E {

        src.remove();
        
    }

}
