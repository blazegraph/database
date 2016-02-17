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
 * Created on Sep 4, 2008
 */

package com.bigdata.rdf.sail;

import java.util.NoSuchElementException;

import info.aduna.iteration.CloseableIteration;

import cutthecrap.utils.striterators.ICloseableIterator;

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
    
    private boolean open = true;

    public Bigdata2SesameIteration(final ICloseableIterator<? extends T> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public void close() throws E {
        
        if (open) {
            
            open = false;
            
            src.close();
            
        }
        
    }

    public boolean hasNext() throws E {
        
        if(open && src.hasNext()) 
            return true;
        
        close();
        
        return false;
        
    }

    public T next() throws E {
        
        if (!hasNext())
            throw new NoSuchElementException();

        return src.next();
        
    }

    public void remove() throws E {

        if(!open)
            throw new IllegalStateException();
        
        src.remove();
        
    }

}
