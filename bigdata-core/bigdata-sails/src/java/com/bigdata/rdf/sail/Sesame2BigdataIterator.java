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
 * Class aligns a Sesame 2 {@link CloseableIteration} with a bigdata
 * {@link ICloseableIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: Bigdata2SesameIteration.java 2265 2009-10-26 12:51:06Z
 *          thompsonbry $
 * @param <T>
 *            The generic type of the visited elements.
 * @param <E>
 *            The generic type of the exceptions thrown by the Sesame 2
 *            {@link CloseableIteration}.
 */
public class Sesame2BigdataIterator<T, E extends Exception> implements
        ICloseableIterator<T> {

    private final CloseableIteration<? extends T,E> src;
    
    private volatile boolean open = true;
    
    public Sesame2BigdataIterator(final CloseableIteration<? extends T,E> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public void close() {

        if (open) {
            open = false;
            try {
                src.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        
    }

    public boolean hasNext() {

        try {

            if (open && src.hasNext())
                return true;

            close();
            
            return false;
            
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    public T next() {

        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            return src.next();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        
    }

    public void remove() {

        if(!open)
            throw new IllegalStateException();

        try {
            src.remove();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        
    }

}
