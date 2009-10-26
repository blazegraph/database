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
 * Created on Sep 18, 2009
 */

package com.bigdata.striterator;

import java.util.Iterator;

/**
 * Wraps a normal {@link Iterator} as an {@link ICloseableIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class CloseableIteratorWrapper<E> implements ICloseableIterator<E> {

    private final Iterator<E> src;
    
    public CloseableIteratorWrapper(final Iterator<E> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }
    
    /** NOP. */
    public void close() {
    }

    public boolean hasNext() {
        return src.hasNext();
    }

    public E next() {
        return src.next();
    }

    public void remove() {
        src.remove();
    }
    
}

