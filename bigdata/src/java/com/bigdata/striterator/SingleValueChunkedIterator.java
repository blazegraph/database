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
 * Created on Jun 26, 2008
 */

package com.bigdata.striterator;


/**
 * An iterator that will visit a single value.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SingleValueChunkedIterator<E> extends ChunkedArrayIterator<E> {

    /**
     * @param e
     *            The value to be visited.
     */
    public SingleValueChunkedIterator(E e) {

        this(e, null/* keyOrder */);

    }

    /**
     * @param e
     *            The value to be visited (MAY not be <code>null</code>).
     * @param keyOrder
     *            The natural sort order (MAY be <code>null</code>).
     */
    public SingleValueChunkedIterator(final E e, final IKeyOrder<E> keyOrder) {

        super( 1, alloc(e), keyOrder);
        
    }
    
    @SuppressWarnings("unchecked")
    static private <E> E[] alloc(E e) {
        
        if (e == null)
            throw new IllegalArgumentException();
        
        return (E[]) java.lang.reflect.Array.newInstance(e.getClass()
                .getComponentType(), 1);
        
    }
    
}
