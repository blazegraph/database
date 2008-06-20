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
 * Created on Jan 26, 2007
 */

package com.bigdata.join;

import java.util.Comparator;

/**
 * Represents the key order used by an index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME drive through {@link IChunkedIterator} or an extension of that
 * interface that knows about natural traversal orders and how to reorder the
 * elements that are being visited to support JOINs where the natural order for
 * the access paths is different for the left- and right-hand side of the JOIN.
 */
public interface IKeyOrder<E> {

    /**
     * Return the comparator that places elements into the natural order for the
     * associated index.
     */
    public Comparator<E> getComparator();

    /**
     * Return the {@link IKeyOrder} that will be used to read from the statement
     * index that is most efficient for the specified triple pattern.
     */
    public IKeyOrder<E> get(IPredicate<E> predicate);

}
