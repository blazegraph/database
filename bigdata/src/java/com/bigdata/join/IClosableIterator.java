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
 * Created on Jun 25, 2008
 */

package com.bigdata.join;

import java.util.Iterator;

/**
 * An iterator that defines a {@link #close()} method - you MUST close instances
 * of this interface. Many implementation depends on this in order to release
 * resources, terminate tasks, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IClosableIterator<E> extends Iterator<E> {

    /**
     * Closes the iterator, releasing any associated resources. This method MAY
     * be invoked safely if the iterator is already closed.
     * <p>
     * Note: Implementations that support {@link Iterator#remove()} MUST NOT
     * eagerly close the iterator when it is exhausted since that would make it
     * impossible to remove the last visited statement. Instead they MUST wait
     * for an explicit {@link #close()} by the application.
     */
    public void close();
    
}
