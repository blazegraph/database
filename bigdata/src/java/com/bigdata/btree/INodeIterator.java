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
 * Created on Jan 10, 2007
 */

package com.bigdata.btree;

import java.util.Iterator;

/**
 * Interface for iterators that visit nodes and leaves rather than entries in
 * leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface INodeIterator extends Iterator {

    /**
     * The value of the key for the last entry visited by
     * {@link Iterator#next()}.
     * 
     * @exception IllegalStateException
     *                if no entries have been visited.
     */
    public Object getKey();
    
    /**
     * The value associated with the last node or leaf visited by
     * {@link Iterator#next()}.
     * 
     * @exception IllegalStateException
     *                if no entries have been visited.
     */
    public IAbstractNode getNode();
    
}
