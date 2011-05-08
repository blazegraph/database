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
 * Created on Dec 15, 2006
 */

package com.bigdata.btree.data;

/**
 * Interface for low-level data access to the #of tuples spanned by a node or
 * leaf of an index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ILeafData.java 4388 2011-04-11 13:35:47Z thompsonbry $
 */
public interface ISpannedTupleCountData {

    /**
     * The #of tuples spanned by this node or leaf. For a leaf this is always
     * the #of keys.
     * 
     * @see INodeData#getChildEntryCounts()
     */
    int getSpannedTupleCount();

}
