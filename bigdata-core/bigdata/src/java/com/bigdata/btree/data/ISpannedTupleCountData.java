/**

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
	 * The #of tuples spanned by this node. For a leaf, the corresponding value
	 * is reported by {@link ILeafData#getKeyCount()} or
	 * {@link ILeafData#getValueCount()}.
	 */
    long getSpannedTupleCount();

    /**
     * Return the #of tuples spanned by the indicated child of this node. The
     * sum of the values returned by this method across the children of the node
     * should always equal the value returned by {@link #getSpannedTupleCount()}
     * . These data are used to support fast computation of the index at which a
     * key occurs and the #of entries in a given key range.
     * 
     * @param index
     *            The index of the child in [0:nkeys].
     * 
     * @return The #of tuples spanned by that child.
     */
    long getChildEntryCount(int index);

}
