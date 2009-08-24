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

import com.bigdata.btree.raba.IRaba;

/**
 * Interface for low-level data access for the leaves of a B+-Tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ILeafData extends IAbstractNodeData {

    /**
     * The #of values in the leaf (this MUST be equal to the #of keys for a
     * leaf).
     * 
     * @return The #of values in the leaf.
     */
    public int getValueCount();

    /**
     * Return the object storing the logical byte[][] containing the values for
     * the leaf. When the leaf maintains delete markers you MUST check whether
     * or not the tuple is deleted before requesting its value.
     * 
     * @see #hasDeleteMarkers()
     * @see #getDeleteMarker(int)
     */
    public IRaba getValues();

    /**
     * The version timestamp for the entry at the specified index.
     * 
     * @return The version timestamp for the index entry.
     * 
     * @throws UnsupportedOperationException
     *             if version timestamps are not being maintained (they are only
     *             required for indices on which transaction processing will be
     *             used).
     */
    public long getVersionTimestamp(int index);
    
    /**
     * Return <code>true</code> iff the entry at the specified index is marked
     * as deleted.
     * 
     * @throws UnsupportedOperationException
     *             if delete markers are not being maintained.
     */
    public boolean getDeleteMarker(int index);
    
    /**
     * Return <code>true</code> iff the leaf maintains version timestamps.
     */
    public boolean hasVersionTimestamps();
    
    /**
     * Return <code>true</code> iff the leaf maintains delete markers.
     */
    public boolean hasDeleteMarkers();
    
}
