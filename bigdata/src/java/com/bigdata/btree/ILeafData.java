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

package com.bigdata.btree;

import java.io.OutputStream;

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
     * the leaf.
     */
    public IRaba getValues();

    /**
     * Return <code>true</code> iff the value stored at the specified index is
     * <code>null</code>. If you are visiting deleted tuples, then check to see
     * if the tuple is deleted and only invoke this method on the non-deleted
     * tuples.
     * 
     * @param index
     *            The index into the leaf.
     * 
     * @deprecated by {@link IRaba#isNull(int)}
     */
    public boolean isNull(int index);

    /**
     * Copy the indicated value onto the callers stream unless the tuple is
     * flagged as deleted or the value associated with the tuple is
     * <code>null</code>.
     * 
     * @param index
     *            The index of the tuple in the leaf.
     * 
     * @param os
     *            The stream onto which to copy the value.
     * 
     * @throws UnsupportedOperationException
     *             if the value stored at that index is <code>null</code> -or-
     *             the tuple is flagged as deleted.
     * 
     * @see #isNull(int)
     * 
     *      FIXME Do we need to defined and use getValue(int), copy(int,OS),
     *      etc. are on the ILeafData interface because they must also test the
     *      deleted flag? Or does that happen at the Tuple and AbstractBTree API
     *      level? The danger is that we could permit unintended access to a
     *      delete value, which would be reported as a <code>null</code> (or for
     *      copy() by throwing a NullPointerException).
     * 
     * @deprecated by
     *             {@link IRaba#copy(int, java.io.DataOutput)}.
     */
    public void copyValue(int index, OutputStream os);

    /**
     * The timestamp for the entry at the specified index.
     * 
     * @return The timestamp for the index entry.
     * 
     * @throws UnsupportedOperationException
     *             if timestamps are not being maintained.
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
