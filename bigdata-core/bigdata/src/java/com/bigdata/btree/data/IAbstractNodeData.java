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
 * Created on Dec 19, 2006
 */

package com.bigdata.btree.data;

import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.IDataRecordAccess;

/**
 * Interface for low-level data access.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IAbstractNodeData extends IDataRecordAccess {

    /**
     * True iff this is a leaf node.
     */
    boolean isLeaf();

    /**
     * True iff this is an immutable data structure.
     */
    boolean isReadOnly();

    /**
     * <code>true</code> iff this is a coded data structure.
     */
    boolean isCoded();

    /**
     * {@inheritDoc}
     * 
     * @throws UnsupportedOperationException
     *             unless {@link #isCoded()} returns <code>true</code>.
     */
    @Override
    AbstractFixedByteArrayBuffer data();
    
    /**
     * Return <code>true</code> iff the leaves maintain tuple revision
     * timestamps. When <code>true</code>, the minimum and maximum tuple
     * revision timestamp for a node or leaf are available from
     * {@link #getMinimumVersionTimestamp()} and
     * {@link #getMaximumVersionTimestamp()}.
     */
    boolean hasVersionTimestamps();

    /**
     * The earliest tuple revision timestamp associated with any tuple spanned
     * by this node or leaf. If there are NO tuples for the leaf, then this MUST
     * return {@link Long#MAX_VALUE} since the initial value of the minimum
     * version timestamp is always the largest possible long integer.
     * 
     * @throws UnsupportedOperationException
     *             unless tuple revision timestamps are being maintained.
     */
    long getMinimumVersionTimestamp();

    /**
     * The most recent tuple revision timestamp associated with any tuple
     * spanned by this node or leaf. If there are NO tuples for the leaf, then
     * this MUST return {@link Long#MIN_VALUE} since the initial value of the
     * maximum version timestamp is always the smallest possible long integer.
     * 
     * @throws UnsupportedOperationException
     *             unless tuple revision timestamps are being maintained.
     */
    long getMaximumVersionTimestamp();
    
}
