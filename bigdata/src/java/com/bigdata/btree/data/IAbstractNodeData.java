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
 * Created on Dec 19, 2006
 */

package com.bigdata.btree.data;

import com.bigdata.btree.raba.IRaba;

/**
 * Interface for low-level data access.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAbstractNodeData {

    /**
     * True iff this is a leaf node.
     */
    public boolean isLeaf();

    /**
     * True iff this is an immutable data structure.
     */
    public boolean isReadOnly();

//    /**
//     * Return <code>true</code> iff the leaves maintain tuple revision
//     * timestamps. When <code>true</code>, the minimum and maximum tuple
//     * revision timestamp for a node or leaf are available from
//     * {@link #getMinimumVersionTimestamp()} and
//     * {@link #getMaximumVersionTimestamp()}.
//     */
//    public boolean hasVersionTimestamps();
//
//    /**
//     * The earliest tuple revision timestamp associated with any tuple spanned
//     * by this node or leaf.
//     * 
//     * @throws UnsupportedOperationException
//     *             unless tuple revision timestamps are being maintained.
//     */
//    public long getMinimumVersionTimestamp();
//
//    /**
//     * The most recent tuple revision timestamp associated with any tuple
//     * spanned by this node or leaf.
//     * 
//     * @throws UnsupportedOperationException
//     *             unless tuple revision timestamps are being maintained.
//     */
//    public long getMaximumVersionTimestamp();
    
    /**
     * The #of tuples spanned by this node or leaf. For a leaf this is always
     * the #of keys.
     * 
     * @see INodeData#getChildEntryCounts()
     */
    public int getSpannedTupleCount();

    /**
     * Return the #of keys in the node or leaf. A node has <code>nkeys+1</code>
     * children. A leaf has <code>nkeys</code> keys and values. The maximum #of
     * keys for a node is one less than the branching factor of the B+Tree. The
     * maximum #of keys for a leaf is the branching factor of the B+Tree.
     * 
     * @return The #of defined keys.
     */
    public int getKeyCount();

    /**
     * The object used to contain and manage the keys.
     */
    public IRaba getKeys();

}
