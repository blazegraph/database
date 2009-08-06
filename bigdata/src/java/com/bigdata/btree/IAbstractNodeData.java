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

package com.bigdata.btree;

import java.io.OutputStream;

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

//    /**
//     * The branching factor is maximum the #of children for a node or maximum
//     * the #of values for a leaf.
//     * 
//     * @return The branching factor.
//     * 
//     * @deprecated This is a field on the AbstractBTree. It is not stored on the
//     *             node/leaf data record and should not be accessed here.
//     */
//    public int getBranchingFactor();

    /**
     * The #of tuples spanned by this node or leaf. For a leaf this is always
     * the #of keys.
     * 
     * @see INodeData#getChildEntryCounts()
     */
    public int getEntryCount();

    /**
     * The #of keys defined keys for the node or leaf. The maximum #of keys for
     * a node is one less than the {@link #getBranchingFactor()}. The maximum
     * #of keys for a leaf is the {@link #getBranchingFactor()}.
     * 
     * @return The #of defined keys.
     */
    public int getKeyCount();

    /**
     * The object used to contain and manage the keys.
     * 
     * @deprecated in favor of {@link #copyKey(int, OutputStream)} or even
     *             {@link #getKey(int)} if you must.
     */
    public IKeyBuffer getKeys();

    /**
     * Copy the indicated key onto the callers stream. This can be more
     * efficient than {@link #getKey(int)} when the caller is single threaded
     * and a buffer can be reused for each request.
     * 
     * @param index
     *            The index of the key in the node or leaf.
     * 
     * @param os
     *            The stream onto which to copy the key.
     */
    public void copyKey(int index, OutputStream os);

    /**
     * Return the key at the indicated index.
     * 
     * @param index
     *            The index of the key in the node or leaf.
     * 
     * @return The key at that index.
     */
    public byte[] getKey(int index);

}
