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

/**
 * Interface for low-level data access for the non-leaf nodes of a B+-Tree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface INodeData extends IAbstractNodeData {

    /**
     * The #of children of this node. Either all children will be nodes or all
     * children will be leaves. The #of children of a node MUST be
     * <code>{@link IAbstractNodeData#getKeyCount()}+1</code>
     * 
     * @return The #of children of this node.
     */
    public int getChildCount();

    /**
     * The backing array of the persistent addresses of the children. Only the
     * first {@link #getChildCount()} entries in the returned array are defined.
     * If an entry is zero(0L), then the corresponding child is not persistent.
     * The use of this array is dangerous since mutations are directly reflected
     * in the node, but it may be highly efficient. Callers MUST exercise are to
     * perform only read-only operations against the returned array.
     * 
     * @return The backing array of persistent child addresses.
     * 
     * @deprecated This is only used to persistent the child addresses, which is
     *             done by the {@link NodeSerializer}. With the refactor to use
     *             a binary image for the persistent nodes and leaves we will no
     *             need this method. It can disappear into the implementation.
     *             <p>
     *             However, getChildAddr(int) will be needed by
     *             {@link Node#getChild(int)}.
     */
    public long[] getChildAddr();

    /**
     * Return the persistent addresses of the specified child node.
     * 
     * @param index
     *            The index of the child in [0:nkeys+1].
     * 
     * @return The persistent child address -or- zero(0L) if the child is not
     *         persistent.
     */
    public long getChildAddr(int index);

//    /**
//     * The #of entries (aka keys or values) spanned by each child of this node.
//     * The sum of the defined values in this array should always be equal to the
//     * value returned by {@link #getSpannedTupleCount()}. These data are used to
//     * support fast computation of the index at which a key occurs and the #of
//     * entries in a given key range.
//     * 
//     * @deprecated by #getChildEntryCount(int).
//     */
//    public int[] getChildEntryCounts();

    /**
     * Return the #of tuples spanned by the indicated child of this node. The
     * sum of the values returned by this method across the children of the node
     * should always equal the value returned by {@link #getSpannedTupleCount()}
     * . These data are used to support fast computation of the index at which a
     * key occurs and the #of entries in a given key range.
     * 
     * @param index
     *            The index of the child in [0:nkeys+1].
     * 
     * @return The #of tuples spanned by that child.
     */
    public int getChildEntryCount(int index);

}
