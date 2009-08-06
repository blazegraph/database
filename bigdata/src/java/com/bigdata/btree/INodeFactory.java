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
package com.bigdata.btree;

/**
 * Interface for creating mutable nodes or leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This will be replaced with a factory suitable for creating a node
 *             or leaf data object from a ByteBuffer.
 */
public interface INodeFactory {

    /**
     * Create a node. The implementation is encouraged to steal the <i>keys</i>
     * and <i>childAddr</i> references rather than cloning them.
     * 
     * @param btree
     *            The owning btree.
     * @param addr
     *            The address from which the node was read.
     * @param branchingFactor
     *            The branching factor for the node.
     * @param nentries
     *            The #of entries spanned by this node.
     * @param keys
     *            A representation of the defined keys in the node.
     * @param childAddr
     *            An array of the persistent addresses for the children of this
     *            node.
     * @param childEntryCount
     *            An of the #of entries spanned by each direct child.
     * 
     * @return A node initialized from those data.
     */
    public INodeData allocNode(IIndex btree, long addr, int branchingFactor,
            int nentries, IKeyBuffer keys, long[] childAddr,
            int[] childEntryCount);

    /**
     * Create a leaf. The implementation is encouraged to steal the <i>keys</i>
     * and <i>values</i> references rather than cloning them.
     * 
     * @param btree
     *            The owning btree.
     * @param addr
     *            The address from which the leaf was read.
     * @param branchingFactor
     *            The branching factor for the leaf.
     * @param keys
     *            A representation of the defined keys in the node.
     * @param values
     *            An array containing the values found in the leaf.
     * @param versionTimestamps
     *            An array of the version timestamps (iff the version metadata
     *            is being maintained).
     * @param deleteMarkers
     *            An array of the delete markers (iff the version metadata is
     *            being maintained).
     * @param priorAddr
     *            The address of the previous leaf in key order, <code>0L</code>
     *            if it is known that there is no previous leaf, and
     *            <code>-1L</code> if either: (a) it was not known whether
     *            there is a previous leaf; or (b) it was known that there was a
     *            previous leaf but the address of that leaf was not known.
     * @param nextAddr
     *            The address of the next leaf in key order, <code>0L</code>
     *            if it is known that there is no next leaf, and
     *            <code>-1L</code> if either: (a) it was not known whether
     *            there is a next leaf; or (b) it was known that there was a
     *            next leaf but the address of that leaf was not known.
     * 
     * @return A leaf initialized from those data.
     */
    public ILeafData allocLeaf(IIndex btree, long addr, int branchingFactor,
            IKeyBuffer keys, byte[][] values, long[] versionTimestamps,
            boolean[] deleteMarkers, long priorAddr, long nextAddr);

}
