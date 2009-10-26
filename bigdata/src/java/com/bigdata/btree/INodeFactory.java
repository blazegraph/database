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

import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;

/**
 * Interface for creating nodes or leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface INodeFactory {

    /**
     * Create a node.
     * 
     * @param btree
     *            The owning B+Tree.
     * @param addr
     *            The address from which the node was read.
     * @param data
     *            The node data record.
     *            
     * @return A node initialized from those data.
     */
    public Node allocNode(AbstractBTree btree, long addr, INodeData data);

    /**
     * Create a leaf.
     * 
     * @param btree
     *            The owning B+Tree.
     * @param addr
     *            The address from which the leaf was read.
     * @param data
     *            The leaf data record.
     * 
     * @return A leaf initialized from those data.
     */
    public Leaf allocLeaf(AbstractBTree btree, long addr, ILeafData data);

}
