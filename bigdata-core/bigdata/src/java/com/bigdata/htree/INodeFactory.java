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
package com.bigdata.htree;

import com.bigdata.btree.data.ILeafData;
import com.bigdata.htree.data.IDirectoryData;

/**
 * Interface for creating nodes or leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: INodeFactory.java 2265 2009-10-26 12:51:06Z thompsonbry $
 */
public interface INodeFactory {

	/**
	 * Create a node.
	 * 
	 * @param htree
	 *            The owning {@link HTree}.
	 * @param addr
	 *            The address from which the node was read.
	 * @param data
	 *            The node data record.
	 * 
	 * @return A node initialized from those data.
	 */
	public DirectoryPage allocNode(AbstractHTree htree, long addr,
			IDirectoryData data);

	/**
	 * Create a leaf.
	 * 
	 * @param htree
	 *            The owning {@link HTree}.
	 * @param addr
	 *            The address from which the leaf was read.
	 * @param data
	 *            The leaf data record.
	 * 
	 * @return A leaf initialized from those data.
	 */
	public BucketPage allocLeaf(AbstractHTree htree, long addr, ILeafData data);

}
