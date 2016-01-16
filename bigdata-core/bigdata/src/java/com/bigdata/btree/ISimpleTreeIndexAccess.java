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
 * Created on Aug 1, 2012
 */
package com.bigdata.btree;

import com.bigdata.htree.HTree;

/**
 * Extended interface for tree-structured indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ISimpleTreeIndexAccess extends ISimpleIndexAccess {

    /**
     * The #of non-leaf nodes in the tree.
     */
    long getNodeCount();

    /**
     * The #of leaf nodes in the tree.
     */
    long getLeafCount();

    /**
     * The #of tuples in the tree.
     */
    long getEntryCount();

    /**
     * Return <code>true</code> iff the tree is balanced.
     * <p>
     * Note: Not all tree-structured indices are balanced. For example, the
     * {@link BTree} is balanced, but the {@link HTree} is not balanced. The
     * height of an unbalanced tree must be discovered through a traversal of
     * the tree.
     */
    boolean isBalanced();
    
    /**
     * The height of the tree. The height is the #of levels minus one. A tree
     * with only a root leaf has <code>height := 0</code>. A tree with a root
     * node and one level of leaves under it has <code>height := 1</code>.
     * <p>
     * Note that all leaves of a balanced tree (such as a B+Tree) are at the
     * same height (this is what is means to be "balanced"). Also note that the
     * height only changes when we split or join the root node (a B+Tree
     * maintains balance by growing and shrinking in levels from the top rather
     * than the leaves).
     * 
     * @throws UnsupportedOperationException
     *             if the tree is not a balanced tree.
     * 
     * @see #isBalanced()
     */
    int getHeight();

}
