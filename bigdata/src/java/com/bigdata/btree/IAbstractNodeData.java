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
     * The branching factor is maximum the #of children for a node or maximum
     * the #of values for a leaf.
     * 
     * @return The branching factor.
     */
    public int getBranchingFactor();
    
    /**
     * The #of entries (aka keys or values) spanned by this node or leaf.
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
     */
    public IKeyBuffer getKeys();
    
}
