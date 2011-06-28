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
 * Created on Oct 8, 2007
 */

package com.bigdata.btree;

/**
 * An interface that may be used to learn when a {@link BTree} becomes
 * dirty.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IDirtyListener {

    /**
     * The btree has become dirty.
     * <p>
     * Note: This event is always generated for a new btree. Once a btree is
     * created it remains dirty until the root (and any dirty children) have
     * been flushed to the backing store. A btree that is read from the backing
     * store is always clean and consists of "immutable" nodes and/or leaves. A
     * btree remains clean until there is a write on some node or leaf. That
     * write triggers copy-on-write, which percolates from the point of the
     * write up to the root node and results in the reference to the root node
     * being replaced.  When that happens a dirty event is generated.
     * 
     * @param btree
     *            The btree.
     */
    public void dirtyEvent(BTree btree);
    
}
