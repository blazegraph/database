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
 * Created on May 2nd, 2011
 */
package com.bigdata.btree.data;

import com.bigdata.btree.raba.IRaba;

/**
 * Interface for access to the keys {@link IRaba} of a node or leaf in an index
 * data structure. 
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ILeafData.java 4388 2011-04-11 13:35:47Z thompsonbry $
 */
public interface IKeysData {

    /**
     * Return the #of keys in the node or leaf. A node has <code>nkeys+1</code>
     * children. A leaf has <code>nkeys</code> keys and values. The maximum #of
     * keys for a node is one less than the branching factor of the B+Tree. The
     * maximum #of keys for a leaf is the branching factor of the B+Tree. For a
     * hash bucket, this is the #of entries in the bucket.
     * 
     * @return The #of defined keys.
     */
    int getKeyCount();

    /**
     * The object used to contain and manage the keys.
     */
    IRaba getKeys();

}
