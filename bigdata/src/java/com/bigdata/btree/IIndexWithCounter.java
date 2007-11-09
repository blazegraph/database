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
 * Created on May 17, 2007
 */

package com.bigdata.btree;

/**
 * Interface that exposes a counter associated with an index or index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndexWithCounter extends IIndex {
    
    /**
     * A restart-safe counter. For an unpartitioned index, this a single counter
     * for the entire index with an initial value of zero (0) and it is stored
     * in the index metadata record. For a partitioned index, there is a
     * distinct counter for each index partition, the partition identifier is
     * used as the high int32 bits of the counter, and the low int32 of the
     * counter has an initial value of zero (0) in each index partition.
     * 
     * @todo consider supporting named counters.
     */
    public ICounter getCounter();

}
