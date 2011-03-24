/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Oct 1, 2010
 */

package com.bigdata.btree;

/**
 * B+Tree utilization report.
 * <p>
 * Note: The utilization numbers reported by this interface are a bit
 * misleading. They only consider the #of index positions in the node or leaf
 * which is full, but do not take into account the manner in which the
 * persistence store allocates space to the node or leaf. For example, for the
 * WORM we do perfect allocations but retain many versions. For the RWStore, we
 * do best-fit allocations but recycle old versions. The space efficiency of the
 * persistence store is typically the main driver, not the utilization rate as
 * reported here.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IBTreeUtilizationReport.java 3713 2010-10-01 15:36:09Z
 *          thompsonbry $
 */
public interface IBTreeUtilizationReport {

    /**
     * The leaf utilization percentage [0:100]. The leaf utilization is computed
     * as the #of values stored in the tree divided by the #of values that could
     * be stored in the #of allocated leaves.
     */
    int getLeafUtilization();

    /**
     * The node utilization percentage [0:100]. The node utilization is computed
     * as the #of non-root nodes divided by the #of non-root nodes that could be
     * addressed by the tree.
     */
    int getNodeUtilization();

    /**
     * The total utilization percentage [0:100]. This is the average of the leaf
     * utilization and the node utilization.
     */
    int getTotalUtilization();

}
