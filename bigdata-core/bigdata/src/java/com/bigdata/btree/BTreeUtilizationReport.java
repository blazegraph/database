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
 * Created on Oct 1, 2010
 */

package com.bigdata.btree;

import java.io.Serializable;

/**
 * A btree utilization report.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTreeUtilizationReport implements IBTreeUtilizationReport,
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final int leafUtilization;

    private final int nodeUtilization;

    private final int totalUtilization;

    public BTreeUtilizationReport(final IBTreeStatistics stats) {

        final long nnodes = stats.getNodeCount();

        final long nleaves = stats.getLeafCount();

        final long nentries = stats.getEntryCount();

        final long numNonRootNodes = nnodes + nleaves - 1;

        final int branchingFactor = stats.getBranchingFactor();

        /*
         * Note: It is quite possible for the intermediate products in
         * these formulae to exceed Integer.MAX_VALUE, which is why we
         * need to cast to long.  The result will always be in [0:100].
         */
        
		nodeUtilization = (int) (nnodes == 0 ? 100 : (100L * numNonRootNodes)
				/ (nnodes * (long) branchingFactor));

        leafUtilization = (int) (nleaves == 0 ? 0 : (100L * nentries)
                / (nleaves * (long) branchingFactor));

        totalUtilization = (nodeUtilization + leafUtilization) / 2;

    }

    public int getLeafUtilization() {
        return leafUtilization;
    }

    public int getNodeUtilization() {
        return nodeUtilization;
    }

    public int getTotalUtilization() {
        return totalUtilization;
    }

    /**
     * Human readable representation.
     */
    public String toString() {
        return super.toString() + //
                "{leafUtil=" + leafUtilization+ //
                ",nodeUtil=" + nodeUtilization+ //
                ",totalUtil=" + totalUtilization+ //
                "}";
    }

}
