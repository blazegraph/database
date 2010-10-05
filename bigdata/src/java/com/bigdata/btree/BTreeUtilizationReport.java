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

        final int nnodes = stats.getNodeCount();

        final int nleaves = stats.getLeafCount();

        final int nentries = stats.getEntryCount();

        final int numNonRootNodes = nnodes + nleaves - 1;

        final int branchingFactor = stats.getBranchingFactor();

        nodeUtilization = nnodes == 0 ? 100 : (100 * numNonRootNodes)
                / (nnodes * branchingFactor);

        leafUtilization = (100 * nentries) / (nleaves * branchingFactor);

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
