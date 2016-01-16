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
 * A snapshot of the B+Tree statistics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BTreeStatistics implements IBTreeStatistics, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final int m;

    private final int height;

    private final long nodeCount;

    private final long leafCount;

    private final long entryCount;

    private final IBTreeUtilizationReport utilReport;

    public BTreeStatistics(final AbstractBTree btree) {
        this.m = btree.getBranchingFactor();
        this.height = btree.getHeight();
        this.nodeCount = btree.getNodeCount();
        this.leafCount = btree.getLeafCount();
        this.entryCount = btree.getEntryCount();
        this.utilReport = btree.getUtilization();
    }

    public int getBranchingFactor() {
        return m;
    }

    public int getHeight() {
        return height;
    }

    public long getNodeCount() {
        return nodeCount;
    }

    public long getLeafCount() {
        return leafCount;
    }

    public long getEntryCount() {
        return entryCount;
    }

    public IBTreeUtilizationReport getUtilization() {
        return utilReport;
    }

    /**
     * Human readable representation.
     */
    public String toString() {
        return super.toString() + //
                "{m=" + m+ //
                ",entryCount=" + entryCount+ //
                ",height=" + height+ //
                ",leafCount=" + leafCount+ //
                ",nodeCount=" + nodeCount+ //
                ",utilReport=" + utilReport+ //
                "}";
    }

}
