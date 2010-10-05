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

    private final int entryCount;

    private final int height;

    private final int leafCount;

    private final int nodeCount;

    private final IBTreeUtilizationReport utilReport;

    public BTreeStatistics(final AbstractBTree btree) {
        this.m = btree.getBranchingFactor();
        this.entryCount = btree.getEntryCount();
        this.height = btree.getHeight();
        this.leafCount = btree.getLeafCount();
        this.nodeCount = btree.getNodeCount();
        this.utilReport = btree.getUtilization();
    }

    public int getBranchingFactor() {
        return m;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public int getHeight() {
        return height;
    }

    public int getLeafCount() {
        return leafCount;
    }

    public int getNodeCount() {
        return nodeCount;
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
                ",nodeCount=" + leafCount+ //
                ",utilReport=" + utilReport+ //
                "}";
    }

}
