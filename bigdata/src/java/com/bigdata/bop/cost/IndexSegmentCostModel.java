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
 * Created on Sep 30, 2010
 */
package com.bigdata.bop.cost;

import com.bigdata.btree.IndexSegment;

/**
 * A cost model for a range scan on an {@link IndexSegment}.
 * <p>
 * Note: This uses a summary description of the {@link IndexSegment} for the
 * cost model. This makes sense because we generally have 100s of index segments
 * in scale-out and we do not want to probe them all for their exact costs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentCostModel {

    private final DiskCostModel diskCostModel;

    /**
     * 
     * @param diskCostModel
     *            The disk cost model.
     */
    public IndexSegmentCostModel(final DiskCostModel diskCostModel) {
        
        if (diskCostModel == null)
            throw new IllegalArgumentException();
        
        this.diskCostModel = diskCostModel;
        
    }
    
    /**
     * 
     * @param rangeCount
     *            The range count for the index scan.
     * @param branchingFactor
     *            The branching factor for the index segments for this scale-out
     *            index.
     * @param averageBytesPerLeaf
     *            The average #of bytes per leaf for this scale-out index.
     * @param xferBufferSize
     *            The size of the disk transfer buffer.
     * 
     * @return The estimated time for the range scan (milliseconds).
     */
    public double rangeScan(final int rangeCount, final int branchingFactor,
            final int averageBytesPerLeaf, final int xferBufferSize) {

        if (rangeCount == 0)
            return 0d;

        if (xferBufferSize == 0)
            throw new IllegalArgumentException();

        // One seek per leaf.
        final double averageSeekTime = diskCostModel.seekTime;

        // Expected #of leaves to visit.
        final int expectedLeafCount = (int) Math.ceil(((double) rangeCount)
                / branchingFactor);

        // Expected #of bytes to transfer.
        final int leafBytesToXFer = expectedLeafCount * averageBytesPerLeaf;

        // Expected #of disk transfers.
        final int xfers = (int) Math.ceil(((double) leafBytesToXFer)
                / xferBufferSize);

        // Expected transfer time (ms).
        final double xferTime = leafBytesToXFer
                / (diskCostModel.transferRate / 1000);

        // Expected disk seek time (ms).
        final double seekTime = averageSeekTime * xfers;

        // Expected total time (ms).
        final double totalTime = seekTime + xferTime;

        return totalTime;

    }

}
