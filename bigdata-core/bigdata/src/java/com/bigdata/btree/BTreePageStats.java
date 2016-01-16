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
package com.bigdata.btree;

import com.bigdata.util.Bytes;

public class BTreePageStats extends PageStats {

    public BTreePageStats() {
    }

    public void visit(final AbstractBTree btree, final AbstractNode<?> node) {

        if (m == 0) {

            // Make a note of the configured branching factor.
            m = btree.getBranchingFactor();

            ntuples = btree.getEntryCount();

            height = btree.getHeight();

        }

        super.visit(btree, node);
        
    }

    @Override
    public int getRecommendedBranchingFactor() {

        if (nnodes == 0) {

            // Not enough data to make an estimate.
            return m;
            
        }

        // Nominal (target) page size.
        final int NOMINAL_PAGE_SIZE = 8 * Bytes.kilobyte32;

        // The maximum #of allocations that can be blobs.
        final float maxPercentBlobs = .05f;

        // The percentage of the total allocations in each slot size.
        final float[] percentages = new float[SLOT_SIZES.length];
        // The percentage of allocations that are blobs.
        final float percentBlobs;
        {
            final long nallocs = nnodes + nleaves;

            for (int i = 0; i < SLOT_SIZES.length; i++) {

                percentages[i] = histogram[i] / nallocs;

            }

            percentBlobs = blobs / nallocs;

        }

        if (percentBlobs > maxPercentBlobs) {

            /*
             * We need to reduce the branching factor for this index in order to
             * bring the majority of the allocations under the blobs threshold
             * (aka the NOMINAL_PAGE_SIZE).
             * 
             * This heuristic simply reduces the branching factor by the
             * percentage that we are over the target maximum percentage of blob
             * allocations in the index.
             */

            final int newM = (int) (m * (1.0 - (percentBlobs - maxPercentBlobs)));

            return newM;

        }

        /*
         * Estimate the best branching factor for this index.
         */

        final double averageNodeBytes = (nodeBytes / (double) nnodes);

        final double averageLeafBytes = (leafBytes / (double) nleaves);

        /*
         * The factor that we reduce the target branching factor below the
         * perfect fit for the average node/leaf in order to decrease the risk
         * that the histogram of allocations will include a significant fraction
         * of blobs. On the WORM (and cluster) blobs are single contiguous
         * allocations. On the RW mode backing stores, blobs are one allocation
         * for the blob header plus at least two allocations for the blob (since
         * a blob is always larger than a single allocation). Thus, when we move
         * up to blobs, we do THREE (3) IOs rather than ONE (1). However, we
         * still want to keep the maximum page size to a reasonable target (8k
         * or 16k) since the RWStore can otherwise wind up with unusable and
         * unrecoverable allocators.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/592">
         * Optimize RWStore allocator sizes</a>
         */
        final double reductionFactor = .80;

        // Estimate based on the average node size.
        final int newM_nodes = (int) (reductionFactor * (m * NOMINAL_PAGE_SIZE) / averageNodeBytes);

        // Estimate based on the average leaf size.
        final int newM_leaves = (int) (reductionFactor * (m * NOMINAL_PAGE_SIZE) / averageLeafBytes);

        // The average of those two estimates.
        final int newM = (newM_nodes + newM_leaves) / 2;

        return newM;

    }

}
