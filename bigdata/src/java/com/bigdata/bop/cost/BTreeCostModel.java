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

import java.text.NumberFormat;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeUtilizationReport;
import com.bigdata.btree.IBTreeStatistics;
import com.bigdata.btree.IBTreeUtilizationReport;
import com.bigdata.journal.Journal;

/**
 * A cost model for a range scan on a {@link BTree} backed by a {@link Journal}.
 * The on disk representation of the {@link BTree} does not reflect the index
 * order so a range scan on the {@link BTree} is basically turned into one
 * random seek per node or leaf visited.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Add a parameter for the write retention queue? The capacity of the
 *       queue could be turned into an estimate of the #of nodes and leaves
 *       buffered. Alternatively, we have an estimate of the #of distinct nodes
 *       and leaves on the queue in
 *       {@link AbstractBTree#ndistinctOnWriteRetentionQueue}. With that, we
 *       could decide how likely it is that the first N leaves of the
 *       {@link BTree} are in the cache. However, this is all fuzzy since a
 *       focus on one branch of the {@link BTree} could cause nothing but the
 *       root to be in the cache when probing a different branch.
 */
public class BTreeCostModel {

    private final DiskCostModel diskCostModel;

    /**
     * 
     * @param diskCostModel
     *            The cost model for the disk on which the {@link Journal}
     *            backing the {@link BTree} is located.
     */
    public BTreeCostModel(final DiskCostModel diskCostModel) {
        
        if (diskCostModel == null)
            throw new IllegalArgumentException();
        
        this.diskCostModel = diskCostModel;
        
    }

    /**
     * Compute the height of the B+Tree from its entry count and branching
     * factor (this can also be used to find the minimum height at which there
     * could exist a given range count).
     */
    static public int estimateHeight(final int entryCount,
            final int branchingFactor) {

        if (entryCount < branchingFactor)
            return 0;

        final double logm = Math.log(branchingFactor);

        final double logn = Math.log(entryCount);
        
        final double h = (logm / logn) - 1;

        return (int) Math.ceil(h);

    }
    
    /**
     * Return the estimated cost of a range scan of the index.
     * 
     * @param rangeCount
     *            The range count for the scan.
     * @param m
     *            The B+Tree branching factor.
     * @param h
     *            The B+Tree height.
     * @param leafUtilization
     *            The leaf utilization percentage [0:100].
     * 
     * @return The estimated cost (milliseconds).
     */
    public double rangeScan(final long rangeCount, final int m, final int h,
            final int leafUtilization) {

        if (rangeCount == 0)
            return 0d;

        // average seek time to a leaf.
        final double averageSeekTime = Math.max(0, (h - 1))
                * diskCostModel.seekTime;

        // the percentage of the leaves which are full.
        // final double leafFillRate = .70d;
        final double leafFillRate = ((double) leafUtilization) / 100;

        /*
         * The expected #of leaves to visit for that range scan.
         * 
         * Note: There is an edge condition when the root leaf is empty
         * (fillRate is zero).
         */
        final double expectedLeafCount = Math.ceil((((double) rangeCount) / m)
                * Math.min(1, (1 / leafFillRate)));

        /*
         * Expected total time for the key range scan. Overestimates since
         * ignores cache reuse and OS caching of visited nodes. Ignores transfer
         * costs.
         */
        final double estimatedCost = averageSeekTime * expectedLeafCount;

        return estimatedCost;

    }

    /**
     * Prints out some tables based on different disk cost models, branching
     * factors, and range scans. To validate this, you can do a scatter plot of
     * the rangeCount and cost columns and observe the log linear curve of the
     * B+Tree.
     * 
     * @param args
     *            ignored.
     * 
     * @see <a
     *      href="src/resources/architectures/query-cost-model.xls">query-cost-model.xls</a>
     */
    public static void main(String[] args) {

        final DiskCostModel[] diskCostModels = new DiskCostModel[] { DiskCostModel.DEFAULT };

        final int[] branchingFactors = new int[] { 32, 64, 128, 256, 512, 1024 };

        final int[] heights = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        
        final int[] rangeCounts = new int[] { 1, 10, 100, 1000, 2000, 5000, 10000, 2000, 50000, 100000 };

        final int leafUtilization = 65; // average percent "full" per leaf.
        
        System.out.println("seekTime\txferRate\tleafUtil\tm\theight\trangeCount\tcost(ms)");
        
        final NumberFormat millisFormat = NumberFormat.getIntegerInstance();
        millisFormat.setGroupingUsed(true);

        final NumberFormat percentFormat = NumberFormat.getPercentInstance();
        percentFormat.setMinimumFractionDigits(0);

        final StringBuilder sb = new StringBuilder();

        for (DiskCostModel diskCostModel : diskCostModels) {

            final BTreeCostModel btreeCostModel = new BTreeCostModel(
                    diskCostModel);

            for (int m : branchingFactors) {

                for (int h : heights) {

                    for (int rangeCount : rangeCounts) {

                        final int estimatedHeight = estimateHeight(rangeCount,
                                m);

                        if (estimatedHeight > h) {
                            /*
                             * Skip range counts which are too large for the
                             * current B+Tree height.
                             */
                            break;
                        }
                        
                        final double cost = btreeCostModel.rangeScan(
                                rangeCount, m, h, leafUtilization);

                        sb.setLength(0); // reset.
                        sb.append(millisFormat.format(diskCostModel.seekTime));
                        sb.append('\t');
                        sb.append(millisFormat
                                .format(diskCostModel.transferRate));
                        sb.append('\t');
                        sb.append(percentFormat.format(leafUtilization / 100d));
                        sb.append('\t');
                        sb.append(m);
                        sb.append('\t');
                        sb.append(h);
                        sb.append('\t');
                        sb.append(rangeCount);
                        sb.append('\t');
                        sb.append(millisFormat.format(cost));
                        System.out.println(sb);

                    }

                }

            }
            
        }

    }

    private static class MockBTreeStatistics implements IBTreeStatistics {

        private final int m;

        private final int entryCount;

        private final int height;

        private final int leafCount;

        private final int nodeCount;

        private final IBTreeUtilizationReport utilReport;

        public MockBTreeStatistics(final int m, final int entryCount,
                final int height, final int leafCount, final int nodeCount) {
            this.m = m;
            this.entryCount = entryCount;
            this.height = height;
            this.leafCount = leafCount;
            this.nodeCount = nodeCount;
            this.utilReport = new BTreeUtilizationReport(this);
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

    }

}
