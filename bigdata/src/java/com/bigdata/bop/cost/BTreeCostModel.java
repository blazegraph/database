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

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.journal.IIndexManager;
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

    /**
     * Return the estimated cost of a range scan of the index.
     * 
     * @param diskCostModel
     *            The cost model for the disk.
     * @param rangeCount
     *            The range count for the scan.
     * @param btree
     *            The index.
     * 
     * @return The estimated cost (milliseconds).
     * 
     * @todo how to get the right view onto the BTree without locking? or raise
     *       the cost model into the {@link IIndexManager}?
     */
    public double rangeScan(final DiskCostModel diskCostModel,
            final int rangeCount, final BTree btree) {

        if (rangeCount == 0)
            return 0d;

        // double height = (Math.log(branchingFactor) / Math.log(entryCount)) -
        // 1;

        final int m = btree.getBranchingFactor();

        final int entryCount = btree.getEntryCount();

        final int height = btree.getHeight();

        // average seek time to a leaf.
        final double averageSeekTime = Math.max(0, (height - 1))
                * diskCostModel.seekTime;

        // the percentage of the leaves which are full.
        // final double leafFillRate = .70d;
        final double leafFillRate = ((double) btree.getUtilization()[1]) / 100;

        /*
         * The expected #of leaves to visit for that range scan.
         * 
         * Note: There is an edge condition when the root leaf is empty
         * (fillRate is zero).
         */
        final double expectedLeafCount = Math.ceil((rangeCount / m)
                * Math.min(1, (1 / leafFillRate)));

        /*
         * Expected total time for the key range scan. Overestimates since
         * ignores cache reuse and OS caching of visited nodes. Ignores transfer
         * costs.
         */
        final double estimatedCost = averageSeekTime * expectedLeafCount;

        return estimatedCost;

    }

}
