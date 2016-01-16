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
package com.bigdata.htree;

import com.bigdata.btree.PageStats;

public class HTreePageStats extends PageStats {

    public HTreePageStats() {
    }

    public void visit(final AbstractHTree htree, final AbstractPage node) {

        if (m == 0) {

            // Make a note of the configured address bits.
            m = htree.getAddressBits();

            ntuples = htree.getEntryCount();

            // Note: must be computed dynamically since not a balanced tree.
            // height = tmp.getHeight();

        }

        /*
         * Note: Must be computed dynamically since the HTree is not a balanced
         * tree.
         */

        height = Math.max(((AbstractPage) node).getLevel(), height);

        super.visit(htree, node);

    }

    /**
     * {@inheritDoc}
     * 
     * TODO This method always returns the current value of [addressBits]. It
     * should be modified to compute a target value for [addressBits] based on
     * the same criteria that are used for the B+Tree.
     */
    @Override
    public int getRecommendedBranchingFactor() {

        return m;

    }

}
