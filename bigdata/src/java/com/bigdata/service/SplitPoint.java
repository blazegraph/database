/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jan 24, 2008
 */

package com.bigdata.service;


/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SplitPoint {

    /**
     * Index of the first key in this split.
     */
    public final int fromIndex;

    /**
     * Index of the first key NOT included in this split.
     */
    public final int toIndex;

    /**
     * The #of keys in this split (toIndex - fromIndex).
     */
    public final int ntuples;

    /**
     * Create a representation of a split point.
     * 
     * @param fromIndex
     *            The index of the first key that will enter that index
     *            partition (inclusive lower bound).
     * @param toIndex
     *            The index of the first key that will NOT enter that index
     *            partition (exclusive upper bound).
     */
    public SplitPoint(int fromIndex, int toIndex) {

        assert fromIndex >= 0;
        assert toIndex >= fromIndex;

        this.fromIndex = fromIndex;

        this.toIndex = toIndex;

        this.ntuples = toIndex - fromIndex;

    }

    public boolean equals(Split o) {

        if (fromIndex != o.fromIndex)
            return false;

        if (toIndex != o.toIndex)
            return false;

        if (ntuples != o.ntuples)
            return false;

        return true;

    }

}
