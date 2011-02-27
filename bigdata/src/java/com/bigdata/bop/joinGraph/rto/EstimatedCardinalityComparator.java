/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
package com.bigdata.bop.joinGraph.rto;

import java.util.Comparator;


/**
 * Places edges into order by ascending estimated cardinality. Edges which
 * are not weighted are ordered to the end.
 * 
 * TODO unit tests, including with unweighted edges.
 */
class EstimatedCardinalityComparator implements Comparator<Path> {

    public static final transient Comparator<Path> INSTANCE = new EstimatedCardinalityComparator();

    // @Override
    public int compare(final Path o1, final Path o2) {
        if (o1.edgeSample == null && o2.edgeSample == null) {
            // Neither edge is weighted.
            return 0;
        }
        if (o1.edgeSample == null) {
            // o1 is not weighted, but o2 is. sort o1 to the end.
            return 1;
        }
        if (o2.edgeSample == null) {
            // o2 is not weighted. sort o2 to the end.
            return -1;
        }
        final long id1 = o1.edgeSample.estCard;
        final long id2 = o2.edgeSample.estCard;
        if (id1 < id2)
            return -1;
        if (id1 > id2)
            return 1;
        return 0;
    }

}
