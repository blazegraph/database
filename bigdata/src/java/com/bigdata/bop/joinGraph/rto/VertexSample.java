/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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

import com.bigdata.bop.IBindingSet;

/**
 * A sample of a {@link Vertex} (an access path).
 */
public class VertexSample extends SampleBase {

    /**
     * 
     * Note: For a {@link Vertex}, the estimatedCardinality is the fast range
     * count. This will be the same for each sample taken (assuming a read
     * historical view or even a time scale of query which is significantly
     * faster than update).
     * 
     * @param estCard
     *            The estimated cardinality.
     * @param limit
     *            The cutoff limit used to make that cardinality estimate.
     * @param estimateEnum
     *            The type of estimate. For a vertex this should be
     *            {@link EstimateEnum#Exact} iff the sample is the fully
     *            materialized access path for that vertex and otherwise
     *            {@link EstimateEnum#Normal}.
     * @param sample
     *            The sample.
     */
    public VertexSample(final long estCard, final int limit,
            final EstimateEnum estimateEnum, final IBindingSet[] sample) {

        super(estCard, limit, estimateEnum, sample);

        switch (estimateEnum) {
        case Normal:
        case Exact:
            break;
        default:
            throw new IllegalArgumentException();
        }

    }

}
