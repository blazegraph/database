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

/**
 * Type safe enumeration describes the edge condition (if any) for a cardinality
 * estimate.
 */
public enum EstimateEnum {
    /**
     * An estimate, but not any of the edge conditions.
     */
    Normal(" "),
    /**
     * The cardinality estimate is exact.
     */
    Exact("E"),
    /**
     * The cardinality estimation is a lower bound (the actual cardinality may
     * be higher than the estimated value).
     * <p>
     * Note: The estimated cardinality reported for a {@link #LowerBound} is the
     * sum of the fast range counts for the sampled access paths. See the logic
     * which handles cutoff join sampling for details on this.
     */
    LowerBound("L"),
    /**
     * Flag is set when the cardinality estimate underflowed (false zero (0)).
     */
    Underflow("U"),
    ;

    private EstimateEnum(final String code) {

        this.code = code;

    }

    private final String code;

    public String getCode() {

        return code;

    }

}
