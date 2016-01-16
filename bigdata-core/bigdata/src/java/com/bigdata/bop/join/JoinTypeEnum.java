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
/*
 * Created on Dec 8, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.IPredicate;

/**
 * A type safe enumeration of the different flavors of hash index "joins".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum JoinTypeEnum {

    /**
     * A normal join. The output is the combination of the left and right hand
     * solutions. Only solutions which join are output.
     */
    Normal,
    /**
     * An optional join. The output is the combination of the left and right
     * hand solutions. Solutions which join are output, plus any left solutions
     * which did not join. Constraints on the join ARE NOT applied to the
     * "optional" path.
     */
    Optional,
    /**
     * A join where the left solution is output iff there exists at least one
     * right solution which joins with that left solution. For each left
     * solution, that solution is output either zero or one times. In order to
     * enforce this cardinality constraint, the hash join logic winds up
     * populating the "join set" and then outputs the join set once all
     * solutions which join have been identified.
     */
    Exists,
    /**
     * A join where the left solution is output iff there is no right solution
     * which joins with that left solution. This basically an optional join
     * where the solutions which join are not output.
     * <p>
     * Note: This is also used for "MINUS" since the only difference between
     * "NotExists" and "MINUS" deals with the scope of the variables.
     */
    NotExists,
    /**
     * A distinct filter (not a join). Only the distinct left solutions are
     * output. Various annotations pertaining to JOIN processing are ignored
     * when the hash index is used as a DISTINCT filter.
     */
    Filter;

    /**
     * Return <code>true</code> iff this is a DISTINCT SOLUTIONS filter.
     * 
     * @see #Filter
     */
    public boolean isFilter() {
        
        return this == Filter;
        
    }

    /**
     * Return <code>true</code> iff this is a JOIN with OPTIONAL semantics.
     * 
     * @see #Optional
     * @see IPredicate.Annotations#OPTIONAL
     */
    public boolean isOptional() {

        return this == Optional;
        
    }

    /**
     * Return <code>true</code> iff this is a {@link #Normal} join.
     */
    public boolean isNormal() {

        return this == Normal;

    }

}
