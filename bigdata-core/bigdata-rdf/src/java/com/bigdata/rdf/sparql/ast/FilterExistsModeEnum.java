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
 * Created on Aug 28, 2012
 */
package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.controller.SubqueryOp;

/**
 * Used to specify the query plan for FILTER (NOT) EXISTS. There are two basic
 * plans: vectored sub-plan and subquery with LIMIT ONE. Each plan has its
 * advantages.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/988"> bad performance for FILTER
 *      EXISTS </a>
 */
public enum FilterExistsModeEnum {

    /**
     * This evaluation mode builds a hash index from all source solutions,
     * vectors the solutions from the hash index into the sub-plan, and the does
     * a hash join of the sub-plan with the hash index to determine which
     * solutions pass the filter.
     * <p>
     * This plan solves the FILTER (NOT) EXISTS for multiple source solutions at
     * the same time (vectoring). It is more efficient if the sub-plan requires
     * relatively little work per solution to fully evaluate the sub-plan and
     * there are a large number of solutions flowing into the FILTER (NOT)
     * EXISTS.
     */
    VectoredSubPlan,
    
    /**
     * This evaluation mode routes each source solution (one by one) into a
     * separate {@link SubqueryOp subquery} and imposes a LIMIT ONE.
     * <p>
     * This plan is more efficient if there are many solutions to the FILTER for
     * each source solution, if it is relatively expensive to find all such
     * solutions, and if there are relatively few source solutions. Under these
     * conditions, the FILTER (NOT) EXISTS sub-query is cutoff once it finds the
     * first solution to each source solution and the overhead of submitting
     * multiple sub-queries is modest because there are not that many source
     * solution that need to flow into the FILTER.
     */
    SubQueryLimitOne;
    
}
