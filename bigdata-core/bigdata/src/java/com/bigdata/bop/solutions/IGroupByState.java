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
 * Created on Jul 27, 2011
 */

package com.bigdata.bop.solutions;

import java.util.LinkedHashSet;

import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.IAggregate;

/**
 * The state associated with a validated aggregation operator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IGroupByState {

    /**
     * The ordered array of value expressions which define the basis for
     * aggregating solutions into groups.
     */
    public IValueExpression<?>[] getGroupByClause();

    /**
     * Top-level variables in the GROUP_BY clause in the order in which they
     * were declared. Computed value expressions which are not bound on a
     * variable explicitly are NOT reported here.
     */
    public LinkedHashSet<IVariable<?>> getGroupByVars();
    
    /**
     * The value expressions to be projected out of the SELECT clause.
     */
    public IValueExpression<?>[] getSelectClause();

    /**
     * Top-level variables in the SELECT clause in the order in which they were
     * declared. 
     */
    public LinkedHashSet<IVariable<?>> getSelectVars();
    
    /**
     * Optional constraints applied to the aggregated solutions.
     */
    public IConstraint[] getHavingClause();

    /**
     * Variables for which we will need column projections. For example, given
     * <code>SELECT SUM(DISTINCT ?x), SUM(?y)</code>, this would report
     * <code>{?x,?y}</code>. The variables will be reported in the order they
     * were encountered during a left-to-right scan of the SELECT expressions
     * followed by a left-to-right scan of the HAVING constraints.
     */
    public LinkedHashSet<IVariable<?>> getColumnVars();

//    /**
//     * Variables for which we will need column projections of the distinct
//     * values. For example, given <code>SELECT SUM(DISTINCT ?x), SUM(?y)</code>,
//     * this would report <code>{?x}</code>. The variables will be reported in
//     * the order they were encountered during a left-to-right scan of the SELECT
//     * expressions followed by a left-to-right scan of the HAVING constraints.
//     */
//    public LinkedHashSet<IVariable<?>> getDistinctColumnVars();

    /**
     * <code>true</code> iff any aggregate functions will be applied to the
     * DISTINCT values arising from their inner value expression in either the
     * SELECT or HAVING clause. When <code>false</code> certain optimizations
     * are possible.
     */
    public boolean isAnyDistinct();

    /**
     * <code>true</code> iff any aggregate expression uses a reference to
     * another aggregate expression in the select clause. When
     * <code>false</code> certain optimizations are possible (parallel
     * evaluation of the aggregates across the column projections).
     */
    public boolean isSelectDependency();

    /**
     * <code>true</code> if any aggregate expression nests another aggregate
     * expression. For example:
     * 
     * <pre>
     * SUM(?x+AVG(?x))
     * </pre>
     * 
     * is a nested aggregation expression but:
     * 
     * <pre>
     * SUM(?x+1)
     * </pre>
     * 
     * and
     * 
     * <pre>
     * 1+SUM(?x)+AVG(?x)
     * </pre>
     * 
     * are not nested aggregation expressions. When <code>false</code> certain
     * optimizations may be possible.
     */
    public boolean isNestedAggregates();
    
    /**
     * <code>true</code> if none of the value expressions in the optional HAVING
     * clause use {@link IAggregate} functions. When <code>true</code> certain
     * optimizations are possible.
     * <p>
     * For example, the following query does not use an {@link IAggregate}
     * function in the HAVING clause. Instead, it references an aggregate
     * defined in the SELECT clause.
     * 
     * <pre>
     * SELECT SUM(?y) as ?x
     * GROUP BY ?z
     * HAVING ?x > 10
     * </pre>
     * 
     * As a degenerate case, this is also <code>true</code> when there is no
     * HAVING clause.
     */
    public boolean isSimpleHaving();
    
}
