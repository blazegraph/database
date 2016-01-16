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
 * Created on Jul 29, 2011
 */

package com.bigdata.bop.solutions;

import java.util.LinkedHashMap;

import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.IAggregate;

/**
 * A rewrite of a {@link GroupByState} in which all {@link IAggregate}
 * expressions have been lifted out in order to (a) minimize redundancy when
 * computing the aggregates; and (b) simplify the logic required to compute the
 * {@link IAggregate}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IGroupByRewriteState {

    /**
     * The set of all unique {@link IAggregate} expressions paired with
     * anonymous variables. Any internal {@link IAggregate} have been lifted out
     * and will appear before any {@link IAggregate}s which use them. The
     * {@link IAggregate} MAY have a complex internal {@link IValueExpression},
     * but it WILL NOT have a nested {@link IAggregate}.
     */
    public LinkedHashMap<IAggregate<?>, IVariable<?>> getAggExpr();

    /**
     * A modified version of the original HAVING expression which has the same
     * semantics (and <code>null</code> iff the original was <code>null</code>
     * or empty). However, the modified select expressions DO NOT contain any
     * {@link IAggregate} functions. All {@link IAggregate} functions have been
     * lifted out into {@link #aggExp}.
     */
    public IConstraint[] getHaving2();

    /**
     * A modified version of the original SELECT expression which has the same
     * semantics. However, the modified select expressions DO NOT contain any
     * {@link IAggregate} functions. All {@link IAggregate} functions have been
     * lifted out into {@link #aggExp}.
     */
    public IValueExpression<?>[] getSelect2();

}
