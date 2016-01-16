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
 * Created on Sep 4, 2010
 */

package com.bigdata.bop.solutions;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.aggregate.IAggregate;

/**
 * Base class for operators which perform aggregation operations on binding
 * sets.
 * 
 * <h2>Aggregation semantics and errors.</h2>
 * 
 * Per the DAWG:
 * <ul>
 * The result of an Aggregate which returns an error, is an error, but the
 * SELECT expression result of projecting an error is unbound. (Hence, the
 * variable is not projected for which the error occurred.)</li>
 * <li>
 * An aggregation of an empty source solution set produces an empty output
 * solution. <br/>
 * See http://www.openrdf.org/issues/browse/SES-884 (Aggregation with an
 * solution set as input should produce an empty solution as output).
 * 
 * <pre>
 * > In response to 
 * > http://lists.w3.org/Archives/Public/public-rdf-dawg-comments/2011Sep/0000.html
 * > 
 * > > What is the output of this query if no solutions satisify the WHERE clause?
 * > > An empty solution set or a solution set with a single solution having 
 * > > a binding of 0^^xsd:integer?
 * > >
 * > > PREFIX : <http://example/>
 * > > SELECT (SUM(?lprice) AS ?totalPrice)
 * > > WHERE {
 * > >   ?book :price ?lprice .
 * > > }
 * > > GROUP BY ?book
 * > 
 * > An empty solution set.
 * </pre>
 * 
 * </li>
 * </ul>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: SortOp.java 3665 2010-09-28 16:53:22Z thompsonbry $
 */
abstract public class GroupByOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {
        
        /**
         * This required annotation describes the {@link IGroupByState} for the
         * aggregation operator, including the SELECT value expressions to be
         * projected, the optional GROUP BY value expressions, and the optional
         * HAVING value expressions.
         * 
         * @see IGroupByState
         */
        String GROUP_BY_STATE = GroupByOp.class.getName() + ".groupByState";

        /**
         * This required annotation provides the {@link IGroupByRewriteState},
         * which is a rewrite of the {@link GroupByState} that allows for
         * certain optimizations. This data is a required annotation because the
         * rewrite includes anonymous variables and the identity of those
         * anonymous variables MUST be stable across each invocation of the
         * operator. This requirement exists both for pipelined aggregation
         * operators running on a single node and for distributed aggregation
         * operators running on a cluster.
         * 
         * @see IGroupByRewriteState
         */
        String GROUP_BY_REWRITE = GroupByOp.class.getName() + ".groupByRewrite";
        
    }

    /**
     * @param op
     */
    public GroupByOp(final GroupByOp op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public GroupByOp(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Return <code>true</code> iff this operator supports pipelined aggregation
     * <p>
     * Note: Pipelined aggregation is only possible when (a) DISTINCT is not
     * used in the {@link IAggregate}s; and (b) {@link IAggregate}s are not
     * nested within other {@link IAggregate}s. An implementation which returns
     * <code>true</code> for this method MUST NOT be used for aggregation
     * operations which would violate either of these constraints.
     */
    abstract public boolean isPipelinedAggregationOp();

    /**
     * @see Annotations#GROUP_BY_STATE
     */
    public IGroupByState getGroupByState() {

        return (IGroupByState) getRequiredProperty(Annotations.GROUP_BY_STATE);

    }

    /**
     * @see Annotations#GROUP_BY_REWRITE
     */
    public IGroupByRewriteState getGroupByRewrite() {

        return (IGroupByRewriteState) getRequiredProperty(Annotations.GROUP_BY_REWRITE);

    }

}
