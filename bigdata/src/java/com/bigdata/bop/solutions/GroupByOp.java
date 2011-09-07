/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 4, 2010
 */

package com.bigdata.bop.solutions;

import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.aggregate.IAggregate;

/**
 * Base class for operators which perform aggregation operations on binding
 * sets.
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
     * Logs the type error.
     * 
     * @param log
     * @param t
     * @param expr
     */
    static protected void handleTypeError(final Logger log, final Throwable t,
            final IValueExpression<?> expr) {

        if (log.isInfoEnabled())
            log.info("Will not bind solution for aggregate due to error: expr="
                    + expr + ", cause=" + t);

    }

}
