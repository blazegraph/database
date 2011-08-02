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

import com.bigdata.bop.BOp;
import com.bigdata.bop.IConstraint;
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
         * The ordered list of {@link IValueExpression}s to be projected out of
         * the aggregation operator (required). The {@link IValueExpression}s
         * will be computed against the aggregated solution sets (one solution
         * set per group).
         */
        String SELECT = GroupByOp.class.getName() + ".select";

        /**
         * The ordered set of or one or more {@link IValueExpression}s defining
         * the aggregation groups (required). Variables references will be
         * resolved against the incoming solutions.
         */
        String GROUP_BY = GroupByOp.class.getName() + ".groupBy";

        /**
         * An {@link IConstraint}[] applied to the aggregated solutions
         * (optional).
         */
        String HAVING = GroupByOp.class.getName() + ".having";
        
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
    
}
