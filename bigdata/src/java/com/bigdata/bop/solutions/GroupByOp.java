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
		 * The ordered set of variables declared by {@link #COMPUTE} which are
		 * projected out of the group by operator.
		 */
        String SELECT = GroupByOp.class.getName() + ".select";

		/**
		 * The ordered set of {@link IValueExpression}s which are to be
		 * computed.
		 * 
		 * TODO This really needs to be VAR := EXPR. EXPR can only reference the
		 * source variables or variables declared earlier in the ordered
		 * collection. If an EXPR references a source variable, then it must
		 * wrap that source variable with an aggregation operator (SUM, COUNT,
		 * MIN, MAX, AVG, GROUP_CONCAT, or SAMPLE). Only source variables and
		 * constants may appear as operands of aggregation operators. [We need a
		 * BIND() operator for this, which might wind up being the same as a
		 * LET.]
		 * 
		 * TODO Decide how we will handle AVG.
		 */
        String COMPUTE = GroupByOp.class.getName() + ".compute";

		/**
		 * The ordered set of or one or more variables defining the aggregation
		 * groups (required). The variables named in this collection MUST be
		 * variables declared for the incoming solutions.
		 */
        String GROUP_BY = GroupByOp.class.getName() + ".groupBy";

		/**
		 * An {@link IConstraint}[] applied to the aggregated solutions
		 * (optional). The {@link IConstraint}s MAY NOT include aggregation
		 * operators and may only reference variables declared by
		 * {@link #COMPUTE}.
		 * 
		 * TODO Should be the BEV of an {@link IValueExpression}, which might or
		 * might not be an {@link IConstraint}.
		 */
        String HAVING =  GroupByOp.class.getName() + ".having";
        
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

}
