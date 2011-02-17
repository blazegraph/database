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
import com.bigdata.bop.Bind;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
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
		 * The ordered set of {@link IValueExpression}s which are projected out
		 * of the group by operator. The variable references will be resolved
		 * against the in {@link #SELECT} must either: (a) appear the
		 * {@link #GROUP_BY} declaration as simple {@link IVariable} s; or (b)
		 * be declared by {@link #COMPUTE}.
		 */
        String SELECT = GroupByOp.class.getName() + ".select";

//		/**
//		 * The ordered set of {@link IValueExpression}s which are to be
//		 * computed.
//		 * <p>
//		 * The top-level for each element of {@link #COMPUTE} must be either an
//		 * {@link IVariable} or a {@link Bind}. When present, the {@link Bind}
//		 * has the effect of assigning the result of an {@link IValueExpression}
//		 * to an {@link IVariable}. Only {@link IVariable}s declared in the
//		 * input solutions may be referenced in a {@link #COMPUTE}
//		 * {@link IValueExpression}.
//		 * 
//		 * TODO This really needs to be VAR := EXPR. EXPR can only reference the
//		 * source variables or variables declared earlier in the ordered
//		 * collection. If an EXPR references a source variable, then it must
//		 * wrap that source variable with an aggregation operator (SUM, COUNT,
//		 * MIN, MAX, AVG, GROUP_CONCAT, or SAMPLE). Only source variables and
//		 * constants may appear as operands of aggregation operators. [We need a
//		 * BIND() operator for this, which might wind up being the same as a
//		 * LET.]
//		 * 
//		 * TODO Decide how we will handle AVG.
//		 */
//        String COMPUTE = GroupByOp.class.getName() + ".compute";

		/**
		 * The ordered set of or one or more {@link IValueExpression}s defining
		 * the aggregation groups (required). Variables references will be 
		 * resolved against the incoming solutions.
		 */
        String GROUP_BY = GroupByOp.class.getName() + ".groupBy";

		/**
		 * An {@link IConstraint}[] applied to the aggregated solutions
		 * (optional).
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
