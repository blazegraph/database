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
package com.bigdata.bop.solutions;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableFactory;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;

/**
 * Unit tests for {@link MemoryGroupByOp}.
 * 
 * @author thompsonbry
 */
public class TestMemoryGroupByOp extends AbstractAggregationTestCase {
    
	public TestMemoryGroupByOp() {
	}

	public TestMemoryGroupByOp(String name) {
		super(name);
	}

    @Override
    protected GroupByOp newFixture(IValueExpression<?>[] select,
            IValueExpression<?>[] groupBy, IConstraint[] having) {

        final int groupById = 1;

        final IVariableFactory variableFactory = new MockVariableFactory();

        final IGroupByState groupByState = new GroupByState(//
                select, groupBy, having);

        final IGroupByRewriteState groupByRewrite = new GroupByRewriter(
                groupByState) {

            private static final long serialVersionUID = 1L;

            @Override
            public IVariable<?> var() {
                return variableFactory.var();
            }

        };
        
        final GroupByOp query = new MemoryGroupByOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(BOp.Annotations.BOP_ID, groupById),//
                        new NV(BOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        new NV(PipelineOp.Annotations.PIPELINED, false),//
                        new NV(PipelineOp.Annotations.MAX_MEMORY, 0),//
                        new NV(GroupByOp.Annotations.GROUP_BY_STATE, groupByState), //
                        new NV(GroupByOp.Annotations.GROUP_BY_REWRITE, groupByRewrite), //
                }));

        return query;
    }

    @Override
    protected boolean isPipelinedAggregationOp() {
        return false;
    }

}
