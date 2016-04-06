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

/**
 * https://jira.blazegraph.com/browse/BLZG-1862
 * and
 * https://jira.blazegraph.com/browse/BLZG-856
 * take this optimizer into an infinite loop.
 * 
 * BLZG-856
SELECT *
WITH
{
SELECT * { 
  INCLUDE %sub_record
}} AS %foo
{
  INCLUDE %foo
}
 *  and
 *  BLZG-1862
select *
{ select ?product
  { INCLUDE %solutionSet1 } 
  OFFSET 1 LIMIT 1
}

BLZG-1862 is:

from
WITH {

  QueryType: SELECT

  SELECT ( VarNode(p) AS VarNode(p) )

    JoinGroupNode {

      INCLUDE %sx

    }

  slice(offset=1,limit=1)

} AS -subSelect-1

QueryType: SELECT

includeInferred=true

SELECT ( VarNode(p) AS VarNode(p) )

  JoinGroupNode {

    INCLUDE -subSelect-1

  }
  
we should get:


WITH {

  QueryType: SELECT

  SELECT ( VarNode(p) AS VarNode(p) )

    JoinGroupNode {

      INCLUDE %sx

    }

  slice(offset=1,limit=1)

} AS -subSelect-1 JOIN ON () DEPENDS ON (%sx)

QueryType: SELECT

includeInferred=true

SELECT ( VarNode(p) AS VarNode(p) )

  JoinGroupNode {

    INCLUDE -subSelect-1 JOIN ON ()

  }
 */

package com.bigdata.rdf.sparql.ast.optimizers;


import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SolutionSetStats;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

public class TestASTNamedSubqueryOptimizer2 extends AbstractOptimizerTestCaseWithUtilityMethods {

	/**
	 * Modify the inherited Helper class to have a named solution set.
	 * @author jeremycarroll
	 *
	 */
	private class Helper extends AbstractOptimizerTestCase.Helper {
		IBindingSet bs = new ListBindingSet();
		{ bs.set(toValueExpression(varNode(p)), toValueExpression((ConstantNode) constantNode(a))); }
		@SuppressWarnings("deprecation")
		SolutionSetStats sss = new SolutionSetStats(new IBindingSet[]{bs, bs});

		AST2BOpContext getAST2BOpContext(QueryRoot given) {
			return new AST2BOpContext(new ASTContainer(given), store) {
						@Override
						public ISolutionSetStats getSolutionSetStats(final String localName) {
							if ("solutionSet".equals(localName)) {
								return sss;
							}
							return super.getSolutionSetStats(localName);
						}
					};
		}
	}
	@Override
	IASTOptimizer newOptimizer() {
		return new ASTNamedSubqueryOptimizer();
	}
	/**
	 * See https://jira.blazegraph.com/browse/BLZG-856
	 */
    public void testNamedSolutionSetInsideNamedSubQuery() {
    	new Helper(){{
			given = select( 
			                 varNodes(p),
			                 namedSubQuery("foo",varNode(p),
			                		 where(joinGroupNode(namedSubQueryInclude("solutionSet")))),
			                 where(joinGroupNode(namedSubQueryInclude("foo")))
			              );
			expected = select( 
			        varNodes(p),
			        namedSubQuery("foo",varNode(p),
			       		 where(joinGroupNode(namedSubQueryInclude("solutionSet"))),
			    		 joinOn(varNodes()), dependsOn("solutionSet")),
			        where(joinGroupNode(namedSubQueryInclude("foo", joinOn(varNodes()))))
			     );
  	    }}.test();
    }
	/**
	 * See https://jira.blazegraph.com/browse/BLZG-1862
	 */
    public void testNamedSolutionSetLimit() {
    	new Helper(){{
			given = select( 
			                 varNodes(p),
			                 namedSubQuery("subQuery",varNode(p),
			                		 where(joinGroupNode(namedSubQueryInclude("solutionSet"))),
			                		 slice(1,1)),
			                 where(joinGroupNode(namedSubQueryInclude("subQuery")))
			              );
			expected = select( 
			        varNodes(p),
			        namedSubQuery("subQuery",varNode(p),
			       		 where(joinGroupNode(namedSubQueryInclude("solutionSet"))),
			    		 slice(1,1), joinOn(varNodes()), dependsOn("solutionSet")),
			        where(joinGroupNode(namedSubQueryInclude("subQuery", joinOn(varNodes()))))
			     );
  	    }}.test();
    }

}
