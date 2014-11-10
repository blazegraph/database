/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Sep 14, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.List;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.DistinctTermAdvancer;

/**
 * Optimizes <code>SELECT DISTINCT ?property WHERE { ?x ?property ?y . }</code>
 * and similar patterns using an O(N) algorithm, where N is the number of
 * distinct solutions.
 * 
 * TODO We are doing something very similar for <code>GRAPH ?g {}</code>. It
 * would be worth while to look at that code in the light of this optimizer.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
 * @see DistinctTermAdvancer
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ASTDistinctTermScanOptimizer implements IASTOptimizer {

    /**
     * 
     */
    public ASTDistinctTermScanOptimizer() {
    }

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {
    	
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                // Note: works around concurrent modification error.
                final List<NamedSubqueryRoot> list = BOpUtility.toList(
                        namedSubqueries, NamedSubqueryRoot.class);
                
                for (NamedSubqueryRoot namedSubquery : list) {

					// Rewrite the named sub-select
					doSelectQuery(context, sa, namedSubquery);

				}

            }

        }
        
        // rewrite the top-level select
		doSelectQuery(context, sa, (QueryRoot) queryNode);

    	return queryNode;
    	
	}
   
	private void doRecursiveRewrite(final AST2BOpContext context,
			final StaticAnalysis sa,
			final GraphPatternGroup<IGroupMemberNode> group) {

		final int arity = group.arity();

		for (int i = 0; i < arity; i++) {

			final BOp child = (BOp) group.get(i);

			if (child instanceof GraphPatternGroup<?>) {

				// Recursion into groups.
				doRecursiveRewrite(context, sa,
						((GraphPatternGroup<IGroupMemberNode>) child));

			} else if (child instanceof SubqueryRoot) {

				// Recursion into subqueries.
				final SubqueryRoot subqueryRoot = (SubqueryRoot) child;
				doRecursiveRewrite(context, sa, subqueryRoot.getWhereClause());

				// rewrite the sub-select
				doSelectQuery(context, sa, (SubqueryBase) child);

			} else if (child instanceof ServiceNode) {

				// Do not rewrite things inside of a SERVICE node.
				continue;

			}

		}

	}

	/**
	 * Attempt to rewrite the SELECT.
	 * 
	 * @param context
	 * @param sa
	 * @param queryBase
	 */
    private void doSelectQuery(final AST2BOpContext context,
            final StaticAnalysis sa, final QueryBase queryBase) {

        // recursion first.
        doRecursiveRewrite(context, sa, queryBase.getWhereClause());
        
		if (queryBase.getQueryType() != QueryType.SELECT) {
			return;
		}

		/*
		 * Looking for SELECT ?var { triple-or-quads-pattern }
		 * 
		 * where ?var is one of the variables in that triple or quads pattern.
		 */
		final ProjectionNode projection = queryBase.getProjection();

		if (!projection.isDistinct() && !projection.isReduced()) {
			/*
			 * The distinct term scan automatically eliminates duplicates.
			 * Therefore it is only allowable with SELECT DISTINCT or SELECT
			 * REDUCED.
			 */
			return;
		}
		
		if (projection.isEmpty())
			return;

		if (projection.arity() > 1)
			return;

		final AssignmentNode assignmentNode = projection.getExpr(0);

		if (!(assignmentNode.getValueExpressionNode() instanceof VarNode)) {
			/*
			 * The projection needs to be a single, simple variable.
			 */
			return;
		}
		
		final IVariable<?> projectedVar = assignmentNode.getVar();
		
		/**
		 * Looking for a single triple or quad pattern in the WHERE clause.
		 */

		final GraphPatternGroup<IGroupMemberNode> whereClause = queryBase
				.getWhereClause();

		if (whereClause == null || whereClause.arity() != 1) {
			// Not simple triple pattern.
			return;
		}

		if (!(whereClause.get(0) instanceof StatementPatternNode)) {
			// Not simple triple pattern.
			return;
		}

		// The single triple pattern.
		final StatementPatternNode sp = (StatementPatternNode) whereClause
				.get(0);
		
		final Set<IVariable<?>> producedBindings = sp.getProducedBindings();
		
		if(!producedBindings.contains(projectedVar)) {
			/*
			 * The projected variable is not any of the variables used in the
			 * triple pattern.
			 * 
			 * TODO Does this already handle named graph APs?
			 */
			return;
		}
		
		/*
		 * FIXME We need to look at the variables projected into and out of this
		 * SELECT (if any) and those bound by the triple pattern. This rewrite
		 * is only advantageous when a single variable is bound by the triple
		 * pattern is projected out of the query (or perhaps when 2 variables
		 * are bound by a quad pattern and projected out of the query). The
		 * benefit of this rewrite is that we can avoid binding the variables
		 * that are NOT projected out of the query.
		 * 
		 * At the same time, if the SELECT appears as a sub-SELECT and the
		 * evaluation context would have already bound the projected variable(s)
		 * then there is no point using the distinct term scan (unless we lift
		 * it out into a named subquery).
		 * 
		 * TODO We can mark the SELECT as a named-subquery. This will cause it
		 * to be lifted out to run it first. This could be advantageous if there
		 * would otherwise be bindings entering into the distinct term scan.
		 */

//		final VarNode theVar = assignmentNode.getVarNode();
//
//		// Mark the triple pattern with the FAST-RANGE-COUNT attribute.
//		sp.setFastRangeCount(theVar);
//
//		/*
//		 * Mark the triple pattern as having an ESTIMATED-CARDINALITY one ONE.
//		 * 
//		 * Note: We will compute the COUNT(*) for the triple pattern using two
//		 * key probes. Therefore we set the estimated cost of computing that
//		 * cardinality to the minimum.
//		 */
//		sp.setProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, 1L);
//		
//		// Rewrite the projection as SELECT ?var.
//		final ProjectionNode newProjection = new ProjectionNode();
//		newProjection.addProjectionVar(theVar);
//		queryBase.setProjection(newProjection);
		
		// FIXME Finish this optimizer.
		throw new UnsupportedOperationException();
		
	}

}
