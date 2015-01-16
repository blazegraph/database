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
 * Created on Jan 9, 2015
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;


/**
 * Optimizes
 * <code>
 	SELECT (COUNT(*) as ?count) ?z WHERE {  ?x rdf:type ?z  } GROUP BY ?z
 * </code>
 * and similar patterns using an O(N) algorithm, where N is the number of
 * distinct solutions.
 * <p>
 * The optimizer adds annotations that allow to transform the query into
 * a combination of distinct term scan pattern (to efficiently compute the
 * distinct values for the group variable) and fast range count pattern
 * to efficiently calculate the COUNT, without materialization of the variables
 * on which the COUNT operation is performed. Basically, this optimizer combines
 * the optimization strategies performed in the 
 * {@link ASTDistinctTermScanOptimizer} (which operates over the DISTINCT
 * modifier rather than a GROUP BY clause) and the
 * {@link ASTRangeCountOptimizer} for efficient computation of COUNT in
 * non GROUP BY queries.
 *
 * @see <a href="http://trac.bigdata.com/ticket/1059">
 		GROUP BY optimization using distinct-term-scan and fast-range-count</a>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class ASTCombinedFastRangeCountDistinctTermScanOptimizer implements IASTOptimizer {

    public ASTCombinedFastRangeCountDistinctTermScanOptimizer() {
    }
    
    @Override
	public IQueryNode optimize(final AST2BOpContext context,
			final IQueryNode queryNode, final IBindingSet[] bindingSets) {

		final QueryRoot queryRoot = (QueryRoot) queryNode;

		final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);

		final DatasetNode dataset = queryRoot.getDataset();

		if (context.getAbstractTripleStore().isQuads()) {
			boolean ok = false;
			if (dataset == null || dataset.getNamedGraphs() == null) {
				/*
				 * The dataset is all graphs.
				 */
				ok = true;
			}

			if (!ok) {
				return queryNode;
			}
		}
        
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
					doSelectQuery(context, sa, (QueryRoot) queryNode,
							namedSubquery);

				}

            }

        }
        
		// rewrite the top-level select
		doSelectQuery(context, sa, (QueryRoot) queryNode, (QueryBase) queryNode);

		return queryNode;

	}
    
	/**
	 * Attempt to rewrite the SELECT.
	 * 
	 * @param context
	 * @param sa
	 * @param queryRoot
	 *            The top-level of the query.
	 * @param queryBase
	 *            Either a top-level query or a sub-query.
	 */
	private void doSelectQuery(final AST2BOpContext context,
			final StaticAnalysis sa, final QueryRoot queryRoot,
			final QueryBase queryBase) {
		
		/**
		 *  The prerequisites for the optimizer, which we check in the
		 *  following, are as follows:
		 *  
		 *  (C1) Query must be a SELECT query
		 *  (C2) Single triple pattern in body
		 *  (C3) Single GROUP BY variable that is part of the triple pattern
		 *  (C4) Projection for the GROUP BY variable plus a COUNT operation,
		 *      which operates over * or any other variable in the triple
		 *      pattern (the COUNT must not be DISTINCT)
		 */
		ProjectionNode projectionNode = null;
		Integer indexOfVarNode = null;	// index of VarNode in projectionNode
		Integer indexOfCountNode = null;// index of COUNT node in projectionNode
		VarNode countNodeVar = null; 	// VarNode bound through COUNT operation
		GraphPatternGroup<IGroupMemberNode> graphPattern; // surrounding gp
		StatementPatternNode stmtPattern = null; // the inner statement pattern
		VarNode groupingVar = null;	// the variable which is grouped
		
		{
			GroupByNode groupByNode = queryBase.getGroupBy();
			if (groupByNode==null || groupByNode.arity()!=1 || 
				!(groupByNode.get(0) instanceof AssignmentNode)) {
				return;
			}			
			AssignmentNode assignmentNodeInGroupBy =
				(AssignmentNode) groupByNode.get(0);
			if (assignmentNodeInGroupBy.arity()!=2 ||
				!(assignmentNodeInGroupBy.get(0) instanceof VarNode) ||
				!(assignmentNodeInGroupBy.get(1) instanceof VarNode)) {
				return; // something's wrong here
			}
			
			groupingVar = (VarNode) assignmentNodeInGroupBy.get(1);
			VarNode groupingVarRenamed = (VarNode) assignmentNodeInGroupBy.get(0); 			
			
			
			// Check for condition (C1)
			QueryType queryType = queryBase.getQueryType();
			if (!QueryType.SELECT.equals(queryType)) {
				return; // optimization not applicable
			}
		
			// Check for condition (C2)
			graphPattern = queryBase.getGraphPattern();
			if (graphPattern.args().size()!=1) {
				return;
			}
			
			BOp potentialStmtPattern = graphPattern.get(0);
			if (!(potentialStmtPattern instanceof StatementPatternNode)) {
				return;
			}
			
			stmtPattern = 
				(StatementPatternNode) potentialStmtPattern;
			Set<VarNode> varNodesInStmtPattern = new HashSet<VarNode>();
			VarNode graphVarNode = null;
			for (int i=0; i<stmtPattern.arity(); i++) {
				BOp arg = stmtPattern.get(i);
				if (arg instanceof VarNode) {
					varNodesInStmtPattern.add((VarNode)arg);
					if (i==3) {
						graphVarNode = (VarNode)arg;
					}
				}
			}
			
			// Check for condition (C3)
			if (!varNodesInStmtPattern.contains(groupingVar)) {
				return; // illegal grouping, optimization not applicable
			}
			
			// Check for condition (C4)
			projectionNode = queryBase.getProjection();
			if (projectionNode.size()!=2) {
				return;
			}

			indexOfVarNode = null;
			indexOfCountNode = null;
			for (int i=0; i<2; i++) {
				AssignmentNode curNode = projectionNode.getExpr(i);
				
				IValueExpressionNode valNode = curNode.getValueExpressionNode();
				
				if (valNode instanceof FunctionNode && indexOfCountNode==null) {
					final FunctionNode fNode = 
							(FunctionNode) curNode.getValueExpressionNode();
					
					if (!fNode.getFunctionURI().equals(FunctionRegistry.COUNT))
						return; // NOT COUNT
					
					// check for problematic COUNT(DISTINCT ...)) pattern
					Map<String,Object> scalarVals = fNode.getScalarValues();
					Object isDistinct =
						scalarVals.get(AggregateBase.Annotations.DISTINCT);
					if (isDistinct!=null && isDistinct instanceof Boolean
						&& (Boolean)isDistinct) {
						return; // COUNT (DISTINCT ...)	 cannot be optimized			
					}

					if (fNode.args().size()!=1) {
						return;
					}
					
					BOp inner = fNode.args().get(0);
					if (!(inner instanceof VarNode)) {
						return;
					}
					VarNode innerVarNode = (VarNode)inner;
	
					// the count operation must be performed on a variable
					// distinct from the group variable and distinct from the
					// named graph variable, if any (the latter is not
					// necessarily bound); alternatively, a COUNT(*) is also ok
					boolean countOnUngroupedStmtVariable = 
						varNodesInStmtPattern.contains(innerVarNode) &&
						!innerVarNode.equals(graphVarNode) &&
						!innerVarNode.equals(groupingVarRenamed);
					boolean isWildcard = innerVarNode.isWildcard();
					
					if (!(countOnUngroupedStmtVariable || isWildcard)) {
						return; // optimization in overall not applicable
					}

					if (!(curNode.get(0) instanceof VarNode)) {
						return; // first exp of assignment node is assigned var
					}
					
					countNodeVar = (VarNode)curNode.get(0);
					indexOfCountNode=i;
					
				} else if (valNode instanceof VarNode && indexOfVarNode==null) {
					
					VarNode valNodeAsVarNode = (VarNode)valNode;
					if (!valNodeAsVarNode.equals(groupingVarRenamed)) {
						return; // optimization not applicable
					}
					indexOfVarNode=i;
					
				} else {
					return; // invalid
				}
			}
		}
		
		
		/**
		 * Once we reach this point, we're sure that the optimization is
		 *  applicable, we now rewrite the query plan accordingly; the following
		 *  needs to be done:
		 *  
		 *  (O1) Wrap the existing statement pattern into a SELECT DISTINCT
		 *       subquery, which could (potentially) be optimized to a distinct
		 *       term scan by the #{ASTDistinctTermScanOptimzer}
		 *  (O2) Duplicate the statement pattern and append a fast range
		 *       count annotation, binding the count var of the SELECT clause
		 *  (O3) Transform the SELECT clause: the COUNT expression is replaced
		 *       by a projection for the variable introduced in O2
		 *  (O4) Eliminate the group by clause
		 */
		{
			// apply optimization step (O1)
			SubqueryRoot selectDistinct = new SubqueryRoot(QueryType.SELECT);
			ProjectionNode projection = new ProjectionNode();
			AssignmentNode assignemntNode =
				new AssignmentNode(groupingVar, groupingVar);
			projection.addArg(assignemntNode);
			projection.setDistinct(true);
			selectDistinct.setProjection(projection);
			JoinGroupNode join = new JoinGroupNode();
			join.addArg(stmtPattern);
			selectDistinct.setWhereClause(join);
			graphPattern.setArg(0, selectDistinct);
			
			// apply optimization step (O2)
			StatementPatternNode stmtPatternClone = 
					new StatementPatternNode(stmtPattern);
			stmtPatternClone.setFastRangeCount(countNodeVar);
			graphPattern.addArg(stmtPatternClone);
			
			// apply optimization step (O3)
			projectionNode.setArg(
					indexOfCountNode,
					new AssignmentNode(
							new VarNode(countNodeVar),
							new VarNode(countNodeVar)));
			
			// apply optimization step (O4)
			queryBase.setGroupBy(null);
		}
	}	
}
