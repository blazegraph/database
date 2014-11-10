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

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
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
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUpdateContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Optimizes SELECT COUNT(*) { triple-pattern } using the fast range count
 * mechanisms when that feature would produce exact results for the KB instance.
 * 
 * <h2>Cases handled by this optimizer</h2>
 * 
 * Basic combinations with identical semantics:
 * <pre>SELECT COUNT(DISTINCT *) {?s ?p ?o}</pre>
 * <pre>SELECT COUNT(REDUCED *) {?s ?p ?o}</pre>
 * <pre>SELECT COUNT(*) {?s ?p ?o}</pre>
 * 
 * Combinations using a constrained range-count.
 * <pre>SELECT COUNT(*) {:s ?p ?o}</pre>
 * <pre>SELECT COUNT(*) {?s :p ?o}</pre>
 * <pre>SELECT COUNT(*) {?s ?p :o}</pre>
 * <pre>SELECT COUNT(*) {:s ?p :o}</pre>

 * Combinations using a constrained range-count where the triple pattern is
 * 1-unbound and the COUNT() references the unbound variable.
 * <pre>SELECT COUNT(?s) {?s :p :o}</pre>
 * <pre>SELECT COUNT(?p) {:s ?p :o}</pre>
 * <pre>SELECT COUNT(?o) {:s :p ?o}</pre>

 * Combinations using a constrained range-count with a QUADS mode access path.
 * <pre>SELECT COUNT(*) { GRAPH ?g {?s ?p ?o} }</pre>
 * <pre>SELECT COUNT(*) { GRAPH :g {:s ?p ?o} }</pre>
 * 
 * Combinations using a constrained range-count with a QUADS mode access path
 * where the triple pattern is 1-unbound and the COUNT() references the unbound variable.
 * <pre>SELECT COUNT(?s) { GRAPH :g {?s :p :o} }</pre>
 * <pre>SELECT COUNT(?g) { GRAPH ?g {:s :p :o} }</pre>
 * 
 * Combinations using a sub-select with nothing projected in:
 * <pre>SELECT * { { SELECT COUNT(*) {?s ?p ?o} } }</pre>
 * <pre>SELECT * { { SELECT COUNT(*) {?s ?p ?o} } :s :p :o .}</pre>
 * 
 * Combinations using a sub-select with something projected in:
 * <pre>SELECT * { ?s a :b . { SELECT COUNT(*) {?s ?p ?o} .}</pre>
 *
 * <h2>Correct rejection cases NOT handled by this optimizer</h2>
 *
 * Combinations using DISTINCT/REDUCED and a constrained range-count,
 * explicitly naming the variables, and having variables that are not in
 * the COUNT() aggregate and not projected in are NOT handled here. These
 * are covered by the {@link ASTDistinctTermScanOptimizer} instead:
 * 
 * <pre>SELECT COUNT(?p) {:s ?p ?o}</pre>
 * <pre>SELECT COUNT(DISTINCT ?p) {:s ?p ?o}</pre>
 * <pre>SELECT COUNT(REDUCED ?p) {:s ?p ?o}</pre>
 * 
 * Sub-select that would be handled as a distinct term scan with something
 * projected in.
 * <pre>SELECT * { ?s a :b . { SELECT COUNT(?p) {?s ?p ?o} .}</pre>
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1037" > Rewrite SELECT
 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} as ESTCARD </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ASTFastRangeCountOptimizer implements IASTOptimizer {

    /**
     * 
     */
    public ASTFastRangeCountOptimizer() {
    }

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

		if (context instanceof AST2BOpUpdateContext
				&& TimestampUtility.isReadWriteTx(context
						.getAbstractTripleStore().getTimestamp())) {
			/*
			 * Disallow for SPARQL UPDATE when using full read-write tx.
			 * 
			 * Full read-write transactions use delete markers. However, I
			 * believe that the delete markers are expunged when we merge the
			 * transaction write set into the unisolated indices. In this case,
			 * the fast-range count will remain accurate for any read-only
			 * query. However, it could be only approximate if we are rewriting
			 * the AST during the evaluation of a SPARQL UPDATE against a KB
			 * protected by full read/write transactions. Thus, I am allowing
			 * read-write tx to use this optimization here but we need to
			 * disable this if the evaluation context is for SPARQL UPDATE not
			 * SPARQL query.
			 */
    		return queryNode;
    	}
		
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

//		if (!StaticAnalysis.isAggregate(queryBase)) {
//			return;
//		}

		/*
		 * Looking for COUNT([DISTINCT|REDUCED]? "*")
		 */
		final ProjectionNode projection = queryBase.getProjection();

		if (projection.isEmpty())
			return;

		if (projection.arity() > 1)
			return;
		
		final AssignmentNode assignmentNode = projection.getExpr(0);

		if (!(assignmentNode.getValueExpressionNode() instanceof FunctionNode))
			return;

		final FunctionNode functionNode = (FunctionNode) assignmentNode
				.getValueExpressionNode();

		if (!functionNode.getFunctionURI().equals(FunctionRegistry.COUNT))
			// Not COUNT
			return;

		if (functionNode.arity() != 1
				|| !(functionNode.get(0) instanceof VarNode)
				|| !(((VarNode) functionNode.get(0)).isWildcard())) {
			/*
			 * Not COUNT(*)
			 * 
			 * Note: if a specific variable is given rather than "*" then we can
			 * use a distinct-term-scan instead of a fast-range-count. This
			 * means that both cases can be recognized easily by a single
			 * optimizer, but we still need to translate them differently when
			 * generating the query plan in order to target the appropriate
			 * physical operators.
			 */
			return;
		}

		/**
		 * SELECT COUNT([DISTINCT|REDUCED]? "*")
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

		final VarNode theVar = assignmentNode.getVarNode();

		// Mark the triple pattern with the FAST-RANGE-COUNT attribute.
		sp.setFastRangeCount(theVar);

		/*
		 * Mark the triple pattern as having an ESTIMATED-CARDINALITY one ONE.
		 * 
		 * Note: We will compute the COUNT(*) for the triple pattern using two
		 * key probes. Therefore we set the estimated cost of computing that
		 * cardinality to the minimum.
		 */
		sp.setProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, 1L);
		
		// Rewrite the projection as SELECT ?var.
		final ProjectionNode newProjection = new ProjectionNode();
		newProjection.addProjectionVar(theVar);
		queryBase.setProjection(newProjection);
		
	}

}
