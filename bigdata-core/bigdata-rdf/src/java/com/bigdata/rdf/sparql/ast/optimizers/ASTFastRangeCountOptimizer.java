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
 * Created on Sep 14, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.SPOPredicate;

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
 * @see <a href="http://trac.blazegraph.com/ticket/1037" > Rewrite SELECT
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
    public QueryNodeWithBindingSet optimize(
       final AST2BOpContext context, final QueryNodeWithBindingSet input) {

       final IQueryNode queryNode = input.getQueryNode();
       final IBindingSet[] bindingSets = input.getBindingSets();     

       if (context.getAbstractTripleStore().
             getSPORelation().indicesHaveDeleteMarkers()) {
			/**
			 * Disallow for optimization when using delete markers.
			 * <p>
			 * The presence of deleteMarkers means that the fast-range count will
			 * be turned into a key-range scan, which is not desired.
			 * <p>
			 * While AccessPath.rangeCountExact(true) method will do the right
			 * thing even if the index has delete markers (it will convert to a
			 * scan for either delete markers or if there is a FILTER attached
			 * to the index), converting to a scan defeats the purpose of the
			 * ASTFastRangeCountOptimizer. In this case, the cost would have
			 * been the same if we had not rewritten the AST. Hence we do not
			 * rewrite the query.
			 */

          return new QueryNodeWithBindingSet(queryNode, bindingSets);
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

      return new QueryNodeWithBindingSet(queryNode, bindingSets);
    	
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
		 * 
		 * The DISTINCT and REDUCED are optional for triples mode APs and for
		 * quads mode APs where all 4 components of the quad are captured in the
		 * COUNT( expression-list ). In both cases the fast range count will
		 * automatically give us the DISTINCT triples / quads.
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

		/*
		 * Extract the single triple pattern from the WHERE clause.
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

		if (context.getAbstractTripleStore().isQuads()) {
			final DatasetNode dataset = sa.getQueryRoot().getDataset();
			boolean ok = false;
			if (dataset == null || dataset.getNamedGraphs() == null) {
				/*
				 * The dataset is all graphs.
				 */
				ok = true;
			}
			if (sp.getScope() == Scope.DEFAULT_CONTEXTS) {
				final Map<String, Object> scalarValues = functionNode
						.getScalarValues();
				if (scalarValues != null) {
					final Boolean isDistinct = (Boolean) scalarValues
							.get(AggregateBase.Annotations.DISTINCT);
					if (isDistinct != null && isDistinct) {
						/*
						 * We can not use the fast-range-count for a quads-mode
						 * default graph query. If there are multiple graphs in
						 * the default graph query, then we need to take the RDF
						 * merge of those named graphs. This is done by feeding
						 * the quads into a filter that stripes off the context
						 * position and then imposes a DISTINCT-SPO filter. The
						 * result of that DISTINCT-SPO filter are then the
						 * distinct triples (vs distinct quads). The count of
						 * those distinct triples is what is required for
						 * COUNT(DISTINCT) for a quads mode default graph query.
						 * 
						 * TODO We can do this for the case where there is only
						 * a single named graph that is being considered by the
						 * default graph query since that case reduces to the
						 * same as having the graph be a constant.
						 */
						ok = false;
					}
				}
			}
			if (!ok) {
				// Can not rewrite.
				return;
			}
		}
		
		/**
		 * When in history mode, we can't do fast range count with two key-
		 * probes, unless the StatementPatternNode has been marked to read
		 * history.  Without that a scan+filter is necessary.
		 */
		if (context.getAbstractTripleStore().isRDRHistory()) {
		    
		    if (!sp.getQueryHintAsBoolean(QueryHints.HISTORY, false)) {
                // Can not rewrite.
                return;
		    }
		    
		}

		/**
		 * Figure out if this is COUNT(*) or semantically equivalent to
		 * COUNT(*).
		 * 
		 * Note: COUNT(x y z) is semantically equivalent to COUNT(*) if x, y,
		 * and z are the names of the variables in the triple pattern.
		 * 
		 * Note: A simple BPG does not declare the graph variable. The graph
		 * variable is ONLY declared by GRAPH ?g {BPG}. For quads mode APs, the
		 * graph variable needs to be declared (using GRAPH ?g {BPG}) and used
		 * in the COUNT( expression-list ) in order for the rewrite to have the
		 * correct semantics.
		 * 
		 * FIXME We also need to handle named graph vs default graph.
		 * 
		 * Thus any of the following can be converted:
		 * 
		 * - COUNT(*) {GRAPH ?g {?s ?p ?o}}
		 * 
		 * - COUNT(*) {GRAPH ?g {:s ?p ?o}}
		 * 
		 * - COUNT(*) {GRAPH :g {?s ?p ?o}}
		 * 
		 * - COUNT(?s ?p ?p ?g) {GRAPH ?g {?s ?p ?o}}
		 * 
		 * However, in quads mode the following MAY NOT be converted (unless
		 * there is a single named graph) because the hidden graph variable is
		 * not part of the expression-list for COUNT.
		 * 
		 * - COUNT(*) {?s ?p ?o}
		 * 
		 * - COUNT(?s ?p ?o) {?s ?p ?o}
		 * 
		 * In particular, we would get the WRONG answer if we converted the
		 * following in quads mode since the COUNT(DISTINCT ?s ?p ?o) is just
		 * distinct TRIPLES but the AP fast range count would report distinct
		 * QUADS.
		 * 
		 * - COUNT(DISTINCT ?s ?p ?o) {?s ?p ?o} where defaultGraph=ALL
		 * 
		 * TODO Another possibility for this last case is to explicitly compute
		 * the sum of the range counts over the triple pattern for the set of
		 * named or default graphs.
		 */
		boolean isCountStar = false;
		if (functionNode.arity() == 1
				&& (functionNode.get(0) instanceof VarNode)
				&& (((VarNode) functionNode.get(0)).isWildcard())) {
			/*
			 * COUNT(*)
			 */
			isCountStar = true;
		}
		if (!isCountStar
				&& functionNode.arity() == context.getAbstractTripleStore()
						.getSPOKeyArity()) {
			/*
			 * There are as many function arguments as the arity of the KB.
			 * 
			 * Check to see if all variables in the associated triple pattern
			 * are declared in the COUNT( expression-list ).
			 */
			final Set<IVariable<?>> boundVars = sp.getProducedBindings();
			for (int i = 0; i < functionNode.arity(); i++) {
				final ValueExpressionNode arg = (ValueExpressionNode) functionNode
						.get(i);
				if (!(arg instanceof VarNode)) {
					// Not a simple variable.
					break;
				}
				// remove any variable in the COUNT( expression-list )
				boundVars.remove(((VarNode) arg).getValueExpression());
			}
			if (boundVars.isEmpty()) {
				/*
				 * If boundVars is now empty then all variables appearing in the
				 * triple pattern also appear in the COUNT( expression-list ).
				 * So this is effectively equivalent to a COUNT(*) expression.
				 */
				isCountStar = true;
			}
		}

		if (!isCountStar) {
			/*
			 * Neither explicit nor implicit COUNT(*).
			 */
			return;
		}
		
		/**
		 * Rewrite the (sub-)SELECT.
		 */

		final VarNode theVar = assignmentNode.getVarNode();

		// Mark the triple pattern for fast range count.
		markForFastRangeCount(sp, theVar);
		
		// Rewrite the projection as SELECT ?var.
		final ProjectionNode newProjection = new ProjectionNode();
		newProjection.addProjectionVar(theVar);
		queryBase.setProjection(newProjection);
		
	}

   protected void markForFastRangeCount(final StatementPatternNode sp,
                                        final VarNode fastRangeCountVariable)
   {
      // Mark the triple pattern with the FAST-RANGE-COUNT attribute.
      sp.setFastRangeCount(fastRangeCountVariable);

      /*
       * Mark the triple pattern as having an ESTIMATED-CARDINALITY one ONE.
       * 
       * Note: We will compute the COUNT(*) for the triple pattern using two
       * key probes. Therefore we set the estimated cost of computing that
       * cardinality to the minimum.
       */
      sp.setProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, 1L);
   }

}
