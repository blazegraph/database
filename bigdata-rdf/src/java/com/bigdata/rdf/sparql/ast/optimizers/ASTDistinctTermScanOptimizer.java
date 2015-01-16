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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
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
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IKeyOrder;

/**
 * Optimizes
 * <code>SELECT (DISTINCT|REDUCED) ?property WHERE { ?x ?property ?y . }</code>
 * and similar patterns using an O(N) algorithm, where N is the number of
 * distinct solutions.
 * <p>
 * The main advantage here is to turn an access path that is fully unbound into
 * a distinct-term scan. If the access path would be evaluated as-bound with at
 * least one variable bound, then the distinct term scan might not have any
 * advantage over the pipeline join (and the semantics of DISTINCT would be
 * violated with multiple as-bound evaluations of the distinct-term-scan without
 * a hash index to impose the DISTINCT constraint).
 * 
 * TODO We are doing something very similar for <code>GRAPH ?g {}</code>. It
 * would be worth while to look at that code in the light of this optimizer.
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1035" > DISTINCT PREDICATEs
 *      query is slow </a>
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

	private void doRecursiveRewrite(final AST2BOpContext context,
			final StaticAnalysis sa, final QueryRoot queryRoot,
			final GraphPatternGroup<IGroupMemberNode> group) {

		final int arity = group.arity();

		for (int i = 0; i < arity; i++) {

			final BOp child = (BOp) group.get(i);

			if (child instanceof GraphPatternGroup<?>) {

				// Recursion into groups.
				doRecursiveRewrite(context, sa, queryRoot,
						((GraphPatternGroup<IGroupMemberNode>) child));

			} else if (child instanceof SubqueryRoot) {

				// Recursion into subqueries.
				final SubqueryRoot subqueryRoot = (SubqueryRoot) child;
				doRecursiveRewrite(context, sa, queryRoot,
						subqueryRoot.getWhereClause());

				// rewrite the sub-select
				doSelectQuery(context, sa, queryRoot, (SubqueryBase) child);

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
	 * @param queryRoot
	 *            The top-level of the query.
	 * @param queryBase
	 *            Either a top-level query or a sub-query.
	 */
	private void doSelectQuery(final AST2BOpContext context,
			final StaticAnalysis sa, final QueryRoot queryRoot,
			final QueryBase queryBase) {

		// recursion first.
		doRecursiveRewrite(context, sa, queryRoot, queryBase.getWhereClause());

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

		final GraphPatternGroup<IGroupMemberNode> whereClause = 
				queryBase.getWhereClause();

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
		
		IKeyOrder<ISPO> keyOrder = 
			getApplicableKeyOrderIfExists(sp, projectedVar, context);
		if (keyOrder==null) {
			return;
		}
		
		/*
		 * Make sure that there are no correlated variables in the SP.
		 */
		{
			final Set<VarNode> vars = new LinkedHashSet<VarNode>();

			for (VarNode varNode : BOpUtility.toList(sp, VarNode.class)) {

				if (!vars.add(varNode)) {

					// This variable appears more than once.
					return;

				}

			}
			
		}
		
		final Set<IVariable<?>> producedBindings = sp.getProducedBindings();

		if (!producedBindings.contains(projectedVar)) {
			/*
			 * The projected variable is not any of the variables used in the
			 * triple pattern.
			 * 
			 * Note: This rewrite is only advantageous when a single variable is
			 * bound by the triple pattern is projected out of the query (or
			 * perhaps when 2 variables are bound by a quad pattern and
			 * projected out of the query). The benefit of this rewrite is that
			 * we can avoid binding the variables that are NOT projected out of
			 * the query.
			 * 
			 * TODO Does this already handle named graph APs?
			 */
			return;
		}

		if (queryBase instanceof SubqueryRoot) {

			/*
			 * The pattern is detected is a sub-select.
			 * 
			 * Mark the SELECT as "run-once". This will cause it to be lifted
			 * out as a named subquery. This is how we enforce bottom-up
			 * evaluation. In this case it is REQUIRED since the semantics of
			 * DISTINCT / REDUCED would not be enforced across multiple as-bound
			 * invocations of the rewritten sub-SELECT.
			 * 
			 * FIXME Make sure that we have a correctness test for the case of
			 * an embedded sub-select where the DISTINCT semantics would
			 * otherwise break. (We are REQUIRED to use bottom-up evaluation in
			 * order to have the semantics of DISTINCT/REDUCED across the SELECT
			 * if this clause is appearing as a sub-SELECT. Otherwise the
			 * sub-SELECT could be evaluated for multiple source bindings
			 * leading to multiple applications of the distinct-term-scan and
			 * that would break the DISTINCT/REDUCED semantics of the operator.)
			 */
			((SubqueryRoot) queryBase).setRunOnce(true/* runOnce */);
			
		}

		/*
		 * Disable DISTINCT/REDUCED. The distinct-term-scan will automatically
		 * enforce this.
		 */
		projection.setDistinct(false);
		projection.setReduced(false);

		/*
		 * Setup the distinct-term-scan annotation with the variables that will
		 * be projected out of the SELECT.
		 */
		final VarNode distinctTermScanVar = new VarNode(projectedVar.getName());
		sp.setDistinctTermScanVar(distinctTermScanVar);
		sp.setQueryHint(IPredicate.Annotations.KEY_ORDER, keyOrder.toString());

		/**
		 * Change the estimated cardinality.
		 * 
		 * The new cardinality is:
		 * 
		 * <pre>
		 * newCard = oldCard * 1.0 / arity(context,sp)
		 * </pre>
		 * 
		 * where arity() is 3 for triples and 4 for quads.
		 */
		final Long oldCard = (Long) sp
				.getProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY);

		if (oldCard == null) {
			throw new AssertionError(
					"Expecting estimated-cardinality to be bound: sp=" + sp);
		}
		final int arity = context.isQuads() ? 4 : 3;
		final long newCard = (long) (1.0 / arity);

		sp.setProperty(AST2BOpBase.Annotations.ESTIMATED_CARDINALITY, newCard);

	}

	/**
	 * Computes an applicable key order for performing a distinct range term
	 * scan, if exists. Such a key order must be formed out of a prefix 
	 * [ConstList + DistinctVar], where ConstList is the list of constants
	 * in the triple pattern.
	 * 
	 * @param sp
	 * @param context
	 * @return matching key order, if exists, null if not (indicating failure)
	 */
	private IKeyOrder<ISPO> getApplicableKeyOrderIfExists(
		StatementPatternNode sp, IVariable<?> termScanVar, AST2BOpContext context) {
		
		boolean isQuads = context.getAbstractTripleStore().isQuads();
		
		// first, construct a predicate for index probing
		IVariableOrConstant[] args = new IVariableOrConstant[isQuads ? 4 : 3 ];
		args[0] = sp.s().getValueExpression();
		args[1] = sp.p().getValueExpression();
		args[2] = sp.o().getValueExpression();
		if (isQuads) {
			args[3] = sp.c()==null ?
				Var.var("--anon-"+context.nextId()) : 
				sp.c().getValueExpression();
		}

        // The graph term/variable iff specified by the query.		
		Map<String,Object> annotations = new HashMap<String,Object>();		
		Predicate pred = new Predicate(args,annotations);
		
		// next, retrieve access path for the predicate
		SPORelation spor = context.getAbstractTripleStore().getSPORelation();
		
		// before looking up the access path, we must append 
		IAccessPath<ISPO> accessPath = spor.getAccessPath(pred);
		
		// retrieve the key order
		IKeyOrder<ISPO> keyOrder = accessPath.getKeyOrder();
		
		// project the triple pattern on the index
		int keyArity = keyOrder.getKeyArity();
		IVariableOrConstant[] projection = new IVariableOrConstant[keyArity];
		for (int i=0; i<keyArity; i++) {
			projection[i] = args[keyOrder.getKeyOrder(i)];
		}
		
		// Verify that the calculated projection (which projects the sp on the
		// key order) has the form  (IConstant)* ProjectionVar (IVariable)*
		//
		// We do so by a DFA with three stati 0, 1, and 2 (error), defined
		// as follows:
		//
		//  (IConstant)* ProjectionVar (IVariable || null)		
		// ^                          ^
		// |                          |
		// 0                          1    <--- status variable
		int status = 0; // nothing found yet
		for (int i=0; i<projection.length && status<2; i++) {
			IVariableOrConstant varOrConst = projection[i] ;
			if (status==0 && varOrConst instanceof IConstant) {
				// stay in status 0
			} else if (status==0 && varOrConst instanceof IVariable) {
				// make sure the variable is the projection variable
				IVariable var = (IVariable)varOrConst;
				if (var.equals(termScanVar)) {
					status = 1; // from now on, allow only other vars
				} else {
					status = 2; // error, key order invalid
				}
			} else if (status==1 && varOrConst==null) {
				// legal, no state change				
			} else if (status==1 && varOrConst instanceof IVariable) {
				// make sure the variable is *not* the projection variable 
				IVariable var = (IVariable)varOrConst;
				if (var.equals(termScanVar)) {
					status = 2; // error, key order invalid
				}
			} else {
				status = 2; // error
			}
		}
		
		return status==1 /* success */? keyOrder : null; 
	}

}
