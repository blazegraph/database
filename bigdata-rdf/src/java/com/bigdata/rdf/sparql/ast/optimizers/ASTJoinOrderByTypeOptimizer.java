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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;

/**
 * This optimizer simply puts each type of {@link IGroupMemberNode} within a
 * {@link JoinGroupNode} in the right order w.r.t. to the other types.
 */
public class ASTJoinOrderByTypeOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTJoinOrderByTypeOptimizer.class);

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;
        
        final StaticAnalysis sa = new StaticAnalysis(queryRoot);

        // Main WHERE clause
        {

            @SuppressWarnings("unchecked")
			final GraphPatternGroup<IGroupMemberNode> whereClause = 
            	(GraphPatternGroup<IGroupMemberNode>) queryRoot.getWhereClause();

            if (whereClause != null) {

                optimize(context, sa, whereClause);
                
            }

        }

        // Named subqueries
        if (queryRoot.getNamedSubqueries() != null) {

            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            /*
             * Note: This loop uses the current size() and get(i) to avoid
             * problems with concurrent modification during visitation.
             */
            for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                @SuppressWarnings("unchecked")
				final GraphPatternGroup<IGroupMemberNode> whereClause = 
                	(GraphPatternGroup<IGroupMemberNode>) namedSubquery.getWhereClause();

                if (whereClause != null) {

                    optimize(context, sa, whereClause);

                }

            }

        }

        // log.error("\nafter rewrite:\n" + queryNode);

        return queryNode;

    }

    /**
     * Get the group member nodes into the right order:
     * 
     * 1. Pre-filters
     * 2. In-filters
     * 3. Service calls
     * 4. Subquery-includes without join vars
     * 5. Statement patterns
     * 6. Join filters
     * 7. Sparql11 subqueries
     * 8. Subquery-includes with join vars
     * 9. Non-optional subgroups
     * 10. Simple optionals
     * 11. Optional subgroups
     * 12. Assignments
     * 13. Post-conditionals
     * 
     * Most of this logic was lifted out of {@link AST2BOpUtility}.
     * 
     */
    private void optimize(final AST2BOpContext ctx, final StaticAnalysis sa,
    		final GraphPatternGroup<?> op) {

    	if (op instanceof JoinGroupNode) {
    		
    		final JoinGroupNode joinGroup = (JoinGroupNode) op;
    	
	    	final List<IGroupMemberNode> ordered = new LinkedList<IGroupMemberNode>();
	    	
	        /*
	         * Add the pre-conditionals to the pipeline.
	         * 
	         * TODO These filters should be lifted into the parent group (by a
	         * rewrite rule) so we can avoid starting a subquery only to have it
	         * failed by a filter. We will do less work if we fail the solution in
	         * the parent group.
	         */
	    	for (IGroupMemberNode n : sa.getPreFilters(joinGroup)) {
	    		
	    		ordered.add(n);
	    		
	    	}
	    	
	        /*
	         * FIXME We need to move away from the DataSetJoin class and replace it
	         * with an IPredicate to which we have attached an inline access path.
	         * That transformation needs to happen in a rewrite rule, which means
	         * that we will wind up removing the IN filter and replacing it with an
	         * AST node for that inline AP (something conceptually similar to a
	         * statement pattern but for a column projection of the variable for the
	         * IN expression). That way we do not have to magically "subtract" the
	         * known "IN" filters out of the join- and post- filters.
	         * 
	         * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Replace
	         * DataSetJoin with an "inline" access path.)
	         * 
	         * @see JoinGroupNode#getInFilters()
	         */
	    	for (IGroupMemberNode n : joinGroup.getInFilters()) {
	    		
	    		ordered.add(n);
	    		
	    	}
	    	
	        /*
	         * Required joins and non-optional subqueries.
	         * 
	         * Note: SPARQL 1.1 style subqueries are currently always pipelined.
	         * Like named subquery includes, they are also never optional. However,
	         * there is no a-priori reason why we should run pipelined subqueries
	         * before named subquery includes and, really, no reason why we can not
	         * mix these with the required joins (above). I believe that this is
	         * being done solely for expediency (because the static query optimizer
	         * can not handle it).
	         * 
	         * Also, note that named subquery includes are hash joins. We have an
	         * index. If the operator supported cutoff evaluation then we could
	         * easily reorder them with the other required joins using the RTO.
	         * 
	         * Ditto for pipelined SPARQL 1.1 subquery. If it supported cutoff
	         * evaluation, then the RTO could reorder them with the required joins.
	         * This is even true when the subquery uses GROUP BY or ORDER BY, which
	         * imply the use of at once operators. While we must fully materialize
	         * the solutions for each evaluation of the subquery, the evaluation is
	         * based on the as-bound solutions flowing into the subquery. If the
	         * subquery is unselective, then clearly this will be painful and it
	         * might be better to lift such unselective subqueries into named
	         * subqueries in order to obtain a hash index over the entire subquery
	         * solution set when evaluated with an empty source binding set.
	         * 
	         * Note: This logic was originally constructed before we had required
	         * joins other than on a statement pattern. This shaped how the FILTERs
	         * were attached and how the materialization pipeline was generated in
	         * order to have materialized RDF Values on hand for those FILTERs.
	         * 
	         * We now have several kinds of required joins: pipelined statement
	         * pattern joins, SPARQL 1.1 subquery, named subquery include, subquery
	         * hash joins (when the subquery is optional), service call joins, etc.
	         * 
	         * FIXME The code currently only handles the FILTER attachment and
	         * materialization pipeline for the required statement pattern joins.
	         * However, for efficiency, FILTERs MUST be attached to these joins as
	         * appropriate for ALL CASES and variables MUST be materialized as
	         * required for those filters to run.
	         * 
	         * FIXME All of these joins can be reordered by either static analysis
	         * of cardinality (which has not been extended to handle this yet) or by
	         * the RTO. The filter attachment decisions (and the materialization
	         * pipeline generation) needs to be deferred until we actually evaluate
	         * the join graph (at least for the RTO).
	         */
	        {

	            /*
	             * Run service calls first.
	             */
		    	for (IGroupMemberNode n : joinGroup.getServiceNodes()) {
		    		
		    		ordered.add(n);
		    		
		    	}

	            /*
	             * Add joins against named solution sets from WITH AS INCLUDE style
	             * subqueries for which there are NO join variables. Such includes
	             * will be a cross product so we want to run them as early as
	             * possible.
	             * 
	             * Note: This corresponds to a very common use case where the named
	             * subquery is used to constrain the remainder of the join group.
	             * 
	             * Note: If there ARE join variables then the named subquery include
	             * MUST NOT be run until after the join variables have been bound.
	             * Failure to observe this rule will cause the unbound variable to
	             * be included when computing the hash code of a solution and the
	             * join will not produce the correct solutions. [If it is desired to
	             * always run named subqueries first then you need to make sure that
	             * the join variables array is empty for the INCLUDE.]
	             */
	            for (IGroupMemberNode child : joinGroup) {
	                if (child instanceof NamedSubqueryInclude) {
                        ordered.add(child);
	                }
	            }

	            /*
	             * Add required statement pattern joins and the filters on those
	             * joins.
	             * 
	             * Note: This winds up handling materialization steps as well (it
	             * calls through to Rule2BOpUtility).
	             */
		    	for (IGroupMemberNode n : joinGroup.getStatementPatterns()) {
		    		
		    		final StatementPatternNode sp = (StatementPatternNode) n;
		    		
		    		if (!sp.isOptional()) {
		    		
		    			ordered.add(n);
		    			
		    		}
		    		
		    	}
		    	for (IGroupMemberNode n : sa.getJoinFilters(joinGroup)) {
		    		
		    		ordered.add(n);
		    		
		    	}
	            
	            /*
	             * Add SPARQL 1.1 style subqueries which were not lifted out into
	             * named subqueries.
	             */
	            for (IGroupMemberNode child : joinGroup) {
	                if (child instanceof SubqueryRoot) {
	                    ordered.add(child);
	                }
	            }

	        }

	        /*
	         * Add the subqueries (individual optional statement patterns, optional
	         * join groups, and nested union).
	         */

	        /*
	         * First do the non-optional subgroups
	         */
	        for (IGroupMemberNode child : joinGroup) {

	            if (!(child instanceof GraphPatternGroup<?>)) {
	                continue;
	            }

	            @SuppressWarnings("unchecked")
	            final GraphPatternGroup<IGroupMemberNode> subgroup = (GraphPatternGroup<IGroupMemberNode>) child;

	            if (subgroup.isOptional()) {
	                continue;
	            }

				ordered.add(subgroup);

	        }

	        /*
	         * Next do the optional sub-groups.
	         */
	        for (IGroupMemberNode child : joinGroup) {

	            if (child instanceof StatementPatternNode) {

	                final StatementPatternNode sp = (StatementPatternNode) child;

	                if (sp.isSimpleOptional()) {

	                    /*
	                     * ASTSimpleOptionalOptimizer will recognize and lift out
	                     * simple optionals into the parent join group. A simple
	                     * optional is basically a single a statement pattern in an
	                     * optional join group. If there were any FILTERs in the
	                     * simple optional join group, then they were lifted out as
	                     * well and attached to this StatementPatternNode. Such
	                     * FILTER(s) MUST NOT have materialization requirements for
	                     * variables which were not already bound before the
	                     * optional JOIN on this statement pattern.
	                     * 
	                     * TODO Move logic to set OPTIONAL on the Predicate into
	                     * toPredicate. It can already see the isSimpleOptional
	                     * annotation on the StatementPatternNode.
	                     */

	                	ordered.add(sp);

	                }

	            }
	            
	            if (!(child instanceof GraphPatternGroup<?>)) {
	                continue;
	            }

	            @SuppressWarnings("unchecked")
	            final GraphPatternGroup<IGroupMemberNode> subgroup = (GraphPatternGroup<IGroupMemberNode>) child;

	            if (!subgroup.isOptional()) {
	                continue;
	            }

                ordered.add(subgroup);

	        }
	        
	        
	        /*
	         * Add the LET assignments to the pipeline.
	         * 
	         * TODO Review as generated query plans: Make sure that we do not
	         * reorder LET/BIND in a join group. I believe that we are supposed to
	         * run them in the given order, just not in the given location (they
	         * run last).
	         */
	    	for (IGroupMemberNode n : joinGroup.getAssignments()) {
	    		
	    		ordered.add(n);
	    		
	    	}

	        /*
	         * Add the post-conditionals to the pipeline.
	         */
	    	for (IGroupMemberNode n : sa.getPostFilters(joinGroup)) {
	    		
	    		ordered.add(n);
	    		
	    	}

	        if (ordered.size() != op.arity()) {
	    		
	    		throw new AssertionError("should not be pruning any children");
	    		
	    	}
	    	
	    	for (int i = 0; i < op.arity(); i++) {
	    		
	    		op.setArg(i, (BOp) ordered.get(i));
	    		
	    	}
	    	
    	}
    	
        /*
         * Recursion, but only into group nodes (including within subqueries).
         */
        for (int i = 0; i < op.arity(); i++) {

            final BOp child = op.get(i);

            if (child instanceof GraphPatternGroup<?>) {

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) child;

                optimize(ctx, sa, childGroup);
                
            } else if (child instanceof QueryBase) {

                final QueryBase subquery = (QueryBase) child;

                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
                        .getWhereClause();

                optimize(ctx, sa, childGroup);

            }
            
        }

    }
    
}
