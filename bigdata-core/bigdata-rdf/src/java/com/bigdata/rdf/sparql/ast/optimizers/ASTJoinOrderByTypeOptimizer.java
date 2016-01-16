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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IJoinNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.PropertyPathUnionNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.ZeroLengthPathNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;

/**
 * This optimizer simply puts each type of {@link IGroupMemberNode} within a
 * {@link JoinGroupNode} in the right order with respect to the other types.
 * 
 * TODO TEST SUITE!
 * 
 * This optimizer is deprecated. It can enabled using the query hint
 * {@link QueryHints#DEFAULT_OLD_JOIN_ORDER_OPTIMIZER}. The new optimizer
 * replacing this one is the {@link ASTJoinGroupOrderOptimizer}.
 */
@Deprecated
public class ASTJoinOrderByTypeOptimizer extends AbstractJoinGroupOptimizer 
		implements IASTOptimizer {

//    private static final Logger log = Logger
//            .getLogger(ASTJoinOrderByTypeOptimizer.class);

//    @Override
//    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
//            IBindingSet[] bindingSets) {
//
//        if (!(queryNode instanceof QueryRoot))
//            return queryNode;
//
//        final QueryRoot queryRoot = (QueryRoot) queryNode;
//        
//        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);
//
//        // Main WHERE clause
//        {
//
//            @SuppressWarnings("unchecked")
//			final GraphPatternGroup<IGroupMemberNode> whereClause = 
//            	(GraphPatternGroup<IGroupMemberNode>) queryRoot.getWhereClause();
//
//            if (whereClause != null) {
//
//                optimize(context, sa, whereClause);
//                
//            }
//
//        }
//
//        // Named subqueries
//        if (queryRoot.getNamedSubqueries() != null) {
//
//            final NamedSubqueriesNode namedSubqueries = queryRoot
//                    .getNamedSubqueries();
//
//            /*
//             * Note: This loop uses the current size() and get(i) to avoid
//             * problems with concurrent modification during visitation.
//             */
//            for (NamedSubqueryRoot namedSubquery : namedSubqueries) {
//
//                @SuppressWarnings("unchecked")
//				final GraphPatternGroup<IGroupMemberNode> whereClause = 
//                	(GraphPatternGroup<IGroupMemberNode>) namedSubquery.getWhereClause();
//
//                if (whereClause != null) {
//
//                    optimize(context, sa, whereClause);
//
//                }
//
//            }
//
//        }
//
//        // log.error("\nafter rewrite:\n" + queryNode);
//
//        return queryNode;
//
//    }
//
//    private void optimize(final IEvaluationContext ctx, final StaticAnalysis sa,
//    		final GraphPatternGroup<?> op) {
//
//    	if (op instanceof JoinGroupNode) {
//    		
//    		final JoinGroupNode joinGroup = (JoinGroupNode) op;
//    		
//    		if (ASTStaticJoinOptimizer.isStaticOptimizer(ctx, joinGroup)) {
//
//                doOrderByType(ctx, joinGroup, sa);
//
//    		}
//    		
//    	} // is JoinGroupNode
//    	
//        /*
//         * Recursion, but only into group nodes (including within subqueries).
//         */
//        final int arity = op.arity();
//
//        for (int i = 0; i < arity; i++) {
//
//            final BOp child = op.get(i);
//
//            if (child instanceof GraphPatternGroup<?>) {
//
//                @SuppressWarnings("unchecked")
//                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) child;
//
//                optimize(ctx, sa, childGroup);
//                
//            } else if (child instanceof QueryBase) {
//
//                final QueryBase subquery = (QueryBase) child;
//
//                @SuppressWarnings("unchecked")
//                final GraphPatternGroup<IGroupMemberNode> childGroup = (GraphPatternGroup<IGroupMemberNode>) subquery
//                        .getWhereClause();
//
//                optimize(ctx, sa, childGroup);
//
//            }
//            
//        }
//
//    }

    /**
     * Get the group member nodes into the right order:
     * <pre> 
     * 1. Pre-filters
     * 2. In-filters
     * x. Assignments with a constant
     * 
     *    Required joins:
     *    
     *   3. Some Service calls (e.g. Bigdata SEARCH)
     *   4. Subquery-includes
     *   5. Statement patterns
     *   7. Sparql11 subqueries
     *   8. Non-optional subgroups
     *   9. Other service calls
     *   
     *   TODO: the placement of OPTIONALS should really be more complicated than this.
     *   e.g. consider interaction with SERVICE calls etc.
     *   Optional joins:
     *   10. Simple optionals & optional subgroups
     * 
     * 11. Assignments
     * 12. Post-conditionals
     *     
     * </pre> 
     * Most of this logic was lifted out of {@link AST2BOpUtility}.
     * <p>
     * Note: Join filters are now attached to {@link IJoinNode}s.
     */
	@Override
    protected void optimizeJoinGroup(final AST2BOpContext ctx,
    		final StaticAnalysis sa, final IBindingSet[] bSets, final JoinGroupNode joinGroup) {

		if (!ASTStaticJoinOptimizer.isStaticOptimizer(ctx, joinGroup))
			return;
			
        final List<IGroupMemberNode> ordered = new LinkedList<IGroupMemberNode>();

        final List<AssignmentNode> assignments = joinGroup.getAssignments();

        final List<ServiceNode> serviceNodes = joinGroup.getServiceNodes();

        final List<SubqueryRoot> askSubqueries = new LinkedList<SubqueryRoot>();

        for (BindingsClause values : joinGroup.getChildren(BindingsClause.class)) {

            ordered.add(values);
            
        }

        /*
         * Assignments for a constant.
         * 
         * Note: This supports query engines which use BIND() to convey
         * a binding into a remote SPARQL end point (openrdf does this).
         * For example, see their service09 test.
         */
        {

            final Iterator<AssignmentNode> aitr = assignments.iterator();

            while (aitr.hasNext()) {

                final AssignmentNode n = aitr.next();

                @SuppressWarnings("rawtypes")
                final IValueExpression<? extends IV> valExpr = n
                        .getValueExpression();

                if (valExpr instanceof IConstant) {

                    ordered.add(n);

                    aitr.remove();

                }

            }

        }

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
        
        { // begin required joins.

            /*
             * Run some service calls first (or as early as possible) and
             * schedule service calls to be run last
             */
            {

                final Iterator<ServiceNode> sitr = serviceNodes.iterator();

                while (sitr.hasNext()) {

                    final ServiceNode n = sitr.next();

                    if (n.getServiceRef().isConstant()) {

                        final URI serviceURI = (URI) n.getServiceRef()
                                .getValue();

                        final ServiceFactory f = ServiceRegistry.getInstance()
                                .get(serviceURI);

                        
                        if (f!=null) {
                           
                            /**
                             * Queue services in the beginning or in the end.
                             * Note that the query hint can be used to override
                             * the service defaults.
                             */
                            if (f.getServiceOptions().isRunFirst()) {

                                ordered.add(n);

                                sitr.remove();
                              
                            } 
                           
                        }

                    }

                }

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
             * Add required statement pattern joins, the filters on those
             * joins, and property path stuff.
             * 
             * Note: This winds up handling materialization steps as well (it
             * calls through to Rule2BOpUtility).
             */
            for (IGroupMemberNode child : joinGroup) {
            	
            	if (child instanceof StatementPatternNode) {
                
	                final StatementPatternNode sp = (StatementPatternNode) child;
	                
	                if (!sp.isOptional()) {
	                
	                    ordered.add(child);
	                    
	                }
	                
            	} else if (child instanceof ArbitraryLengthPathNode ||
            				child instanceof ZeroLengthPathNode ||
            				  child instanceof PropertyPathUnionNode) {
            		
                    ordered.add(child);
                    
            	}
                
            }
            
            /*
             * TODO Why is this here?!? It should either be empty or run
             * after the last required join, right?
             */
            for (IGroupMemberNode n : sa.getJoinFilters(joinGroup)) {
                
                ordered.add(n);
                
            }
            
            /*
             * Add SPARQL 1.1 style subqueries which were not lifted out into
             * named subqueries.
             */
            for (IGroupMemberNode child : joinGroup) {
                if (child instanceof SubqueryRoot) {
                    final SubqueryRoot subquery = (SubqueryRoot) child;
                    if (subquery.getQueryType() == QueryType.ASK) {
                        /**
                         * ASK subqueries are used for FILTER EXISTS and FILTER
                         * NOT EXISTS. They can not be run before the required
                         * join groups.
                         * 
                         * @see <a
                         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/515">
                         *      Query with two "FILTER NOT EXISTS" expressions
                         *      returns no results</a>
                         */
                        askSubqueries.add(subquery);
                        continue;
                    }
                    ordered.add(child);
                }
            }

            /*
             * Do the non-optional sub-groups (Join groups and UNION).
             */
            for (IGroupMemberNode child : joinGroup) {

                if (!(child instanceof GraphPatternGroup<?>)) {
                    continue;
                }

                @SuppressWarnings("unchecked")
                final GraphPatternGroup<?> subgroup = (GraphPatternGroup<?>) child;

                if (subgroup.isOptional()) {
                    continue;
                }
                
                if (subgroup instanceof PropertyPathUnionNode) {
                	continue;
                }

                ordered.add(subgroup);

            }

            /*
             * Run services which have constant URIs next.
             * 
             * TODO These could be ordered by the #of unbound variables
             * or some such. Simple triple patterns for which we can use
             * ESTCARD could be ordered more precisely.
             */
            {

                final Iterator<ServiceNode> sitr = serviceNodes
                        .iterator();

                while (sitr.hasNext()) {

                    final ServiceNode n = sitr.next();

                    if (!n.getServiceRef().isConstant())
                        continue;

                    sitr.remove();
                    
                    ordered.add(n);

                }

            }

            /*
             * Run remaining service calls (those with a variable
             * service ref that have not been scheduled as run first or run
             * last).
             */
            for (ServiceNode n : serviceNodes) {

                ordered.add(n);

            }

        } // end of required joins.

//      /*
//       * Add the subqueries (individual optional statement patterns, optional
//       * join groups, and nested union).
//       */

        /**
         * Run the ASK subqueries (FILTER EXISTS, FILTER NOT EXISTS).
         * 
         * TODO This should be (I think) a permissible placement for the ASK
         * subqueries. However, we might still run into problems if FILTER (NOT)
         * EXISTS is run for a variable which is only bound by an OPTIONAL.
         * 
         * TODO There could also be a problem with the ordering of MINUS. Both
         * FILTER (NOT) EXISTS and MINUS need further inspection of the
         * constraints on when they may be evaluated, both in terms of
         * efficiency and correctness. I believe that the correct constraint for
         * FILTER (NOT) EXISTS is simply that for FILTER attachment: That is
         * (a)for variables bound by required joins, no sooner than their filter
         * variables are either known to be bound; and (b) for variables only
         * bound by OPTIONALS, not until after the last point at which they
         * MIGHT be bound.
         * 
         * Note: While that while the change for ticket 515 fixes that query, it
         * is possible that we still could get bad join orderings when the
         * variables used by the filter are only bound by OPTIONAL joins. It is
         * also possible that we could run the ASK subquery for FILTER (NOT)
         * EXISTS earlier if the filter variables are bound by required joins.
         * This is really identical to the join filter attachment problem. The
         * problem in the AST is that both the ASK subquery and the FILTER are
         * present. It seems that the best solution would be to attach the ASK
         * subquery to the FILTER and then to run it immediately before the
         * FILTER, letting the existing filter attachment logic decide where to
         * place the filter. We would also have to make sure that the FILTER was
         * never attached to a JOIN since the ASK subquery would have to be run
         * before the FILTER was evaluated.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/515">
         *      Query with two "FILTER NOT EXISTS" expressions returns no
         *      results</a>
         */
        for (SubqueryRoot askSubquery : askSubqueries) {

            ordered.add(askSubquery);
            
        }
        
//        /*
//         * Next do the property paths.
//         */
//        for (PropertyPathNode pathNode : joinGroup.getChildren(PropertyPathNode.class)) {
//        	
//        	ordered.add(pathNode);
//        	
//        }
        
        /*
         * Next do the optional sub-groups.
         */
        for (IGroupMemberNode child : joinGroup) {

            if (child instanceof StatementPatternNode) {

                final StatementPatternNode sp = (StatementPatternNode) child;

                if (sp.isOptional()) {

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
                     */

                    ordered.add(sp);

                }

            }
            
            if (!(child instanceof GraphPatternGroup<?>)) {
                continue;
            }

            @SuppressWarnings("unchecked")
            final GraphPatternGroup<?> subgroup = (GraphPatternGroup<?>) child;

            if (!subgroup.isOptional()) {
                continue;
            }
            
            if (subgroup instanceof PropertyPathUnionNode) {
            	continue;
            }

            ordered.add(subgroup);

        }
        
        /*
         * Add the LET assignments to the pipeline.
         */
        for (AssignmentNode n : assignments) {
            
            ordered.add(n);
            
        }

        /*
         * Add the post-conditionals to the pipeline.
         */
        for (IGroupMemberNode n : sa.getPostFilters(joinGroup)) {
            
            ordered.add(n);
            
        }
        
        final int arity = joinGroup.arity();

        if (ordered.size() != arity) {
    
            throw new AssertionError("should not be pruning any children");
            
        }

        // Replace the children with those in the [ordered] list.
        for (int i = 0; i < arity; i++) {

            joinGroup.setArg(i, (BOp) ordered.get(i));

        }

    }

}
