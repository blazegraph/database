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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;

import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.eval.ASTFulltextSearchOptimizer;
import com.bigdata.rdf.sparql.ast.eval.ASTGeoSpatialSearchOptimizer;
import com.bigdata.rdf.sparql.ast.eval.ASTSearchInSearchOptimizer;
import com.bigdata.rdf.sparql.ast.eval.ASTSearchOptimizer;
import com.bigdata.util.ClassPathUtil;

/**
 * Pre-populated list of the default optimizers.
 * 
 * <pre>
 * optimizerList.add(new BindingAssigner()); // done.
 * optimizerList.add(new ConstantOptimizer(strategy));
 * optimizerList.add(new CompareOptimizer());
 * optimizerList.add(new ConjunctiveConstraintSplitter());
 * optimizerList.add(new SameTermFilterOptimizer());
 * // only need to optimize the join order this way if we are not
 * // using native joins
 * if (nativeJoins == false) {
 *     optimizerList.add(new QueryJoinOptimizer(new BigdataEvaluationStatistics(
 *             this)));
 * }
 * optimizerList.add(new FilterOptimizer());
 * optimizerList.optimize(tupleExpr, dataset, bindings);
 * replaceValues(dataset, tupleExpr, bindings); // yes.
 * </pre>
 * 
 * FIXME {@link CompareOptimizer}. Replaces Compare with SameTerm whenever
 * possible. (I think that we handle this in the {@link FunctionRegistry}, but
 * that should be verified and documented here.)
 * 
 * FIXME {@link ConjunctiveConstraintSplitter}. Takes a FILTER with an AND of
 * constraints and replaces it with one filter per left or right hand side of
 * the AND. This flattens the value expression hierarchy. It also might allow us
 * to reject some solutions earlier in the pipeline since we do not have to wait
 * for all of the variables to become bound and we can potentially evaluate some
 * of the individual constraints earlier than others.
 * 
 * FIXME {@link SameTermFilterOptimizer}. Optimizes SameTerm(X,Y) by renaming
 * one of the variables to the other variable. Optimizes SameTerm(X,aURI) by
 * assigning the constant to the variable.
 * 
 * FIXME {@link FilterOptimizer}. Pushes filters as far down in the query model
 * as possible. (What purpose does this serve for us? We want to attach filters
 * to the first required join where they can possible by evaluated.)
 * 
 * FIXME Sesame has define a bunch of IQueryOptimizers that we are not using
 * (even before this refactor). Many of them apply to their SQL backends. A few
 * might be relevant to us:
 * <ul>
 * <li>{@link DisjunctiveConstraintOptimizer} - moves SameTerm closer to joins
 * for queries involving UNIONs.</li>
 * <li>{@link IterativeEvaluationOptimizer} - ???</li>
 * <li>{@link QueryModelNormalizer} - various simplifications of their tuple
 * expression query model. I am not sure whether or not there is anything here
 * we could leverage.</li>
 * </ul>
 * 
 * TODO Recognize OR of constraints and rewrite as IN. We can then optimize the
 * IN operator in a variety of ways (in fact, the {@link FunctionRegistry}
 * already handles those optimizations for IN).
 * 
 * TODO The combination of DISTINCT and ORDER BY can be optimized using an ORDER
 * BY in which duplicate solutions are discarded after the sort by a filter
 * which compares the current solution with the prior solution.
 * 
 * TODO A query with a LIMIT of ZERO (0) should be failed as there will be no
 * solutions.
 * 
 * TODO Minor optimization: A bare constant in the ORDER BY value expression
 * list should be dropped. If there are no remaining value expressions, then the
 * entire ORDER BY operation should be dropped.
 * 
 * FIXME Write AST optimizer which rejects queries that SELECT variables which
 * do not otherwise appear in the query.
 * 
 * TODO If a child group is non-optional and not a union, then flatten it out
 * (lift it into the parent). (This sort of thing is not directly expressible in
 * the SPARQL syntax but it might arise through other AST transforms.)
 * 
 * TODO Optimizer to lift optionals outside of an ORDER BY when the OPTIONALs
 * are not used by the ORDER BY. This let's us avoid optional joins until we
 * know what is in the result set.
 * 
 * TODO ORDER BY with LIMIT could be optimized as a pipelined + last pass
 * operator which maintained a sorted array of LIMIT+OFFSET items. Any items
 * which order after the LIMIT+OFFSET item in the list are discarded. This will
 * keep down both the memory overhead of the ORDER BY operator and the #of
 * comparisons to be made. (We still need a SLICE to handle the OFFSET unless
 * the ORDER BY operator takes care of that during its last pass by only writing
 * the items from OFFSET through LIMIT-1.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DefaultOptimizerList extends ASTOptimizerList {

//    private static final Logger log = Logger.getLogger(DefaultOptimizerList.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private static final MapgraphOptimizers mapgraphOptimizers = new MapgraphOptimizers();

    
    public DefaultOptimizerList() {

        
       // DO NOT ADD ANY OPTIMIZERS BEFORE THIS WITHOUT CHECKING 
       // THE COMMENT BELOW! ALSO, DO NOT ADD ANY OPTIMIZERS 
       // AFTER ASTOrderByAggregateFlatteningOptimizer THAT MAY
       // INTRODUCE ANONYMOUS AGGREGATES IN 'ORDER BY'.
        
       /** Flattens ORDER BY arguments by introducing auxiliary aliases 
        *  in the corresponding SELECTs.
        *  This is not a real optimizer as it is not optional -- 
        *  this AST transformation is necessary to comply with the SPARQL 
        *  semantics. It must be applied before any optional optimisations 
        *  and <b>no consequent optimizer calls are allowed to introduce 
        *  anonymous aggregates in ORDER BY<b>. 
        */  
       add(new ASTOrderByAggregateFlatteningOptimizer());
        
        
       /**
        * Query hints are identified applied to AST nodes based on the
        * specified scope and the location within the AST in which they are
        * found.
        */
       add(new ASTQueryHintOptimizer());
       
       /**
        * Brings complex filter expressions into CNF, decomposes them to
        * allow for more exact placement and reasoning on individual filter
        * components, and eliminates duplicate and simple redundant filter
        * expressions.
        */
       add(new ASTFilterNormalizationOptimizer());       
       
       /**
        * Optimizes various constructs that lead to global static bindings 
        * for query execution, such as BIND/VALUES clauses involving constants,
        * but also FILTER expressions binding a variable via sameTerm() or
        * IN to one ore more constants. These constructs are removed from
        * the query and added to the binding set we start out with.
        * 
        * IMPORTANT NOTE: setting up the starting binding set, this optimizer
        * is an important prerequisite for others and should be run early in
        * the optimzer pipeline.
        */
       add(new ASTStaticBindingsOptimizer());       
       

    	/**
    	 * Converts a BDS.SEARCH_IN_SEARCH function call (inside a filter)
    	 * into a full text index to determine the IN set.
    	 * 
    	 * Convert:
    	 * 
    	 * filter(<BDS.SEARCH_IN_SEARCH>(?o,"foo")) .
    	 * 
    	 * To:
    	 * 
    	 * filter(?o IN ("foo", "foo bar", "hello foo", ...)) .
    	 * 
    	 * This is a way of using the full text index to filter instead of
    	 * using regex.
    	 */
    	add(new ASTSearchInSearchOptimizer());
    	
        /**
         * Many (most) property path expressions can be re-written as simple
         * joins and UNIONs and filters.  We need to do this before we set
         * the value expressions.
         */
        add(new ASTPropertyPathOptimizer());

        /**
         * Visit all the value expression nodes and convert them into value
         * expressions. If a value expression can be evaluated to a constant,
         * then it is replaced by that constant.
         */
        add(new ASTSetValueExpressionsOptimizer());

        
        /**
         * Flatten UNIONs where possible.
         * 
         * <pre>
         * UNION(A,B,C) := UNION(A,UNION(B,C)) -or- UNION(UNION(A,B),C))
         * </pre>
         * 
         * Note: This must run before the {@link ASTEmptyGroupOptimizer} in
         * order to eliminate certain UNION/group combinations.
         */
        add(new ASTFlattenUnionsOptimizer());
        
        /**
         * Look for groups that have a single union and some filters, and lift
         * those filters inside the union.
         * 
         * <pre>
         * { UNION(A,B)+F } -> { UNION(A+F,B+F) }
         * </pre>
         */
        add(new ASTUnionFiltersOptimizer());
        
        /**
         * Eliminate semantically empty join group nodes which are the sole
         * child of another join groups.
         * 
         * <pre>
         * { { ... } } => { ... }
         * </pre>
         * 
         * and for non-GRAPH groups:
         * 
         * <pre>
         * { ... {} } => { ... }
         * </pre>
         * <p>
         * Note: as a policy decision in bigdata 1.1, we do not WANT to combine
         * non-empty join groups. The group structure is left AS IS and provides
         * a means for people to control the pruning of variables. Also, even if
         * the group structure were automatically flattened as much as possible
         * for non-empty groups, the optimizer(s) responsible for pruning
         * intermediate variables would cause a group structure to be
         * re-introduced.
         */
        add(new ASTEmptyGroupOptimizer());
        
        
        /**
         * Rewrites any {@link ProjectionNode} with a wild card into the set of
         * variables visible to the {@link QueryBase} having that projection.
         * This is done first for the {@link NamedSubqueriesNode} and then
         * depth-first for the WHERE clause. Only variables projected by a
         * subquery will be projected by the parent query.
         * <p>
         * Note: This needs to be run before anything else which looks at the
         * {@link ProjectionNode}.
         */
        add(new ASTWildcardProjectionOptimizer());
        
        

        /**
         * Translate {@link BD#SEARCH} and associated magic predicates into a a
         * {@link ServiceNode}. If there are multiple searches in the query,
         * then each is translated into its own {@link ServiceNode}. The magic
         * predicates identify the bindings to be projected out of the named
         * subquery (rank, cosine, etc).
         * <p>
         * Note: Search is most efficiently expressed within named subqueries.
         * This let's you perform additional joins against the solutions from
         * the search service before the results are materialized on a hash
         * index to be joined into the main query.
         */
        add(new ASTSearchOptimizer());
        

        /**
         * Translate {@link FTS#SEARCH} and associated magic predicates
         * into a a {@link ServiceNode}. If there are multiple external Solr
         * searches in the query, then each is translated into its own
         * {@link ServiceNode}. The magic predicates identify the bindings to
         * be projected out of the named subquery (score, snippet, etc).
         */
        add(new ASTFulltextSearchOptimizer());

        /**
         * Translate {@link GeoSpatial#SEARCH} and associated magic predicates
         * into a a {@link ServiceNode}. If there are multiple GeoSpatial
         * searches in the query, then each is translated into its own
         * {@link ServiceNode}.
         */
        add(new ASTGeoSpatialSearchOptimizer());
        
        /**
         * Imposes a LIMIT of ONE for a non-aggregation ASK query.
         */
        add(new AskOptimizer());

        /**
         * Rewrites the projection node of a DESCRIBE query into, generating a
         * CONSTRUCT clause and extending the WHERE clause to capture the
         * semantics of the DESCRIBE query. The query type is also changed to
         * CONSTRUCT.
         */
        add(new ASTDescribeOptimizer());

        /**
         * Creates and assigns a PROJECTION of all variables in the CONSTRUCT
         * clause.
         */
        add(new ASTConstructOptimizer());

        /**
         * Rewrites the group graph pattern annotation of an {@link ExistsNode}
         * or a {@link NotExistsNode} into an ASK subquery in the containing
         * join group.
         */
        add(new ASTExistsOptimizer());

        /**
         * Handles a variety of special constructions related to graph graph
         * groups.
         * <p>
         * Note: This optimizer MUST run before optimizers which lift out named
         * subqueries in order to correctly impose the GRAPH constraints on the
         * named subquery.
         * 
         * FIXME Semantics for GRAPH ?g {} (and unit test).
         * 
         * FIXME Semantics for GRAPH <uri> {} (and unit test).
         */
        add(new ASTGraphGroupOptimizer());

        /**
         * Lift FILTERs which can be evaluated based solely on the bindings in
         * the parent group out of a child group. This helps because we will
         * issue the subquery for the child group less often (assuming that the
         * filter rejects any solutions).
         * 
         * FIXME This is not implemented yet.
         */
        add(new ASTLiftPreFiltersOptimizer());

        /**
         * Pruning rules for unknown IVs in statement patterns:
         * 
         * If an optional join is known to fail, then remove the optional group
         * in which it appears from the group (which could be an optional group,
         * a join group, or a union).
         * 
         * If a required join is known to fail, then the parent will also fail.
         * Continue recursively up the parent hierarchy until we hit a UNION or
         * an OPTIONAL. If we reach the root of the where clause for a subquery,
         * then continue up the groups in which the subquery appears.
         * 
         * If the parent is a UNION, then remove the child from the UNION.
         * 
         * If a UNION has one child, then replace the UNION with the child.
         * 
         * If a UNION is empty, then fail the group in which it fails (unions
         * are not optional).
         * 
         * These rules should be triggered if a join is known to fail, which
         * includes the case of an unknown IV in a statement pattern as well
         * <code>GRAPH uri {}</code> where uri is not a named graph.
         * 
         * <pre>
         * 
         * TODO From BigdataEvaluationStrategyImpl3#945
         * 
         * Prunes the sop tree of optional join groups containing values
         * not in the lexicon.
         * 
         *         sopTree = stb.pruneGroups(sopTree, groupsToPrune);
         * 
         * 
         * If after pruning groups with unrecognized values we end up with a
         * UNION with no subqueries, we can safely just return an empty
         * iteration.
         * 
         *         if (SOp2BOpUtility.isEmptyUnion(sopTree.getRoot())) {
         *             return new EmptyIteration<BindingSet, QueryEvaluationException>();
         *         }
         * </pre>
         * 
         * and also if we encounter a value not in the lexicon, we can still
         * continue with the query if the value is in either an optional tail or
         * an optional join group (i.e. if it appears on the right side of a
         * LeftJoin). We can also continue if the value is in a UNION. Otherwise
         * we can stop evaluating right now.
         * 
         * <pre>
         *                 } catch (UnrecognizedValueException ex) {
         *                     if (sop.getGroup() == SOpTreeBuilder.ROOT_GROUP_ID) {
         *                         throw new UnrecognizedValueException(ex);
         *                     } else {
         *                         groupsToPrune.add(sopTree.getGroup(sop.getGroup()));
         *                     }
         *                 }
         * </pre>
         * 
         * 
         * ASTUnknownTermOptimizer: If an unknown terms appears in a
         * StatementPatternNode then we get to either fail the query or prune
         * that part of the query. If it appears in an optional, then prune the
         * optional. if it appears in union, the prune that part of the union.
         * if it appears at the top-level then there are no solutions for that
         * query. This is part of what
         * BigdataEvaluationStrategyImpl3#toPredicate(final StatementPattern
         * stmtPattern) is doing. Note that toVE() as called from that method
         * will throw an UnknownValueException if the term is not known to the
         * database.
         * 
         * FIXME Mike started on this, but it is not yet finished.
         */
//        add(new ASTUnknownTermOptimizer());
        
        /**
         * Convert an ALP service call into an ArbitraryLengthPathNode
         */
        add(new ASTALPServiceOptimizer());

        /**
         * Rewrites aspects of queries where bottom-up evaluation would produce
         * different results.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/1113"> Hook to configure
         * the ASTOptimizerList </a>
         */
        add(new ASTBottomUpOptimizer());

        /**
         * Lifts a simple optional out of the child group.
         * <p>
         * Note: In order for this to work we have to attach any FILTER(s)
         * lifted out of the optional group to the statement pattern node and
         * then cause them to be attached to the JOIN when we generate the JOIN.
         */
        add(new ASTSimpleOptionalOptimizer());
        
        /**
         * Flattens non-optional, non-minus JoinGroupNodes with their parent
         * JoinGroupNode, eliminating unnecessary hash joins.
         */
        add(new ASTFlattenJoinGroupsOptimizer());

        /**
         * Rewrites the WHERE clause of each query by lifting out any
         * {@link ServiceNode}s into a named subquery. Rewrites the WHERE clause
         * of any named subquery such that there is no more than one
         * {@link ServiceNode} in that subquery by lifting out additional
         * {@link ServiceNode}s into new named subqueries.
         * <p>
         * Note: This rewrite step is necessary to preserve the "run-once"
         * contract for a SERVICE call.  If a {@link ServiceNode} appears in
         * any position other than the head of a named subquery, it will be
         * invoked once for each solution which flows through that part of
         * the query plan.  This is wasteful since the SERVICE call does not
         * run "as-bound" (source solutions are not propagated to the SERVICE
         * when it is invoked).
         */
        add(new ASTServiceNodeOptimizer());

        /**
         * Brings the children in group nodes into an order that implements
         * the SPARQL 1.1 semantics, trying to optimize this order based on
         * various heuristics.
         */
        if (!QueryHints.DEFAULT_OLD_JOIN_ORDER_OPTIMIZER)
           add(new ASTJoinGroupOrderOptimizer());
        else
           add(new ASTJoinOrderByTypeOptimizer());
           
        /**
         * Uses the query hints RUN_FIRST and RUN_LAST to rearrange IJoinNodes.
         */
        add(new ASTRunFirstRunLastOptimizer());

        /*
         * FIXME Datatype and value range constraints. Per the notes immediately
         * above, incorporate an optimizer which leverages information about
         * ground and non-ground datatype constraints and value-range
         * constraints within the allowable ground datatypes for a variable when
         * it is first bound by an SP.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/238
         */
        add(new ASTRangeOptimizer());
        
        /**
         * Add range counts to all statement patterns.
         */
        addRangeCountOptimizer();
        
        /**
         * Attach cardinality to join groups and unions.  Not fully implemented
         * yet.
         */
        add(new ASTCardinalityOptimizer());
        
		/**
		 * Optimizes SELECT COUNT(*) { triple-pattern } using the fast range
		 * count mechanisms when that feature would produce exact results for
		 * the KB instance.
		 * 
		 * @see <a href="http://trac.blazegraph.com/ticket/1037" > Rewrite SELECT
		 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} as ESTCARD
		 *      </a>
		 */
		if (QueryHints.DEFAULT_FAST_RANGE_COUNT_OPTIMIZER)
			addFastRangeCountOptimizer();

		/**
		 * Optimizes SELECT COUNT(*) ?z { triple-pattern } GROUP BY ?z using
		 * the fast rang count pattern documented above, i.e. the COUNT is
		 * rewritten to be performed via fast range count optimization where
		 * possible. In addition, the computation of bindings for grouping
		 * variable ?z is pushed inside a SELECT DISTINCT ?z { triple-pattern }
		 * subquery, which may be amenable to optimization through the
		 * {@link ASTDistinctTermScanOptimizer}, which is applied in the
		 * subsequent step.
		 */
		if (QueryHints.DEFAULT_FAST_RANGE_COUNT_OPTIMIZER)
			add(new ASTSimpleGroupByAndCountOptimizer());
		
        /**
		 * Optimizes
		 * <code>SELECT DISTINCT ?property WHERE { ?x ?property ?y . }</code>
		 * and similar patterns using an O(N) algorithm, where N is the number
		 * of distinct solutions.
		 * <p>
		 * Note: Either this must run after the {@link ASTRangeCountOptimizer}
		 * in order to modify the estimated cardinality associated with using
		 * the {@link DistinctTermAdvancer} (which does less work than a scan)
		 * or the {@link ASTRangeCountOptimizer} must not overwrite the
		 * cardinality estimates attached by this optimizer and this optimizer
		 * could run first.
		 * 
		 * @see <a href="http://trac.blazegraph.com/ticket/1035" > DISTINCT
		 *      PREDICATEs query is slow </a>
		 */
		if (QueryHints.DEFAULT_DISTINCT_TERM_SCAN_OPTIMIZER)
			add(new ASTDistinctTermScanOptimizer());
        
        /**
         * Run the static join order optimizer. This attaches the estimated
         * cardinality data (fast range counts) and uses fast algorithm to
         * reorder the joins in each required or optional join group.
         * 
         * TODO Make the static join optimizer more robust by modifying it to
         * process a "flattened" join group, in which SPs are lifted up from
         * child groups and subqueries. (This is possible unless the subquery
         * uses some aggregation mechanisms, SLICE, etc., in which case its
         * cardinality is governed by the #of groups output by the aggregation
         * and/or by the OFFSET/LIMIT).
         * 
         * FIXME The static optimizer does not take responsibility for all kinds
         * of IJoinNode. It needs to handle UnionNode, JoinGroupNode,
         * SubqueryRoot, NamedSubqueryInclude, and ServiceNode, not just
         * StatementPatternNode. In many cases, it can do this by running the
         * optimization as if the sub-groups were flattened or as-if the where
         * clause of a SubqueryRoot were flattened into the query. It must treat
         * the variables bound by a NamedSubqueryInclude or a ServiceNode, but
         * it should not attempt to reorder those join nodes when ordering the
         * joins in an parent join group.
         * 
         * FIXME The static optimizer needs to be aware of the effective range
         * count which results from a datatype and/or range constraints and
         * treat those variables as "somewhat" bound. The importance of the
         * range constraint depends on the ordinal index of the key component.
         * For example, a datatype or value range constraint on the OS(C)P index
         * is much more selective than a fully unbound SP, but is significantly
         * less selective than a 1-bound SP. The actual range count can be
         * determined when the SP appears first in the join order, but it must
         * be summed over the union of the ground datatypes and value range
         * constraints for the SP. (This may require reasoning about the
         * datatypes and value range constraints before the static join
         * optimizer runs followed by compilation of the appropriate datatype
         * and range constraint after the join order has been fixed.)
         */
        add(new ASTStaticJoinOptimizer());

        /**
         * No optimization, just guarantee that the order of FILTERs and nodes
         * with special semantics gets right. We apply this step only in case
         * the query hint to enable the old optimizer is turned off.
         */
        if (!QueryHints.DEFAULT_OLD_JOIN_ORDER_OPTIMIZER)
           add(new ASTJoinGroupOrderOptimizer(true /* assertCorrectnessOnly */));

        /*
         * The joins are now ordered. Everything from here down MUST NOT change
         * the join order when making changes to the join groups and MAY rely on
         * the join order to make decisions about filter attachment, whether a
         * variable will be needed outside of a sub-group context, etc.
         */
        
        /**
         * Uses the query hints RUN_FIRST and RUN_LAST to rearrange IJoinNodes.
         */
        add(new ASTRunFirstRunLastOptimizer());
        
        /**
         * Optimizer attaches FilterNodes which will run as "join filters" to
         * StatementPatternNodes.
         */
        add(new ASTAttachJoinFiltersOptimizer());
        
        /**
         * Rewrite each join group having two or more complex optionals as named
         * subqueries. This optimizer proceeds in two steps.
         * 
         * (1) Any required joins before the first such complex optional are
         * lifted out into a single named subquery.
         * 
         * (2) Each complex optional is then turned into a named subquery. Any
         * intermediate variables used solely within the complex optional group
         * are NOT projected out of the named subquery (variable pruning).
         * 
         * Each such named subquery will INCLUDE either:
         * 
         * (a) the named subquery lifted out in step (1) (if the complex
         * optionals are independent)
         * 
         * -or-
         * 
         * (b) the result of the previous named subquery (if they complex
         * optionals must be fed into one another).
         * 
         * Note: Sub-groups, including complex optionals, are handled using a
         * hash join pattern even without this optimizer, but they are not
         * lifted into named subqueries unless this optimizer runs and
         * intermediate variables used solely during the complex optional group
         * are not eliminated from the query.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/397
         */
//        add(new ASTComplexOptionalOptimizer());

        /**
         * Rewrites join groups having one or more joins which would involve a
         * full cross product as hash joins of sub-groups. This handles queries
         * such as BSBM Q5 by, in effect, "pushing down" sub-groups.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/253
         * 
         *      FIXME The blocking issue is now that the static join order
         *      optimizer does not order the sub-groups and sub-selects within
         *      the parent group, but only the SPs within each group. The
         *      ASTHashJoinOptimizer needs to run before the static join order
         *      optimizer since, it were to run after, it could attempt to push
         *      down SPs into subgroups which could create a very bad ordering
         *      in the parent.
         */
//        add(new ASTHashJoinOptimizer());

//        /**
//         * Recognizes cases where intermediate variables are not required
//         * outside of a group and rewrites the group into a subquery having a
//         * projection which does not include the intermediate variables.
//         * 
//         * Note: The sub-group and sub-query evaluation plans are nearly
//         * identical (they are both based on building a hash index, flooding the
//         * sub-group/query with the solutions from the hash index, and then
//         * joining the solutions from the sub-group/query back against the hash
//         * index). The advantage of turning the group into a sub-query is that
//         * we can eliminate variables using the projection of the subquery. So,
//         * the subquery is used simply for the added flexibility rather than
//         * extending the AST to allow a PROJECTION for a sub-group.
//         * 
//         * TODO There is a lot of overlap with the ASTHashJoinOptimizer. That
//         * optimizer is responsible for creating sub-groups when there are
//         * intermediate variables in a join group which could be eliminated if
//         * we move some of the joins into a subgroup and then rewrite the
//         * subgroup as a subquery in which the intermediate variables are not
//         * projected out of the subquery.
//         */
////        add(new ASTSubgroupProjectionOptimizer());
        
        /**
         * Lift {@link SubqueryRoot}s into named subqueries when appropriate or
         * necessary.
         * 
         * TODO In fact, we could do "as-bound" evaluation of sub-selects with a
         * SLICE if we issued the sub-select as a full IRunningQuery using the
         * same hash join pattern. (We can also achieve essentially the same
         * result by feeding the result of one named subquery into another.)
         */
        add(new ASTSparql11SubqueryOptimizer());

        /**
         * Validates named subquery / include patterns, identifies the join
         * variables, and annotates the named subquery root and named subquery
         * include with those join variables.
         * 
         * TODO This should probably recognize the pattern of INCLUDEs which
         * licenses a MERGE JOIN. Or we can just do that when we generate the
         * query plan (might be simpler since we do not have to mess with the
         * INCLUDE AST).
         */
        add(new ASTNamedSubqueryOptimizer());
        
        /**
         * Prepare Mapgraph GPU acceleration of join groups.
         */
        addGPUAccelerationOptimizer();
        
        /**
         * Identify and assign the join variables to sub-groups.
         */
        add(new ASTSubGroupJoinVarOptimizer());
    }

    /**
     * Tries to add the GPU-based {@link ASTRangeCountOptimizer}. If adding
     * this optimizer fails, this method adds {@link ASTRangeCountOptimizer}.
     *
     * @see https://github.com/SYSTAP/bigdata-gpu/issues/23
     */
    protected void addRangeCountOptimizer() {

        /*
         * Note: We have not implemented the GPU based range count optimizer
         * yet. When that operator is implemented, this code should be modified
         * to conditionally instantiate the GPU based triple pattern range 
         * count optimizer.
         * 
         * See https://github.com/SYSTAP/mapgraph-operators/issues/7 Implement
         * operator to obtain range counts for a set of triple patterns
         */
       final IASTOptimizer o = null; // initGPURangeCountOptimizer();
//     final IASTOptimizer o = mapgraphOptimizers.rangeCountOptimizer;
       if ( o != null ) {
          add(o);
       } else {
          add(new ASTRangeCountOptimizer());
       }

    }

    /**
     * Tries to add the GPU-based {@link ASTFastRangeCountOptimizer}. If adding
     * this optimizer fails, this method adds {@link ASTFastRangeCountOptimizer}.
     *
     * @see https://github.com/SYSTAP/bigdata-gpu/issues/101
     */
    protected void addFastRangeCountOptimizer() {

        final IASTOptimizer o = mapgraphOptimizers.fastRangeCountOptimizer;
        if ( o != null ) {
           add(o);
        } else {
           add(new ASTFastRangeCountOptimizer());
        }

    }

    /**
     * Tries to add the {@link IASTOptimizer} for using GPUs.
     */
    protected void addGPUAccelerationOptimizer() {
        final IASTOptimizer o = mapgraphOptimizers.gpuAccelerationOptimizer;
        if (o != null ) {
           add(o);
        }
    }    
    
    
    /**
     * Helper class for one-time static initialization of the mapgraph
     * optimizers.
     *
     * @author bryan
     */
    private static class MapgraphOptimizers {
        
        private final IASTOptimizer rangeCountOptimizer;
        private final IASTOptimizer fastRangeCountOptimizer;
        private final IASTOptimizer gpuAccelerationOptimizer;

        /**
         * Tries to create the GPU-based {@link ASTRangeCountOptimizer}; returns
         * <code>null</code> if the attempt fails.
         *
         * @see https://github.com/SYSTAP/bigdata-gpu/issues/23
         */
        protected IASTOptimizer initGPURangeCountOptimizer() {

           return ClassPathUtil.classForName(//
                 "com.blazegraph.rdf.gpu.sparql.ast.optimizers.ASTGPURangeCountOptimizer", // preferredClassName,
                 null, // defaultClass,
                 IASTOptimizer.class, // sharedInterface,
                 getClass().getClassLoader() // classLoader
           );

        }

        /**
         * Tries to create the GPU-based {@link ASTFastRangeCountOptimizer};
         * returns <code>null</code> if the attempt fails.
         *
         * @see https://github.com/SYSTAP/bigdata-gpu/issues/101
         */
        protected IASTOptimizer initGPUFastRangeCountOptimizer() {

           return ClassPathUtil.classForName(//
                 "com.blazegraph.rdf.gpu.sparql.ast.optimizers.ASTGPUFastRangeCountOptimizer", // preferredClassName,
                 null, // defaultClass,
                 IASTOptimizer.class, // sharedInterface,
                 getClass().getClassLoader() // classLoader
           );

        }

        /**
         * Tries to create the {@link IASTOptimizer} for using GPUs; returns
         * <code>null</code> if the attempt fails.
         */
        protected IASTOptimizer initGPUAccelerationOptimizer() {

            return ClassPathUtil.classForName(//
                    "com.blazegraph.rdf.gpu.sparql.ast.optimizers.ASTGPUAccelerationOptimizer", // preferredClassName,
                    null, // defaultClass,
                    IASTOptimizer.class, // sharedInterface,
                    getClass().getClassLoader() // classLoader
            );

        }

        MapgraphOptimizers() {
            rangeCountOptimizer = initGPURangeCountOptimizer();
            fastRangeCountOptimizer = initGPUFastRangeCountOptimizer();
            gpuAccelerationOptimizer = initGPUAccelerationOptimizer();
        }
        
    }
    

}
