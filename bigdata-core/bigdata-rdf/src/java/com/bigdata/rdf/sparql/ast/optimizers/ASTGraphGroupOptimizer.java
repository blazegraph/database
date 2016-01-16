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

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpJoins;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.eval.DataSetSummary;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Handles a variety of special constructions related to graph graph groups.
 * 
 * <dl>
 * <dt>GRAPH ?foo</dt>
 * <dd>
 * Anything nested (even if a subquery) is constrained to be from
 * <code>?foo</code>. All nested statement patterns must have <code>?foo</code>
 * as their context, even if they occur within a subquery. (This is not true for
 * a named subquery which just projects its solutions but does not inherit the
 * parent's graph context. However, if we lifted the named subquery out, e.g.,
 * for bottom up evaluation semantics, then we need to impose the GRAPH
 * constraint on the named subquery which means running this optimizer before
 * the one which lifts out the named subquery.)</dd>
 * <dt>GRAPH ?foo { GRAPH ?bar } }</dt>
 * <dd>The easy way to enforce this constraint when there are nested graph
 * patterns is with a <code>SameTerm(?foo,?bar)</code> constraint inside of the
 * nested graph pattern.
 * <p>
 * The problem with this is that it does not enforce the constraint as soon as
 * possible under some conditions. A rewrite of the variable would have that
 * effect but the rewrite needs to be aware of variable scope rules so we do not
 * rewrite the variable within a subquery if it is not projected by that
 * subquery. We would also have to add a BIND(?foo AS ?bar) to make ?bar visible
 * in the scope of parent groups.
 * <p>
 * However, there is an INCLUDE problem too. That could be handled by moving the
 * INCLUDE into a subgroup with a BIND to renamed the variable or by adding a
 * "projection" to the INCLUDE so we could rename the variable there.
 * <p>
 * Since this construction of nested graph patterns is rare, and since it is
 * complicated to make it more efficient, we are going with the SameTerm()
 * constraint for now.</dd>
 * <dt>GRAPH uri</dt>
 * <dd>
 * This is only allowed if the uri is in the named data set (or if no data set
 * was given). Translation time error.</dd>
 * <dt>GRAPH uri { ... GRAPH uri2 ... }</dt>
 * <dd>It is an query error if a <code>GRAPH uri</code> is nested within another
 * <code>GRAPH uri</code> for distinct IRIs.</dd>
 * <dt>GRAPH uri { ... GRAPH ?foo ... }</dt>
 * <dd>The outer graph imposes a constant constraint. The inner graph needs to
 * inherit that constraint. Either a SameTerm() constraint must be added to the
 * inner graph or context for the inner graph could be rewritten using
 * Constant/2. Again, this is an optimization which may not contribute much
 * value except in very rare cases. Unlike the case below, we do need to impose
 * a SameTerm() constraint to make this case correct.</dd>
 * <dt>GRAPH ?foo { ... GRAPH uri ... }</dt>
 * <dd>If a constant is nested within a <i>non-optional</i>
 * <code>GRAPH uri</code> then that constant could be lifted up and bound using
 * Constant/2 on the outer graph pattern. Again, this is an optimization which
 * may not contribute much value except in very rare cases. We do not need to do
 * anything additional to make this case correct.</dd>
 * <dt>GRAPH ?g {}</dt>
 * <dd>This matches the distinct named graphs in the named graph portion of the
 * data set (special case). There are several variations on this which need to
 * be handled:
 * <ul>
 * <li>If ?g might be bound or is not bound:
 * <ul>
 * <li>If there is no data set, then this should be translated into
 * sp(_,_,_,?g)[filter=distinct] that should be recognized and evaluated using a
 * distinct term advancer on CSPO.</li>
 * <li>If the named graphs are explicitly given, then annotate
 * {@link StatementPatternNode} with an "IN" for <code>(?g,namedGraphs)</code>.</li>
 * </ul>
 * Either way, if there is a filter then apply the filter to the scan/list (this
 * happens in AST2BOPUtility#toPredicate()).</li>
 * <li>If <code>?g</code> is known bound coming into <code>graph ?g {}</code>
 * then we want to test for the existence of at least one statement on the CSPO
 * index for <code>?g</code>.This is basically ASK sp(_,_,_,uri) LIMIT 1, but we
 * must run this for each binding on <code>?g</code>.</li>
 * </ul>
 * </dd>
 * <dt>GRAPH uri {}</dt>
 * <dd>This is an existence test for the graph. This is a CSPO iterator with C
 * bound and a limit of one. Lift this into a named subquery since we only want
 * to run it once. (This is basically ASK sp(_,_,_,uri) LIMIT 1.)</dd>
 * </dl>
 * 
 * Note: This optimizer MUST run before optimizers which lift out named
 * subqueries in order to correctly impose the GRAPH constraints on the named
 * subquery.
 * 
 * @see ASTEmptyGroupOptimizer, which handles <code>{}</code> for non-GRAPH
 *      groups.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTEmptyGroupOptimizer.java 5177 2011-09-12 17:49:44Z
 *          thompsonbry $
 * 
 *          TODO If <code>?g</code> can be statically analyzed as being bound to
 *          a specific constant then we would rewrite <code>?g</code> using
 *          Constant/2 and then handle this as <code>GRAPH uri {}</code>
 *          <p>
 *          This is basically what {@link AST2BOpJoins} does when it follows the
 *          decision tree for named and default graphs. So, maybe that logic can
 *          be lifted into this class as a rewrite?
 */
public class ASTGraphGroupOptimizer implements IASTOptimizer {

    // private static final Logger log = Logger
    // .getLogger(ASTGraphGroupOptimizer.class);

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     


        if (!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        // The data set node (if any).
        final DatasetNode dataSet = queryRoot.getDataset();

        {

            // WHERE clause for named subqueries.
            if (queryRoot.getNamedSubqueries() != null) {

                for (NamedSubqueryRoot namedSubquery : queryRoot
                        .getNamedSubqueries()) {

                    visitGroups(context, dataSet,
                            namedSubquery.getWhereClause(), null/* context */);
                }

            }

            // Top-level WHERE clause.
            visitGroups(context, dataSet, queryRoot.getWhereClause(),
                    null/* context */);


        }

        return new QueryNodeWithBindingSet(queryNode, bindingSets);
        
    }

    /**
     * Visit groups, applying and verifying GRAPH constraints.
     * <p>
     * Note: This will NOT visit stuff inside of SERVICE calls. If those graph
     * patterns get rewritten it has to be by the SERVICE, not us.
     * <p>
     * Note: This <em>will</em> visit stuff inside of subqueries. A GRAPH
     * constraint outside of a subquery applies within the subquery as well.
     * 
     * @param context
     * @param dataSet
     * @param group
     * @param graphContext
     * @param parent
     */
    @SuppressWarnings("unchecked")
    private void visitGroups(
            //
            final IEvaluationContext context,//
            final DatasetNode dataSet,//
            final IGroupNode<IGroupMemberNode> group, //
            TermNode graphContext//
            ) {

        if (group instanceof JoinGroupNode && group.getContext() != null) {

            final TermNode innerGraphContext = group.getContext();

            if (innerGraphContext.isConstant()) {

                /*
                 * If there is a named graphs data set, then verify that the
                 * given URI is a member of that data set.
                 */

                assertGraphInNamedDataset(
                        (BigdataURI) ((TermNode) innerGraphContext).getValue(),
                        dataSet);

            }

            if (graphContext == null) {

                /*
                 * Top-most GRAPH group in this part of the query.
                 */

                graphContext = innerGraphContext;

            } else {

                /*
                 * There is an existing GRAPH context.
                 * 
                 * Make sure the constraints are compatible and/or enforced.
                 */

                if (graphContext.isConstant() && innerGraphContext.isConstant()) {

                    /*
                     * GRAPH uri { ... GRAPH uri { ... } ... }
                     */

                    assertSameURI(graphContext, innerGraphContext);

                } else if (graphContext.isVariable()
                        && innerGraphContext.isVariable()
                        && !graphContext.equals(innerGraphContext)) {

                    /*
                     * GRAPH ?foo { ... GRAPH ?bar { ... } ... }
                     * 
                     * Adds a SameTerm(foo,bar) constraint to the inner GRAPH
                     * pattern.
                     */

                    final FilterNode filterNode = new FilterNode(
                            FunctionNode.sameTerm(graphContext,
                                    innerGraphContext));

                    final GlobalAnnotations globals = new GlobalAnnotations(
                    		context.getLexiconNamespace(),
                    		context.getTimestamp()
                    		);
                    
                    AST2BOpUtility.toVE(context.getBOpContext(), globals,
                            filterNode.getValueExpressionNode());

                    group.addChild(filterNode);

                }

                /*
                 * TODO GRAPH ?foo { ... GRAPH uri ... } could be handled here
                 * (optimization, not correctness).
                 */

            }

            /**
             * Handle edge cases GRAPH ?g { } and GRAPH <uri>, which require
             * special handling (if not rewritten, they will return wrong
             * results in some cases).
             */
            if (group.isEmpty() && graphContext.isVariable()) {

               // our approach is to wrap around a dummy graph pattern with
               // a distinct term scan annotation
               final StatementPatternNode sp = 
                  new StatementPatternNode(
                     VarNode.freshVarNode(),
                     VarNode.freshVarNode(),
                     VarNode.freshVarNode(), 
                     graphContext, Scope.NAMED_CONTEXTS);
               sp.setDistinctTermScanVar((VarNode)graphContext);

               group.addChild(sp);
               
            } else if (group.isEmpty() && graphContext.isConstant()) {
               
               /**
                *  We need to verify that there is one or more stmt in that
                *  named graph. We do that using an ASK subquery.
                *  
                *  Note that it is *not* safe to drop the whole construct, even
                *  if we statically detect that the graphContext IV is not in
                *  the dictionary. As a counter example, consider the query
                *  
                *  SELECT * {
                *    GRAPH <http://uri.not.in.dictionary> { }
                *  }
                *  
                *  The expected result is the empty set, but when removing the
                *  GRAPH pattern completely, we get SELECT * WHERE {}, which
                *  gives the empty binding set as result. Catching cases where
                *  the pattern can be dropped seems not worth the effort, in
                *  particular considering that the ASK query for the simple
                *  pattern should be quite efficient anyways.
                */
               final StatementPatternNode sp = 
                  new StatementPatternNode(
                     VarNode.freshVarNode(),
                     VarNode.freshVarNode(),
                     VarNode.freshVarNode(), 
                     graphContext, Scope.NAMED_CONTEXTS);
                     
               final SubqueryRoot subquery = new SubqueryRoot(QueryType.ASK);
               final ProjectionNode projection = new ProjectionNode();
               subquery.setProjection(projection);
               subquery.addArg(new JoinGroupNode(sp));
               group.addChild(sp);                  
               
            }

        }

        /*
         * Visit all direct children of this group.
         * 
         * Note: The group might not be a GRAPH, but context will be non-null if
         * the group is bounded by a GRAPH.
         */
        for (IGroupMemberNode child : group) {

            if (child instanceof ServiceNode) {

                /*
                 * Do NOT translate SERVICE nodes (unless they are a bigdata
                 * service).
                 */

                continue;

            }

            if (graphContext != null) {

                if (child instanceof StatementPatternNode) {

                    /*
                     * All statement patterns within a GRAPH {...} MUST have a
                     * constraint on [c] and MUST specify NAMED_CONTEXTS as
                     * their scope.
                     */

                    final StatementPatternNode sp = (StatementPatternNode) child;

                    final Scope scope = sp.getScope();

                    if (scope == null) {

                        // This is a required annotation.
                        throw new AssertionError("No scope? " + sp);

                    }

                    switch (scope) {
                    case NAMED_CONTEXTS:
                        break;
                    case DEFAULT_CONTEXTS:
                        throw new AssertionError(
                                "Statement pattern bounded by GRAPH but has default context scope: "
                                        + sp);
                    }

                    if (sp.c() == null) {

                        /*
                         * Impose the context if it is missing.
                         * 
                         * TODO Should it be an error if this is not bound? Who
                         * really has responsibility for attaching the [c]
                         * constraint? The code generating the SP or this code?
                         */
                        sp.setArg(3/* c */, graphContext);

                    }

                }

            }

            if (!(child instanceof IGroupNode<?>))
                continue;

            /*
             * Recursion.
             */
            visitGroups(context, dataSet, (IGroupNode<IGroupMemberNode>) child,
                    graphContext);

        }

    }

    /**
     * Assert that the contexts are the same URI.
     * 
     * @param context
     * @param innerContext
     */
    private void assertSameURI(final TermNode context,
            final TermNode innerContext) {

        // GRAPH uri1 { ... GRAPH uri2 {...} ... }
        if (!context.getValue().equals(innerContext.getValue())) {

            // uri1 != uri2
            throw new InvalidGraphContextException("Conflicting GRAPH IRIs: "
                    + context + " and " + innerContext.getValue());

        }

    }

    /**
     * Assert that the given URI is in the named data set.
     * 
     * @param uri
     *            A URI.
     * @param dataSet
     *            The dataset.
     */
    private void assertGraphInNamedDataset(final BigdataURI uri,
            final DatasetNode dataSet) {

        if (dataSet == null) {
            /*
             * The data set was not explicitly specified.
             */
            return;
        }

        if (uri == null)
            throw new IllegalArgumentException();

        final DataSetSummary namedGraphs = dataSet.getNamedGraphs();

        if (namedGraphs == null) {

            /*
             * No constraint on the named graphs (or just a filter, which will
             * get applied at runtime).
             */

            return;

        }

        // GRAPH uri
        if (!namedGraphs.getGraphs().contains(uri.getIV())) {

            // uri is not in the named graphs.
//            throw new RuntimeException("URI not in named graphs: " + uri);

        }

    }

}
