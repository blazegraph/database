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
 * Created on Oct 26, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryFunctionNodeBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.hints.IQueryHint;
import com.bigdata.rdf.sparql.ast.hints.QueryHintException;
import com.bigdata.rdf.sparql.ast.hints.QueryHintRegistry;
import com.bigdata.rdf.sparql.ast.hints.QueryHintScope;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Query hints are identified applied to AST nodes based on the specified scope
 * and the location within the AST in which they are found. Query hints
 * recognized by this optimizer have the form:
 * 
 * <pre>
 * scopeURL propertyURL value
 * </pre>
 * 
 * Where <i>scope</i> is any of the {@link QueryHintScope}s; and <br/>
 * Where <i>propertyURL</i> identifies the query hint;<br/>
 * Where <i>value</i> is a literal.
 * <p>
 * Once recognized, query hints are removed from the AST. All query hints are
 * declared internally using interfaces. It is an error if the specified query
 * hint has not been registered with the {@link QueryHintRegistry}, if the
 * {@link IQueryHint} can not validate the value, if the {@link QueryHintScope}
 * is not legal for the {@link IQueryHint}, etc.
 * <p>
 * For example:
 * 
 * <pre>
 * ...
 * {
 *    # query hint binds for this join group.
 *    hint:Group hint:com.bigdata.bop.PipelineOp.maxParallel 10
 *    
 *    ...
 *    
 *    # query hint binds for the next basic graph pattern in this join group.
 *    hint:Prior hint:com.bigdata.relation.accesspath.IBuffer.chunkCapacity 100
 *    
 *    ?x rdf:type foaf:Person .
 * }
 * </pre>
 * 
 * @see QueryHints
 * @see QueryHintScope
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ASTQueryHintOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTQueryHintOptimizer.class);
    
    @SuppressWarnings("unchecked")
    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     


        final QueryRoot queryRoot = (QueryRoot) queryNode;

        if (context.queryHints != null && !context.queryHints.isEmpty()) {

            // Apply any given query hints globally.
            applyGlobalQueryHints(context, queryRoot, context.queryHints);
            
        }
        
        // First, process any pre-existing named subqueries.
        {
            
            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            if (namedSubqueries != null) {

                for (NamedSubqueryRoot namedSubquery : namedSubqueries) {

                    processGroup(context, queryRoot, namedSubquery,
                            namedSubquery.getWhereClause());

                }

            }

        }

        // Now process the main where clause.
        processGroup(context, queryRoot, queryRoot, queryRoot.getWhereClause());

        return new QueryNodeWithBindingSet(queryNode, bindingSets);

    }

    /**
     * Tnis is used to avoid descent into parts of the AST which can not have
     * query hints, such as value expressions.
     */
    private boolean isNodeAcceptingQueryHints(final BOp op) {
      
        /**
         * Note: Historically, only visited QueryNodeBase, but that does not let
         * query hints be applied to a variety of relevant AST nodes, including
         * the SliceOp. The new pattern is to exclude those parts of the AST
         * interface heirarchy that should not have query hints applied, which
         * is basically the value expressions.
         * 
         * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
         *      Clean up query hints </a>
         */
        
        if (op instanceof IValueExpressionNode) {
            
            // Skip value expressions.
            return false;

        }

        if (op instanceof IValueExpression) {

            // Skip value expressions.
            return false;

        }

        // Visit anything else.
        return true;
        
    }
    
    /**
     * Applies the global query hints to each node in the query.
     * 
     * @param queryRoot
     * @param queryHints
     */
    private void applyGlobalQueryHints(final AST2BOpContext context,
            final QueryRoot queryRoot, final Properties queryHints) {

        if (queryHints == null || queryHints.isEmpty())
            return;
        
//        validateQueryHints(context, queryHints);
        
        final Iterator<BOp> itr = BOpUtility
                .preOrderIteratorWithAnnotations(queryRoot);

        // Note: working around a ConcurrentModificationException.
        final List<ASTBase> list = new LinkedList<ASTBase>();
        
        while (itr.hasNext()) {

            final BOp op = itr.next();

            if (!isNodeAcceptingQueryHints(op))
                continue;

            final ASTBase t = (ASTBase) op;

            list.add(t);

        }

        for (ASTBase t : list) {

            applyQueryHints(context, queryRoot, QueryHintScope.Query, t,
                    queryHints);

        }
        
    }

    /**
     * Apply each query hint in turn to the AST node.
     * 
     * @param t
     *            The AST node.
     * @param queryHints
     *            The query hints.
     */
    private void applyQueryHints(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase t,
            final Properties queryHints) {

        /*
         * Apply the query hints to the node.
         */

        @SuppressWarnings("rawtypes")
        final Enumeration e = queryHints.propertyNames();

        while (e.hasMoreElements()) {

            final String name = (String) e.nextElement();

            final String value = queryHints.getProperty(name);

            _applyQueryHint(context, queryRoot, scope, t, name, value);

        }
        
    }
    
    /**
     * Apply the query hint to the AST node.
     * 
     * @param t
     *            The AST node to which the query hint will be bound.
     * @param name
     *            The name of the query hint.
     * @param value
     *            The value.
     */
    @SuppressWarnings("unchecked")
    private void _applyQueryHint(final AST2BOpContext context, final QueryRoot queryRoot,
            final QueryHintScope scope, final ASTBase t, final String name,
            final String value) {

        @SuppressWarnings("rawtypes")
        final IQueryHint queryHint = QueryHintRegistry.get(name);
        
        if (queryHint == null) {
            
            // Unknown query hint.
            throw new QueryHintException(scope, t, name, value);
            
        }

        // Parse/validate and handle type conversion for the query hint value.
        final Object value2 = queryHint.validate(value);

        if (log.isTraceEnabled())
            log.trace("Applying hint: hint=" + queryHint.getName() + ", value="
                    + value2 + ", node=" + t.getClass().getName());

        // Apply the query hint.
        queryHint.handle(context, queryRoot, scope, t, value2);

//        if (scope == QueryHintScope.Query) {
//            /*
//             * All of these hints either are only permitted in the Query scope
//             * or effect the default settings for the evaluation of this query
//             * when they appear in the Query scope. For the latter category, the
//             * hints may also appear in other scopes. For example, you can
//             * override the access path sampling behavior either for the entire
//             * query, for all BGPs in some scope, or for just a single BGP.
//             */
//            if (name.equals(QueryHints.ANALYTIC)) {
//                context.nativeHashJoins = Boolean.valueOf(value);
//                context.nativeDistinctSolutions = Boolean.valueOf(value);
//                context.nativeDistinctSPO = Boolean.valueOf(value);
//            } else if (name.equals(QueryHints.MERGE_JOIN)) {
//                context.mergeJoin = Boolean.valueOf(value);
//            } else if (name.equals(QueryHints.NATIVE_HASH_JOINS)) {
//                context.nativeHashJoins = Boolean.valueOf(value);
//            } else if (name.equals(QueryHints.NATIVE_DISTINCT_SOLUTIONS)) {
//                context.nativeDistinctSolutions = Boolean.valueOf(value);
//            } else if (name.equals(QueryHints.NATIVE_DISTINCT_SPO)) {
//                context.nativeDistinctSPO = Boolean.valueOf(value);
//            } else if (name.equals(QueryHints.REMOTE_APS)) {
//                context.remoteAPs = Boolean.valueOf(value);
//            } else if (name.equals(QueryHints.ACCESS_PATH_SAMPLE_LIMIT)) {
//                context.accessPathSampleLimit = Integer.valueOf(value);
//            } else if (name.equals(QueryHints.ACCESS_PATH_SCAN_AND_FILTER)) {
//                context.accessPathScanAndFilter = Boolean.valueOf(value);
//            } else {
//                t.setQueryHint(name, value);
//            }
//        } else {
//            t.setQueryHint(name, value);
//        }

    }
    
//    /**
//     * Validate the global query hints.
//     * 
//     * @param queryHints
//     */
//    private void validateQueryHints(final AST2BOpContext context,
//            final Properties queryHints) {
//
//        final Enumeration<?> e = queryHints.propertyNames();
//
//        while (e.hasMoreElements()) {
//
//            final String name = (String) e.nextElement();
//
//            final String value = queryHints.getProperty(name);
//
//            validateQueryHint(context, name, value);
//            
//        }
//        
//    }

    /**
     * Recursively process a join group, applying any query hints found and
     * removing them from the join group.
     * 
     * @param context
     * @param queryRoot
     *            The {@link QueryRoot}.
     * @param group
     *            The join group.
     */
    @SuppressWarnings("unchecked")
    private void processGroup(final AST2BOpContext context,
            final QueryRoot queryRoot, final QueryBase queryBase,
            final GraphPatternGroup<IGroupMemberNode> group) {

        if (group == null)
            return;
        
        /*
         * Note: The loop needs to be carefully written as query hints will be
         * removed after they are processed. This can either be done on a hint
         * by hint basis or after we have processed all hints in the group. It
         * is easier to make the latter robust, so this uses a second pass over
         * each group in which a query hint was identified to remove the query
         * hints once they have been interpreted.
         */
        
        /*
         * Identify and interpret query hints.
         */
        // #of query hints found in this group.
        int nfound = 0;
        {

            final int arity = group.arity();
            
            // The previous AST node which was not a query hint. 
            ASTBase prior = null;

            for (int i = 0; i < arity; i++) {
            
                final IGroupMemberNode child = (IGroupMemberNode) group.get(i);

                if (child instanceof StatementPatternNode) {

                    // Look for a query hint.
                    final StatementPatternNode sp = (StatementPatternNode) child;

                    if (!isQueryHint(sp)) {
                        // Not a query hint.
                        prior = sp;
                        continue;
                    }

                    // Apply query hint.
                    applyQueryHint(context, queryRoot, queryBase, group, prior,
                            sp);

                    nfound++;
                    
                    continue;

                }

                /*
                 * Recursion (looking for query hints to be applied).
                 */
                if(child instanceof GraphPatternGroup<?>) {
                    processGroup(context, queryRoot, queryBase,
                            (GraphPatternGroup<IGroupMemberNode>) child);
                } else if (child instanceof SubqueryRoot) {
                    processGroup(context, queryRoot, (SubqueryRoot)child,
                            ((SubqueryRoot) child).getWhereClause());
                } else if (child instanceof ServiceNode) {
                    processGroup(context, queryRoot, queryBase,
                            ((ServiceNode) child).getGraphPattern());
                } else if (child instanceof FilterNode
                        && ((FilterNode) child).getValueExpressionNode() instanceof SubqueryFunctionNodeBase) {
                    /**
                     * @see <a href="http://trac.blazegraph.com/ticket/990"> Query hint not recognized in FILTER</a>
                     */
                    final GraphPatternGroup<IGroupMemberNode> filterGraphPattern = ((SubqueryFunctionNodeBase) ((FilterNode) child)
                            .getValueExpressionNode()).getGraphPattern();

                    processGroup(context, queryRoot, queryBase,
                            filterGraphPattern);
                    
                }
                
                prior = (ASTBase) child;
                
            }
            
        }

        /*
         * Remove the query hints from this group.
         */
        if (nfound > 0) {
            
            for (int i = 0; i < group.arity(); ) {

                final IGroupMemberNode child = (IGroupMemberNode) group.get(i);

                if (!(child instanceof StatementPatternNode)) {
                    i++;
                    continue;
                }

                final StatementPatternNode sp = (StatementPatternNode) child;

                if (isQueryHint(sp)) {
                    // Remove and redo the loop.
                    group.removeArg(sp);
                    continue;
                }
                
                // Next child.
                i++;
                
            }

        }
        
    }

    /**
     * Return <code>true</code> iff this {@link StatementPatternNode} is a query
     * hint. This method checks the namespace of the predicate. All query hints
     * MUST start with the <code>hint:</code> namespace, which is given by
     * {@link QueryHints#NAMESPACE}.
     * <p>
     * Within that namespace we have public query hints whose localName is
     * declared in the {@link QueryHints} interface, e.g.,
     * {@link QueryHints#CHUNK_SIZE} which appears as follows in a query:
     * 
     * <pre>
     * hint:chunkSize
     * </pre>
     * 
     * We also have non-public, fully qualified, query hints, e.g.,
     * 
     * <pre>
     * hint:com.bigdata.relation.accesspath.IBuffer.chunkCapacity
     * </pre>
     * 
     * In both cases, an {@link IQueryHint} is registered with the
     * {@link QueryHintRegistry}. The "non-public" query hints are simply not
     * declared by the {@link QueryHints} interface and are intended more for
     * internal development and performance testing rather than as generally
     * useful query hints.
     * 
     * @param sp
     *            The {@link StatementPatternNode}.
     * 
     * @return <code>true</code> if the SP is a query hint.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
     *      Clean up query hints </a>
     */
    private boolean isQueryHint(final StatementPatternNode sp) {
        
        if(!(sp.p() instanceof ConstantNode))
            return false;
        
        final BigdataValue p = ((ConstantNode) sp.p()).getValue();
        
        if(!(p instanceof URI))
            return false;
        
        final BigdataURI u = (BigdataURI) p;
        
        final String str = u.stringValue();
        
        if (str.startsWith(QueryHints.NAMESPACE)) {
            
            // A possible query hint.
            return true;
            
        }

        return false;

    }
    
    /**
     * Extract and return the {@link QueryHintScope}.
     * 
     * @param t
     *            The subject position of the query hint
     *            {@link StatementPatternNode}.
     * 
     * @return The {@link QueryHintScope}.
     * 
     * @throws RuntimeException
     *             if something goes wrong.
     * @throws IllegalArgumentException
     *             if something goes wrong.
     */
    private QueryHintScope getScope(final TermNode t) {

        if(!(t instanceof ConstantNode))
            throw new RuntimeException(
                    "Subject position of query hint must be a constant.");

        final BigdataValue v = ((ConstantNode) t).getValue();

        if (!(v instanceof BigdataURI))
            throw new RuntimeException("Query hint scope is not a URI.");

        final BigdataURI u = (BigdataURI) v;

        return QueryHintScope.valueOf(u);

    }

    /**
     * Extract, validate, and return the name of the query hint property.
     * 
     * @param t
     *            The predicate position of the query hint
     *            {@link StatementPatternNode}.
     * 
     * @return The name of the query hint property.
     */
    private String getName(final TermNode t) {

        if (!(t instanceof ConstantNode)) {
            throw new RuntimeException(
                    "Predicate position of query hint must be a constant.");
        }

        final BigdataValue v = ((ConstantNode) t).getValue();

        if (!(v instanceof BigdataURI))
            throw new RuntimeException("Predicate position of query hint is not a URI.");

        final BigdataURI u = (BigdataURI) v;

        final String name = u.getLocalName();
        
        return name;
        
    }
    
    /**
     * Return the value for the query hint.
     * 
     * @param t
     * @return
     */
    private String getValue(final TermNode t) {

//        if (!(t instanceof ConstantNode)) {
//            throw new RuntimeException(
//                    "Object position of query hint must be a constant.");
//        }
        
        /*
         * Let the variable through as a string.
         */
        if (t instanceof VarNode) {
            return '?'+((VarNode) t).getValueExpression().getName();
        }

        final BigdataValue v = ((ConstantNode) t).getValue();

        if (!(v instanceof Literal))
            throw new RuntimeException(
                    "Object position of query hint is not a Literal.");

        final Literal lit = (Literal) v;
        
        return lit.stringValue();
        
    }
    
    /**
     * @param context
     * @param queryRoot
     * @param group
     * @param prior
     * @param s The subject position of the query hint.
     * @param o The object position of the query hint.
     */
    private void applyQueryHint(//
            final AST2BOpContext context,//
            final QueryRoot queryRoot,// top-level query
            final QueryBase queryBase,// either top-level query or subquery.
            final GraphPatternGroup<IGroupMemberNode> group,//
            final ASTBase prior, // MAY be null IFF scope != Prior
            final StatementPatternNode hint //
            ) {
        
        if(context == null)
            throw new IllegalArgumentException();
        if(queryRoot == null)
            throw new IllegalArgumentException();
        if(group == null)
            throw new IllegalArgumentException();
        if(hint == null)
            throw new IllegalArgumentException();

        final QueryHintScope scope = getScope(hint.s());

        final String name = getName(hint.p());

        final String value = getValue(hint.o());

        if (log.isInfoEnabled())
            log.info("name=" + name + ", scope=" + scope + ", value=" + value);
        
//        validateQueryHint(context, name, value);

        switch (scope) {
        case Query: {
            applyToQuery(context, queryRoot, scope, name, value);
            break;
        }
        case SubQuery: {
            applyToSubQuery(context, queryRoot, scope, queryBase, group, name, value);
            break;
        }
        case Group: {
            applyToGroup(context, queryRoot, scope, group, name, value);
            break;
        }
        case GroupAndSubGroups: {
            applyToGroupAndSubGroups(context, queryRoot, scope, group, name, value);
            break;
        }
        case Prior: {
            if (scope == QueryHintScope.Prior && prior == null) {
                throw new RuntimeException(
                        "Query hint with Prior scope must follow the AST node to which it will bind.");
            }
            // Apply to just the last SP.
            _applyQueryHint(context, queryRoot, scope, prior, name, value);
            break;
        }
        default:
            throw new UnsupportedOperationException("Unknown scope: " + scope);
        }

    }

    /**
     * @param group
     * @param name
     * @param value
     */
    private void applyToSubQuery(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope,
            final QueryBase queryBase,
            GraphPatternGroup<IGroupMemberNode> group, final String name,
            final String value) {

        /*
         * Find the top-level parent group for the given group.
         */
        GraphPatternGroup<IGroupMemberNode> parent = group;

        while (parent != null) {

            group = parent;

            parent = parent.getParentGraphPatternGroup();

        }

        // Apply to all child groups of that top-level group.
        applyToGroupAndSubGroups(context, queryRoot, scope, group, name, value);

//        if (isNodeAcceptingQueryHints(queryBase)) {

        _applyQueryHint(context, queryRoot, scope, (ASTBase) queryBase, name, value);

//        }

    }

    /**
     * Applies the query hint to the entire query.
     * 
     * @param queryRoot
     * @param name
     * @param value
     */
    private void applyToQuery(final AST2BOpContext context,
            final QueryRoot queryRoot, 
            final QueryHintScope scope, 
            final String name, final String value) {

        if (false && context.queryHints != null) {
            /**
             * Also stuff the query hint on the global context for things which
             * look there.
             * 
             * Note: HINTS: This was putting the literal given name and value of
             * the query hint. This is basically backwards. The query hints in
             * the global scope provide a default. Explicitly given query hints
             * in the query are delegated to IQueryHint implementations and then
             * applied to the appropriate AST nodes.
             * 
             * @see <a
             *      href="http://sourceforge.net/apps/trac/bigdata/ticket/791" >
             *      Clean up query hints </a>
             */
            context.queryHints.setProperty(name, value);
        }
        
        final Iterator<BOp> itr = BOpUtility
                .preOrderIteratorWithAnnotations(queryRoot);

        // Note: working around a ConcurrentModificationException.
        final List<ASTBase> list = new LinkedList<ASTBase>();
        
        while (itr.hasNext()) {

            final BOp op = itr.next();

            if (!isNodeAcceptingQueryHints(op))
                continue;

            /*
             * Set a properties object on each AST node. The properties object
             * for each AST node will use the global query hints for its
             * defaults so it can be overridden selectively.
             */

            final ASTBase t = (ASTBase) op;

            list.add(t);
            
        }
        
        for(ASTBase t : list) {

            _applyQueryHint(context, queryRoot, scope, t, name, value);
            
        }
        
    }

    /**
     * Apply the query hint to the group and, recursively, to any sub-groups.
     * 
     * @param group
     * @param name
     * @param value
     */
    @SuppressWarnings("unchecked")
    private void applyToGroupAndSubGroups(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope,
            final GraphPatternGroup<IGroupMemberNode> group, final String name,
            final String value) {

        for (IGroupMemberNode child : group) {

            _applyQueryHint(context, queryRoot, scope, (ASTBase) child, name, value);

            if (child instanceof GraphPatternGroup<?>) {

                applyToGroupAndSubGroups(context, queryRoot, scope,
                        (GraphPatternGroup<IGroupMemberNode>) child, name,
                        value);

            }

        }

//        if(isNodeAcceptingQueryHints(group)) {

        _applyQueryHint(context, queryRoot, scope, (ASTBase) group, name, value);
            
//        }

    }

    /**
     * Apply the query hint to the group.
     * 
     * @param group
     * @param name
     * @param value
     */
    private void applyToGroup(final AST2BOpContext context,
            final QueryRoot queryRoot,
            final QueryHintScope scope,
            final GraphPatternGroup<IGroupMemberNode> group, final String name,
            final String value) {

        for (IGroupMemberNode child : group) {

            _applyQueryHint(context, queryRoot, scope, (ASTBase) child, name, value);

        }

//        if (isNodeAcceptingQueryHints(group)) {

        _applyQueryHint(context, queryRoot, scope, (ASTBase) group, name, value);
            
//        }

    }

}
