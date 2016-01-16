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
 * Created on Sep 8, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.service.fts.FTS;

/**
 * Translate {@link FTS#SEARCH} and related magic predicates into a
 * {@link ServiceNode} which will invoke the bigdata search engine.
 * 
 * <pre>
 * with {
 *    select ?subj ?score
 *    where {
 *      ?res fts:search "foo" .
 *      ?res fts:endpoint "http://my.solr.endpoint"
 *      ?res fts:relevance ?score .
 *    }
 * } as %searchSet1
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public abstract class ASTSearchOptimizerBase implements IASTOptimizer {

    private static final Logger log = 
       Logger.getLogger(ASTSearchOptimizerBase.class);
    
    
    /**
     * The known search URIs.
     */
    private Set<URI> searchUrisInternal;
    
    /**
     * The known search URIs.
     */
    private String namespace;

    /**
     * The known search URIs.
     */
    private URI searchPredicate;

    protected ASTSearchOptimizerBase() {
       searchUrisInternal = getSearchUris();
       namespace = getNamespace();
       searchPredicate = getSearchPredicate();
    }
    
    /**
     * Get the search URIs fields.
     */
    abstract protected Set<URI> getSearchUris();

    /**
     * Get the namespace variables.
     */
    abstract protected String getNamespace();

    /**
     * Get the search predicate variables.
     */
    abstract protected URI getSearchPredicate();

    
    @SuppressWarnings("unchecked")
    @Override
    public QueryNodeWithBindingSet optimize(
          final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();
       
        if(!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        if (queryRoot.getNamedSubqueries() != null) {

            /*
             * Look for, validate, and rewrite magic predicates for search if
             * they appear within a named subquery.
             */
            for (NamedSubqueryRoot namedSubquery : queryRoot
                    .getNamedSubqueries()) {

                extractSearches(context, queryRoot, namedSubquery,
                        (GroupNodeBase<IGroupMemberNode>) namedSubquery
                                .getWhereClause());

            }

        }
        
        if (queryRoot.getWhereClause() != null) {
            
            /*
             * Look for, validate, and rewrite magic predicates for search if
             * they appear within the main WHERE clause.
             */
            
            extractSearches(context, queryRoot, queryRoot,
                    (GroupNodeBase<IGroupMemberNode>) queryRoot
                            .getWhereClause());

        }

        return new QueryNodeWithBindingSet(queryRoot, bindingSets);

    }

    /**
     * Rewrite search predicates for each distinct <code>searchVar</code>. All
     * such predicates for a given <code>searchVar</code> MUST appear within the
     * same group.
     */
    private void extractSearches(//
            final AST2BOpContext ctx,
//            final AbstractTripleStore database,//
            final QueryRoot queryRoot,//
            final QueryBase queryBase,//
            final GroupNodeBase<IGroupMemberNode> group) {

        // lazily allocate iff we find some search predicates in this group.
        Map<IVariable<?>, Map<URI, StatementPatternNode>> tmp = null;

        {

//            if (log.isDebugEnabled())
//                log.debug("Checking group: " + group);
            
            final int arity = group.arity();

            for (int i = 0; i < arity; i++) {

                final BOp child = group.get(i);

                if (child instanceof StatementPatternNode) {

                    final StatementPatternNode sp = (StatementPatternNode) child;

                    final TermNode p = sp.p();

                    /**
                     * This test only allows a binding for the predicate to
                     * be a URI.
                     * 
                     * @see <a href=
                     *      "https://sourceforge.net/apps/trac/bigdata/ticket/633"
                     *      > ClassCastException when binding non-uri values to
                     *      a variable that occurs in predicate position.<a>
                     */
                    if (p.isConstant() && p.getValue() instanceof URI) {
                        
                        final URI uri = (URI) ((ConstantNode) p).getValue();

                        if (uri != null // Must be a known value.
                                && uri.stringValue().startsWith(namespace)) {

                            /*
                             * Some search predicate.
                             */

                            if (!searchUrisInternal.contains(uri))
                                throw new RuntimeException(
                                        "Unknown search predicate: " + uri);

                            final TermNode s = sp.s();

                            if (!s.isVariable())
                                throw new RuntimeException(
                                        "Subject of search predicate is constant: "
                                                + sp);

                            final IVariable<?> searchVar = ((VarNode) s)
                                    .getValueExpression();

                            // Lazily allocate map.
                            if (tmp == null) {
                                tmp = new LinkedHashMap<IVariable<?>, Map<URI, StatementPatternNode>>();
                            }

                            // Lazily allocate set for that searchVar.
                            Map<URI, StatementPatternNode> statementPatterns = tmp
                                    .get(searchVar);
                            if (statementPatterns == null) {
                                tmp.put(searchVar,
                                        statementPatterns = new LinkedHashMap<URI, StatementPatternNode>());
                            }

                            // Add search predicate to set for that searchVar.
                            statementPatterns.put(uri, sp);

                        }

                    }

                } else if (child instanceof GroupNodeBase<?>) {

                    /*
                     * Recursion.
                     */

                    @SuppressWarnings("unchecked")
                    final GroupNodeBase<IGroupMemberNode> subGroup = (GroupNodeBase<IGroupMemberNode>) child;

                    extractSearches(ctx, queryRoot, queryBase, subGroup);

                }

            }

        }
        
        if (tmp != null) {

            for (Map.Entry<IVariable<?>, Map<URI, StatementPatternNode>> e : tmp
                    .entrySet()) {

                final IVariable<?> searchVar = e.getKey();
                
                final Map<URI, StatementPatternNode> statementPatterns = e
                        .getValue();

                /*
                 * Remove search predicates from the group.
                 */
                removeSearchPredicates(group, statementPatterns);

                /*
                 * Translate search predicates into a ServiceNode and associated
                 * filters.
                 */
                final ServiceNode serviceNode = createServiceNode(ctx,
                        queryBase, group, searchVar, statementPatterns);

                group.addChild(serviceNode);

//                if (group.getContext() != null)
                enforceGraphConstraint(ctx, queryRoot, searchVar, group);
                
                if (log.isInfoEnabled())
                    log.info("Rewrote group: " + group);
                
            }

        }
        
    }
    
    /**
     * If there is no join to the subject position for the search variable (?s
     * ?p ?searchVar) and the search is restricted to a subset of the named
     * graphs (either via a dataset declaration or through a GRAPH graph
     * context), then we insert a join to the subject position now. This join
     * basically imposes a constraint that the search results will only be
     * reported for the statement appearing in graphs which are visible to the
     * query. For a <code>GRAPH ?g {...}</code> group without an explicit
     * subject join, it also serves to bind the graph variable, which could
     * otherwise not be bound as nothing was actually joined against a statement
     * index.
     * 
     * @param queryRoot
     *            Used to gain access to the {@link DatasetNode}.
     * @param searchVar
     *            The search variable (the literal whose text is the free text
     *            query).
     * @param group
     *            The group in which the search magic predicates appear.
     */
    private void enforceGraphConstraint(//
            final AST2BOpContext ctx,//
            final QueryRoot queryRoot,//
            final IVariable<?> searchVar,//
            final GroupNodeBase<IGroupMemberNode> group) {

        StatementPatternNode subjectJoin = null;
        for (IGroupMemberNode child : group) {

            if (!(child instanceof StatementPatternNode))
                continue;
            
            final StatementPatternNode sp = (StatementPatternNode) child;
            
            if (searchVar.equals(sp.o().getValueExpression())) {
            
                subjectJoin = sp;
                
                break;
                
            }
            
        }

        if (subjectJoin != null) {
            /*
             * There is an explicit subject join (?subj _ ?lit), so we do not
             * need to do anything more.
             */
            return;
        }

        /*
         * We may need to impose a constraint.
         */
        if (group.getContext() != null) {

            /*
             * This group is, or is embedded within, a GRAPH group.
             * 
             * We need to impose a constraint since the graph variable might
             * otherwise not be bound and bindings for ?lit for statements not
             * in the named graph would otherwise be visible.
             */
            
            // Add the join to impose the named graph constraint.
            group.addChild(new StatementPatternNode(//
                    new VarNode("--anon-" + ctx.nextId()),// s
                    new VarNode("--anon-" + ctx.nextId()),// p
                    new VarNode(searchVar.getName()),// o
                    group.getContext(), // c
                    Scope.NAMED_CONTEXTS // scope
            ));

            if (log.isInfoEnabled())
                log.info("Added subject join to imposed named graph constraint: "
                        + group);

        } else {
            
            /*
             * This is a default graph group.
             * 
             * We need to impose a constraint IFF the default graph data set is
             * non-null. Otherwise it would be possible to observe solutions for
             * ?lit when there was no statement in the default graph which used
             * that binding of ?lit.
             */
            
            final DatasetNode datasetNode = queryRoot.getDataset();

            if (datasetNode == null) {
                /*
                 * All graphs are in the default graph so no constraint is
                 * required.
                 */
                return;
            }

            if (datasetNode.getDefaultGraphs() == null
                    && datasetNode.getDefaultGraphFilter() != null) {
                /*
                 * All graphs are in the default graph so no constraint is
                 * required. (We have to check for a filter if the default
                 * graphs were not specified since the filter can also restrict
                 * what is visible.)
                 */
                return;
            }
            
            // Add the join to impose the default graph constraint.
            group.addChild(new StatementPatternNode(//
                    new VarNode("--anon-" + ctx.nextId()),// s
                    new VarNode("--anon-" + ctx.nextId()),// p
                    new VarNode(searchVar.getName()),// o
                    null, // // c
                    Scope.DEFAULT_CONTEXTS // scope
            ));

            if (log.isInfoEnabled())
                log.info("Added subject join to imposed default graph constraint: "
                        + group);

        }

    }

    /**
     * @param queryBase
     * @param group
     * @param searchVar
     * @param statementPatterns
     * @return
     */
    private ServiceNode createServiceNode(final AST2BOpContext ctx,
            final QueryBase queryBase,
            final GroupNodeBase<IGroupMemberNode> group,
            IVariable<?> searchVar,
            final Map<URI, StatementPatternNode> statementPatterns) {
        
        final JoinGroupNode groupNode = new JoinGroupNode();
        
        for(StatementPatternNode sp : statementPatterns.values()) {
        
            groupNode.addChild(sp);
            
        }

        @SuppressWarnings("unchecked")
        final TermId<BigdataURI> iv = (TermId<BigdataURI>) TermId
                .mockIV(VTE.URI);

        iv.setValue(ctx.db.getValueFactory().asValue(searchPredicate));

        return new ServiceNode(new ConstantNode(iv), groupNode);

    }

    /**
     * Remove each {@link StatementPatternNode} from the group.
     * 
     * @param group
     *            The group.
     * @param statementPatterns
     *            The statement pattern nodes.
     */
    private void removeSearchPredicates(
            final GroupNodeBase<IGroupMemberNode> group,
            final Map<URI, StatementPatternNode> statementPatterns) {

        for(StatementPatternNode sp : statementPatterns.values()) {

            if (!group.removeArg(sp))
                throw new AssertionError();
            
        }
        
    }
    
}
