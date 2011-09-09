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
 * Created on Sep 8, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.ServiceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;

/**
 * Translate {@link BD#SEARCH} and within a {@link NamedSubqueryRoot} into a
 * {@link ServiceNode} to invoke the search engine.
 * 
 * <pre>
 * with {
 *    select ?subj ?score
 *    where {
 *      ?lit bd:search "foo" .
 *      ?lit bd:relevance ?score .
 *      ?subj ?p ?lit .
 *    }
 *   ORDER BY DESC(?score)
 *   LIMIT 10
 *   OFFSET 0 
 * } as %searchSet1
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTSearchOptimizer implements IASTOptimizer {

    private static final Logger log = Logger.getLogger(ASTSearchOptimizer.class);
    
    /**
     * The known search URIs.
     * <p>
     * Note: We can recognize anything in {@link BD#SEARCH_NAMESPACE}, but the
     * predicate still has to be something that we know how to interpret.
     */
    static final Set<URI> searchUris;
    
    static {
        
        final Set<URI> set = new LinkedHashSet<URI>();
    
        set.add(BD.SEARCH);
        set.add(BD.RELEVANCE);
        set.add(BD.RANK);
        set.add(BD.MAX_RANK);
        set.add(BD.MIN_RANK);
        set.add(BD.MAX_RELEVANCE);
        set.add(BD.MIN_RELEVANCE);
        set.add(BD.MATCH_ALL_TERMS);
        
        searchUris = Collections.unmodifiableSet(set);
        
    }

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if(!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        if (queryRoot.getNamedSubqueries() != null) {

            /*
             * Look for, validate, and rewrite magic predicates for search if
             * they appear within a named subquery.
             */
            
            for (NamedSubqueryRoot namedSubquery : queryRoot
                    .getNamedSubqueries()) {

                extractSearches(context.db, namedSubquery,
                        (GroupNodeBase<IGroupMemberNode>) namedSubquery
                                .getWhereClause());

            }

        }

        /*
         * The magic predicates for search should not appear in the main WHERE
         * clause for the query.
         */
        validateNoSearch((BOp) queryRoot.getWhereClause());

        return queryRoot;

    }

    /**
     * Verify that no magic predicates for search are found within the given
     * operator (recursive).
     */
    private void validateNoSearch(final BOp op) {

        if(op == null)
            return;
        
        if (op instanceof StatementPatternNode) {

            final StatementPatternNode sp = (StatementPatternNode) op;

            final TermNode p = sp.p();

            if (p.isConstant()) {

                final BigdataValue value = p.getValue();

                if (value != null) {
                    
                    // Must be a Value known to the database.

                    if (((ConstantNode) p).getValue().stringValue()
                            .startsWith(BD.SEARCH_NAMESPACE)) {

                        throw new RuntimeException(
                                "Search predicates are only allowed in named subqueries.");

                    }

                }

            }

        }
        
        final int arity = op.arity();
        
        for (int i = 0; i < arity; i++) {
            
            final BOp child = op.get(i);
            
            validateNoSearch(child);

        }

    }

    /**
     * Rewrite search predicates for each distinct <code>searchVar</code>. All
     * such predicates for a given <code>searchVar</code> MUST appear within the
     * same group.
     */
    private void extractSearches(final AbstractTripleStore database,
            final QueryBase queryBase,
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

                    if (p.isConstant()) {

                        final URI uri = (URI) ((ConstantNode) p).getValue();

                        if (uri != null // Must be a known value.
                                && uri.stringValue().startsWith(
                                        BD.SEARCH_NAMESPACE)) {

                            /*
                             * Some search predicate.
                             */

                            if (!searchUris.contains(uri))
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

                    extractSearches(database, queryBase, subGroup);

                }

            }

        }
        
        if (tmp != null) {

            /*
             * Validate searches.
             */
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
                final ServiceNode serviceNode = createServiceNode(queryBase,
                        group, searchVar, statementPatterns);

                group.addChild(serviceNode);
                
                if (log.isInfoEnabled())
                    log.info("Rewrote group: " + group);
                
            }

        }
        
    }

    /**
     * @param queryBase
     * @param group
     * @param searchVar
     * @param statementPatterns
     * @return
     */
    private ServiceNode createServiceNode(QueryBase queryBase,
            GroupNodeBase<IGroupMemberNode> group, IVariable<?> searchVar,
            Map<URI, StatementPatternNode> statementPatterns) {
        
        final JoinGroupNode groupNode = new JoinGroupNode();
        
        for(StatementPatternNode sp : statementPatterns.values()) {
        
            groupNode.addChild(sp);
            
        }

        return new ServiceNode(searchVar.getName(), BD.SEARCH, groupNode);

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
