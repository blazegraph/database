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

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

import cutthecrap.utils.striterators.Striterator;

/**
 * Rewrites any {@link ProjectionNode} with a wild card into the set of
 * variables visible to the {@link QueryBase} having that projection. This is
 * done first for the {@link NamedSubqueriesNode} and then depth-first for the
 * WHERE clause. Only variables projected by a subquery will be projected by the
 * parent query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ASTWildcardProjectionOptimizer implements IASTOptimizer {

    @Override
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();     


        if(!(queryNode instanceof QueryRoot))
           return new QueryNodeWithBindingSet(queryNode, bindingSets);
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final StaticAnalysis sa = new StaticAnalysis(queryRoot, context);
        
        // collect named subquery includes that have been resolved already
        final Set<String> resolvedNSIs = new HashSet<String>();
        
        /*
         * NAMED SUBQUERIES
         * 
         * Rewrite the named subquery projections before the where clause.
         */
        if (queryRoot.getNamedSubqueries() != null) {

            for (NamedSubqueryRoot subqueryRoot : queryRoot
                    .getNamedSubqueries()) {

               @SuppressWarnings("unchecked")
               final Iterator<QueryBase> itr = (Iterator<QueryBase>) new Striterator(
                       BOpUtility.postOrderIteratorWithAnnotations((BOp) subqueryRoot
                               .getWhereClause())).addTypeFilter(QueryBase.class);

               while (itr.hasNext()) {

                   final QueryBase queryBase = itr.next();

                   rewriteProjection(sa, queryBase, null, resolvedNSIs);

               }

               rewriteProjection(sa, subqueryRoot, null, resolvedNSIs);
               
            }

        }
        
        /*
         * WHERE CLAUSE
         * 
         * Bottom up visitation so we can get rewrite the projections of
         * subqueries before we rewrite the projections of the parent query.
         * 
         * @see <a href="http://trac.bigdata.com/ticket/757" > Wildcard projection
         * was not rewritten. </a>
         */
        if (queryRoot.getWhereClause() != null) {
            
            @SuppressWarnings("unchecked")
            final Iterator<QueryBase> itr = (Iterator<QueryBase>) new Striterator(
                    BOpUtility.postOrderIteratorWithAnnotations((BOp) queryRoot
                            .getWhereClause())).addTypeFilter(QueryBase.class);

            while (itr.hasNext()) {

                final QueryBase queryBase = itr.next();

                rewriteProjection(sa, queryBase, null, resolvedNSIs);

            }

        }

        // Rewrite the projection on the QueryRoot last.
        rewriteProjection(
           sa, queryRoot, context.getSolutionSetStats().getUsedVars(), resolvedNSIs);

        return new QueryNodeWithBindingSet(queryRoot, bindingSets);
    
    }

    /**
     * Rewrite the projection for the {@link QueryBase}.
     * 
     * @param sa
     *            {@link StaticAnalysis} helper.
     * @param queryBase
     *            The {@link QueryBase} whose {@link ProjectionNode} will be
     *            rewritten.
     */
    private void rewriteProjection(final StaticAnalysis sa,
            final QueryBase queryBase, Set<IVariable<?>> exogeneousVars,
            final Set<String> resolvedNSIs /* to break infinite loops */) {

        /**
         * BLZG-1763: recurse into named subquery includes. 
         */
        if (queryBase instanceof NamedSubqueryRoot) {
            
            final NamedSubqueryRoot queryBaseAsNsr = (NamedSubqueryRoot)queryBase;
            
            final GraphPatternGroup<?> gpg = queryBaseAsNsr.getGraphPattern();
            
            @SuppressWarnings("unchecked")
            final Iterator<NamedSubqueryInclude> itr = 
                (Iterator<NamedSubqueryInclude>) new Striterator(
                    BOpUtility.postOrderIteratorWithAnnotations((BOp) gpg)).
                    addTypeFilter(NamedSubqueryInclude.class);
            
            while (itr.hasNext()) {
                
                final NamedSubqueryInclude nsi = itr.next();
                
                final String name = nsi.getName();

                final NamedSubqueryRoot nsr = sa.getNamedSubqueryRoot(name);

                if (!resolvedNSIs.contains(name)) { // otherwise: already rewritten
                    
                    resolvedNSIs.add(name); // -> do not process again, will be resolved
                    rewriteProjection(sa, nsr, exogeneousVars, resolvedNSIs);

                }
            }
        }
        
        final ProjectionNode projection = queryBase.getProjection();

        if (projection != null && projection.isWildcard()) {

            final GroupNodeBase<IGroupMemberNode> whereClause = 
                  (GroupNodeBase<IGroupMemberNode>) queryBase.getWhereClause();

            final ProjectionNode p2 = new ProjectionNode();
            
            queryBase.setProjection(p2);
            
            if(projection.isDistinct())
                p2.setDistinct(true);

            if(projection.isReduced())
                p2.setReduced(true);
            
            final Set<IVariable<?>> varSet = sa.getSpannedVariables(
                    whereClause, new LinkedHashSet<IVariable<?>>());
            
            if (exogeneousVars!=null) {
               varSet.addAll(exogeneousVars);
            }
            if (queryBase.getBindingsClause()!=null) {
               final BindingsClause bc = queryBase.getBindingsClause();
               varSet.addAll(bc.getDeclaredVariables());
            }


            for(IVariable<?> var : varSet) {
            
            	if (!((Var) var).isAnonymous())
            		p2.addProjectionVar(new VarNode(var.getName()));
                
            }
            
        }
        
    }

}
