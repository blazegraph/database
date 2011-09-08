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

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryBase;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SubqueryBase;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;

import cutthecrap.utils.striterators.Striterator;

/**
 * Rewrites any {@link ProjectionNode} with a wild card into the set of
 * variables visible to the {@link QueryBase} having that projection. This is
 * done first for the {@link NamedSubqueriesNode} and then depth-first for the
 * WHERE clause. Only variables projected by a subquery will be projected by the
 * parent query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTWildcardProjectionOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if(!(queryNode instanceof QueryRoot))
            return queryNode;
        
        final QueryRoot queryRoot = (QueryRoot) queryNode;

        /*
         * NAMED SUBQUERIES
         * 
         * Rewrite the named subquery projections before the where clause.
         */
        if (queryRoot.getNamedSubqueries() != null) {

            for (NamedSubqueryRoot subqueryRoot : queryRoot
                    .getNamedSubqueries()) {

                rewriteProjection(queryRoot, subqueryRoot);

            }

        }
        
        /*
         * WHERE CLAUSE
         * 
         * Bottom up visitation so we can get rewrite the projections of
         * subqueries before we rewrite the projections of the parent query.
         */
        if (queryRoot.getWhereClause() != null) {
            
            @SuppressWarnings("unchecked")
            final Iterator<QueryBase> itr = (Iterator<QueryBase>) new Striterator(
                    BOpUtility.postOrderIterator((BOp) queryRoot
                            .getWhereClause())).addTypeFilter(QueryBase.class);

            while (itr.hasNext()) {

                final QueryBase queryBase = itr.next();

                rewriteProjection(queryRoot, queryBase);

            }

        }

        // Rewrite the projection on the QueryRoot last.
        rewriteProjection(queryRoot, queryRoot);

        return queryRoot;
    
    }

    /**
     * Rewrite the projection for the {@link QueryBase}.
     * 
     * @param queryRoot
     *            The {@link QueryRoot} is used to resolve
     *            {@link NamedSubqueryInclude}s to the corresponding
     *            {@link NamedSubqueryRoot}.
     * @param queryBase
     *            The {@link QueryBase} whose {@link ProjectionNode} will be
     *            rewritten.
     */
    private void rewriteProjection(final QueryRoot queryRoot,
            final QueryBase queryBase) {

        final ProjectionNode projection = queryBase.getProjection();

        if (projection != null && projection.isWildcard()) {

            final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryBase
                    .getWhereClause();

            final Set<IVariable<?>> varSet = new LinkedHashSet<IVariable<?>>();

            getSpannedVariables(queryRoot, whereClause, varSet);

            final ProjectionNode p2 = new ProjectionNode();
            
            queryBase.setProjection(p2);
            
            for(IVariable<?> var : varSet) {
            
                p2.addProjectionVar(new VarNode(var.getName()));
                
            }
            
        }
        
    }

    /**
     * Return the distinct variables recursively using a pre-order traversal
     * present whether in the operator tree or on annotations attached to
     * operators. Variables projected by a subquery are included, but not
     * variables within the WHERE clause of the subquery. Variables projected by
     * a {@link NamedSubqueryInclude} are also reported.
     */
    private static void getSpannedVariables(final QueryRoot queryRoot,
            final BOp op, final Set<IVariable<?>> varSet) {

        if (op == null) {

            return;
            
        } else if (op instanceof IVariable<?>) {
         
            varSet.add((IVariable<?>)op);
            
        } else if(op instanceof IConstant<?>) {
            
            final IConstant<?> c = (IConstant<?>)op;
            
            final IVariable<?> var = (IVariable<?> )c.getProperty(Constant.Annotations.VAR);
                
            if( var != null) {
                
                varSet.add(var);
                
            }
            
        } else if (op instanceof SubqueryRoot) {

            /*
             * Do not recurse into a subquery, but report any variables
             * projected by that subquery.
             */

            final SubqueryRoot subquery = (SubqueryRoot) op;

            addProjectedVariables(subquery, varSet);
            
            // DO NOT RECURSE INTO THE SUBQUERY!
            return;

        } else if (op instanceof NamedSubqueryInclude) {

            final NamedSubqueryInclude namedInclude = (NamedSubqueryInclude) op;

            final NamedSubqueryRoot subquery = getNamedSubqueryRoot(
                    queryRoot, namedInclude);

            addProjectedVariables(subquery, varSet);
            
            // DO NOT RECURSE INTO THE SUBQUERY!
            return;
            
        }
        
        /*
         * Recursion.
         */

        final int arity = op.arity();

        for (int i = 0; i < arity; i++) {

            getSpannedVariables(queryRoot, op.get(i), varSet);

        }

    }

    /**
     * Add all variables on the {@link ProjectionNode} of the subquery to the
     * set of distinct variables visible within the scope of the parent query.
     * 
     * @param subquery
     * @param varSet
     */
    static private void addProjectedVariables(final SubqueryBase subquery,
            final Set<IVariable<?>> varSet) {
        
        final ProjectionNode proj = subquery.getProjection();

        if (proj.isWildcard()) {
            /* The subquery's projection should already have been rewritten. */
            throw new AssertionError();
        }

        for (IVariable<?> var : proj.getProjectionVars()) {

            varSet.add(var);

        }

    }
    
    /**
     * Return the {@link NamedSubqueryRoot} for the {@link NamedSubqueryInclude}
     * .
     * 
     * @param queryRoot
     * @param namedInclude
     * @return
     * 
     * @throws RuntimeException
     *             if there is no corresponding {@link NamedSubqueryRoot}.
     */
    private static NamedSubqueryRoot getNamedSubqueryRoot(
            final QueryRoot queryRoot, final NamedSubqueryInclude namedInclude) {

        final NamedSubqueriesNode namedSubqueriesNode = queryRoot
                .getNamedSubqueries();

        if (namedSubqueriesNode != null) {

            for (NamedSubqueryRoot subqueryRoot : namedSubqueriesNode) {

                if (subqueryRoot.getName().equals(namedInclude.getName())) {

                    return subqueryRoot;

                }

            }

        }

        throw new RuntimeException(
                "Named subquery does not exist for namedSet: " + namedInclude);

    }

}
