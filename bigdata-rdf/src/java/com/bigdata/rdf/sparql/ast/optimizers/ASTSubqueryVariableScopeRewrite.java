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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;

import cutthecrap.utils.striterators.Striterator;

/**
 * Class annotations the AST with scope metadata for variables based on a static
 * analysis.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTSubqueryVariableScopeRewrite.java 5122 2011-09-02 21:21:13Z
 *          thompsonbry $
 *          
 * @deprecated By {@link IBindingSet#push(IVariable[])} and
 *             {@link IBindingSet#pop(IVariable[])}.
 */
public class ASTSubqueryVariableScopeRewrite implements IASTOptimizer {

    private static final Logger log = Logger.getLogger(ASTSubqueryVariableScopeRewrite.class);
    
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final IGroupNode<IGroupMemberNode> whereClause = queryRoot
                .getWhereClause();

        /*
         * Note: This needs to be pre-order traversal since we have to rename
         * variables not projected by an outer subquery first and the proceed to
         * rename variables in an inner subquery.
         * 
         * FIXME Write a unit test for this.
         */
        @SuppressWarnings("unchecked")
        final Iterator<SubqueryRoot> itr = new Striterator(
                BOpUtility.preOrderIterator((BOp) whereClause))
                .addTypeFilter(SubqueryRoot.class);

        while (itr.hasNext()) {

            final SubqueryRoot subqueryRoot = itr.next();

            if (log.isInfoEnabled())
                log.info("subqueryBefore=" + subqueryRoot);

            rewriteBindXAsY(subqueryRoot);

            rewriteVariablesNotProjected(context, subqueryRoot);

            if (log.isInfoEnabled()) {
                log.info("subqueryAfter=" + subqueryRoot);
                if (itr.hasNext())
                    log.info("------");
            }

        }

        return queryRoot;

    }

    /**
     * In the special case where a variable is renamed in the projection using
     * BIND(x AS y), appearances of [x] in the projection and the subquery body
     * will be replaced by [y]. This allows bindings on [y] in the parent scope
     * to flow into the subquery so it will be more constrained when it is
     * evaluated rather than filtering solutions where the computed value of [y]
     * for the subquery conflicts with the bound value of [y] in the parent.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void rewriteBindXAsY(final SubqueryRoot subqueryRoot) {

        final ProjectionNode projection = subqueryRoot.getProjection();

        /*
         * If BIND(x AS y), then rename [x => y] within the subquery.
         */
        {

            final List<Bind<?>> renameList = new LinkedList<Bind<?>>();

            for (AssignmentNode assignment : projection) {

                if (assignment.getValueExpression() instanceof IVariable<?>) {

                    renameList.add(new Bind(assignment.getVar(),
                            (IVariable<?>) assignment.getValueExpression()));

                }

            }

            /*
             * For all BIND(x AS y), rename x => y within the subquery.
             */
            for (Bind<?> bind : renameList) {

                final IVariable<?> oldVar = bind.getVar(); // x

                final IVariable<?> newVar = (IVariable<?>) bind.getExpr(); // y

                subqueryRoot.replaceAllWith(oldVar/* x */, newVar/* y */);

            }

        }

    }

    /**
     * Rewrite a subquery, turning all variables not projected by the subquery
     * into unique variables. This preserves the bottom up evaluation semantics
     * since variables which would otherwise have the same name in the parent
     * group will no longer interact with variables in the subquery which are
     * not projected by the subquery.
     * 
     * @param subqueryRoot
     */
    @SuppressWarnings("rawtypes")
    private void rewriteVariablesNotProjected(final AST2BOpContext context,
            final SubqueryRoot subqueryRoot) {

        final ProjectionNode projection = subqueryRoot.getProjection();

        // The distinct variables projected by the subquery.
        final List<IVariable> projected = BOpUtility.toList(BOpUtility
                .getSpannedVariables(projection));

        // The distinct variables used within the subquery.
        final List<IVariable> renameList = BOpUtility.toList(BOpUtility
                .getSpannedVariables(subqueryRoot));

        // Variables which appear within the subquery but which are not
        // projected out of the subquery.
        renameList.removeAll(projected);

        // Rename such variable.
        final Map<IVariable, IVariable> rename = new LinkedHashMap<IVariable, IVariable>();

        for (IVariable v : renameList) {

            rename.put(v,
                    Var.var(context.createVar("-alias-" + v.getName() + "-")));

        }
        
        int ntotal = 0;
        
        // Rename non-projected variables within the subquery.
        for(Map.Entry<IVariable, IVariable> e : rename.entrySet()) {
            
            final IVariable oldVal = e.getKey();
            
            final IVariable newVal = e.getValue();

            final int nmods = subqueryRoot.replaceAllWith(oldVal, newVal);

            if (log.isInfoEnabled())
                log.info("Replaced " + nmods + " instances of " + oldVal
                        + " with " + newVal);

            assert nmods > 0; // Failed to replace something.

            ntotal += nmods;
            
        }

        if (log.isInfoEnabled())
            log.info("Replaced "
                    + ntotal
                    + " instances of "
                    + rename.size()
                    + " variables with variable which are unique within the query scope");

    }

}
