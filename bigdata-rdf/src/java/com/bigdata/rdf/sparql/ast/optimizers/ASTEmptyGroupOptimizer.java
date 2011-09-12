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

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueriesNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.QueryRoot;

/**
 * Eliminate semantically empty join group nodes which are the sole child of
 * another join groups. Such nodes either do not specify a context or they
 * specify the same context as the parent.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTEmptyGroupOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTEmptyGroupOptimizer.class);

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;
        
        /*
         * Lift any ServiceNode out of the main WHERE clause (including any
         * embedded subqueries). We can not have any service invocations run
         * from the main WHERE clause because they will be invoked once for each
         * solution pushed into the query, even if the ServiceNode is the first
         * operator in the query plan. Each such ServiceNode is replaced by a
         * named subquery root and a named subquery include.
         */

        {

            final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) queryRoot
                    .getWhereClause();

            if (whereClause != null) {

                eliminateEmptyGroups(whereClause);

            }

        }

        /*
         * Examine each named subquery. If there is more than one ServiceNode,
         * or if a ServiceNode is embedded in a subquery, then lift it out into
         * its own named subquery root, replacing it with a named subquery
         * include.
         */
        if (queryRoot.getNamedSubqueries() != null) {

            final NamedSubqueriesNode namedSubqueries = queryRoot
                    .getNamedSubqueries();

            /*
             * Note: This loop uses the current size() and get(i) to avoid
             * problems with concurrent modification during visitation.
             */
            for (int i = 0; i < namedSubqueries.size(); i++) {

                final NamedSubqueryRoot namedSubquery = (NamedSubqueryRoot) namedSubqueries
                        .get(i);

                final GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>) namedSubquery
                        .getWhereClause();

                if (whereClause != null) {

                    eliminateEmptyGroups(whereClause);

                }

            }

        }

//        log.error("\nafter rewrite:\n" + queryNode);

        return queryNode;

    }

    private static void eliminateEmptyGroups(
            final GroupNodeBase<IGroupMemberNode> op) {

        /*
         * First check whether this operator is a join group having a single
         * child join group, in which case we eliminate the child, lifting its
         * children into this join group.
         */

        if((op instanceof JoinGroupNode) && op.arity()==1 && 
                op.get(0) instanceof JoinGroupNode) {
            
            /*
             * Verify that the two join groups can be merged into one. We can do
             * this unless they both have a different graph context.
             * 
             * Note: We can eliminate the parent even if it is optional or has a
             * context by setting those attributes on the child (as long as the
             * child does not have a different context).
             */

            final JoinGroupNode parent = (JoinGroupNode) op;

            final JoinGroupNode child = (JoinGroupNode) op.get(0);
            
            if (parent.getContext() == child.getContext()
                    || parent.getContext() == null
                    || child.getContext() == null) {

                /*
                 * Lift the children of this child into its parent.
                 * 
                 * TODO We probably should scan the child's annotations for
                 * other things which could be lifted onto to the parent. That
                 * will let us preserve hints for a join group if the child
                 * existed only to communicate those hints.
                 */
                
                if (log.isInfoEnabled())
                    log.info("Lifting children of child group into parent: parent="
                            + parent + ", child=" + child);

                /*
                 * If the child has a context, then lift it onto the parent.
                 */
                if (child.getContext() != null) {
                    parent.setContext(child.getContext());
                }

                /*
                 * If the child was optional, then lift that annotation onto
                 * the parent.
                 */
                if (child.isOptional()) {
                    parent.setOptional(child.isOptional());
                }

                /*
                 * Remove the child from the parent.
                 */
                parent.removeChild(child);
                
                /*
                 * Lift the children of the child onto the parent.
                 */
                final int n = child.arity();

                for (int i = 0; i < n; i++) {

                    parent.addChild((IGroupMemberNode) child.get(i));

                }
                
            }

        }

        /*
         * Recursion, but only into group nodes.
         */
        final int arity = op.arity();

        for (int i = 0; i < arity; i++) {

            final BOp child = op.get(i);

            if (!(child instanceof GroupNodeBase<?>))
                continue;

            @SuppressWarnings("unchecked")
            final GroupNodeBase<IGroupMemberNode> childGroup = (GroupNodeBase<IGroupMemberNode>) child;

            eliminateEmptyGroups(childGroup);

        }

    }

}
