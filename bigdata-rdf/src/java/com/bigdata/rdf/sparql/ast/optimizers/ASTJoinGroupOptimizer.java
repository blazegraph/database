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
 * Created on Sep 6, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;

/**
 * If the top-level join group has a single child, then it is replaced by that
 * child. Embedded non-optional join groups without a context which contain a
 * single child by lifting the child into the parent.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ASTSubqueryRootInGroupOptimizer.java 5143 2011-09-07 11:39:17Z
 *          thompsonbry $
 */
public class ASTJoinGroupOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTJoinGroupOptimizer.class);
    
    @SuppressWarnings("unchecked")
    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        GroupNodeBase<IGroupMemberNode> whereClause = (GroupNodeBase<IGroupMemberNode>)queryRoot
                .getWhereClause();

        if (whereClause == null) {

            throw new RuntimeException("Missing where clause? : " + queryNode);

        }

        /*
         * If the root join group has only a single child join group, then
         * replace it with that child.
         */

        if ((whereClause.size() == 1)
                && (whereClause.get(0) instanceof JoinGroupNode)) {

            if (log.isInfoEnabled())
                log.info("Replacing top-level group: "
                        + whereClause.toShortString() + " with only child "
                        + whereClause.get(0).toShortString());

            final GroupNodeBase<IGroupMemberNode> child = (GroupNodeBase<IGroupMemberNode>) whereClause
                    .get(0);

            // remove child from the parent (clears parent reference).
            whereClause.removeChild(child);

            // set child as the new where clause.
            queryRoot.setWhereClause(child);

        }
        
        rewrite((GroupNodeBase<IGroupMemberNode>) whereClause);

        return queryRoot;
        
    }

    /**
     * @param p
     *            The parent.
     */
    @SuppressWarnings("unchecked")
    private void rewrite(final GroupNodeBase<IGroupMemberNode> p) {

        final int arity = p.size();

        for (int i = 0; i < arity; i++) {

            final IGroupMemberNode child = (IGroupMemberNode) p.get(i);

            if (child instanceof JoinGroupNode) {

                final JoinGroupNode group = (JoinGroupNode) child;

                if (!group.isOptional() && group.size() == 1
                        && group.getContext() == p.getContext()) {

                    // lift the child into the parent.
                    p.setArg(i, group);
                    
                    if (log.isInfoEnabled())
                        log.info("Lifting " + group.toShortString() + " into "
                                + p.toShortString());
                    
                }
                
            }

            if(child instanceof GroupNodeBase<?>) {

                // Recursion
                rewrite((GroupNodeBase<IGroupMemberNode>) child);

            }
            
        }
        
    }

}
