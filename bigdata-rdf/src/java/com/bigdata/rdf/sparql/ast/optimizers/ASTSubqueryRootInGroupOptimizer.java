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

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;

/**
 * If a {@link SubqueryRoot} appears in an otherwise empty (and non-optional)
 * {@link JoinGroupNode}, then the join group is replaced by the
 * {@link SubqueryRoot}. This eliminates a subquery for the otherwise empty
 * {@link JoinGroupNode} since we can process the {@link SubqueryRoot} in the
 * parent directly.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTSubqueryRootInGroupOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        final IGroupNode<IGroupMemberNode> whereClause = queryRoot
                .getWhereClause();

        if (whereClause == null) {

            throw new RuntimeException("Missing where clause? : " + queryNode);

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
                        && group.get(0) instanceof SubqueryRoot) {

                    // lift the SubqueryRoot into the parent.
                    p.setArg(i, group.get(0));
                    
                }
                
            }

            if(child instanceof GroupNodeBase<?>) {

                // Recursion
                rewrite((GroupNodeBase<IGroupMemberNode>) child);

            }
            
        }
        
    }

}
