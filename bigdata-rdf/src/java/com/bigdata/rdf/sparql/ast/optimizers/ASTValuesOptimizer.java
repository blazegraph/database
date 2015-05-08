/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.BindingsClause;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * If we have a singleton BindingsClause inside the main where clause and no
 * BindingsClause attached to the QueryRoot, we can promote the BC from inline
 * to top-level and avoid an extra hash index / hash join later.
 */
public class ASTValuesOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTValuesOptimizer.class);

    @Override
    public IQueryNode optimize(final AST2BOpContext context, 
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        final QueryRoot queryRoot = (QueryRoot) queryNode;
        
        if (queryRoot.getBindingsClause() == null) {

            // Main WHERE clause
            @SuppressWarnings("unchecked")
            final GraphPatternGroup<IGroupMemberNode> whereClause = 
                (GraphPatternGroup<IGroupMemberNode>) queryRoot.getWhereClause();

            if (whereClause != null) {

                final List<BindingsClause> bc = 
                        whereClause.getChildren(BindingsClause.class);

                if (!bc.isEmpty()) {
                    
                    /*
                     * Promote to query root.
                     */
                    queryRoot.setBindingsClause(bc.get(0));
                    whereClause.removeChild(bc.get(0));
                    
                }
                
            }

        }

        return queryNode;

    }

}
