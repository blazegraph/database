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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.ArrayList;
import java.util.Iterator;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;

/**
 * Visit all the value expression nodes and convert them into value expressions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTSetValueExpressionsOptimizer implements IASTOptimizer {

    /**
     * 
     */
    public ASTSetValueExpressionsOptimizer() {
    }

    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        final QueryRoot query = (QueryRoot) queryNode;

        final String lex = context.db.getLexiconRelation().getNamespace();

        final Iterator<IValueExpressionNode> it = BOpUtility.visitAll(query,
                IValueExpressionNode.class);

        final ArrayList<IValueExpressionNode> allNodes = new ArrayList<IValueExpressionNode>();

        while (it.hasNext()) {

            allNodes.add(it.next());

        }

        for (IValueExpressionNode ven : allNodes) {

            /*
             * Convert and cache the value expression on the node as a
             * side-effect.
             */

            AST2BOpUtility.toVE(lex, ven);

        }

        return query;
        
    }

}
