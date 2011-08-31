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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.Stack;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.INodeOrAttribute;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * Examines the source {@link IBindingSet}[]. If there is a single binding set
 * in the source, then any variable bound in that input is rewritten in the AST
 * to be a {@link ConstantNode}. The rewrite uses the special form of the
 * {@link Constant} constructor which associates the variable with the constant.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Other optimizations are possible when the {@link IBindingSet}[]
 *          has multiple solutions. In particular, the possible values which a
 *          variable may take on can be written into an IN constraint and
 *          associated with the query in the appropriate scope.
 */
public class ASTBindingAssigner implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTBindingAssigner.class);
    
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final DatasetNode dataset,
            final IBindingSet[] bindingSet) {

        if (bindingSet == null || bindingSet.length > 1) {
            // used iff there is only one input solution.
            return queryNode;
        }

        if (!(queryNode instanceof QueryRoot))
            return queryNode;

        // consider only the first solution.
        final IBindingSet bset = bindingSet[0];

        final QueryRoot queryRoot = (QueryRoot) queryNode;

        // Only transform the WHERE clause.
        final IGroupNode<?> whereClause = queryRoot.getWhereClause();

        if (whereClause == null)
            return queryNode;

        final Stack<INodeOrAttribute> stack = new Stack<INodeOrAttribute>();

        final Iterator<BOp> itr = BOpUtility.preOrderIterator(
                (BOp) whereClause, stack);

        while (itr.hasNext()) {

            final BOp node = (BOp) itr.next();

            if (!(node instanceof VarNode))
                continue;

            final VarNode varNode = (VarNode) node;

            final IVariable<IV> var = varNode.getValueExpression();

            if (bset.isBound(var)) {

                /*
                 * Replace the variable with the constant from the binding set,
                 * but preserve the reference to the variable on the Constant.
                 */

                final IV asBound = (IV) bset.get(var).get();

                if (log.isInfoEnabled())
                    log.info("Replacing: var=" + var + " with " + asBound
                            + " (" + asBound.getClass() + ")");

                final ConstantNode constNode = new ConstantNode(
                        new Constant<IV>(var, asBound));

//                varNode.setValueExpression(new Constant<IV>(var, asBound));

                final INodeOrAttribute x = stack.peek();

                x.getNode();
                
                // FIXME replace varNode on parent with constNode.
                ASTUtil.replaceWith(x.getNode(), varNode,constNode);
                
            }

        }

        return queryNode;

    }

}
