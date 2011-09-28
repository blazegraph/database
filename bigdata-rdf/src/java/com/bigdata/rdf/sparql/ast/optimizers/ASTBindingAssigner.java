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

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTBase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Examines the source {@link IBindingSet}[]. If there is a single binding set
 * in the source, then any variable bound in that input is rewritten in the AST
 * to be a {@link ConstantNode}. The rewrite uses the special form of the
 * {@link Constant} constructor which associates the variable with the constant.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTBindingAssigner implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTBindingAssigner.class);
    
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, //final DatasetNode dataset,
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

        /*
         * Gather the VarNodes for variables which have bindings.
         */
        
        final Map<VarNode,ConstantNode> replacements = new LinkedHashMap<VarNode, ConstantNode>();
        
        final Iterator<BOp> itr = BOpUtility
                .preOrderIterator((BOp) whereClause);

        while (itr.hasNext()) {

            final BOp node = (BOp) itr.next();

            if (!(node instanceof VarNode))
                continue;

            final VarNode varNode = (VarNode) node;

            if(replacements.containsKey(varNode))
                continue;
            
            final IVariable<IV> var = varNode.getValueExpression();

            if (bset.isBound(var)) {

                /*
                 * Replace the variable with the constant from the binding set,
                 * but preserve the reference to the variable on the Constant.
                 */

                final IV asBound = (IV) bset.get(var).get();

                final ConstantNode constNode = new ConstantNode(
                        new Constant<IV>(var, asBound));

                if (log.isInfoEnabled())
                    log.info("Will replace: var=" + var + " with " + asBound
                            + " (" + constNode + ")");

                replacements.put(varNode, constNode);
                
            }

        }

        int ntotal = 0;
        
        for(Map.Entry<VarNode, ConstantNode> e : replacements.entrySet()) {

            final VarNode oldVal = e.getKey();
            final ConstantNode newVal = e.getValue();

            final int nmods = ((ASTBase) whereClause).replaceAllWith(oldVal,
                    newVal);

            if (log.isInfoEnabled())
                log.info("Replaced " + nmods + " instances of " + oldVal
                        + " with " + newVal);

            assert nmods > 0; // Failed to replace something.

            ntotal += nmods;
            
        }

        if (log.isInfoEnabled())
            log.info("Replaced " + ntotal + " instances of "
                    + replacements.size() + " bound variables with constants");

        return queryNode;

    }

}
