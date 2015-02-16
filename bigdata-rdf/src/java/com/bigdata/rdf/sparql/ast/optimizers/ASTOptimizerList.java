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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * An executable list of query optimizers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTOptimizerList extends LinkedList<IASTOptimizer> implements
        IASTOptimizer {
    
    private static final Logger log = Logger.getLogger(ASTOptimizerList.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public ASTOptimizerList(final Collection<IASTOptimizer> c) {

        super(c);
        
    }

    public ASTOptimizerList(final IASTOptimizer... optimizers) {

        this(Arrays.asList(optimizers));
        
    }
    
    @Override
    public boolean add(final IASTOptimizer opt) {
        
        if(opt == null)
            throw new IllegalArgumentException();
        
        if(opt == this)
            throw new IllegalArgumentException();
        
        return super.add(opt);
        
    }

    /**
     * Run all the optimizers in the list.
     * <p>
     * Note: This makes a deep copy of the AST before applying destructive
     * modifications.
     */
    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            IQueryNode queryNode, final IBindingSet[] bindingSets) {

        if (log.isDebugEnabled())
            log.debug("Original AST:\n" + queryNode);

        // Avoid side-effects on the original AST!
        queryNode = (IQueryNode) BOpUtility.deepCopy((BOp) queryNode);
        
        for (IASTOptimizer opt : this) {

            if (log.isInfoEnabled())
                log.info("Applying: " + opt);

            queryNode = opt.optimize(context, queryNode, bindingSets);

            if (queryNode == null)
                throw new AssertionError("Optimized discarded query: " + opt);

            if (log.isDebugEnabled())
                log.debug("Rewritten AST:\n" + queryNode);
      
        }

        return queryNode;

    }

}
