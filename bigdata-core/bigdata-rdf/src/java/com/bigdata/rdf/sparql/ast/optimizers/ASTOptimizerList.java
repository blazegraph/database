/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import com.bigdata.bop.engine.StaticAnalysisStats;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * An executable list of query optimizers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ASTOptimizerList extends LinkedList<IASTOptimizer> implements
        IASTOptimizer {
    
    private static final Logger log = Logger.getLogger(ASTOptimizerList.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static private boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static private boolean DEBUG = log.isDebugEnabled();

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
    public QueryNodeWithBindingSet optimize(
        final AST2BOpContext context, final QueryNodeWithBindingSet input) {

        final StaticAnalysisStats saStats = context.getStaticAnalysisStats();
        
        final long startLoop = System.nanoTime();

        final IQueryNode queryNode = input.getQueryNode();
        final IBindingSet[] bindingSets = input.getBindingSets();    

        if (DEBUG)
            log.debug("Original AST:\n" + queryNode);

        // Avoid side-effects on the original AST!
        QueryNodeWithBindingSet tmp = 
           new QueryNodeWithBindingSet(
              (IQueryNode) BOpUtility.deepCopy((BOp) queryNode), bindingSets);
        
        for (IASTOptimizer opt : this) {
           
            final long startOpt = System.nanoTime();

            if (INFO)
                log.info("Applying: " + opt);

            tmp = opt.optimize(context, tmp);
            
            if (queryNode == null)
                throw new AssertionError("Optimized discarded query: " + opt);

            if (DEBUG)
                log.debug("Rewritten AST:\n" + tmp.getQueryNode());

            saStats.registerOptimizerCall(
               opt.getClass().getSimpleName(), 
               System.nanoTime() - startOpt);
      
        }

        saStats.registerOptimizerLoopCall(
           System.nanoTime() - startLoop);

        return tmp;

    }
}
