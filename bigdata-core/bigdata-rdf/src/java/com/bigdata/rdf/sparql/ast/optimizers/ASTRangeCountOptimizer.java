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

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.StaticAnalysisStats;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.RangeNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTStaticJoinOptimizer.Annotations;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Attach range counts to all statement patterns in the query.
 * 
 * @author mikepersonick
 *
 */
public class ASTRangeCountOptimizer extends AbstractJoinGroupOptimizer
		implements IASTOptimizer {

    private static final transient Logger log = Logger
            .getLogger(ASTRangeCountOptimizer.class);
	
    /**
     * Optimize the join group.
     */
    @Override
	protected void optimizeJoinGroup(final AST2BOpContext ctx, 
    		final StaticAnalysis sa, final IBindingSet[] bSets,
    		final JoinGroupNode group) {

		final List<StatementPatternNode> spNodes = group.getStatementPatterns();
		
        if (!spNodes.isEmpty()) {

            // Always attach the range counts.
            attachRangeCounts(ctx, spNodes, getExogenousBindings(bSets));

        }
        
    }
	
    /**
     * Use the {@link SPORelation} from the database to grab the appropriate
     * range counts for the {@link StatementPatternNode}s. Only tries to attach
     * them if the annotation {@link Annotations#ESTIMATED_CARDINALITY} is not
     * already attached to the node. This makes it possible to write unit tests
     * without real data.
     */
    protected void attachRangeCounts(final AST2BOpContext ctx,
            final List<StatementPatternNode> spNodes,
            final IBindingSet exogenousBindings) {

        final AbstractTripleStore db = ctx.getAbstractTripleStore();

        // Setup tasks to obtain estimated range counts.
        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();
        for (StatementPatternNode sp : spNodes) {

            if (sp.getProperty(Annotations.ESTIMATED_CARDINALITY) == null) {

                tasks.add(new RangeCountTask(sp, ctx, exogenousBindings));

            }

        }

        // Obtain range counts in parallel.
        final List<Future<Void>> futures;
        try {

            futures = db.getExecutorService().invokeAll(tasks);

        } catch (InterruptedException e) {
            // propagate interrupt.
            Thread.currentThread().interrupt();
            return;
        }

        // Check futures for errors.
        final List<Throwable> causes = new LinkedList<Throwable>();
        for (Future<Void> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                log.error(e);
                causes.add(e);
            } catch (ExecutionException e) {
                log.error(e);
                causes.add(e);
            }
        }

        /*
         * If there were any errors, then throw an exception listing them.
         */
        if (!causes.isEmpty()) {
            // Throw exception back to the leader.
            if (causes.size() == 1)
                throw new RuntimeException(causes.get(0));
            throw new RuntimeException("nerrors=" + causes.size(),
                    new ExecutionExceptions(causes));
        }

    }

    /**
     * Task unconditionally obtains the range count for the
     * {@link StatementPatternNode}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class RangeCountTask implements Callable<Void> {

        private final StatementPatternNode sp;
        private final AST2BOpContext ctx;
        private final IBindingSet exogenousBindings;
        
        public RangeCountTask(final StatementPatternNode sp,
                final AST2BOpContext ctx,
                final IBindingSet exogenousBindings) {
            this.sp = sp;
            this.ctx = ctx;
            this.exogenousBindings = exogenousBindings;
        }
        
        @Override
        public Void call() throws Exception {

            estimateCardinality(sp, ctx, exogenousBindings);
            
            return null;
        }
        
    }
    
    /**
     * For testing purposes we can override this method.
     * @param sp
     * @param db
     * @param exogenousBindings
     */
	protected void estimateCardinality(StatementPatternNode sp, 
	      final AST2BOpContext ctx,
			final IBindingSet exogenousBindings) {
		final IV<?, ?> s = getIV(sp.s(), exogenousBindings);
		final IV<?, ?> p = getIV(sp.p(), exogenousBindings);
		final IV<?, ?> o = getIV(sp.o(), exogenousBindings);
		final IV<?, ?> c = getIV(sp.c(), exogenousBindings);
		
		estimateCardinalities(sp, s, p, o, c, ctx);
	}

	protected void estimateCardinalities(StatementPatternNode sp, final IV<?, ?> s, final IV<?, ?> p,
			final IV<?, ?> o, final IV<?, ?> c, final AST2BOpContext ctx) {
	   
	   final AbstractTripleStore db = ctx.getAbstractTripleStore();
		final RangeNode rangeNode = sp.getRange();
		final RangeBOp range = rangeNode != null ? rangeNode.getRangeBOp() : null;
		
		final IAccessPath<?> ap = db.getAccessPath(s, p, o, c, range);
		

		final StaticAnalysisStats saStats = ctx.getStaticAnalysisStats();
		long start = System.nanoTime();
		
		final long cardinality = ap.rangeCount(false/* exact */);

      saStats.registerRangeCountCall(System.nanoTime() - start);
		
		
		// Annotate with the fast range count.
		sp.setProperty(Annotations.ESTIMATED_CARDINALITY, cardinality);
		
		/*
		 * Annotate with the index which would be used if we did not run
		 * access path "as-bound". This is the index that will be used
		 * if we wind up doing a hash join for this predicate.
		 * 
		 * TODO It would make sense to lift this annotation into a
		 * different AST optimizer so it is always present. An
		 * optimization for index locality for as-bound evaluation
		 * depends on the presence of this annotation.
		 * 
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/150"
		 * (Choosing the index for testing fully bound access paths
		 * based on index locality)
		 */
		sp.setProperty(Annotations.ORIGINAL_INDEX, ap.getKeyOrder());
	}

    /**
     * Helper method grabs the IV out of the TermNode, doing the appropriate
     * NULL and constant/var checks.
     * 
     * @param term
     * @param exogenousBindings
     *            The externally given bindings (optional).
     */
    @SuppressWarnings("rawtypes")
    static protected IV getIV(final TermNode term,
            final IBindingSet exogenousBindings) {

        if (term != null && term.isVariable() && exogenousBindings != null) {

            @SuppressWarnings("unchecked")
            final IConstant<IV> c = (IConstant<IV>) exogenousBindings
                    .get((IVariable) term.getValueExpression());
            
            if(c != null) {
                
                return c.get();
                
            }
            
        }
        
    	if (term != null && term.isConstant()) {
    		
    		final IV iv = ((IConstant<IV>) term.getValueExpression()).get();
    		
    		if (iv == null) {
    			
    			throw new AssertionError("this optimizer cannot run with unknown IVs in statement patterns");
    			
    		}
    		
    		return iv;
    		
    	} else {
    		
    		return null;
    		
    	}
    	
    }
    
    /**
     * Return the exogenous bindings.
     * <p>
     * Note: This is considering only a single exogenous solution. It can not
     * really use more than one solution to estimate the range counts unless it
     * does the sum across all exogenous solutions and then somehow combines
     * that information in order to make a decision on a single query plan which
     * is "best" overall for those solutions.
     * <p>
     * This takes the simplifying assumption that each solution will have the
     * same pattern of bindings. This is not true of necessity, but it will be
     * true (for example) if the BINDINGS are from the openrdf API (just one
     * exogenous solution) or if the BINDINGS are being sent with a SERVICE call
     * and were generated by some pattern of non-optional JOINs.
     * <p>
     * This can get things wrong if there are variables which are only bound in
     * some of the solutions. The RTO is insensitive to that because it will
     * feed all source solutions into the first cutoff joins and thus capture
     * the estimated costs for the data, the query, and the source bindings.
     * 
     * @param bindingSets
     *            The given solutions (optional).
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/412
     *      (StaticAnalysis#getDefinitelyBound() ignores exogenous variables.)
     */
    static private IBindingSet getExogenousBindings(
            final IBindingSet[] bindingSets) {

        if (bindingSets == null || bindingSets.length == 0)
            return null;

        return bindingSets[0];

    }
    
}
