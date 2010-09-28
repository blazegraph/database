/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.engine;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.bset.StartOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlan2;
import com.bigdata.relation.rule.eval.IRangeCountFactory;

/**
 * Utility class converts {@link IRule}s to {@link BOp}s.
 * <p>
 * Note: This is a stopgap measure designed to allow us to evaluate SPARQL
 * queries and verify the standalone {@link QueryEngine} while we develop a
 * direct translation from Sesame's SPARQL operator tree onto {@link BOp}s and
 * work on the scale-out query buffer transfer mechanisms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Implement Rule2BOpUtility.
 */
public class Rule2BOpUtility {

    protected static final Logger log = Logger.getLogger(Rule2BOpUtility.class);
    
    /**
     * Convert an {@link IStep} into an operator tree. This should handle
     * {@link IRule}s and {@link IProgram}s as they are currently implemented
     * and used by the {@link BigdataSail}.
     * 
     * @param step
     *            The step.
     * 
     * @return
     */
    public static PipelineOp convert(final IStep step, 
            final int startId, final QueryEngine queryEngine) {
        
        if (step instanceof IRule)
            return convert((IRule) step, startId, queryEngine);
        
        throw new UnsupportedOperationException();

    }

    /**
     * Convert a rule into an operator tree.
     * 
     * @param rule
     * 
     * @return
     */
    public static PipelineOp convert(final IRule rule, 
            final int startId, final QueryEngine queryEngine) {

        int bopId = startId;
        
        final PipelineOp startOp = new StartOp(new BOp[] {},
                NV.asMap(new NV[] {//
                        new NV(Predicate.Annotations.BOP_ID, bopId++),//
                        new NV(SliceOp.Annotations.EVALUATION_CONTEXT,
                                BOpEvaluationContext.CONTROLLER),//
                        }));
        
        /*
         * First put the tails in the correct order based on the logic in
         * DefaultEvaluationPlan2.
         */
        final BOpContextBase context = new BOpContextBase(queryEngine);
        final DefaultEvaluationPlan2 plan = new DefaultEvaluationPlan2(
                new IRangeCountFactory() {

            public long rangeCount(final IPredicate pred) {
                return context.getRelation(pred).getAccessPath(pred)
                                .rangeCount(false);
            }
            
        }, rule);
        
        final int[] order = plan.getOrder();
        
        /*
         * Map the constraints from the variables they use.  This way, we can
         * properly attach constraints to only the first tail in which the
         * variable appears.  This way we only run the appropriate constraint
         * once, instead of for every tail. 
         */
        final Map<IVariable<?>, Collection<IConstraint>> constraintsByVar = 
            new HashMap<IVariable<?>, Collection<IConstraint>>();
        for (int i = 0; i < rule.getConstraintCount(); i++) {
            final IConstraint c = rule.getConstraint(i);
            
            if (log.isDebugEnabled()) {
                log.debug(c);
            }
            
            final Set<IVariable<?>> uniqueVars = new HashSet<IVariable<?>>();
            final Iterator<IVariable<?>> vars = BOpUtility.getSpannedVariables(c);
            while (vars.hasNext()) {
                final IVariable<?> v = vars.next();
                uniqueVars.add(v);
            }
            
            for (IVariable<?> v : uniqueVars) {

                if (log.isDebugEnabled()) {
                    log.debug(v);
                }
                
                Collection<IConstraint> constraints = constraintsByVar.get(v);
                if (constraints == null) {
                    constraints = new LinkedList<IConstraint>();
                    constraintsByVar.put(v, constraints);
                }
                constraints.add(c);
            }
        }
        
        PipelineOp left = startOp;
        
        for (int i = 0; i < order.length; i++) {
            
            final int joinId = bopId++;
            
            // assign a bop id to the predicate
            final IPredicate<?> pred = rule.getTail(order[i]).setBOpId(bopId++);
            
            /*
             * Collect all the constraints for this predicate based on which
             * variables make their first appearance in this tail
             */
            final Collection<IConstraint> constraints = 
                new LinkedList<IConstraint>();
            
            /*
             * Peek through the predicate's args to find its variables. Use
             * these to attach constraints to the join based on the variables
             * that make their first appearance in this tail.
             */
            for (BOp arg : pred.args()) {
                if (arg instanceof IVariable) {
                    final IVariable<?> v = (IVariable) arg;
                    /*
                     * We do a remove because we don't ever need to run these
                     * constraints again during subsequent joins once they
                     * have been run once at the initial appearance of the
                     * variable.
                     * 
                     * FIXME revisit this when we dynamically re-order running
                     *          joins
                     */ 
                    if (constraintsByVar.containsKey(v))
                        constraints.addAll(constraintsByVar.remove(v));
                }
            }
            
            final PipelineOp joinOp = new PipelineJoin(//
                    left, pred,//
                    NV.asMap(new NV[] {//
                            new NV(BOp.Annotations.BOP_ID, joinId),//
                            new NV(PipelineJoin.Annotations.CONSTRAINTS, 
                                    constraints.size() > 0 ? 
                                            constraints.toArray(new IConstraint[constraints.size()]) : null),//
                            new NV(PipelineJoin.Annotations.OPTIONAL, pred.isOptional()),//
                            // Note: shard-partitioned joins!
                            new NV( Predicate.Annotations.EVALUATION_CONTEXT,
                                    BOpEvaluationContext.SHARDED),//
                            }));
            
            left = joinOp;
            
        }
        
        // just for now while i'm debugging
        System.err.println(toString(left));
        
        return left;
        
    }
    
    private static String toString(BOp bop) {
        
        StringBuilder sb = new StringBuilder();
        
        toString(bop, sb, 0);
        
        // chop off the last \n
        sb.setLength(sb.length()-1);
        
        return sb.toString();
        
    }
    
    private static void toString(final BOp bop, final StringBuilder sb, 
            final int indent) {
        
        for (int i = 0; i < indent; i++) {
            sb.append(' ');
        }
        sb.append(bop).append('\n');

        if (bop != null) {
            List<BOp> args = bop.args();
            for (BOp arg : args) {
                toString(arg, sb, indent+4);
            }
            IConstraint[] constraints = (IConstraint[]) bop
                    .getProperty(PipelineJoin.Annotations.CONSTRAINTS);
            if (constraints != null) {
                for (IConstraint c : constraints) {
                    toString(c, sb, indent+4);
                }
            }
        }
        
    }
    
    /**
     * Convert a program into an operator tree.
     * 
     * @param program
     * 
     * @return
     * 
     * FIXME What is the pattern for UNION?
     */
    public static PipelineOp convert(final Program program) {

        throw new UnsupportedOperationException();

    }

}
