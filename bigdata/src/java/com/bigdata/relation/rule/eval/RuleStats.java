/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 30, 2007
 */

package com.bigdata.relation.rule.eval;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;
import com.bigdata.service.ILoadBalancerService;

/**
 * Statistics about what the {@link IProgram} did.
 * 
 * @todo Report as counters aggregated by the {@link ILoadBalancerService}?
 * 
 * FIXME factor out an interface since this is being used by
 * {@link Rule#getQueryCallable(IJoinNexus, com.bigdata.relation.accesspath.IBlockingBuffer)}
 * and {@link Rule#getMutationCallable(ActionEnum, IJoinNexus, IBuffer)}
 * 
 * @author mikep
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleStats {

    /**
     * Initilizes statistics for a {@link Rule}.
     * <p>
     * Note: This form is used when statistics will be aggregated across the
     * execution of multiple steps in a program, including computing the closure
     * (fix point) of the program.
     * 
     * @param program
     *            The program.
     * 
     * @todo when aggregating for a rule, we need to identify all rules that are
     *       derived from the same rule whether by a re-write (for truth
     *       maintenance) or specialization. Those should all get rolled up
     *       under the same base rule, but probably not crossing a fixed point
     *       closure. The total should get rolled up under the top-level
     *       program.
     */
    public RuleStats(IProgram program) {
       
        this.rule = program;

        this.name = program.getName();

        if (program.isRule()) {

            final int tailCount = ((IRule) program).getTailCount();

            this.elementCount = new long[tailCount];

            this.nsubqueries = new int[tailCount];

            this.order = new int[tailCount];

        } else {

            this.elementCount = null;

            this.nsubqueries = null;

            this.order = null;

        }

//        this.nexecutions = 0;

        this.aggregate = true;
        
    }

    /**
     * Initilizes statistics from a {@link Rule}'s execution {@link RuleState}.
     * <p>
     * Note: This ctor variant makes the order of execution for the
     * {@link IPredicate}s in the {@link Rule} available.
     * 
     * @param ruleState
     *            The rule execution state.
     */
    RuleStats(RuleState ruleState) {
        
        this(ruleState.getRule());
        
        System.arraycopy(ruleState.order, 0, order, 0, ruleState.order.length);

//        this.nexecutions = 1;
        
        this.aggregate = false;
        
    }

    /**
     * True iff this is an aggregation of individual rule execution {@link RuleState}s.
     */
    private boolean aggregate;
    
    /**
     * The name of the rule.
     */
    public final String name;
    
    /**
     * The rule itself.
     */
    public final IProgram rule;
    
    /**
     * The #of elements considered for the each predicate in the body of the
     * rule (in the order in which they were declared, not the order in which
     * they were evaluated).
     */
    public final long[] elementCount;

    /**
     * The #of subqueries examined for each predicate in the rule. The indices
     * are correlated 1:1 with the order in which the predicates were declared.
     * While there are N indices for a rule with N predicates, we only evaluate
     * a subquery for N-1 predicates so at least one index will always be
     * zero(0).
     */
    public final int[] nsubqueries;
    
    /**
     * The order of execution of the predicates in the body of the rule (only
     * available at the detail level of a single {@link RuleState} execution. When
     * aggregated, the order[] will always contain zeros since it can not be
     * meaningfully combined across executions.
     */
    public final int[] order;
    
    /**
     * #of {@link ISolution}s computed by the rule regardless of whether or not
     * they are written onto an {@link IMutableRelation} and regardless of
     * whether or not they duplicate a solution already computed.
     */
    public long solutionCount;
    
    /**
     * The #of elements that were actually added to (or removed from) the
     * relation indices. This is updated based on the {@link IMutableRelation}
     * API. Correct reporting by that API and correct aggregation here are
     * critical to the correct termination of the fix point of some rule set.
     * <p>
     * Note: This value is ONLY incremented when the {@link IBuffer} is flushed.
     * Since evaluation can multiple the rules writing on a buffer and since
     * more than one rule may be run (in parallel or in sequence) before the
     * {@link IBuffer} is flushed, the mutation count will often be non-zero
     * except for the top-level {@link IProgram} that is being executed.
     */
    public AtomicLong mutationCount = new AtomicLong();
    
    /**
     * Time to compute the entailments (ms).
     */
    public long elapsed;

    /**
     * The #of elements considered by the program (total over
     * {@link #elementCount}).
     */
    public int getElementCount() {
        
        int n = 0;
        
        for(int i=0; i<elementCount.length; i++) {
            
            n += elementCount[i];
            
        }
        
        return n;
        
    }
    
    public String getEntailmentsPerMillisecond() {

        return (elapsed == 0 ? "N/A" : "" + solutionCount / elapsed);
        
    }
    
    /**
     * Reports only the aggregate.
     */
    public String toStringSimple() {
        
        final String computedPerSec = (solutionCount == 0 ? "N/A"
                : (elapsed == 0L ? "0" : ""
                        + (long) (solutionCount * 1000d / elapsed)));

        final long mutationCount = this.mutationCount.get();
        
        final String newPerSec = (mutationCount == 0 ? "N/A" : (elapsed == 0L ? "0" : ""
                + (long) (mutationCount * 1000d / elapsed)));

        return name //
             + (!aggregate
                     ?(", #order="+Arrays.toString(order)//
                       )
                     :(""//", #exec="+nexecutions//
                     ))
             + ", #subqueries="+Arrays.toString(nsubqueries)//
             + ", elementCount=" + Arrays.toString(elementCount) //
             + ", elapsed=" + elapsed//
             + ", solutionCount=" + solutionCount//
             + ", computed/sec="+computedPerSec//
             + ", mutationCount=" + mutationCount//
             + ", new/sec="+newPerSec//
             ;

    }

    /**
     * Reports aggregate and details.
     */
    public String toString() {
        
        if(detailStats.isEmpty()) return toStringSimple();
        
        // Note: uses Vector.toArray() to avoid concurrent modification issues.
        final RuleStats[] a = detailStats.toArray(new RuleStats[] {});
        
        final StringBuilder sb = new StringBuilder();
        
        // aggregate level.
        sb.append("\ntotal : "+toStringSimple());
        
        // detail level.
        for( int i = 0; i<a.length; i++ ) {
            
            RuleStats x = (RuleStats)a[i];
            
            sb.append("\n");
            
            sb.append("detail: "+x.toString());
            
        }
        
        return sb.toString();
        
    }
    
    /*
     * FIXME restore code that examines the cost to summarize by rule.
     */
//    public String toString() {
//
//        if(rules.isEmpty()) return "No rules were run.\n";
//        
//        StringBuilder sb = new StringBuilder();
//        
//        // summary
//        
//        sb.append("rule    \tms\t#entms\tentms/ms\n");
//
//        long elapsed = 0;
//        long numComputed = 0;
//        
//        for( Map.Entry<String,RuleStats> entry : rules.entrySet() ) {
//            
//            RuleStats stats = entry.getValue();
//            
//            // note: hides low cost rules (elapsed<=10)
//            
//            if(stats.elapsed>=10) {
//            
//                sb.append(stats.name + "\t" + stats.elapsed + "\t"
//                        + stats.solutionCount + "\t"
//                        + stats.getEntailmentsPerMillisecond());
//
//                sb.append("\n");
//                
//            }
//            
//            elapsed += stats.elapsed;
//            
//            numComputed += stats.solutionCount;
//            
//        }
//
//        sb.append("totals: elapsed="
//                + elapsed
//                + ", nadded="
//                + nentailments
//                + ", numComputed="
//                + numComputed
//                + ", added/sec="
//                + (elapsed == 0 ? "N/A"
//                        : (long) (nentailments * 1000d / elapsed))
//                + ", computed/sec="
//                + (elapsed == 0 ? "N/A"
//                        : (long) (numComputed * 1000d / elapsed)) + "\n");
//        
//        /* details.
//         * 
//         * Note: showing details each time for high cost rules.
//         */
//        
////        if(InferenceEngine.DEBUG) {
//
//            for( Map.Entry<String,RuleStats> entry : rules.entrySet() ) {
//
//                RuleStats stats = entry.getValue();
//
//                if(stats.elapsed>100) {
//
//                    sb.append(stats+"\n");
//                    
//                }
//            
//            }
//            
////        }
//        
//        return sb.toString();
//        
//    }

    /**
     * When execution {@link RuleState}s are being aggregated, this will contain
     * the individual {@link RuleStats} for each execution {@link RuleState}. 
     */
    public List<RuleStats> detailStats = new Vector<RuleStats>();
    
    /**
     * Aggregates statistics.
     * 
     * @param o Statistics for another rule.
     */
    synchronized public void add(RuleStats o) {
    
        if (o == null)
            throw new IllegalArgumentException();
        
        detailStats.add(o);
        
        if (elementCount != null && o.elementCount != null) {

            for (int i = 0; i < elementCount.length; i++) {

                elementCount[i] += o.elementCount[i];

                nsubqueries[i] += o.nsubqueries[i];

                // Note: order[] is NOT aggregated.

            }
            
        }
        
        solutionCount += o.solutionCount;
        
        mutationCount.addAndGet(o.mutationCount.get());
    
        elapsed += o.elapsed;
    
    }
    
}
