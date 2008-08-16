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

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Rule;
import com.bigdata.service.ILoadBalancerService;

/**
 * Statistics about what the {@link IProgram} did.
 * <p>
 * Program execution has the general form of either a set of {@link IStep}s
 * executed, at least logically, in parallel, or a sequence of {@link IStep}s
 * executed in sequence. An {@link IStep} may be a closure operation of one or
 * more {@link IRule}s, even when it is the top-level {@link IStep}. Inside of
 * a closure operation, there are one or more rounds and each rule in the
 * closure will be run in each round. There is no implicit order on the rules
 * within a closure operation, but they may be forced to execute in a sequence
 * if the total program execution context forces sequential execution.
 * <p>
 * In order to aggregate the data on rule execution, we want to roll up the data
 * for the individual rules along the same lines as the program structure.
 * 
 * @todo Report as counters aggregated by the {@link ILoadBalancerService}?
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
    public RuleStats(IStep program) {
       
        this.rule = program;

        this.name = program.getName();

        if (program.isRule()) {

            final int tailCount = ((IRule) program).getTailCount();

            this.chunkCount = new long[tailCount];

            this.elementCount = new long[tailCount];

            this.subqueryCount = new int[tailCount];

            this.evalOrder = new int[tailCount];

        } else {

            this.chunkCount = null;
            
            this.elementCount = null;

            this.subqueryCount = null;

            this.evalOrder = null;

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
        
        System.arraycopy(ruleState.order, 0, evalOrder, 0, ruleState.order.length);

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
    public final IStep rule;
    
    /**
     * The round is zero unless this is a closure operations and then it is an
     * integer in [1:nrounds]. 
     */
    public int closureRound = 0;
    
    /**
     * The #of {@link ISolution}s computed by the rule regardless of whether or
     * not they are written onto an {@link IMutableRelation} and regardless of
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

    /*
     * The following are only available for the execution of a single rule.
     */
    
    /**
     * The order of execution of the predicates in the body of a rule (only
     * available at the detail level of a single rule instance execution). When
     * aggregated, the {@link #evalOrder} will always contain zeros since it can
     * not be meaningfully combined across executions of either the same or
     * different rules.
     */
    public final int[] evalOrder;
    
    /**
     * The #of chunks materialized for each predicate in the body of the rule
     * (in the order in which they were declared, not the order in which they
     * were evaluated).
     */
    public final long[] chunkCount;
    
    /**
     * The #of subqueries examined for each predicate in the rule (in the order
     * in which they were declared, not the order in which they were evaluated).
     * While there are N indices for a rule with N predicates, we only evaluate
     * a subquery for N-1 predicates so at least one index will always be
     * zero(0).
     */
    public final int[] subqueryCount;
    
    /**
     * The #of elements considered for the each predicate in the body of the
     * rule (in the order in which they were declared, not the order in which
     * they were evaluated).
     */
    public final long[] elementCount;

    /**
     * The #of elements considered by the program (total over
     * {@link #elementCount}).
     */
    public int getElementCount() {

        int n = 0;

        for (int i = 0; i < elementCount.length; i++) {

            n += elementCount[i];

        }

        return n;
        
    }
    
    /**
     * Returns the headings.
     * <p>
     * The following are present for every record.
     * <dl>
     * <dt>rule</dt>
     * <dd>The name of the rule.</dd>
     * <dt>elapsed</dt>
     * <dd>Elapsed execution time in milliseconds. When the rules are executed
     * concurrently the times will not be additive.</dd>
     * <dt>solutionCount</dt>
     * <dd>The #of solutions computed.</dd>
     * <dt>solutions/sec</dt>
     * <dd>The #of solutions computed per second.</dd>
     * <dt>mutationCount</dt>
     * <dd>The #of solutions that resulted in mutations on a relation (i.e.,
     * the #of distinct and new solutions). This will be zero unless the rule is
     * writing on a relation.</dd>
     * <dt>mutations/sec</dt>
     * <dd>The #of mutations per second.</dd>
     * </dl>
     * The following are only present for individual {@link IRule} execution
     * records. Each of these is an array containing one element per tail
     * predicate in the {@link IRule}.
     * <dl>
     * <dt>evalOrder</dt>
     * <dd>The evaluation order for the predicate(s) in the rule.</dd>
     * <dt>chunkCount</dt>
     * <dd>The #of chunks that were generated for the left-hand side of the
     * JOIN for each predicate in the tail of the rule.</dd>
     * <dt>subqueryCount</dt>
     * <dd>The #of subqueries issued for the right-hand side of the JOIN for
     * each predicate in the tail of the rule.</dd>
     * <dt>elementCount</dt>
     * <dd>The #of elements materialized from each tail predicate in the rule.</dd>
     * </dl>
     * 
     * @todo collect data on the size of the subquery results. A large #of
     *       subqueries with a small number of results each is the main reason
     *       to unroll the JOIN loops.
     * 
     * @todo we are actually aggregating the elapsed time, which is a no-no as
     *       pointed out above when there are rules executing concurrently.
     */
    public String getHeadings() {
     
        return "rule, elapsed"
                + ", solutionCount, solutions/sec, mutationCount, mutations/sec"
                + ", evalOrder, subqueryCount, chunkCount, elementCount"
        ;
        
    }
    
    /**
     * Reports just the data for this record.
     * 
     * @param depth
     *            The depth at which the record was encountered within some
     *            top-level aggregation.
     * @param titles
     *            When <code>true</code> the titles will be displayed inline,
     *            e.g., <code>foo=12</code> vs <code>12</code>.
     */
    public String toStringSimple(int depth, boolean titles) {
        
        // the symbol used when a count was zero, so count/sec is also zero.
        final String NA = "";
        
        // the symbol used when the elapsed time was zero, so count/sec is divide by zero.
        final String DZ = "";
        
        final String solutionsPerSec = (solutionCount == 0 ? NA
                : (elapsed == 0L ? DZ : ""
                        + (long) (solutionCount * 1000d / elapsed)));

        final long mutationCount = this.mutationCount.get();
        
        final String mutationsPerSec = (mutationCount == 0 ? NA : (elapsed == 0L ? DZ : ""
                + (long) (mutationCount * 1000d / elapsed)));

        final String q = ""; //'\"';

        return "\""+depthStr.substring(0,depth)+name+(closureRound==0?"":" round#"+closureRound)+"\""//
             + ", "+(titles?"elapsed=":"") + elapsed//
             + ", "+(titles?"solutionCount=":"") + solutionCount//
             + ", "+(titles?"solutions/sec=":"") + solutionsPerSec//
             + ", "+(titles?"mutationCount=":"") + mutationCount//
             + ", "+(titles?"mutations/sec=":"") + mutationsPerSec//
             + (!aggregate
                     ?(", "+(titles?"evalOrder=":"")+q+toString(evalOrder)+q //
                     + ", "+(titles?"subqueryCount=":"")+q+toString(subqueryCount)+q //
                     + ", "+(titles?"chunkCount=":"")+q+ toString(chunkCount)+q //
                     + ", "+(titles?"elementCount=":"")+q+ toString(elementCount)+q //
                     )
                     :(""//", #exec="+nexecutions//
                     ))
             ;

    }
    
    private String depthStr = ".........";
    
    private StringBuilder toString(final int[] a) {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("[");
        
        for (int i = 0; i < a.length; i++) {
            
            if (i > 0)
                sb.append(" ");
            
            sb.append(a[i]);
            
        }
        
        sb.append("]");
        
        return sb;
        
    }

    private StringBuilder toString(final long[] a) {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("[");
        
        for (int i = 0; i < a.length; i++) {
            
            if (i > 0)
                sb.append(" ");
            
            sb.append(a[i]);
            
        }
        
        sb.append("]");
        
        return sb;
        
    }

    /**
     * Reports aggregate and details.
     */
    public String toString() {

        return toString(0L/*minElapsed*/);
        
    }
    
    /**
     * 
     * @param minElapsed
     *            The minimum elapsed time for which details will be shown.
     * 
     */
    public String toString(long minElapsed) {
            
        final int depth = 0;
        
        if (detailStats.isEmpty())
            return toStringSimple(depth, true/* titles */);
        
        final StringBuilder sb = new StringBuilder();
        
        // Note: uses Vector.toArray() to avoid concurrent modification issues.
        final RuleStats[] a = detailStats.toArray(new RuleStats[] {});

        // aggregate level.
        sb.append("\n" + getHeadings());

        sb.append("\n" + toStringSimple(depth, false/* titles */));

        toString(minElapsed, depth+1, sb, a);

        return sb.toString();

    }

    private StringBuilder toString(long minElapsed, int depth,
            StringBuilder sb, RuleStats[] a) {

        // detail level.
        for (int i = 0; i < a.length; i++) {

            final RuleStats x = (RuleStats) a[i];

            if (x.elapsed >= minElapsed) {

                sb.append("\n" + x.toStringSimple(depth, false/* titles */));

                if (x.aggregate && !x.detailStats.isEmpty()) {

                    toString(minElapsed, depth + 1, sb, x.detailStats
                            .toArray(new RuleStats[] {}));

                }

            }

        }

        return sb;
        
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

                chunkCount[i] += o.chunkCount[i];

                elementCount[i] += o.elementCount[i];

                subqueryCount[i] += o.subqueryCount[i];

                // Note: order[] is NOT aggregated.

            }
            
        }
        
        solutionCount += o.solutionCount;
        
        mutationCount.addAndGet(o.mutationCount.get());
    
        elapsed += o.elapsed;
    
    }
   
//    public static class ClosureStats extends RuleStats {
//        
//    }
//    
//    public static class SequenceStats extends RuleStats {
//        
//    }
//
//    public static class ProgramStats extends RuleStats {
//        
//    }

}
