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
import com.bigdata.relation.rule.ISlice;
import com.bigdata.relation.rule.IStep;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.striterator.IKeyOrder;

/**
 * Statistics about what an {@link IStep} did when it was executed.
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
     * Initilizes statistics for an {@link IStep}.
     * <p>
     * Note: This form is used when statistics will be aggregated across the
     * execution of multiple {@link IStep}s, such as when computing the closure
     * (fix point) of a set of {@link IRule}s or a sequential {@link IProgram}.
     * 
     * @param step
     *            The {@link IStep}.
     */
    public RuleStats(IStep step) {
       
        this.rule = step;

        this.name = step.getName();

        if (step.isRule()) {

            final int tailCount = ((IRule) step).getTailCount();

            this.evalOrder = new int[tailCount];

            this.permutation = new int[tailCount];

            this.keyOrder = new IKeyOrder[tailCount];
            
            this.chunkCount = new long[tailCount];

            this.subqueryCount = new int[tailCount];

            this.elementCount = new long[tailCount];

            this.rangeCount = new long[tailCount];
            
        } else {

            this.evalOrder = null;

            this.permutation = null;
            
            this.keyOrder = null;
            
            this.chunkCount = null;
            
            this.subqueryCount = null;
            
            this.elementCount = null;

            this.rangeCount = null;
            
        }

//        this.nexecutions = 0;

        this.aggregation = true;
        
    }

    /**
     * Initilizes statistics from an {@link iRule} and its
     * {@link IEvaluationPlan}.
     * <p>
     * Note: This ctor variant makes available the order of execution and range
     * count metadata from the {@link IEvaluationPlan}.
     * 
     * @param rule
     *            The {@link IRule}.
     * @param plan
     *            The {@link IEvaluationPlan}.
     * @param keyOrder
     *            Identifies which index will be used at each step in the
     *            evaluation plan (the indices are correlated with the tail
     *            predicate index, not the evaluation order index).
     */
    public RuleStats(IRule rule, IEvaluationPlan plan, IKeyOrder[] keyOrder) {
        
        this(rule);
        
        final int tailCount = rule.getTailCount();
        
        final int[] order = plan.getOrder();
        
        System.arraycopy(order, 0, evalOrder, 0, tailCount);

        System.arraycopy(keyOrder, 0, this.keyOrder, 0, tailCount);

        /*
         * Construct the permutation of the tail index order for the rule that
         * corresponds to the evaluation order.
         * 
         * permutation[i] is the sequence in the evaluation order at which
         * tail[i] is evaluated.
         */
        for (int i = 0; i < tailCount; i++) {

            permutation[evalOrder[i]] = i;

        }
        
        for (int i = 0; i < tailCount; i++) {
            
            /*
             * Copy range count data, etc.
             * 
             * @todo This data is normally cached, but for some rules and some
             * evaluation planners the plan is formed without requesting some
             * (or any) of the range counts. An optimization would allow us to
             * NOT force a range count if it was not already take.
             * 
             * However, a caching IRangeCountFactory might hide these costs
             * anyway.
             */
            
            rangeCount[ i ] = plan.rangeCount(i);
            
        }

        this.aggregation = false;
        
    }

    /**
     * True iff this is an aggregation of individual rule executions.
     */
    private boolean aggregation;
    
    /**
     * The name of the rule.
     */
    public final String name;
    
    /**
     * The {@link IStep} that was executed.
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
     * <p>
     * Note: this counter will be larger than the actual #of solutions generated
     * when evaluating an {@link ISlice} since the pattern used is to invoke
     * {@link AtomicLong#incrementAndGet()} and add the {@link ISolution} to the
     * {@link IBuffer} iff the post-increment value is LT
     * {@link ISlice#getLast()}. Since we are working with the post-increment
     * value when handling an {@link ISlice} this is naturally one larger than
     * the #of {@link ISolution}s actually added to the {@link IBuffer} if we
     * halt processing because we have reached the limit on the {@link ISlice}.
     */
    public AtomicLong solutionCount = new AtomicLong();
    
    /**
     * The #of elements that were actually added to (or removed from) the
     * relation indices. This is updated based on the {@link IMutableRelation}
     * API. Correct reporting by that API and correct aggregation here are
     * critical to the correct termination of the fix point of some rule set.
     * <p>
     * Note: This value is ONLY incremented when the {@link IBuffer} is flushed.
     * <p>
     * Note: Each {@link IRule} will have its own {@link RuleStats} on which it
     * reports its mutation count. However, since evaluation of {@link IRule}s
     * MAY proceed in parallel and since multiple {@link IRule}s can write on
     * the same {@link IBuffer}, the mutation counts can not be credited
     * unambiguously to any given rule when rules run in parallel.
     * <p>
     * Note: {@link IBuffer#flush()} maintains a running mutationCount.
     * Therefore only the end state of that counter should be set on
     * {@link #mutationCount}. Otherwise the reported {@link #mutationCount}
     * may overreport the actual mutation count.
     * 
     * @see #add(RuleStats)
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
     * The permutation of the tail predicate index order for an {@link IRule}
     * (only available when the {@link IStep} is an {@link IRule}) that
     * corresponds to the evaluation order of the tail predicate in the
     * {@link IRule}. <code>permutation[i]</code> is the sequence in the
     * evaluation order at which <code>tail[i]</code> was evaluated.
     */
    public final int[] permutation;
    
    /**
     * An array of the {@link IKeyOrder} that was used for each predicate in the
     * tail of the rule. The array is correlated with the predicates index in
     * the tail of the rule NOT with its evaluation order.
     */
    public final IKeyOrder[] keyOrder;
    
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
     * The #of elements considered for each predicate in the body of the rule
     * (in the order in which they were declared, not the order in which they
     * were evaluated).
     */
    public final long[] elementCount;

    /**
     * The predicated range counts for each predicate in the body of the rule
     * (in the order in which they were declared, not the order in which they
     * were evaluated) as reported by the {@link IRangeCountFactory}. The range
     * counts are used by the {@link IEvaluationPlan}. You can compare the
     * {@link #elementCount}s with the {@link #rangeCount}s to see the actual
     * vs predicated #of elements visited per predicate.
     */
    public final long[] rangeCount;
    
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
     * <dt>keyOrder</dt>
     * <dd>The {@link IKeyOrder} for the predicate(s) in the rule. Basically,
     * this tells you which index was used for each predicate.</dd>
     * <dt>chunkCount</dt>
     * <dd>The #of chunks that were generated for the left-hand side of the
     * JOIN for each predicate in the tail of the rule.</dd>
     * <dt>subqueryCount</dt>
     * <dd>The #of subqueries issued for the right-hand side of the JOIN for
     * each predicate in the tail of the rule.</dd>
     * <dt>elementCount</dt>
     * <dd>The #of elements that were actually visited for each tail predicate
     * in the rule.</dd>
     * <dt>rangeCount</dt>
     * <dd>The #of elements predicated for each tail predicate in the rule by
     * the {@link IRangeCountFactory} on behalf of the {@link IEvaluationPlan}.</dd>
     * <dt>tailIndex</dt>
     * <dd>The index in which the tail predicate(s) for rule were declared for
     * the rule. This information is present iff
     * <code>joinDetails == true</code> was specified. Since the tail
     * predicates will be written into the table in this order, this information
     * is mainly of use if you want to resort the table while keeping the
     * relationship between the predicate evaluation order and the declared
     * predicate order.</dd>
     * <dt>tailPredicate</dt>
     * <dd>The tail predicate(s) for rule in the order in which they were
     * declared for the rule. This information is present iff
     * <code>joinDetails == true</code> was specified. When present, the tail
     * predicate details will be found on their own rows in the table and will
     * be aligned under the column whose details are being broken out.</dd>
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
                + ", evalOrder, keyOrder, subqueryCount, chunkCount, elementCount, rangeCount"
                + ", tailIndex, tailPredicate"
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
     * @param joinDetails
     *            When <code>true</code> , presents a tablular display of the
     *            details for each JOIN IFF this {@link RuleStats} was collected
     *            for an {@link IRule} (rather than an aggregation of
     *            {@link IRule}).
     */
    public String toStringSimple(final int depth, final boolean titles,
            final boolean joinDetails) {
        
        // the symbol used when a count is zero.
        final String ZE = "0";
        
        // the symbol used when a count was zero, so count/sec is also zero.
        final String NA = "0";
        
        // the symbol used when the elapsed time was zero, so count/sec is divide by zero.
        final String DZ = "0";
        
        final long solutionCount = this.solutionCount.get();
        
        final String solutionCountStr = solutionCount == 0 ? ZE : "" + solutionCount;

        final String solutionsPerSec = (solutionCount == 0 ? NA //
                : (elapsed == 0L ? DZ //
                        : "" + (long) (solutionCount * 1000d / elapsed)));

        final long mutationCount = this.mutationCount.get();

        final String mutationCountStr = mutationCount == 0 ? ZE : "" + mutationCount;

        final String mutationsPerSec = (mutationCount == 0 ? NA //
                : (elapsed == 0L ? DZ //
                        : "" + (long) (mutationCount * 1000d / elapsed)));

        final String q = ""; // '\"';

        final StringBuilder sb = new StringBuilder();

        final String ruleNameStr = "\"" + depthStr.substring(0, depth) + name
                + (closureRound == 0 ? "" : " round#" + closureRound) + "\"";

        sb.append(ruleNameStr);
        sb.append( ", "+(titles?"elapsed=":"") + elapsed );
        sb.append( ", "+(titles?"solutionCount=":"") + solutionCountStr);
        sb.append( ", "+(titles?"solutions/sec=":"") + solutionsPerSec);
        sb.append( ", "+(titles?"mutationCount=":"") + mutationCountStr);
        sb.append( ", "+(titles?"mutations/sec=":"") + mutationsPerSec);
        
        if(!aggregation) {
            
            if(!joinDetails) {
            
            sb.append(", "+(titles?"evalOrder=":"")+q+toString(evalOrder)+q);
            sb.append(", "+(titles?"keyOrder=":"")+q+toString(keyOrder)+q);
            sb.append(", "+(titles?"subqueryCount=":"")+q+toString(subqueryCount)+q);
            sb.append(", "+(titles?"chunkCount=":"")+q+ toString(chunkCount)+q);
            sb.append(", "+(titles?"elementCount=":"")+q+ toString(elementCount)+q);
            sb.append(", "+(titles?"rangeCount=":"")+q+ toString(rangeCount)+q);

            } else {
                
                final IRule r = (IRule)rule;
                
                final int tailCount = r.getTailCount();
                
                for (int tailIndex = 0; tailIndex < tailCount; tailIndex++) {
                    
                    if (tailIndex > 0)
                        sb.append("\n"+ruleNameStr+",,,,,");
                    
                    final int i = showInEvalOrder?evalOrder[tailIndex]:tailIndex;
                    final int orderIndex = showInEvalOrder?tailIndex:permutation[i];
                    
                    sb.append(", "+orderIndex);
                    
                    sb.append(", "+keyOrder[i]);
                    sb.append(", "+subqueryCount[i]);
                    sb.append(", "+chunkCount[i]);
                    sb.append(", "+elementCount[i]);
                    sb.append(", "+rangeCount[i]);
                    
                    sb.append(", "+i);
                    
                    sb.append(", \""+toString(r.getTail(i))+"\"");
                    
                }
                
            }
            
        }

        return sb.toString();
        
    }

    /**
     * When <code>true</code> {@link #toStringSimple(int, boolean, boolean)}
     * will show the table view with the predicates in evaluation order rather
     * than the given order. This view is easier to read when you are examining
     * the join performance but you have to indirect through the tail index to
     * relate the predicates back to the original query form (which may already
     * have been re-ordered so that is not a great loss in many cases).
     */
    final private boolean showInEvalOrder = true;
    
    /**
     * Return a human readable representation of the predicate. Subclasses may
     * be created that know how to externalize the predicate correctly for its
     * relation. It is a good idea to strip commas from the representation so
     * that the table can be more readily imported into worksheets that exhibit
     * problems with quoted strings embedding commas. This can be done using
     * {@link String#replace(CharSequence, CharSequence)}.
     * 
     * @param pred
     *            A predicate from the tail of an {@link IRule}.
     * 
     * @return The representaiton of that predicate.
     */
    protected String toString(IPredicate pred) {
       
        return pred.toString();
        
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


    private StringBuilder toString(final Object[] a) {
        
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

        return toString(0L/* minElapsed */, true/* joinDetails */);

    }
    
    /**
     * Set this to [true] if you want a single rule to be formatted in a table,
     * just like a set of rules. Set it to [false] if you want a single rule all
     * on one line using [title=value] for each column.
     */
    static private final boolean showSingleRuleInTable = true;
    
    /**
     * 
     * @param minElapsed
     *            The minimum elapsed time for which details will be shown.
     * @param joinDetails
     *            When <code>true</code>, also presents a tablular display of
     *            the details for each JOIN in each {@link IRule}.
     */
    public String toString(final long minElapsed, final boolean joinDetails) {
            
        final int depth = 0;
        
        if (detailStats.isEmpty() && !showSingleRuleInTable) {

            return toStringSimple(depth, true/* titles */, joinDetails);
            
        }
        
        final StringBuilder sb = new StringBuilder();
        
        // Note: uses Vector.toArray() to avoid concurrent modification issues.
        final RuleStats[] a = detailStats.toArray(new RuleStats[] {});

        // aggregate level.
        sb.append("\n" + getHeadings());

        sb.append("\n" + toStringSimple(depth, false/* titles */, joinDetails));

        toString(minElapsed, joinDetails, depth+1, sb, a);

        return sb.toString();

    }

    private StringBuilder toString(final long minElapsed,
            final boolean joinDetails, final int depth, final StringBuilder sb,
            final RuleStats[] a) {

        // detail level.
        for (int i = 0; i < a.length; i++) {

            final RuleStats x = (RuleStats) a[i];

            if (x.elapsed >= minElapsed) {

                sb.append("\n" + x.toStringSimple(depth, false/* titles */, joinDetails));

                if (x.aggregation && !x.detailStats.isEmpty()) {

                    toString(minElapsed, joinDetails, depth + 1, sb,
                            x.detailStats.toArray(new RuleStats[] {}));

                }

            }

        }

        return sb;
        
    }
    
    /**
     * When execution {@link RuleState}s are being aggregated, this will contain
     * the individual {@link RuleStats} for each execution {@link RuleState}. 
     */
    public List<RuleStats> detailStats = new Vector<RuleStats>();
    
    /**
     * Aggregates statistics.
     * <p>
     * Note: since the mutation count as reported by each buffer is cumulative
     * we DO NOT aggregate the mutation counts from sub-steps as it would cause
     * double-counting when steps are executed in parallel.
     * <p>
     * Instead, once the total program is finished, the total mutation count is
     * computed as the value reported by {@link IBuffer#flush()} for each buffer
     * on which the program writes.
     * 
     * @param o
     *            Statistics for another rule.
     *            
     * @see ProgramTask#executeMutation(IStep)
     * @see MutationTask#flushBuffers(IJoinNexus, RuleStats, java.util.Map)
     */
    synchronized public void add(RuleStats o) {
    
        if (o == null)
            throw new IllegalArgumentException();
        
        detailStats.add(o);
        
        if (elementCount != null && o.elementCount != null) {

            for (int i = 0; i < elementCount.length; i++) {

                // Note: order[] is NOT aggregated.

                chunkCount[i] += o.chunkCount[i];

                subqueryCount[i] += o.subqueryCount[i];

                elementCount[i] += o.elementCount[i];

                rangeCount[i] += o.rangeCount[i];

            }
            
        }
        
        solutionCount.addAndGet( o.solutionCount.get() );

        // See javadoc above.
//        mutationCount.addAndGet(o.mutationCount.get());
    
        elapsed += o.elapsed;
    
    }

}
