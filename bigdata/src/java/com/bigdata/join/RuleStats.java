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

package com.bigdata.join;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import com.bigdata.join.Rule.State;

/**
 * Statistics about what the Rule did.
 * 
 * @todo it would be nice to report the #of new entailments and not just the #of
 *       computed entailments. The {@link SPOAssertionBuffer} knows how many new
 *       entailments are written each time the buffer is
 *       {@link SPOAssertionBuffer#flush()}ed. However, the buffer can be
 *       flushed more than once during the execution of the rule, so we need to
 *       aggregate those counts across flushes and then reset once the rule is
 *       done. Also note that we sometimes defer a flush across rules if only a
 *       few entailments were generated, so correct reporting would mean
 *       modifying that practice.
 * 
 * @author mikep
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleStats {

    /**
     * Initilizes statistics for a {@link Rule}.
     * <p>
     * Note: This form is used when statistics will be aggregated across
     * multiple execution {@link State}s for the same rule.
     * 
     * @param rule The rule.
     */
    RuleStats(Rule rule) {
       
        this.rule = rule;
        
        this.name = rule.getName();
        
        this.nstmts = new int[rule.body.length];

        this.nsubqueries = new int[rule.body.length];
        
        this.order = new int[rule.body.length];
        
        this.nexecutions = 0;
        
        this.aggregate = true;
        
    }

    /**
     * Initilizes statistics from a {@link Rule}'s execution {@link State}.
     * <p>
     * Note: This makes the order of execution for the body predicates
     * available.
     * 
     * @param state
     *            The rule execution state.
     */
    RuleStats(State state) {
        
        this(state.getRule());
        
        System.arraycopy(state.order, 0, order, 0, state.order.length);

        this.nexecutions = 1;
        
        this.aggregate = false;
        
        this.focusIndex = state.focusIndex;
        
        // @todo report the focus index?
        
    }

    /**
     * True iff this is an aggregation of individual rule execution {@link State}s.
     */
    private boolean aggregate;
    
    /**
     * The name of the rule.
     */
    public final String name;
    
    /**
     * The rule itself.
     */
    public final Rule rule;
    
    /**
     * The #of times that the rule has been executed (ONE(1) if executed once;
     * N if executed N times when evaluating an N predicate rule during truth
     * maintenance).
     */
    public int nexecutions;

    /**
     * This field has meaning iff the rule is being used for truth maintenance,
     * in which case it is the index of the predicate in the body that will read
     * from the [focusStore] rather than the fused view [focusStore+database].
     * The field is only available for detail records.
     */
    public int focusIndex = -1;
    
    /**
     * The #of statement considered for the each predicate in the body of the
     * rule (in the order in which they were declared, not the order in which
     * they were evaluated).
     */
    public final int[] nstmts;

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
     * available at the detail level of a single {@link State} execution. When
     * aggregated, the order[] will always contain zeros since it can not be
     * meaningfully combined across executions.
     */
    public final int[] order;
    
    /**
     * #of entailments computed by the rule (does not consider whether or not
     * the entailments were pre-existing in the database nor whether or not the
     * entailments filtered out such that they did not enter the database).
     */
    public int numComputed;
    
    /**
     * The #of entailments that were actually added to the statement indices
     * (that is to say, the entailments were new to the database). This is
     * computed using {@link AbstractTripleStore#getStatementCount()} before and
     * after the rule is applied.
     */
    public int numAdded;
    
    /**
     * Time to compute the entailments (ms).
     */
    public long elapsed;

    /**
     * The #of statements considered by the rule (total over {@link #nstmts}).
     */
    public int getStatementCount() {
        
        int n = 0;
        
        for(int i=0; i<nstmts.length; i++) {
            
            n += nstmts[i];
            
        }
        
        return n;
        
    }
    
    public String getEntailmentsPerMillisecond() {

        return (elapsed == 0 ? "N/A" : "" + numComputed / elapsed);
        
    }
    
    /**
     * Reports only the aggregate.
     */
    public String toStringSimple() {

//        int n = getStatementCount();
        
        String computedPerSec = (numComputed == 0 ? "N/A"
                : (elapsed == 0L ? "0" : ""
                        + (long) (numComputed * 1000d / elapsed)));

        String newPerSec = (numAdded == 0 ? "N/A" : (elapsed == 0L ? "0" : ""
                + (long) (numAdded * 1000d / elapsed)));

        return name //
             + (!aggregate
                     ?(", focusIndex="+focusIndex+//
                       ", #order="+Arrays.toString(order)//
                       )
                     :(", #exec="+nexecutions//
                     ))
             + ", #subqueries="+Arrays.toString(nsubqueries)//
             + ", #stmts=" + Arrays.toString(nstmts) //
             + ", elapsed=" + elapsed//
             + ", #computed=" + numComputed//
             + ", computed/sec="+computedPerSec//
             + ", #new=" + numAdded//
             + ", new/sec="+newPerSec//
             ;

    }

    /**
     * Reports aggregate and details.
     */
    public String toString() {
        
        if(detailStats.isEmpty()) return toStringSimple();
        
        Object[] a = detailStats.toArray();
        
        StringBuilder sb = new StringBuilder();
        
        // aggregate level.
        sb.append("total : "+toStringSimple());
        
        // detail level.
        for( int i = 0; i<a.length; i++ ) {
            
            RuleStats x = (RuleStats)a[i];
            
            sb.append("\n");
            
            sb.append("detail: "+x.toString());
            
        }
        
        return sb.toString();
        
    }
    
//    /**
//     * Resets all of the counters.
//     */
//    public void reset() {
//        
//        for(int i=0; i<nstmts.length; i++) {
//
//            nstmts[i] = nsubqueries[i] = order[i] = 0;
//
//        }
//
//        nexecutions = 0;
//        
//        numComputed = 0;
//        
//        elapsed = 0L;
//        
//    }

    /**
     * When execution {@link State}s are being aggregated, this will contain
     * the individual {@link RuleStats} for each execution {@link State}. 
     */
    public List<RuleStats> detailStats = new Vector<RuleStats>();
    
    /**
     * Aggregates statistics.
     * 
     * @param o Statistics for another rule.
     */
    public void add(RuleStats o) {
    
        detailStats.add(o);
        
        for(int i=0; i<nstmts.length; i++) {

            nstmts[i] += o.nstmts[i];
            
            nsubqueries[i] += o.nsubqueries[i];

            // Note: order[] is NOT aggregated.
            
        }
        
        nexecutions += o.nexecutions;
        
        numComputed += o.numComputed;
    
        elapsed += o.elapsed;
    
    }
    
}
