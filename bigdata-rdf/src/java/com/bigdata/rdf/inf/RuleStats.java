/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 30, 2007
 */

package com.bigdata.rdf.inf;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import com.bigdata.rdf.inf.Rule.State;

/**
 * Statistics about what the Rule did.
 * 
 * @todo it would be nice to report the #of new entailments and not just the #of
 *       computed entailments. The {@link SPOBuffer} knows how many new
 *       entailments are written each time the buffer is
 *       {@link SPOBuffer#flush()}ed. However, the buffer can be flushed more
 *       than once during the execution of the rule, so we need to aggregate
 *       those counts across flushes and then reset once the rule is done. Also
 *       note that we sometimes defer a flush across rules if only a few
 *       entailments were generated, so correct reporting would mean modifying
 *       that practice. One approach is to modify {@link SPOBuffer#flush()} to
 *       accept a boolean argument. When true, the #of statements written by the
 *       buffer would be reset after the flush. When false the counter would
 *       continue to aggregate across flushes. The javadoc on flush would have
 *       to indicate that the return value was a running sum since the last
 *       reset of the counter.
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

        int n = getStatementCount();
        
        String stmtsPerSec = (n == 0 ? "N/A" : (elapsed == 0L ? "0" : ""
                + (long) (n * 1000d / elapsed)));

        return name //
             + (!aggregate
                     ?(", focusIndex="+focusIndex+//
                       ", #order="+Arrays.toString(order)//
                       )
                     :(", #exec="+nexecutions//
                     ))
             + ", #subqueries="+Arrays.toString(nsubqueries)//
             + ", #stmts=" + Arrays.toString(nstmts) //
             + ", #computed=" + numComputed//
             + ", elapsed=" + elapsed//
             + ", stmts/sec="+stmtsPerSec//
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
