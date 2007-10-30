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

/**
 * Statistics about what the Rule did.
 * 
 * @author mikep
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleStats {

    RuleStats(Rule rule) {
       
        this.rule = rule;
        
        this.name = rule.getName();
        
        this.nstmts = new int[rule.body.length];

        this.nsubqueries = new int[rule.body.length];
        
    }

    /**
     * The name of the rule.
     */
    public final String name;
    
    /**
     * The rule itself.
     */
    public final Rule rule;
    
    /**
     * The #of rounds that have been executed for this rule.
     */
    public int nrounds;
    
    /**
     * The #of statement considered for the each predicate in the body of the
     * rule (in the order in which they were declared, not the order in which
     * they were evaluated).
     */
    public int[] nstmts;

    /**
     * The #of subqueries examined for each predicate in the rule. The indices
     * are correlated 1:1 with the order in which the predicates were declared.
     * While there are N indices for a rule with N predicates, we only evaluate
     * a subquery for N-1 predicates so at least one index will always be
     * zero(0).
     */
    public int[] nsubqueries;
    
    /**
     * #of entailments computed by the rule (does not consider whether or not
     * the entailments were pre-existing in the database nor whether or not the
     * entailments filtered out such that they did not enter the database).
     */
    public int numComputed;
    
    /**
     * Time to compute the entailments (ms).
     * 
     * @todo convert this to nano seconds?
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
    
    public String toString() {

        int n = getStatementCount();
        
        String stmtsPerSec = (n == 0 ? "N/A" : (elapsed == 0L ? "0" : ""
                + ((long) (((double) n) / ((double) elapsed) * 1000d))));

        return name //
             + ", #rounds="+nrounds//
             + ", #stmts=" + Arrays.toString(nstmts) //
             + ", #subqueries="+Arrays.toString(nsubqueries)//
             + ", #computed=" + numComputed//
             + ", elapsed=" + elapsed//
             + ", stmts/sec="+stmtsPerSec//
             ;

    }

    /**
     * Resets all of the counters.
     */
    public void reset() {
        
        for(int i=0; i<nstmts.length; i++) {

            nstmts[i] = nsubqueries[i] = 0;

        }

        nrounds = 0;
        
        numComputed = 0;
        
        elapsed = 0L;
        
    }

    /**
     * Aggregates statistics.
     * 
     * @param o Statistics for another rule.
     */
    public void add(RuleStats o) {
        
        for(int i=0; i<nstmts.length; i++) {

            nstmts[i] += o.nstmts[i];
            
            nsubqueries[i] += o.nsubqueries[i];

        }
        
        nrounds += o.nrounds;
        
        numComputed += o.numComputed;
    
        elapsed += o.elapsed;
    
    }
    
}
