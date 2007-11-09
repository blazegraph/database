/*

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
package com.bigdata.rdf.inf;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Statistics collected when performing inference.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClosureStats {
    
    /**
     * The #of entailments that were added to the database (this includes axioms
     * and statements entailed given the data). This reports only the actual
     * change in the #of statements in the database across the closure
     * operation.
     */
    public int nentailments;

    /**
     * Time to compute the entailments and store them within the database
     * (milliseconds).
     */
    public long elapsed;

    /**
     * The rules that were executed in order by their names.
     */
    private Map<String,RuleStats> rules = new TreeMap<String,RuleStats>();

    /**
     * The statistics on the rules.
     */
    public Collection<RuleStats> getRuleStats() {
        
        return Collections.unmodifiableCollection(rules.values());
        
    }
    
    /**
     * @todo thread-safe add()
     */ 
    public void add(RuleStats stats) {
        
        RuleStats tmp = rules.get(stats.name);
        
        if(tmp==null) {
        
            rules.put(stats.name, stats);
            
        } else {
            
            tmp.add( stats );
            
        }
        
    }
    
    public void add(ClosureStats closureStats) {
        
        nentailments += closureStats.nentailments;
        
        elapsed += closureStats.elapsed;
        
        for(RuleStats ruleStats:closureStats.getRuleStats()) {
            
            add(ruleStats);
            
        }
        
    }
    
    /*
     * @todo obtain lock when generating the representation to avoid concurrent modification.
     */
    public String toString() {

        if(rules.isEmpty()) return "No rules were run.\n";
        
        StringBuilder sb = new StringBuilder();
        
        // summary
        
        sb.append("rule    \tms\t#entms\tentms/ms\n");

        long elapsed = 0;
        long numComputed = 0;
        
        for( Map.Entry<String,RuleStats> entry : rules.entrySet() ) {
            
            RuleStats stats = entry.getValue();
            
            // note: hides low cost rules (elapsed<=10)
            
            if(stats.elapsed>=10) {
            
                sb.append(stats.name + "\t" + stats.elapsed + "\t"
                        + stats.numComputed + "\t"
                        + stats.getEntailmentsPerMillisecond());

                sb.append("\n");
                
            }
            
            elapsed += stats.elapsed;
            
            numComputed += stats.numComputed;
            
        }

        sb.append("totals: elapsed="
                + elapsed
                + ", nadded="
                + nentailments
                + ", numComputed="
                + numComputed
                + ", added/sec="
                + (elapsed == 0 ? "N/A"
                        : (long) (nentailments * 1000d / elapsed))
                + ", computed/sec="
                + (elapsed == 0 ? "N/A"
                        : (long) (numComputed * 1000d / elapsed)) + "\n");
        
        /* details.
         * 
         * Note: showing details each time for high cost rules.
         */
        
//        if(InferenceEngine.DEBUG) {

            for( Map.Entry<String,RuleStats> entry : rules.entrySet() ) {

                RuleStats stats = entry.getValue();

                if(stats.elapsed>100) {

                    sb.append(stats+"\n");
                    
                }
            
            }
            
//        }
        
        return sb.toString();
        
    }
    
}
