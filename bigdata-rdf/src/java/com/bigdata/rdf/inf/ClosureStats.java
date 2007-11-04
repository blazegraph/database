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
    
    /*
     * @todo obtain lock when generating the representation to avoid concurrent modification.
     */
    public String toString() {

        StringBuilder sb = new StringBuilder();
        
        // summary
        
        sb.append("rule    \tms\t#entms\tentms/ms\n");

        long elapsed = 0;
        long numComputed = 0;
        
        for( Map.Entry<String,RuleStats> entry : rules.entrySet() ) {
            
            RuleStats stats = entry.getValue();
            
            // @todo consider hiding low cost rules (elapsed<=10)
            
            sb.append(stats.name
                    + "\t"
                    + stats.elapsed
                    + "\t"
                    + stats.numComputed
                    + "\t"
                    + stats.getEntailmentsPerMillisecond());
            
            sb.append("\n");
            
            elapsed += stats.elapsed;
            
            numComputed += stats.numComputed;
            
        }

        sb.append("totals: elapsed=" + elapsed + ", numComputed="
                        + numComputed + ", entailments/sec="
                        + (elapsed==0?"N/A":(long) (numComputed * 1000d / elapsed)+"\n"));
        
        /* details.
         * 
         * @todo consider showing details each time for high cost rules.
         */
        
        if(InferenceEngine.DEBUG) {

            for( Map.Entry<String,RuleStats> entry : rules.entrySet() ) {

                sb.append(entry.getValue()+"\n");
            
            }
            
        }
        
        return sb.toString();
        
    }
    
}
