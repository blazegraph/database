package com.bigdata.rdf.inf;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import com.bigdata.rdf.inf.Rule.RuleStats;

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
    public int numComputed;

    /**
     * Time to compute the entailments and store them within the database
     * (milliseconds).
     */
    public long elapsed;

    /**
     * The rules that were executed and their statistics.
     * 
     * @see Rule#stats
     * @see RuleStats
     */
    private Map<String,Rule> rules = new TreeMap<String,Rule>();

    public void add(Rule rule) {
        
        rules.put(rule.getName(),rule);
        
    }
    
    public void addAll(Collection<Rule> rules) {
        
        for(Rule r : rules) {
            
            add(r);
            
        }
        
    }
    
    public String toString() {

        StringBuilder sb = new StringBuilder();
        
        sb.append("rule    \tms\t#entms\tentms/ms\n");
        
        for( Map.Entry<String,Rule> entry : rules.entrySet() ) {
            
            Rule r = entry.getValue();
            
            sb.append(r.getName()
                    + "\t"
                    + r.stats.elapsed
                    + "\t"
                    + r.stats.numComputed
                    + "\t"
                    + r.stats.getEntailmentsPerMillisecond());
            
            sb.append("\n");
            
        }

        return sb.toString();
        
    }
    
}
