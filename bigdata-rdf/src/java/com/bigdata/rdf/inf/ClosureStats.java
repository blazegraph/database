package com.bigdata.rdf.inf;

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
        
        sb.append("rule    \tms\t#entms\tentms/ms\n");

        for( Map.Entry<String,RuleStats> entry : rules.entrySet() ) {
            
            RuleStats stats = entry.getValue();
            
            sb.append(stats.name
                    + "\t"
                    + stats.elapsed
                    + "\t"
                    + stats.numComputed
                    + "\t"
                    + stats.getEntailmentsPerMillisecond());
            
            sb.append("\n");
            
        }

        return sb.toString();
        
    }
    
}
