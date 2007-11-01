package com.bigdata.rdf.rio;

import com.bigdata.rdf.inf.ClosureStats;

/**
 * Used to report statistics when loading data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadStats {

    public long toldTriples;
    public long loadTime;
    public long commitTime;
    public long totalTime;
    
    /**
     * Set iff the closure is computed as the data are loaded.
     */
    public ClosureStats closureStats;
    
    public long triplesPerSecond() {
        
        return ((long)( ((double)toldTriples) / ((double)totalTime) * 1000d ));
        
    }
    
    public void add(LoadStats stats) {
        
        toldTriples += stats.toldTriples;
        
        loadTime += stats.loadTime;
        
        commitTime += stats.commitTime;
        
        totalTime += stats.totalTime;
        
    }
    
    /**
     * Human readable representation.
     */
    public String toString() {

        return toldTriples
                + " stmts added in "
                + ((double) loadTime)
                / 1000d
                + " secs, rate= "
                + triplesPerSecond()
                + ", commitLatency="
                + commitTime
                + "ms"
                + (closureStats != null ? "closure:: "
                        + closureStats.toString() : "");

    }
    
}
