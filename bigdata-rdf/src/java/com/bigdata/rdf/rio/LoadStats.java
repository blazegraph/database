package com.bigdata.rdf.rio;

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
    
    public long triplesPerSecond() {
        
        return ((long)( ((double)toldTriples) / ((double)totalTime) * 1000d ));
        
    }
    
    /**
     * Human readable representation.
     */
    public String toString() {

        return toldTriples+" stmts added in " + 
                ((double)loadTime) / 1000d +
                " secs, rate= " + 
                triplesPerSecond()+
                ", commitLatency="+
                commitTime+"ms" 
                ;

    }
    
}