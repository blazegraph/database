package com.bigdata.journal;

/**
 * Enum of transaction run states.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum RunState {
    
    ACTIVE("active"),
    PREPARED("prepared"),
    COMMITTED("committed"),
    ABORTED("aborted");
    
    private final String name;
    
    RunState(String name) {
    
        this.name = name;
        
    }
    
    public String toString() {
    
        return name;
        
    }
    
}