package com.bigdata.counters;

/**
 * A type-safe enumeration of the different periods at which samples may be
 * combined within a {@link History}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum PeriodEnum {
    
    Minutes(60*1000L),
    Hours(60*60*1000L),
    Days(24*60*60*1000L);
    
    private PeriodEnum(long basePeriodMillis) {
        
        this.basePeriodMillis = basePeriodMillis;
        
    }
    
    private long basePeriodMillis;

    /**
     * The #of milliseconds within a reporting period. E.g., a
     * {@link #Minute} has a base reporting period of <code>60,000</code>
     * milliseconds. All samples arriving within the same base reporting
     * period are combined in the same "slot" of the {@link History}.
     * Samples arriving in the next period are combined within the next
     * "slot".
     * 
     * @return The base reporting period in milliseconds.
     */
    public long getBasePeriodMillis() {
        
        return basePeriodMillis;
        
    }
    
}