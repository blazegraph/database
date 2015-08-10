package com.bigdata.counters;

import java.util.concurrent.TimeUnit;

/**
 * A type-safe enumeration of the different periods at which samples may be
 * combined within a {@link History}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum PeriodEnum {
    
    /***/
    Minutes(60 * 1000L),
    /***/
    Hours(60 * 60 * 1000L),
    /***/
    Days(24 * 60 * 60 * 1000L);

    private PeriodEnum(final long basePeriodMillis) {
        
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
    public long getPeriodMillis() {
        
        return basePeriodMillis;
        
    }

    public TimeUnit toTimeUnit() {

        switch (this) {
        case Minutes:
            return TimeUnit.MINUTES;
        case Hours:
            return TimeUnit.HOURS;
        case Days:
            return TimeUnit.DAYS;
        default:
            throw new AssertionError();
        }

    }

    public static PeriodEnum getValue(final TimeUnit unit) {
        if (unit == null)
            throw new IllegalArgumentException();
        switch (unit) {
        case MINUTES:
            return Minutes;
        case HOURS:
            return Hours;
        case DAYS:
            return Days;
        default:
            throw new UnsupportedOperationException("unit=" + unit);

        }
    }

}
