package com.bigdata.counters.query;

/**
 * Type-safe enum for the options used to render the timestamp of the
 * row in a history or correlated history.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public enum TimestampFormatEnum {

    /**
     * 
     */
    dateTime,
    
    /**
     * Report the timestamp of the counter value in milliseconds since
     * the epoch (localtime).
     */
    epoch;

}