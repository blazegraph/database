package com.bigdata.counters.query;

/**
 * The different kinds of reports that we can generate.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum ReportEnum {

    /**
     * This is the navigational view of the performance counter
     * hierarchy.
     */
    hierarchy,
    
    /**
     * A correlated view will be generated for the spanned counters
     * showing all samples for each counter in a column where the rows
     * are all for the same sample period. Counters without history are
     * elided in this view. This is useful for plotting timeseries.
     */
    correlated,
    
    /**
     * This is a pivot table ready view, which is useful for aggregating
     * the performance counter data in a variety of ways.
     */
    pivot,
    
    /**
     * Plot a timeline of events.
     */
    events;
    
}