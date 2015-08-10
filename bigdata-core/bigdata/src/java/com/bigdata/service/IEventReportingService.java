package com.bigdata.service;

import java.util.Iterator;

/**
 * Extension of the common service interface to support event reporting.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public interface IEventReportingService {
    
    /**
     * Visits completed events that <i>start</i> in the given interval in order
     * by their start time.
     */
    public Iterator<Event> rangeIterator(long fromTime, long toTime);

    /**
     * Reports the #of completed events that <i>start</i> in the given
     * interval.
     * 
     * @param fromTime
     *            The first start time to be included.
     * @param toTime
     *            The first start time to be excluded.
     * 
     * @return The #of events whose start time is in that interval.
     */
    public long rangeCount(long fromTime, long toTime);

}
