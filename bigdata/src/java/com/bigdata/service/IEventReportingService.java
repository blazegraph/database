package com.bigdata.service;

import java.util.Iterator;

/**
 * Extension of the common service interface to support event reporting.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public interface IEventReportingService {
    
//    /**
//     * Return the events from this event reporting service, such as the load
//     * balancer.
//     * <p>
//     * Basically a ring buffer of events without a capacity limit and with
//     * random access by the event {@link UUID}.
//     * <p>
//     * Since this is a "linked" collection, the order of the events in the
//     * collection will always reflect their arrival time. This lets us scan the
//     * events in temporal order, filtering for desired criteria and makes it
//     * possible to prune the events in the buffer as new events arrive.
//     * 
//     * @return the reported events
//     */
//    LinkedHashMap<UUID,Event> getEvents();

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
