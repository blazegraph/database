package com.bigdata.service;

import java.util.LinkedHashMap;
import java.util.UUID;

/**
 * Extension of the common service interface to support event reporting.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public interface IEventReportingService extends IService {
    /**
     * Return the events from this event reporting service, such as the load
     * balancer.
     * <p>
     * Basically a ring buffer of events without a capacity limit and with
     * random access by the event {@link UUID}.
     * <p>
     * Since this is a "linked" collection, the order of the events in the
     * collection will always reflect their arrival time. This lets us scan the
     * events in temporal order, filtering for desired criteria and makes it
     * possible to prune the events in the buffer as new events arrive.
     * 
     * @return the reported events
     */
    LinkedHashMap<UUID,Event> getEvents();
}
