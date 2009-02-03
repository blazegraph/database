/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Feb 3, 2009
 */

package com.bigdata.service;

import java.io.Serializable;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITransactionService;

/**
 * An event. Events are queued by the {@link IBigdataClient} and self-reported
 * periodically to the {@link ILoadBalancerService}. The event is assigned a
 * {@link UUID} when it is created and the {@link ILoadBalancerService }
 * assigned start and end event times based on its local clock as the events are
 * received (this helps to reduce the demand on the {@link ITransactionService}
 * for global timestamp).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Event implements Serializable {

    protected static transient final Logger log = Logger.getLogger(Event.class);

    protected static transient final boolean INFO = log.isInfoEnabled();

    protected static transient final boolean DEBUG = log.isDebugEnabled();

    /**
     * 
     */
    private static final long serialVersionUID = 2651293369056916231L;

    private transient AbstractFederation fed;

    /**
     * Unique event identifier.
     */
    public final UUID uuid;

    /**
     * Event type (classification or category).
     */
    public final String type;

    /**
     * Event details.
     */
    public final String details;

    /**
     * The event start time (auto-assigned by the recipient if 0L).
     */
    private long startTime;

    /**
     * The event end time (auto-assigned by the recipient if 0L).
     */
    private long endTime;

    /**
     * <code>true</code> iff the event event has been generated.
     */
    private boolean complete = false;

    public long getStartTime() {

        return startTime;

    }

    public long getEndTime() {

        return startTime;

    }

    public Event(final AbstractFederation fed, final String type,
            final String details) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (type == null)
            throw new IllegalArgumentException();

        if (details == null)
            throw new IllegalArgumentException();

        this.fed = fed;
        this.uuid = UUID.randomUUID();
        this.type = type;
        this.details = details;

        /*
         * @todo Assignment should be based on a federation configuration
         * option.
         * 
         * if global timestamps, then assign startTime.
         * 
         * if auto-assign, then assigned by the recipient.
         * 
         * if local-assign, then use System.currentTimeMillis().
         */

        start();

    }

    /**
     * Send the start event.
     */
    protected void start() {

        fed.sendEvent(this);

    }

    /**
     * Sends the end event.
     */
    public void end() {

        if (complete) {

            log.error("Event already complete: " + this);

            return;

        }

        complete = true;

        fed.sendEvent(this);

    }

}
