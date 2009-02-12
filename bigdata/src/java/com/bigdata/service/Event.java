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

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.journal.ITransactionService;

/**
 * An event. Events are queued by the {@link IBigdataClient} and self-reported
 * periodically to the {@link ILoadBalancerService}. The event is assigned a
 * {@link UUID} when it is created and the {@link ILoadBalancerService} assigned
 * start and end event times based on its local clock as the events are received
 * (this helps to reduce the demand on the {@link ITransactionService} for
 * global timestamp).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo compact event serialization when reporting to the
 *       {@link ILoadBalancerService}, including factoring out of the common
 *       metadata (some stuff will always be the same for a given reported and
 *       does not need to be reported with each event).
 * 
 * @todo Add a level property to events? (but events are not meant to replace
 *       logging)
 */
public class Event implements Serializable {

    protected static transient final Logger log = Logger.getLogger(Event.class);

    protected static transient final boolean INFO = log.isInfoEnabled();

    protected static transient final boolean DEBUG = log.isDebugEnabled();

    /**
     * 
     */
    private static final long serialVersionUID = 2651293369056916231L;

    private transient IBigdataFederation fed;

    /**
     * Unique event identifier.
     */
    public final UUID eventUUID;

    /**
     * The host on which the event was generated.
     */
    public final String hostname;
    
    /**
     * The most interesting class or interface for the service which generated
     * that event.
     */
    public final Class serviceIface;
    
    /**
     * The name of the service which generated the event.
     */
    public final String serviceName;
    
    /**
     * The {@link UUID} for the service which generated that event.
     */
    public final UUID serviceUUID;
    
    /**
     * The resource for which the event is reported (store file, index name,
     * etc). Note that the {@link #serviceUUID} will also be reported.
     */
    public final EventResource resource;
    
    /**
     * Major event type (classification or category).
     */
    public final Object majorEventType;

    /**
     * Minor event type (classification or category).
     */
    public final Object minorEventType;

    /**
     * Event details.
     */
    protected String details;
    
    /**
     * Event details.
     */
    public String getDetails() {
        
        return details;
        
    }

    /**
     * The event start time. Assigned locally. The ecipient may use [endTime -
     * startTime] to adjust the event to its local clock.
     */
    protected long startTime;

    /**
     * The event end time. Assigned locally. The recipient may use [endTime -
     * startTime] to adjust the event to its local clock.
     */
    protected long endTime;

    /**
     * The time when the event was received. The recipient may use [endTime -
     * startTime] to adjust the event to its local clock.
     */
    transient protected long receiptTime;
    
    /**
     * <code>true</code> iff the event event has been generated.
     */
    protected boolean complete = false;

    /**
     * <code>true</code> iff the event event has been generated.
     */
    public boolean isComplete() {
        
        return complete;
        
    }
    
    public long getStartTime() {

        return startTime;

    }

    public long getEndTime() {

        return startTime;

    }

    /**
     * Event ctor.
     * 
     * @param fed
     *            The federation object (used to send the event to an
     *            aggregator).
     * @param resource
     *            The resource for which the event was generated (store, index
     *            partition, etc).
     * @param majorEventType
     *            The major type of the event (use of enums is encouraged).
     * @param details
     *            Some details for the event (unstructured).
     */
    public Event(final IBigdataFederation fed, final EventResource resource,
            final Object majorEventType, final String details) {

        this(fed, resource, majorEventType, ""/* minorEventType */, details);
        
    }

    /**
     * Sub-event ctor.
     * 
     * @param fed
     *            The federation object (used to send the event to an
     *            aggregator).
     * @param resource
     *            The resource for which the event was generated (store, index
     *            partition, etc).
     * @param majorEventType
     *            The major type of the event (use of enums is encouraged).
     * @param minorEventType
     *            The minor type of the event (use of enums is encouraged).
     * @param details
     *            Some details for the event (unstructured).
     * 
     * @todo consider passing along the {@link UUID} of the parent event but
     *       then must correlate that {@link UUID} when the event is recieved.
     */
    protected Event(final IBigdataFederation fed, final EventResource resource,
            final Object majorEventType, final Object minorEventType,
            final String details) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (resource == null)
            throw new IllegalArgumentException();

        if (majorEventType == null)
            throw new IllegalArgumentException();

        if (minorEventType == null)
            throw new IllegalArgumentException();

        if (details == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.eventUUID = UUID.randomUUID();

        this.hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

        this.serviceIface = this.fed.getServiceIface();

        this.serviceName = this.fed.getServiceName();

        this.serviceUUID = this.fed.getServiceUUID();

        this.resource = resource;
        
        this.majorEventType = majorEventType;

        this.minorEventType = minorEventType;

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

    }

    /**
     * A child event (major type is the type of the parent).
     * 
     * @param minorEventType

     * @param details
     * 
     * @return
     */
    public Event newSubEvent(Object minorEventType, String details) {

        return new Event(fed, this.resource, this.majorEventType,
                minorEventType, details);
        
    }

    /**
     * Send the start event.
     * <p>
     * Note: If you want to report an event with a duration then you MUST use
     * {@link #start()} when the event starts and {@link #end()} when the event
     * ends. If you want to report an "instantenous" event then you can just use
     * {@link #end()}.
     */
    synchronized public Event start() {

        if (startTime != 0) {
            
            // start already reported.
            throw new IllegalStateException();
            
        }
        
        // assign start time.
        startTime = System.currentTimeMillis();
        
        if(fed instanceof AbstractFederation) {

            try {
                ((AbstractFederation) fed).sendEvent(this);
            } catch (Throwable t) {
                log.warn(t);
            }
            
        } else {
            
            if (INFO)
                log.info(this);
            
        }
        
        return this;

    }

    /**
     * Variant of {@link #end()} which may be used to append more details to the
     * event.
     * 
     * @param moreDetails
     *            Additional details (optional).
     * 
     * @return The event.
     */
    synchronized public Event end(Object moreDetails) {
        
        if (moreDetails != null) {

            details = details + "::" + moreDetails;
            
        }

        return end();
        
    }
    
    /**
     * Sends the end event.
     * <p>
     * Note: You can use this method for "instantenous" events.
     */
    synchronized public Event end() {
        
        if (complete) {

            throw new IllegalStateException();
            
        }

        complete = true;

        endTime = System.currentTimeMillis();

        if (startTime == 0L) {

            // an "instantenous" event.
            startTime = endTime;
            
        }
        
        if(fed instanceof AbstractFederation) {

            try {
                ((AbstractFederation) fed).sendEvent(this);
            } catch(Throwable t) {
                log.warn(t);
            }

        } else {

            if (INFO)
                log.info(this);

        }
        
        return this;

    }

    /**
     * A header that can be used to interpret the output of {@link #toString()}
     * (with newline).
     */
    static public String getHeader() {

        return  "eventUUID"
            + "\tresource"
            + "\tmajorEventType"
            + "\tmajorEventValue"
            + "\tminorEventType"
            + "\tminorEventValue"
            + "\tstartTime"
            + "\tendTime"
            + "\telapsed"
            + "\tcomplete"
            + "\thostname"
            + "\tserviceIface"
            + "\tserviceName"
            + "\tserviceUUID"
            + "\tdetails"
            + "\n"
            ;
        
    }
    
    /**
     * Tab-delimited format (with newline).
     */
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(eventUUID); sb.append('\t'); 
        sb.append(resource.indexName); sb.append('\t');
        sb.append(resource.partitionId); sb.append('\t');
        sb.append(resource.file); sb.append('\t');
        sb.append(majorEventType.getClass().getName()); sb.append('\t'); 
        sb.append(majorEventType); sb.append('\t'); 
        sb.append(minorEventType.getClass().getName()); sb.append('\t'); 
        sb.append(minorEventType); sb.append('\t'); 
        sb.append(startTime); sb.append('\t'); 
        sb.append(endTime); sb.append('\t');
        if (complete) {
            sb.append(endTime - startTime);
            sb.append('\t');
        } else {
            sb.append('\t');
        }
        sb.append(complete); sb.append('\t'); 
        sb.append(hostname); sb.append('\t'); 
        sb.append(serviceIface); sb.append('\t'); 
        sb.append(serviceName); sb.append('\t'); 
        sb.append(serviceUUID); sb.append('\t'); 
        sb.append(details); sb.append('\n');
        
        return sb.toString();
        
    }
    
}
