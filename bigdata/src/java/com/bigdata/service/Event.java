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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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

    protected transient IBigdataFederation fed;

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
     * <p>
     * Note: This is a {@link ConcurrentHashMap} since otherwise concurrent
     * modification exceptions can occur if the {@link #start()} event is being
     * serialized concurrent with the application signaling the {@link #end()}
     * event.
     */
    protected ConcurrentHashMap<String,Object> details;
    
    /**
     * Event details.
     */
    public Map<String,Object> getDetails() {
        
        return details;
        
    }

    /**
     * Add details.
     * @param details
     * @return This {@link Event}.
     */
    public Event addDetails(final Map<String, Object> details) {

        if(details == null)
            throw new IllegalArgumentException();
        
        synchronized (this) {

            if (this.details == null) {

                this.details = new ConcurrentHashMap<String, Object>(details);

            } else {

                this.details.putAll(details);

            }

        }

        return this;

    }
    
    /**
     * Add a detail.
     * @param name
     * @param value
     * @return This {@link Event}.
     */
    public Event addDetail(final String name, final Object value) {

        if(name == null)
            throw new IllegalArgumentException();

        synchronized (this) {

            if (details == null) {

                details = new ConcurrentHashMap<String, Object>();

            }

            details.put(name, value);

        }
        
        return this;
        
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

        return endTime;

    }

    public Event(final IBigdataFederation fed, final EventResource resource,
            final Object majorEventType) {
        
        this(fed, resource, majorEventType, (Map<String,Object>) null/* details */);
        
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
     *            Optional details for the event.
     */
    public Event(final IBigdataFederation fed, final EventResource resource,
            final Object majorEventType, final Map<String,Object> details) {

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
     *            Optional details for the event.
     * 
     * @todo consider passing along the {@link UUID} of the parent event but
     *       then must correlate that {@link UUID} when the event is recieved.
     */
    protected Event(final IBigdataFederation fed, final EventResource resource,
            final Object majorEventType, final Object minorEventType,
            final Map<String,Object> details) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (resource == null)
            throw new IllegalArgumentException();

        if (majorEventType == null)
            throw new IllegalArgumentException();

        if (minorEventType == null)
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

        // MAY be null.
        if (details != null) {

            addDetails(details);

        }
        
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
     * 
     * @return The sub-event.
     */
    public Event newSubEvent(final Object minorEventType) {

        return new Event(fed, this.resource, this.majorEventType,
                minorEventType, this.details);
        
    }

    /**
     * A child event (major type is the type of the parent).
     * 
     * @param minorEventType
     * 
     * @param details
     *            Optional ordered (name,value) array.
     * 
     * @return The sub-event.
     */
    public Event newSubEvent(final Object minorEventType,
            final Map<String, Object> details) {

        final Event e = newSubEvent(minorEventType);
        
        if( details != null ) {
            
            e.addDetails(details);
            
        }
        
//        final Map<String, Object> m;
//        if (this.details == null && details == null) {
//            m = null;
//        } else {
//            m = new HashMap<String, Object>();
//            if (this.details != null) {
//                // details from the parent event.
//                m.putAll(this.details);
//            }
//            if (details != null) {
//                // given details.
//                m.putAll(details);
//            }
//        }

        return e;
        
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

        try {
            sendEvent();
        } catch (Throwable t) {
            log.warn(t);
        }

        return this;

    }

    /**
     * Sends the end event.
     * <p>
     * Note: You can use this method for "instantenous" events.
     * 
     * @return The event.
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

        try {
            sendEvent();
        } catch (Throwable t) {
            log.warn(t);
        }
        
        return this;

    }

    /**
     * Dispatche the {@link Event} via
     * {@link AbstractFederation#sendEvent(Event)}.
     */
    protected void sendEvent() throws IOException {

        if(fed instanceof AbstractFederation) {

                ((AbstractFederation) fed).sendEvent(this);
            
        } else {
            
            if (INFO)
                log.info(this);
            
        }

    }
    
    /**
     * A header that can be used to interpret the output of {@link #toString()}
     * (with newline).
     */
    static public String getHeader() {

        return  "eventUUID"
            + "\tindexName"
            + "\tpartitionId"
            + "\tfile"
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
     * Tab-delimited format (with newline). The details columns are written in a
     * dense format where each (name,value) pair is expressed as
     * <code>name=value</code>, so only the non-null columns are written out.
     */
    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
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
        sb.append(serviceIface.getName()); sb.append('\t'); 
        sb.append(serviceName); sb.append('\t'); 
        sb.append(serviceUUID);
        if (details != null) {
            final String[] keys = details.keySet().toArray(new String[]{});
            Arrays.sort(keys);
            for (String s : keys) {
                sb.append('\t');
                sb.append(s);
                sb.append("=");
                sb.append("" + details.get(s));
            }
//            for (Map.Entry<String, Object> entry : details.entrySet()) {
//                sb.append('\t');
//                sb.append(entry.getKey());
//                sb.append("=");
//                sb.append("" + entry.getValue());
//            }
        }
        sb.append('\n');
        
        return sb.toString();
        
    }

//    /**
//     * A class that knows how to render events in a stable tab-delimited format.
//     * Known detail columns are predeclared and will be written out in the order
//     * in which they were declared. Additional detail columns are recognized the
//     * first time they are used and will always be written out at the column
//     * index that they were assigned. This makes it possible to reimport the
//     * events data into a worksheet for processing.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class EventRenderer {
//        
//        public void declareColumn(String) {
//            
//        }
//        public String render(Event e) {
//            
//        }
//        
//    }
//    
//    /**
//     * Class which parses events written by the {@link EventRenderer}.
//     * 
//     * @todo we can't really reverse the {@link EventRenderer} when there are
//     *       columns that are not predeclared since their values will never be
//     *       written out.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class EventParser {
//        
//        public Event parseEvent(String s) {
//            
//        }
//        
//    }
    
    /**
     * Construct an event from the tab-delimited serialization produced from
     * {@link #toString()}.
     * 
     * @param s
     *            the tab delimited serialization
     *            
     * @throws ClassNotFoundException
     *             if any fields specify an invalid classname
     */
    protected Event(final String s) throws ClassNotFoundException {
//        System.err.println(s);
        final MyStringTokenizer st = new MyStringTokenizer(s, "\t");
        try {
        this.eventUUID = UUID.fromString(st.nextToken());    
//        System.err.println("eventUUID: <"+eventUUID+">");
        String resourceIndexName = st.nextToken();
//        System.err.println("resource.indexName: <"+resourceIndexName+">");
        String resourcePartitionId = st.nextToken();
//        System.err.println("resource.partitionId: <"+resourcePartitionId+">");
        String resourceFile = st.nextToken();
//        System.err.println("resource.file: <"+resourceFile+">");
        this.resource = new EventResource(resourceIndexName, resourcePartitionId, resourceFile);
        String majorEventClass = st.nextToken();
//        System.err.println("majorEventClass: <"+majorEventClass+">");
        if (majorEventClass != null) {
            Class majorEventType = Class.forName(majorEventClass);  
            String majorEventValue = st.nextToken();
//            System.err.println("majorEventValue: <"+majorEventValue+">");
            if (majorEventValue == null) {
                majorEventValue = "";
            }
            if (majorEventType.equals(String.class)) {
                this.majorEventType = majorEventValue;
            } else if (majorEventType.isEnum()) {
                this.majorEventType = Enum.valueOf(majorEventType, majorEventValue);
            } else {
                throw new UnsupportedOperationException();
            }
        } else {
            this.majorEventType = "";
        }
        String minorEventClass = st.nextToken();
//        System.err.println("minorEventClass: <"+minorEventClass+">");
        if (minorEventClass != null) {
            Class minorEventType = Class.forName(minorEventClass);  
            String minorEventValue = st.nextToken();
//            System.err.println("minorEventValue: <"+minorEventValue+">");
            if (minorEventValue == null) {
                minorEventValue = "";
            }
            if (minorEventType.equals(String.class)) {
                this.minorEventType = minorEventValue;
            } else if (minorEventType.isEnum()) {
                this.minorEventType = Enum.valueOf(minorEventType, minorEventValue);
            } else {
                throw new UnsupportedOperationException();
            }
        } else {
            this.minorEventType = "";
        }
        String startTime = st.nextToken();
//        System.err.println("startTime: <"+startTime+">");
        this.startTime = Long.valueOf(startTime);
        String endTime = st.nextToken();
//        System.err.println("endTime: <"+endTime+">");
        this.endTime = Long.valueOf(endTime);
        // do nothing with this one
        String elapsed = st.nextToken();
        this.complete = Boolean.valueOf(st.nextToken());    
        this.hostname = st.nextToken();    
        String serviceIfaceName = st.nextToken();
        this.serviceIface = Class.forName(serviceIfaceName);    
        this.serviceName = st.nextToken(); 
        this.serviceUUID = UUID.fromString(st.nextToken());
        this.details = new ConcurrentHashMap<String,Object>();
        while(st.hasMoreTokens()) {
            final String t = st.nextToken();
            final int pos = t.indexOf('=');
            final String name = t.substring(0,pos);
            final String value = t.substring(pos+1,t.length());
            this.details.put(name,value);
        }
        } catch(Throwable t) {
            throw new RuntimeException("At field: " + st.getCurrentFieldIndex()
                    + " of " + st.getFieldCount() + " : " + t, t);
        }
    }
    
    /**
     * Reconstruct an event object from a string.  Useful for post-mortem
     * analysis.
     * 
     * @param s the tab-delimited format create by {@link #toString()}.
     * @return a new event object
     * @throws ClassNotFoundException 
     *          if any CSV fields specify an invalid classname
     */
    public static Event fromString(String s) throws ClassNotFoundException {
        return new Event(s);
    }
    
    /**
     * Dissimilar from  the regular StringTokenizer, which is cannot handle 
     * empty fields.
     * 
     * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
     */
    private static class MyStringTokenizer {
        
        private final List<String> tokens;
        
        private final String delim;
        
        private int i;
        
        public MyStringTokenizer(final String s, final String delim) {
            this.tokens = new ArrayList<String>(20);
            final StringTokenizer st = new StringTokenizer(s, delim, true);
            while (st.hasMoreTokens()) {
                tokens.add(st.nextToken());
            }
            this.delim = delim;
            this.i = 0;
        }

        /**
         * The #of fields that were extracted.
         */
        public int getFieldCount() {
            
            return tokens.size();
            
        }
        
        /**
         * The index of the last field returned in [0:nfields-1]
         */
        public int getCurrentFieldIndex() {
            
            return i;
            
        }
        
        public boolean hasMoreTokens() {
            return i < tokens.size() &&
                   !(i == tokens.size()-1 && delim.contains(tokens.get(tokens.size()-1)));
        }
        
        public String nextToken() {
            String token = tokens.get(i++);
            if (delim.contains(token)) {
                // handles a blank column
                token = tokens.get(i);
                if (delim.contains(token)) {
                    token = BLANK;
                } else {
                    i++;
                }
            }
            if (token != null) {
                token = token.trim();
                if (token.length() == 0) {
                    // handles a column of whitespace.
                    token = BLANK;
                }
            }
            return token;
        }
        
    }
    
    static final String BLANK = "";
    
}