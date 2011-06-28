package com.bigdata.counters.httpd;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.service.Event;
import com.bigdata.service.EventReceiver;
import com.bigdata.service.IEventReportingService;
import com.bigdata.service.IService;

/**
 * A dummy implementation of the {@link IEventReportingService} interface, used
 * for testing the HTTP telemtry service.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class DummyEventReportingService extends EventReceiver implements
        IService {

    /**
     * Construct with an empty set of events
     */
    public DummyEventReportingService() {
    
        super(Long.MAX_VALUE/* eventHistoryMillis */, EventBTree
                .createTransient());

    }
    
    /**
     * Parse a CSV file into a LinkedHashMap of events.  The CSV file is assumed
     * to be tab-delimited, have one event per row, and follow the schema
     * implied by {@link Event#getHeader()}.
     * 
     * @param reader a reader for the CSV file
     */
    public void readCSV(final BufferedReader reader) throws IOException {
        String header = reader.readLine();
        if (header == null) {
            // no data
            return;
        }
//        System.err.println(header);
//        System.err.println(Event.getHeader());
        
//        if (!(header+"\n").equals(Event.getHeader())) {
        if (!header.startsWith(Event.getHeader())) {
            throw new IOException("Invalid schema");
        }
        // create a temporary list in case the CSV rows are not ordered
        // correctly
        final List<Event> events = new LinkedList<Event>();
        String row;
        int lineno = 0;
        while ((row = reader.readLine()) != null) {
            lineno++;
            try {
                Event e = Event.fromString(row);
//                System.err.println(e.toString());
                events.add(e);
            } catch (Exception ex) {
                ex.printStackTrace();
                System.err.println("skipping bad row: " + row);
                throw new IOException("bad row("+lineno+") : " + row);
            }
        }
        // sort into time order
        Collections.sort(events, new Comparator<Event>() {
            public int compare(Event e1, Event e2) {
                if (e1.getStartTime() < e2.getStartTime()) {
                    return -1;
                }
                if (e1.getStartTime() > e2.getStartTime()) {
                    return 1;
                }
                return 0;
            }
        });

        for (Event e : events) {
        
            notifyEvent(e);
            
        }

    }
    
    public String getHostname() throws IOException {
        
        return AbstractStatisticsCollector.fullyQualifiedHostName;
        
    }

    public Class getServiceIface() throws IOException {
        
        return getClass();
        
    }

    public String getServiceName() throws IOException {
        
        return getClass().getName()+"#"+hashCode();
        
    }

    public UUID getServiceUUID() throws IOException {
    
        return serviceUUID;
        
    }

    final UUID serviceUUID = UUID.randomUUID();

    public void destroy() {
        
        // NOP (eventBTree is transient for this mock object).
        
    }
    
}
