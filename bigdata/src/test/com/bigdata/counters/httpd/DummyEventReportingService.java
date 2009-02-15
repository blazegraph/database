package com.bigdata.counters.httpd;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import com.bigdata.service.Event;
import com.bigdata.service.IEventReportingService;

/**
 * A dummy implementation of the {@link IEventReportingService} interface, used
 * for testing the HTTP telemtry service.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class DummyEventReportingService implements IEventReportingService {

    /**
     * The events for this simulated event reporting service.
     */
    private final LinkedHashMap<UUID, Event> events;
    
    /**
     * Construct with an empty set of events
     */
    public DummyEventReportingService() {
        this.events = new LinkedHashMap<UUID, Event>();
    }
    
    /**
     * Construct using supplied events.
     * 
     * @param events the events
     */
    public DummyEventReportingService(LinkedHashMap<UUID, Event> events) {
        this.events = events;
    }
    
    /**
     * Parse a CSV file into a LinkedHashMap of events.  The CSV file is assumed
     * to be tab-delimited, have one event per row, and follow the schema
     * implied by {@link Event#getHeader()}.
     * 
     * @param reader a reader for the CSV file
     */
    public void readCSV(BufferedReader reader) throws IOException {
        String header = reader.readLine();
        if (header == null) {
            // no data
            return;
        }
//        System.err.println(header);
//        System.err.println(Event.getHeader());
        
        if (!(header+"\n").equals(Event.getHeader())) {
            throw new IOException("Invalid schema");
        }
        // create a temporary list in case the CSV rows are not ordered
        // correctly
        List<Event> events = new LinkedList<Event>();
        String row;
        while ((row = reader.readLine()) != null) {
            try {
                Event e = Event.fromString(row);
//                System.err.println(e.toString());
                events.add(e);
            } catch (Exception ex) {
                ex.printStackTrace();
                System.err.println("skipping bad row: " + row);
                throw new IOException("bad row: " + row);
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
        // add into LinkedHashMap
        for (Event e : events) {
            this.events.put(e.eventUUID, e);
        }
    }
    
    /**
     * Return the events for this simulated event reporting service.
     * 
     * @return the events
     */
    public LinkedHashMap<UUID, Event> getEvents() {
        return events;
    }

    /**
     * Not implemented.
     * 
     * @throws NotImplementedException
     */
    public String getHostname() throws IOException {
        throw new NotImplementedException();
    }

    /**
     * Not implemented.
     * 
     * @throws NotImplementedException
     */
    public Class getServiceIface() throws IOException {
        throw new NotImplementedException();
    }

    /**
     * Not implemented.
     * 
     * @throws NotImplementedException
     */
    public String getServiceName() throws IOException {
        throw new NotImplementedException();
    }

    /**
     * Not implemented.
     * 
     * @throws NotImplementedException
     */
    public UUID getServiceUUID() throws IOException {
        throw new NotImplementedException();
    }
}
