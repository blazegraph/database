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
 * Created on May 1, 2008
 */

package com.bigdata.counters;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Iterator;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.bigdata.counters.History.SampleIterator;
import com.bigdata.counters.ICounterSet.IInstrumentFactory;
import com.bigdata.util.HTMLUtility;

/**
 * XML (de-)serialization of {@link CounterSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class XMLUtility {

    static protected final Logger log = Logger.getLogger(XMLUtility.class);

    public static final XMLUtility INSTANCE = new XMLUtility();
    
    private XMLUtility() {}
    
    /**
     * Serializes an {@link ICounterSet} as XML.
     * 
     * @param root
     *            The {@link ICounterSet}.
     * @param w
     *            Where to write the XML.
     * @param filter
     *            A filter to be applied to the counters (optional). Only the
     *            matched counters will be serialized.
     *            
     * @throws IOException
     */
    public void writeXML(CounterSet root, final Writer w, final Pattern filter) throws IOException {
        
        w.write("<counters");
        w.write(" xmlns:xs=\""+NAMESPACE_XSD+"\"");
        w.write("\n>");
        
        final Iterator itr = root.postOrderIterator();

        while(itr.hasNext()) {
            
            final CounterSet counterSet = (CounterSet)itr.next();
            
            final Iterator<ICounter> itr2 = counterSet.counterIterator(filter);

            if(!itr2.hasNext()) {
                
                /*
                 * do not emit counter sets that do not have directly attached
                 * counters.
                 */
                
                continue;
                
            }
            
            w.write("<cs");
            w.write(" path=\""+counterSet.getPath()+"\"");
            w.write("\n>");

            while(itr2.hasNext()) {
                
                final ICounter counter = itr2.next();
                
                final String name = counter.getName();
                
                final Object value = counter.getValue();
                
                final long time = counter.lastModified();

                if (time == 0L || value == null) {
                    
                    /*
                     * Zero timestamps and null values are generally an
                     * indicator that the counter value is not yet defined.
                     */
                    
                    if (log.isInfoEnabled())
                        log.info("Ignoring counter: name=" + name
                                + ", timestamp=" + time + ", value=" + value);

                    continue;
                    
                }
                
                final String type = getXSDType(value);
            
                if (time < 0L) {
                    
                    /*
                     * Negative timestamps are not expected.
                     */
                    
                    log.warn("Ignoring counter with invalid timestamp: name="
                                    + name
                                    + ", timestamp="
                                    + time
                                    + ", value="
                                    + value);

                    continue;
                    
                }
                
                w.write("<c");
                w.write(" name=\"" + name + "\"");
                w.write(" type=\"" + type + "\"");
                w.write(" time=\"" + time + "\"");
                w.write(" value=\""+HTMLUtility.escapeForXHTML(value==null?"":value.toString())+"\"");
                w.write(">");
                
                if(counter.getInstrument() instanceof HistoryInstrument) {

                    HistoryInstrument inst = (HistoryInstrument)counter.getInstrument();
                    
                    writeHistory(w, inst.minutes, "minutes");
                    
                    writeHistory(w, inst.hours, "hours");
                    
                    writeHistory(w, inst.days, "days");
                       
                }
                
                w.write("</c\n>");
                
            }

            w.write("</cs\n>");

        }
        
        w.write("</counters\n>");
        
        w.flush();
        
    }
    
    /**
     * Write the sample values for a {@link History} of some {@link ICounter}.
     * 
     * @param w
     * @param h
     * @param units
     * 
     * @throws IOException
     */
    protected void writeHistory(Writer w, History h, String units)
            throws IOException {

        /*
         * Note: synchronized on the history to prevent concurrent modification.
         */
        synchronized(h) {
        
            w.write("<h");
            w.write(" units=\"" + units + "\"");
//                w.write(" type=\"" + type + "\"");
            w.write("\n>");
            
            final SampleIterator itr = h.iterator();

            while (itr.hasNext()) {

                final IHistoryEntry entry = itr.next();
                
                w.write("<v");
                
                // last modified timestamp for the sample period.
                w.write(" time=\"" + entry.lastModified() + "\"");
                
                // average for the sample period.
                w.write(" value=\""
                        + HTMLUtility.escapeForXHTML(entry.getValue().toString()) + "\"");

                w.write("></v\n>");
                
            }
            
            w.write("</h\n>");
        
        }

    }
    
    public void readXML(CounterSet root, InputStream is, IInstrumentFactory instrumentFactory,
            Pattern filter) throws IOException, ParserConfigurationException, SAXException {

        if (is == null)
            throw new IllegalArgumentException();

        if (instrumentFactory == null)
            throw new IllegalArgumentException();
        
        final SAXParser p;
        {
            
            SAXParserFactory f = SAXParserFactory.newInstance();
        
            f.setNamespaceAware(true);
            
            p = f.newSAXParser();
            
        }
        
        MyHandler handler = new MyHandler(root, instrumentFactory, filter);
        
        p.parse(is, handler /*@todo set validating and pass in systemId*/);
        
    }
    
    /**
     * Helper class for SAX based parse of counter XML.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private class MyHandler extends DefaultHandler {
        
        /** Note: inner class so named with '$' vs '.' */
        protected static final Logger log = Logger.getLogger(MyHandler.class);
        
        private final AbstractCounterSet root;
        
        private final IInstrumentFactory instrumentFactory;

        private final Pattern filter;
        
        public MyHandler(AbstractCounterSet root, IInstrumentFactory instrumentFactory,
                Pattern filter) {

            if (root == null)
                throw new IllegalArgumentException();

            if (instrumentFactory == null)
                throw new IllegalArgumentException();

            this.root = root;
            
            this.instrumentFactory = instrumentFactory;
            
            this.filter = filter;
            
        }
        
        /**
         * Set each time we enter a <code>cs</code> element.
         */
        private String path;
        
//            /**
//             * Set each time we enter a <code>c</code> element. The value will be
//             * <code>null</code> if there is no node with the same path as the
//             * described counter (the {@link #path} plus the counter
//             * <code>name</code> attribute), a {@link CounterSet} if the path
//             * describes a {@link CounterSet} rather than a {@link Counter}, or a
//             * {@link Counter} if there is a pre-existing counter for that path.
//             */
//            private ICounterNode node;

//            /**
//             * The value of the <code>name</code> attribute from the last
//             * <code>c</code> element.
//             */
//            private String name;
//            
//            /**
//             * The value of the <code>time</code> attribute from the last
//             * <code>c</code> element.
//             */
//            private long time;
//            
//            /**
//             * The value of the <code>type</code> attribute from the last
//             * <code>c</code> element.
//             */
//            private String type;
        
        /** The current counter. */
        private ICounter counter;
        
        /**
         * The current history and <code>null</code> if we are not reading
         * some {@link History} for the current {@link #counter}.
         */
        private History history;
        
        /** qualified name for the <code>cs</code> element (counter set). */
        private final String cs = "cs"; 

        /** qualified name for the <code>c</code> element (counter). */
        private final String c = "c"; 
        
        /** qualified name for the <code>h</code> element (history). */
        private final String h = "h"; 

        /** qualified name for the <code>h</code> element (history value). */
        private final String v = "v"; 
        
        /** buffers the cdata content inside of each element. */
        private StringBuilder cdata = new StringBuilder();
        
        public void startElement(String uri, String localName, String qName,
                Attributes attributes) throws SAXException {

            if (log.isInfoEnabled())
            log.info("uri=" + uri + ",localName=" + localName + ", qName="
                    + qName);

            if(qName.equals("counters")) {
              
                // ignore.
                
            } else if(qName.equals(cs)) {
                
                path = attributes.getValue("path");
                
                if (log.isInfoEnabled())
                log.info("path="+path);
                
            } else if(qName.equals(c)) {
                
                final String name = attributes.getValue("name");

                final String type = attributes.getValue("type");
                
                final long time = Long.parseLong(attributes.getValue("time"));
                
                final String value = attributes.getValue("value");

                if (log.isInfoEnabled())
                log.info("path="+path+", name="+name+", type="+type+", value="+value+", time="+time);

                // determine value class from XSD attribute.
                final Class typ = getType(type);

                // find/create counter given its path, etc.
                final ICounter counter = getCounter(path, name, typ);

                if (counter == null) {
                
                    log.warn("Conflict: path="+path+", name="+name);
                    
                } else {

                    // set the value on the counter.
                    setValue(counter, typ, value, time);
                    
                }

                /*
                 * Set in case we need to read the counter's history also.
                 * 
                 * Note: this will be [null] if we could not find/create the
                 * counter above.
                 */
                
                this.counter = counter;

                /*
                 * clear history reference - set when we see the [h] element and
                 * know the units for the history to be read.
                 */
                
                this.history = null;

            } else if (qName.equals(h)) {

                // clear - will be set below based on units and otherwise not available.
                history = null;

                if (counter == null) {

                    // The counter could not be read so ignore its history.
                    return;

                }

                if(!(counter.getInstrument() instanceof HistoryInstrument)) {
                    
                    /*
                     * Counter does not support history (either the factory is
                     * wrong or the counter pre-existed but was created without
                     * history support).
                     */
                    
                    log.warn("Ignoring history: inst="
                            + counter.getInstrument().getClass().getName()
                            + ", path" + counter);
                    
                    return;
                    
                }
                
                final HistoryInstrument inst = (HistoryInstrument) counter
                        .getInstrument();
                
                final String units = attributes.getValue("units");

                if (units == null) {

                    throw new SAXException("No units");
                    
                } else if (units.equals("minutes")) {

                    history = inst.minutes;
                    
                } else if (units.equals("hours")) {

                    history = inst.hours;
                    
                } else if (units.equals("days")) {

                    history = inst.days;
                    
                } else {
                    
                    throw new SAXException("Bad units: " + units);
                    
                }
                
            } else if(qName.equals(v)) {
            
                if (counter == null || history == null) {

                    // Ignore history.
                    return;
                    
                }
                
                final long time = Long.parseLong(attributes.getValue("time"));

                final String value = attributes.getValue("value");

                if (log.isInfoEnabled())
                    log.info("counter=" + counter + ", time=" + time
                            + ", value=" + value);

                addValue(history, time, value);

            } else {
                
                throw new SAXException("Unknown start tag: "+qName);
                
            }
            
        }

        public void characters(char[] ch, int start, int length)
                throws SAXException {

            cdata.append(ch, start, length);

        }
        
        public void endElement(String uri, String localName, String qName)
                throws SAXException {

            try {

//                    if (!qName.equals(c))
//                        return;

            } finally {

                // clear any buffered data.
                cdata.setLength(0);
                
            }
            
        }
  
        /**
         * Find/create a counter given its path, name, and value class.
         * 
         * @param path
         * @param name
         * @param typ
         * 
         * @return The counter -or- <code>null</code> iff the path and name
         *         identify a pre-existing {@link CounterSet}, which conflicts
         *         with the described {@link ICounter}.
         */
        protected ICounter getCounter(final String path, final String name, Class typ) {
            
            final ICounter counter;

            // iff there is an existing node for that path.
            final ICounterNode node;
            
            // atomic makePath + counter create iff necessary.
            synchronized (root) {

                /*
                 * Note: use just the name when the path is '/' to avoid
                 * forming a path that begins '//'.
                 */
                node = root.getPath(path.equals(ICounterSet.pathSeparator) ? name : path
                        + ICounterSet.pathSeparator + name);

                if (node == null) {

                    final IInstrument inst = instrumentFactory
                            .newInstance(typ);

                    counter = ((CounterSet)root.makePath(path)).addCounter(name, inst);

                } else if (node.isCounter()) {

                    counter = (ICounter) node;

                } else {

                    return null;

                }
                
            }

            return counter;
            
        }
        
        /**
         * Interpret an XSD attribute value, returning the corresponding Java class.
         * 
         * @param type
         *            The XSD attribute value.
         * 
         * @return
         */
        static protected Class getType(String type) {
            
            final String localType = type.substring(type.lastIndexOf("#")+1);
            
            final Class typ;
            
            if(localType.equals(xsd_int)||localType.equals(xsd_long)) {
                
                typ = Long.class;
                
            } else if(localType.equals(xsd_float)||localType.equals(xsd_double)) {
                
                typ = Double.class;
                
            } else {
                
                typ = String.class;
                
            }

            return typ;

        }
        
        /**
         * Set the counter value given its value type and the text of its value.
         * 
         * @param counter
         *            The counter whose value will be set.
         * @param typ
         *            The value type of the counter.
         * @param text
         *            The text of the value to be interpreted.
         * @param time
         *            The timestamp for the value.
         */
        static protected void setValue(final ICounter counter, final Class typ,
                final String text, final long time) {
            
            final IInstrument inst = counter.getInstrument();
            
            if (inst instanceof OneShotInstrument) {

                /*
                 * This instrument can not be updated. However, new values for a
                 * variety of one-shot counters will be reported by each client
                 * that starts on the same host. E.g., the #of CPUs and that
                 * sort of thing. We just ignore the redundent updates.
                 */
                
                log.warn(OneShotInstrument.class.getName()
                        + " : ignoring update: path=" + counter.getPath()
                        + ", value=" + text);
                
                return;
                
            }
            
            try {

                if (typ == Long.class) {

                    counter.setValue(Long.parseLong(text), time);

                } else if (typ == Double.class) {

                    counter.setValue(Double.parseDouble(text), time);

                } else {

                    counter.setValue(text, time);

                }
                
            } catch (Exception ex) {
                
                log.warn("Could not set counter value: path=" + counter.getPath()
                        + " : " + ex, ex);
                
            }

        }
        
        static protected void addValue(final History history, final long time,
                final String text) {

            final Class typ = history.getValueType();

            if (typ == Long.class) {

                history.add(time, Long.parseLong(text));

            } else if (typ == Double.class) {

                history.add(time, Double.parseDouble(text));

            } else {

                history.add(time, text);

            }

        }
        
    }

    private static final transient String NAMESPACE_XSD = "http://www.w3.org/2001/XMLSchema";
    
    /** assuming xs == http://www.w3.org/2001/XMLSchema */
    private static final transient String xsd = "xs:";
    private static final transient String xsd_anyType = xsd+"anyType";
    private static final transient String xsd_long    = xsd+"long";
    private static final transient String xsd_int     = xsd+"int";
    private static final transient String xsd_double  = xsd+"double";
    private static final transient String xsd_float   = xsd+"float";
    private static final transient String xsd_string  = xsd+"string";
    private static final transient String xsd_boolean = xsd+"boolean";

    /**
     * Return the XML datatype for an {@link ICounter}'s value.
     * 
     * @param value
     *            The current counter value.
     * 
     * @return The corresponding XML datatype -or- "xsd:anyType" if no more
     *         specific datatype could be determined.
     */
    private String getXSDType(Object value) {
        
        if (value == null)
            return xsd_anyType;

        Class c = value.getClass();
        
        if (c.equals(Long.class)) 
            
            return xsd_long;

        else if (c.equals(Integer.class))
            
            return xsd_int;
        
        else if (c.equals(Double.class))
        
            return xsd_double;
        
        else if (c.equals(Float.class))
            
            return xsd_float;
        
        else if (c.equals(String.class))
            
            return xsd_string;
        
        else if (c.equals(Boolean.class))
            
            return xsd_boolean;
        
        else
            
            return xsd_anyType;

    }

}
