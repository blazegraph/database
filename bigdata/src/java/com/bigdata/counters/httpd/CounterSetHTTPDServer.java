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
 * Created on Apr 18, 2008
 */

package com.bigdata.counters.httpd;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.DefaultInstrumentFactory;
import com.bigdata.service.IEventReportingService;
import com.bigdata.util.httpd.AbstractHTTPD;
import com.bigdata.util.httpd.NanoHTTPD;

/**
 * An httpd server exposing a {@link CounterSet}. This may be used either for
 * testing the {@link CounterSetHTTPD} class or for post-mortem analysis of a
 * saved {@link CounterSet}.
 * 
 * @see #main(String[])
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CounterSetHTTPDServer implements Runnable {
    
    final static protected Logger log = Logger.getLogger(NanoHTTPD.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * Runs the httpd server. When the optional file(s) are given, they will be
     * read into a {@link CounterSet} on startup. This is useful for post-mortem
     * analysis.
     * 
     * @param args
     *            [-p port] [-d {debug,info,warn,error,fatal}] <i>file(s)</i>
     * 
     * @throws IOException
     */
    public static void main(final String[] args) throws Exception {

        // default port.
        int port = 8080;

        final CounterSet counterSet = new CounterSet();
        
        final DummyEventReportingService service = new DummyEventReportingService();

        for (int i = 0; i < args.length; i++) {

            final String arg = args[i];

            if (arg.startsWith("-")) {

                if (arg.equals("-p")) {

                    port = Integer.parseInt(args[++i]);

                    System.out.println("port: "+port);
                    
                } else if( arg.equals("-d")) {
                    
                    final Level level = Level.toLevel(args[++i]);
                    
                    System.out.println("Setting server and service log levels: "+level);
                    
                    // set logging level on the server.
                    CounterSetHTTPDServer.log.setLevel(level);
                    
                    // set logging level for the view.
                    XHTMLRenderer.log.setLevel(level);

                    // set logging level on the service.
                    NanoHTTPD.log.setLevel(level);
                    
                } else if( arg.equals("-events")) {
                    
                    final File file = new File(args[++i]);
                    
                    System.out.println("reading events file: "+file);
                    
                    BufferedReader reader = null;

                    try {

                        reader = new BufferedReader(new FileReader(file));

                        service.readCSV(reader);

                    } finally {

                        if (reader != null) {

                            reader.close();

                        }
                        
                    }

                } else {
                    
                    System.err.println("Unknown option: "+arg);
                    
                    System.exit( 1 );
                    
                }
                
            } else {

                final File file = new File(arg);
                
                System.out.println("reading file: "+file);
                
                InputStream is = null;

                try {

                    is = new BufferedInputStream(new FileInputStream(file));

                    counterSet.readXML(is, DefaultInstrumentFactory.INSTANCE,
                            null/* filter */);

                } finally {

                    if (is != null) {

                        is.close();

                    }
                    
                }

            }

        }
        
        System.out.println("starting httpd server on port="+port);

        // new server.
        CounterSetHTTPDServer server = new CounterSetHTTPDServer(port,
                counterSet, service);

        // run server.
        server.run();

    }

    /** The server. */
    private AbstractHTTPD httpd;

    /**
     * 
     * @param port
     */
    public CounterSetHTTPDServer(final int port, final CounterSet counterSet,
            final IEventReportingService service) throws Exception {

        /*
         * The runtime shutdown hook appears to be a robust way to handle ^C by
         * providing a clean service termination.
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));

        httpd = new CounterSetHTTPD(port, counterSet, service);

    }
    
    public void run() {

        Object keepAlive = new Object();
        
        synchronized (keepAlive) {
            
            try {
                
                keepAlive.wait();
                
            } catch (InterruptedException ex) {
                
                if(INFO)
                    log.info(ex);
                
            } finally {

                // terminate.

                shutdownNow();

            }

        }

    }

    synchronized public void shutdownNow() {

        if(INFO)
            log.info("begin");

        if (httpd != null) {

            httpd.shutdownNow();

            httpd = null;
        
        }

        if(INFO)
            log.info("done");

    }

    /**
     * Runs {@link #shutdownNow()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ShutdownThread extends Thread {
        
        final CounterSetHTTPDServer server;
        
        public ShutdownThread(CounterSetHTTPDServer httpd) {
            
            if (httpd == null)
                throw new IllegalArgumentException();
            
            this.server = httpd;
            
        }
        
        public void run() {
            
            try {

                if(INFO)
                    log.info("Running shutdown.");

                /*
                 * Note: This is the "server" shutdown.
                 */
                
                server.shutdownNow();
                
            } catch (Exception ex) {

                log.error("While shutting down service: " + ex, ex);

            }

        }
        
    }
    
}
