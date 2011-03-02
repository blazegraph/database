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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.query.QueryUtil;
import com.bigdata.counters.render.XHTMLRenderer;
import com.bigdata.service.Event;
import com.bigdata.service.IService;
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
    
    final static protected transient Logger log = Logger.getLogger(NanoHTTPD.class);

    /**
     * Runs the httpd server. When the optional file(s) are given, they will be
     * read into a {@link CounterSet} on startup. This is useful for post-mortem
     * analysis. The usage is
     * 
     * <pre>
     * [-p port] [-d {debug,info,warn,error,fatal}] [-filter filter]* [-regex regex]* [-events file] file(s)
     * </pre>
     * 
     * where
     * <dl>
     * <dt>-p</dt>
     * <dd>The port at which httpd server will answer queries</dt>
     * <dt>-filter</dt>
     * <dd>Each instance of this argument specifies a string to be matched and
     * is used to generate a {@link Pattern} which will filter the counters read
     * from the file(s). If there are multiple instances they are combined
     * together by an OR.</dd>
     * <dt>-regex</dt>
     * <dd>Each instance of this argument specifies a regular expression to be
     * matched and is used to generate a {@link Pattern} which will filter the
     * counters read from the file(s). If there are multiple instances they are
     * combined together by an OR.</dd>
     * <dt>-events</dt>
     * <dd>An optional file of {@link Event} s logged in a tab-delimited format</dd>
     * <dt>-events</dt>
     * <dd>An optional file of {@link Event} s logged in a tab-delimited format</dd>
     * <dt>file(s)</dt>
     * <dd>XML representations of logged {@link CounterSet}s</dd>
     * </dl>
     * 
     * @param args
     *            The command line arguments.
     * 
     * @throws IOException
     */
    public static void main(final String[] args) throws Exception {

        // default port.
        int port = 8080;

        // @todo args
        final int unitsToRetain = 1000;
        final PeriodEnum unit = PeriodEnum.Minutes;
        
        final CounterSet counterSet = new CounterSet();
        
        final DummyEventReportingService service = new DummyEventReportingService();

        // any -filter arguments.
        final Collection<String> filter = new LinkedList<String>();

        // any -regex arguments.
        final Collection<String> regex = new LinkedList<String>();

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
                    Logger.getLogger(XHTMLRenderer.class).setLevel(level);

                    // set logging level on the service.
                    Logger.getLogger(NanoHTTPD.class).setLevel(level);
                    
                } else if (arg.equals("-events")) {

                    QueryUtil.readEvents(service, new File(args[++i]));
                    
                } else if( arg.equals("-filter")) {
                    
                    filter.add(args[++i]);

                } else if( arg.equals("-regex")) {
                    
                    regex.add(args[++i]);

                } else {
                    
                    System.err.println("Unknown option: "+arg);
                    
                    System.exit( 1 );
                    
                }
                
            } else {

                /*
                 * Compute the optional filter to be applied when reading this
                 * file.
                 */
                final Pattern pattern = QueryUtil.getPattern(filter, regex);

                /*
                 * Read counters accepted by the optional filter into the
                 * counter set to be served.
                 */
                QueryUtil.readCountersFromFile(new File(arg), counterSet,
                        pattern, unitsToRetain, unit);

            }

        }

        System.out.println("Starting httpd server on port=" + port);

        // new server.
        final CounterSetHTTPDServer server = new CounterSetHTTPDServer(port,
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
            final IService service) throws Exception {

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
                
                if(log.isInfoEnabled())
                    log.info(ex);
                
            } finally {

                // terminate.

                shutdownNow();

            }

        }

    }

    synchronized public void shutdownNow() {

        if(log.isInfoEnabled())
            log.info("begin");

        if (httpd != null) {

            httpd.shutdownNow();

            httpd = null;
        
        }

        if(log.isInfoEnabled())
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

                if(log.isInfoEnabled())
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
