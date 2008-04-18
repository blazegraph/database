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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.util.httpd.AbstractHTTPD;
import com.bigdata.util.httpd.NanoHTTPD;

/**
 * Utility class for testing {@link CounterSetHTTPD}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CounterSetHTTPDServer implements Runnable {
    
    final static protected Logger log = Logger.getLogger(NanoHTTPD.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Runs the httpd server.
     * 
     * @param args [port]
     * 
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {

        int port = 80;
        if(args.length>0) {
            port = Integer.parseInt(args[0]);
        }

        log.info("port: "+port);
        
        CounterSet counterSet = new CounterSet();

        new CounterSetHTTPDServer(port,counterSet).run();
        
    }

    private AbstractHTTPD httpd;

    /**
     * 
     * @param port
     * 
     * @throws IOException
     */
    public CounterSetHTTPDServer(int port,CounterSet counterSet) throws Exception {

        /*
         * The runtime shutdown hook appears to be a robust way to handle ^C by
         * providing a clean service termination.
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));

        setUp(counterSet);

        httpd = new CounterSetHTTPD(port, counterSet);

    }

    /**
     * Invoked during server startup to allow customization of the
     * {@link CounterSet} exposed by the httpd server.
     * 
     * @param root
     */
    protected void setUp(CounterSet root) throws Exception {

        final Random r = new Random();
        {

            CounterSet cset = root.makePath("localhost");

            cset.addCounter("hostname", new OneShotInstrument<String>(
                    InetAddress.getLocalHost().getHostName()));
            
            cset.addCounter("ipaddr", new OneShotInstrument<String>(
                    InetAddress.getLocalHost().getHostAddress()));

            final HistoryInstrument<Double> history1 = new HistoryInstrument<Double>(
                    new Double[] {});

            cset.addCounter("random", new Instrument<Integer>() {

                @Override
                protected void sample() {

                    final Integer val = r.nextInt(100);

                    setValue(val);

                    /*
                     * Note: A multiplier is used to have time, as reported to
                     * the history instrument, pass faster than the clock time.
                     * This lets you test the UI out time minutes, hours, and
                     * days in far less than the corresponding wall clock time.
                     */
                    final long timestamp = System.currentTimeMillis()*60*60;
                    
                    history1.setValue((double)value, timestamp);
                    
                }

            });

            cset.addCounter("history1", history1);
            
        }
        
        {
            
            CounterSet cset = root.makePath("www.bigdata.com");

            cset.addCounter("ipaddr", new OneShotInstrument<String>(
                    InetAddress.getByName("www.bigdata.com").getHostAddress()));

            cset.makePath("foo").addCounter("bar",
                    new OneShotInstrument<String>("baz"));
            
        }
        
    }
    
    public void run() {

        Object keepAlive = new Object();
        
        synchronized (keepAlive) {
            
            try {
                
                keepAlive.wait();
                
            } catch (InterruptedException ex) {
                
                log.info(""+ex);
                
            } finally {

                // terminate.

                shutdownNow();

            }

        }

    }

    synchronized public void shutdownNow() {

        log.info("begin");

        if (httpd != null) {

            httpd.shutdownNow();

            httpd = null;
        
        }

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
