/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.ganglia;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import junit.framework.TestCase;

/**
 * Unit test for shutdown of the {@link GangliaListener}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestGangliaServiceShutdown extends TestCase {

    /**
     * 
     */
    public TestGangliaServiceShutdown() {
    }

    /**
     * @param name
     */
    public TestGangliaServiceShutdown(String name) {
        super(name);
    }

    /**
     * Test verifies correct shutdown of the {@link GangliaService} ASSUMING
     * that the {@link GangliaListener} shuts down correctly.
     * 
     * @see TestGangliaListenerShutdown
     */
    public void test_gangliaService_shutdown() throws UnknownHostException,
            InterruptedException {

        /*
         * The host name for this host.
         */
        final String hostName = GangliaService.getCanonicalHostName();

        final String serviceName = GangliaService.class.getSimpleName();

        final int quietPeriod = IGangliaDefaults.QUIET_PERIOD;

        final int initialDelay = IGangliaDefaults.INITIAL_DELAY;

        /*
         * Note: Use ZERO (0) if you are running gmond on the same host. That
         * will prevent the GangliaService from transmitting a different
         * heartbeat, which would confuse gmond and gmetad.
         */
        final int heartbeatInterval = 0; // IFF using gmond.
//      final int heartbeatInterval = IGangliaDefaults.HEARTBEAT_INTERVAL;
        
        final int monitoringInterval = IGangliaDefaults.MONITORING_INTERVAL;
        
        final InetAddress listenGroup = InetAddress
                .getByName(IGangliaDefaults.DEFAULT_GROUP);
        
        final int listenPort = IGangliaDefaults.DEFAULT_PORT;

        final String defaultUnits = IGangliaDefaults.DEFAULT_UNITS;
        
        final GangliaSlopeEnum defaultSlope = IGangliaDefaults.DEFAULT_SLOPE;

        final int defaultTMax = IGangliaDefaults.DEFAULT_TMAX;

        final int defaultDMax = IGangliaDefaults.DEFAULT_DMAX;
        
        final InetSocketAddress[] metricsServers = new InetSocketAddress[] { new InetSocketAddress(//
                IGangliaDefaults.DEFAULT_GROUP,//
                IGangliaDefaults.DEFAULT_PORT//
        ) };

        /*
         * Extensible factory for declaring and resolving metrics.
         * 
         * Note: you can layer on the ability to (a) recognize and align your
         * own host performance counters hierarchy with those declared by
         * ganglia and; (b) provide nice declarations for various application
         * counters of interest.
         */
        final GangliaMetadataFactory metadataFactory = new GangliaMetadataFactory(
                new DefaultMetadataFactory(//
                        defaultUnits,//
                        defaultSlope,//
                        defaultTMax,//
                        defaultDMax//
                        ));
        
        ExecutorService executorService = null;

        // The embedded ganglia service.
        GangliaService service = null;

        FutureTask<Void> ft = null;

        try {

            executorService = Executors.newSingleThreadExecutor();

            service = new GangliaService(//
                    hostName,//
                    serviceName, //
                    metricsServers, //
                    listenGroup, listenPort,//
                    true,// listen
                    true,// report
                    true,// mock (does not transmit when true).
                    quietPeriod,//
                    initialDelay,//
                    heartbeatInterval,//
                    monitoringInterval, //
                    defaultDMax,//
                    metadataFactory//
            );

            ft = new FutureTask<Void>(service, (Void) null);

            /*
             * Run the ganglia service.
             */
            executorService.submit(ft);

            Thread.sleep(2000/* ms */);

            assertTrue(service.isListening());

            ft.cancel(true/* mayInterruptIfRunning */);

            Thread.sleep(1000/* ms */);

            assertFalse(service.isListening());

            /*
             * May be uncommented if you want to look at what is happening in
             * a debugger.
             */
//            Thread.sleep(Long.MAX_VALUE);
            
        } finally {

            /*
             * Stop host/application metric collection here.
             */
            if (executorService != null)
                executorService.shutdownNow();

        }

    }
    
}
