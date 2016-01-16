/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.journal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.ganglia.BigdataGangliaService;
import com.bigdata.counters.ganglia.BigdataMetadataFactory;
import com.bigdata.counters.ganglia.HostMetricsCollector;
import com.bigdata.counters.ganglia.QueryEngineMetricsCollector;
import com.bigdata.ganglia.DefaultMetadataFactory;
import com.bigdata.ganglia.GangliaMetadataFactory;
import com.bigdata.ganglia.GangliaService;
import com.bigdata.ganglia.GangliaSlopeEnum;
import com.bigdata.ganglia.IGangliaDefaults;
import com.bigdata.ganglia.util.GangliaUtil;

/**
 * A plugin for ganglia.
 * <p>
 * Note: This plugin will not start (and will not be loaded from the classpath)
 * unless {@link PlatformStatsPlugIn.Options#COLLECT_PLATFORM_STATISTICS} is set to
 * <code>true</code>.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/441"> Ganglia
 *      Integration</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/609">
 *      bigdata-ganglia is required dependency for Journal </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GangliaPlugIn implements IPlugIn<Journal, GangliaService> {

    private static final Logger log = Logger.getLogger(GangliaPlugIn.class);

    /**
     * Configuration options.
     */
    public interface Options {
        
        // Listen

        /**
         * The multicast group used to join the ganglia performance monitoring
         * network.
         */
        String GANGLIA_LISTEN_GROUP = Journal.class.getName()
                + ".ganglia.listenGroup";

        String DEFAULT_GANGLIA_LISTEN_GROUP = IGangliaDefaults.DEFAULT_GROUP;

        /**
         * The port for the multicast group used to join the ganglia performance
         * monitoring network.
         */
        String GANGLIA_LISTEN_PORT = Journal.class.getName()
                + ".ganglia.listenPort";

        String DEFAULT_GANGLIA_LISTEN_PORT = Integer
                .toString(IGangliaDefaults.DEFAULT_PORT);

        /**
         * When <code>true</code>, the embedded {@link GangliaService} will
         * listen on to the specified multicast group and build up an internal
         * model of the metrics in the ganglia network.
         * <p>
         * Note: If both {@link #GANGLIA_LISTEN} and {@link #GANGLIA_REPORT} are
         * <code>false</code> then the embedded {@link GangliaService} will not
         * be started.
         */
        String GANGLIA_LISTEN = Journal.class.getName()
                + ".ganglia.listen";

        String DEFAULT_GANGLIA_LISTEN = "false";

        // Report

        /**
         * When <code>true</code>, the embedded {@link GangliaService} will
         * report performance metrics to the specified gmetad server(s).
         * <p>
         * Note: If both {@link #GANGLIA_LISTEN} and {@link #GANGLIA_REPORT} are
         * <code>false</code> then the embedded {@link GangliaService} will not
         * be started.
         */
        String GANGLIA_REPORT = Journal.class.getName()
                + ".ganglia.report";

        String DEFAULT_GANGLIA_REPORT = "false";

        /**
         * An list of the metric servers (<code>gmetad</code> instances) to
         * which metrics will be sent. The default is to send metrics to the
         * well known multicast group for ganglia. Zero or more hosts may be
         * specified, separated by whitespace or commas. The port for each host
         * is optional and defaults to the well known port for ganglia. Each
         * host may be either a unicast address or a multicast group.
         */
        String GANGLIA_SERVERS = Journal.class.getName()
                + ".ganglia.servers";

        String DEFAULT_GANGLIA_SERVERS = IGangliaDefaults.DEFAULT_GROUP;

        /**
         * The delay between reports of performance counters in milliseconds (
         * {@value #DEFAULT_REPORT_DELAY}). When ZERO (0L), performance counter
         * reporting will be disabled.
         * 
         * @see #DEFAULT_REPORT_DELAY
         */
        String REPORT_DELAY = Journal.class.getName() + ".reportDelay";

        /**
         * The default {@link #REPORT_DELAY}.
         */
        String DEFAULT_REPORT_DELAY = "" + (60 * 1000);
        
    }
    
    /**
     * Future for an embedded {@link GangliaService} which listens to
     * <code>gmond</code> instances and other {@link GangliaService}s and
     * reports out metrics from {@link #getCounters()} to the ganglia network.
     */
    private final AtomicReference<FutureTask<Void>> gangliaFuture = new AtomicReference<FutureTask<Void>>();

    /**
     * The embedded ganglia peer.
     */
    private final AtomicReference<BigdataGangliaService> gangliaService = new AtomicReference<BigdataGangliaService>();

    /**
     * {@inheritDoc}
     * <P>
     * Start embedded Ganglia peer. It will develop a snapshot of the
     * metrics in memory for all nodes reporting in the ganglia network
     * and will self-report metrics from the performance counter
     * hierarchy to the ganglia network.
     */
    @Override
    public void startService(final Journal journal) {

        final AbstractStatisticsCollector statisticsCollector = journal
                .getPlatformStatisticsCollector();

        if (statisticsCollector == null)
            return;
            
        final Properties properties = journal.getProperties();

        final boolean listen = Boolean.valueOf(properties.getProperty(
                Options.GANGLIA_LISTEN, Options.DEFAULT_GANGLIA_LISTEN));

        final boolean report = Boolean.valueOf(properties.getProperty(
                Options.GANGLIA_REPORT, Options.DEFAULT_GANGLIA_REPORT));

        if (!listen && !report)
            return;

        try {

            final String hostName = AbstractStatisticsCollector.fullyQualifiedHostName;

            /*
             * Note: This needs to be the value reported by the statistics
             * collector since that it what makes it into the counter set
             * path prefix for this service.
             * 
             * TODO This implies that we can not enable the embedded ganglia
             * peer unless platform level statistics collection is enabled.
             * We should be able to separate out the collection of host
             * metrics from whether or not we are collecting metrics from
             * the bigdata service. Do this when moving the host and process
             * (pidstat) collectors into the bigdata-ganglia module.
             */
            final String serviceName = statisticsCollector.getProcessName();

            final InetAddress listenGroup = InetAddress
                    .getByName(properties.getProperty(
                            Options.GANGLIA_LISTEN_GROUP,
                            Options.DEFAULT_GANGLIA_LISTEN_GROUP));

            final int listenPort = Integer.valueOf(properties.getProperty(
                    Options.GANGLIA_LISTEN_PORT,
                    Options.DEFAULT_GANGLIA_LISTEN_PORT));

//                final boolean listen = Boolean.valueOf(properties.getProperty(
//                        Options.GANGLIA_LISTEN,
//                        Options.DEFAULT_GANGLIA_LISTEN));
//
//                final boolean report = Boolean.valueOf(properties.getProperty(
//                        Options.GANGLIA_REPORT,
//                        Options.DEFAULT_GANGLIA_REPORT));

            // Note: defaults to the listenGroup and port if nothing given.
            final InetSocketAddress[] metricsServers = GangliaUtil.parse(
                    // server(s)
                    properties.getProperty(
                    Options.GANGLIA_SERVERS,
                    Options.DEFAULT_GANGLIA_SERVERS),
                    // default host (same as listenGroup)
                    listenGroup.getHostName(),
                    // default port (same as listenGroup)
                    listenPort
                    );

            final int quietPeriod = IGangliaDefaults.QUIET_PERIOD;

            final int initialDelay = IGangliaDefaults.INITIAL_DELAY;

            /*
             * Note: Use ZERO (0) if you are running gmond on the same host.
             * That will prevent the GangliaService from transmitting a
             * different heartbeat, which would confuse gmond and gmetad.
             */
            final int heartbeatInterval = 0; // IFF using gmond.
            // final int heartbeatInterval =
            // IGangliaDefaults.HEARTBEAT_INTERVAL;

            // Use the report delay for the interval in which we scan the
            // performance counters.
            final int monitoringInterval = (int) TimeUnit.MILLISECONDS
                    .toSeconds(Long.parseLong(properties.getProperty(
                            Options.REPORT_DELAY,
                            Options.DEFAULT_REPORT_DELAY)));

            final String defaultUnits = IGangliaDefaults.DEFAULT_UNITS;

            final GangliaSlopeEnum defaultSlope = IGangliaDefaults.DEFAULT_SLOPE;

            final int defaultTMax = IGangliaDefaults.DEFAULT_TMAX;

            final int defaultDMax = IGangliaDefaults.DEFAULT_DMAX;

            // Note: Factory is extensible (application can add its own
            // delegates).
            final GangliaMetadataFactory metadataFactory = new GangliaMetadataFactory(
                    new DefaultMetadataFactory(//
                            defaultUnits,//
                            defaultSlope,//
                            defaultTMax,//
                            defaultDMax//
                    ));

            /*
             * Layer on the ability to (a) recognize and align host
             * bigdata's performance counters hierarchy with those declared
             * by ganglia and; (b) provide nice declarations for various
             * application counters of interest.
             */
            metadataFactory.add(new BigdataMetadataFactory(hostName,
                    serviceName, defaultSlope, defaultTMax, defaultDMax,
                    heartbeatInterval));

            // The embedded ganglia peer.
            final BigdataGangliaService gangliaService = new BigdataGangliaService(
                    hostName, //
                    serviceName, //
                    metricsServers,//
                    listenGroup,//
                    listenPort, //
                    listen,// listen
                    report,// report
                    false,// mock,
                    quietPeriod, //
                    initialDelay, //
                    heartbeatInterval,//
                    monitoringInterval, //
                    defaultDMax,// globalDMax
                    metadataFactory);

            // Collect and report host metrics.
            gangliaService.addMetricCollector(new HostMetricsCollector(
                    statisticsCollector));

            // Collect and report QueryEngine metrics.
            gangliaService
                    .addMetricCollector(new QueryEngineMetricsCollector(
                            journal, statisticsCollector));

            /*
             * TODO The problem with reporting per-service statistics is
             * that ganglia lacks a facility to readily aggregate statistics
             * across services on a host (SMS + anything). The only way this
             * can readily be made to work is if each service has a distinct
             * metric for the same value (e.g., Mark and Sweep GC). However,
             * that causes a very large number of distinct metrics. I have
             * commented this out for now while I think it through some
             * more. Maybe we will wind up only reporting the per-host
             * counters to ganglia?
             * 
             * Maybe the right way to handle this is to just filter by the
             * service type? Basically, that is what we are doing for the
             * QueryEngine metrics.
             */
            // Collect and report service metrics.
//                gangliaService.addMetricCollector(new ServiceMetricsCollector(
//                        statisticsCollector, null/* filter */));

            // Wrap as Future.
            final FutureTask<Void> ft = new FutureTask<Void>(
                    gangliaService, (Void) null);
            
            // Save reference to future.
            gangliaFuture.set(ft);

            // Set the state reference.
            GangliaPlugIn.this.gangliaService.set(gangliaService);

            // Start the embedded ganglia service.
            journal.getExecutorService().submit(ft);

        } catch (RejectedExecutionException t) {
            
            /*
             * Ignore.
             * 
             * Note: This occurs if the federation shutdown() before we
             * start the embedded ganglia peer. For example, it is common
             * when running a short lived utility service such as
             * ListServices.
             */
            
        } catch (Throwable t) {

            log.error(t, t);

        }

    }
        
    /**
     * {@inheritDoc}
     * <p>
     * Note: The embedded GangliaService is executed on the main thread pool. We
     * need to terminate the GangliaService in order for the thread pool to
     * shutdown.
     */
    @Override
    public void stopService(final boolean immediateShutdown) {

        final FutureTask<Void> ft = gangliaFuture.getAndSet(null);

        if (ft != null) {

            ft.cancel(immediateShutdown/* mayInterruptIfRunning */);

        }

        // Clear the state reference.
        gangliaService.set(null);

    }

    @Override
    public boolean isRunning() {

        final FutureTask<Void> ft = gangliaFuture.get();

        if (ft == null || ft.isDone())
            return false;

        return true;
        
    }
    
    @Override
    public GangliaService getService() {

        return gangliaService.get();
        
    }

}
