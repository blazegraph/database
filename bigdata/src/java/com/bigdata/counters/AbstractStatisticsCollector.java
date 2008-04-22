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
 * Created on Mar 13, 2008
 */

package com.bigdata.counters;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.counters.linux.StatisticsCollectorForLinux;
import com.bigdata.counters.win.StatisticsCollectorForWindows;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Base class for collecting data on a host. The data are described by a
 * hierarchical collection of {@link ICounterSet}s and {@link ICounter}s. A
 * {@link IRequiredHostCounters minimum set of counters} is defined which SHOULD
 * be available for decision-making. Implementations are free to report any
 * additional data which they can make available. Reporting is assumed to be
 * periodic, e.g., every 60 seconds or so. The purpose of these data is to
 * support decision-making concerning the over- and under-utilization of hosts
 * in support of load balancing of services deployed over those hosts.
 * <p>
 * An effort has been made to align the core set of counters for both Windows
 * and Un*x platforms so as to support the declared counters on all platforms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractStatisticsCollector implements IStatisticsCollector {

    protected static final String ps = ICounterSet.pathSeparator;
    
    static protected final Logger log = Logger
            .getLogger(AbstractStatisticsCollector.class);

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /** {@link InetAddress#getHostName()} for this host. */
    static final public String hostname;

    /** {@link InetAddress#getCanonicalHostName()} for this host. */
    static final public String fullyQualifiedHostName;

    /** The path prefix under which all counters for this host are found. */
    static final public String hostPathPrefix;

    static {
    
        try {

            hostname = InetAddress.getLocalHost().getHostName();
            
            fullyQualifiedHostName = InetAddress.getLocalHost().getCanonicalHostName();
            
        } catch (UnknownHostException e) {
            
            throw new AssertionError(e);
            
        }

        hostPathPrefix = ICounterSet.pathSeparator + fullyQualifiedHostName
                + ICounterSet.pathSeparator;

        log.info("hostname  : "+hostname);
        log.info("FQDN      : "+fullyQualifiedHostName);
        log.info("hostPrefix: "+hostPathPrefix);
        
    }
    
    /** Reporting interval in seconds. */
    final protected int interval;
    
    /**
     * The interval in seconds at which the counter values are read from the
     * host platform.
     */
    public int getInterval() {

        return interval;
        
    }
    
    protected AbstractStatisticsCollector(int interval) {
    
        if (interval <= 0)
            throw new IllegalArgumentException();

        log.info("interval=" + interval);
        
        this.interval = interval;
        
    }
    
//    /**
//     * Return the load average for the last minute if available and -1
//     * otherwise.
//     * <p>
//     * Note: The load average is available on 1.6+ JVMs.
//     * 
//     * @see OperatingSystemMXBean
//     */
//    public double getSystemLoadAverage()
//    {
//        
////        double version = Double.parseDouble(System.getProperty("java.vm.version"));
////      if(version>=1.6) {
//        
//        double loadAverage = -1;
//        
//        final OperatingSystemMXBean mbean = ManagementFactory
//                .getOperatingSystemMXBean();
//        
//        /*
//         * Use reflection since method is only available as of 1.6
//         */
//        Method method;
//        try {
//            method = mbean.getClass().getMethod("getSystemLoadAverage",
//                    new Class[] {});
//            loadAverage = (Double) method.invoke(mbean, new Object[] {});
//        } catch (SecurityException e) {
//            log.warn(e.getMessage(), e);
//        } catch (NoSuchMethodException e) {
//            // Note: method is only defined since 1.6
//            log.warn(e.getMessage(), e);
//        } catch (IllegalAccessException e) {
//            log.warn(e.getMessage(), e);
//        } catch (InvocationTargetException e) {
//            log.warn(e.getMessage(), e);
//        }
//
//        return loadAverage;
//
//    }
    
    /**
     * {@link CounterSet} hierarchy.
     */
    private CounterSet countersRoot;

    /**
     * Return the counter hierarchy. The returned hierarchy only includes those
     * counters whose values are available from the JVM. This collection is
     * normally augmented with platform specific performance counters collected
     * using an {@link AbstractProcessCollector}.
     * <p>
     * Note: Subclasses MUST extend this method to initialize their own
     * counters.
     */
    synchronized public CounterSet getCounters() {
        
        if (countersRoot == null) {

            countersRoot = new CounterSet();

            // os.arch
            countersRoot.addCounter(hostPathPrefix
                    + IHostCounters.Info_Architecture,
                    new OneShotInstrument<String>(System.getProperty("os.arch")));
            
            // os.name
            countersRoot.addCounter(hostPathPrefix
                    + IHostCounters.Info_OperatingSystemName,
                    new OneShotInstrument<String>(System.getProperty("os.name")));
            
            // os.version
            countersRoot.addCounter(hostPathPrefix
                    + IHostCounters.Info_OperatingSystemVersion,
                    new OneShotInstrument<String>(System.getProperty("os.version")));
            
            // #of processors.
            countersRoot.addCounter(hostPathPrefix
                    + IHostCounters.Info_NumProcessors,
                    new OneShotInstrument<Integer>(SystemUtil.numProcessors()));
            
            // processor info
            countersRoot.addCounter(hostPathPrefix
                    + IHostCounters.Info_ProcessorInfo,
                    new OneShotInstrument<String>(SystemUtil.cpuInfo()));
            
        }
        
        return countersRoot;
        
    }
    
    /**
     * Start collecting host performance data -- must be extended by the
     * concrete subclass.
     */
    public void start() {

        log.info("Starting collection.");

        installShutdownHook();

    }

    /**
     * Stop collecting host performance data -- must be extended by the concrete
     * subclass.
     */
    public void stop() {
        
        log.info("Stopping collection.");

    }

    /**
     * Installs a {@link Runtime#addShutdownHook(Thread)} that executes
     * {@link #stop()}.
     * <p>
     * Note: The runtime shutdown hook appears to be a robust way to handle ^C
     * by providing a clean service termination. However, under eclipse (at
     * least when running under Windows) you may find that the shutdown hook
     * does not run when you terminate a Java application and that typedef
     * process build up in the Task Manager as a result. This should not be the
     * case during normal deployment.
     */
    protected void installShutdownHook() {
     
        Runtime.getRuntime().addShutdownHook(new Thread() {
        
            public void run() {
                
                AbstractStatisticsCollector.this.stop();
                
            }
            
        });

    }

    /**
     * Options for {@link AbstractStatisticsCollector}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
        
        /**
         * The interval in seconds at which the performance counters of the host
         * platform will be sampled (default 60).
         */
        public String INTERVAL = "counters.interval";
        
        public String DEFAULT_INTERVAL = "60";
        
        /**
         * The name of the process whose per-process performance counters are to
         * be collected (required, no default). This causes the per-process
         * counters to be reported using the path:
         * 
         * <strong>/<i>fullyQualifiedHostname</i>/<i>processName</i>/...</strong>
         * <p>
         * Note: Services are generally associated with a {@link UUID} and that
         * {@link UUID} is generally used as the service name. A single host may
         * run many different services and will report the counters for each
         * service using the path formed as described above.
         */
        public String PROCESS_NAME = "counters.processName";
        
    }
    
    /**
     * Create an instance appropriate for the operating system on which the JVM
     * is running.
     * 
     * @param properties
     *            See {@link Options}
     * 
     * @throws UnsupportedOperationException
     *             If there is no implementation available on the operating
     *             system on which you are running.
     * 
     * @see Options
     */
    public static AbstractStatisticsCollector newInstance(Properties properties) {
        
        final int interval = Integer.parseInt(properties.getProperty(
                Options.INTERVAL, Options.DEFAULT_INTERVAL));

        if (interval <= 0)
            throw new IllegalArgumentException();
        
        final String processName = properties.getProperty(Options.PROCESS_NAME);
        
        if (processName == null)
            throw new IllegalArgumentException(
                    "Required option not specified: " + Options.PROCESS_NAME);
        
        final String osname = System.getProperty("os.name").toLowerCase();
        
        if(osname.equalsIgnoreCase("linux")) {
            
            return new StatisticsCollectorForLinux(interval, processName);
            
        } else if(osname.contains("windows")) {
            
            return new StatisticsCollectorForWindows(interval);
            
        } else {
            
            throw new UnsupportedOperationException(
                    "No implementation available on "
                            + System.getProperty("os.getname"));
            
        }
        
    }
    
    /**
     * Utility runs the {@link AbstractStatisticsCollector} appropriate for your
     * operating system. Before performance counter collection starts the static
     * counters will be written on stdout. The appropriate process(es) are then
     * started to collect the dynamic performance counters. Collection will
     * occur every {@link Options#INTERVAL} seconds. The program will make 10
     * collections by default and will write the updated counters on stdout
     * every {@link Options#INTERVAL} seconds.
     * <p>
     * Parameters also may be specified using <code>-D</code>. See
     * {@link Options}.
     * 
     * @param args [
     *            <i>interval</i> [<i>count</i>]]
     *            <p>
     *            <i>interval</i> is the collection interval in seconds and
     *            defaults to {@link Options#DEFAULT_INTERVAL}.
     *            <p>
     *            <i>count</i> is the #of collections to be made and defaults
     *            to <code>10</code>. Specify zero (0) to run until halted.
     * 
     * @throws InterruptedException
     * @throws RuntimeException
     *             if the arguments are not valid.
     * @throws UnsupportedOperationException
     *             if no implementation is available for your operating system.
     */
    public static void main(String[] args) throws InterruptedException {

        final int DEFAULT_COUNT = 10;
        final int nargs = args.length;
        final int interval;
        final int count;
        if (nargs == 0) {
            interval = Integer.parseInt(Options.DEFAULT_INTERVAL);
            count = DEFAULT_COUNT;
        } else if (nargs == 1) {
            interval = Integer.parseInt(args[0]);
            count = DEFAULT_COUNT;
        } else if (nargs == 2) {
            interval = Integer.parseInt(args[0]);
            count = Integer.parseInt(args[1]);
        } else {
            throw new RuntimeException("usage: [interval [count]]");
        }
        
        if (interval <= 0)
            throw new RuntimeException("interval must be positive");
        
        if (count < 0)
            throw new RuntimeException("count must be non-negative");

        Properties properties = new Properties(System.getProperties());
        
        if (nargs != 0) {
            
            // Override the interval property from the command line.
            properties.setProperty(Options.INTERVAL,""+interval);
            
        }

        if(properties.getProperty(Options.PROCESS_NAME)==null) {
            
            /*
             * Set a default process name if none was specified in the
             * environment.
             * 
             * Note: Normally the process name is specified explicitly by the
             * service which instantiates the performance counter collection for
             * that process. We specify a default here since main() is used for
             * testing purposes only.
             */

            properties.setProperty(Options.PROCESS_NAME,"testService");
            
        }
        
        final AbstractStatisticsCollector client = AbstractStatisticsCollector
                .newInstance( properties );

        // write counters before we start the client
        System.out.println(client.getCounters().toString());
        
        System.err.println("Starting performance counter collection: interval="
                + client.interval + ", count=" + count);
        
        client.start();

        /*
         * HTTPD service reporting out statistics.
         */
        AbstractHTTPD httpd = null;
        {
            final int port = 8080;
            if (port != 0) {
                try {
                    httpd = new CounterSetHTTPD(port,client.countersRoot);
                } catch (IOException e) {
                    log.warn("Could not start httpd: port=" + port+" : "+e);
                }
            }
            
        }
        
        int n = 0;
        
        final long begin = System.currentTimeMillis();
        
        while (count ==0 || n < count) {
        
            Thread.sleep(client.interval * 1000/*ms*/);

            final long elapsed = System.currentTimeMillis() - begin;
            
            System.err.println("Report #"+n+" after "+(elapsed/1000)+" seconds ");
            
            System.out.println(client.getCounters().toString());
            
            n++;
            
        }
        
        System.err.println("Stopping performance counter collection");
        
        client.stop();

        if (httpd != null)
            httpd.shutdown();
        
        System.err.println("Done");
        
    }

    /**
     * Converts KB to bytes.
     * 
     * @param kb
     *            The #of kilobytes.
     *            
     * @return The #of bytes.
     */
    static public Double kb2b(String kb) {

        double d = Double.parseDouble(kb);
        
        double x = d * Bytes.kilobyte32;
        
        return x;
        
    }

}
