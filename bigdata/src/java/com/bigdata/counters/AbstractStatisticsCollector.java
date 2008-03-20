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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.rawstore.Bytes;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
 * @todo a varient of counters can appear on both a per-host and a per-process
 *       basis. review carefully for correct use.
 * 
 * FIXME make sure that all "%" counters are normalized to [0.0:1.0]. it would
 * be useful to have declared ranges for some counters for this purpose
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractStatisticsCollector {

    protected static final String ps = ICounterSet.pathSeparator;
    
    static protected final Logger log = Logger
            .getLogger(AbstractStatisticsCollector.class);

    /**
     * Various namespaces for per-host and per-process counters.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ICounterHierarchy {

        /**
         * The namespace for counters describing the host platform. These are
         * essentially "unchanging" counters.
         */
        String Info = "Info";
        
        /**
         * The namespace for counters dealing with processor(s) (CPU).
         */
        String CPU = "CPU";
        
        /**
         * The namespace for counters dealing with memory (RAM).
         */
        String Memory = "Memory";

        /**
         * The namespace for counters dealing with logical aggregations of disk.
         */
        String LogicalDisk = "LogicalDisk";

        /**
         * The namespace for counters dealing with physical disks.
         */
        String PhysicalDisk = "PhysicalDisk";

    }

    /**
     * The set of core (required) counters that must be reported for all
     * platforms. The items declared on this interface are relative path names
     * for {@link ICounterSet}s and {@link ICounter}s. The root for the path
     * is generally the fully qualified domain name of a host (as reported by
     * {@link InetAddress#getCanonicalHostName()}, a federation, or a service.
     * <p>
     * Note: it is good practice to keep these three namespaces distinct so that
     * you can aggregate counters readily without these different contexts.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IRequiredHostCounters extends ICounterHierarchy {

        /*
         * INFO
         */
        
        /**
         * The name of the operating system running on the platform as reported
         * by {@link System#getProperty(String)} for the <code>os.name</code>
         * property.
         */
        String Info_OperatingSystemName = Info + ps
                + "Operating System Name";

        /**
         * The version of the operating system running on the platform as
         * reported by {@link System#getProperty(String)} for the
         * <code>os.version</code> property.
         */
        String Info_OperatingSystemVersion = Info + ps
                + "Operating System Version";

        /**
         * System architecture as reported by {@link System#getProperty(String)}
         * for the <code>os.arch</code> property.
         */
        String Info_Architecture = Info + ps + "Architecture";

        /*
         * CPU
         */
        
        /** Percentage of the time the processor is not idle. */
        String CPU_PercentProcessorTime = CPU + ps
                + "% Processor Time";

        /*
         * Memory
         */
        
        /** Major page faults per second. */
        String Memory_PageFaultsPerSecond = Memory + ps
                + "Page Faults Per Second";

        /*
         * LogicalDisk
         */
        
        /** Percentage of the disk space that is free (unused) [0.0:1.0]. */
        String LogicalDisk_PercentFreeSpace = LogicalDisk + ps + "% Free Space";

        /*
         * PhysicalDisk
         */
        
        /** Disk bytes read per second for the host. */
        String PhysicalDisk_BytesReadPerSec = PhysicalDisk + ps
                + "Bytes Read Per Second";

        /** Disk bytes written per second for the host. */
        String PhysicalDisk_BytesWrittenPerSec = PhysicalDisk + ps
                + "Bytes Written Per Second";


    };
 
    /**
     * Additional counters that hosts can report.
     * 
     * @todo pageFaultsPerSec (majflt/s)
     * 
     * @todo os diskCache (dis|en)abled
     * @todo #disks
     * @todo disk descriptions
     * @todo disk space, space avail, hardware disk cache (dis|en)abled.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IHostCounters extends IRequiredHostCounters {
        
        /*
         * Info
         */
        
        /** CPU family information. */
        String Info_ProcessorInfo = Info+ps+"Processor Info";

        /** The #of processors. */
        String Info_NumProcessors = Info+ps+"Number of Processors";
        
        /*
         * CPU
         */

        /**
         * Percentage of the time the processor is not idle that it is executing
         * at the user level (normalized to 100% in single CPU and SMP
         * environments).
         */
        String CPU_PercentUserTime = CPU + ps + "% User Time";

        /**
         * Percentage of the time the processor is not idle that it is executing
         * at the system (aka kernel) level (normalized to 100% in single CPU
         * and SMP environments).
         */
        String CPU_PercentSystemTime = CPU + ps + "% System Time";

        /**
         * Percentage of the time the CPU(s) were idle while the system had an
         * outstanding IO.
         */
        String CPU_PercentIOWait = CPU + ps + "% IO Wait";

        /*
         * Memory
         */

        /** The total amount of memory available to the host. */
        String Memory_Available = Memory + ps + "Total bytes available";

        /*
         * PhysicalDisk
         */

        /** #of disk read operations per second. */
        String PhysicalDisk_ReadsPerSec = PhysicalDisk + ps
                + "Reads Per Second";

        /** #of disk write operations per second. */
        String PhysicalDisk_WritesPerSec = PhysicalDisk + ps
                + "Writes Per Second";

    }

    /**
     * Counters defined on a per-process basis.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IProcessCounters extends ICounterHierarchy {

        /*
         * CPU
         */

        /**
         * Percentage of the time the processor is not idle that it is executing
         * at the user level (normalized to 100% in single CPU and SMP
         * environments).
         */
        String CPU_PercentUserTime = CPU + ps + "% User Time";

        /**
         * Percentage of the time the processor is not idle that it is executing
         * at the system (aka kernel) level (normalized to 100% in single CPU
         * and SMP environments).
         */
        String CPU_PercentSystemTime = CPU + ps + "% System Time";

        /** Percentage of the time the processor is not idle. */
        String CPU_PercentProcessorTime = CPU + ps + "% Processor Time";

        /*
         * Memory
         */

        /**
         * Faults that did not require loading a page from disk.
         */
        String Memory_minorFaultsPerSec = Memory + ps
                + "Minor Faults per Second";

        /**
         * Faults which required loading a page from disk.
         */
        String Memory_majorFaultsPerSec = Memory + ps
                + "Major Faults per Second";

        /**
         * The virtual memory usage of the process in bytes.
         */
        String Memory_virtualSize = Memory + ps + "Virtual Size";

        /**
         * The non-swapped physical memory used by the process in bytes.
         */
        String Memory_residentSetSize = Memory + ps + "Resident Set Size";

        /**
         * The percentage of the phsyical memory used by the process.
         */
        String Memory_percentMemorySize = Memory + ps + "Percent Memory Size";

        /**
         * The value reported by {@link Runtime#maxMemory()} (the maximum amount
         * of memory that the JVM will attempt to use). This should be a
         * {@link OneShotInstrument}.
         */
        String Memory_runtimeMaxMemory = Memory + ps + "Runtime Max Memory";
        
        /**
         * The value reported by {@link Runtime#freeMemory()} (the amount of
         * free memory in the JVM)).
         */
        String Memory_runtimeFreeMemory = Memory + ps + "Runtime Free Memory";
        
        /**
         * The value reported by {@link Runtime#totalMemory()} (the amount of
         * total memory in the JVM, which may vary over time).
         */
        String Memory_runtimeTotalMemory = Memory + ps + "Runtime Total Memory";
        
        /*
         * IO
         */
        
        /**
         * The rate at which the process is reading data from disk in bytes per
         * second.
         */
        String PhysicalDisk_BytesReadPerSec = PhysicalDisk + ps
                + "Bytes Read per Second";

        /**
         * The rate at which the process is writing data on the disk in bytes
         * per second (cached writes may be reported in this quantity).
         */
        String PhysicalDisk_BytesWrittenPerSec = PhysicalDisk + ps
                + "Bytes Written per Second";

    }

    /** {@link InetAddress#getHostName()} for this host. */
    final public String hostname;

    /** {@link InetAddress#getCanonicalHostName()} for this host. */
    final public String fullyQualifiedHostName;

    /** The path prefix under which all counters for this host are found. */
    final public String hostPathPrefix;

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
    
    /**
     * Return the load average for the last minute if available and -1
     * otherwise.
     * <p>
     * Note: The load average is available on 1.6+ JVMs.
     * 
     * @see OperatingSystemMXBean
     */
    public double getSystemLoadAverage()
    {
        
//        double version = Double.parseDouble(System.getProperty("java.vm.version"));
//      if(version>=1.6) {
        
        double loadAverage = -1;
        
        final OperatingSystemMXBean mbean = ManagementFactory
                .getOperatingSystemMXBean();
        
        /*
         * Use reflection since method is only available as of 1.6
         */
        Method method;
        try {
            method = mbean.getClass().getMethod("getSystemLoadAverage",
                    new Class[] {});
            loadAverage = (Double) method.invoke(mbean, new Object[] {});
        } catch (SecurityException e) {
            log.warn(e.getMessage(), e);
        } catch (NoSuchMethodException e) {
            // Note: method is only defined since 1.6
            log.warn(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            log.warn(e.getMessage(), e);
        } catch (InvocationTargetException e) {
            log.warn(e.getMessage(), e);
        }

        return loadAverage;

    }
    
//    /**
//     * Return the JVM PID.
//     * 
//     * @throws UnsupportedOperationException
//     *             if the pid can not be extracted.
//     * 
//     * @see RuntimeMXBean#getName(), A web search will show that this is
//     *      generally of the form "pid@host".  However this is definately
//     *      NOT guarenteed by the javadoc.
//     */
//    public int getPID() {
//        
//        String name = ManagementFactory.getRuntimeMXBean().getName();
//        
//        Matcher matcher = pidPattern.matcher(name);
//        
//        if(!matcher.matches()) {
//            
//            throw new UnsupportedOperationException("Could not extract pid from ["+name+"]");
//            
//        }
//        
//        final int pid = Integer.parseInt(matcher.group(1));
//        
//        log.info("pid="+pid);
//        
//        return pid;
//        
//    }
//    private final Pattern pidPattern = Pattern.compile("^([0-9]+)@");

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

    protected abstract class AbstractProcessReader implements Runnable {
        
        InputStream is;
        
        public void start(InputStream is) {
        
            assert is!=null;
            
            this.is = is;
            
        }
        
    }
    
    protected abstract class ProcessReaderHelper extends AbstractProcessReader {
        
        protected final ActiveProcess activeProcess;
        
        protected final LineNumberReader r;
        
        public ProcessReaderHelper(ActiveProcess activeProcess) {
        
            this.activeProcess = activeProcess;
            
            r = new LineNumberReader( new InputStreamReader( is ));
            
        }
        
        /**
         * Returns the next line and blocks if a line is not available.
         * 
         * 
         * @return The next line.
         * 
         * @throws InterruptedException 
         * 
         * @throws IOException
         *             if the source is closed.
         * @throws InterruptedException
         *             if the thread has been interrupted (this is
         *             normal during shutdown).
         */
        public String readLine() throws IOException, InterruptedException {
            
            while(activeProcess.isAlive()) {
                
                if(Thread.currentThread().isInterrupted()) {
                    
                    throw new InterruptedException();
                    
                }
                
                if(!r.ready()) {
                    
                    Thread.sleep(100/*ms*/);
                    
                    continue;
                    
                }

                return r.readLine();
                
            }
            
            throw new IOException("Closed");
            
        }
        
        public void run() {
            
            try {
                readProcess();
            } catch (InterruptedException e) {
                AbstractStatisticsCollector.log.info("Interrupted - will halt.");
            } catch (Exception e) {
                AbstractStatisticsCollector.log.fatal(e.getMessage(),e);
            }
            
        }

        /**
         * Responsible for reading the data.
         * 
         * @throws Exception
         */
        abstract protected void readProcess() throws Exception;
    
    }
    
    /**
     * Command manages the execution and termination of a native process and an
     * object reading the output of that process.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ActiveProcess {
        
        /**
         * Used to read {@link #is} and aggregate the performance data reported
         * by the {@link #process}.
         */
        protected final ExecutorService readService = Executors
                .newSingleThreadExecutor(DaemonThreadFactory
                        .defaultThreadFactory());
        
        protected Process process = null;

        protected InputStream is = null;

        protected Future readerFuture;
        
        public ActiveProcess(List<String> command, AbstractProcessCollector collector, AbstractProcessReader processReader) {
                    
            // log the command that will be run.
            {
                
                StringBuilder sb = new StringBuilder();

                for (String s : command) {

                    sb.append( s +" ");

                }

                log.info("command:\n" + sb);

            }
            
            try {

                ProcessBuilder tmp = new ProcessBuilder(command);

                collector.setEnvironment(tmp.environment());
                
                process = tmp.start();

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

            is = process.getInputStream();

            /*
             * @todo restart processes if it dies before we shut it down, but no
             * more than some #of tries. if the process dies then we will not
             * have any data for this host.
             */
            
            processReader.start(is);
            
            readerFuture = readService.submit(processReader);

        }
        
        public void stop() {
            
            // attempt to cancel the reader.
            readerFuture.cancel(true/*mayInterruptIfRunning*/);
            
            // shutdown the thread running the reader.
            readService.shutdownNow();

            if (process != null) {

                // destroy the running process.
                process.destroy();

                process = null;

                is = null;

            }

        }
        
        /**
         * Return <code>true</code> unless the process is known to be dead.
         */
        public boolean isAlive() {
            
            if(readerFuture.isDone() || process == null || is == null) {
                
                return false;
                
            }
            
            return true;
            
        }
        
    }

    /**
     * Base class for collection of performance counters as reported by a native process.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract public static class AbstractProcessCollector extends AbstractStatisticsCollector {

        protected ActiveProcess activeProcess;

        /**
         * 
         * @param interval
         *            The interval at which the performance counters will be
         *            read.
         */
        public AbstractProcessCollector(int interval) {
            
            super(interval);

        }

        /**
         * Override if you want to impose settings on environment variables.
         */
        protected void setEnvironment(Map<String,String> env) {
            
        }
        
        public void start() {

            super.start();

            activeProcess = new ActiveProcess(getCommand(), this, getProcessReader());

        }

        public void stop() {

            super.stop();

            if (activeProcess != null) {

                activeProcess.stop();

                activeProcess = null;

            }

        }

        abstract public List<String> getCommand();

        abstract public AbstractProcessReader getProcessReader();

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
        
        final String osname = System.getProperty("os.name").toLowerCase();
        
        if(osname.equalsIgnoreCase("linux")) {
            
            return new StatisticsCollectorForLinux(interval);
            
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
     * Parameters may be specified using <code>-D</code>. See {@link Options}.
     * 
     * @param args
     *            <i>count</i> The #of collections to be made. Default is 10.
     *            Specify zero (0) to run until halted.
     * 
     * @throws InterruptedException
     * 
     * @throws UnsupportedOperationException
     *             if no implementation is available for your operating system.
     */
    public static void main(String[] args) throws InterruptedException {

        final int count = (args.length == 0 ? 10 : Integer.parseInt(args[0]));
        
        if (count < 0)
            throw new RuntimeException("count must be non-negative");
        
        final AbstractStatisticsCollector client = AbstractStatisticsCollector
                .newInstance(System.getProperties());

        // write counters before we start the client
        System.out.println(client.getCounters().toString());
        
        System.err.println("Starting performance counter collection: interval="
                + client.interval + ", count=" + count);
        
        client.start();

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
