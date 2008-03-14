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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.util.CSVReader;
import com.bigdata.util.CSVReader.Header;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Base class for collecting data on a host. The data are described by a
 * hierarchy collection of {@link ICounterSet}s. A
 * {@link IRequiredHostCounters minimum set of counters} is defined which SHOULD
 * be available for decision-making, but implementations are free to report any
 * additional data which they can make available.
 * <p>
 * An effort has been made to align the core set of counters for both Windows
 * and Un*x platforms so as to support the declared counters on all platforms.
 * 
 * @todo implementations should restart processes that are reporting platform
 *       specific counter data if those processes are killed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractStatisticsCollector {

    static protected final Logger log = Logger
            .getLogger(AbstractStatisticsCollector.class);

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
    public interface IRequiredHostCounters {
        
        public final String ps = ICounterSet.pathSeparator; 

        /*
         * counter set definitions (hierarchy).
         */
        
        /**
         * The namespace for counters dealing with memory (RAM).
         */
         String Memory = "Memory";

        /**
         * The namespace for counters dealing with processor(s) (CPU).
         */
         String Processor = "Processor";
        
        /**
         * The namespace for counters dealing with logical aggregations of disk.
         */
         String LogicalDisk = "LogicalDisk";
        
        /**
         * The namespace for counters dealing with physical disks.
         */
         String PhysicalDisk = "PhysicalDisk";

        /*
         * counter definitions.
         */
        
        /** Major page faults per second. */
         String Memory_PageFaultsPerSecond = Memory + ps
                + "Page Faults Per Second";

        /** Percentage of the time the processor is not idle. */
         String Processor_PercentProcessorTime = Processor + ps
                + "% Processor Time";

        /** Percentage of the disk space that is free (unused). */
         String LogicalDisk_PercentFreeSpace = LogicalDisk + ps
                + "% Free Space";

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
     * @todo could report os, architecture, #of cpus, #of disks, amount of ram
     *       (the more or less static host information).
     * @todo pageFaultsPerSec (majflt/s)
     * 
     * @todo os diskCache (dis|en)abled
     * @todo #cpus
     * @todo architecture
     * @todo #disks
     * @todo disk descriptions
     * @todo disk space, space avail, hardware disk cache (dis|en)abled.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IHostCounters extends IRequiredHostCounters {
        
        /** #of disk read operations per second. */
        String PhysicalDisk_ReadsPerSec = PhysicalDisk+ps+"Reads Per Second";

        /** #of disk write operations per second. */
        String PhysicalDisk_WritesPerSec = PhysicalDisk+ps+"Writes Per Second";
        
        /**
         * The namespace for counters describing the host platform. These are
         * essentially "unchanging" counters.
         * 
         * @todo it would be nice if they were reported only once rather than
         *       every 60 seconds.
         * 
         * @todo these could be moved into their respective categories (Memory,
         *       Processor, OperatingSystem).
         */
        String Platform = "Platform";

        /** CPU family information. */
        String Platform_ProcessorInfo = Platform+ps+"Processor Info";

        /** The #of processors. */
        String Platform_NumProcessors = Platform+ps+"Number of Processors";
        
        /** System architecture <code>os.arch</code> */
        String Platform_Architecture = Platform+ps+"Architecture";

        /** The name of the operating system running on the platform <code>os.name</code>. */
        String Platform_OperatingSystemName = Platform+ps+"Operating System Name";

        /** The version of the operating system running on the platform <code>os.version</code>. */
        String Platform_OperatingSystemVersion = Platform+ps+"Operating System Version";
        
        /** The total amount of memory available to the host. */
        String Memory_Available = Memory+ps+"Total bytes available";

    }

    /**
     * Map containing 60 seconds of data for each counter.
     */
    protected Map<String,Object[]> counters = new HashMap<String,Object[]>();
    
    /** {@link InetAddress#getHostName()} for this host. */
    final protected String hostname;

    /** {@link InetAddress#getCanonicalHostName()} for this host. */
    final protected String fullyQualifiedHostName;

    /** The path prefix under which all counters for this host are found. */
    final protected String hostPathPrefix;

    protected AbstractStatisticsCollector() {
    
        try {

            hostname = InetAddress.getLocalHost().getHostName();
            
            fullyQualifiedHostName = InetAddress.getLocalHost().getCanonicalHostName();
            
        } catch (UnknownHostException e) {
            
            throw new AssertionError(e);
            
        }

        hostPathPrefix = ICounterSet.pathSeparator + fullyQualifiedHostName
                + ICounterSet.pathSeparator;

        System.err.println("hostname  : "+hostname);
        System.err.println("FQDN      : "+fullyQualifiedHostName);
        System.err.println("hostPrefix: "+hostPathPrefix);

        countersRoot = new CounterSet(fullyQualifiedHostName);

        // os.arch
        countersRoot.addCounterByPath(hostPathPrefix
                + IHostCounters.Platform_Architecture,
                new IInstrument<String>() {
                    public String getValue() {
                        return System.getProperty("os.arch");
                    }
                });
        
        // os.name
        countersRoot.addCounterByPath(hostPathPrefix
                + IHostCounters.Platform_OperatingSystemName,
                new IInstrument<String>() {
                    public String getValue() {
                        return System.getProperty("os.name");
                    }
                });
        
        // os.version
        countersRoot.addCounterByPath(hostPathPrefix
                + IHostCounters.Platform_OperatingSystemVersion,
                new IInstrument<String>() {
                    public String getValue() {
                        return System.getProperty("os.version");
                    }
                });
        
        // #of processors.
        countersRoot.addCounterByPath(hostPathPrefix
                + IHostCounters.Platform_NumProcessors,
                new IInstrument<Integer>() {
                    public Integer getValue() {
                        return SystemUtil.numProcessors();
                    }
                });
        
        // processor info
        countersRoot.addCounterByPath(hostPathPrefix
                + IHostCounters.Platform_ProcessorInfo,
                new IInstrument<String>() {
                    public String getValue() {
                        return SystemUtil.cpuInfo();
                    }
                });
        
//        // @todo this is per-process, not per host so move it to the service api and add host mem reporting here.
//        countersRoot.addCounterByPath(hostPathPrefix
//                + IHostCounters.Memory_Available,
//                new IInstrument<Long>() {
//                    public Long getValue() {
//                        return Runtime.getRuntime().maxMemory();
//                    }
//                });
        
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
    
    /**
     * Return the JVM PID.
     * 
     * @throws UnsupportedOperationException
     *             if the pid can not be extracted.
     * 
     * @see RuntimeMXBean#getName(), A web search will show that this is
     *      generally of the form "pid@host".  However this is definately
     *      NOT guarenteed by the javadoc.
     */
    public int getPID() {
        
        String name = ManagementFactory.getRuntimeMXBean().getName();
        
        Matcher matcher = pidPattern.matcher(name);
        
        if(!matcher.matches()) {
            
            throw new UnsupportedOperationException("Could not extract pid from ["+name+"]");
            
        }
        
        final int pid = Integer.parseInt(matcher.group(1));
        
        log.info("pid="+pid);
        
        return pid;
        
    }
    private final Pattern pidPattern = Pattern.compile("^([0-9]+)@");

    /**
     * {@link CounterSet} hierarchy.
     */
    final protected CounterSet countersRoot;

    public ICounterSet getCounterSet() {
        
        if (countersRoot == null)
            throw new IllegalStateException();
        
        return countersRoot;
        
    }
    
    /**
     * Start collecting process performance data.
     */
    abstract protected void start();

    /**
     * Stop collecting process performance data.
     */
    abstract protected void stop();

//    /**
//     * Report on the last 60 seconds.
//     */
//    public Map<String,Double> getAverages() {
//        
//        Map<String,Double> averages = new TreeMap<String,Double>();
//        
//        for(Map.Entry<String,Object[]> entry : counters.entrySet() ) {
//            
//            String counter = entry.getKey();
//            
//            Object[] samples = entry.getValue();
//
//            Double average = getAverage(samples);
//
//            if(average == null) {
//                
//                log.debug("Not a number: "+counter);
//             
//                continue;
//                
//            }
//            
//            averages.put(counter, average);
//            
//        }
//
//        return averages;
//        
//    }

    /**
     * Return the ring buffer of samples for the named counter.
     * 
     * @param name
     *            The counter name.
     * 
     * @return The samples. The samples are loosly typed, but in general all
     *         samples of the same counter SHOULD have the same data type
     *         (Date, Long, Double, etc).
     */
    public Object[] getSamples(String name) {
        
        return counters.get(name);
        
    }
    
    /**
     * Computes the average of the samples for some counter.
     * 
     * @param samples
     *            The samples.
     * 
     * @return The average -or- <code>null</code> if the samples are not
     *         numbers (no average is reported for dates, strings, etc).
     */
    public String getAverage(Object[] samples) {
        
        if (samples == null) {

            // No samples yet.
            
            return "N/A";
            
        }
        
        double total = 0.0;

        int n = 0;
        
        for(int i=0; i<samples.length; i++) {
            
            Object sample = samples[i];
            
            if(sample==null) continue;
            
            if(sample instanceof Long) {

                total += (Long)sample;
                
            } else if(sample instanceof Double) {

                total += (Double) sample;
                
            } else {
                
                // Not a number.
                return null;
                
            }
            
            n++;
            
        }
        
        return ""+(Double)(total/n);
        
    }
    
    /**
     * A text report on the last 60 seconds. 
     */
    public String getStatistics() {
        
//        return getAverages().toString();
        
        StringBuilder sb = new StringBuilder();
        
        Iterator<ICounter> itr = countersRoot.getCounters(null/*filter*/);
        
        while(itr.hasNext()) {
            
            ICounter c = itr.next();
            
            sb.append("\n"+c.getPath()+"="+c.getValue());
            
        }
        
        return sb.toString();
        
    }

    public static class StatisticsCollectorForLinux extends
            AbstractStatisticsCollector {

        @Override
        protected void start() {
            // TODO Auto-generated method stub

        }

        @Override
        protected void stop() {
            // TODO Auto-generated method stub

        }

    }

    /**
     * Collects counters on a Windows platoform using <code>typeperf</code>
     * and aligns them with those declared by {@link IRequiredHostCounters}.
     * <p>
     * Note: The names of counters under Windows are NOT case-sensitive.
     * 
     * @todo configuration for properties to be collected.
     * 
     * @todo make sure that [typeperf] is getting killed properly during stop()
     *       or if the application quits.
     * 
     * @see http://technet2.microsoft.com/windowsserver/en/library/45b6fbfc-9fcd-4e58-b070-1ace9ca93f2e1033.mspx?mfr=true,
     *      for a description of the <code>typeperf</code> command.
     * 
     * @see http://www.microsoft.com.nsatc.net/technet/archive/winntas/maintain/monitor/perform.mspx?mfr=true,
     *      for a list of counters and their descriptions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class StatisticsCollectorForWindows extends
            AbstractStatisticsCollector {

        private Process process = null;

        private InputStream is = null;

        /**
         * Used to read {@link #is} and aggregate the performance data reported
         * by the {@link #process}.
         */
        private final ExecutorService readService = Executors
                .newSingleThreadScheduledExecutor(DaemonThreadFactory
                        .defaultThreadFactory());

        /**
         * A declaration of a counter available on a Windows platform. Each
         * declaration knows both the name of the counter under Windows and how
         * to report that counter under the appropriate name for our
         * {@link ICounterSet}s.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class WindowsCounterDecl implements IInstrument {
            
            private final String counterNameForWindows;
            private final String path;
            
            /**
             * The name of the source counter on a Windows platform.
             */
            public String getCounterNameForWindows() {

                return counterNameForWindows;
                
            }
            
            /**
             * The path for the {@link ICounter}.
             * 
             * @see IRequiredHostCounters
             */
            public String getPath() {
                
                return path;
                
            }
            
            public WindowsCounterDecl(String counterNameForWindows, String path) {
                
                if (counterNameForWindows== null)
                    throw new IllegalArgumentException();

                if (path == null)
                    throw new IllegalArgumentException();
                
                this.counterNameForWindows = counterNameForWindows;

                this.path = path;
                
            }

            /**
             * Return the average of the samples for the counter with that name.
             * <p>
             * Note: We rename the headers when the {@link CSVReader} starts
             * such that it reports the Windows counters under the names
             * declared by {@link IRequiredHostCounters} and friends (prefixed
             * with the
             * {@link AbstractStatisticsCollector#fullyQualifiedHostName}).
             * This is exactly how the counters are declared in
             * {@link AbstractStatisticsCollector#countersRoot}. This means we
             * can pass {@link AbstractStatisticsCollector#getSamples(String)}
             * the name of a counter defined by {@link IRequiredHostCounters}
             * and it will return the samples for that counter (assuming that
             * they are being collected).
             */
            public String getValue() {

                // path for the counter.
                final String name = hostPathPrefix + getPath();
                
                Object[] samples = getSamples( name );
                
                return getAverage( samples );

            }
            
        }
                
        final String physicalDisk = IRequiredHostCounters.PhysicalDisk;
        
        /**
         * Declare the Windows counters to be collected.
         */
        // String prefix = "\\Process(javaw#"+getPID()+")\\";
        final List<WindowsCounterDecl> names = Arrays
                .asList(new WindowsCounterDecl[] {
                        
                new WindowsCounterDecl("\\Memory\\Pages/Sec",
                                IRequiredHostCounters.Memory_PageFaultsPerSecond),

                new WindowsCounterDecl(
                                "\\Processor(_Total)\\% Processor Time",
                                IRequiredHostCounters.Processor_PercentProcessorTime),

                new WindowsCounterDecl(
                                "\\LogicalDisk(_Total)\\% Free Space",
                                IRequiredHostCounters.LogicalDisk_PercentFreeSpace),

                /*
                 * These are system wide counters for the network interface.
                 * There are also counters for the network queue length,
                 * packets discarded, and packet errors that might be
                 * interesting. (I can't find _Total versions declared for
                 * these counters so I am not including them but the
                 * counters for the specific interfaces could be enabled and
                 * then aggregated, eg:
                 * 
                 * \NetworkInterface(*)\Bytes Send/Sec
                 */
                // "\\Network Interface(_Total)\\Bytes Received/Sec",
                // "\\Network Interface(_Total)\\Bytes Sent/Sec",
                // "\\Network Interface(_Total)\\Bytes Total/Sec",

                /*
                 * System wide counters for DISK IO.
                 */
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\Avg. Disk Queue Length",
                        physicalDisk+ICounterSet.pathSeparator+
                        "Avg. Disk Queue Length"),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\% Idle Time",
                        physicalDisk+ICounterSet.pathSeparator+ "% Idle Time"),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\% Disk Time",
                        physicalDisk+ICounterSet.pathSeparator+ "% Disk Time"),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\% Disk Read Time",
                        physicalDisk+ICounterSet.pathSeparator+ "% Disk Read Time"),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\% Disk Write Time",
                        physicalDisk+ICounterSet.pathSeparator+ "% Disk Write Time"),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\Disk Read Bytes/Sec",
                        IRequiredHostCounters.PhysicalDisk_BytesReadPerSec),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\Disk Write Bytes/Sec",
                        IRequiredHostCounters.PhysicalDisk_BytesWrittenPerSec),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\Disk Reads/Sec",
                        IHostCounters.PhysicalDisk_ReadsPerSec),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\Disk Writes/Sec",
                        IHostCounters.PhysicalDisk_WritesPerSec),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\Avg. Disk Bytes/Read",
                        physicalDisk+ICounterSet.pathSeparator+
                        "Avg. Disk Bytes per Read"),
                new WindowsCounterDecl(
                        "\\PhysicalDisk(_Total)\\Avg. Disk Bytes/Write",
                        physicalDisk+ICounterSet.pathSeparator+
                        "Avg. Disk Bytes per Write"),
//"\\PhysicalDisk(_Total)\\Disk Writes/sec",
//"\\PhysicalDisk(_Total)\\Avg. Disk Bytes/Read",
//"\\PhysicalDisk(_Total)\\Avg. Disk Bytes/Write",

        // /*
        // * The names of the process specific counters that
        // * we want.
        // *
        // * Note: The IO counters here are aggregated across
        // * disk and network usage by the process.
        // */
        // prefix+"% Processor Time",
        // prefix+"IO Read Bytes/sec",
        // prefix+"IO Read Operations/sec",
        // prefix+"IO Write Bytes/sec",
        // prefix+"IO Write Operations/sec",


        });

        /**
         * Note: While this approach seems fine to obtain the system wide
         * counters under Windows it runs into problems when you attempt to
         * obtain the process specific counters. The difficulty is that you can
         * not use the PID when you request counters for a process and it
         * appears that the counters for all processes having the same _name_
         * are aggregated, or at least can not be distinguished, using
         * [typeperf]. I am trying to approach that problem by reporting
         * "counters" for the data service (really, for the CPU, RAM, and DISK
         * resources that it is using).
         * 
         * @throws IOException
         */
        public void start() {

            /*
             * The runtime shutdown hook appears to be a robust way to handle ^C by
             * providing a clean service termination.
             */
            {
             
                final AbstractStatisticsCollector f = this;
                
                Runtime.getRuntime().addShutdownHook(new Thread() {
                
                    public void run() {
                        
                        f.stop();
                        
                    }
                    
                });
            }

            // log the command that we will execute.
            final List<String> command = new LinkedList<String>();
            {

                /*
                 * Command to write counters on the console.
                 * 
                 * The sample interval format is -si [hh:[mm:]]ss.
                 * 
                 * Note: The code assumes a one second sample interval.
                 * 
                 * Note: typeperf supports csv, tab, and bin output formats.
                 * However specifying tsv (tab delimited) causes it to always
                 * write on a file so I am using csv (comma delimited, which is
                 * the default in any case).
                 */

                command.add("typeperf");
                
                command.add("-si");
                command.add("1");

                for (WindowsCounterDecl decl : names) {

                    // the counter names need to be double quoted.
                    command.add("\""+decl.getCounterNameForWindows()+"\"");

                    // add into our counter hierarchy.
                    countersRoot.addCounterByPath(hostPathPrefix
                            + decl.getPath(), decl);

                }
                
                {
                
                    StringBuilder sb = new StringBuilder();

                    for (String s : command) {

                        sb.append( s +" ");

                    }

                    log.info("command:\n" + sb);

                }

            }

            try {

                ProcessBuilder tmp = new ProcessBuilder(command);

                process = tmp.start();

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

            is = process.getInputStream();

            readService.submit(new ProcessReader());

        }

        @Override
        protected void stop() {

            // interrupt the
            readService.shutdownNow();

            if (process != null) {

                process.destroy();

                process = null;

                is = null;

            }

        }

        private class ProcessReader implements Runnable {

            private final SimpleDateFormat f;

            ProcessReader() {

                f = new SimpleDateFormat("MM/dd/yyyy kk:mm:ss.SSS");

                // System.err.println("Format: "+f.format(new Date()));
                //            
                // try {
                // System.err.println("Parsed: "+f.parse("03/12/2008
                // 14:14:46.902"));
                // } catch (ParseException e) {
                // log.error("Could not parse?");
                // }

            }

            public void run() {

                try {

                    read();

                } catch (Exception e) {

                    log.fatal(e.getMessage(), e);

                }

                log.info("Terminated");

            }

            private void read() throws Exception {

                long nsamples = 0;

                LineNumberReader reader = new LineNumberReader(
                        new InputStreamReader(is));

                CSVReader csvReader = new CSVReader(reader);

                /*
                 * Note: I am using the same value as the sample interval, but
                 * it may not matter much.
                 */
                csvReader.setTailDelayMillis(1000/* ms */);

                // read headers from the file.
                csvReader.readHeaders();

                /*
                 * replace the first header definition so that we get clean
                 * timestamps.
                 */
                csvReader.setHeader(0, new Header("Timestamp") {
                    public Object parseValue(String text) {

                        try {
                            return f.parse(text);
                        } catch (ParseException e) {
                            log.error("Could not parse: " + text, e);
                            return text;
                        }

                    }
                });

                /*
                 * replace other headers so that data are named by our counter
                 * names.
                 */
                {
                    int i = 1;

                    for (WindowsCounterDecl decl : names) {

                        String path = hostPathPrefix + decl.getPath();
                        
                        log.info("setHeader[i="+i+"]="+path);
                        
                        csvReader.setHeader(i++, new Header(path));

                    }
                    
                }
                
                // setup the samples.
                {

                    for (Header h : csvReader.getHeaders()) {
                        
                        counters.put(h.getName(), new Object[60]);

                    }

                }

                while (process != null && is != null
                        && !Thread.currentThread().isInterrupted()
                        && csvReader.hasNext()) {

                    Map<String, Object> row = csvReader.next();

                    for (Map.Entry<String, Object> entry : row.entrySet()) {

                        log.debug(entry.getKey() + "=" + entry.getValue());

                        Object[] samples = getSamples(entry.getKey());

                        assert samples != null;

                        samples[(int) (nsamples % 60)] = entry.getValue();

                    }

                    nsamples++;

                    log.info(getStatistics());

                }

            }

        }

        /**
         * Runs the {@link StatisticsCollectorForWindows}.
         * 
         * @param args
         * 
         * @throws InterruptedException
         */
        public static void main(String[] args) throws InterruptedException {

            AbstractStatisticsCollector client = new StatisticsCollectorForWindows();

            client.start();

            Thread.sleep(2 * 1000/*ms*/);
            
//            {
//                
//                Iterator<ICounter> itr = client.countersRoot
//                        .getCounters(null/* filter */);
//
//                while (itr.hasNext()) {
//
//                    ICounter c = itr.next();
//
//                    String path = c.getPath();
//                    
//                    String value = ""+c.getValue();
//                    
//                    System.err.println(path + "=" + value);
//
//                }
//
//            }

            Thread.sleep(60 * 1000/*ms*/);

            client.stop();

            log.info(client.getStatistics());

        }

    }

}
