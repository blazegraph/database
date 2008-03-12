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
 * Created on Mar 2, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.util.CSVReader;
import com.bigdata.util.CSVReader.Header;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.ibm.icu.text.SimpleDateFormat;

/**
 * Interface for collecting, reporting, and decision-making based on node and
 * service utilization statistics.
 * 
 * @todo begin simply, with what we can and need to collect in order to identify
 *       an underutilized host (iostat + vmstat for memory) and service
 *       (pidstat) or just use sar.
 * 
 * @todo load monitoring Under windows:
 *       <p>
 *       http://www.javaworld.com/javaworld/jw-11-2004/jw-1108-windowspm.html
 *       <p>
 *       <p>
 *       http://nsclient.ready2run.nl/ (GPL, installed as a Windows service,
 *       used by Nagios).
 *       <p>
 *       https://nsclient4j.dev.java.net/ (LGPL java interface to nsclient
 *       providing access to that service, so you need both this and nsclient to
 *       get windows performance counters into Java.)
 *       <p>
 *       http://www.nagios.org/faqs/viewfaq.php?faq_id=32
 *       <p>
 *       http://snmpboy.msft.net/
 *       <p>
 *       http://www.google.com/search?hl=en&q=windows+performance+monitor+java
 *       <p>
 *       http://java.sun.com/performance/jvmstat/#Download
 *       <p>
 *       typeperf can be run as a command line utility to report on counters.
 *       <p>
 *       http://www.scl.ameslab.gov/Projects/Rabbit/
 * 
 * @todo SNMP
 *       <p>
 *       http://en.wikipedia.org/wiki/Simple_Network_Management_Protocol
 *       <p>
 *       http://www.snmp4j.org/
 *       <p>
 *       http://sourceforge.net/projects/joesnmp/ (used by jboss)
 *       <p>
 *       http://net-snmp.sourceforge.net/ (Un*x and Windows SNMP support).
 * 
 * @todo {@link SystemUtil}
 * 
 * @todo {@link Runtime#availableProcessors()}, {@link Runtime#maxMemory()}, etc.
 * 
 * @todo performance testing links: Grinder, etc.
 *       <p>
 *       http://www.opensourcetesting.org/performance.php
 * 
 * iostat
 * 
 * <pre>
 *      Linux 2.6.18-1.2798.fc6 (dp-aether1.dpp2.org)   03/03/2008
 *      
 *      avg-cpu:  %user   %nice %system %iowait  %steal   %idle
 *                 5.01    0.00    1.88    0.61    0.00   92.50
 *      
 *      Device:            tps   Blk_read/s   Blk_wrtn/s   Blk_read   Blk_wrtn
 *      sda              18.89       209.75       135.41  995990159  642992550
 *      sdb               0.00         0.00         0.00       1272          0
 *      dm-0             32.68       209.71       135.37  995771418  642807736
 *      dm-1              0.01         0.05         0.04     215048     184776
 * </pre>
 * 
 * vmstat
 * 
 * <pre>
 *      [root@dp-aether1 src]# vmstat
 *      procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu------
 *       r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 *       0  0  19088 1099996 210388 2130812    0    0    26    17    0    0  5  2 92  1  0
 * </pre>
 * 
 * pidstat -p 17324 (-d gives IO statistics with kernels 2.6.20 and up)
 * 
 * <pre>
 *      Linux 2.6.18-1.2798.fc6 (dp-aether1.dpp2.org)   03/03/2008
 *      
 *      09:00:13 AM       PID   %user %system    %CPU   CPU  Command
 *      09:00:13 AM     17324    0.01    0.00    0.02     2  java
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStatisticsService {

    /**
     * Register a service on a node.
     * 
     * @param nodeDesc
     * @param serviceUUID
     * @param serviceClass
     * 
     * @todo not necessary - the {@link IStatisticsService} should discover
     *       services and then collect statistics from those services.
     */
    public void register(NodeDescription nodeDesc, UUID serviceUUID, Class serviceClass);

    /**
     * Notify this service that you are alive.
     */
    public void heartbeat(NodeStats nodeStats, ServiceStats serviceStats);

    /**
     * Return the identifier of an under utilized service.
     * 
     * @todo pass the service type, get the service identifier.
     */
    public UUID[] getUnderUtilizedService(Class iface, int max) throws IOException;    

    public class NodeDescription {

        /** UUID assigned by the reporting service to the node. */
        public UUID nodeUUID;
        
        /*
         * CPU Info.
         */
        public int m_processors;
        public String m_cpuInfo;
        public String m_architecture;
        public String m_osName;
        public String m_osVersion;

        /*
         * Memory info.
         */
        public long memAvail;

        // bytes per disk, spindle speed, cache, etc.
        public long[] diskInfo;

        // @todo plus network E10/E100/E1000, full or half-duplex etc.
        public InetAddress[] inetAddr;

        public String hostname;
        public String description;
        
    }
    
    /**
     * Abstract base class for per-node and per-service statistics.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class Stats {
        
        /** The service identifier. */
        public UUID nodeUUID;
        /** Reporting interval (millis). */
        public long elapsed;
        /** Local time on the host. */
        public long localTime;
        /** % CPU utilization per CPU. */
        public int[] cpu;
        /** memory used and available (bytes). */
        public long memUsed, memAvail;
        /** % network IO (bytes read/written per interface). */
        public long[] nio;
        /** % disk IO (bytes read/written per interface, requests per interface, disk-queue length). */
        public int[] dio;
        /** Total disk space and free disk space (bytes per disk). */
        public long[] diskSpace, diskFree;

    }
    
    public class NodeStats extends Stats {

    }
    
    public class ServiceStats extends Stats {

        /** The service identifier. */
        public UUID serviceUUID;

        // interface or classname for service
        public Class serviceClass;
        
    }

    /**
     * Client gathers statistics and periodically reports them to the
     * {@link IStatisticsService}.
     * 
     * <pre>
     *   1. Start-up with local service.
     *   
     *   2. Discover IStatisticsService.
     *   
     *   2. Notify service of start of life cycle, sending {@link SystemUtil} data.
     *   
     *   2. Get pid.  Obtain per-process counters for cpu, mem, dio, nio.
     *   
     *   2. start thread, reporting counters every N milliseconds to service
     *   
     *   3. shutdown with local service, optionally notifying statistics service of end of life cycle.
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract public static class AbstractStatisticsClient {

        static protected final Logger log = Logger.getLogger(AbstractStatisticsClient.class);

        /**
         * Map containing 60 seconds of data for each counter.
         */
        protected Map<String,Object[]> counters = new HashMap<String,Object[]>();
        
        /**
         * Return the load average for the last minute if available and -1 otherwise.
         * <p>
         * Note: The load average is available on 1.6+ JVMs.
         * 
         * @see OperatingSystemMXBean
         */
        public double getSystemLoadAverage()
        {
            
//            double version = Double.parseDouble(System.getProperty("java.vm.version"));
//          if(version>=1.6) {
            
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
    
        public void shutdown() {
            
        }
        
        public void shutdownNow() {
            
        }

        /**
         * Start collecting process performance data.
         */
        abstract protected void start();

        /**
         * Stop collecting process performance data.
         */
        abstract protected void stop();

        /**
         * Report on the last 60 seconds.
         */
        public Map<String,Double> getAverages() {
            
            Map<String,Double> averages = new TreeMap<String,Double>();
            
            for(Map.Entry<String,Object[]> entry : counters.entrySet() ) {
                
                String counter = entry.getKey();
                
                Object[] samples = entry.getValue();

                Double average = getAverage(samples);

                if(average == null) {
                    
                    log.debug("Not a number: "+counter);
                 
                    continue;
                    
                }
                
                averages.put(counter, average);
                
            }

            return averages;
            
        }

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
        public Double getAverage(Object[] samples) {
            
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
            
            return (Double)(total/n);
            
        }
        
        /**
         * A text report on the last 60 seconds. 
         */
        public String getStatistics() {
            
            return getAverages().toString();
            
        }
        
    }
    
    public static class StatisticsClientForLinux extends AbstractStatisticsClient {

        @Override
        protected void start() {
            // TODO Auto-generated method stub
            
        }

        @Override
        protected void stop() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public Map<String,Double> getAverages() {
            // TODO Auto-generated method stub
            return null;
        }
     
    }
    
    /**
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
    public static class StatisticsClientForWindows extends AbstractStatisticsClient {
     
        private Process process = null;
        private InputStream is = null;

        /**
         * Used to read {@link #is} and aggregate the performance data reported by
         * the {@link #process}.
         */
        private final ExecutorService readService = Executors
                .newSingleThreadScheduledExecutor(DaemonThreadFactory
                        .defaultThreadFactory());
        
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
            
//            String prefix = "\\Process(javaw#"+getPID()+")\\";
            
            String[] command =                     new String[]{
                    /*
                     * Command to write counters on the console
                     */
                    "typeperf",
                    /*
                     * Sample interval.  Format is [mm:]ss.
                     */
                    "-si","1", // one second.
                    /*
                     * Output format (tsv is tab delimited, which is the
                     * easier to parse correctly).  Consider 
                     * http://sourceforge.net/projects/javacsv/ for CSV
                     * parsing (LGPL).
                     * 
                     * Note: It appears to want to write on a file if
                     * you specify anything other than CSV.
                     */
//                    "-f","tsv",
                    
                    /*
                     * The names of the system wide counters that we
                     * want.
                     * 
                     * Note: These three counters provide a good
                     * overview of the resource demand for a Windows
                     * host. It is bad when pages/sec is high, and
                     * generally indicates good utilization when the
                     * average disk queue length and % processor time
                     * are high.
                     */
                    "\"\\Memory\\Pages/Sec\"",
                    "\"\\PhysicalDisk(_Total)\\Avg. Disk Queue Length\"",
                    "\"\\Processor(_Total)\\% Processor Time\"",
                    "\"\\LogicalDisk(_Total)\\% Free Space\"",
//                    /*
//                     * These are system wide counters for the network
//                     * interface. There are also counters for the
//                     * network queue length, packets discarded, and
//                     * packet errors that might be interesting.
//                     */
//                    "\\Network Interface(_Total)\\Bytes Received/Sec",
//                    "\\Network Interface(_Total)\\Bytes Sent/Sec",
//                    "\\Network Interface(_Total)\\Bytes Total/Sec",
                    
                    /*
                     * We also request system wide counters that correspond to
                     * the per-process IO counters so that we can figure out the
                     * percentage of the IO that is due to the process that we
                     * are monitoring.
                     */
                    "\\PhysicalDisk(_Total)\\Disk Read Bytes/sec",
                    "\\PhysicalDisk(_Total)\\Disk Write Bytes/sec",
                    "\\PhysicalDisk(_Total)\\Disk Reads/sec",
                    "\\PhysicalDisk(_Total)\\Disk Writes/sec",

//                    /*
//                     * The names of the process specific counters that
//                     * we want.
//                     * 
//                     * Note: The IO counters here are aggregated across
//                     * disk and network usage by the process.
//                     */
//                    prefix+"% Processor Time",
//                    prefix+"IO Read Bytes/sec",
//                    prefix+"IO Read Operations/sec",
//                    prefix+"IO Write Bytes/sec",
//                    prefix+"IO Write Operations/sec",
                    
            };  

            // log the command that we will execute.
            {
                
                StringBuilder sb = new StringBuilder();
                
                for(String s : command) {
                    
                    sb.append(s+" ");
                    
                }
                
                log.info("command:\n"+sb);
                
            }
            
            try {

                ProcessBuilder tmp = new ProcessBuilder( command );
                
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
                
//                System.err.println("Format: "+f.format(new Date()));
//                
//                try {
//                    System.err.println("Parsed: "+f.parse("03/12/2008 14:14:46.902"));
//                } catch (ParseException e) {
//                    log.error("Could not parse?");
//                }
                
            }
            
            public void run() {

                try {
                    
                    read();
                    
                } catch (Exception e) {

                    log.fatal(e.getMessage(),e);
                    
                }

                log.info("Terminated");
                
            }
            
            private void read() throws Exception {
                
                long nsamples = 0;
                
                LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));
                
                CSVReader csvReader = new CSVReader(reader);

                /*
                 * Note: I am using the same value as the sample interval, but
                 * it may not matter much.
                 */
                csvReader.setTailDelayMillis(1000/*ms*/);

                // read headers from the file.
                csvReader.readHeaders();
                
                // replace the first header definition so that we get clean timestamps.
                csvReader.setHeader(0, new Header("Timestamp"){
                    public Object parseValue(String text) {
                     
                        try {
                            return f.parse(text);
                        } catch (ParseException e) {
                            log.error("Could not parse: "+text,e);
                            return text;
                        }
                        
                    }
                });

                // setup the samples.
                {
                    
                    for(Header h : csvReader.getHeaders()) {
                        
                        counters.put(h.getName(), new Object[60]);
                        
                    }
                    
                }
                
                while (process != null && is != null
                        && !Thread.currentThread().isInterrupted() && csvReader.hasNext()) {

                    Map<String,Object> row = csvReader.next();
                    
                    for(Map.Entry<String,Object> entry : row.entrySet() ) {
                        
                        log.debug(entry.getKey()+"="+entry.getValue());
                        
                        Object[] samples = counters.get(entry.getKey());
                        
                        assert samples != null;
                        
                        samples[(int)(nsamples % 60)] = entry.getValue();
                        
                    }

                    nsamples++;

                    log.info(getStatistics());
                    
                }

            }
            
        }

        /**
         * Runs the {@link StatisticsClientForWindows}.
         * 
         * @param args
         * @throws InterruptedException
         */
        public static void main(String[] args) throws InterruptedException {
            
            AbstractStatisticsClient client = new StatisticsClientForWindows();
            
            client.start();
            
            Thread.sleep(60*1000/*ms*/);
            
            client.stop();
            
            log.info(client.getStatistics());
            
        }

    }
    
    /**
     * Service collects statistics (averages of counters) from various hosts and
     * services, can report those statistics, and can identify over and under
     * utilized hosts and services based on the collected statistics.
     * 
     * @todo should i report the source counter names (they are 60 second
     *       average)? With aliases?  Or just map the actual source counters
     *       onto a set of required reporting metrics?
     */
    public static class StatisticsService implements IStatisticsService {

        public void register(NodeDescription nodeDesc, UUID serviceUUID, Class serviceClass) {
            
        }
        
        public void heartbeat(NodeStats nodeStats, ServiceStats serviceStats) {
            // TODO Auto-generated method stub
            
        }
        
        public UUID[] getUnderUtilizedService(Class iface, int max) throws IOException {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
}
