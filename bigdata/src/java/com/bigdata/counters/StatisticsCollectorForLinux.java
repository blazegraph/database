package com.bigdata.counters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bigdata.rawstore.Bytes;

/**
 * Collection of host performance data using the <code>sysstat</code> suite.
 * 
 * @see http://pagesperso-orange.fr/sebastien.godard/
 * 
 * FIXME verify scaling for all collected counters.
 * 
 * @todo configuration parameters to locate the sysstat utilities (normally installed
 * into /usr/bin).
 * 
 * @todo do a vmstat or sar collector to gain more information about the overall
 *       host performance
 * 
 * <pre>
 *  vmstat
 *  procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu------
 *   r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 *   0  0  19088 1099996 210388 2130812    0    0    26    17    0    0  5  2 92  1  0
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatisticsCollectorForLinux extends
        AbstractStatisticsCollector {

    /**
     * Used to parse the timestamp associated with each row of the
     * [pidstat] output.
     */
    static protected final SimpleDateFormat f;
    static {

        f = new SimpleDateFormat("hh:mm:ss aa");

        /*
         * Code may be enabled for a runtime test of the date format.
         */
        if (true) {

            System.err.println("Format: " + f.format(new Date()));

            try {

                System.err.println("Parsed: " + f.parse("06:35:15 AM"));

            } catch (ParseException e) {

                log.error("Could not parse?");

            }

        }
        
    }
    
    /**
     * The process identifier for this process (the JVM).
     */
    static protected int pid;
    static {

        pid = getLinuxPIDWithBash();

    }
    
    /**
     * The Linux {@link KernelVersion}.
     */
    static protected StatisticsCollectorForLinux.KernelVersion kernelVersion;
    static {

        kernelVersion = KernelVersion.get();

    }

    /** reports on the host CPU utilization. */
    StatisticsCollectorForLinux.SarCpuUtilizationCollector sar1;

    /** reports on process performance counters (CPU, MEM, IO). */
    StatisticsCollectorForLinux.PIDStatCollector pidstat;

    public void start() {

        log.info("starting collectors");
        
        super.start();
        
        sar1.start();

        pidstat.start();

    }

    public void stop() {

        log.info("stopping collectors");
        
        super.stop();
        
        sar1.stop();

        pidstat.stop();

    }
    
    /**
     * Reports on the kernel version for a linux host.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * 
     * @todo there is more information lurking in the kernel version.
     * 
     * @see http://en.wikipedia.org/wiki/Linux_kernel for a summary of linux
     *      kernel versioning.
     */
    public static class KernelVersion {
        
        public final int version;
        public final int major;
        public final int minor;
        
        public KernelVersion(String val) {
            
            String[] x = val.split("[\\.]");
            
            assert x.length >= 2 : "Not a kernel version? "+val;
            
            version = Integer.parseInt(x[0]);
            major   = Integer.parseInt(x[1]);
            minor   = Integer.parseInt(x[2]);
            
        }
        
        /**
         * Return the version of the Linux kernel as reported by
         * <code>uname</code>
         * 
         * @return The {@link KernelVersion}
         * 
         * @throws RuntimeException
         *             if anything goes wrong.
         */
        static public StatisticsCollectorForLinux.KernelVersion get() {

            final List<String> commands = new LinkedList<String>();

            final Process pr;
            
            try {
            
                commands.add("/bin/uname");
                
                commands.add("-r");
                
                ProcessBuilder pb = new ProcessBuilder(commands);
                
                pr = pb.start();
                
                pr.waitFor();
            
            } catch (Exception ex) {
            
                throw new RuntimeException("Problem running command: ["
                        + commands + "]", ex);
                
            }
            
            if (pr.exitValue() == 0) {
            
                BufferedReader outReader = new BufferedReader(
                        new InputStreamReader(pr.getInputStream()));
                
                final String val;
                try {
                
                    val = outReader.readLine().trim();
                    
                } catch (IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
                log.info("read: [" + val + "]");

                return new KernelVersion(val);
                
            } else {
                
                throw new RuntimeException("Could not get PID: exitValue="
                        + pr.exitValue());
                
            }

        }

    }

    /**
     * Return the PID of the Java VM under Linux using bash.
     * 
     * @return The PID.
     * 
     * @throws RuntimeException
     *             if anything goes wrong.
     */
    static public int getLinuxPIDWithBash() {

        final List<String> commands = new LinkedList<String>();

        final Process pr;
        
        try {
        
            commands.add("/bin/bash");
            
            commands.add("-c");
            
            commands.add("echo $PPID");
            
            ProcessBuilder pb = new ProcessBuilder(commands);
            
            pr = pb.start();
            
            pr.waitFor();
        
        } catch (Exception ex) {
        
            throw new RuntimeException("Problem running command: ["
                    + commands + "]", ex);
            
        }
        
        if (pr.exitValue() == 0) {
        
            BufferedReader outReader = new BufferedReader(
                    new InputStreamReader(pr.getInputStream()));
            
            final String val;
            try {
            
                val = outReader.readLine().trim();
                
            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            log.info("read: [" + val + "]");
            
            int pid = Integer.parseInt(val);
            
            return pid;
            
        } else {
            
            throw new RuntimeException("Could not get PID: exitValue="
                    + pr.exitValue());
            
        }

    }

    public StatisticsCollectorForLinux(int interval) {

        super(interval);

        // host wide collection
        sar1 = new SarCpuUtilizationCollector(interval,kernelVersion);

        // process specific collection.
        pidstat = new PIDStatCollector(pid, interval, kernelVersion);

    }

    /**
     * <code>sar -u</code>
     * 
     * <pre>
     *            Linux 2.6.18-1.2798.fc6 (hostname)   03/03/2008
     *            
     *            avg-cpu:  %user   %nice %system %iowait  %steal   %idle
     *                       5.01    0.00    1.88    0.61    0.00   92.50
     *            
     *            Device:            tps   Blk_read/s   Blk_wrtn/s   Blk_read   Blk_wrtn
     *            sda              18.89       209.75       135.41  995990159  642992550
     *            sdb               0.00         0.00         0.00       1272          0
     *            dm-0             32.68       209.71       135.37  995771418  642807736
     *            dm-1              0.01         0.05         0.04     215048     184776
     * </pre>
     * 
     * @todo <code>sar -b</code> to get the iowait data.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class SarCpuUtilizationCollector extends AbstractProcessCollector {

        /**
         * Inner class integrating the current values with the {@link Counter}
         * hierarchy.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class I<T> implements IInstrument<T> {
            
            private final String path;
            
            public String getPath() {
                
                return path;
                
            }
            
            public I(String path) {
                
                assert path != null;
                
                this.path = path;
                
            }
            
            public T getValue() {
             
                return (T) vals.get(path);
                
            }

            public long lastModified() {

                return lastModified;
                
            }

            /**
             * @throws UnsupportedOperationException
             *             always.
             */
            public void setValue(T value, long timestamp) {
               
                throw new UnsupportedOperationException();
                
            }

        }

        /**
         * Map containing the current values for the configured counters. The
         * keys are paths into the {@link CounterSet}. The values are the data
         * most recently read from <code>sar</code>.
         */
        private Map<String,Object> vals = new HashMap<String, Object>();
        
        /**
         * The timestamp associated with the most recently collected values.
         */
        private long lastModified = System.currentTimeMillis();
        
        public SarCpuUtilizationCollector(int interval,
                KernelVersion kernelVersion) {

            super(interval);

        }
        
        public List<String> getCommand() {

            List<String> command = new LinkedList<String>();
            
            command.add("/usr/bin/sar");

            command.add("-u"); // CPU stats
            
            command.add(""+getInterval());
            
            return command;
            
        }

        /**
         * Extended to declare the counters that we will collect using
         * <code>sar</code>.
         */
        public CounterSet getCounters() {
            
            CounterSet root = super.getCounters();
            
            if(inst == null) {
            
                inst = new LinkedList<I>();
                
                /*
                 * Note: Counters are all declared as Double to facilitate
                 * aggregation.
                 */

                inst.add(new I<Double>(IRequiredHostCounters.CPU_PercentProcessorTime));
                
                inst.add(new I<Double>(IHostCounters.CPU_PercentUserTime));
                inst.add(new I<Double>(IHostCounters.CPU_PercentSystemTime));
                inst.add(new I<Double>(IHostCounters.CPU_PercentIOWait));
                
            }
            
            for(Iterator<I> itr = inst.iterator(); itr.hasNext(); ) {
                
                I i = itr.next();
                
                root.addCounter(i.getPath(), i);
                
            }
            
            return root;
            
        }
        private List<I> inst = null;

        /**
         * Extended to force <code>sar</code> to use a consistent timestamp
         * format regardless of locale by setting
         * <code>S_TIME_FORMAT="ISO"</code> in the environment.
         */
        protected void setEnvironment(Map<String, String> env) {

            super.setEnvironment(env);
            
            env.put("S_TIME_FORMAT", "ISO");
            
        }

        public AbstractProcessReader getProcessReader() {
            
            return new SarReader();
            
        }

        /**
         * Sample output for <code>sar -u 1 10</code>
         * <pre>
         *    Linux 2.6.22.14-72.fc6 (hostname)    2008-03-17
         *   
         *   04:14:45 PM     CPU     %user     %nice   %system   %iowait    %steal     %idle
         *   04:14:46 PM     all      0.00      0.00      0.00      0.00      0.00    100.00
         *   ...
         *   Average:        all      0.00      0.00      0.00      0.00      0.00    100.00
         * </pre>
         * 
         * There is a banner, which is followed by the a repeating sequence
         * {blank line, header, data line(s)}.  This sequence repeats every
         * so often when sar emits new headers.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        private class SarReader extends ProcessReaderHelper {
            
            protected ActiveProcess getActiveProcess() {
                
                if (activeProcess == null)
                    throw new IllegalStateException();
                
                return activeProcess;
                
            }

            public SarReader() {

                super();
                
            }

            @Override
            protected void readProcess() throws Exception {
                
                // The most recently read header.
                String header;
                
                // skip banner.
                final String banner = readLine();
                
                if(INFO) log.info("banner: " + banner);

                {
                
                    // skip blank line.
                    final String blank = readLine();
                    assert blank.trim().length() == 0 : "Expecting a blank line";

                    // header.
                    header = readLine();
                    
                    if (INFO)
                        log.info("header: "+header);

                }
                
                while(true) {
                    
                    // data.
                    final String data = readLine();
                    
                    if(data.trim().length()==0) {
                        
                        header = readLine();

                        if (INFO)
                            log.info("header: "+header);

                        continue;
                        
                    }
                    
//                    *   04:14:45 PM     CPU     %user     %nice   %system   %iowait    %steal     %idle
//                    *   04:14:46 PM     all      0.00      0.00      0.00      0.00      0.00    100.00
 
                    final String s = data.substring(0, 11);
                    try {
                        lastModified = f.parse(s).getTime();
                    } catch (ParseException e) {
                        log.warn("Could not parse time: [" + s + "] : " + e);
                        lastModified = System.currentTimeMillis(); // should be pretty close.
                    }
                                    
                    final String user = data.substring(20-1, 30-1);
//                    final String nice = data.substring(30-1, 40-1);
                    final String system = data.substring(40-1, 50-1);
                    final String iowait = data.substring(50-1, 60-1);
//                    final String steal = data.substring(60-1, 70-1);
                    final String idle = data.substring(70-1, 80-1);

                    if (INFO)
                        log.info("\n%user=" + user + ", %system=" + system
                                + ", iowait=" + iowait + ", idle="+idle+ "\n" + header + "\n"
                                + data);

                    vals.put(IHostCounters.CPU_PercentUserTime, Double.parseDouble(user));
                    
                    vals.put(IHostCounters.CPU_PercentSystemTime, Double.parseDouble(system));

                    vals.put(IHostCounters.CPU_PercentIOWait, Double.parseDouble(iowait));

                    vals.put(IRequiredHostCounters.CPU_PercentProcessorTime, 
                            (100d - Double.parseDouble(idle)));
                    
                }
                
            }

        }
        
    }
    
    /**
     * <code>pidstat -p 501 -u -I -r -d -w</code> [[<i>interval</i> [<i>count</i>]]
     * <p>
     * -p is the pid to montitor -u is cpi (-I normalizes to 100% for SMP),
     * -r is memory stats, -d gives IO statistics with kernels 2.6.20 and
     * up; -w is context switching data; The interval is in seconds. The
     * count is optional - when missing or zero will repeat forever if
     * interval was specified.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public static class PIDStatCollector extends AbstractProcessCollector {

        /** process to be monitored. */
        protected final int pid;

        /**
         * set <code>true</code> if per-process IO data collection should be
         * supported based on the {@link KernelVersion}.
         */
        protected final boolean perProcessIOData;

        /**
         * Inner class integrating the current values with the {@link Counter}
         * hierarchy.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class I<T> implements IInstrument<T> {
            
            private final String path;
            
            public String getPath() {
                
                return path;
                
            }
            
            public I(String path) {
                
                assert path != null;
                
                this.path = path;
                
            }
            
            public T getValue() {
             
                return (T) vals.get(path);
                
            }

            public long lastModified() {

                return lastModified;
                
            }

            /**
             * @throws UnsupportedOperationException
             *             always.
             */
            public void setValue(T value, long timestamp) {
               
                throw new UnsupportedOperationException();
                
            }

        }

        /**
         * Updated each time a new row of data is read from the process and
         * reported as the last modified time for counters based on that
         * process.
         */
        private long lastModified = System.currentTimeMillis();
        
        /**
         * Map containing the current values for the configured counters. The
         * keys are paths into the {@link CounterSet}. The values are the data
         * most recently read from <code>pidstat</code>.
         */
        private Map<String,Object> vals = new HashMap<String, Object>();

        /**
         * 
         * @param pid
         *            Process to be monitored.
         * @param interval
         *            Reporting interval in seconds.
         * @param kernelVersion
         *            The Linux {@link KernelVersion}.
         */
        public PIDStatCollector(int pid, int interval,
                StatisticsCollectorForLinux.KernelVersion kernelVersion) {

            super(interval);

            this.pid = pid;

            perProcessIOData = kernelVersion.version >= 2
                    && kernelVersion.major >= 6 && kernelVersion.minor >= 20;

        }

        @Override
        public List<String> getCommand() {
            
            List<String> command = new LinkedList<String>();
            
            command.add("/usr/bin/pidstat");

            command.add("-p");
            command.add(""+pid);
        
            command.add("-u"); // CPU stats
            command.add("-I"); // normalize to 100% iff SMP?
            
            if(perProcessIOData) {
             
                command.add("-d"); // disk IO report.
                
            }
            
            command.add("-r"); // memory report
            
//              command.add("-w"); // context switching report (not implemented in our code).
            
            command.add(""+getInterval());
            
            return command;
            
        }
        
        /**
         * Extended to declare the counters that we will collect using
         * <code>pidstat</code>.
         */
        public CounterSet getCounters() {
            
            CounterSet root = super.getCounters();
            
            if(inst == null) {
            
                inst = new LinkedList<I>();
                
                /*
                 * Note: Counters are all declared as Double to facilitate
                 * aggregation.
                 */
                
                inst.add(new I<Double>(IRequiredHostCounters.CPU_PercentProcessorTime));
                
                inst.add(new I<Double>(IProcessCounters.CPU_PercentUserTime));
                
                inst.add(new I<Double>(IProcessCounters.CPU_PercentSystemTime));
                
                inst.add(new I<Double>(IProcessCounters.Memory_minorFaultsPerSec));
                inst.add(new I<Double>(IProcessCounters.Memory_majorFaultsPerSec));
                inst.add(new I<Double>(IProcessCounters.Memory_virtualSize));
                inst.add(new I<Double>(IProcessCounters.Memory_residentSetSize));
                inst.add(new I<Double>(IProcessCounters.Memory_percentMemorySize));

                inst.add(new I<Double>(IProcessCounters.PhysicalDisk_BytesReadPerSec));
                inst.add(new I<Double>(IProcessCounters.PhysicalDisk_BytesWrittenPerSec));

            }
            
            for(Iterator<I> itr = inst.iterator(); itr.hasNext(); ) {
                
                I i = itr.next();
                
                root.addCounter(i.getPath(), i);
                
            }
            
            return root;
            
        }
        private List<I> inst = null;
        
        /**
         * Extended to force <code>pidstat</code> to use a consistent
         * timestamp format regardless of locale by setting
         * <code>S_TIME_FORMAT="ISO"</code> in the environment.
         */
        protected void setEnvironment(Map<String, String> env) {

            super.setEnvironment(env);
            
            env.put("S_TIME_FORMAT", "ISO");
            
        }

        @Override
        public AbstractProcessReader getProcessReader() {

            return new PIDStatReader();
            
        }
    
        /**
         * Reads  <code>pidstat</code> output and extracts and updates counter values.
         * <p>
         * Sample <code>pidstat</code> output.
         * 
         * <pre>
         *         Linux 2.6.22.14-72.fc6 (hostname)    03/16/2008
         *         
         *         06:35:15 AM       PID   %user %system    %CPU   CPU  Command
         *         06:35:15 AM       501    0.00    0.01    0.00     1  kjournald
         *         
         *         06:35:15 AM       PID  minflt/s  majflt/s     VSZ    RSS   %MEM  Command
         *         06:35:15 AM       501      0.00      0.00       0      0   0.00  kjournald
         *         
         *         06:35:15 AM       PID   kB_rd/s   kB_wr/s kB_ccwr/s  Command
         *         06:35:15 AM       501      0.00      1.13      0.00  kjournald
         *         
         *         06:35:15 AM       PID   cswch/s nvcswch/s  Command
         *         06:35:15 AM       501      0.00      0.00  kjournald
         * </pre>
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        protected class PIDStatReader extends ProcessReaderHelper {

            protected ActiveProcess getActiveProcess() {
                
                if (activeProcess == null)
                    throw new IllegalStateException();

                return activeProcess;
                
            }

            public PIDStatReader() {
                
                super();

            }
            
            /**
             * The input arrives as an initial banner and a sequence of data
             * reporting events, each of which is three lines. The first
             * line is blank. The second line gives the headers for the
             * event. The third line gives the data for the event. The 2nd
             * and 3rd lines carry a timestamp as the first value.
             * <p>
             * When you request more than one kind of reporting, the three
             * line set simply repeats for each class of events that are to
             * be reported. E.g., cpu and io. This code diagnoses the event
             * types by examining the header line.
             * 
             * @todo since we are examining header lines and since
             *       <code>sysstat</code> is ported to many languages, it
             *       is possible that this will not work when the host is
             *       using an English locale.
             */
            protected void readProcess() throws IOException, InterruptedException {
            
            // skip banner.
            final String banner = readLine();
            
            if(INFO)
                log.info("banner: "+banner);

            // #of "events" read.  each event is three lines.
            long n = 0;
            
            while(true) {
            
                // skip blank line.
                final String blank = readLine();
                
                assert blank.trim().length() == 0 : "Expecting a blank line";

                // header.
                final String header = readLine();

                // data.
                final String data = readLine();
                
                final String s = data.substring(0, 11);
                try {
                    lastModified = f.parse(s).getTime();
                } catch (ParseException e) {
                    log.warn("Could not parse time: [" + s + "] : " + e);
                    lastModified = System.currentTimeMillis(); // should be pretty close.
                }
                                
                if(header.contains("%CPU")) {
                    
                    /*
                     * CPU data for the specified process.
                     */

                    // 06:35:15 AM       PID   %user %system    %CPU   CPU  Command
                    // 06:35:15 AM       501    0.00    0.01    0.00     1  kjournald
                    
                    final String user    = data.substring(22-1,30-1).trim();
                    final String system  = data.substring(30-1,38-1).trim();
                    final String cpu     = data.substring(38-1,46-1).trim();
                    
                    if (INFO)
                            log.info("\n%user=" + user + ", %system=" + system
                                    + ", %cpu=" + cpu + "\n" + header + "\n"
                                    + data);
                    
                    vals.put(IProcessCounters.CPU_PercentProcessorTime,
                                Double.parseDouble(cpu));

                    vals.put(IProcessCounters.CPU_PercentUserTime,
                            Double.parseDouble(user));

                    vals.put(IProcessCounters.CPU_PercentSystemTime,
                            Double.parseDouble(system));
                    
                } else if(header.contains("RSS")) {
                    
                    /*
                     * Memory data for the specified process.
                     */
                    
//                      *       06:35:15 AM       PID  minflt/s  majflt/s     VSZ    RSS   %MEM  Command
//                      *       06:35:15 AM       501      0.00      0.00       0      0   0.00  kjournald
                  
                    final String minorFaultsPerSec = data.substring(22-1,32-1).trim();
                    final String majorFaultsPerSec = data.substring(32-1,42-1).trim();
                    final String virtualSize       = data.substring(42-1,50-1).trim();
                    final String residentSetSize   = data.substring(50-1,57-1).trim();
                    final String percentMemory     = data.substring(57-1,64-1).trim();

                    if(INFO)
                        log.info("\nminorFaultsPerSec="
                                + minorFaultsPerSec
                                + ", majorFaultsPerSec="
                                + majorFaultsPerSec + ", virtualSize="
                                + virtualSize + ", residentSetSize="
                                + residentSetSize + ", percentMemory="
                                + percentMemory + "\n"+header+"\n"+data);
                    
                    vals.put(IProcessCounters.Memory_minorFaultsPerSec,
                            Double.parseDouble(minorFaultsPerSec));
                    
                    vals.put(IProcessCounters.Memory_majorFaultsPerSec,
                            Double.parseDouble(majorFaultsPerSec));
                    
                    vals.put(IProcessCounters.Memory_virtualSize, 
                            Double.parseDouble(virtualSize));
                    
                    vals.put(IProcessCounters.Memory_residentSetSize,
                            Double.parseDouble(residentSetSize));
                    
                    vals.put(IProcessCounters.Memory_percentMemorySize, 
                            Double.parseDouble(percentMemory));

                } else if(perProcessIOData && header.contains("kB_rd/s")) {

                    /*
                     * IO data for the specified process.
                     */
                    
//                        *         06:35:15 AM       PID   kB_rd/s   kB_wr/s kB_ccwr/s  Command
//                        *         06:35:15 AM       501      0.00      1.13      0.00  kjournald

                    final String kBrdS = data.substring(22-1, 32-1).trim();
                    final String kBwrS = data.substring(32-1, 42-1).trim();

                    if(INFO)
                    log.info("\nkB_rd/s=" + kBrdS + ", kB_wr/s="
                                + kBwrS + "\n" + header + "\n" + data);

                    vals.put(IProcessCounters.PhysicalDisk_BytesReadPerSec,
                            Double.parseDouble(kBrdS) * Bytes.kilobyte32);
                    
                    vals.put(IProcessCounters.PhysicalDisk_BytesWrittenPerSec,
                            Double.parseDouble(kBrdS) * Bytes.kilobyte32);
                    
                } else {
                    
                    log.warn("Could not identify event type from header: ["+header+"]");

                    continue;
                    
                }
                n++;
            
            }
            
            }

        }
        
    }
    
}
