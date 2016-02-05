/*

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
/*
 * Created on Mar 26, 2008
 */

package com.bigdata.counters.linux;

import com.bigdata.counters.*;
import com.bigdata.counters.linux.SarCpuUtilizationCollector.DI;
import com.bigdata.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects statistics on the JVM process relating to CPU, memory, and IO
 * statistics (when available) using <code>pidstat -p 501 -u -I -r -d -w</code>
 * [[<i>interval</i> [<i>count</i>]]
 * <p>
 * Where <code>-p</code> is the pid to monitor, <code>-u</code> is cpu
 * utilization (<code>-I</code> normalizes to 100% for SMP), <code>-r</code>
 * gives the process memory statistics, <code>-d</code> gives IO statistics with
 * kernels 2.6.20 and up; <code>-w</code> gives context switching data; The
 * interval is in seconds. The count is optional - when missing or zero will
 * repeat forever if interval was specified.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class PIDStatCollector extends AbstractProcessCollector implements
        ICounterHierarchy, IProcessCounters {

//    static protected final Logger log = Logger.getLogger(PIDStatCollector.class);
//
//    /**
//     * True iff the {@link #log} level is DEBUG or less.
//     */
//    final protected static boolean DEBUG = log.isDebugEnabled();
//
//    /**
//     * True iff the {@link #log} level is log.isInfoEnabled() or less.
//     */
//    final protected static boolean log.isInfoEnabled() = log.isInfoEnabled();

    /** process to be monitored. */
    protected final int pid;
    
    /**
     * set <code>true</code> if per-process IO data collection should be
     * supported based on the {@link KernelVersion}.
     */
    protected final boolean perProcessIOData;
    
    /**
     * Abstract inner class integrating the current values with the {@link ICounterSet}
     * hierarchy.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    abstract class AbstractInst<T> implements IInstrument<T> {
        
        protected final String path;
        
        final public String getPath() {
            
            return path;
            
        }
        
        protected AbstractInst(final String path) {

            if (path == null)
                throw new IllegalArgumentException();
            
            this.path = path;
            
        }
        
        @Override
        final public long lastModified() {

            return lastModified.get();
            
        }

        /**
         * @throws UnsupportedOperationException
         *             always.
         */
        @Override
        final public void setValue(final T value, final long timestamp) {
           
            throw new UnsupportedOperationException();
            
        }

    }
        
    /**
     * Inner class integrating the current values with the {@link ICounterSet}
     * hierarchy.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    class IL extends AbstractInst<Long> {
        
        protected final long scale;
        
        public IL(final String path, final long scale) {
            
            super( path );
            
            this.scale = scale;
            
        }
        
        @Override
        public Long getValue() {
         
            final Long value = (Long) vals.get(path);

            // no value is defined.
            if (value == null)
                return 0L;

            final long v = value.longValue() * scale;

            return v;
            
        }

    }
    
    /**
     * Inner class integrating the current values with the {@link ICounterSet}
     * hierarchy.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    class ID extends AbstractInst<Double> {
        
        protected final double scale;
        
        public ID(final String path, final double scale) {

            super(path);
            
            this.scale = scale;
            
        }
        
        @Override
        public Double getValue() {
         
            final Double value = (Double) vals.get(path);

            // no value is defined.
            if (value == null)
                return 0d;

            final double d = value.doubleValue() * scale;

            return d;
            
        }

    }

    /**
     * Updated each time a new row of data is read from the process and reported
     * as the last modified time for counters based on that process and
     * defaulted to the time that we begin to collect performance data.
     */
    private final AtomicLong lastModified = new AtomicLong(System.currentTimeMillis());
    
    /**
     * Map containing the current values for the configured counters. The keys
     * are paths into the {@link CounterSet}. The values are the data most
     * recently read from <code>pidstat</code>.
     * <p>
     * Note: The paths are in fact relative to how the counters are declared by
     * {@link #getCounters()}. Likewise {@link DI#getValue()} uses the paths
     * declared within {@link #getCounters()} and not whatever path the counters
     * are eventually placed under within a larger hierarchy.
     */
    private final Map<String,Object> vals = new ConcurrentHashMap<String, Object>();

    /**
     * @param pid
     *            Process to be monitored.
     * @param interval
     *            Reporting interval in seconds.
     * @param kernelVersion
     *            The Linux {@link KernelVersion}.
     * 
     * @todo kernelVersion could be static.
     */
    public PIDStatCollector(final int pid, final int interval,
            final KernelVersion kernelVersion) {

        super(interval);
        
        if (interval <= 0)
            throw new IllegalArgumentException();
        
        if (kernelVersion == null)
            throw new IllegalArgumentException();

        this.pid = pid;
        
        perProcessIOData = kernelVersion.version > 2 || (kernelVersion.version == 2
                && kernelVersion.major >= 6 && kernelVersion.minor >= 20);

    }

    @Override
    public List<String> getCommand() {
        
        final List<String> command = new LinkedList<String>();
        
        command.add(SysstatUtil.getPath("pidstat").getPath());

        command.add("-p");
        command.add(""+pid);
    
        command.add("-u"); // CPU stats
        
        command.add("-I"); // normalize CPU stats to 100% iff SMP.
        
        if(perProcessIOData) {
         
            command.add("-d"); // disk IO report.
            
        }
        
        command.add("-r"); // memory report
        
//          command.add("-w"); // context switching report (not implemented in our code).

        command.add("" + getInterval());

        return command;
        
    }
    
    @Override
    public CounterSet getCounters() {
        
        final List<AbstractInst<?>> inst = new LinkedList<AbstractInst<?>>();

        /*
         * Note: Counters are all declared as Double to facilitate aggregation
         * and scaling.
         * 
         * Note: pidstat reports percentages as [0:100] so we normalize them to
         * [0:1] using a scaling factor.
         */

        inst.add(new ID(IProcessCounters.CPU_PercentUserTime, .01d));
        inst.add(new ID(IProcessCounters.CPU_PercentSystemTime, .01d));
        inst.add(new ID(IProcessCounters.CPU_PercentProcessorTime, .01d));

        inst.add(new ID(IProcessCounters.Memory_minorFaultsPerSec, 1d));
        inst.add(new ID(IProcessCounters.Memory_majorFaultsPerSec, 1d));
        inst.add(new IL(IProcessCounters.Memory_virtualSize, Bytes.kilobyte));
        inst.add(new IL(IProcessCounters.Memory_residentSetSize, Bytes.kilobyte));
        inst.add(new ID(IProcessCounters.Memory_percentMemorySize, .01d));

        /*
         * Note: pidstat reports in kb/sec so we normalize to bytes/second using
         * a scaling factor.
         */
        inst.add(new ID(IProcessCounters.PhysicalDisk_BytesReadPerSec,
                Bytes.kilobyte32));
        inst.add(new ID(IProcessCounters.PhysicalDisk_BytesWrittenPerSec,
                Bytes.kilobyte32));

        final CounterSet root = new CounterSet();
        
        for (AbstractInst<?> i : inst) {

            root.addCounter(i.getPath(), i);

        }

        return root;

    }

    /**
     * Extended to force <code>pidstat</code> to use a consistent timestamp
     * format regardless of locale by setting <code>S_TIME_FORMAT="ISO"</code>
     * in the environment.
     */
    @Override
    protected void setEnvironment(final Map<String, String> env) {

        super.setEnvironment(env);

        env.put("S_TIME_FORMAT", "ISO");

    }

    @Override
    public AbstractProcessReader getProcessReader() {

        return new PIDStatReader();
        
    }

    /**
     * Reads <code>pidstat</code> output and extracts and updates counter
     * values.
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
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    protected class PIDStatReader extends ProcessReaderHelper {

        private static final String PIDSTAT_FIELD_CPU_PERCENT_USR = "%usr";
        private static final String PIDSTAT_FIELD_CPU_PERCENT = "%CPU";
        private static final String PIDSTAT_FIELD_CPU_PERCENT_SYSTEM = "%system";
        private static final String PIDSTAT_FIELD_MEM_MINOR_FAULTS_PERS = "minflt/s";
        private static final String PIDSTAT_FIELD_MEM_MAJOR_FAULTS_PERS = "majflt/s";
        private static final String PIDSTAT_FIELD_MEM_VIRTUAL_SIZE = "VSZ";
        private static final String PIDSTAT_FIELD_MEM_RESIDENT_SET_SIZE = "RSS";
        private static final String PIDSTAT_FIELD_MEM_SIZE_PERCENT = "%MEM";
        private static final String PIDSTAT_FIELD_DISK_KB_READ_PERS = "kB_rd/s";
        private static final String PIDSTAT_FIELD_DISK_KB_WRITTEN_PERS = "kB_wr/s";

        @Override
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
        @Override
        protected void readProcess() throws IOException, InterruptedException {

            if(log.isInfoEnabled())
                log.info("begin");
            
            for(int i=0; i<10 && !getActiveProcess().isAlive(); i++) {

                if(log.isInfoEnabled())
                    log.info("waiting for the readerFuture to be set.");

                Thread.sleep(100/*ms*/);
                
            }

            if(log.isInfoEnabled())
                log.info("running");
            
        // skip banner.
        final String banner = readLine();
        
        if(log.isInfoEnabled())
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

            try {
                
            // timestamp
//            {
//
//                final String s = data.substring(0, 11);
//
//                try {
//
//                    lastModified = f.parse(s).getTime();
//
//                } catch (Exception e) {
//
//                    log.warn("Could not parse time: [" + s + "] : " + e);
//
//                    // should be pretty close.
//                    lastModified = System.currentTimeMillis();
//
//                }
//
//            }                /*
            /*
             * Note: This timestamp should be _very_ close to the value reported
             * by sysstat. Also, using the current time is MUCH easier and less
             * error prone than attempting to parse the TIME OF DAY written by
             * sysstat and correct it into a UTC time by adjusting for the UTC
             * time of the start of the current day, which is what we would have
             * to do.
             */
            lastModified.set(System.currentTimeMillis());

            final Map<String, String> fields = SysstatUtil.getDataMap(header, data);
            if (log.isInfoEnabled()) {
                StringBuilder sb = new StringBuilder();
                for ( Map.Entry<String, String> e: fields.entrySet()) {
                    sb.append(e.getKey());
                    sb.append("=");
                    sb.append(e.getValue());
                    sb.append(", ");
                }
                log.info(sb.toString());
                log.info(header + ";" + data);
            }
            if(fields.containsKey(PIDSTAT_FIELD_CPU_PERCENT)) {
                
                /*
                 * CPU data for the specified process.
                 */

                // 06:35:15 AM       PID   %user %system    %CPU   CPU  Command
                // 06:35:15 AM       501    0.00    0.01    0.00     1  kjournald
                

                vals.put(IProcessCounters.CPU_PercentUserTime,
                        Double.parseDouble(fields.get(PIDSTAT_FIELD_CPU_PERCENT_USR)));

                vals.put(IProcessCounters.CPU_PercentSystemTime,
                        Double.parseDouble(fields.get(PIDSTAT_FIELD_CPU_PERCENT_SYSTEM)));
                
                vals.put(IProcessCounters.CPU_PercentProcessorTime,
                        Double.parseDouble(fields.get(PIDSTAT_FIELD_CPU_PERCENT)));

            } else if(fields.containsKey("RSS")) {
                
                /*
                 * Memory data for the specified process.
                 *
                 *       06:35:15 AM       PID  minflt/s  majflt/s     VSZ    RSS   %MEM  Command
                 *       06:35:15 AM       501      0.00      0.00       0      0   0.00  kjournald
                 */


                vals.put(IProcessCounters.Memory_minorFaultsPerSec,
                        Double.parseDouble(fields.get(PIDSTAT_FIELD_MEM_MINOR_FAULTS_PERS)));
                
                vals.put(IProcessCounters.Memory_majorFaultsPerSec,
                        Double.parseDouble(fields.get(PIDSTAT_FIELD_MEM_MAJOR_FAULTS_PERS)));
                
                vals.put(IProcessCounters.Memory_virtualSize, 
                        Long.parseLong(fields.get(PIDSTAT_FIELD_MEM_VIRTUAL_SIZE)));
                
                vals.put(IProcessCounters.Memory_residentSetSize,
                        Long.parseLong(fields.get(PIDSTAT_FIELD_MEM_RESIDENT_SET_SIZE)));
                
                vals.put(IProcessCounters.Memory_percentMemorySize, 
                        Double.parseDouble(fields.get(PIDSTAT_FIELD_MEM_SIZE_PERCENT)));

            } else if(perProcessIOData && header.contains("kB_rd/s")) {

                /*
                 * IO data for the specified process.
                 *
                 *         06:35:15 AM       PID   kB_rd/s   kB_wr/s kB_ccwr/s  Command
                 *         06:35:15 AM       501      0.00      1.13      0.00  kjournald
                */

                vals.put(IProcessCounters.PhysicalDisk_BytesReadPerSec,
                        Double.parseDouble(fields.get(PIDSTAT_FIELD_DISK_KB_READ_PERS)));
                
                vals.put(IProcessCounters.PhysicalDisk_BytesWrittenPerSec,
                        Double.parseDouble(fields.get(PIDSTAT_FIELD_DISK_KB_WRITTEN_PERS)));
                
            } else {
                
                log.warn("Could not identify event type from header: ["+header+"]");
                continue;
                
            }
            
            } catch(Exception ex) {

                /*
                 * Issue warning for parsing problems.
                 */
                
                log.warn(ex.getMessage() //
                            + "\nheader: " + header //
                            + "\n  data: " + data   //
                            //, ex//
                            );
                
            }

            n++;
        
        }
        
        }

    }
    
}
