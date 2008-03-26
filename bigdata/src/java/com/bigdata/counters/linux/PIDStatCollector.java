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
 * Created on Mar 26, 2008
 */

package com.bigdata.counters.linux;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.AbstractStatisticsCollector.AbstractProcessCollector;
import com.bigdata.rawstore.Bytes;

/**
 * Collects statistics on the JVM process relating to CPU, memory, and IO
 * statistics (when available) using <code>pidstat -p 501 -u -I -r -d -w</code> [[<i>interval</i> [<i>count</i>]]
 * <p>
 * Where -p is the pid to montitor -u is cpi (-I normalizes to 100% for SMP), -r
 * is memory stats, -d gives IO statistics with kernels 2.6.20 and up; -w is
 * context switching data; The interval is in seconds. The count is optional -
 * when missing or zero will repeat forever if interval was specified.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PIDStatCollector extends AbstractProcessCollector {

    static protected final Logger log = Logger.getLogger(PIDStatCollector.class);

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

    /** process to be monitored. */
    protected final int pid;

    /**
     * set <code>true</code> if per-process IO data collection should be
     * supported based on the {@link KernelVersion}.
     */
    protected final boolean perProcessIOData;

    /**
     * Inner class integrating the current values with the {@link ICounterSet}
     * hierarchy.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class I implements IInstrument<Double> {
        
        protected final String path;
        protected final double scale;
        
        public String getPath() {
            
            return path;
            
        }
        
        public I(String path, double scale) {
            
            assert path != null;
            
            this.path = path;
            
            this.scale = scale;
            
        }
        
        public Double getValue() {
         
            double d = (Double) vals.get(path);
            
            d *= scale;
            
            return d;
            
        }

        public long lastModified() {

            return lastModified;
            
        }

        /**
         * @throws UnsupportedOperationException
         *             always.
         */
        public void setValue(Double value, long timestamp) {
           
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
            KernelVersion kernelVersion) {

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
        
//          command.add("-w"); // context switching report (not implemented in our code).
        
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
             * aggregation and scaling.
             * 
             * Note: pidstat reports percentages as [0:100] so we normalize them
             * to [0:1] using a scaling factor.
             */
            
            inst.add(new I(IRequiredHostCounters.CPU_PercentProcessorTime,.01d));
            
            inst.add(new I(IProcessCounters.CPU_PercentUserTime,.01d));
            
            inst.add(new I(IProcessCounters.CPU_PercentSystemTime,.01d));
            
            inst.add(new I(IProcessCounters.Memory_minorFaultsPerSec,1d));
            inst.add(new I(IProcessCounters.Memory_majorFaultsPerSec,1d));
            inst.add(new I(IProcessCounters.Memory_virtualSize,1d));
            inst.add(new I(IProcessCounters.Memory_residentSetSize,1d));
            inst.add(new I(IProcessCounters.Memory_percentMemorySize,.01d));

            /*
             * Note: pidstat reports in kb/sec so we normalize to bytes/second
             * using a scaling factor.
             */
            inst.add(new I(IProcessCounters.PhysicalDisk_BytesReadPerSec, Bytes.kilobyte32));
            inst.add(new I(IProcessCounters.PhysicalDisk_BytesWrittenPerSec, Bytes.kilobyte32));

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
                lastModified = StatisticsCollectorForLinux.f.parse(s).getTime();
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
                
//                  *       06:35:15 AM       PID  minflt/s  majflt/s     VSZ    RSS   %MEM  Command
//                  *       06:35:15 AM       501      0.00      0.00       0      0   0.00  kjournald
              
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
                
//                    *         06:35:15 AM       PID   kB_rd/s   kB_wr/s kB_ccwr/s  Command
//                    *         06:35:15 AM       501      0.00      1.13      0.00  kjournald

                final String kBrdS = data.substring(22-1, 32-1).trim();
                final String kBwrS = data.substring(32-1, 42-1).trim();

                if(INFO)
                log.info("\nkB_rd/s=" + kBrdS + ", kB_wr/s="
                            + kBwrS + "\n" + header + "\n" + data);

                vals.put(IProcessCounters.PhysicalDisk_BytesReadPerSec,
                        Double.parseDouble(kBrdS));
                
                vals.put(IProcessCounters.PhysicalDisk_BytesWrittenPerSec,
                        Double.parseDouble(kBrdS));
                
            } else {
                
                log.warn("Could not identify event type from header: ["+header+"]");

                continue;
                
            }
            n++;
        
        }
        
        }

    }
    
}
