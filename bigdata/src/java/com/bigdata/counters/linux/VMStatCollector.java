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
 * Created on Dec 9, 2008
 */

package com.bigdata.counters.linux;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.bigdata.counters.AbstractProcessCollector;
import com.bigdata.counters.AbstractProcessReader;
import com.bigdata.counters.ActiveProcess;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterHierarchy;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IHostCounters;
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.counters.ProcessReaderHelper;

/**
 * Collects some counters using <code>vmstat</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class VMStatCollector extends AbstractProcessCollector implements
        ICounterHierarchy, IRequiredHostCounters, IHostCounters{

    /**
     * Inner class integrating the current values with the {@link ICounterSet}
     * hierarchy.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    abstract class I<T> implements IInstrument<T> {
        
        protected final String path;
        
        public String getPath() {
            
            return path;
            
        }
        
        public I(String path) {
            
            assert path != null;
            
            this.path = path;
            
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
     * Double precision counter with scaling factor.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class DI extends I<Double> {

        protected final double scale;
        
        DI(String path, double scale) {
            
            super( path );
            
            this.scale = scale;
            
        }
        
        
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
     * Map containing the current values for the configured counters. The keys
     * are paths into the {@link CounterSet}. The values are the data most
     * recently read from <code>vmstat</code>.
     */
    private Map<String, Object> vals = new HashMap<String, Object>();
    
    /**
     * The timestamp associated with the most recently collected values.
     */
    private long lastModified = System.currentTimeMillis();

    /**
     * <code>true</code> iff you want collect the user time, system time,
     * and IO WAIT time using vmstat (as opposed to sar).
     */
    protected final boolean cpuStats;

    /**
     * The {@link Pattern} used to split apart the rows read from
     * <code>vmstat</code>.
     */
    final protected Pattern pattern = Pattern.compile("\\s*");

    /**
     * 
     * @param interval
     *            The interval for the collected statistics.
     * @param cpuStats
     *            <code>true</code> iff you want collect the user time, system
     *            time, and IO WAIT time using vmstat (as opposed to sar).
     */
    public VMStatCollector(final int interval, boolean cpuStats) {

        super(interval);

        this.cpuStats = cpuStats;
        
    }
    
    public List<String> getCommand() {

        final List<String> command = new LinkedList<String>();

        command.add("/usr/bin/vmstat");

        // Note: switch indicats that the header will be displayed only once.
        command.add("-n");
        
        // Note: The configured interval in seconds between reports.
        command.add(""+getInterval());
        
        return command;
        
    }

    /**
     * Declares the counters that we will collect
     * 
     * @todo bi (blocks in) and bo (block out)?
     */
    synchronized public CounterSet getCounters() {
        
        if(root == null) {
        
            root = new CounterSet();
            
            inst = new LinkedList<I>();
            
            /*
             * Note: Counters are all declared as Double to facilitate
             * aggregation.
             */

            /*
             * Note: [si] is "the #of blocks swapped in per second."
             */
            inst.add(new DI(IRequiredHostCounters.Memory_majorFaultsPerSecond,
                    1d));

            /*
             * Note: [swpd] is "the amount of virtual memory used". The counter
             * is reported in 1024 byte blocks, so we convert to bytes using a
             * scaling factor.
             * 
             * @todo where do I get the amount of swap space available?
             */
            inst.add(new DI(IHostCounters.Memory_SwapUsed, 1024d));

            /*
             * Note: [free] is "the amount of idle memory". The counter is
             * reported in 1024 byte blocks, so we convert to bytes using a
             * scaling factor.
             */
            inst.add(new DI(IHostCounters.Memory_Idle, 1024d));

            if (cpuStats) {

                /*
                 * Note: vmstat reports percentages in [0:100] so we convert
                 * them to [0:1] using a scaling factor.
                 */

                // Note: processor time = (100-idle), converted to [0:1].
                inst.add(new DI(IRequiredHostCounters.CPU_PercentProcessorTime,.01d));
                // Note: column us
                inst.add(new DI(IHostCounters.CPU_PercentUserTime, .01d));
                // Note: column sy
                inst.add(new DI(IHostCounters.CPU_PercentSystemTime, .01d));
                // Note: column wa
                inst.add(new DI(IHostCounters.CPU_PercentIOWait, .01d));
                
            }
            
            for(Iterator<I> itr = inst.iterator(); itr.hasNext(); ) {
                
                final I i = itr.next();
                
                root.addCounter(i.getPath(), i);
                
            }
            
        }
        
        return root;
        
    }
    private List<I> inst = null;
    private CounterSet root = null;

    public AbstractProcessReader getProcessReader() {
        
        return new VMStatReader();
        
    }

    /**
     * Sample output for <code>vmstat -n 1</code>, where <code>-n</code>
     * suppresses the repeat of the header and <code>1</code> is the interval.
     * 
     * <pre>
     * procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu------
     *  r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
     *  1  0     96 178580 206520 1170604    0    0     0     7    1    0  1  0 99  0  0
     *  0  0     96 178572 206520 1170608    0    0     0     0    4  619  2  0 98  0  0
     *  0  0     96 178572 206520 1170608    0    0     0     0   22  574  1  0 100  0  0
     *  0  0     96 178572 206520 1170608    0    0     0     0   53  615  1  0 99  0  0
     *  0  0     96 178572 206520 1170608    0    0     0     0   20  570  1  0 99  0  0
     *  0  0     96 178324 206520 1170608    0    0     0    80   15  584  0  0 99  0  0
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class VMStatReader extends ProcessReaderHelper {
        
        protected ActiveProcess getActiveProcess() {
            
            if (activeProcess == null)
                throw new IllegalStateException();
            
            return activeProcess;
            
        }

        public VMStatReader() {

            super();
            
        }

        @Override
        protected void readProcess() throws Exception {
            
            if(INFO)
                log.info("begin");

            for(int i=0; i<10 && !getActiveProcess().isAlive(); i++) {

                if(INFO)
                    log.info("waiting for the readerFuture to be set.");

                Thread.sleep(100/*ms*/);
                
            }

            if(INFO)
                log.info("running");

            // skip 1st header.
            {

                final String header = readLine();

                if (INFO)
                    log.info("header: " + header);
            }

            // read 2nd header and verify expected fields.
            final String header;
            {

                header = readLine();

                final String[] fields = pattern.split(header, 0/* limit */);

                if (INFO)
                    log.info("header: " + header);

                assertField(header, fields, 2, "swpd");
                assertField(header, fields, 3, "free");
                
                assertField(header, fields, 6, "si");
                assertField(header, fields, 7, "so");
                
                assertField(header, fields, 12, "us");
                assertField(header, fields, 13, "sy");
                assertField(header, fields, 14, "id");
                assertField(header, fields, 15, "wa");

            }

            // read lines until interrupted.
            while(true) {
                
                // data.
                final String data = readLine();
                
// 0  1      2      3      4      5    6    7     8     9   10   11 12 13 14 15 16 (fields)
// r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st (header)
// 1  0     96 178580 206520 1170604    0    0     0     7    1    0  1  0 99  0  0 (data)

                try {

                    // timestamp
                    lastModified = System.currentTimeMillis();

                    final String[] fields = pattern.split(data, 0/* limit */);

                    final String swpd = fields[2];
                    final String free = fields[3];

                    final String si = fields[6];
                    final String so = fields[7];

                    final String user = fields[12];
                    final String system = fields[13];
                    final String idle = fields[14];
                    final String iowait = fields[15];
                    // final String steal = fields[16];

                    if (INFO)
                        log.info("\nswpd=" + swpd + ", free=" + free + ", si="
                                + si + ", so=" + so + ", %user=" + user
                                + ", %system=" + system + ", idle=" + idle
                                + ", iowait=" + iowait + "\n" + header + "\n"
                                + data);

                    vals.put(IHostCounters.Memory_SwapUsed, Double
                            .parseDouble(swpd));

                    vals.put(IHostCounters.Memory_Idle, Double
                            .parseDouble(free));

                    vals.put(IHostCounters.Memory_majorFaultsPerSecond, Double
                            .parseDouble(si));

                    if (cpuStats) {

                        vals.put(IHostCounters.CPU_PercentUserTime, Double
                                .parseDouble(user));

                        vals.put(IHostCounters.CPU_PercentSystemTime, Double
                                .parseDouble(system));

                        vals.put(IHostCounters.CPU_PercentIOWait, Double
                                .parseDouble(iowait));

                        vals.put(
                                IRequiredHostCounters.CPU_PercentProcessorTime,
                                (100d - Double.parseDouble(idle)));

                    }

                } catch (Exception ex) {

                    /*
                     * Issue warning for parsing problems.
                     */

                    log.warn(ex.getMessage() //
                            + "\nheader: " + header //
                            + "\n  data: " + data //
                    , ex);

                }

            }

        }

        /**
         * Used to verify that the header corresponds to our expectations. Logs
         * errors when the expectations are not met.
         * 
         * @param header
         *            The header line.
         * @param fields
         *            The fields parsed from that header.
         * @param field
         *            The field number in [0:#fields-1].
         * @param expected
         *            The expected value of the header for that field.
         */
        protected void assertField(final String header, final String[] fields,
                final int field, final String expected) {
            
            if (header == null)
                throw new IllegalArgumentException();
            
            if (fields == null)
                throw new IllegalArgumentException();

            if (expected == null)
                throw new IllegalArgumentException();
 
            if (field < 0)
                throw new IllegalArgumentException();
            
            if (field >= fields.length)
                log.error("There are only " + fields.length
                        + " fields, but field=" + field + "\n" + header);
            
            if (!expected.equals(fields[field])) {

                log.error("Expected field=" + field + " to be [" + expected
                        + "], actual=" + fields[field] + "\n" + header);
                
            }
            
        }
        
    }

}
