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
 * Created on Dec 9, 2008
 */

package com.bigdata.counters.linux;

import com.bigdata.counters.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Collects some counters using <code>vmstat</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class VMStatCollector extends AbstractProcessCollector implements
        ICounterHierarchy, IRequiredHostCounters, IHostCounters{

    /**
     * Inner class integrating the current values with the {@link ICounterSet}
     * hierarchy.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    abstract class I<T> implements IInstrument<T> {
        
        protected final String path;
        
        public String getPath() {
            
            return path;
            
        }
        
        public I(final String path) {
            
            if (path == null)
                throw new IllegalArgumentException();
            
            this.path = path;
            
        }

        @Override
        public long lastModified() {

            return lastModified.get();
            
        }

        /**
         * @throws UnsupportedOperationException
         *             always.
         */
        @Override
        public void setValue(final T value, final long timestamp) {
           
            throw new UnsupportedOperationException();
            
        }

    }
    
    /**
     * Double precision counter with scaling factor.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    class DI extends I<Double> {

        protected final double scale;
        
        DI(final String path, final double scale) {

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
     * Map containing the current values for the configured counters. The keys
     * are paths into the {@link CounterSet}. The values are the data most
     * recently read from <code>vmstat</code>.
     */
    final private Map<String, Object> vals = new ConcurrentHashMap<String, Object>();
    
    /**
     * The timestamp associated with the most recently collected values.
     */
    private final AtomicLong lastModified = new AtomicLong(
            System.currentTimeMillis());

    /**
     * <code>true</code> iff you want collect the user time, system time,
     * and IO WAIT time using vmstat (as opposed to sar).
     */
    private final boolean cpuStats;

    /**
     * The {@link Pattern} used to split apart the rows read from
     * <code>vmstat</code>.
     */
    // Note: Exposed to the test suite.
    final static Pattern pattern = Pattern.compile("\\s+");

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
    
    @Override
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
     */
    @Override
	public CounterSet getCounters() {

	    @SuppressWarnings("rawtypes")
        final List<I> inst = new LinkedList<I>();

		/*
		 * Note: Counters are all declared as Double to facilitate aggregation.
		 */

		/*
		 * Note: [si] is "the #of blocks swapped in per second."
		 */
		inst.add(new DI(IRequiredHostCounters.Memory_majorFaultsPerSecond, 1d));

		/*
		 * Note: [swpd] is "the amount of virtual memory used". The counter is
		 * reported in 1024 byte blocks, so we convert to bytes using a scaling
		 * factor.
		 * 
		 * @todo where do I get the amount of swap space available?
		 */
		inst.add(new DI(IHostCounters.Memory_SwapBytesUsed, 1024d));

		/*
		 * Note: [free] is "the amount of idle memory". The counter is reported
		 * in 1024 byte blocks, so we convert to bytes using a scaling factor.
		 */
		inst.add(new DI(IHostCounters.Memory_Bytes_Free, 1024d));

		/*
		 * Note: [bi] is "the blocks received from a device / second". The
		 * counter is reported in 1024 byte blocks, so we convert to bytes using
		 * a scaling factor.
		 */
		inst.add(new DI(IRequiredHostCounters.PhysicalDisk_BytesReadPerSec,
				1024d));

		/*
		 * Note: [bo] is "the blocks sent to a device / second". The counter is
		 * reported in 1024 byte blocks, so we convert to bytes using a scaling
		 * factor.
		 */
		inst.add(new DI(IRequiredHostCounters.PhysicalDisk_BytesWrittenPerSec,
				1024d));

		if (cpuStats) {

			/*
			 * Note: vmstat reports percentages in [0:100] so we convert them to
			 * [0:1] using a scaling factor.
			 */

			// Note: processor time = (100-idle), converted to [0:1].
			inst.add(new DI(IRequiredHostCounters.CPU_PercentProcessorTime,
					.01d));
			// Note: column us
			inst.add(new DI(IHostCounters.CPU_PercentUserTime, .01d));
			// Note: column sy
			inst.add(new DI(IHostCounters.CPU_PercentSystemTime, .01d));
			// Note: column wa
			inst.add(new DI(IHostCounters.CPU_PercentIOWait, .01d));

		}

        final CounterSet root = new CounterSet();

        for (@SuppressWarnings("rawtypes") I i : inst) {

            root.addCounter(i.getPath(), i);

        }

		return root;

	}

    @Override
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
     */
    private class VMStatReader extends ProcessReaderHelper {

        private static final String VMSTAT_FIELD_SWPD = "swpd";
        private static final String VMSTAT_FIELD_FREE = "free";
        private static final String VMSTAT_FIELD_SI = "si";
        private static final String VMSTAT_FIELD_SO = "so";
        private static final String VMSTAT_FIELD_BI = "bi";
        private static final String VMSTAT_FIELD_BO = "bo";
        private static final String VMSTAT_FIELD_US = "us";
        private static final String VMSTAT_FIELD_SY = "sy";
        private static final String VMSTAT_FIELD_ID = "id";
        private static final String VMSTAT_FIELD_WA = "wa";

        @Override
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
            
            if(log.isInfoEnabled())
                log.info("begin");

            for (int i = 0; i < 10 && !getActiveProcess().isAlive(); i++) {

                if(log.isInfoEnabled())
                    log.info("waiting for the readerFuture to be set.");

                Thread.sleep(100/*ms*/);
                
            }

            if(log.isInfoEnabled())
                log.info("running");

            // skip 1st header.
            {

                final String header = readLine();

                if (log.isInfoEnabled())
                    log.info("header: " + header);
            }

            // read 2nd header and verify expected fields.
            final String header;
            {

                header = readLine();

                if (log.isInfoEnabled())
                    log.info("header: " + header);

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
                    lastModified.set(System.currentTimeMillis());

                    final Map<String, String> fields = SysstatUtil.getDataMap(header, data);


                    if (log.isInfoEnabled())
                        log.info("\nswpd=" + fields.get(VMSTAT_FIELD_SWPD) + ", free=" + fields.get(VMSTAT_FIELD_FREE) + ", si="
                                + fields.get(VMSTAT_FIELD_SI) + ", so=" + fields.get(VMSTAT_FIELD_SO) + ", bi=" + fields.get(VMSTAT_FIELD_BI) + ", bo="
                                + fields.get(VMSTAT_FIELD_BO) + ", %user=" + fields.get(VMSTAT_FIELD_US) + ", %system="
                                + fields.get(VMSTAT_FIELD_SY) + ", idle=" + fields.get(VMSTAT_FIELD_ID) + ", iowait="
                                + fields.get(VMSTAT_FIELD_WA) + "\n" + header + "\n" + data);

                    vals.put(IHostCounters.Memory_SwapBytesUsed, Double
                            .parseDouble(fields.get(VMSTAT_FIELD_SWPD)));

                    vals.put(IHostCounters.Memory_Bytes_Free, Double
                            .parseDouble(fields.get(VMSTAT_FIELD_FREE)));

                    vals.put(IRequiredHostCounters.Memory_majorFaultsPerSecond, Double
                            .parseDouble(fields.get(VMSTAT_FIELD_SI)));

                    vals.put(IRequiredHostCounters.PhysicalDisk_BytesReadPerSec, Double
                            .parseDouble(fields.get(VMSTAT_FIELD_BI)));

                    vals.put(IRequiredHostCounters.PhysicalDisk_BytesWrittenPerSec, Double
                            .parseDouble(fields.get(VMSTAT_FIELD_BO)));

                    if (cpuStats) {

                        vals.put(IHostCounters.CPU_PercentUserTime, Double
                                .parseDouble(fields.get(VMSTAT_FIELD_US)));

                        vals.put(IHostCounters.CPU_PercentSystemTime, Double
                                .parseDouble(fields.get(VMSTAT_FIELD_SY)));

                        vals.put(IHostCounters.CPU_PercentIOWait, Double
                                .parseDouble(fields.get(VMSTAT_FIELD_WA)));

                        vals.put(
                                IRequiredHostCounters.CPU_PercentProcessorTime,
                                (100d - Double.parseDouble(fields.get(VMSTAT_FIELD_ID))));

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

    }

}
