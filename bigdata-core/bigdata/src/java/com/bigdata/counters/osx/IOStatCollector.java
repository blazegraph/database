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

package com.bigdata.counters.osx;

import com.bigdata.counters.*;
import com.bigdata.util.Bytes;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Collects some counters using <code>iostat</code> under OSX. Unfortunately,
 * <code>iostat</code> does not break down the reads and writes and does not
 * report IO Wait. This information is obviously available from OSX as it is
 * provided by the ActivityMonitor, but we can not get it from
 * <code>iostat</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class IOStatCollector extends AbstractProcessCollector implements
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
            
            assert path != null;
            
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

        DI(final String path) {
        	
            this(path, 1d);

        }

        DI(final String path, final double scale) {
            
            super( path );
            
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
     * recently read from <code>iostat</code>.
     */
    final private Map<String, Object> vals = new ConcurrentHashMap<String, Object>();
    
	/**
	 * The timestamp associated with the most recently collected values.
	 */
	private final AtomicLong lastModified = new AtomicLong(System.currentTimeMillis());

	/**
	 * The {@link Pattern} used to split apart the rows read from
	 * <code>iostat</code>.
	 */
	final static Pattern pattern = Pattern.compile("\\s+");

	final private boolean cpuStats;
	
	/**
	 * 
	 * @param interval
	 *            The interval for the collected statistics.
	 * @param cpuStats
	 *            <code>true</code> if the collector should report on the CPU
	 *            stats (us, sy, id).
	 */
    public IOStatCollector(final int interval, final boolean cpuStats) {

        super(interval);
      
        this.cpuStats = cpuStats;
        
    }

    @Override   
    public List<String> getCommand() {

		final List<String> command = new LinkedList<String>();

		command.add("/usr/sbin/iostat");

		// report only device stats.
		command.add("-d");
		
		// report on ALL devices
		command.add("-n");
		command.add("999");
		
		// report CPU stats (explicitly request, so in addition to device stats).
        command.add("-C");

		// Note: The configured interval in seconds between reports.
		command.add("-w");
		command.add("" + getInterval());

		return command;
        
    }

    @Override
    public CounterSet getCounters() {
        
		final CounterSet root = new CounterSet();

		@SuppressWarnings("rawtypes")
        final List<I> inst = new LinkedList<I>();

		/*
		 * Note: Counters are all declared as Double to facilitate aggregation.
		 */

		/*
		 * This reports CPU (us, sy, id), bytes read/written per second, and
		 * transfers per second.
		 * 
		 * Note: We could also report KB/t (kilobytes per transfer), but it can
		 * be derived from MB/s and tps.
		 */

		/*
		 * Note: The counter is reported in MB/s, so we convert to bytes using a
		 * scaling factor.
		 */
		inst.add(new DI(IHostCounters.PhysicalDisk_BytesPerSec,
				(double) Bytes.megabyte32));

		/*
		 * Note: The counter is reported in transfers/s.
		 */
		inst.add(new DI(IHostCounters.PhysicalDisk_TransfersPerSec));

        if (cpuStats) {

			/*
			 * Note: iostats reports percentages in [0:100] so we convert them
			 * to [0:1] using a scaling factor.
			 */

            // Note: processor time = (100-idle), converted to [0:1].
            inst.add(new DI(IRequiredHostCounters.CPU_PercentProcessorTime,.01d));
            // Note: column us
            inst.add(new DI(IHostCounters.CPU_PercentUserTime, .01d));
            // Note: column sy
            inst.add(new DI(IHostCounters.CPU_PercentSystemTime, .01d));
//            // Note: IO Wait is NOT reported by iostat.
//            inst.add(new DI(IHostCounters.CPU_PercentIOWait, .01d));
            
        }

        for (@SuppressWarnings("rawtypes") I i : inst) {

            root.addCounter(i.getPath(), i);

        }

		return root;
        
    }

    @Override
    public AbstractProcessReader getProcessReader() {
        
        return new IOStatReader();
        
    }

	/**
	 * Sample output for <code>iostat -d -C -n 999 -w 60</code>, where
	 * <code>60</code> is the interval. There is no option to suppress the
	 * periodic repeat of the header. The header repeats in its entirety every
	 * "page" full.
	 * 
	 * <pre>
	 *           disk0           disk1           disk2           disk3           disk4       cpu
	 *     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s  us sy id
	 *   197.14  26  5.10    79.10   1  0.07     3.74   0  0.00    41.31   0  0.00    13.03   0  0.00  31  6 63
	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  53  4 43
	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  56  8 37
	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  54  9 37
	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  57  8 35
	 * </pre>
	 * 
	 * Note that the #of device columns will vary for this format. The #of
	 * columns for the report can presumably vary as devices are connected and
	 * disconnected!
	 * <p>
	 * Note: The first data line is ignored as it will be a historical average.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
    protected class IOStatReader extends ProcessReaderHelper {

        public static final int IOSTAT_CPU_FIELDS_NUM = 3;
        public static final String IOSTAT_FIELD_KB_T = "KB/t";
        public static final String IOSTAT_FIELD_TPS = "tps";
        public static final String IOSTAT_FIELD_MB_S = "MB/s";
        public static final String IOSTAT_FIELD_CPU_US = "us";
        public static final String IOSTAT_FIELD_CPU_SY = "sy";
        public static final String IOSTAT_FIELD_CPU_ID = "id";

        @Override
        protected ActiveProcess getActiveProcess() {
            
            if (activeProcess == null)
                throw new IllegalStateException();
            
            return activeProcess;
            
        }

        public IOStatReader() {

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

            // 1st header. this has the "disk#" metadata, which we are ignoring.
            {



			}

			/*
			 * Read headers. this is also ignored, but we spot check a few
			 * things.
			 * 
			 * Note: We have to recompute the #of devices for each data line as
			 * devices may be connected and disconnected at any time.
			 */
			final int ncpuFields = cpuStats ? 3 : 0; // us sy id
			final int nfieldsPerDevice; // KB/t tps MB/s
            final Map<String, Integer> deviceFields = new HashMap<>();
            final Map<String, Integer> cpuFields = new HashMap<>();
			String header1 = null;
            {
                final String h0;
                h0 = readLine();

                if (log.isInfoEnabled())
                    log.info("header: " + h0);

                final String h1;
                header1 = h1 = readLine();

                if (log.isInfoEnabled())
                    log.info("header: " + h1);

                int ndevices = pattern.split(h0.trim(), 0).length - 1/* cpu */;

				final String[] fields = pattern
						.split(h1.trim(), 0/* limit */);

				final int nfields = fields.length;
				final int ndeviceFields = fields.length - IOSTAT_CPU_FIELDS_NUM;
                nfieldsPerDevice = ndeviceFields/ndevices;

                for (int i = 0; i < nfieldsPerDevice; i++) {
                    deviceFields.put(fields[i], i);
                }

                for (int i = ndeviceFields; i < nfields; i++) {
                    cpuFields.put(fields[i], i-ndeviceFields);
                }

				if (log.isInfoEnabled()) {
					log.info("ndevices=" + ndevices);
				}
				
            }

//    	 *           disk0           disk1           disk2           disk3           disk4       cpu
//    	 *     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s  us sy id
//    	 *   197.14  26  5.10    79.10   1  0.07     3.74   0  0.00    41.31   0  0.00    13.03   0  0.00  31  6 63
//    	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  53  4 43

            // read lines until interrupted.
            boolean first = true;
            while(true) {
                
                // read the next line of data.
            	final String data;
            	{
            		String s = readLine();
					if (s.contains("disk") || s.contains("cpu")) {
						// We just read the 1st header
						header1 = s = readLine(); // 2nd header line.
            			s = readLine(); // data line.
            			if(log.isInfoEnabled())
            				log.info("Skipped headers.");
            		}
            		data = s;
            	}
            	
            	if(first) {
            		// Skip the first data line.  It is a historical summary.
            		first = false;
            		continue;
            	}
                
                try {

                    // timestamp
                    lastModified.set(System.currentTimeMillis());

					final String[] fields = pattern
							.split(data.trim(), 0/* limit */);
                    
    				final int nfields = fields.length;
    				final int ndeviceFields = fields.length - IOSTAT_CPU_FIELDS_NUM;
    				final int ndevices = ndeviceFields / nfieldsPerDevice;

					if (cpuStats) {

                        final String us = fields[nfields - IOSTAT_CPU_FIELDS_NUM + cpuFields.get(IOSTAT_FIELD_CPU_US)];
                        final String sy = fields[nfields - IOSTAT_CPU_FIELDS_NUM + cpuFields.get(IOSTAT_FIELD_CPU_SY)];
                        final String id = fields[nfields - IOSTAT_CPU_FIELDS_NUM + cpuFields.get(IOSTAT_FIELD_CPU_ID)];

                        vals.put(IHostCounters.CPU_PercentUserTime,
                                Double.parseDouble(us));

                        vals.put(IHostCounters.CPU_PercentSystemTime,
                                Double.parseDouble(sy));

                        // Note: NOT reported by iostat under OSX.
//							vals.put(IHostCounters.CPU_PercentIOWait,
//									Double.parseDouble(iowait));

                        vals.put(
                                IRequiredHostCounters.CPU_PercentProcessorTime,
                                (100d - Double.parseDouble(id)));


					}

					// Aggregate across all devices.
					double totalKBPerXFer = 0;
					double totalxferPerSec = 0;
					double totalMBPerSec = 0;
					for (int i = 0; i < ndevices; i++) {

						final int off = i * nfieldsPerDevice;

						final String kbPerXfer = fields[off + deviceFields.get(IOSTAT_FIELD_KB_T)]; // KB/t
						final String xferPerSec = fields[off + deviceFields.get(IOSTAT_FIELD_TPS)]; // tps
						final String mbPerSec = fields[off + deviceFields.get(IOSTAT_FIELD_MB_S)]; // MB/s

						final double _kbPerXFer = Double.parseDouble(kbPerXfer);
						final double _xferPerSec = Double
								.parseDouble(xferPerSec);
						final double _mbPerSec = Double.parseDouble(mbPerSec);

						totalKBPerXFer += _kbPerXFer;
						totalxferPerSec += _xferPerSec;
						totalMBPerSec += _mbPerSec;

						if (log.isInfoEnabled())
							log.info("\ntotalKBPerXfer=" + totalKBPerXFer
									+ ", totalXFerPerSec=" + totalxferPerSec
									+ ", totalMBPerSec=" + totalMBPerSec + "\n"
									+ header1 + "\n" + data);
	                    
						vals.put(IHostCounters.PhysicalDisk_TransfersPerSec,
								totalxferPerSec);

						vals.put(IHostCounters.PhysicalDisk_BytesPerSec,
								totalMBPerSec);

					}

                } catch (Exception ex) {

                    /*
                     * Issue warning for parsing problems.
                     */

                    log.warn(ex.getMessage() //
                            + "\nheader: " + header1 //
                            + "\n  data: " + data //
                    , ex);

                }

            } // while(true)

        } // readProcess()

    } // class IOStatReader

	private static void assertField(final int index, final String[] fields,
			final String expected) {
    	
		if (!expected.equals(fields[index]))
			throw new RuntimeException("Expecting '" + expected + "', found: '"
					+ fields[0] + "'");

	}

}
