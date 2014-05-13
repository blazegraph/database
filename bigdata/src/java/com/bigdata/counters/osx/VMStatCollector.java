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

package com.bigdata.counters.osx;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
 * @version $Id: VMStatCollector.java 4289 2011-03-10 21:22:30Z thompsonbry $
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
        public void setValue(T value, long timestamp) {
           
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
    private final Map<String, Object> vals = new ConcurrentHashMap<String, Object>();

	/**
	 * The timestamp associated with the most recently collected values.
	 */
    private final AtomicLong lastModified = new AtomicLong(
            System.currentTimeMillis());

	/**
	 * The {@link Pattern} used to split apart the rows read from
	 * <code>vmstat</code>.
	 */
	final static Pattern pattern = Pattern.compile("\\s+");

    /**
     * 
     * @param interval
     *            The interval for the collected statistics.
     */
    public VMStatCollector(final int interval) {

        super(interval);
        
    }

    @Override
    public List<String> getCommand() {

		final List<String> command = new LinkedList<String>();

		command.add("/usr/bin/vm_stat");

		// Note: The configured interval in seconds between reports.
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
		 * Note: [pageins] is "the #of blocks swapped in".
		 */
		inst.add(new DI(IRequiredHostCounters.Memory_majorFaultsPerSecond));

		/*
		 * This is an aggregation of the counters for "active", "wire", and
		 * "spec".
		 */
		inst.add(new DI(IHostCounters.Memory_SwapBytesUsed));

		/*
		 * Note: [free] is "the total number of free pages in the system".
		 */
		inst.add(new DI(IHostCounters.Memory_Bytes_Free));

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
	 * Sample output for <code>vm_stat 60</code>, where <code>60</code> is the
	 * interval. Unlike the linux <code>vmstat</code>, there is no option to
	 * suppress the periodic repeat of the header. The header repeats in its
	 * entirety every "page" full.
	 * 
	 * <pre>
	 * Mach Virtual Memory Statistics: (page size of 4096 bytes, cache hits 0%)
	 *   free active   spec inactive   wire   faults     copy    0fill reactive  pageins  pageout
	 * 346061  1246K  51002   196922 190727 1004215K 19835511  525442K  7555575  2897558  2092534 
	 * 346423  1247K  50754   196922 189767     2707        0     1263        0        0        0 
	 * 343123  1247K  52180   198906 189767     3885        0     1754        0        0        0 
	 * 342474  1247K  52101   198978 190116    21739     2013     4651        0        0        0 
	 * 342542  1247K  52016   199319 189799     5792       17     2561        0        0        0
	 * </pre>
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
    private class VMStatReader extends ProcessReaderHelper {
        
        @Override
        protected ActiveProcess getActiveProcess() {
            
            if (activeProcess == null)
                throw new IllegalStateException();
            
            return activeProcess;
            
        }

        public VMStatReader() {

            super();
            
        }

        /**
         * 
         * @see TestParse_vm_stat#test_vmstat_header_and_data_parse()
         */
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

            // 1st header: we need the pageSize.
            final String h0;
            final int pageSize;
            {

                h0 = readLine();

                final String[] fields = pattern.split(h0.trim(), 0/* limit */);

				if (log.isInfoEnabled())
					log.info("header: " + h0);

				assertField(0, fields, "Mach");

				pageSize = Integer.valueOf(fields[7]);

				if (pageSize <= 0 || (pageSize % 512 != 0))
					throw new RuntimeException("pageSize=" + pageSize);
				
				if (log.isInfoEnabled())
					log.info("pageSize: " + pageSize);

			}

            // read 2nd header and verify expected fields.
            final String h1;
            {

                h1 = readLine();

                if (log.isInfoEnabled())
                    log.info("header: " + h1);

				final String[] fields = pattern
						.split(h1.trim(), 0/* limit */);

				assertField(0, fields, "free");
				assertField(9, fields, "pageins");

            }

			/*
			 * Note: Some fields are reported with a 'K' suffix. Some are in
			 * "pages". The pageSize was extracted from the header above.
			 */
        //  Mach Virtual Memory Statistics: (page size of 4096 bytes, cache hits 0%)
        //  free active   spec inactive   wire   faults     copy    0fill reactive  pageins  pageout
        //398649  1196K  47388   203418 185941  145234K  2036323 82012367  1353888   351301   149940 
        //400080  1197K  44784   205254 183886     1829        0     1046        0        0        0 

            // read lines until interrupted.
            long pageout_tm1 = 0;
            boolean first = true;
            while(true) {
                
                // read the next line of data.
            	final String data;
            	{
            		String s = readLine();
            		if(s.startsWith("Mach")) { // 1st header line.
            			s = readLine(); // 2nd header line.
            			s = readLine(); // data line.
            			if(log.isInfoEnabled())
            				log.info("Skipped headers.");
            		}
            		data = s;
            	}
                
                try {

                    // timestamp
                    lastModified.set(System.currentTimeMillis());

                    final String[] fields = pattern.split(data.trim(), 0/* limit */);
                    
                    final String free     = fields[0]; // free
                    final String active   = fields[1]; // in use and pageable
                    final String spec     = fields[2]; // speculative
//                    final String inactive = fields[3]; // 
                    final String wire     = fields[4]; // wired down
//                    final String faults   = fields[5]; // translation faults

                    final String pageins = fields[9]; // pageins
                    final String pageout = fields[10]; // pageout

					if (log.isInfoEnabled())
						log.info("\nfree=" + free + ", active=" + active
								+ ", spec=" + spec + ", wire=" + wire + ", si="
								+ pageins + ", so=" + pageout + "\n" + h1
								+ "\n" + data);

					{

						final double _free = parseDouble(free);
						final double _active = parseDouble(active);
						final double _spec = parseDouble(spec);
						final double _wire = parseDouble(wire);

						final double swapBytesUsed = _active + _spec + _wire;

						vals.put(IHostCounters.Memory_Bytes_Free, _free
								* pageSize);

						vals.put(IHostCounters.Memory_SwapBytesUsed,
								swapBytesUsed * pageSize);

					}

					/*
					 * pageout is reported as a total over time. we have to
					 * compute a delta, then divide through by the interval to
					 * get pages/second.
					 */
					{
						final double _pageout = parseDouble(pageout);

						if (!first) {

							final double delta = _pageout - pageout_tm1;

							final double majorPageFaultsPerSec = delta
									/ getInterval();

							vals.put(
									IRequiredHostCounters.Memory_majorFaultsPerSecond,
									majorPageFaultsPerSec);

						}
					}
					first = false;

                } catch (Exception ex) {

                    /*
                     * Issue warning for parsing problems.
                     */

                    log.warn(ex.getMessage() //
                            + "\nheader: " + h1 //
                            + "\n  data: " + data //
                    , ex);

                }

            } // while(true)

        } // readProcess()

    } // class VMStatReader

	/**
	 * Parse a string which may have a "K" suffix, returning a double. If the
	 * "K" suffix is present, then the returned value is scaled by 1000. This
	 * handles an OSX specific oddity for <code>vm_stat</code>
	 * 
	 * @param s
	 *            The string.
	 * 
	 * @return The (possibly scaled) value.
	 */
	private static double parseDouble(final String s) {

		final int pos = s.indexOf("K");

		if(pos == -1) {

			return Long.valueOf(s);

		}

		final long val = Long.valueOf(s.substring(0, pos));

		return val * 1000;
    	
    }
    
	private static void assertField(final int index, final String[] fields,
			final String expected) {
    	
		if (!expected.equals(fields[index]))
			throw new RuntimeException("Expecting '" + expected + "', found: '"
					+ fields[0] + "'");

	}

}
