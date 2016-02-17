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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bigdata.counters.*;

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
     * A map from the name of a column to the order of that column in the output (origin ZERO).
     * This mapping was added when the format was changed to make the parser more robust to such
     * changes.
     * 
     * @see #1125 (OSX vm_stat output has changed)
     */
    private final Map<String,Integer> keys = new LinkedHashMap<String,Integer>();
    
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
    * For more recent OSX releases (e.g., Yosemite) there are now additional
    * columns:
    * 
    * <pre>
    * Mach Virtual Memory Statistics: (page size of 4096 bytes)
    *     free   active   specul inactive throttle    wired  prgable   faults     copy    0fill reactive   purged file-backed anonymous cmprssed cmprssor  dcomprs   comprs  pageins  pageout  swapins swapouts
    *    17656  1502912   110218  1329298        0   783121    32302 1122834K 22138464  897721K  4538117  2220029      382716   2559712  1093181   449012  1856773  4298532 67248050   263644   779970   886296
    *    41993  1479331   110174  1329290        0   782241    33940     2115        1     1316        0        0      382671   2536124  1093181   449012        0        0       24        0        0        0
    * </pre>
    * 
    * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
    *         Thompson</a>
    */
   protected class VMStatReader extends ProcessReaderHelper {

      @Override
      protected ActiveProcess getActiveProcess() {

         if (activeProcess == null)
            throw new IllegalStateException();

         return activeProcess;

      }

      public VMStatReader() {

         super();

      }

      private final Pattern pageSizePattern = Pattern.compile("size of (\\d+) ");

      /**
       * The index of the field associated with the "free" counter.
       */
      private final AtomicInteger INDEX_FREE = new AtomicInteger();
      /**
       * The index of the field associated with the "active" counter (in use and
       * pageable).
       */
      private AtomicInteger INDEX_ACTIVE = new AtomicInteger();
      /**
       * The index of the field associated with "spec" or "specul" (aka
       * speculative) counter.
       */
      private AtomicInteger INDEX_SPEC = new AtomicInteger();
      /**
       * The index of the field associated with "speculative" counter.
       */
      private AtomicInteger INDEX_WIRED = new AtomicInteger();
      /**
       * The index of the field associated with "pagein" counter.
       */
      private AtomicInteger INDEX_PAGEINS = new AtomicInteger();
      /**
       * The index of the field associated with "pageout" counter.
       */
      private AtomicInteger INDEX_PAGEOUT = new AtomicInteger();

      /**
       *
       * @see TestParse_vm_stat#test_vmstat_header_and_data_parse()
       */
      @Override
      protected void readProcess() throws Exception {

         if (log.isInfoEnabled())
            log.info("begin");

         for (int i = 0; i < 10 && !getActiveProcess().isAlive(); i++) {

            if (log.isInfoEnabled())
               log.info("waiting for the readerFuture to be set.");

            Thread.sleep(100/* ms */);

         }

         if (log.isInfoEnabled())
            log.info("running");

         // 1st header: we need the pageSize.
         final String h0;
         final int pageSize;
         {

            h0 = readLine();


            if (log.isInfoEnabled())
               log.info("header: " + h0);

            if (!h0.contains("Mach")) {
                throw new RuntimeException("Wrong input, expected hader: " + h0);
            }

            Matcher matcher = pageSizePattern.matcher(h0);
            if (matcher.find()) {
                pageSize = Integer.valueOf(matcher.group(1));
            } else {
                throw new RuntimeException("Failed to extract page size");
            }

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

            final String[] fields = pattern.split(h1.trim(), 0/* limit */);

            // Store mapping from field name to ordinal index (origin zero).
            for (int i = 0; i < fields.length; i++) {
               keys.put(fields[i], i);
            }

            // Check for field names that we use.
            getIndexIfDefined(INDEX_FREE, "free");
            getIndexIfDefined(INDEX_ACTIVE, "active");
            getIndexIfDefined(INDEX_SPEC, "spec", "specul");
            getIndexIfDefined(INDEX_WIRED, "wire", "wired");
            getIndexIfDefined(INDEX_PAGEINS, "pageins");
            getIndexIfDefined(INDEX_PAGEOUT, "pageout");

         }

         /*
          * Note: Some fields are reported with a 'K' suffix. Some are in
          * "pages". The pageSize was extracted from the header above.
          */
         // Mach Virtual Memory Statistics: (page size of 4096 bytes, cache hits
         // 0%)
         // free active spec inactive wire faults copy 0fill reactive pageins
         // pageout
         // 398649 1196K 47388 203418 185941 145234K 2036323 82012367 1353888
         // 351301 149940
         // 400080 1197K 44784 205254 183886 1829 0 1046 0 0 0

         // read lines until interrupted.
         long pageout_tm1 = 0;
         boolean first = true;
         while (true) {

            // read the next line of data.
            final String data;
            {
               String s = readLine();
               if (s.startsWith("Mach")) { // 1st header line.
                  s = readLine(); // 2nd header line.
                  s = readLine(); // data line.
                  if (log.isInfoEnabled())
                     log.info("Skipped headers.");
               }
               data = s;
            }

            try {

               // timestamp
               lastModified.set(System.currentTimeMillis());

               final String[] fields = pattern.split(data.trim(), 0/* limit */);

               final String free = fields[INDEX_FREE.get()]; // free
               final String active = fields[INDEX_ACTIVE.get()]; // in use and pageable
               final String spec = fields[INDEX_SPEC.get()]; // speculative
               // final String inactive = fields[3]; //
               final String wire = fields[INDEX_WIRED.get()]; // wired down
               // final String faults = fields[5]; // translation faults

               final String pageins = fields[INDEX_PAGEINS.get()]; // pageins
               final String pageout = fields[INDEX_PAGEOUT.get()]; // pageout

               if (log.isInfoEnabled())
                  log.info("\nfree=" + free + ", active=" + active + ", spec="
                        + spec + ", wire=" + wire + ", si=" + pageins + ", so="
                        + pageout + "\n" + h1 + "\n" + data);

               {

                  final double _free = parseDouble(free);
                  final double _active = parseDouble(active);
                  final double _spec = parseDouble(spec);
                  final double _wire = parseDouble(wire);

                  final double swapBytesUsed = _active + _spec + _wire;

                  vals.put(IHostCounters.Memory_Bytes_Free, _free * pageSize);

                  vals.put(IHostCounters.Memory_SwapBytesUsed, swapBytesUsed
                        * pageSize);

               }

               /*
                * pageout is reported as a total over time. we have to compute a
                * delta, then divide through by the interval to get
                * pages/second.
                */
               {
                  final double _pageout = parseDouble(pageout);

                  if (!first) {

                     final double delta = _pageout - pageout_tm1;

                     final double majorPageFaultsPerSec = delta / getInterval();

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
    * Parse a string which may have a "K" suffix, returning a double. If the "K"
    * suffix is present, then the returned value is scaled by 1000. This handles
    * an OSX specific oddity for <code>vm_stat</code>
    * 
    * @param s
    *           The string.
    * 
    * @return The (possibly scaled) value.
    */
   private static double parseDouble(final String s) {

      final int pos = s.indexOf("K");

      if (pos == -1) {

         return Long.valueOf(s);

      }

      final long val = Long.valueOf(s.substring(0, pos));

      return val * 1000;

   }

   private static void assertFieldByPosition(final int index,
         final String[] fields, final String expected) {

      if (!expected.equals(fields[index]))
         throw new RuntimeException("Expecting '" + expected + "', found: '"
               + fields[0] + "'");

   }

   /**
    * If the name of the performance counter appears in {@link #keys} then set
    * the index of that counter on the <i>indexOf</i> field.
    * 
    * @param indexOf
    *           The index of that performance counter in the table generated by
    *           the OS.
    * @param name
    *           The name(s) of the performance counter.
    */
   private void getIndexIfDefined(final AtomicInteger indexOf,
         final String... name) {
      Integer index = null;
      for (String s : name) {
         index = keys.get(s);
         if (index != null) {
            indexOf.set(index);
            return;
         }
      }
      throw new RuntimeException("Required performance counter not found: '"
            + Arrays.toString(name) + "'");
   }

}
