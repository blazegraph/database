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

package com.bigdata.counters.win;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.counters.AbstractProcessCollector;
import com.bigdata.counters.AbstractProcessReader;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IHostCounters;
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.util.CSVReader;
import com.bigdata.util.CSVReader.Header;

/**
 * Collects per-host performance counters on a Windows platform using
 * <code>typeperf</code> and aligns them with those declared by
 * {@link IRequiredHostCounters}.
 * <p>
 * Note: The names of counters under Windows are NOT case-sensitive.
 * 
 * @todo declarative configuration for properties to be collected and their
 *       alignment with our counter hierarchy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TypeperfCollector extends AbstractProcessCollector {

    static protected final Logger log = Logger.getLogger(TypeperfCollector.class);

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

    /**
     * Updated each time a new row of data is read from the process and reported
     * as the last modified time for counters based on that process and
     * defaulted to the time that we begin to collect performance data.
     */
    private long lastModified = System.currentTimeMillis();

    /**
     * Map containing the current values for the configured counters. The keys
     * are paths into the {@link CounterSet}. The values are the data most
     * recently read from <code>typeperf</code>.
     * <p>
     * Note: The paths are in fact relative to how the counters are declared by
     * {@link #getCounters()}. Likewise {@link InstrumentForWPC#getValue()}
     * uses the paths declared within {@link #getCounters()} and not whatever
     * path the counters are eventually placed under within a larger hierarchy.
     */
    private Map<String,Object> vals = new HashMap<String, Object>();

    /**
     * @param interval
     */
    public TypeperfCollector(int interval) {

        super(interval);
        
    }

    /**
     * An {@link IInstrument} for a performance counter on a Windows platform.
     * Each declaration knows both the name of the performance counter on the
     * Windows platform and how to report that counter under the appropriate
     * name for our {@link ICounterSet}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class InstrumentForWPC implements IInstrument<Double> {

        private final String counterNameForWindows;

        private final String path;
        
        private final double scale;
        
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

        /**
         * Declare a performance counter for the Windows platform.
         * 
         * @param counterNameForWindows
         *            The name of the performance counter to be collected.
         * @param path
         *            The path of the corresponding {@link ICounter} under which
         *            the collected performance counter data will be reported.
         * @param scale
         *            The counter value will be multiplied through by the scale.
         *            This is used to adjust counters that are reporting in some
         *            other unit. E.g., KB/sec vs bytes/sec or percent in
         *            [0:100] vs percent in [0.0:1.0].
         */
        public InstrumentForWPC(String counterNameForWindows, String path, double scale) {

            if (counterNameForWindows == null)
                throw new IllegalArgumentException();

            if (path == null)
                throw new IllegalArgumentException();

            this.counterNameForWindows = counterNameForWindows;

            this.path = path;

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
     * Generate command to write performance counters on the console.
     * <p>
     * The sample interval format is -si [hh:[mm:]]ss.
     * <p>
     * Note: typeperf supports csv, tab, and bin output formats. However
     * specifying tsv (tab delimited) causes it to always write on a file so I
     * am using csv (comma delimited, which is the default in any case).
     * 
     * @todo While this approach seems fine to obtain the system wide counters
     *       under Windows it runs into problems when you attempt to obtain the
     *       process specific counters. The difficulty is that you can not use
     *       the PID when you request counters for a process and it appears that
     *       the counters for all processes having the same _name_ are
     *       aggregated, or at least can not be distinguished, using [typeperf].
     * 
     * @throws IOException
     */
    public List<String> getCommand() {

        // make sure that our counters have been declared.
        getCounters();

        final List<String> command = new LinkedList<String>();
        {

            command.add("typeperf");

            command.add("-si");
            command.add("" + getInterval());

            for (InstrumentForWPC decl : decls) {

                // counter names need to be double quoted for the command line.
                command.add("\"" + decl.getCounterNameForWindows() + "\"");

                log.info("Will collect: \""
                        + decl.getCounterNameForWindows() + "\" as "
                        + decl.getPath());

            }

        }

        return command;

    }

    public AbstractProcessReader getProcessReader() {

        return new ProcessReader();

    }

    /**
     * Class reads the CSV output of <code>typeperf</code>.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ProcessReader extends AbstractProcessReader {

        /**
         * Used to parse the timestamp associated with each row of the
         * [typeperf] output.
         */
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

            log.info("");
            
            try {

                // run
                read();

            } catch (InterruptedException e) {

                // Note: This is a normal exit.
                log.info("Interrupted - will terminate");

            } catch (Exception e) {

                // Unexpected error.
                log.fatal(e.getMessage(), e);

            }

            log.info("Terminated");

        }

        private void read() throws Exception {

            log.info("");

            long nsamples = 0;

            final LineNumberReader reader = new LineNumberReader(
                    new InputStreamReader(is));

            final CSVReader csvReader = new CSVReader(reader);

            /*
             * @todo This is a bit delicate.
             * 
             * [typedef] may not start instantly, so we may have to wait to read
             * the headers.
             * 
             * Each new row after the headers should appear at ~ [interval]
             * second intervals.
             * 
             * The logic for how tail gets realized (including the delay used)
             * should probably be moved inside of the CSVReader and the caller
             * just sets a flag.
             */
            csvReader.setTailDelayMillis(100/* ms */);

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
                        log.error(
                                "Could not parse: " + text, e);
                        return text;
                    }

                }
            });

            /*
             * replace other headers so that data are named by our counter
             * names.
             */
            {
                
                log.info("setting up headers.");

                int i = 1;

                for (final InstrumentForWPC decl : decls) {

                    String path = decl.getPath();
                    // String path = hostPathPrefix + decl.getPath();

                    log.info("setHeader[i=" + i
                            + "]=" + path);

                    csvReader.setHeader(i++, new Header(path));

                }

            }

            log.info("starting row reads");

            while (!Thread.currentThread().isInterrupted()
                    && csvReader.hasNext()) {

                final Map<String, Object> row = csvReader.next();

                final long timestamp = ((Date)row.get("Timestamp")).getTime();

                // update the sample time.
                TypeperfCollector.this.lastModified = timestamp;
                
                for (Map.Entry<String, Object> entry : row.entrySet()) {

                    final String path = entry.getKey();

                    if(path.equals("Timestamp")) continue;
                    
                    final String value = "" + entry.getValue();

                    log.debug(path + "=" + value);

                    // update the value from which the counter reads.
                    vals.put(path,
                            Double.parseDouble(value));

                }

                nsamples++;

            }
            
            log.info("done.");

        }

    }

    /**
     * Declares the performance counters to be collected from the Windows
     * platform.
     */
    public CounterSet getCounters() {

        if (root == null) {

            root = new CounterSet();
            
            final String p = ""; // @todo remove this variable.

            decls = Arrays
                    .asList(new InstrumentForWPC[] {

                new InstrumentForWPC(
                        "\\Memory\\Pages/Sec",
                        p + IRequiredHostCounters.Memory_majorFaultsPerSecond, 1d),

                new InstrumentForWPC(
                        "\\Processor(_Total)\\% Processor Time",
                        p + IRequiredHostCounters.CPU_PercentProcessorTime, .01d),

                new InstrumentForWPC(
                        "\\LogicalDisk(_Total)\\% Free Space",
                        p + IRequiredHostCounters.LogicalDisk_PercentFreeSpace, .01d),

                /*
                 * These are system wide counters for the network interface.
                 * There are also counters for the network queue length, packets
                 * discarded, and packet errors that might be interesting. (I
                 * can't find _Total versions declared for these counters so I
                 * am not including them but the counters for the specific
                 * interfaces could be enabled and then aggregated, eg:
                 * 
                 * \NetworkInterface(*)\Bytes Send/Sec
                 */
                // "\\Network Interface(_Total)\\Bytes
                // Received/Sec",
                // "\\Network Interface(_Total)\\Bytes Sent/Sec",
                // "\\Network Interface(_Total)\\Bytes Total/Sec",
                                
                /*
                 * System wide counters for DISK IO.
                 */
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\Avg. Disk Queue Length",
                        p + IRequiredHostCounters.PhysicalDisk
                                + ICounterSet.pathSeparator
                                + "Avg. Disk Queue Length", 1d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\% Idle Time",
                        p + IRequiredHostCounters.PhysicalDisk
                                + ICounterSet.pathSeparator
                                + "% Idle Time", .01d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\% Disk Time",
                        p + IRequiredHostCounters.PhysicalDisk
                                + ICounterSet.pathSeparator
                                + "% Disk Time", .01d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\% Disk Read Time",
                        p + IRequiredHostCounters.PhysicalDisk
                                + ICounterSet.pathSeparator
                                + "% Disk Read Time", .01d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\% Disk Write Time",
                        p + IRequiredHostCounters.PhysicalDisk
                                + ICounterSet.pathSeparator
                                + "% Disk Write Time", .01d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\Disk Read Bytes/Sec",
                        p+ IRequiredHostCounters.PhysicalDisk_BytesReadPerSec, 1d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\Disk Write Bytes/Sec",
                        p+ IRequiredHostCounters.PhysicalDisk_BytesWrittenPerSec, 1d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\Disk Reads/Sec",
                        p + IHostCounters.PhysicalDisk_ReadsPerSec, 1d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\Disk Writes/Sec",
                        p + IHostCounters.PhysicalDisk_WritesPerSec, 1d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\Avg. Disk Bytes/Read",
                        p + IRequiredHostCounters.PhysicalDisk
                                + ICounterSet.pathSeparator
                                + "Avg. Disk Bytes per Read", 1d),
                new InstrumentForWPC(
                        "\\PhysicalDisk(_Total)\\Avg. Disk Bytes/Write",
                        p + IRequiredHostCounters.PhysicalDisk
                                + ICounterSet.pathSeparator
                                + "Avg. Disk Bytes per Write", 1d),
                // "\\PhysicalDisk(_Total)\\Disk Writes/sec",
                // "\\PhysicalDisk(_Total)\\Avg. Disk Bytes/Read",
                // "\\PhysicalDisk(_Total)\\Avg. Disk Bytes/Write",

                });

            for (InstrumentForWPC inst : decls) {

                root.addCounter(inst.getPath(), inst);

            }

        }

        return root;

    }

    private CounterSet root;
    
    /**
     * List of performance counters that we will be collecting.
     * 
     * @see #getCounters(), which sets up this list.
     */
    protected List<InstrumentForWPC> decls = null;
    
}
