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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
 * Collects statistics on the CPU utilization for the entire host using
 * <code>sar -u</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SarCpuUtilizationCollector extends AbstractProcessCollector
        implements ICounterHierarchy, IRequiredHostCounters, IHostCounters {

    static protected final Logger log = Logger
            .getLogger(SarCpuUtilizationCollector.class);

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

        // Note: Request the CPU stats.
        command.add("-u");
        
        // Note: configured interval.
        command.add(""+getInterval());
        
        // Note: count of zero means to repeat for ever.
        command.add("0");
        
        return command;
        
    }

    /**
     * Declares the counters that we will collect using <code>sar</code>.
     */
    synchronized public CounterSet getCounters() {
        
        if(root == null) {
        
            root = new CounterSet();
            
            inst = new LinkedList<I>();
            
            /*
             * Note: Counters are all declared as Double to facilitate
             * aggregation.
             * 
             * Note: sar reports percentages in [0:100] so we convert them to
             * [0:1] using a scaling factor.
             */

            inst.add(new DI(IRequiredHostCounters.CPU_PercentProcessorTime,.01d));
            
            inst.add(new DI(IHostCounters.CPU_PercentUserTime,.01d));
            inst.add(new DI(IHostCounters.CPU_PercentSystemTime,.01d));
            inst.add(new DI(IHostCounters.CPU_PercentIOWait,.01d));
            
            for(Iterator<I> itr = inst.iterator(); itr.hasNext(); ) {
                
                I i = itr.next();
                
                root.addCounter(i.getPath(), i);
                
            }
            
        }
        
        return root;
        
    }
    private List<I> inst = null;
    private CounterSet root = null;
   
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
            
            log.info("begin");

            for(int i=0; i<10 && !getActiveProcess().isAlive(); i++) {

                log.info("waiting for the readerFuture to be set.");

                Thread.sleep(100/*ms*/);
                
            }

            log.info("running");
            
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

                try {
                
//                *   04:14:45 PM     CPU     %user     %nice   %system   %iowait    %steal     %idle
//                *   04:14:46 PM     all      0.00      0.00      0.00      0.00      0.00    100.00

                {
                    final String s = data.substring(0, 11);
                    try {
                        
                        lastModified = f.parse(s).getTime();
                    } catch (Exception e) {
                        log.warn("Could not parse time: [" + s + "] : " + e);
                        // should be pretty close.
                        lastModified = System.currentTimeMillis();
                    }
                }
                                
                final String user = data.substring(20-1, 30-1);
//                final String nice = data.substring(30-1, 40-1);
                final String system = data.substring(40-1, 50-1);
                final String iowait = data.substring(50-1, 60-1);
//                final String steal = data.substring(60-1, 70-1);
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

            }
            
        }

    }
    
    /**
     * Used to parse the timestamp associated with each row of the [pidstat]
     * output.
     */
    protected final SimpleDateFormat f;
    {

        f = new SimpleDateFormat("hh:mm:ss aa");

        /*
         * Code may be enabled for a runtime test of the date format.
         */
        if (true) {

            System.err.println("Format: " + f.format(new Date()));

            try {

                System.err.println("Parsed: " + f.parse("06:35:15 AM"));
                
                System.err.println("Parsed: " + f.parse("02:08:24 PM"));
                
            } catch (ParseException e) {

                log.error("Could not parse?");

            }

        }
        
    }
    
}

