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
 * Created on Mar 2, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.AbstractStatisticsCollector.IRequiredHostCounters;
import com.bigdata.counters.AbstractStatisticsCollector.StatisticsCollectorForWindows;
import com.bigdata.journal.IJournal;

/**
 * Interface for collecting, reporting, and decision-making based on node and
 * service utilization statistics.
 * 
 * @todo A core set of variables to support decision-making is defined by
 *       {@link IRequiredHostCounters}. However, the decision-making logic
 *       itself should be pluggable so that people can reply on the data that
 *       they have for their platform(s) that seems to best support
 *       decision-making and can apply rules for their platforms, environment,
 *       and applications which provide the best overall QOS.
 * 
 * @see http://www.google.com/search?hl=en&q=load+balancing+jini
 * 
 * @todo begin simply, with what we can and need to collect in order to identify
 *       an underutilized host (iostat + vmstat for memory) and service
 *       (pidstat) or just use sar.
 * 
 * @todo load monitoring Under windows:
 *       <p>
 *       http://www.javaworld.com/javaworld/jw-11-2004/jw-1108-windowspm.html
 *       <p>
 *       <p>
 *       http://nsclient.ready2run.nl/ (GPL, installed as a Windows service,
 *       used by Nagios).
 *       <p>
 *       https://nsclient4j.dev.java.net/ (LGPL java interface to nsclient
 *       providing access to that service, so you need both this and nsclient to
 *       get windows performance counters into Java.)
 *       <p>
 *       http://www.nagios.org/faqs/viewfaq.php?faq_id=32
 *       <p>
 *       http://snmpboy.msft.net/
 *       <p>
 *       http://www.google.com/search?hl=en&q=windows+performance+monitor+java
 *       <p>
 *       http://java.sun.com/performance/jvmstat/#Download
 *       <p>
 *       typeperf can be run as a command line utility to report on counters.
 *       <p>
 *       http://www.scl.ameslab.gov/Projects/Rabbit/
 * 
 * @todo SNMP
 *       <p>
 *       http://en.wikipedia.org/wiki/Simple_Network_Management_Protocol
 *       <p>
 *       http://www.snmp4j.org/
 *       <p>
 *       http://sourceforge.net/projects/joesnmp/ (used by jboss)
 *       <p>
 *       http://net-snmp.sourceforge.net/ (Un*x and Windows SNMP support).
 * 
 * @todo {@link SystemUtil}
 * 
 * @todo {@link Runtime#availableProcessors()}, {@link Runtime#maxMemory()},
 *       etc.
 * 
 * @todo performance testing links: Grinder, etc.
 *       <p>
 *       http://www.opensourcetesting.org/performance.php
 * 
 * iostat
 * 
 * <pre>
 *         Linux 2.6.18-1.2798.fc6 (dp-aether1.dpp2.org)   03/03/2008
 *         
 *         avg-cpu:  %user   %nice %system %iowait  %steal   %idle
 *                    5.01    0.00    1.88    0.61    0.00   92.50
 *         
 *         Device:            tps   Blk_read/s   Blk_wrtn/s   Blk_read   Blk_wrtn
 *         sda              18.89       209.75       135.41  995990159  642992550
 *         sdb               0.00         0.00         0.00       1272          0
 *         dm-0             32.68       209.71       135.37  995771418  642807736
 *         dm-1              0.01         0.05         0.04     215048     184776
 * </pre>
 * 
 * vmstat
 * 
 * <pre>
 *         [root@dp-aether1 src]# vmstat
 *         procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu------
 *          r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 *          0  0  19088 1099996 210388 2130812    0    0    26    17    0    0  5  2 92  1  0
 * </pre>
 * 
 * pidstat -p 17324 1 (-u is cpi, -r is memory stats, -d gives IO statistics
 * with kernels 2.6.20 and up; number is the interval in seconds)
 * 
 * <pre>
 *         Linux 2.6.18-1.2798.fc6 (dp-aether1.dpp2.org)   03/03/2008
 *         
 *         09:00:13 AM       PID   %user %system    %CPU   CPU  Command
 *         09:00:13 AM     17324    0.01    0.00    0.02     2  java
 * </pre>
 * 
 * pidstat -r -p 9096 1
 * 
 * <pre>
 *   Linux 2.6.18-1.2798.fc6 (dp-aether3.dpp2.org)   03/12/2008
 *   
 *   04:04:13 PM       PID  minflt/s  majflt/s     VSZ    RSS   %MEM  Command
 *   04:04:14 PM      9096      0.00      0.00   11212   4888   0.13  sshd
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStatisticsService {

    /**
     * Summary metrics for a service. The reporting period is every 60 seconds.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ServiceStats implements Serializable {

        private static final long serialVersionUID = 6216334263256766294L;

        /** As reported by {@link InetAddress#getCanonicalHostName()}. */
        public String hostname;

        public UUID serviceUUID;
//        public Class serviceClass;
        
        public double requestsPerSec;
        
        public double diskBytesReadPerSec;
        public double diskBytesWrittenPerSec;
        
        /** As reported by {@link Runtime#totalMemory()}. */
        public double totalMemory;

        /** As reported by {@link Runtime#freeMemory()}. */
        public double freeMemory;

        /** As reported by {@link Runtime#maxMemory()}. */
        public double maxMemory;
        
        /**
         * De-serialization ctor.
         */
        public ServiceStats() {
            
        }
        
        /**
         * 
         * @param serviceUUID
         *            Unique service identifier.
         * @param requestsPerSec
         *            #of requests per second at the {@link IDataService} API.
         * @param diskBytesReadSec
         *            #of bytes read per second from the {@link IJournal} or
         *            {@link IndexSegmentFileStore}s for that
         *            {@link IDataService}.
         * @param diskBytesWrittenPerSec
         *            #of bytes written per second on the {@link IJournal} or
         *            {@link IndexSegmentFileStore}s for that
         *            {@link IDataService}.
         * 
         * The following are available from mpstat (linux). However we can't get
         * per-process data from Windows (it aggregates all counters for
         * processes having the same name).
         * @todo pageFaultsPerSec
         * @todo virtualProcessSize
         * @todo realProcessSize
         * @todo percentMem
         * 
         * @todo could report the PID. that might be of interest when trying to
         *       go backwards from the aggregated data to a specific host.
         */
        public ServiceStats(UUID serviceUUID, //
                double requestsPerSec,//
                double diskBytesReadSec, double diskBytesWrittenPerSec
                ) {
            
            try {
                this.hostname = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }

            this.serviceUUID = serviceUUID;
            
            this.requestsPerSec = requestsPerSec;
            
            this.diskBytesReadPerSec = diskBytesReadSec;
            this.diskBytesWrittenPerSec = diskBytesWrittenPerSec;
            
            this.totalMemory = Runtime.getRuntime().totalMemory();
            this.freeMemory = Runtime.getRuntime().freeMemory();
            this.maxMemory = Runtime.getRuntime().maxMemory();
            
        }
        
    }

    /**
     * Client gathers statistics and periodically reports them to the
     * {@link IStatisticsService}.
     * 
     * <pre>
     *                    1. Start-up with local service.
     *                    
     *                    2. Discover IStatisticsService.
     *                    
     *                    2. Notify service of start of life cycle, sending {@link SystemUtil} data.
     *                    
     *                    2. Get pid.  Obtain per-process counters for cpu, mem, dio, nio.
     *                    
     *                    2. start thread, reporting counters every N milliseconds to service
     *                    
     *                    3. shutdown with local service, optionally notifying statistics service of end of life cycle.
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo This needs to be refactored:
     *       <p>
     *       Some of the counters should come from the {@link IDataService}
     *       implementation itself (as a way of working around the difficulty in
     *       obtaining per-process counters for various platforms). Some aspects
     *       of these counters are not being collected right now, for example,
     *       the #of bytes read or written when generating {@link IndexSegment}s.
     *       Also, I need to instrument the {@link DataService} to report the
     *       #of requests. While requestsPerSec is not a very good proxy for the
     *       amount of work that is being done by the data service, it should be
     *       Ok in combination with the bytes read/written on the disk.
     *       <p>
     *       There needs to be a class heirarchy for collecting the data and
     *       getting it into the right shape that is platform specific. This
     *       needs to handle massaging the per-host data into a
     *       {@link HostStats} object.
     *       <p>
     *       There needs to be a class heirarchy for discovering the
     *       {@link IStatisticsService} and reporting data to that service so
     *       that we can have service framework implementations other than jini.
     *       <p>
     *       The general framework for counters is just named variables where
     *       those values are Strings and the client needs to interpret them.
     *       This is the SNMP view and the view of [typeperf] as well. In order
     *       to interpret the counters you need to know what they mean, whether
     *       they are averages, what the units are, etc. This approach would
     *       allow us to deliver more information when it was available and the
     *       information source would have to bear the burden of generating the
     *       required (minimum set of) "variables".
     * 
     * @todo Consider an SNMP adaptor for the clients so that they can report to
     *       SNMP aware applications. In this context we could report both the
     *       original counters (as averaged) and the massaged metrics on which
     *       we plan to make decisions.
     */
    public class StatisticsClient {
       
        AbstractStatisticsCollector collector;
        
        public StatisticsClient() {
            
            // @todo choose by OS.
            String osName = System.getProperty( "os.name" );
            String osVersion = System.getProperty( "os.version" );
            
//            if(host is windows) {

            collector = new StatisticsCollectorForWindows();
            
//            } else if (...) {
//                
//            }
            
        }
        
    }
    
    /**
     * Service collects statistics (averages of counters) from various hosts and
     * services, can report those statistics, and can identify over and under
     * utilized hosts and services based on the collected statistics.
     * 
     * @todo should i report the source counter names (they are 60 second
     *       average)? With aliases?  Or just map the actual source counters
     *       onto a set of required reporting metrics?
     */
    public static class StatisticsService implements IStatisticsService {

        static protected final Logger log = Logger.getLogger(StatisticsService.class);

//        public void notify(HostStats nodeStats) {
//            
//            log.info("nodeStats: "+nodeStats);
//            
//        }
//
//        public void notify(ServiceStats serviceStats) {
//
//            log.info("serviceStats: "+serviceStats);
//            
//        }

        /**
         * Return the identifier of an under utilized service.
         * 
         * @todo pass the service type, get the service identifier.
         */
        public UUID[] getUnderUtilizedService(Class iface, int max) throws IOException {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
}
