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
import java.net.InetAddress;
import java.util.UUID;

import org.apache.system.SystemUtil;

/**
 * Interface for collecting, reporting, and decision-making based on node and
 * service utilization statistics.
 * 
 * @todo begin simply, with what we can and need to collect in order to identify
 *       an underutilized host (iostat + vmstat for memory) and service
 *       (pidstat) or just use sar.
 * 
 * @todo load monitoring Under windows:
 *       <p>
 *       http://www.javaworld.com/javaworld/jw-11-2004/jw-1108-windowspm.html
 *       <p>
 *       http://www.nagios.org/faqs/viewfaq.php?faq_id=32
 *       <p>
 *       http://snmpboy.msft.net/
 *       <p>
 *       http://www.google.com/search?hl=en&q=windows+performance+monitor+java
 *       <p>
 *       http://java.sun.com/performance/jvmstat/#Download
 * 
 * @todo SNMP
 *       <p>
 *       http://en.wikipedia.org/wiki/Simple_Network_Management_Protocol
 *       <p>
 *       http://www.snmp4j.org/
 * 
 * @todo performance testing links: Grinder, etc.
 *       <p>
 *       http://www.opensourcetesting.org/performance.php
 * 
 * iostat
 * 
 * <pre>
 * Linux 2.6.18-1.2798.fc6 (dp-aether1.dpp2.org)   03/03/2008
 * 
 * avg-cpu:  %user   %nice %system %iowait  %steal   %idle
 *            5.01    0.00    1.88    0.61    0.00   92.50
 * 
 * Device:            tps   Blk_read/s   Blk_wrtn/s   Blk_read   Blk_wrtn
 * sda              18.89       209.75       135.41  995990159  642992550
 * sdb               0.00         0.00         0.00       1272          0
 * dm-0             32.68       209.71       135.37  995771418  642807736
 * dm-1              0.01         0.05         0.04     215048     184776
 * </pre>
 * 
 * vmstat
 * 
 * <pre>
 * [root@dp-aether1 src]# vmstat
 * procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu------
 *  r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
 *  0  0  19088 1099996 210388 2130812    0    0    26    17    0    0  5  2 92  1  0
 * </pre>
 * 
 * pidstat -p 17324 (-d gives IO statistics with kernels 2.6.20 and up) 
 * <pre>
 * Linux 2.6.18-1.2798.fc6 (dp-aether1.dpp2.org)   03/03/2008
 * 
 * 09:00:13 AM       PID   %user %system    %CPU   CPU  Command
 * 09:00:13 AM     17324    0.01    0.00    0.02     2  java
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStatisticsService {

    /**
     * Register a service on a node.
     * 
     * @param nodeDesc
     * @param serviceUUID
     * @param serviceClass
     * 
     * @todo not necessary - the {@link IStatisticsService} should discover
     *       services and then collect statistics from those services.
     */
    public void register(NodeDescription nodeDesc, UUID serviceUUID, Class serviceClass);

    /**
     * Notify this service that you are alive.
     */
    public void heartbeat(NodeStats nodeStats, ServiceStats serviceStats);

    /**
     * Return the identifier of an under utilized service.
     * 
     * @todo pass the service type, get the service identifier.
     */
    public UUID[] getUnderUtilizedService(Class iface, int max) throws IOException;    

    public class NodeDescription {

        /** UUID assigned by the reporting service to the node. */
        public UUID nodeUUID;
        
        /*
         * CPU Info.
         */
        public int m_processors;
        public String m_cpuInfo;
        public String m_architecture;
        public String m_osName;
        public String m_osVersion;

        /*
         * Memory info.
         */
        public long memAvail;

        // bytes per disk, spindle speed, cache, etc.
        public long[] diskInfo;

        // @todo plus network E10/E100/E1000, full or half-duplex etc.
        public InetAddress[] inetAddr;

        public String hostname;
        public String description;
        
    }
    
    /**
     * Abstract base class for per-node and per-service statistics.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class Stats {
        
        /** The service identifier. */
        public UUID nodeUUID;
        /** Reporting interval (millis). */
        public long elapsed;
        /** Local time on the host. */
        public long localTime;
        /** % CPU utilization per CPU. */
        public int[] cpu;
        /** memory used and available (bytes). */
        public long memUsed, memAvail;
        /** % network IO (bytes read/written per interface). */
        public long[] nio;
        /** % disk IO (bytes read/written per interface, requests per interface, disk-queue length). */
        public int[] dio;
        /** Total disk space and free disk space (bytes per disk). */
        public long[] diskSpace, diskFree;

    }
    
    public class NodeStats extends Stats {

    }
    
    public class ServiceStats extends Stats {

        /** The service identifier. */
        public UUID serviceUUID;

        // interface or classname for service
        public Class serviceClass;
        
    }

    /**
     * Client gathers statistics and periodically reports them to the
     * {@link IStatisticsService}.
     * 
     * <pre>
     *   1. Start-up with local service.
     *   
     *   2. Discover IStatisticsService.
     *   
     *   2. Notify service of start of life cycle, sending {@link SystemUtil} data.
     *   
     *   2. Get pid.  Obtain per-process counters for cpu, mem, dio, nio.
     *   
     *   2. start thread, reporting counters every N milliseconds to service
     *   
     *   3. shutdown with local service, optionally notifying statistics service of end of life cycle.
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AbstractStatisticsClient {
        
    }
    
    public static class StatisticsClientForLinux extends AbstractStatisticsClient {
        
    }
    
    public static class StatisticsClientForWindows extends AbstractStatisticsClient {
        
    }
    
    /**
     * 
     */
    public static class StatisticsService implements IStatisticsService {

        public void register(NodeDescription nodeDesc, UUID serviceUUID, Class serviceClass) {
            
        }
        
        public void heartbeat(NodeStats nodeStats, ServiceStats serviceStats) {
            // TODO Auto-generated method stub
            
        }
        
        public UUID[] getUnderUtilizedService(Class iface, int max) throws IOException {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
}
