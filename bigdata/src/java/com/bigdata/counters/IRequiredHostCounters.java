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

package com.bigdata.counters;

import java.net.InetAddress;

/**
 * The set of core (required) counters that must be reported for all
 * platforms. The items declared on this interface are relative path names
 * for {@link ICounterSet}s and {@link ICounter}s. The root for the path
 * is generally the fully qualified domain name of a host (as reported by
 * {@link InetAddress#getCanonicalHostName()}, a federation, or a service.
 * <p>
 * Note: it is good practice to keep these three namespaces distinct so that
 * you can aggregate counters readily without these different contexts.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRequiredHostCounters extends ICounterHierarchy {

    /*
     * INFO
     */
    
    /**
     * The name of the operating system running on the platform as reported
     * by {@link System#getProperty(String)} for the <code>os.name</code>
     * property.
     */
    String Info_OperatingSystemName = Info + ps
            + "Operating System Name";

    /**
     * The version of the operating system running on the platform as
     * reported by {@link System#getProperty(String)} for the
     * <code>os.version</code> property.
     */
    String Info_OperatingSystemVersion = Info + ps
            + "Operating System Version";

    /**
     * System architecture as reported by {@link System#getProperty(String)}
     * for the <code>os.arch</code> property.
     */
    String Info_Architecture = Info + ps + "Architecture";

    /*
     * CPU
     */
    
    /** Percentage of the time the processor is not idle. */
    String CPU_PercentProcessorTime = CPU + ps
            + "% Processor Time";

    /*
     * Memory
     */
    
    /**
     * Faults which required loading a page from disk.
     */
    String Memory_majorFaultsPerSecond = Memory + ps
            + "Major Page Faults Per Second";

    /*
     * LogicalDisk
     */
    
    /**
     * Percentage of the disk space that is free (unused) [0.0:1.0].
     * 
     * @todo This should only be monitoring local disk since NAS will typically
     *       be shared across a cluster and hence of its space remaining will be
     *       of little use to the LBS.
     *       <p>
     *       It will probably require platform specific configuration to select
     *       only the appropriate devices (which would also address the above
     *       concern).
     * 
     * @todo not collected under linux.
     */
    String LogicalDisk_PercentFreeSpace = LogicalDisk + ps + "% Free Space";

    /*
     * PhysicalDisk
     */
    
    /** Disk bytes read per second for the host (vmstat). */
    String PhysicalDisk_BytesReadPerSec = PhysicalDisk + ps
            + "Bytes Read Per Second";

    /** Disk bytes written per second for the host (vmstat). */
    String PhysicalDisk_BytesWrittenPerSec = PhysicalDisk + ps
            + "Bytes Written Per Second";

};
