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

/**
 * Additional counters that hosts can report.
 * 
 * @todo pageFaultsPerSec (majflt/s)
 * 
 * @todo os diskCache (dis|en)abled
 * @todo #disks
 * @todo disk descriptions
 * @todo disk space, space avail, hardware disk cache (dis|en)abled.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IHostCounters extends IRequiredHostCounters {
    
    /*
     * Info
     */
    
    /** CPU family information. */
    String Info_ProcessorInfo = Info+ps+"Processor Info";

    /** The #of processors. */
    String Info_NumProcessors = Info+ps+"Number of Processors";
    
    /*
     * CPU
     */

    /**
     * Percentage of the time the processor is not idle that it is executing
     * at the user level (normalized to 100% in single CPU and SMP
     * environments).
     */
    String CPU_PercentUserTime = CPU + ps + "% User Time";

    /**
     * Percentage of the time the processor is not idle that it is executing
     * at the system (aka kernel) level (normalized to 100% in single CPU
     * and SMP environments).
     */
    String CPU_PercentSystemTime = CPU + ps + "% System Time";

    /**
     * Percentage of the time the CPU(s) were idle while the system had an
     * outstanding IO.
     * <p>
     * Note: The Windows platform does not appear to be able to report this
     * counter. If it did I would move this into the "required" category. The
     * LBS DOES use this for its decisions and has to use a faked value for
     * Windows.
     */
    String CPU_PercentIOWait = CPU + ps + "% IO Wait";

    /*
     * Memory
     */

    /**
     * The total amount of memory available to the host.
     * 
     * @todo not collected or windows or linux.
     */
    String Memory_Bytes_Available = Memory + ps + "Bytes available";

    /**
     * The #of bytes of idle memory (vmstat, /proc/meminfo/MemFree).
     * 
     * @todo not collected for windows.
     */
    String Memory_Bytes_Free = Memory + ps + "Bytes Free";

    /**
     * The #of bytes of swap space that are in available
     * (/proc/meminfo/SwapTotal).
     * 
     * @todo not collected for windows or linux.
     */
    String Memory_SwapBytesAvailable = Memory + ps + "Swap Bytes Available";

    /**
     * The #of bytes of swap space that are in use (vmstat or
     * /proc/meminfo/SwapFree).
     * 
     * @todo not collected for windows.
     */
    String Memory_SwapBytesUsed = Memory + ps + "Swap Bytes Used";

    /**
     * Faults that did not require loading a page from disk.
     * 
     * @see IRequiredHostCounters#Memory_majorFaultsPerSecond
     */
    String Memory_MinorFaultsPerSec = Memory + ps
            + "Minor Faults per Second";

    /*
     * PhysicalDisk
     */

    /** #of disk read operations per second. */
    String PhysicalDisk_ReadsPerSec = PhysicalDisk + ps
            + "Reads Per Second";

    /** #of disk write operations per second. */
    String PhysicalDisk_WritesPerSec = PhysicalDisk + ps
            + "Writes Per Second";

}
