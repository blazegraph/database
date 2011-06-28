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
 * Counters defined on a per-process basis.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IProcessCounters extends ICounterHierarchy {

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

    /** Percentage of the time the processor is not idle. */
    String CPU_PercentProcessorTime = CPU + ps + "% Processor Time";

    /*
     * Memory
     */

    /**
     * Faults that did not require loading a page from disk.
     */
    String Memory_minorFaultsPerSec = Memory + ps
            + "Minor Faults per Second";

    /**
     * Faults which required loading a page from disk.
     */
    String Memory_majorFaultsPerSec = Memory + ps
            + "Major Faults per Second";

    /**
     * The virtual memory usage of the process in bytes.
     */
    String Memory_virtualSize = Memory + ps + "Virtual Size";

    /**
     * The non-swapped physical memory used by the process in bytes.
     */
    String Memory_residentSetSize = Memory + ps + "Resident Set Size";

    /**
     * The percentage of the phsyical memory used by the process.
     */
    String Memory_percentMemorySize = Memory + ps + "Percent Memory Size";

    /**
     * The value reported by {@link Runtime#maxMemory()} (the maximum amount
     * of memory that the JVM will attempt to use). This should be a
     * {@link OneShotInstrument}.
     */
    String Memory_runtimeMaxMemory = Memory + ps + "Runtime Max Memory";
    
    /**
     * The value reported by {@link Runtime#freeMemory()} (the amount of
     * free memory in the JVM)).
     */
    String Memory_runtimeFreeMemory = Memory + ps + "Runtime Free Memory";
    
    /**
     * The value reported by {@link Runtime#totalMemory()} (the amount of
     * total memory in the JVM, which may vary over time).
     */
    String Memory_runtimeTotalMemory = Memory + ps + "Runtime Total Memory";
    
    /*
     * IO
     */
    
    /**
     * The rate at which the process is reading data from disk in bytes per
     * second.
     */
    String PhysicalDisk_BytesReadPerSec = PhysicalDisk + ps
            + "Bytes Read per Second";

    /**
     * The rate at which the process is writing data on the disk in bytes
     * per second (cached writes may be reported in this quantity).
     */
    String PhysicalDisk_BytesWrittenPerSec = PhysicalDisk + ps
            + "Bytes Written per Second";

}
