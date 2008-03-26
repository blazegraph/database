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
 * Various namespaces for per-host and per-process counters.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ICounterHierarchy {

    /**
     * The path separator string.
     * 
     * @see ICounterSet#pathSeparator
     */
    String ps = ICounterSet.pathSeparator;
    
    /**
     * The namespace for counters describing the host platform. These are
     * essentially "unchanging" counters.
     */
    String Info = "Info";
    
    /**
     * The namespace for counters dealing with processor(s) (CPU).
     */
    String CPU = "CPU";
    
    /**
     * The namespace for counters dealing with memory (RAM).
     */
    String Memory = "Memory";

    /**
     * The namespace for counters dealing with logical aggregations of disk.
     */
    String LogicalDisk = "LogicalDisk";

    /**
     * The namespace for counters dealing with physical disks.
     */
    String PhysicalDisk = "PhysicalDisk";

}
