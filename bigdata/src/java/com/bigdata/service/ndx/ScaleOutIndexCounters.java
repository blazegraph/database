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
 * Created on Apr 15, 2009
 */

package com.bigdata.service.ndx;

import com.bigdata.counters.CounterSet;
import com.bigdata.service.IScaleOutClientIndex;
import com.bigdata.service.ndx.pipeline.IndexWriteStats;
import com.bigdata.util.concurrent.TaskCounters;

/**
 * Counters pertaining to a scale-out index. The {@link IScaleOutClientIndex}
 * has two APIs. One is synchronous RPC. The other is an asynchronous stream
 * oriented API for writes. Both sets of performance counters are available from
 * this class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ScaleOutIndexCounters {

    public ScaleOutIndexCounters() {
        
    }

    /**
     * Return a new {@link CounterSet} reporting both the {@link TaskCounters}
     * and the {@link IndexWriteStats} for this index.
     */
    public CounterSet getCounters() {

        final CounterSet counterSet = new CounterSet();

        // attach the statistics for asynchronous (pipeline) writes.
        counterSet.makePath("synchronous").attach(
                synchronousCounters.getCounters());

        // attach the statistics for synchronous RPC (reads and writes).
        counterSet.makePath("asynchronous").attach(
                asynchronousStats.getCounterSet());

        return counterSet;
        
    }

    /**
     * These counters are used only for the asynchronous write pipeline.
     */
    final public IndexWriteStats asynchronousStats = new IndexWriteStats();

    /**
     * These counters are used for the synchronous RPC calls (reads and writes).
     */
    final public TaskCounters synchronousCounters = new TaskCounters();

    public String toString() {
        
        return getClass().getName() + "{asynchronous=" + asynchronousStats
                + ", synchronous=" + synchronousCounters + "}";
        
    }
    
}
