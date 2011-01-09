/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.rio;

import com.bigdata.counters.CAT;
import com.bigdata.rdf.inf.ClosureStats;

/**
 * Used to report statistics when loading data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LoadStats {

    public final CAT toldTriples = new CAT();
    public final CAT loadTime = new CAT();
    public final CAT commitTime = new CAT();
    public final CAT totalTime = new CAT();
    
    private transient volatile long lastReportTime = 0l;

    /**
     * The internal with which this class will log on {@link System#out} in
     * milliseconds (it is se to every 10 minutes). This helps to track progress
     * on very large data loads.
     */
    protected static transient long REPORT_INTERVAL = 10 * 60 * 1000; 
    
    /**
     * Used iff the closure is computed as the data are loaded.
     */
    public final ClosureStats closureStats = new ClosureStats();

    public long triplesPerSecond() {

        return ((long) (((double) toldTriples.estimate_get()) / ((double) totalTime.estimate_get()) * 1000d));

    }

    public void add(final LoadStats stats) {

        toldTriples.add(stats.toldTriples.get());

        loadTime.add(stats.loadTime.get());

        commitTime.add(stats.commitTime.get());

        totalTime.add(stats.totalTime.get());

        if (stats.closureStats != null) {

            closureStats.add(stats.closureStats);

        }

        /*
         * Handle incremental reporting for large data loads.
         */
        final long now = System.currentTimeMillis();

        if (lastReportTime == 0L) {

            if (loadTime.estimate_get() >= REPORT_INTERVAL) {

                System.out.println("loading: " + toString());

                lastReportTime = now;

            }

        } else {

            if ((now - lastReportTime) >= REPORT_INTERVAL) {

                System.out.println("loading: " + toString());

                lastReportTime = now;

            }

        }

    }
    
    /**
     * Human readable representation.
     */
    public String toString() {

        return toldTriples
                + " stmts added in "
                + ((double) loadTime.estimate_get())
                / 1000d
                + " secs, rate= "
                + triplesPerSecond()
                + ", commitLatency="
                + commitTime.estimate_get()
                + "ms"
                + (closureStats.elapsed.estimate_get()!=0L? "\n"+closureStats.toString() : "");

    }
    
}
