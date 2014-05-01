/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp.lbs.policy.ganglia;

import java.util.Map;

import com.bigdata.ganglia.IGangliaMetricMessage;
import com.bigdata.ganglia.IHostReport;
import com.bigdata.journal.GangliaPlugIn;

/**
 * Best effort computation of a workload score based on CPU Utilization and IO
 * Wait defined as follows:
 * 
 * <pre>
 * (1d + cpu_wio * 100d) / (1d + cpu_idle)
 * </pre>
 * <p>
 * Note: Not all platforms report all metrics. For example, OSX does not report
 * IO Wait, which is a key metric for the workload of a database. If a metric is
 * not available for a host, then a fallback value is used.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see GangliaLBSPolicy.InitParams#REPORT_ON
 * 
 *      FIXME GC time is a per-JVM metric that should be incorporated into our
 *      default scoring strartegy. It will only get reported by the
 *      {@link GangliaPlugIn} if it is setup to self-report that data. And it
 *      may not report it correctly if there is more than one
 *      {@link HAJournalService} per host. It is also available from the
 *      <code>/bigdata/counters</code> and could be exposed as a JMX MBean.
 */
public class DefaultHostScoringRule implements IHostScoringRule {

    @Override
    public double getScore(final IHostReport hostReport) {

        final Map<String, IGangliaMetricMessage> metrics = hostReport
                .getMetrics();

        final double cpu_idle;
        {

            final IGangliaMetricMessage m = metrics.get("cpu_idle");

            if (m != null)
                cpu_idle = m.getNumericValue().doubleValue();
            else
                cpu_idle = .5d;

        }

        final double cpu_wio;
        {

            final IGangliaMetricMessage m = metrics.get("cpu_wio");

            if (m != null)
                cpu_wio = m.getNumericValue().doubleValue();
            else
                cpu_wio = .05d;

        }

        final double hostScore = (1d + cpu_wio * 100d) / (1d + cpu_idle);

        return hostScore;

    }

}
