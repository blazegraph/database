/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import com.bigdata.rdf.sail.webapp.lbs.IHostMetrics;
import com.bigdata.rdf.sail.webapp.lbs.IHostScoringRule;

/**
 * This {@link IHostScoringRule} uses <code>load_one</code> to score and rank
 * the hosts. The <code>lode_one</code> metric is available on Linux and FreeBSD
 * platforms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a
 *      href="http://en.wikipedia.org/wiki/Load_%28computing%29#Unix-style_load_calculation"
 *      > Unix Style Load Calculation </a>
 */
public class LoadOneHostScoringRule implements IHostScoringRule {

    private static final String LOAD_ONE = "load_one";
    
    @Override
    public String[] getMetricNames() {
     
        return new String[]{LOAD_ONE};
        
    }
    
    @Override
    public double getScore(final IHostMetrics metrics) {

        /**
         * Note: A fallback value of 1.0 implies that the system is at 100%
         * load.
         */
        return metrics.getNumeric(LOAD_ONE, .75d);

    }

}
