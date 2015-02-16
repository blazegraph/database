/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.sail.webapp.lbs.policy.ganglia;

import com.bigdata.ganglia.IGangliaMetricMessage;
import com.bigdata.ganglia.IHostReport;
import com.bigdata.rdf.sail.webapp.lbs.AbstractHostMetrics;
import com.bigdata.rdf.sail.webapp.lbs.IHostMetrics;

/**
 * Wraps an {@link IHostReport} as an {@link IHostMetrics} object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GangliaHostMetricWrapper extends AbstractHostMetrics {

    private final IHostReport hostReport;
    
    @Override
    public String toString() {

        return getClass().getName() + "{hostReport=" + hostReport.toString()
                + "}";

    }
    
    public GangliaHostMetricWrapper(final IHostReport hostReport) {

        if (hostReport == null)
            throw new IllegalArgumentException();
        
        this.hostReport = hostReport;
        
    }

    @Override
    public String[] getMetricNames() {

        return hostReport.getMetrics().keySet().toArray(new String[] {});

    }

    @Override
    public Number getNumeric(final String name) {

        if(name == null)
            throw new IllegalArgumentException();

        final IGangliaMetricMessage msg = hostReport.getMetrics().get(name);

        if (msg == null) {

            // Not found.
            return null;

        }

        return msg.getNumericValue();

    }

}
