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
package com.bigdata.rdf.sail.webapp.lbs.policy.counters;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;
import com.bigdata.rdf.sail.webapp.lbs.AbstractHostMetrics;

public class CounterSetHostMetricsWrapper extends AbstractHostMetrics {

    private final CounterSet counterSet;

    @Override
    public String toString() {

        return getClass().getName() + "{counters=" + counterSet + "}";

    }
    
    public CounterSetHostMetricsWrapper(final CounterSet counterSet) {

        if (counterSet == null)
            throw new IllegalArgumentException();
        
        this.counterSet = counterSet;
        
    }

    @Override
    public String[] getMetricNames() {

        final List<String> list = new LinkedList<String>();
        
        @SuppressWarnings("rawtypes")
        final Iterator<ICounter> itr = counterSet
                .getCounters(null/* filter */);

        while (itr.hasNext()) {
        
            final ICounter<?> c = itr.next();

            final String path = c.getPath();
            
            list.add(path);
            
        }

        return list.toArray(new String[list.size()]);

    }

    @SuppressWarnings("rawtypes")
    @Override
    public Number getNumeric(final String name) {

        if (name == null)
            throw new IllegalArgumentException();

        final ICounterNode c = counterSet.getPath(name);

        if (c == null) {
            // Not found.
            return null;
        }

        if (!c.isCounter()) {

            // Not a counter (an abstract node, not a leaf).
            return null;

        }

        return (Number) ((ICounter) c).getValue();

    }

}
