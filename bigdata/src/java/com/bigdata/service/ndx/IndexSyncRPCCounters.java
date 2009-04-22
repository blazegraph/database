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
 * Created on Apr 22, 2009
 */

package com.bigdata.service.ndx;

import java.util.concurrent.TimeUnit;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.util.concurrent.TaskCounters;

/**
 * Counters used for sync RPC on scale-out indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSyncRPCCounters extends TaskCounters {

    /**
     * 
     */
    public IndexSyncRPCCounters() {
    }

    /**
     * The #of redirects ({@link StaleLocatorException}s) that were handled.
     */
    public long redirectCount = 0L;

    /**
     * The #of requests which have been submitted. This counter is incremented
     * when the task is actually dispatched via RMI to the data service so it
     * will not reflect tasks which are in the client's queue.
     */
    public long requestCount = 0L;
    
    /**
     * The #of elements in a synchronous RPC request. Single point requests are
     * reported as ONE (1) element out. Key-range requests are NOT reported
     * under this counter (they correspond to iterators and range counts).
     * Key-array requests report the #of tuples in the request (they correspond
     * to batch read and write operations).
     */
    public long elementsOut = 0L;

    /**
     * #Of point requests issued (a single key).
     */
    public long pointRequestCount;

    /** #of key-range requests issued (generally these are range counts). */
    public long keyRangeRequestCount;

    /** #of key-array requests issued (batch read or write operations). */
    public long keyArrayRequestCount;

    /**
     * #of read-only requests.
     */
    public long readOnlyRequestCount;
    
    /**
     * The average #of nanoseconds per request.
     */
    public double getAverageNanosPerRequest() {

        return (requestCount == 0L ? 0 : serviceNanoTime.get()
                / (double) requestCount);

    }

    /**
     * The average #of elements (tuples) per request. Because
     * {@link #elementsOut} and {@link #requestCount} are incremented when the
     * request is submitted, this reflects both the completed requests and any
     * requests which might be outstanding.
     */
    public double getAverageElementsPerRequest() {

        return (requestCount == 0L ? 0 : elementsOut / (double) requestCount);

    }

    public CounterSet getCounters() {
        
        final CounterSet t = super.getCounters();
        
        t.addCounter("redirectCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(redirectCount);
            }
        });

        t.addCounter("requestCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(requestCount);
            }
        });

        t.addCounter("pointRequestCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(pointRequestCount);
            }
        });
        t.addCounter("keyRangeRequestCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(keyRangeRequestCount);
            }
        });

        t.addCounter("keyArrayRequestCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(keyArrayRequestCount);
            }
        });

        t.addCounter("readOnlyRequestCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(readOnlyRequestCount);
            }
        });

        t.addCounter("elementsOut", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elementsOut);
            }
        });

        t.addCounter("averageMillisPerRequest", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(TimeUnit.NANOSECONDS
                        .toMillis((long) getAverageNanosPerRequest()));
            }
        });

        t.addCounter("averageElementsPerWrite", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(getAverageElementsPerRequest());
            }
        });

        return t;
        
    }
    
}
