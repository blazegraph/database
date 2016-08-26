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
/*
 * Created on Feb 8, 2012
 */

package com.bigdata.bop.engine;

import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.Instrument;

/**
 * {@link QueryEngine} performance counters.
 * 
 * @author thompsonbry
 */
public class QueryEngineCounters implements ICounterSetAccess {

    /**
     * The #of queries which have been executed (set on completion).
     */
    protected final CAT queryStartCount = new CAT();

    /**
     * The #of queries which have been executed (set on completion).
     * <p>
     * Note: This counts both normal and abnormal query termination.
     */
    protected final CAT queryDoneCount = new CAT();

    /**
     * The #of instances of the query which terminated abnormally.
     */
    protected final CAT queryErrorCount = new CAT();

    /**
     * The total elapsed time (millis) for evaluation queries. This is the wall
     * clock time per query. The aggregated wall clock time per query will sum
     * to greater than the elapsed wall clock time in any interval where there
     * is more than one query running concurrently.
     */
    protected final CAT elapsedMillis = new CAT();

    /*
     * Lower level counters dealing with the work queues and executing chunk
     * tasks.
     */

    // /**
    // * The #of non-empty work queues.
    // */
    // final CAT workQueueCount = new CAT();

    /**
     * The #of work queues which are currently blocked.
     */
    protected final CAT blockedWorkQueueCount = new CAT();

    /**
     * The #of times that a work queue has blocked.
     */
    protected final CAT blockedWorkQueueRunningTotal = new CAT();

    /**
     * The total number of chunks of solutions currently buffered for the input
     * queues for operators on the query engine (regardless of whether the
     * native heap or the managed object heap is being used).
     * 
     * @see BLZG-533 (Vector query engine on native heap).
     */
    protected final CAT bufferedChunkMessageCount = new CAT();
    
    /**
     * The total number of bytes for solutions currently buffered on the
     * <strong>native heap</strong> for the input queues for operators on the
     * query engine.
     * 
     * @see BLZG-533 (Vector query engine on native heap).
     */
    protected final CAT bufferedChunkMessageBytesOnNativeHeap = new CAT();
    
    /**
     * The #of active operator evaluation tasks (chunk tasks).
     */
    protected final CAT operatorActiveCount = new CAT();

    /**
     * The #of operator evaluation tasks (chunk tasks) which have started.
     */
    protected final CAT operatorStartCount = new CAT();

    /**
     * The #of operator evaluation tasks (chunk tasks) which have ended.
     */
    protected final CAT operatorHaltCount = new CAT();

    /**
     * The size of the deadline queue.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/772">
     *      Query timeout only checked at operator start/stop. </a>
     */
    protected final CAT deadlineQueueSize = new CAT();

    @Override
    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        // #of queries started on this server.
        root.addCounter("queryStartCount", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(queryStartCount.get());
            }
        });

        // #of queries retired on this server.
        root.addCounter("queryDoneCount", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(queryDoneCount.get());
            }
        });

        // #of queries with abnormal termination on this server.
        root.addCounter("queryErrorCount", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(queryErrorCount.get());
            }
        });

        // average #of operator tasks evaluated per query
        root.addCounter("operatorTasksPerQuery", new Instrument<Double>() {
            @Override
            public void sample() {
                final long opCount = operatorHaltCount.get();
                final long n = queryDoneCount.get();
                final double d = n == 0 ? 0d : (opCount / (double) n);
                setValue(d);
            }
        });

        // #of queries retired per second on this server.
        root.addCounter("queriesPerSecond", new Instrument<Double>() {
            @Override
            public void sample() {
                final long ms = elapsedMillis.get();
                final long n = queryDoneCount.get();
                // compute throughput, normalized to q/s := (q*1000)/ms.
                final double d = ms == 0 ? 0d : ((1000d * n) / ms);
                setValue(d);
            }
        });

        // // #of non-empty work queues.
        // root.addCounter("workQueueCount", new Instrument<Long>() {
        // public void sample() {
        // setValue(workQueueCount.get());
        // }
        // });

        // #of blocked work queues.
        root.addCounter("blockedWorkQueueCount", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(blockedWorkQueueCount.get());
            }
        });

        // #of times that a work queue has blocked.
        root.addCounter("blockedWorkQueueRunningTotal", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(blockedWorkQueueRunningTotal.get());
            }
        });

        // Number of buffered IChunkMessages
        root.addCounter("bufferedChunkMessageCount", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(bufferedChunkMessageCount.get());
            }
        });

        // The number of bytes on the native (not managed) heap occupied by buffered IChunkMessages.
        root.addCounter("bufferedChunkMessageBytesOnNativeHeap", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(bufferedChunkMessageBytesOnNativeHeap.get());
            }
        });

        // #of concurrently executing operator tasks.
        root.addCounter("operatorActiveCount", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(operatorActiveCount.get());
            }
        });

        // #of operator evaluation tasks which have started.
        root.addCounter("operatorStartCount", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(operatorStartCount.get());
            }
        });

        // #of operator evaluation tasks which have ended.
        root.addCounter("operatorHaltCount", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(operatorHaltCount.get());
            }
        });

        // The size of the deadlineQueue.
        root.addCounter("deadlineQueueSize", new Instrument<Long>() {
            @Override
            public void sample() {
                setValue(deadlineQueueSize.get());
            }
        });

        return root;

    }

}
