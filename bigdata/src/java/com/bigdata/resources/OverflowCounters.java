/*

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Mar 18, 2010
 */
package com.bigdata.resources;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.resources.OverflowManager.IIndexPartitionTaskCounters;
import com.bigdata.resources.OverflowManager.IOverflowManagerCounters;
import com.bigdata.resources.OverflowManager.Options;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;

/**
 * Utility class exposes some counter values while protecting the actual
 * counters on the service from modification.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class OverflowCounters implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -7374339581752061839L;

    /**
     * #of synchronous overflows that have taken place. This counter is
     * incremented each time the synchronous overflow operation is complete.
     */
    public final AtomicLong synchronousOverflowCounter = new AtomicLong(0L);

    /**
     * The elapsed milliseconds for synchronous overflow processing to date.
     */
    public final AtomicLong synchronousOverflowMillis = new AtomicLong(0L);

    /**
     * The time in milliseconds since the epoch at which the most recent
     * asynchronous overflow started. This is used to compute the elapsed
     * asynchronous overflow time when the service is currently performing
     * asynchronous overflow processing. The value is set each time asynchronous
     * overflow processing starts, but never cleared.
     */
    public final AtomicLong asynchronousOverflowStartMillis = new AtomicLong(0L);

    /**
     * #of asynchronous overflows that have taken place. This counter is
     * incremented each time the entire overflow operation is complete,
     * including any asynchronous post-processing of the old journal.
     */
    public final AtomicLong asynchronousOverflowCounter = new AtomicLong(0L);

    /**
     * The elapsed milliseconds for asynchronous overflow processing to date.
     */
    public final AtomicLong asynchronousOverflowMillis = new AtomicLong(0L);

    /**
     * The #of asynchronous overflow operations which fail.
     * 
     * @see AsynchronousOverflowTask
     */
    public final AtomicLong asynchronousOverflowFailedCounter = new AtomicLong(
            0L);

    /**
     * The #of asynchronous overflow tasks (index partition splits, joins, or
     * moves) that failed.
     */
    public final AtomicLong asynchronousOverflowTaskFailedCounter = new AtomicLong(
            0L);

    /**
     * The #of asynchronous overflow tasks (index partition splits, joins, or
     * moves) that were canceled due to timeout.
     * 
     * @see Options#OVERFLOW_TIMEOUT
     */
    public final AtomicLong asynchronousOverflowTaskCancelledCounter = new AtomicLong(
            0L);

    /**
     * #of successful index partition incremental build operations.
     */
    public final AtomicLong indexPartitionBuildCounter = new AtomicLong(0L);

    /**
     * #of successful index partition compacting merge operations.
     */
    public final AtomicLong indexPartitionMergeCounter = new AtomicLong(0L);

    /**
     * #of successful index partition split operations.
     */
    public final AtomicLong indexPartitionSplitCounter = new AtomicLong(0L);

    /**
     * #of successful index partition tail split operations.
     */
    public final AtomicLong indexPartitionTailSplitCounter = new AtomicLong(0L);

    /**
     * #of successful index partition join operations.
     */
    public final AtomicLong indexPartitionJoinCounter = new AtomicLong(0L);

    /**
     * #of successful index partition move operations.
     */
    public final AtomicLong indexPartitionMoveCounter = new AtomicLong(0L);

    /**
     * #of successful index partition move operations where this service was the
     * target of the move (it received the index partition).
     */
    public final AtomicLong indexPartitionReceiveCounter = new AtomicLong(0L);

    /**
     * Constructor with zeros for the counter values.
     */
    public OverflowCounters() {

    }

    @Override
    public OverflowCounters clone() {

        final OverflowCounters tmp = new OverflowCounters();

        tmp.add(this);

        return tmp;

    }

    public void add(final OverflowCounters c) {

        if (c == null)
            throw new IllegalArgumentException();

        synchronousOverflowCounter
                .addAndGet(c.synchronousOverflowCounter.get());

        synchronousOverflowMillis.addAndGet(c.synchronousOverflowMillis.get());

        asynchronousOverflowStartMillis
                .addAndGet(c.asynchronousOverflowStartMillis.get());

        asynchronousOverflowCounter.addAndGet(c.asynchronousOverflowCounter
                .get());

        asynchronousOverflowMillis
                .addAndGet(c.asynchronousOverflowMillis.get());

        asynchronousOverflowFailedCounter
                .addAndGet(c.asynchronousOverflowFailedCounter.get());

        asynchronousOverflowTaskFailedCounter
                .addAndGet(c.asynchronousOverflowTaskFailedCounter.get());

        asynchronousOverflowTaskCancelledCounter
                .addAndGet(c.asynchronousOverflowTaskCancelledCounter.get());

        indexPartitionBuildCounter
                .addAndGet(c.indexPartitionBuildCounter.get());

        indexPartitionMergeCounter
                .addAndGet(c.indexPartitionMergeCounter.get());

        indexPartitionSplitCounter
                .addAndGet(c.indexPartitionSplitCounter.get());

        indexPartitionTailSplitCounter
                .addAndGet(c.indexPartitionTailSplitCounter.get());

        indexPartitionJoinCounter.addAndGet(c.indexPartitionJoinCounter.get());

        indexPartitionMoveCounter.addAndGet(c.indexPartitionMoveCounter.get());

        indexPartitionReceiveCounter.addAndGet(c.indexPartitionReceiveCounter
                .get());

    }

    public CounterSet getCounters() {

        final CounterSet tmp = new CounterSet();

        tmp.addCounter(IOverflowManagerCounters.SynchronousOverflowMillis,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(synchronousOverflowMillis.get());
                    }
                });

        tmp.addCounter(IOverflowManagerCounters.AsynchronousOverflowCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(asynchronousOverflowCounter.get());
                    }
                });

        tmp.addCounter(
                IOverflowManagerCounters.AsynchronousOverflowFailedCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(asynchronousOverflowFailedCounter.get());
                    }
                });

        tmp.addCounter(
                IOverflowManagerCounters.AsynchronousOverflowTaskFailedCount,
                new Instrument<Long>() {
                    public void sample() {
                        setValue(asynchronousOverflowTaskFailedCounter.get());
                    }
                });

        tmp
                .addCounter(
                        IOverflowManagerCounters.AsynchronousOverflowTaskCancelledCount,
                        new Instrument<Long>() {
                            public void sample() {
                                setValue(asynchronousOverflowTaskCancelledCounter
                                        .get());
                            }
                        });

        {

            final CounterSet tmp2 = tmp
                    .makePath(IResourceManagerCounters.IndexPartitionTasks);

            tmp2.addCounter(IIndexPartitionTaskCounters.BuildCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(indexPartitionBuildCounter.get());
                        }
                    });

            tmp2.addCounter(IIndexPartitionTaskCounters.MergeCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(indexPartitionMergeCounter.get());
                        }
                    });

            tmp2.addCounter(IIndexPartitionTaskCounters.SplitCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(indexPartitionSplitCounter.get());
                        }
                    });

            tmp2.addCounter(IIndexPartitionTaskCounters.TailSplitCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(indexPartitionTailSplitCounter.get());
                        }
                    });

            tmp2.addCounter(IIndexPartitionTaskCounters.JoinCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(indexPartitionJoinCounter.get());
                        }
                    });

            tmp2.addCounter(IIndexPartitionTaskCounters.MoveCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(indexPartitionMoveCounter.get());
                        }
                    });

            tmp2.addCounter(IIndexPartitionTaskCounters.ReceiveCount,
                    new Instrument<Long>() {
                        public void sample() {
                            setValue(indexPartitionReceiveCounter.get());
                        }
                    });

        }

        return tmp;

    }

}
