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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

import sun.awt.windows.ThemeReader;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.resources.StaleLocatorException;

/**
 * Abstract base class providing statistics for the {@link AbstractMasterTask}
 * and a factory for the statistics for the subtasks.
 * <p>
 * Note: Since there are concurrent threads which need to write on the counters
 * on this class the practice is to be <code>synchronized</code> on this
 * reference before you update those counters. Without this, the counters might
 * not update correctly as thing like <code>a += 2</code> may not produce the
 * correct result.
 * 
 * @param <L>
 *            The generic type of the key used to lookup subtasks in the internal
 *            map.
 * @param <HS>
 *            The generic type of the subtask statistics.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractMasterStats<L, HS extends AbstractSubtaskStats> {
    
    /**
     * The #of subtasks which have started.
     */
    public long subtaskStartCount = 0L;

    /**
     * The #of subtasks which have finished (either the buffer has been closed
     * and all buffered data has been flushed -or- the task was interrupted or
     * otherwise threw an exception).
     */
    public long subtaskEndCount = 0L;

    /**
     * The #of subtasks which were closed due to an idle timeout.
     */
    public long subtaskIdleTimeout = 0L;

    /**
     * The maximum #of distinct partitions for which the master has caused
     * subtasks to be created at any given time.
     */
    private int maximumPartitionCount = 0;

    /**
     * The maximum #of distinct partitions for which the master has caused
     * subtasks to be created at any given time.
     */
    public int getMaximumPartitionCount() {

        return maximumPartitionCount;
        
    }
    
    /**
     * The #of redirects ({@link StaleLocatorException}s) that were handled.
     */
    public long redirectCount = 0L;

    /**
     * Elapsed nanoseconds in
     * {@link AbstractMasterTask#handleRedirect(AbstractSubtask, Object[])}.
     */
    public long elapsedRedirectNanos = 0L;
    
    /**
     * The #of chunks drained from the {@link BlockingBuffer} by the
     * {@link AbstractMasterTask}.
     */
    public long chunksIn = 0L;

    /**
     * The #of elements drained from the {@link BlockingBuffer} by the
     * {@link AbstractMasterTask}.
     */
    public long elementsIn = 0L;

    /**
     * The #of elements in the output chunks written onto the index partitions
     * (not including any eliminated duplicates).
     */
    public long elementsOut = 0L;

    /**
     * The #of elements on the output sink queues. This is incremented when a
     * chunk of elements is transferred onto an output sink queue and
     * decremented when a chunk of elements is drained from an output sink
     * queue. It does not reflect the #of elements on the master queue, which
     * can be approximated as {@link #elementsTransferred}/
     * {@link #chunksTransferred} X the averageMasterQueueSize). Neither does it
     * include the #of elements in a prepared or outstanding write on an index
     * partition.
     */
    public long elementsOnSinkQueues = 0L;

    /**
     * The #of chunks transferred from the master to the sinks. Where there is
     * more than one index partition, there will be more than one sink and each
     * chunk written on the master will be divided among the sinks based on the
     * key-ranges of the tuples in the chunks.
     */
    public long chunksTransferred = 0L;

    /**
     * The #of elements transferred from the master to the sinks. Where there is
     * more than one index partition, there will be more than one sink and each
     * chunk written on the master will be divided among the sinks based on the
     * key-ranges of the tuples in the chunks.  This reduces the average chunk
     * size entering the sink accordingly.
     */
    public long elementsTransferred = 0L;
    
    /**
     * The #of chunks written onto index partitions using RMI.
     */
    public long chunksOut = 0L;

    /**
     * Elapsed nanoseconds the master spends offering a chunk for transfer to a
     * sink.
     */
    public long elapsedSinkOfferNanos = 0L;

    /**
     * Elapsed time across sinks waiting for another chunk to be ready so that
     * it can be written onto the index partition.
     */
    public long elapsedSinkChunkWaitingNanos = 0L;

    /**
     * Elapsed nanoseconds across sinks writing chunks on an index partition
     * (RMI requests).
     */
    public long elapsedSinkChunkWritingNanos = 0L;

    /**
     * Map for the per-index partition statistics. This ensures that we can
     * report the aggregate statistics in detail. A weak value hash map is used
     * so that the statistics for inactive index partitions can be discarded.
     * The backing hard reference queue is not used since the
     * {@link AbstractSubtask sink task}s are strongly held until they are
     * closed so we don't really need a backing hard reference queue at all in
     * this case.
     */
    private final ConcurrentWeakValueCache<L, HS> currentPartitionStats = new ConcurrentWeakValueCache<L, HS>(
            0/* queueCapacity */);

    /**
     * Weak value hash map of the active masters.
     */
    protected final ConcurrentWeakValueCache<Integer, AbstractMasterTask> masters = new ConcurrentWeakValueCache<Integer, AbstractMasterTask>(
            0/* queueCapacity */);

    /**
     * The #of master tasks which have been created for the index whose
     * asynchronous write statistics are reported on by this object.
     */
    public int masterCreateCount = 0;

    /**
     * The approximate #of active master tasks. This is based on a weak value
     * hash map.  The size of that map is reported.
     */
    public int getMasterActiveCount() {

        /*
         * Note: Map entries for cleared weak references are removed from the
         * map before the size of that map is reported.
         */
        return masters.size();

    }

    /**
     * A new master task declares itself to this statistics object using this
     * method. This allows the statistics object to report on the #of master
     * tasks, their queue sizes, and the sizes of their sink queues.
     * 
     * @param master
     */
    @SuppressWarnings("unchecked")
    void addMaster(final AbstractMasterTask master) {

        if (master == null)
            throw new IllegalArgumentException();
        
        if (masters.putIfAbsent(master.hashCode(), master) == null) {

            masterCreateCount++;
            
        }
        
    }
    
    /**
     * Return the statistics object for the specified index partition and never
     * <code>null</code> (a new instance is created if none exists).
     * 
     * @param locator
     *            The index partition.
     * 
     * @return The statistics for that index partition.
     */
    public HS getSubtaskStats(final L locator) {

        synchronized (currentPartitionStats) {
         
            HS t = currentPartitionStats.get(locator);

            if (t == null) {

                t = newSubtaskStats(locator);

                currentPartitionStats.put(locator, t);

                maximumPartitionCount = Math.max(maximumPartitionCount,
                        currentPartitionStats.size());
                
            }

            return t;

        }

    }

    /**
     * Factory for the subtask statistics.
     * 
     * @param locator
     *            The subtask key.
     *            
     * @return The statistics for the subtask.
     */
    protected abstract HS newSubtaskStats(L locator);

    /**
     * Return a snapshot of the statistics for each index partition.
     */
    public Map<L, HS> getSubtaskStats() {

        final Map<L, HS> m = new LinkedHashMap<L, HS>(currentPartitionStats
                .size());

        final Iterator<Map.Entry<L, WeakReference<HS>>> itr = currentPartitionStats
                .entryIterator();

        while (itr.hasNext()) {

            final Map.Entry<L, WeakReference<HS>> e = itr.next();

            final HS subtaskStats = e.getValue().get();

            if (subtaskStats == null) {

                // weak reference was cleared.
                continue;

            }

            m.put(e.getKey(), subtaskStats);

        }

        return m;

    }

    public AbstractMasterStats() {

    }
 
    /**
     * Return a {@link CounterSet} which may be used to report the statistics on
     * the index write operation. The {@link CounterSet} is NOT placed into any
     * namespace.
     */
    public CounterSet getCounterSet() {
        
        final CounterSet t = new CounterSet();
        
        t.addCounter("masterCreateCount", new Instrument<Integer>() {
            @Override
            protected void sample() {
                setValue(masterCreateCount);
            }
        });

        t.addCounter("masterActiveCount", new Instrument<Integer>() {
            @Override
            protected void sample() {
                setValue(getMasterActiveCount());
            }
        });

        t.addCounter("subtaskStartCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(subtaskStartCount);
            }
        });

        t.addCounter("subtaskEndCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(subtaskEndCount);
            }
        });
        
        t.addCounter("subtaskIdleTimeout", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(subtaskIdleTimeout);
            }
        });

        t.addCounter("maximumPartitionCount", new Instrument<Integer>() {
            @Override
            protected void sample() {
                setValue(maximumPartitionCount);
            }
        });

        t.addCounter("activePartitionCount", new Instrument<Integer>() {
            @Override
            protected void sample() {
                setValue(currentPartitionStats.size());
            }
        });

        t.addCounter("redirectCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(redirectCount);
            }
        });

        t.addCounter("elapsedRedirectNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedRedirectNanos);
            }
        });

        t.addCounter("chunksIn", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(chunksIn);
            }
        });

        t.addCounter("elementsIn", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elementsIn);
            }
        });

        t.addCounter("chunksOut", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(chunksOut);
            }
        });

        t.addCounter("elementsOut", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elementsOut);
            }
        });

        t.addCounter("chunksTransferred", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(chunksTransferred);
            }
        });

        t.addCounter("elementsTransferred", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elementsTransferred);
            }
        });
        
        t.addCounter("elapsedSinkOfferNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedSinkOfferNanos);
            }
        });
        
        t.addCounter("elapsedChunkWaitingNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedSinkChunkWaitingNanos);
            }
        });

        t.addCounter("elapsedChunkWritingNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedSinkChunkWritingNanos);
            }
        });

//        t.addCounter("averageMillisPerWait", new Instrument<Long>() {
//            @Override
//            protected void sample() {
//                setValue(TimeUnit.NANOSECONDS
//                        .toMillis((long) getAverageNanosPerWait()));
//            }
//        });
//
//        t.addCounter("averageMillisPerWrite", new Instrument<Long>() {
//            @Override
//            protected void sample() {
//                setValue(TimeUnit.NANOSECONDS
//                        .toMillis((long) getAverageNanosPerWrite()));
//            }
//        });
//
//        t.addCounter("averageElementsPerWrite", new Instrument<Double>() {
//            @Override
//            protected void sample() {
//                setValue(getAverageElementsPerWrite());
//            }
//        });

        return t;

    }

//    /**
//     * The average #of nanoseconds waiting for a chunk to become ready so that
//     * it can be written on an output sink.
//     */
//    public double getAverageNanosPerWait() {
//
//        return (chunksOut == 0L ? 0 : elapsedChunkWaitingNanos
//                / (double) chunksOut);
//
//    }
//
//    /**
//     * The average #of nanoseconds per chunk written on an output sink.
//     */
//    public double getAverageNanosPerWrite() {
//
//        return (chunksOut == 0L ? 0 : elapsedChunkWritingNanos
//                / (double) chunksOut);
//
//    }
//
//    /**
//     * The average #of elements (tuples) per chunk written on an output sink.
//     */
//    public double getAverageElementsPerWrite() {
//
//        return (chunksOut == 0L ? 0 : elementsOut / (double) chunksOut);
//
//    }

    public String toString() {

        return getClass().getName()
                + "{subtaskStartCount="
                + subtaskStartCount
                + ", subtaskEndCount="
                + subtaskEndCount
                + ", subtaskIdleTimeout="
                + subtaskIdleTimeout
                + ", partitionCount="
                + maximumPartitionCount
                + ", activePartitionCount="
                + (subtaskStartCount-subtaskEndCount)
                + ", redirectCount="
                + redirectCount
                + ", chunkIn="
                + chunksIn
                + ", elementIn="
                + elementsIn
                + ", chunksOut="
                + chunksOut
                + ", elementsOut="
                + elementsOut
                + ", elementsOnSinkQueues="
                + elementsOnSinkQueues
                + ", chunksTransferred="
                + chunksTransferred
                + ", elementsTransferred="
                + elementsTransferred
                + ", elapsedChunkWaitingNanos="
                + elapsedSinkChunkWaitingNanos
                + ", elapsedChunkWritingNanos="
                + elapsedSinkChunkWritingNanos
//                + ", averageMillisPerWait="
//                + TimeUnit.NANOSECONDS
//                        .toMillis((long) getAverageNanosPerWait())
//                + ", averageMillisPerWrite="
//                + TimeUnit.NANOSECONDS
//                        .toMillis((long) getAverageNanosPerWrite())
//                + ", averageElementsPerWrite=" + getAverageElementsPerWrite()
                + "}";

    }

}
