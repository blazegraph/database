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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
     * The #of distinct partitions for which the master has caused subtasks to
     * be created. You can compute the #of active subtasks as
     * {@link #subtaskStartCount} - {@link #subtaskEndCount}, which is also
     * reported by {@link #getCounterSet()}.
     * 
     * @todo if {@link #partitions} is made into a weak value hash map then we
     *       need to change how this is maintained and add another counter which
     *       is the maximum #of partitions at any given time (high tide).
     */
    public int partitionCount = 0;
    
    /**
     * The #of redirects ({@link StaleLocatorException}s) that were handled.
     */
    public long redirectCount = 0L;

    /**
     * The #of chunks written on the {@link BlockingBuffer} by the producer.
     */
    public long chunksIn = 0L;

    /**
     * The #of elements in the chunks written on the {@link BlockingBuffer} by
     * the producer.
     */
    public long elementsIn = 0L;

    /**
     * The #of elements in the output chunks written onto the index partitions
     * (not including any eliminated duplicates).
     */
    public long elementsOut = 0L;

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
    public long elapsedChunkWaitingNanos = 0L;

    /**
     * Elapsed nanoseconds across sinks writing chunks on an index partition
     * (RMI requests).
     */
    public long elapsedChunkWritingNanos = 0L;

    /**
     * Map for the per-index partition statistics. This ensures that we can
     * report the aggregate statistics in detail.
     */
    private final Map<L, HS> partitions = new LinkedHashMap<L, HS>();

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

        synchronized (partitions) {
         
            HS t = partitions.get(locator);

            if (t == null) {

                t = newSubtaskStats(locator);

                partitions.put(locator, t);

                partitionCount++;
                
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
    public Map<L,HS> getSubtaskStats() {

        synchronized (partitions) {

            return new LinkedHashMap<L,HS>(partitions);
            
        }

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

        t.addCounter("partitionCount", new Instrument<Integer>() {
            @Override
            protected void sample() {
                setValue(partitionCount);
            }
        });

        /**
         * @todo this does not work and will report small negative as well as
         *       positive values for some reason that I have not determined.
         */
        t.addCounter("activePartitionCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(subtaskStartCount - subtaskEndCount);
            }
        });

        t.addCounter("redirectCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(redirectCount);
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
                setValue(elapsedChunkWaitingNanos);
            }
        });

        t.addCounter("elapsedChunkWritingNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedChunkWritingNanos);
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
                + partitionCount
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
                + ", chunksTransferred="
                + chunksTransferred
                + ", elementsTransferred="
                + elementsTransferred
                + ", elapsedChunkWaitingNanos="
                + elapsedChunkWaitingNanos
                + ", elapsedChunkWritingNanos="
                + elapsedChunkWritingNanos
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
