package com.bigdata.service.ndx.pipeline;

import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.ndx.pipeline.IndexWriteTask.IndexPartitionWriteTask;

/**
 * Statistics for the consumer.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexWriteStats {

    /**
     * The #of index partitions on which an {@link IndexPartitionWriteTask}
     * has been started.
     */
    public long indexPartitionStartCount = 0L;

    /**
     * The #of index partitions on which an {@link IndexPartitionWriteTask}
     * has been finished (either the buffer has been closed and all buffered
     * data has been flushed -or- the task was interrupted or otherwise
     * threw an exception).
     */
    public long indexPartitionDoneCount = 0L;

    /**
     * The #of {@link StaleLocatorException}s that were handled.
     */
    public long staleLocatorCount = 0L;

    /** The #of chunks written on the {@link BlockingBuffer} by the producer. */
    public long chunksIn = 0L;

    /**
     * The #of elements in the chunks written on the {@link BlockingBuffer}
     * by the producer.
     */
    public long elementsIn = 0L;
    
    /**
     * The #of elements in the output chunks written onto the index
     * partitions (not including any eliminated duplicates).
     */
    public long elementsOut = 0L;
    
    /**
     * The #of chunks written onto index partitions using RMI.
     */
    public long chunksOut = 0L;
    
    /**
     * Elapsed nanoseconds for RMI requests.
     */
    public long elapsedNanos = 0L;

    /**
     * Map for the per-index partition statistics. This ensures that we can
     * report the aggregate statistics in detail.
     */
    private final Int2ObjectLinkedOpenHashMap<IndexPartitionWriteStats> partitions;

    /**
     * Return the statistics object for the specified index partition and never
     * <code>null</code> (a new instance is created if none exists).
     * 
     * @param partitionId
     *            The index partition.
     * 
     * @return The statistics for that index partition.
     */
    public IndexPartitionWriteStats getStats(final int partitionId) {

        synchronized (partitions) {
         
            IndexPartitionWriteStats t = partitions.get(partitionId);

            if (t == null) {

                t = new IndexPartitionWriteStats();

                partitions.put(partitionId, t);

            }

            return t;

        }

    }

    /**
     * Return a snapshot of the statistics for each index partition.
     */
    synchronized public IndexPartitionWriteStats[] getStats() {

        return partitions.values().toArray(
                new IndexPartitionWriteStats[partitions.size()]);

    }

    public IndexWriteStats() {
        
        this.partitions = new Int2ObjectLinkedOpenHashMap<IndexPartitionWriteStats>();
        
    }
 
    /**
     * Return a {@link CounterSet} which may be used to report the statistics on
     * the index write operation. The {@link CounterSet} is NOT placed into any
     * namespace.
     */
    public CounterSet getCounterSet() {
        
        final CounterSet t = new CounterSet();
        
        t.addCounter("indexPartitionStartCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(indexPartitionStartCount);
            }
        });

        t.addCounter("indexPartitionDoneCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(indexPartitionDoneCount);
            }
        });

        t.addCounter("staleLocatorCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(staleLocatorCount);
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

        t.addCounter("elapsedNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedNanos);
            }
        });

        t.addCounter("averageNanos/write", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(getAverageNanosPerWrite());
            }
        });

        t.addCounter("averageElements/write", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue((chunksOut == 0L ? 0 : elementsOut
                        / (double) chunksOut));
            }
        });

        return t;

    }

    /**
     * The average #of nanoseconds per chunk written on the index partition.
     */
    public double getAverageNanosPerWrite() {

        return (chunksOut == 0L ? 0 : elapsedNanos / (double) chunksOut);

    }

    /**
     * The average #of elements (tuples) per chunk written on the index
     * partition.
     */
    public double getAverageElementsPerWrite() {

        return (chunksOut == 0L ? 0 : elementsOut / (double) chunksOut);

    }

    public String toString() {

        return getClass().getName() + "{indexPartitionStartCount="
                + indexPartitionStartCount + ", indexPartitionDoneCount="
                + indexPartitionDoneCount + ", staleLocatorCount="
                + staleLocatorCount + ", chunkIn=" + chunksIn + ", elementIn="
                + elementsIn + ", chunksOut=" + chunksOut + ", elementsOut="
                + elementsOut + ", elapsedNanos=" + elapsedNanos
                + ", averageNanos/write=" + getAverageNanosPerWrite()
                + ", averageElements/write=" + getAverageElementsPerWrite()
                + "}";

    }

}
