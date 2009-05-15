package com.bigdata.service.ndx.pipeline;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.service.AbstractFederation;
import com.bigdata.util.concurrent.MovingAverageTask;

/**
 * Statistics for the consumer, including several moving averages based on
 * sampled data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo average time the master waits for a chunk to transfer (I believe that
 *       this is deliberately a short timeout);
 * 
 * @todo average time to handle a redirect? That might be an indication of a
 *       stall during a redirect.
 * 
 * @todo average master queue length
 * 
 * @todo average sink queue length
 */
public class IndexAsyncWriteStats<L, HS extends IndexPartitionWriteStats> extends
        AbstractMasterStats<L, HS> {

    /**
     * The #of duplicates which were filtered out.
     */
    public long duplicateCount = 0L;

    /**
     * Task that will convert sampled data into moving averages.
     */
    private final StatisticsTask statisticsTask = new StatisticsTask();
    
    public IndexAsyncWriteStats(final AbstractFederation<?> fed) {

        /*
         * Add a scheduled task that will sample various counters of interest
         * and convert them into moving averages.
         */

        fed.addScheduledTask(statisticsTask, 1000/* initialDelay */,
                1000/* delay */, TimeUnit.MILLISECONDS);

    }

    /**
     * Task samples various counters of interest and convert them into moving
     * averages.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private class StatisticsTask implements Runnable {

        /*
         * Note: this does not work for two reasons. First, it does not account
         * for eliminated duplicates. Second, redirects can cause
         * double-counting of elements since they are written more than once -
         * one to the original index partition and then once to the redirected
         * index partition.
         */
//        /**
//         * The moving average of the #of buffered elements across the master and
//         * the sinks. 
//         */
//        final MovingAverageTask averageBufferedElements = new MovingAverageTask(
//                "averageBufferedElements", new Callable<Long>() {
//                    public Long call() {
//                        final IndexAsyncWriteStats stats = IndexAsyncWriteStats.this;
//                        final long delta;
//                        synchronized (stats) {
//                            delta = stats.elementsIn - stats.elementsOut;
//                        }
//                        return delta;
//                    }
//                });

        
        /**
         * The moving average of the nanoseconds the master spends offering a
         * chunk for transfer to a sink.
         */
        final MovingAverageTask averageSinkOfferNanos = new MovingAverageTask(
                "averageSinkOfferNanos", new Callable<Double>() {
                    public Double call() {
                        return (chunksTransferred == 0L ? 0
                                : elapsedSinkOfferNanos
                                        / (double) chunksTransferred);
                    }
                });

        /**
         * The moving average of the chunks size when chunks drained from the
         * master queue are split and the splits transferred to the appropriate
         * output sink(s).
         */
        final MovingAverageTask averageTransferChunkSize = new MovingAverageTask(
                "averageTransferChunkSize", new Callable<Double>() {
                    public Double call() {
                        return (chunksTransferred == 0L ? 0
                                : elementsTransferred
                                        / (double) chunksTransferred);
                    }
                });

        /**
         * The moving average of nanoseconds waiting for a chunk to become ready
         * so that it can be written on an output sink.
         */
        final MovingAverageTask averageNanosPerWait = new MovingAverageTask(
                "averageNanosPerWait", new Callable<Double>() {
                    public Double call() {
                        return (chunksOut == 0L ? 0 : elapsedChunkWaitingNanos
                                / (double) chunksOut);
                    }
                });

        /**
         * The moving average of nanoseconds per write for chunks written on an
         * index partition by an output sink.
         */
        final MovingAverageTask averageNanosPerWrite = new MovingAverageTask(
                "averageNanosPerWrite", new Callable<Double>() {
                    public Double call() {
                        return (chunksOut == 0L ? 0 : elapsedChunkWritingNanos
                                / (double) chunksOut);
                    }
                });

        /**
         * The average #of elements (tuples) per chunk written on an output
         * sink.
         */
        final MovingAverageTask averageElementsPerWrite = new MovingAverageTask(
                "averageElementsPerWrite", new Callable<Double>() {
                    public Double call() {
                        return (chunksOut == 0L ? 0 : elementsOut
                                / (double) chunksOut);
                    }
                });
 
        public void run() {
 
//            averageBufferedElements.run();
            averageSinkOfferNanos.run();
            averageTransferChunkSize.run();
            averageNanosPerWait.run();
            averageNanosPerWrite.run();
            averageElementsPerWrite.run();
            
        }
        
    }
    
    /**
     * Scaling factor converts nanoseconds to milliseconds.
     */
    static protected final double scalingFactor = 1d / TimeUnit.NANOSECONDS
            .convert(1, TimeUnit.MILLISECONDS);
    
    /**
     * Return a {@link CounterSet} which may be used to report the statistics on
     * the index write operation. The {@link CounterSet} is NOT placed into any
     * namespace.
     */
    @Override
    public CounterSet getCounterSet() {
        
        final CounterSet t = super.getCounterSet();
        
        t.addCounter("duplicateCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(duplicateCount);
            }
        });

        /*
         * moving averages.
         */
        
//        t.addCounter("averageBufferedElements", new Instrument<Double>() {
//            @Override
//            public void sample() {
//                setValue(statisticsTask.averageBufferedElements
//                        .getMovingAverage());
//            }
//        });

        /*
         * The moving average milliseconds the master spends offering a chunk
         * for transfer to a sink.
         */
        t.addCounter("averageSinkOfferMillis", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageSinkOfferNanos
                        .getMovingAverage()
                        * scalingFactor);
            }
        });

        /*
         * The moving average of the chunks size when chunks drained from the
         * master queue are split and the splits transferred to the appropriate
         * output sink(s).
         */
        t.addCounter("averageTransferChunkSize", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageTransferChunkSize
                        .getMovingAverage());
            }
        });

        /*
         * The moving average of milliseconds waiting for a chunk to become ready
         * so that it can be written on an output sink.
         */
        t.addCounter("averageMillisPerWait", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageNanosPerWait.getMovingAverage()
                        * scalingFactor);
            }
        });

        /*
         * The moving average of milliseconds per write for chunks written on an
         * index partition by an output sink.
         */
        t.addCounter("averageMillisPerWrite", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageNanosPerWrite.getMovingAverage()
                        * scalingFactor);
            }
        });

        /*
         * The moving average of the #of elements (tuples) per chunk written on
         * an output sink.
         */
        t.addCounter("averageElementsPerWrite", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageElementsPerWrite
                        .getMovingAverage());
            }
        });

        return t;

    }

    @Override
    public String toString() {

        return super.toString() + "{duplicateCount=" + duplicateCount + "}";

    }

    @SuppressWarnings("unchecked")
    @Override
    protected HS newSubtaskStats(final L locator) {

        return (HS) new IndexPartitionWriteStats();
        
    }

}
