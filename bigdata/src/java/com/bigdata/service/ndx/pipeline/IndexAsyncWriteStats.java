package com.bigdata.service.ndx.pipeline;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.AbstractFederation;
import com.bigdata.util.concurrent.MovingAverageTask;

/**
 * Statistics for the consumer, including several moving averages based on
 * sampled data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexAsyncWriteStats<L, HS extends IndexPartitionWriteStats> extends
        AbstractMasterStats<L, HS> {

    /**
     * The #of duplicates which were filtered out.
     */
    public long duplicateCount = 0L;

    /**
     * The #of chunks that have passed through
     * {@link IndexWriteTask#handleChunk(com.bigdata.btree.keys.KVO[], boolean)}
     * .
     */
    public long handledChunkCount = 0L;
    
    /**
     * Elapsed nanoseconds in
     * {@link IndexWriteTask#handleChunk(com.bigdata.btree.keys.KVO[], boolean)}
     * required to split a chunk drained from the master.
     */
    public long elapsedSplitChunkNanos = 0L;
    
    /**
     * Elapsed nanoseconds in
     * {@link IndexWriteTask#handleChunk(com.bigdata.btree.keys.KVO[], boolean)}
     * .
     */
    public long elapsedHandleChunkNanos = 0L;
    
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

        protected final transient Logger log = Logger.getLogger(StatisticsTask.class);

        /**
         * The moving average of the nanoseconds the master spends handling a
         * chunk which it has drained from its input queue.
         */
        final MovingAverageTask averageHandleChunkNanos = new MovingAverageTask(
                "averageHandleChunkNanos", new Callable<Double>() {
                    public Double call() {
                        return (handledChunkCount == 0L ? 0
                                : elapsedHandleChunkNanos
                                        / (double) handledChunkCount);
                    }
                });

        /**
         * The moving average of the nanoseconds the master spends splitting a
         * chunk which it has drained from its input queue.
         */
        final MovingAverageTask averageSplitChunkNanos = new MovingAverageTask(
                "averageSplitChunkNanos", new Callable<Double>() {
                    public Double call() {
                        return (handledChunkCount == 0L ? 0
                                : elapsedSplitChunkNanos
                                        / (double) handledChunkCount);
                    }
                });

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
         * The moving average #of elements (tuples) per chunk written on an output
         * sink.
         */
        final MovingAverageTask averageElementsPerWrite = new MovingAverageTask(
                "averageElementsPerWrite", new Callable<Double>() {
                    public Double call() {
                        return (chunksOut == 0L ? 0 : elementsOut
                                / (double) chunksOut);
                    }
                });

        /**
         * The moving average of the #of chunks on the master's input queue for
         * all masters for this index.
         */
        final MovingAverageTask averageMasterQueueSize = new MovingAverageTask(
                "averageMasterQueueSize", new Callable<Integer>() {
                    public Integer call() {
                        int n = 0;
                        final Iterator<WeakReference<AbstractMasterTask>> itr = masters
                                .iterator();
                        while (itr.hasNext()) {
                            final AbstractMasterTask master = itr.next().get();
                            if (master == null)
                                continue;
                            n += master.buffer.size();
                        }
                        return n;
                    }
                });

        /**
         * The moving average of the #of chunks on the input queue for each sink
         * for all masters for this index.
         * 
         * @todo This times the {@link #averageTransferChunkSize} gives a
         *       reasonable estimate of the magnitude of the #of elements queued
         *       on the sinks. To do better than that we could have to actually
         *       scan the sink queues and report the #of elements across those
         *       queues. If {@link BlockingBuffer} were to track the #of
         *       elements outstanding on the buffer then we could report that
         *       and it would be an exact count. We could then also report that
         *       value for the input queue for each master.
         */
        final MovingAverageTask averageTotalSinkQueueSize = new MovingAverageTask(
                "averageTotalSinkQueueSize", new Callable<Integer>() {
                    public Integer call() {
                        final AtomicInteger n = new AtomicInteger(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                n.addAndGet(subtask.buffer.size());
                            }
                        };
                        final Iterator<WeakReference<AbstractMasterTask>> itr = masters
                                .iterator();
                        while (itr.hasNext()) {
                            final AbstractMasterTask master = itr.next().get();
                            if (master == null)
                                continue;
                            try {
                                master.mapOperationOverSubtasks(op);
                            } catch(InterruptedException ex) {
                                break;
                            } catch(Exception ex) {
                                log.error(this,ex);
                                break;
                            }
                        }
                        return n.get();
                    }
                });

        public void run() {
 
//            averageBufferedElements.run();
            averageSplitChunkNanos.run();
            averageHandleChunkNanos.run();
            averageSinkOfferNanos.run();
            averageTransferChunkSize.run();
            averageNanosPerWait.run();
            averageNanosPerWrite.run();
            averageElementsPerWrite.run();
            averageMasterQueueSize.run();
            averageTotalSinkQueueSize.run();
            
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
        
        t.addCounter("handledChunkCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(handledChunkCount);
            }
        });

        t.addCounter("elapsedSplitChunkNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedSplitChunkNanos);
            }
        });

        t.addCounter("elapsedHandleChunkNanos", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(elapsedHandleChunkNanos);
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
         * The moving average of the nanoseconds the master spends handling a
         * chunk which it has drained from its input queue.
         */
        t.addCounter("averageHandleChunkNanos", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageHandleChunkNanos
                        .getMovingAverage()
                        * scalingFactor);
            }
        });

        /*
         * The moving average of the nanoseconds the master spends splitting a
         * chunk which it has drained from its input queue.
         */
        t.addCounter("averageSplitChunkNanos", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageSplitChunkNanos
                        .getMovingAverage()
                        * scalingFactor);
            }
        });
        
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

        /*
         * The moving average of the #of chunks on the master's input queue for
         * all masters for this index.
         */
        t.addCounter("averageMasterQueueSize", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageMasterQueueSize
                        .getMovingAverage());
            }
        });

        /*
         * The moving average of the #of chunks on the input queue for each sink
         * for all masters for this index.
         */
        t.addCounter("averageTotalSinkQueueSize", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageTotalSinkQueueSize
                        .getMovingAverage());
            }
        });

        return t;

    }

    @Override
    public String toString() {

        // @todo report more?
        return super.toString() + "{duplicateCount=" + duplicateCount + "}";

    }

    @SuppressWarnings("unchecked")
    @Override
    protected HS newSubtaskStats(final L locator) {

        return (HS) new IndexPartitionWriteStats();
        
    }

}
