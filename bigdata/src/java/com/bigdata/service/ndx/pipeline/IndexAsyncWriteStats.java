package com.bigdata.service.ndx.pipeline;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

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
         * The moving average of the #of elements on the master queues. This
         * does not count the #of elements which have been drained from a master
         * queue and are being transferred to a sink queue.
         */
        final MovingAverageTask averageElementsOnMasterQueues = new MovingAverageTask(
                "averageElementsOnMasterQueues", new Callable<Long>() {
                    public Long call() {
                        long n = 0;
                        final Iterator<WeakReference<AbstractMasterTask>> itr = masters
                                .iterator();
                        while (itr.hasNext()) {
                            final AbstractMasterTask master = itr.next().get();
                            if (master == null)
                                continue;
                            n += master.buffer.getElementsOnQueueCount();
                        }
                        return n;
                    }
                });

        /**
         * The moving average of the #of elements on the sink queues. This does
         * not count the #of elements on the master queues nor does it count the
         * #of elements which have been drained from a sink queue and are either
         * being prepared for or awaiting completion of a write on an index
         * partition.
         * 
         * @todo Change this to be the average #of elements on each sink queue
         *       rather than the average of the total #of elements across all
         *       sink queues?
         */
        final MovingAverageTask averageElementsOnSinkQueues = new MovingAverageTask(
                "averageElementsOnSinkQueues", new Callable<Long>() {
                    public Long call() {
                        return elementsOnSinkQueues;
                    }
                });

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
        final MovingAverageTask averageSinkChunkWaitingNanos = new MovingAverageTask(
                "averageSinkChunkWaitingNanos", new Callable<Double>() {
                    public Double call() {
                        return (chunksOut == 0L ? 0 : elapsedSinkChunkWaitingNanos
                                / (double) chunksOut);
                    }
                });

        /**
         * The moving average of nanoseconds per write for chunks written on an
         * index partition by an output sink.
         */
        final MovingAverageTask averageSinkChunkWritingNanos = new MovingAverageTask(
                "averageSinkChunkWritingNanos", new Callable<Double>() {
                    public Double call() {
                        return (chunksOut == 0L ? 0 : elapsedSinkChunkWritingNanos
                                / (double) chunksOut);
                    }
                });

        /**
         * The moving average #of elements (tuples) per chunk written on an output
         * sink.
         */
        final MovingAverageTask averageSinkWriteChunkSize = new MovingAverageTask(
                "averageSinkWriteChunkSize", new Callable<Double>() {
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
         * The moving average of the #of chunks on the master's redirect queue
         * for all masters for this index.
         */
        final MovingAverageTask averageMasterRedirectQueueSize = new MovingAverageTask(
                "averageMasterRedirectQueueSize", new Callable<Integer>() {
                    public Integer call() {
                        int n = 0;
                        final Iterator<WeakReference<AbstractMasterTask>> itr = masters
                                .iterator();
                        while (itr.hasNext()) {
                            final AbstractMasterTask master = itr.next().get();
                            if (master == null)
                                continue;
                            n += master.redirectQueue.size();
                        }
                        return n;
                    }
                });

        /**
         * The moving average of the #of chunks on the input queue for each sink
         * for all masters for this index. If there are no index partitions for
         * some index (that is, if the asynchronous write API is not in use for
         * that index) then this will report ZERO (0.0).
         */
        final MovingAverageTask averageSinkQueueSize = new MovingAverageTask(
                "averageSinkQueueSize", new Callable<Double>() {
                    public Double call() {
                        final AtomicInteger n = new AtomicInteger(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                n.addAndGet(subtask.buffer.size());
                            }
                        };
                        final Iterator<WeakReference<AbstractMasterTask>> itr = masters
                                .iterator();
                        int partitionCount = 0;
                        while (itr.hasNext()) {
                            final AbstractMasterTask master = itr.next().get();
                            if (master == null)
                                continue;
                            try {
                                master.mapOperationOverSubtasks(op);
                                partitionCount++;
                            } catch(InterruptedException ex) {
                                break;
                            } catch(Exception ex) {
                                log.error(this,ex);
                                break;
                            }
                        }
                        if (partitionCount == 0) {
                            // avoid divide by zero.
                            return 0d;
                        }
                        return n.get() / (double) partitionCount;
                    }
                });

        public void run() {
 
            averageElementsOnMasterQueues.run();
            averageElementsOnSinkQueues.run();
            averageHandleChunkNanos.run();
            averageSplitChunkNanos.run();
            averageSinkOfferNanos.run();
            averageTransferChunkSize.run();
            averageSinkChunkWaitingNanos.run();
            averageSinkChunkWritingNanos.run();
            averageSinkWriteChunkSize.run();
            averageMasterQueueSize.run();
            averageMasterRedirectQueueSize.run();
            averageSinkQueueSize.run();
            
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

        /*
         * The moving average of the #of elements on the master queues. This
         * does not count the #of elements which have been drained from a master
         * queue and are being transferred to a sink queue.
         */
        t.addCounter("averageElementsOnMasterQueues", new Instrument<Double>() {
            @Override
            public void sample() {
                setValue(statisticsTask.averageElementsOnMasterQueues
                        .getMovingAverage());
            }
        });


        /*
         * The moving average of the #of elements on the sink queues.  This does
         * not count the #of elements on the master queues nor does it count the
         * #of elements which have been drained from a sink queue and are either
         * being prepared for or awaiting completion of a write on an index
         * partition.
         */
        t.addCounter("averageElementsOnSinkQueues", new Instrument<Double>() {
            @Override
            public void sample() {
                setValue(statisticsTask.averageElementsOnSinkQueues
                        .getMovingAverage());
            }
        });

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
                setValue(statisticsTask.averageSinkChunkWaitingNanos.getMovingAverage()
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
                setValue(statisticsTask.averageSinkChunkWritingNanos.getMovingAverage()
                        * scalingFactor);
            }
        });

        /*
         * The ratio of the average consumption time (time to write on an index
         * partition) to the average production time (time to generate a full
         * chunk for an output sink). consumerFaster < 1.0 < producerFaster.
         * 
         * Note: If there is no data (producerRate==0) then the value will be
         * reported as zero, but this does not indicate that the consumer is
         * faster but rather than there is nothing going on for that index.
         */
        t.addCounter("consumerProducerRatio", new Instrument<Double>() {
            @Override
            protected void sample() {

                final double consumerRate = statisticsTask.averageSinkChunkWritingNanos
                        .getMovingAverage();

                final double producerRate = statisticsTask.averageSinkChunkWaitingNanos
                        .getMovingAverage();

                final double rateRatio;
                if (producerRate == 0) {

                    // avoid divide by zero.
                    rateRatio = 0d;

                } else {

                    rateRatio = consumerRate / producerRate;

                }

                setValue(rateRatio);
            }
        });

        /*
         * The moving average of the #of elements (tuples) per chunk written on
         * an output sink.
         */
        t.addCounter("averageElementsPerWrite", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageSinkWriteChunkSize
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
         * The moving average of the #of chunks on the master's redirect queue
         * for all masters for this index.
         */
        t.addCounter("averageMasterRedirectQueueSize", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageMasterRedirectQueueSize
                        .getMovingAverage());
            }
        });

        /*
         * The moving average of the #of chunks on the input queue for each sink
         * for all masters for this index.
         */
        t.addCounter("averageSinkQueueSize", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageSinkQueueSize.getMovingAverage());
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
