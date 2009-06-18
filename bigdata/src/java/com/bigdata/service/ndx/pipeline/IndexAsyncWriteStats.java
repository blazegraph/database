package com.bigdata.service.ndx.pipeline;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
 * 
 * @todo The sink counters based on a {@link MovingAverageTask} will stop
 *       updating once a given sink is discarded. Since the
 *       {@link MovingAverageTask} is no longer run, the counter does not get a
 *       new sample from that task and the counter value thereafter remains the
 *       same.
 */
public class IndexAsyncWriteStats<L, HS extends IndexPartitionWriteStats> extends
        AbstractMasterStats<L, HS> {

    /**
     * The #of duplicates which were filtered out.
     */
    public final AtomicLong duplicateCount = new AtomicLong();

    /**
     * The #of chunks that have passed through
     * {@link IndexWriteTask#handleChunk(com.bigdata.btree.keys.KVO[], boolean)}
     * .
     */
    public final AtomicLong handledChunkCount = new AtomicLong();
    
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
         * The moving average of the nanoseconds the master spends handling a
         * chunk which it has drained from its input queue.
         */
        final MovingAverageTask averageHandleChunkNanos = new MovingAverageTask(
                "averageHandleChunkNanos", new Callable<Double>() {
                    public Double call() {
                        final long t = handledChunkCount.get();
                        return (t == 0L ? 0 : elapsedHandleChunkNanos
                                / (double) t);
                    }
                });

        /**
         * The moving average of the nanoseconds the master spends splitting a
         * chunk which it has drained from its input queue.
         */
        final MovingAverageTask averageSplitChunkNanos = new MovingAverageTask(
                "averageSplitChunkNanos", new Callable<Double>() {
                    public Double call() {
                        final long t = handledChunkCount.get();
                        return (t == 0L ? 0 : elapsedSplitChunkNanos
                                / (double) t);
                    }
                });

        /**
         * The moving average of the nanoseconds the master spends offering a
         * chunk for transfer to a sink.
         */
        final MovingAverageTask averageSinkOfferNanos = new MovingAverageTask(
                "averageSinkOfferNanos", new Callable<Double>() {
                    public Double call() {
                        final long t = chunksTransferred.get();
                        return (t == 0L ? 0 : elapsedSinkOfferNanos
                                / (double) t);
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
                        final long t = chunksTransferred.get();
                        return (t == 0L ? 0 : elementsTransferred.get()
                                / (double) t);
                    }
                });

        /**
         * The moving average of nanoseconds waiting for a chunk to become ready
         * so that it can be written on an index partition.
         */
        final MovingAverageTask averageSinkChunkWaitingNanos = new MovingAverageTask(
                "averageSinkChunkWaitingNanos", new Callable<Double>() {
                    public Double call() {
                        final long t = chunksOut.get();
                        return (t == 0L ? 0 : elapsedSinkChunkWaitingNanos
                                / (double) t);
                    }
                });

        /**
         * The moving average of the maximum #of nanoseconds a sink waits for a
         * chunk to become ready so that it can be written onto an index
         * partition. If there are no index partitions for some index (that is,
         * if the asynchronous write API is not in use for that index) then this
         * will report ZERO (0).
         */
        final MovingAverageTask averageMaximumSinkChunkWaitingNanos = new MovingAverageTask(
                "averageMaximumSinkChunkWaitingNanos", new Callable<Long>() {
                    public Long call() {
                        final AtomicLong max = new AtomicLong(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                final long nanos = subtask.stats.elapsedChunkWaitingNanos;
                                // find the max (sync not necessary since op is serialized).
                                if (nanos > max.get()) {
                                    max.set(nanos);
                                }
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
                        return max.get();
                    }
                });

        /**
         * The moving average of nanoseconds per write for chunks written on an
         * index partition by an output sink.
         */
        final MovingAverageTask averageSinkChunkWritingNanos = new MovingAverageTask(
                "averageSinkChunkWritingNanos", new Callable<Double>() {
                    public Double call() {
                        final long t = chunksOut.get();
                        return (t == 0L ? 0 : elapsedSinkChunkWritingNanos
                                / (double) t);
                    }
                });

        /**
         * The moving average of the maximum #of nanoseconds per write for
         * chunks written on an index partition by an output sink. If there are
         * no index partitions for some index (that is, if the asynchronous
         * write API is not in use for that index) then this will report ZERO
         * (0).
         */
        final MovingAverageTask averageMaximumSinkChunkWritingNanos = new MovingAverageTask(
                "averageMaximumChunkWritingNanos", new Callable<Long>() {
                    public Long call() {
                        final AtomicLong max = new AtomicLong(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                final long nanos = subtask.stats.elapsedChunkWritingNanos;
                                // find the max (sync not necessary since op is serialized).
                                if (nanos > max.get()) {
                                    max.set(nanos);
                                }
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
                        return max.get();
                    }
                });

        /**
         * The moving average #of elements (tuples) per chunk written on an output
         * sink.
         */
        final MovingAverageTask averageSinkWriteChunkSize = new MovingAverageTask(
                "averageSinkWriteChunkSize", new Callable<Double>() {
                    public Double call() {
                        final long t = chunksOut.get();
                        return (t == 0L ? 0 : elementsOut.get() / (double) t);
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
                            n += master.getRedirectQueueSize();
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
                        // #of elements on all subtasks queues.
                        final AtomicLong n = new AtomicLong(0);
                        // #of subtasks.
                        final AtomicInteger m = new AtomicInteger(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                // the subtask queue size.
                                final int queueSize = subtask.buffer.size();
                                // sum of subtask queue lengths.
                                n.addAndGet(queueSize);
                                // #of subtasks reflected by that sum.
                                m.incrementAndGet();
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
                        if (m.get() == 0) {
                            // avoid divide by zero.
                            return 0d;
                        }
                        return n.get() / (double) m.get();
                    }
                });

        /**
         * The standard deviation of the moving average of the #of chunks on the
         * input queue for each sink for all masters for this index. If there
         * are no index partitions for some index (that is, if the asynchronous
         * write API is not in use for that index) then this will report ZERO
         * (0.0).
         */
        final MovingAverageTask averageSinkQueueSizeStdev = new MovingAverageTask(
                "averageSinkQueueSizeStdev", new Callable<Double>() {
                    public Double call() {
                        // the per subtask queue sizes.
                        final LinkedList<Integer> queueSizes = new LinkedList<Integer>();
                        // #of elements on all subtasks queues.
                        final AtomicLong n = new AtomicLong(0);
                        // #of subtasks.
                        final AtomicInteger m = new AtomicInteger(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                // the subtask queue size.
                                final int queueSize = subtask.buffer.size();
                                // track them all.
                                queueSizes.add(queueSize);
                                // sum of subtask queue lengths.
                                n.addAndGet(queueSize);
                                // #of subtasks reflected by that sum.
                                m.incrementAndGet();
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
                        if (m.get() == 0) {
                            // avoid divide by zero.
                            return 0d;
                        }
                        final double mean = n.get() / (double) m.get();// partitionCount;
                        /*
                         * To calculate the standard deviation, we compute the
                         * difference of each data point from the mean, and
                         * square the result.  We keep the running sum of those
                         * squares to compute the average, below.
                         */
                        double sse = 0.0;
                        for (Integer queueSize : queueSizes) {
                            final double delta = (mean - queueSize
                                    .doubleValue());
                            sse += delta * delta;
                        }
                        /*
                         * Next we average these values and take the square
                         * root, which gives the standard deviation.
                         */
                        final double stdev = Math.sqrt(sse / m.get());
                        return stdev;
                    }
                });

        /**
         * The moving average of the maximum #of chunks on the input queue for
         * each sink for all masters for this index. If there are no index
         * partitions for some index (that is, if the asynchronous write API is
         * not in use for that index) then this will report ZERO (0).
         */
        final MovingAverageTask averageMaximumSinkQueueSize = new MovingAverageTask(
                "averageMaximumSinkQueueSize", new Callable<Integer>() {
                    public Integer call() {
                        final AtomicInteger max = new AtomicInteger(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                // the subtask queue size.
                                final int queueSize = subtask.buffer.size();
                                // find the max (sync not necessary since op is serialized).
                                if (queueSize > max.get()) {
                                    max.set(queueSize);
                                }
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
                        return max.get();
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
         *       sink queues? Or just rename as "OnAllSinkQueues"?
         */
        final MovingAverageTask averageElementsOnSinkQueues = new MovingAverageTask(
                "averageElementsOnSinkQueues", new Callable<Long>() {
                    public Long call() {
                        return elementsOnSinkQueues.get();
                    }
                });

        public void run() {
 
            averageElementsOnMasterQueues.run();
            averageHandleChunkNanos.run();
            averageSplitChunkNanos.run();
            averageSinkOfferNanos.run();
            averageTransferChunkSize.run();
            averageSinkChunkWaitingNanos.run();
            averageMaximumSinkChunkWaitingNanos.run();
            averageSinkChunkWritingNanos.run();
            averageMaximumSinkChunkWritingNanos.run();
            averageSinkWriteChunkSize.run();
            averageMasterQueueSize.run();
            averageMasterRedirectQueueSize.run();
            averageSinkQueueSize.run();
            averageSinkQueueSizeStdev.run();
            averageMaximumSinkQueueSize.run();
            averageElementsOnSinkQueues.run();
            
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
                setValue(duplicateCount.get());
            }
        });
        
        t.addCounter("handledChunkCount", new Instrument<Long>() {
            @Override
            protected void sample() {
                setValue(handledChunkCount.get());
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
         * The moving average of the milliseconds the master spends handling a
         * chunk which it has drained from its input queue.
         * 
         * Note: The name of this counter was changed on 6/6/2009 to reflect the
         * fact that this counter is reporting the average #of milliseconds, not
         * nanoseconds.
         */
        t.addCounter("averageHandleChunkMillis", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageHandleChunkNanos
                        .getMovingAverage()
                        * scalingFactor);
            }
        });

        /*
         * The moving average of the milliseconds the master spends splitting a
         * chunk which it has drained from its input queue.
         * 
         * Note: The name of this counter was changed on 6/6/2009 to reflect the
         * fact that this counter is reporting the average #of milliseconds, not
         * nanoseconds.
         */
        t.addCounter("averageSplitChunkMillis", new Instrument<Double>() {
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
         * The moving average of the maximum milliseconds waiting for a chunk to
         * become ready so that it can be written on an output sink.
         */
        t.addCounter("averageMaximumMillisPerWait", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageMaximumSinkChunkWaitingNanos
                        .getMovingAverage()
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
         * The moving average of the maximum milliseconds per write for chunks
         * written on an index partition by an output sink.
         */
        t.addCounter("averageMaximumMillisPerWrite", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageMaximumSinkChunkWritingNanos
                        .getMovingAverage()
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

        /*
         * The moving average of the standard deviation #of chunks on the input
         * queue for each sink for all masters for this index.
         */
        t.addCounter("averageSinkQueueSizeStdev", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageSinkQueueSizeStdev.getMovingAverage());
            }
        });

        /*
         * The moving average of the maximum #of chunks on the input queue for
         * each sink for all masters for this index.
         */
        t.addCounter("averageMaximumSinkQueueSize", new Instrument<Double>() {
            @Override
            protected void sample() {
                setValue(statisticsTask.averageMaximumSinkQueueSize.getMovingAverage());
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
         * Lists the locators for the top N slow sinks (those with the largest
         * queue size). The sink queue needs to have at least M chunks before we
         * will include it in this list.
         */
        t.addCounter("slowSinks", new Instrument<String>() {
            @Override
            public void sample() {
                // the maximum #of sinks that will be reported.
                final int N = 10;
                // the minimum queue length before we will report the sink.
                final int M = 3;
                final TreeSet<SinkQueueSize> sinks = new TreeSet<SinkQueueSize>();
                final SubtaskOp op = new SubtaskOp() {
                    public void call(AbstractSubtask subtask) {
                        final int queueSize = subtask.buffer.size();
                        if (queueSize >= M) {
                            sinks.add(new SinkQueueSize(subtask, queueSize));
                        }
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
                /*
                 * Now format the performance counter message.
                 */
                int n = 0;
                final StringBuilder sb = new StringBuilder();
                for (SinkQueueSize t : sinks) {
                    if (n >= N)
                        break;
                    sb.append("{queueSize=" + t.queueSize + ", sink=" + t.sink
                            + "} ");
                    n++;
                }
                setValue(sb.toString());
            }
        });

        return t;

    }

    /**
     * Places the sinks into descending order by queue length.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class SinkQueueSize implements Comparable<SinkQueueSize> {
        final AbstractSubtask sink;
        final int queueSize;
        public SinkQueueSize(AbstractSubtask sink, int queueSize) {
            this.sink = sink;
            this.queueSize = queueSize;
        }
        public int compareTo(SinkQueueSize o) {
            if(queueSize<o.queueSize) return 1;
            if(queueSize>o.queueSize) return -1;
            return 0;
        }
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
