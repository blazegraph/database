package com.bigdata.service.ndx.pipeline;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.service.AbstractFederation;
import com.bigdata.util.concurrent.MovingAverageTask;

/**
 * Extended to report the moving average of the pending set size for the master
 * and the sinks and to report the maximum pending set size for the sinks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo stdev? min?
 */
public abstract class AbstractPendingSetMasterStats<L, HS extends AbstractSubtaskStats>
        extends AbstractRunnableMasterStats<L, HS> {

    /**
     * @param fed
     */
    public AbstractPendingSetMasterStats(final AbstractFederation<?> fed) {

        super(fed);

    }

    protected PendingSetStatisticsTask newStatisticsTask() {
        
        return new PendingSetStatisticsTask();
        
    }
    
    /**
     * Extended to report the average #of operations in the pending set for the
     * master and the sinks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class PendingSetStatisticsTask extends StatisticsTask {

        /**
         * The moving average of the #of elements on the master queues. This
         * does not count the #of elements which have been drained from a master
         * queue and are being transferred to a sink queue.
         */
        final MovingAverageTask averageMasterPendingSetSize = new MovingAverageTask(
                "averageMasterPendingSetSize", new Callable<Long>() {
                    public Long call() {
                        long n = 0;
                        final Iterator<WeakReference<AbstractMasterTask>> itr = masters
                                .iterator();
                        while (itr.hasNext()) {
                            final AbstractPendingSetMasterTask master = (AbstractPendingSetMasterTask) itr
                                    .next().get();
                            if (master == null)
                                continue;
                            n += master.getPendingSetSize();
                        }
                        return n;
                    }
                });

        final MovingAverageTask averageSinkPendingSetSize = new MovingAverageTask(
                "averageSinkPendingSetSize", new Callable<Double>() {
                    public Double call() {
                        // #of pending items on all subtasks.
                        final AtomicLong n = new AtomicLong(0);
                        // #of subtasks.
                        final AtomicInteger m = new AtomicInteger(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                final int size = ((AbstractPendingSetSubtask) subtask)
                                        .getPendingSetSize();
                                // sum of pending set sizes.
                                n.addAndGet(size);
                                // #of subtasks reflected by that sum.
                                m.incrementAndGet();
                            }
                        };
                        final Iterator<WeakReference<AbstractMasterTask>> itr = masters
                                .iterator();
                        while (itr.hasNext()) {
                            final AbstractPendingSetMasterTask master = (AbstractPendingSetMasterTask) itr
                                    .next().get();
                            if (master == null)
                                continue;
                            try {
                                master.mapOperationOverSubtasks(op);
                            } catch (InterruptedException ex) {
                                break;
                            } catch (ExecutionException ex) {
                                log.error(this, ex);
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

        final MovingAverageTask averageMaximumSinkPendingSetSize = new MovingAverageTask(
                "averageMaximumSinkPendingSetSize", new Callable<Integer>() {
                    public Integer call() {
                        final AtomicInteger max = new AtomicInteger(0);
                        final SubtaskOp op = new SubtaskOp() {
                            public void call(AbstractSubtask subtask) {
                                final int size = ((AbstractPendingSetSubtask) subtask)
                                        .getPendingSetSize();
                                // find the max (sync not necessary since op is
                                // serialized).
                                if (size > max.get()) {
                                    max.set(size);
                                }
                            }
                        };
                        final Iterator<WeakReference<AbstractMasterTask>> itr = masters
                                .iterator();
                        while (itr.hasNext()) {
                            final AbstractPendingSetMasterTask master = (AbstractPendingSetMasterTask) itr
                                    .next().get();
                            if (master == null)
                                continue;
                            try {
                                master.mapOperationOverSubtasks(op);
                            } catch (InterruptedException ex) {
                                break;
                            } catch (ExecutionException ex) {
                                log.error(this, ex);
                                break;
                            }
                        }
                        return max.get();
                    }
                });

        public void run() {

            super.run();

            averageMasterPendingSetSize.run();
            averageSinkPendingSetSize.run();
            averageMaximumSinkPendingSetSize.run();
            
        }
        
    }
 
    public CounterSet getCounters() {
        
        final PendingSetStatisticsTask statisticsTask = ((PendingSetStatisticsTask)this.statisticsTask);
        
        final CounterSet t = super.getCounterSet();
        
        t.addCounter("averageMasterPendingSetSize", new Instrument<Double>() {
            @Override
            public void sample() {
                setValue(statisticsTask.averageMasterPendingSetSize
                        .getMovingAverage());
            }
        });
        
        t.addCounter("averageSinkPendingSetSize", new Instrument<Double>() {
            @Override
            public void sample() {
                setValue(statisticsTask.averageSinkPendingSetSize
                        .getMovingAverage());
            }
        });
        
        t.addCounter("averageMaximumSinkPendingSetSize", new Instrument<Double>() {
            @Override
            public void sample() {
                setValue(statisticsTask.averageMaximumSinkPendingSetSize
                        .getMovingAverage());
            }
        });
        
        return t;
        
    }
    
}
