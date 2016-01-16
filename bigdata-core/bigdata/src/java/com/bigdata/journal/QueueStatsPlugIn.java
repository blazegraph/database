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
package com.bigdata.journal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.util.concurrent.ThreadPoolExecutorBaseStatisticsTask;

/**
 * Plugin for sampling the {@link ExecutorService}. This collects interesting
 * statistics about the thread pool for reporting to the load balancer service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class QueueStatsPlugIn implements
        IPlugIn<Journal, ThreadPoolExecutorBaseStatisticsTask> {

    private static final Logger log = Logger.getLogger(QueueStatsPlugIn.class);

    /**
     * Performance counters options.
     */
    public interface Options {
        
        /**
         * Boolean option for the collection of statistics from the various
         * queues using to run tasks (default
         * {@link #DEFAULT_COLLECT_QUEUE_STATISTICS}).
         */
        String COLLECT_QUEUE_STATISTICS = Journal.class.getName()
                + ".collectQueueStatistics";

        String DEFAULT_COLLECT_QUEUE_STATISTICS = "false";

    }
    
    /**
     * Collects interesting statistics on the {@link ExecutorService}.
     * <p>
     * Note: Guarded by synchronized(this).
     * 
     * @see Options#COLLECT_QUEUE_STATISTICS
     */
    private ThreadPoolExecutorBaseStatisticsTask queueSampleTask = null;

    /**
     * The {@link ScheduledFuture} for the task.
     * <p>
     * Note: Guarded by synchronized(this).
     */
    private ScheduledFuture<?> scheduledFuture = null;
    
    /**
     * {@inheritDoc}
     * <p>
     * Setup sampling on the client's thread pool.
     */
    @Override
    public void startService(final Journal indexManager) {
        
        final boolean collectQueueStatistics = Boolean.valueOf(indexManager
                .getProperty(Options.COLLECT_QUEUE_STATISTICS,
                        Options.DEFAULT_COLLECT_QUEUE_STATISTICS));

        if (log.isInfoEnabled())
            log.info(Options.COLLECT_QUEUE_STATISTICS + "="
                    + collectQueueStatistics);

        if (!collectQueueStatistics) {

            return;

        }

        final long initialDelay = 0; // initial delay in ms.
        final long delay = 1000; // delay in ms.
        final TimeUnit unit = TimeUnit.MILLISECONDS;

        synchronized (this) {

            queueSampleTask = new ThreadPoolExecutorBaseStatisticsTask(
                    (ThreadPoolExecutor) indexManager.getExecutorService());

            scheduledFuture = indexManager.addScheduledTask(queueSampleTask,
                    initialDelay, delay, unit);

        }

    }

    @Override
    public void stopService(final boolean immediateShutdown) {

        synchronized (this) {

            if (scheduledFuture != null) {
        
                scheduledFuture
                        .cancel(immediateShutdown/* mayInterruptIfRunning */);
                
                scheduledFuture = null;
                
            }

            queueSampleTask = null;

        }
        
    }

    @Override
    public ThreadPoolExecutorBaseStatisticsTask getService() {

        synchronized(this) {

            return queueSampleTask;
            
        }
        
    }

    @Override
    public boolean isRunning() {

        synchronized (this) {
        
            if (scheduledFuture == null || scheduledFuture.isDone())
                return false;
            
            return true;
            
        }
        
    }

}
