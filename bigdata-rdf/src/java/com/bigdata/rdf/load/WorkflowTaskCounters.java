package com.bigdata.rdf.load;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.service.IBigdataClient;
import com.bigdata.util.concurrent.TaskCounters;
import com.bigdata.util.concurrent.ThreadPoolExecutorStatisticsTask;

/**
 * Counters updated by a {@link WorkflowTask}. All instances running on the
 * same service should use the same {@link WorkflowTaskCounters} instance. The
 * service should report these counters along with any other counters that it
 * tracks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo extract a common interface or impl for the {@link ThreadPoolExecutorStatisticsTask}
 *       so that we can report the response times here.
 */
public class WorkflowTaskCounters extends TaskCounters {

    /**
     * The #of tasks for which a {@link RejectedExecutionException} was
     * thrown when the task was
     * {@link ExecutorService#submit(java.util.concurrent.Callable) submitted}.
     * <p>
     * Note: A task is not considered to be "submitted" until it is accepted
     * for execution by some service. Therefore a task which is rejected
     * does NOT cause {@link TaskCounters#taskSubmitCount} to be incremented
     * and the retry of a rejected task is not counted by
     * {@link #taskRetryCount}.
     */
    final public AtomicInteger taskRejectCount = new AtomicInteger(0);

    /**
     * The #of tasks that have been re-submitted following some error which
     * caused the task to fail.
     */
    final public AtomicInteger taskRetryCount = new AtomicInteger(0);

    /**
     * #of tasks that failed due to a {@link CancellationException} - this is
     * the exception thrown when the
     * {@link IBigdataClient.Options#CLIENT_TASK_TIMEOUT} is exceeded for some
     * operation causing the operation to be cancelled and the task to fail.
     */
    final public AtomicLong taskCancelCount = new AtomicLong();

    /**
     * The #of tasks which have failed and will not be retried as their max
     * retry count has been reached.
     */
    final public AtomicInteger taskFatalCount = new AtomicInteger(0);

    public String toString() {

        return super.toString() + ", #reject=" + taskRejectCount + ", #retry="
                + taskRetryCount + ", #cancel=" + taskCancelCount + ", #fatal="
                + taskFatalCount;

    }

}
