package com.bigdata.util.concurrent;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import com.bigdata.counters.ICounterHierarchy;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.WriteExecutorService;

/**
 * Interface defines and documents the names and meanings of counters pertaining
 * to blocking queues (of tasks) and services executing tasks and includes
 * counters pertaining to the service executing {@link ITx#UNISOLATED} tasks -
 * the {@link WriteExecutorService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IQueueCounters extends ICounterHierarchy {

    /**
     * The #of tasks not yet assigned to any thread which are waiting to run
     * (moving average).  This is available for any queue.
     */
    String AverageQueueSize = "Average Queue Size";

    /**
     * Counters defined by {@link TaskCounters}. Subsets of these counters are
     * also exposed by other interfaces.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ITaskCounters {

        /**
         * Count of all tasks completed by the service (failed + success).
         */
        String TaskCompleteCount = "Task Complete Count";

        /**
         * Count of all tasks submitted to the service.
         */
        String TaskSubmitCount = "Task Submit Count";

        /**
         * Count of all tasks which failed during execution.
         */
        String TaskFailCount = "Task Failed Count";

        /**
         * Count of all tasks which were successfully executed.
         */
        String TaskSuccessCount = "Task Success Count";

        /**
         * Cumulative milliseconds across tasks of the time that a task was
         * waiting on a queue pending execution.
         */
        String QueueWaitingTime = "Queue Waiting Time";

        /**
         * Cumulative milliseconds across tasks that a task is being serviced by
         * a worker thread (elapsed clock time from when the task was assigned
         * to the thread until the task completes its work).
         * <p>
         * Note: For tasks which acquire resource lock(s), this does NOT include
         * the time waiting to acquire the resource lock(s).
         */
        String ServiceTime = "Service Time";

        /**
         * Cumulative milliseconds across tasks between the submission of a task
         * and its completion including any time spent waiting for resource
         * locks, commit processing and any time spent servicing that task.
         */
        String QueuingTime = "Queuing Time";

    }
    
    /**
     * Additional counters available for any {@link ThreadPoolExecutor}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IThreadPoolExecutorCounters extends IQueueCounters {

        /**
         * Count of all tasks completed by the service (failed + success).
         * 
         * @see ITaskCounters#TaskCompleteCount
         */
        String TaskCompleteCount = ITaskCounters.TaskCompleteCount;

        /**
         * The #of tasks that are currently running (moving average).
         * <p>
         * Note: This count does NOT reflect the #of tasks holding locks for
         * queues where tasks require locks to execute.
         * 
         * @see IWriteServiceExecutorCounters#AverageActiveCountWithLocksHeld
         */
        String AverageActiveCount = "Average Active Count";

        /**
         * The queue length (moving average).
         * <p>
         * Note: this is the primary average of interest - it includes both the
         * tasks waiting to be run and those that are currently running in the
         * definition of the "queue length".
         */
        String AverageQueueLength = "Average Queue Length";

        /**
         * The current size of the thread pool for the service.
         */
        String PoolSize = "Pool Size";

        /**
         * The maximum observed value for the size of the thread pool for the
         * service.
         */
        String LargestPoolSize = "Largest Pool Size";

    }

    /**
     * Additional counters available for any {@link ThreadPoolExecutor} which is
     * processing {@link AbstractTask}s.
     * <p>
     * Note: The {@link ConcurrencyManager} and {@link AbstractTask} work
     * together to maintain the per-service {@link TaskCounters}s on which
     * these additional counters are based.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IThreadPoolExecutorTaskCounters extends IQueueCounters {

        /**
         * Count of all tasks submitted to the service.
         * 
         * @see ITaskCounters#TaskSubmitCount
         */
        String TaskSubmitCount = ITaskCounters.TaskSubmitCount;

        /**
         * Count of all tasks which failed during execution.
         * 
         * @see ITaskCounters#TaskFailCount
         */
        String TaskFailCount = ITaskCounters.TaskFailCount;

        /**
         * Count of all tasks which were successfully executed.
         * 
         * @see ITaskCounters#TaskSuccessCount
         */
        String TaskSuccessCount = ITaskCounters.TaskSuccessCount;

        /**
         * Moving average in milliseconds of the time a task waits on a queue
         * pending execution.
         * 
         * @see ITaskCounters#QueueWaitingTime
         */
        String AverageQueueWaitingTime = "Average Queue Waiting Time";

        /**
         * Moving average in milliseconds of the time that a task is being
         * serviced by a worker thread (elapsed clock time from when the task
         * was assigned to the thread until the task completes its work).
         * <p>
         * Note: For tasks which acquire resource lock(s), this does NOT include
         * the time waiting to acquire the resource lock(s).
         * 
         * @see ITaskCounters#ServiceTime
         */
        String AverageServiceTime = "Average Service Time";

        /**
         * Moving average in milliseconds of the time between the submission of
         * a task and its completion including any time spent waiting for
         * resource locks, commit processing and any time spent servicing that
         * task.
         * 
         * @see ITaskCounters#QueuingTime
         */
        String AverageQueuingTime = "Average Queuing Time";

    }

    /**
     * Additional counters available for the {@link WriteServiceExecutor}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IWriteServiceExecutorCounters extends
            IThreadPoolExecutorTaskCounters {

        /**
         * The #of tasks that are currently running <strong>with locks held</strong>
         * (moving average) (this is only reported for the
         * {@link WriteExecutorService} as that is the only service where tasks
         * must acquire locks in order to execute).
         */
        String AverageActiveCountWithLocksHeld = "Average Active Count With Locks Held";

        /**
         * The #of tasks that are waiting to run on the internal lock used by
         * the {@link WriteExecutorService} to coordinate the start and end of
         * tasks and the group commit. This counter indicates how much potential
         * concurrency is being wasted by the {@link WriteExecutorService}.
         */
        String AverageReadyCount = "Average Ready Count";

        /**
         * Moving average in milliseconds of the time that a task is waiting for
         * resource locks (zero unless the task is unisolated).
         */
        String AverageLockWaitingTime = "Average Lock Waiting Time";

        /**
         * Moving average in milliseconds of the time that the task that
         * initiates the group commit waits for other tasks to join the commit
         * group (zero unless the service is unisolated).
         */
        String AverageCommitWaitingTime = "Average Commit Waiting Time";

        /**
         * Moving average in milliseconds of the time servicing the group commit
         * (zero unless the service is unisolated).
         */
        String AverageCommitServiceTime = "Average Commit Service Time";

        /**
         * Moving average of the #of bytes written since the previous commit
         * (zero unless the service is unisolated).
         * <p>
         * Note: This DOES NOT imply that this many bytes were written in the
         * commit itself. Indices are checkpointed after each task. All writes
         * on the journal should have already occurred before the group commit.
         * The commit protocol itself involves writing a very few bytes and then
         * syncing the disk.
         */
        String AverageByteCountPerCommit = "Average Byte Count Per Commit";

        /**
         * The #of commits (only reported services which do commit processing).
         */
        String CommitCount = "Commit Count";

        /**
         * The #of aborts (only reported services which do commit processing).
         */
        String AbortCount = "Abort Count";

        /**
         * The #of synchronous overflow events (only reported services which do
         * commit processing). A synchronous overflow event is when the index
         * partitions on the live journal are re-defined onto a new live
         * journal. Asynchronous overflow processing then proceeds in the
         * background handling index partition builds, splits, joins, moves,
         * etc.
         */
        String OverflowCount = "Overflow Count";

        /**
         * The #of tasks whose execution was rejected, typically because the
         * queue was at capacity.
         * 
         * @see RejectedExecutionHandler
         */
        String RejectedExecutionCount = "Rejected Execution Count";

        /**
         * The maximum observed value in milliseconds of the time that the task
         * that initiates the group commit waits for other tasks to join the
         * commit group (zero unless the service is unisolated).
         */
        String MaxCommitWaitingTime = "Max Commit Waiting Time";

        /**
         * The maximum observed value in milliseconds of the time servicing the
         * group commit (zero unless the service is unisolated).
         */
        String MaxCommitServiceTime = "Max Commit Service Time";

        /**
         * Moving average of the #of tasks that participate in commit group.
         * (The size of the most recent commit group is sampled and turned into
         * a moving average.)
         */
        String AverageCommitGroupSize = "Average Commit Group Size";

        /**
         * The maximum #of tasks in any commit group.
         */
        String MaxCommitGroupSize = "Max Commit Group Size";

        /**
         * The maximum #of tasks that are concurrently executing without regard
         * to whether or not the tasks have acquired their locks.
         * <p>
         * Note: Since this does not reflect tasks executing concurrently with
         * locks held it is not a measure of the true concurrency of tasks
         * executing on the service.
         */
        String MaxRunning = "Max Running";

    }

}
