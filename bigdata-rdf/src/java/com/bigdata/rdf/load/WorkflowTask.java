package com.bigdata.rdf.load;

import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.service.ClientException;
import com.bigdata.util.InnerCause;

/**
 * A class designed to pass a task from queue to queue treating the queues
 * as workflow states. Tasks begin on the
 * {@link ConcurrentDataLoader#loadService} (which has its own queue of
 * submitted but not yet running tasks) and are moved onto either the
 * {@link ConcurrentDataLoader#successQueue} or the
 * {@link ConcurrentDataLoader#errorQueue} as appropriate. Tasks which can
 * be retried are re-submitted to the
 * {@link ConcurrentDataLoader#loadService} while tasks which can no longer
 * be retried are placed on the {@link ConcurrentDataLoader#failedQueue}.
 * 
 * @param T
 *            The type of the target (T) task.
 * @param F
 *            The return type of the task's {@link Future} (F).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WorkflowTask<T extends Runnable, F> implements Runnable {

    protected static final Logger log = Logger.getLogger(WorkflowTask.class);

    /**
     * True iff the {@link #log} level is WARN or less.
     */
    final protected static boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * The {@link Future} for this {@link ReaderTask}.
     * <p>
     * Note: this field is set each time the {@link ReaderTask} is submitted
     * to the {@link ConcurrentDataLoader#loadService}. It is never
     * cleared, so it always reflects the last {@link Future}.
     */
    private Future<F> future;

    /**
     * The time when the task was first created and on retry set to the time
     * when the task was queued for retry (this may be used to identify
     * tasks that are not terminating).
     */
    final long beginTime;

    /**
     * The maximum #of times this task will be retried on error before
     * failing.
     */
    final int maxtries;

    /**
     * The #of tries so far (0 on the first try).
     */
    final int ntries;

    final T target;

    final ExecutorService service;

    final ReentrantLock lock;

    //        final BlockingQueue<WorkflowTask<T, F>> successQueue;

    final Queue<WorkflowTask<T, F>> errorQueue;

    final Queue<WorkflowTask<T, F>> failedQueue;

    final WorkflowTaskCounters counters;

    private long nanoTime_submitTask;

    private long nanoTime_beginWork;

    private long nanoTime_finishedWork;

    /**
     * Ctor for retry of a failed task.
     * 
     * @param t
     *            The failed task.
     */
    public WorkflowTask(final WorkflowTask<T, F> t) {

        this(t.target, t.service, t.lock, t.errorQueue, t.failedQueue,
                t.counters, t.maxtries, t.ntries);

    }

    /**
     * Ctor for a new task.
     * 
     * @param target
     * @param service
     * @param successQueue
     * @param errorQueue
     * @param counters
     * @param maxtries
     */
    public WorkflowTask(T target, ExecutorService service, ReentrantLock lock,
            Queue<WorkflowTask<T, F>> errorQueue,
            Queue<WorkflowTask<T, F>> failedQueue,
            WorkflowTaskCounters counters, int maxtries) {

        this(target, service, lock, errorQueue, failedQueue, counters,
                maxtries, 0/* ntries */);

    }

    /**
     * Core impl.
     * 
     * @param target
     * @param service
     * @param successQueue
     * @param errorQueue
     * @param failedQueue
     * @param counters
     * @param maxtries
     * @param ntries
     */
    protected WorkflowTask(T target, ExecutorService service,
            ReentrantLock lock,
            Queue<WorkflowTask<T, F>> errorQueue,
            Queue<WorkflowTask<T, F>> failedQueue,
            WorkflowTaskCounters counters, int maxtries, int ntries) {

        if (target == null)
            throw new IllegalArgumentException();

        if (service == null)
            throw new IllegalArgumentException();

        if (lock == null)
            throw new IllegalArgumentException();

        if (errorQueue == null)
            throw new IllegalArgumentException();

        if (failedQueue == null)
            throw new IllegalArgumentException();

        if (counters == null)
            throw new IllegalArgumentException();

        if (maxtries < 0)
            throw new IllegalArgumentException();

        if (ntries >= maxtries)
            throw new IllegalArgumentException();

        this.beginTime = System.currentTimeMillis();

        this.target = target;

        this.service = service;

        this.lock = lock;

        this.errorQueue = errorQueue;

        this.failedQueue = failedQueue;

        this.counters = counters;

        this.maxtries = maxtries;

        this.ntries = ntries + 1;

    }

    /**
     * Submit the task for execution on the {@link #service}
     * 
     * @return The {@link Future}, which is also available from
     *         {@link #getFuture()}.
     * 
     * @throws InterruptedException
     *             if interrupted while waiting to submit the task.
     */
    public Future<F> submit() throws InterruptedException {

        if (future != null) {

            // task was already submitted.
            throw new IllegalStateException();

        }

        if (INFO)
            log.info("Submitting task=" + target + " : " + counters);

        // attempt to submit the task.
        lock.lockInterruptibly();
        try {
            // note submit time.
            nanoTime_submitTask = System.nanoTime();
            // increment the counter.
            counters.taskSubmitCount.incrementAndGet();
            // submit task.
            future = (Future<F>) service.submit(this);
        } catch (RejectedExecutionException ex) {
            // task was rejected.
            // clear submit time.
            nanoTime_submitTask = 0L;
            // and back out the #of submitted tasks.
            counters.taskSubmitCount.decrementAndGet();
            counters.taskRejectCount.incrementAndGet();
            throw ex;
        } finally {
            lock.unlock();
        }

        if (INFO)
            log.info("Submitted task=" + target + " : " + counters);

        return future;

    }

    /**
     * Return the {@link Future} of the target.
     * 
     * @throws IllegalStateException
     *             if the {@link Future} has not been set.
     */
    public Future<F> getFuture() {

        if (future == null)
            throw new IllegalStateException();

        return future;

    }

    public void run() {

        try {
            runTarget();
        } catch (InterruptedException ex) {
            // interrupted during error handling.
            log.warn(ex);
            // done.
            return;
        }

    }

    /**
     * Runs the target, updating various counters and invoking
     * {@link #success()} or {@link #error(Throwable)} as appropriate.
     * 
     * @throws InterruptedException
     *             if interrupted while blocked waiting to put a task with
     *             an execution error onto the {@link #errorQueue}
     */
    protected void runTarget() throws InterruptedException {

        if (INFO)
            log.info("Running task=" + target + " : " + counters);

        nanoTime_beginWork = System.nanoTime();
        counters.queueWaitingNanoTime.addAndGet(nanoTime_beginWork
                - nanoTime_submitTask);

        try {

            try {

                // run task.
                target.run();

            } finally {

                // update counters.
                nanoTime_finishedWork = System.nanoTime();

                // increment by the amount of time that the task was
                // executing.
                counters.serviceNanoTime.addAndGet(nanoTime_finishedWork
                        - nanoTime_beginWork);

                // increment by the total time from submit to completion.
                counters.queuingNanoTime.addAndGet(nanoTime_finishedWork
                        - nanoTime_submitTask);

            }

            // success.
            success();

        } catch (Throwable ex) {

            // handle error
            error(ex);

        }

    }

    /**
     * 
     * @throws InterruptedException
     * 
     * @todo should be factory for {@link WorkflowTask} instances so that
     *       this method may be extended in order to do other things when
     *       the task has been successfully completed.
     */
    protected void success() throws InterruptedException {

        counters.taskCompleteCount.incrementAndGet();

        counters.taskSuccessCount.incrementAndGet();

        if (INFO)
            log.info("Success task=" + target + " : " + counters);

    }

    protected void error(Throwable t) throws InterruptedException {

        counters.taskCompleteCount.incrementAndGet();

        counters.taskFailCount.incrementAndGet();

        if (t instanceof CancellationException) {

            /*
             * An operation submitted by the task was cancelled due to a
             * timeout configured for the IBigdataClient's thread pool.
             */

            counters.taskCancelCount.incrementAndGet();

        }

        if (InnerCause.isInnerCause(t, ClientException.class)) {

            /*
             * Note: The way printStackTrace() behaves it appears to use
             * getStackTrace() to format the stack trace for viewing. This
             * seems to be custom tailored for the [cause] property for a
             * Throwable. However, ClientException can report more than one
             * cause from parallel executions of subtasks split out of an
             * original request. In order for those cause_s_ to be made
             * visible in the stack trace we have to unwrap the thrown
             * exception until we get the [ClientException]. ClientException
             * knows how to print a stack trace that shows ALL of its
             * causes.
             */

            t = InnerCause.getInnerCause(t, ClientException.class);

        }

        // invoke hook for error task.
        final boolean retry = error();

        if (retry) {

            if (WARN)
                log.warn("error (will retry): task=" + target + ", cause=" + t
                //, t
                        );

            /*
             * Note: MUST NOT block when added to the queue while holding
             * the lock.
             */
            while (true) {
                lock.lockInterruptibly();
                try {
                    if (errorQueue.add(this))
                        break;
                } finally {
                    lock.unlock();
                }
            }

        } else {

            // note: always with stack trace on final error.
            log.error("failed (will not retry): task=" + target + ", cause="
                    + t, t);

            counters.taskFatalCount.incrementAndGet();

            /*
             * Note: MUST NOT block when added to the queue while holding
             * the lock.
             */
            while (true) {
                lock.lockInterruptibly();
                try {
                    if (failedQueue.add(this)) {
                        break;
                    }
                } finally {
                    lock.unlock();
                }
            }

        } // else

    } // error()

    /**
     * Hook for error task.
     * 
     * @return true iff the task should be retried
     */
    protected boolean error() {

        return ntries < maxtries;

    }

}
