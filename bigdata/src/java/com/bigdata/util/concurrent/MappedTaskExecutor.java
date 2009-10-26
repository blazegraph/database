package com.bigdata.util.concurrent;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

/**
 * A fly weight helper class that runs tasks either sequentially or with limited
 * parallelism against some thread pool. Deadlock can arise when limited
 * parallelism is applied if there are dependencies among the tasks. Limited
 * parallelism is enforced by running chunks of tasks in sequence. Higher
 * concurrency is possible when using a dedicated thread pool having the desired
 * number of worker threads.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MappedTaskExecutor {

    protected static final transient Logger log = Logger
            .getLogger(MappedTaskExecutor.class);

    private final ExecutorService service;

    public MappedTaskExecutor(final ExecutorService service) {

        if (service == null)
            throw new IllegalArgumentException();

        this.service = service;

    }

    protected ExecutorService getThreadPool() {

        return service;

    }

    /**
     * Runs a set of tasks, checking their futures. When maxParallel is GT ONE
     * (1), tasks are run in chunks of up to maxParallel tasks at once. New
     * chunks are not submitted until all tasks in the existing chunk are
     * complete. Higher concurrency limited parallelism may be obtained with a
     * purpose specific thread pool of a given worker thread capacity, but there
     * is overhead associated with creating the thread pool and its worker
     * threads.
     * 
     * @param maxParallel
     *            The maximum parallelism. This is ONE (1) to run the tasks
     *            sequentially. If the value is 2 or greater, then the tasks
     *            will be executed with limited parallelism. You may specify
     *            {@link Integer#MAX_VALUE} to indicate that this class should
     *            not impose any constraint on their parallel execution.
     * @param tasks
     *            The tasks to be executed.
     * 
     * @return The list of value returned by the {@link Future}(s) of the tasks
     *         IFF all tasks were executed successfully.
     * 
     * @throws TimeoutException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public <T> List<T> runTasks(final List<Callable<T>> tasks,
            final long timeout, final TimeUnit unit, final int maxParallel)
            throws InterruptedException, ExecutionException, TimeoutException {

        if (tasks == null)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();

        if (maxParallel <= 0)
            throw new IllegalArgumentException();

        if (tasks.isEmpty()) {

            return Collections.emptyList();

        }

        if (tasks.size() == 1) {

            return runOne(tasks.get(0), timeout, unit);

        } else if (maxParallel == Integer.MAX_VALUE) {

            return runMaxParallel(tasks, timeout, unit);

        } else if (maxParallel > 1) {

            return runLimitedParallel(tasks, timeout, unit, maxParallel);

        } else {

            return runSequence(tasks, timeout, unit);

        }

    }

    /**
     * Run a single task on the thread pool.
     */
    private <T> List<T> runOne(final Callable<T> task, final long timeout,
            final TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {

//        if (log.isInfoEnabled())
//            log.info("Running one task (#active="
//                    + getThreadPool().getActiveCount() + ", queueSize="
//                    + getThreadPool().getQueue().size() + ") : "
//                    + task.toString());

        final Future<T> f = getThreadPool().submit(task);

        // await completion of the task.
        final T t = f.get(timeout, unit);

        return Collections.singletonList(t);

    }

    /**
     * Runs a list of tasks, one task at a time.
     */
    private <T> List<T> runSequence(final List<Callable<T>> tasks,
            long timeout, final TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {

        // remaining nanoseconds.
        long nanos = timeout = unit.toNanos(timeout);

        // start time.
        final long begin = System.nanoTime();

//        if (log.isInfoEnabled())
//            log.info("Running " + tasks.size() + " tasks in sequence (#active="
//                    + getThreadPool().getActiveCount() + ", queueSize="
//                    + getThreadPool().getQueue().size() + ") : "
//                    + tasks.get(0).toString());

        final Iterator<Callable<T>> itr = tasks.iterator();

        final List<T> list = new LinkedList<T>();

        while (itr.hasNext()) {

            if (nanos < 0)
                throw new TimeoutException();

            final Callable<T> task = itr.next();

            final Future<T> f = getThreadPool().submit(task);

            // await completion of the task.
            list.add(f.get(nanos, TimeUnit.NANOSECONDS));

            // update remaining nanoseconds.
            nanos = timeout - (System.nanoTime() - begin);

        }

        return list;

    }

    /**
     * Executes tasks with full parallelism.
     */
    private <T> List<T> runMaxParallel(final List<Callable<T>> tasks,
            final long timeout, final TimeUnit unit) throws ExecutionException,
            InterruptedException {

        final long begin = System.nanoTime();

//        if (log.isInfoEnabled())
//            log.info("Running " + tasks.size() + " tasks in parallel (#active="
//                    + getThreadPool().getActiveCount() + ", queueSize="
//                    + getThreadPool().getQueue().size() + ") : "
//                    + tasks.get(0).toString());

        final List<Future<T>> futures = getThreadPool().invokeAll(tasks,
                timeout, unit);

        final Iterator<Future<T>> itr = futures.iterator();

        final List<T> list = new LinkedList<T>();

        while (itr.hasNext()) {

            final Future<T> f = itr.next();

            list.add(f.get());

        }

        if (log.isInfoEnabled())
            log.info("Ran " + tasks.size() + " tasks in parallel: elapsed="
                    + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin));

        return list;

    }

    /**
     * Executes tasks with limited parallelism.
     */
    private <T> List<T> runLimitedParallel(final List<Callable<T>> tasks,
            long timeout, final TimeUnit unit, final int maxParallel)
            throws ExecutionException, InterruptedException, TimeoutException {

        // remaining nanoseconds.
        long nanos = timeout = unit.toNanos(timeout);

        // starting time.
        final long begin = System.nanoTime();

//        if (log.isInfoEnabled())
//            log.info("Running " + tasks.size() + " tasks in parallel (#active="
//                    + getThreadPool().getActiveCount() + ", queueSize="
//                    + getThreadPool().getQueue().size() + ") : "
//                    + tasks.get(0).toString());

        // for each task
        final Iterator<Callable<T>> titr = tasks.iterator();

        // values collected from task futures.
        final List<T> list = new LinkedList<T>();

        while (titr.hasNext()) {

            // chunk of tasks.
            final List<Callable<T>> taskChunk = new LinkedList<Callable<T>>();

            // collect a chunk of tasks to run in parallel.
            while (taskChunk.size() < maxParallel && titr.hasNext()) {

                taskChunk.add(titr.next());

            }

            if (nanos <= 0)
                throw new TimeoutException();

            // run chunk of tasks.
            final List<Future<T>> futures = getThreadPool().invokeAll(
                    taskChunk, nanos, TimeUnit.NANOSECONDS);

            // collect values from task futures.
            final Iterator<Future<T>> fitr = futures.iterator();

            while (fitr.hasNext()) {

                final Future<T> f = fitr.next();

                list.add(f.get());

            }

            // update remaining nanoseconds.
            nanos = timeout - (System.nanoTime() - begin);

        }

        if (log.isInfoEnabled())
            log.info("Ran " + tasks.size() + " tasks in parallel: elapsed="
                    + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin));

        return list;

    }

}
