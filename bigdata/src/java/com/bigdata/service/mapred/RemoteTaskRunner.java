/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Sep 23, 2007
 */

package com.bigdata.service.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for running the tasks of a specific job against a set of
 * {@link IJobAndTaskService}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo refactor to use discovery of services and rediscovery on failure (via a
 *       delegate that manages the services on which tasks are executed so that
 *       we can subclass freely while also changing the service infrastructure
 *       freely).
 * 
 * @todo support re-execution of long-running tasks (on a different service!)
 *       Make sure that we cancel the remaining instance(s) of a task once it
 *       completes.
 * 
 * @todo Support the concept of buffering outputs across tasks run on the same
 *       service.
 */
public class RemoteTaskRunner<M extends IJobMetadata,T extends ITask> {

    public static final transient Logger log = Logger
            .getLogger(RemoteTaskRunner.class);

    /**
     * The services on which we will run tasks. The key is the service UUID. The
     * value is the service interface. Services may be dynamically removed from
     * this collection when they become unavailable or added to this collection
     * as they become available.
     */
    protected final Map<UUID, IJobAndTaskService<M, T>> services = new ConcurrentHashMap<UUID, IJobAndTaskService<M, T>>();

    /**
     * The services that can be choosen from. This array is updated from time to
     * time as services are discovered or become unavailable.
     * 
     * Note: the reverse conversion from the service UUID to the service proxy
     * is not necessary since we have the service proxy already on hand and can
     * just use that reference rather than the UUID (the problem is that getting
     * the UUID from the reference is an RPC.)
     */
    private UUID[] serviceUUIDs;
    
    /**
     * The next service in {@link #serviceUUIDs} to be assigned a task using a
     * round-robin approach.
     */
    private int nextService = 0;

    private final long begin = System.currentTimeMillis();

    private long elapsed = 0L;
    
    private boolean done = false;
    
    /**
     * The elapsed time since this object was created and started running tasks
     * (the clock stops when the operation is over).
     */
    public long elapsed() {

        if (!done) {

            // update the elapsed time unless we are done.
            
            elapsed = System.currentTimeMillis() - begin;
            
        }
        
        return elapsed;
        
    }
    
    /**
     * The job metadata for the tasks to be run.
     */
    protected final M jobMetadata;

    /**
     * The iterator from which we read the tasks to be executed.
     */
    private final Iterator<T> tasks;
    
    /**
     * A {@link Timer} for each remote service. The timer is used to send
     * heartbeat messages.
     * <p> 
     * Note: {@link Timer}s are cancelled when possible but will be garbage
     * collected in anycase.
     */
    protected final Map<UUID,Timer> timers = new ConcurrentHashMap<UUID, Timer>();

    /**
     * Tasks that are currently running somewhere (actually, just tasks that
     * have been submitted to a remote service and on which we are awaiting
     * completion).
     * 
     * FIXME This should be a delay queue so that we do not wait forever on a
     * task that was submitted to a service which has since died. Combine
     * {@link RetryTask} with {@link Master#TaskState} (in CVS).
     * 
     * FIXME If the service dies, then retry all jobs that were running (vs
     * already completed) on that service.
     */
    Map<UUID,RetryTask<T>> running = new ConcurrentHashMap<UUID, RetryTask<T>>();
    
    /**
     * A single threaded service used to submit (and retry) tasks for execution
     * on a remote service.
     * <p>
     * Note: this service also limits reading on the task iterator to a single
     * thread. This is important since we can not rely on the iterator being
     * safe for concurrent readers.
     * 
     * @todo if the {@link ReadTasksCommand} dies then progress will stop since
     *       we are not re-submitting tasks to this service.  We should buffer
     *       the tasks in a taskQueue and then run a pool of thread that submit
     *       those tasks to remote services.
     */
    final ExecutorService taskReaderService = Executors
            .newSingleThreadExecutor(DaemonThreadFactory.defaultThreadFactory());
    
    /**
     * Thread drains {@link Outcome}s from remote services.
     */
    final ExecutorService drainOutcomesService = Executors
            .newSingleThreadExecutor(DaemonThreadFactory.defaultThreadFactory());
    
// /**
//     * A bounded queue of tasks to be submitted to a remote service. This is
//     * populated by the {@link #taskReaderService}. The capacity of this queue
//     * is not very important as it is just buffering for the task iterator.
//     */
//    final ArrayBlockingQueue<RetryTask<T>> taskQueue = new ArrayBlockingQueue<RetryTask<T>>(100);
    
    /**
     * An unbounded queue of tasks that we will submit to a remote service. If a
     * task fails and its retry counter permits it will be place into this queue
     * for re-execution.
     */
    final LinkedBlockingQueue<RetryTask<T>> retryQueue = new LinkedBlockingQueue<RetryTask<T>>();
    
    /**
     * The maximum #of tasks that will be submitted for execution in parallel
     * across the available remote services.
     */
    protected final int maxConcurrency;
    
    /**
     * The maximum #of times that we will try to re-execute a task. When 0 we
     * will always retry the task.
     */
    protected final int maxTaskRetries;
    
    /**
     * Individual tasks will timeout after this many milliseconds after they
     * begin to execute on a remote service (use 0L to wait forever).
     */
    protected final long taskTimeout;
    
    /**
     * When non-zero, at most this many tasks will be executed (not counting
     * retries). This may be used to test out a distributed operation on a
     * subset of the input data.
     */
    protected final long maxTasks;

    /**
     * The #of new tasks submitted so far (does not count retries). 
     */
    protected long ntasks  = 0;
    
    /**
     * The #of tasks that were successfully completed.
     * 
     * @todo if we have to retry "success" tasks due to a failure before their
     *       buffered outputs were written then this will need to be adjusted
     *       downwards for each such task that we are forced to rerun.
     */
    protected long nsuccess = 0;
    
    /**
     * The #of tasks that were retried (this counts each retry of each task).
     */
    protected long nretried = 0;
    
    /**
     * The #of tasks that permanently failed.
     */
    protected long nfailed = 0;
    
    /**
     * 
     * @param services
     *            An array of services on which to run the tasks.
     * @param jobMetadata
     *            The job metadata
     * @param tasks
     *            An iterator that will visit the tasks to run.
     * @param maxTasks
     *            When non-zero, at most this many tasks will be executed (not
     *            counting retries). This may be used to test out a distributed
     *            operation on a subset of the input data.
     * @param maxConcurrency
     *            The maximum concurrency for the tasks - no more than this many
     *            tasks will execute concurrently across the available services
     *            (positive integer).
     * @param maxTaskRetries
     *            The maximum #of times to re-try a given task (non-negative
     *            integer, 0 disables retry).
     * @param taskTimeout
     *            Individual tasks will timeout after this many milliseconds
     *            after they begin to execute on a remote service (use 0L to
     *            wait forever).
     */
    public RemoteTaskRunner(IJobAndTaskService<M, T> services[], M jobMetadata,
            Iterator<T> tasks, long maxTasks, int maxConcurrency,
            int maxTaskRetries, long taskTimeout) {

        if (services == null)
            throw new IllegalArgumentException();
        
        if (services.length == 0)
            throw new IllegalArgumentException();
        
        if (jobMetadata == null)
            throw new IllegalArgumentException();
        
        if (tasks == null)
            throw new IllegalArgumentException();

//        if (!tasks.hasNext())
//            return; // NOP.

        if (maxTasks < 0)
            throw new IllegalArgumentException();

        if (maxConcurrency < 1)
            throw new IllegalArgumentException();

        if (maxTaskRetries < 0)
            throw new IllegalArgumentException();

        if (taskTimeout < 0L)
            throw new IllegalArgumentException();

        this.jobMetadata = jobMetadata;

        this.tasks = tasks;

        this.maxTasks = maxTasks;
        
        this.maxConcurrency = maxConcurrency;

        this.maxTaskRetries = maxTaskRetries;
        
        this.taskTimeout = taskTimeout;
        
        /*
         * Start the job on the each remote service and also start a heartbeat
         * timer to keep the job alive on each service.
         * 
         * This also builds up an array of the "live" services.
         */
        int nservices = 0;
        UUID[] serviceUUIDs = new UUID[services.length];
        {

            for (int i = 0; i < services.length; i++) {

                IJobAndTaskService<M, T> service = services[i];

                // the service identifier.
                final UUID serviceUUID;
                try {

                    // get the service identifier.
                    serviceUUID = service.getServiceUUID();
                    
                } catch (Exception ex) {

                    log.warn("Could not obtain UUID for service# "+i, ex);

                    continue;

                }

                try {
                    
                    log.info("Will start job on service: "+serviceUUID);
                    
                    // start the job on the service.
                    service.startJob(jobMetadata);

                    // add to the cache of "live" services.
                    this.services.put(serviceUUID, service);
                    
                    serviceUUIDs[nservices++] = serviceUUID;

                    log.info("Started job on "+serviceUUID);

                } catch (Exception ex) {

                    log.warn("Could not start job on service: " + serviceUUID, ex);

                    continue;

                }

                /*
                 * Setup heartbeat timer for that service.
                 * 
                 * @todo replace with a ScheduledExecutorService so that the
                 * timer can not prevent the application from exiting (it is not
                 * a daemon thread).
                 */

                Timer timer = new Timer();

                timer.schedule(
                        new HeartbeatTask(service, jobMetadata.getUUID()),
                        0/* delay(ms) */, 100/* period(ms) */);

                timers.put(serviceUUID, timer);

            }

            if(nservices==0) {
                
                throw new RuntimeException("No services were available");
                
            }
            
        }

        /*
         * Populate the array of available service UUIDs.
         * 
         * @todo when updating: synchronized(serviceUUIDs).
         */
        {

            this.serviceUUIDs = new UUID[nservices];

            int j = 0;
            
            for(int i=0; i<services.length; i++) {
            
                UUID uuid = serviceUUIDs[i];
                
                if(uuid!=null) {

                    this.serviceUUIDs[j] = serviceUUIDs[i];
                    
                }
                
            }
            
            nextService = 0;
            
        }
        
        /*
         * Set up services and queues used to manage the execution of remote
         * tasks.
         */
        
        // start the task completion service (drains Outcomes).
        drainOutcomesService.execute(new DrainOutcomesCommand());
        
        // start the task reader (submits tasks).
        taskReaderService.execute(new ReadTasksCommand());

    }

    /**
     * Reads tasks from the retry queue and task iterator, submitting them to
     * run on a remote service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo modify this so it does not so strongly prefer to retry a task so
     *       that tasks that are not failing will continue to progress.
     */
    class ReadTasksCommand implements Runnable {

        public void run() {

            while (true) {

                int nrunning = running.size();

                if (nrunning >= maxConcurrency) {

                    if(!snooze()) return;

                    continue;
                    
                }

                // look for a task to be re-tried.
                RetryTask<T> task = retryQueue.poll();

                if (task != null) {

                    log.info("Retrying task: " + task.task.getUUID()
                            + ", ntries=" + task.ntries);

                    nretried++;
                    
                }

                if (task == null) {

                    /*
                     * Get the next task to be executed (from the ctor).
                     * 
                     * Note: We will start at most maxTasks distinct tasks when
                     * maxTasks is non-zero.
                     */

                    if ((maxTasks==0||ntasks<maxTasks) && tasks.hasNext()) {

                        T t = tasks.next();

                        task = new RetryTask<T>(t);

                        log.info("New task: " + t.getUUID());

                    }

                }

                if (task == null) {

                    if (running.isEmpty()) {

                        /*
                         * Nothing to run and nothing running.
                         */

                        done();

                        return;

                    }

                    if(!snooze()) return;

                } else {

                    /*
                     * Submit the task to a service.
                     */

                    if (!submit(jobMetadata.getUUID(), task)) {

                        /*
                         * We were unable to submit the task to the service so
                         * increment its retry counter and place the task on the
                         * retry queue.
                         * 
                         * Note: We increment the retry counter in case there
                         * was a problem with this specific task rather than the
                         * service to which we tried to submit the task.
                         */
                        
                        if (task.ntries++ < maxTaskRetries) {

                            retryQueue.add(task);

                        } else {

                            log.warn("Could not submit: task="
                                    + task.task.getUUID() + ", ntries="
                                    + task.ntries);

                        }

                    } else {

                        /*
                         * Count the #of new task starts.
                         * 
                         * Note: this must be an atomic counter if we are
                         * starting tasks from multiple threads.
                         */
                        if(task.ntries==0) ntasks++;
                        
                    }

                }

            }

        }

        /**
         * Notify folks waiting on us that we are done.
         */
        protected void done() {

            log.info("Done");

            synchronized (semaphore) {

                semaphore.notifyAll();

            }

        }
        
        /**
         * Wait for something to complete.
         * 
         * @return true if execution should continue.
         */
        protected boolean snooze() {

            try {
           
                Thread.sleep(50/* ms */);
                
                return true;
                
            } catch (InterruptedException ex) {
                
                log.info("Interrupted while waiting for tasks to complete", ex);

                // exit on interrupt.
                return false;
                
            }

        }

    }

    /**
     * Drains {@link Outcome}s. If the {@link Outcome} was {@link Status#Error}
     * and the retry count is not exceeded, then the task will be placed on a
     * queue for re-execution.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class DrainOutcomesCommand implements Runnable {

        public void run() {

            while(true) {
                
                final UUID job = jobMetadata.getUUID();

                Iterator<Map.Entry<UUID, IJobAndTaskService<M, T>>> itr = services
                        .entrySet().iterator();

                while (itr.hasNext()) {

                    final Map.Entry<UUID, IJobAndTaskService<M, T>> entry;
                    try {

                        entry = itr.next();

                    } catch (NoSuchElementException ex) {

                        log.info("Concurrent removal of service");

                        continue;

                    }

                    UUID serviceUUID = entry.getKey();

                    IJobAndTaskService<M, T> service = entry.getValue();

                    final Outcome[] outcomes;
                    try {

                        outcomes = service.drain(job);

                    } catch (IOException ex) {

                        log.warn("Could not cancel job: service=" + serviceUUID
                                + ", job=" + job);

                        continue;
                        
                    }
                    
                    for(int i=0; i<outcomes.length; i++) {
                        
                        Outcome outcome = outcomes[i];

                        RetryTask<T> task = running.remove(outcome.getTask());

                        if(task==null) {
                            
                            /*
                             * @todo This can happen if multiple instances of
                             * the same task were distributed. The task will be
                             * removed the first time it has an outcome. This
                             * will have to be modified to support redundent
                             * execution of slow tasks.
                             */
                            
                            log.warn("Task not in local state: "+outcome.getTask());
                            
                            continue;
                            
                        }
                        
                        switch(outcome.getStatus()) {
                        
                        case Success:
                            
                            log.info("Success: "+outcome.getTask()+", message="+outcome.getMessage());

                            nsuccess++;
                            
                            continue;
                            
                        case Cancelled:

                            log.info("Cancelled: "+outcome.getTask()+", message="+outcome.getMessage());
                            
                            continue;

                        case Error:
                            
                            log.warn("Error: "+outcome.getTask()+", message="+outcome.getMessage());
                            
                            if(task.ntries++<maxTaskRetries) {
                            
                                // retry.
                                retryQueue.add(task);
                                
                            } else {
                                
                                // permanent failure.
                                log.error("Task did not succeed: "+outcome.getTask());

                                nfailed++;
                                
                            }
                            
                            continue;

                        }
                        
                    }
                
                } // next service.

                // sleep a moment to give the services time to run.
                try {
                    
                    Thread.sleep(200/*ms*/);
                    
                } catch(InterruptedException ex) {
                    
                    log.info("Interrupted: "+ex, ex);
                    
                    // exiting on interrupt.
                    return;
                    
                }
                
            }
            
        }
        
    }
    
    /**
     * Shuts down our internal services.
     */
    protected void shutdownNow() {

        // do not read any more tasks.
        taskReaderService.shutdownNow();
        
        // do not submit any more tasks.
        drainOutcomesService.shutdownNow();

    }
    
    /**
     * Cancels the job on all services
     */
    protected void cancelJob() {

        log.info("Cancelling job=" + jobMetadata.getUUID());
        
        /*
         * cancel the job on each remote service.
         */
        {

            final UUID job = jobMetadata.getUUID();

            Iterator<Map.Entry<UUID, IJobAndTaskService<M, T>>> itr = services
                    .entrySet().iterator();

            while (itr.hasNext()) {

                final Map.Entry<UUID, IJobAndTaskService<M, T>> entry;
                try {

                    entry = itr.next();

                } catch (NoSuchElementException ex) {

                    log.info("Concurrent removal of service");

                    continue;

                }

                UUID serviceUUID = entry.getKey();

                IJobAndTaskService<M, T> service = entry.getValue();

                try {

                    service.cancelJob(job);

                } catch (IOException ex) {

                    log.warn("Could not cancel job: service=" + serviceUUID
                            + ", job=" + job);

                }

                // Stop the timer generating heartbeats for the job on that
                // service.
                {

                    Timer timer = timers.get(serviceUUID);

                    if (timer == null) {

                        log.error("No timer?: " + serviceUUID);

                    } else {

                        timer.cancel();

                    }

                }

                // remove the service from our pool of services.
                try {

                    itr.remove();

                } catch (NoSuchElementException ex) {

                    log.info("Concurrent removal of service");

                    continue;

                }

            }

        }
        
    }

    /**
     * Submit the task to the next available service.
     * 
     * @return if the task was submitted to the service.
     * 
     * @todo select the service that is "closest" to the data to be read (this
     *       rule works for both map and reduce). On a cluster where the map or
     *       reduce service is available on each host and the data is stored in
     *       local files (using a distributed file system with replication of
     *       the data) across the hosts, then you should always be able to do a
     *       local read of the data by running the task on the same host (or a
     *       close network neighbor).
     */
    protected boolean submit(UUID job, RetryTask<T> task) {

        if(job==null) throw new IllegalArgumentException();

        if(task==null) throw new IllegalArgumentException();
        
        final UUID taskUUID = task.task.getUUID();
        
        final UUID serviceUUID;
        
        synchronized(serviceUUIDs) {
        
            serviceUUID = serviceUUIDs[nextService];
            
            nextService = (nextService+1) % serviceUUIDs.length;
        
        }
        
        IJobAndTaskService<M, T> service = services.get(serviceUUID);
        
        if(service==null) {

            throw new AssertionError("Service not found: "+serviceUUID);
            
        }
        
        try {

            /*
             * Note: We add the task to the collection of running tasks _before_
             * we submit it to the remote service in order avoid early
             * termination when running isEmpty() and no tasks remain to
             * execute.
             */
            running.put(taskUUID,task);

            service.submit(jobMetadata.getUUID(), task.task, taskTimeout);            
            
            return true;
            
        } catch(IOException ex) {
       
            log.warn("Could not submit task: service="+serviceUUID+", task="+taskUUID, ex);
            
            return false;
            
        }
        
    }
    
    /**
     * Await termination of all scheduled tasks. The job is cancelled once all
     * tasks have run or when the timeout occurs, whichever comes first.
     * 
     * @return The success rate in [0:1.0].
     * 
     * @throws InterruptedException
     */
    public void awaitTermination() throws InterruptedException {
    
        awaitTermination(0L);
        
    }

    Object semaphore = new Object();
    
    public void awaitTermination(long timeout) throws InterruptedException {
        
        synchronized(semaphore) {

            semaphore.wait(timeout);
            
        }

        terminate();
        
    }
    
    /**
     * Terminate all tasks and shuts down this service.
     */
    public void terminate() {

        shutdownNow();
        
        cancelJob();

        // update the elapsed time again since we are now done.
        elapsed();
        
        done = true;
        
    }
        
    /**
     * Timer task runs as a daemon and sends heartbeats for a given job. The #of
     * completed tasks for that job is then updated on the
     * {@link RemoteTaskRunner}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <M>
     * @param <T>
     */
    protected class HeartbeatTask extends TimerTask {
        
        final IJobAndTaskService<M,T> service;
        
        final UUID job;
        
        public HeartbeatTask(IJobAndTaskService<M,T> service, UUID job) {
         
            if (service == null)
                throw new IllegalArgumentException();

            if (job == null)
                throw new IllegalArgumentException();
            
            this.service = service;
            
            this.job = job;
            
        }
        
        public void run() {

            try {
                
                service.heartbeat(job);
                
//                int n = service.heartbeat(job);
//
//                // update the #of completed tasks for that service.
//                completed.put(service.getServiceUUID(), Integer.valueOf(n));
                
            } catch(IOException ex) {
        
                log.warn("Could not send heartbeat: "+ex, ex);
                
            }
            
        }
        
    }

    /**
     * Wraps up an {@link ITask} and a retry counter.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     */
    protected static class RetryTask<T extends ITask> {
        
        public int ntries = 0;
        
        public final T task;
        
        public RetryTask(T task) {
            
            this.task = task;
            
        }
        
    }
        
}
