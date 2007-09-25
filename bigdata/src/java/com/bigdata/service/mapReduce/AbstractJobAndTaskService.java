/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Sep 23, 2007
 */

package com.bigdata.service.mapReduce;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for services implementing {@link IJobAndTaskService}.
 * <p>
 * Concrete subclasses must:
 * <ul>
 * <li>provide a factory for the {@link AbstractTaskWorker workers} that will
 * execute the tasks. </li>
 * <li>implement {@link IJobAndTaskService#getServiceUUID()}.</li>
 * </ul>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractJobAndTaskService<M extends IJobMetadata, T extends ITask>
        implements IServiceShutdown, IJobAndTaskService<M, T> {

    public static final transient Logger log = Logger
            .getLogger(AbstractJobAndTaskService.class);

    /**
     * Options for
     * {@link AbstractJobAndTaskService#AbstractJobAndTaskService(Properties)}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options {

        /**
         * The thread pool size (#of workers). The default is a constant
         * multiplier times the #of CPUs available on the platform.
         * 
         * @see #DEFAULT_THREAD_POOL_SIZE
         */
        public static final String THREAD_POOL_SIZE = "threadPoolSize"; 
        
        /**
         * The default thread pool size is 50 per CPU.
         */
        public static final String DEFAULT_THREAD_POOL_SIZE = ""+SystemUtil.numProcessors()*50;  
        
        /**
         * The maximum time that a job may continue to run without receiving a
         * heartbeat message (default is 3000ms).  A value of ZERO (0L) will
         * disable the {@link HeartbeatMonitorTask}.
         */
        public static final String HEARTBEAT_TIMEOUT = "heartbeatTimeout";
        
        /**
         * The default maximum heartbeat timeout (3000ms).
         */
        public static final String DEFAULT_HEARTBEAT_TIMEOUT = "3000";
        
        /**
         * The period between runs of the {@link HeartbeatMonitorTask} (it looks for
         * jobs that have not received a heartbeat).
         */
        public static final String HEARTBEAT_PERIOD = "heartbeatTimeout";
        
        /**
         * The default period between runs of the {@link HeartbeatMonitorTask}
         * (1000ms).
         */
        public static final String DEFAULT_HEARTBEAT_PERIOD = "1000";
        
    }
    
    /**
     * Text of the error message used when the UUID of a job is not available
     * from the {@link IJobMetadata} provided to
     * {@link IJobAndTaskService#startJob(IJobMetadata)}.
     */
    public static final String ERR_NO_JOB_IDENTIFIER = "no job identifier";
    
    /**
     * Text of the error message used when the UUID of a job is already known to
     * the service.
     */
    public static final String ERR_JOB_EXISTS = "job already registered";
    
    /**
     * Text of the error message used when the UUID of a job is not know to this
     * service.
     */
    public static final String ERR_NO_SUCH_JOB = "job not registered";
    
    /**
     * Job state that is relevant to this service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class JobState<M> {

        /**
         * The job identifier.
         */
        final UUID uuid;

        /**
         * The job metadata.
         */
        final M metadata;
        
        /**
         * Used to access the federation from which inputs will be read/written.
         * 
         * @todo break into an input federation and an output federation so that
         * you can run a map/reduce across federations.
         */
        final IBigdataClient client;
        
        /**
         * The job start time.
         */
        final long begin = System.currentTimeMillis();
        
        /**
         * The time at which the last heartbeat was received for this job.
         */
        long heartbeat = begin;
        
        /**
         * The #of tasks started for this job on this service.
         */
        long nstarted = 0L;
        
        /**
         * The #of tasks ended that have produced an {@link Outcome} for this
         * job on this service.
         */
        long nended = 0L;

        /**
         * The running tasks for this job. The key is the task UUID. The value
         * is the {@link Future} for that task.
         */
        Map<UUID,Future<Object>> futures = new ConcurrentHashMap<UUID, Future<Object>>();
        
        /**
         * The {@link Outcome}s for the completed tasks. {@link Outcome}s are
         * placed into this queue by the {@link AbstractTaskWorker}.
         */
        BlockingQueue<Outcome> outcomes = new LinkedBlockingQueue<Outcome>(/*no capacity limit*/);
        
        /**
         * The job identifier.
         */
        public UUID getUUID() {
            
            return uuid;
            
        }
        
        public JobState(UUID uuid, M metadata, IBigdataClient client) {

            if (uuid == null)
                throw new IllegalArgumentException();

            if (metadata == null)
                throw new IllegalArgumentException();

            if (client == null)
                throw new IllegalArgumentException();

            this.uuid = uuid;
            
            this.metadata = metadata;
            
            this.client = client;
            
        }

        /**
         * Cancel all running tasks for this job.
         * <p>
         * Note: The job MUST be cancelled first since otherwise tasks could
         * continue to be queued while this method is running.
         * <p>
         * Note: {@link Outcome}s are discarded when the job is cancelled.
         */
        public void cancelAll() {

            int n = 0;
            
            Iterator<Future<Object>> itr = futures.values().iterator();
            
            while(itr.hasNext()) {
                
                Future<Object> future;
                
                try {
                 
                    future = itr.next();
                    
                } catch(NoSuchElementException ex) {
                    
                    MapService.log.info("Exhausted by concurrent completion.");
                    
                    break;
                    
                }
                
                future.cancel(true/*may interrupt if running*/);
                
                n++;
                
                try {
                    
                    itr.remove();
                    
                } catch(NoSuchElementException ex) {
                    
                    MapService.log.info("Task already gone.");
                    
                }
                
            }
            
            MapService.log.info("Cancelled "+n+" tasks for job="+uuid);

            // discard the outcomes when the job is cancelled.
            outcomes.clear();
            
        }

        /**
         * Cancel the task if it is running.
         * 
         * @param task
         *            The task identifier.
         * 
         * @return true if the job was cancelled.
         */
        public boolean cancel(UUID task) {
            
            Future<Object> future = futures.remove(task);
            
            if(future!=null && future.cancel(true/*may interrupt if running*/)) {

                MapService.log.info("Cancelled task: job="+uuid+", task="+task);
                
                // Note: This is done by the AbstractTaskWorker.
//                outcomes.add(new Outcome(task,Status.Cancelled,null));
                
                return true;
                
            } else {
                
                MapService.log.info("Could not cancel task - not running? : job="+uuid+", task="+task);

                return false;
                
            }
            
        }

        /**
         * A human readable summary of the {@link JobState}.
         */
        public String status() {
            
            // #of active tasks at this instant (can change asynchronously).
            final int nactive = futures.size();
            
            // #of outcomes at this instant (can change asynchronously).
            final int noutcomes = outcomes.size();

            return "job=" + uuid + ", nactive=" + nactive + ", noutcomes="
                    + noutcomes + ", nstarted=" + nstarted + ", nended="
                    + nended;
            
        }
        
    }
    
    /**
     * Abstract base class for task workers running in the {@link MapService} or the
     * {@link ReduceService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static abstract public class AbstractTaskWorker<M,T extends ITask> implements Callable<Object> {

        /**
         * The job state.
         */
        protected final JobState<M> jobState;
        
        /**
         * The task to be executed.
         */
        protected final T task;
        
        protected AbstractTaskWorker(JobState<M> jobState, T task) {

            if (jobState== null)
                throw new IllegalArgumentException();

            if (task == null)
                throw new IllegalArgumentException();

            this.jobState = jobState;
            
            this.task = task;

        }

        /**
         * Invokes {@link #run()} to run the task. On completion, an
         * {@link Outcome} is constructed and placed into
         * {@link JobState#outcomes}.
         * 
         * @return null
         */
        public Object call() throws Exception {

            try {
                
                run();

                jobState.outcomes.add(new Outcome(task.getUUID(),
                        Status.Success, null));

            } catch(IllegalMonitorStateException ex) {

                /*
                 * Note: This is the exception thrown if a job is cancelled.
                 */
                
                jobState.outcomes.add(new Outcome(task.getUUID(),
                        Status.Cancelled, null /*
                                                 * the message for this
                                                 * exception is more confusing
                                                 * than useful.
                                                 */ )); 
                        //ex.getMessage())); 
                
                throw ex;
                
            } catch(Throwable t) {
                
                jobState.outcomes.add(new Outcome(task.getUUID(),
                        Status.Error, t.getMessage()));

            }

            // and remove from the futures since we are done.
            jobState.futures.remove(task.getUUID());
            
            jobState.nended++;
            
            return null;
            
        }
        
        /**
         * Actually run the task.
         * 
         * @throws Exception
         */
        abstract protected void run() throws Exception;
        
    }

    /**
     * The size of the thread pool.
     */
    final int threadPoolSize;
    
    /**
     * Queue of executing {@link ITask}s.
     * 
     * @see Options#THREAD_POOL_SIZE
     */
    final protected ExecutorService taskService;

    /**
     * Service that looks for task execution timeouts and cancels a task if it
     * exceeds its timeout.
     */
    final protected ScheduledExecutorService timeoutService = Executors
            .newScheduledThreadPool(1, DaemonThreadFactory
                    .defaultThreadFactory());

    /**
     * Service that looks for heartbeat messages and cancels a job if it misses
     * more than N heartbeats.
     */
    final protected Timer heartbeatMonitor;

    /**
     * The heartbeat timeout (ms).  A job will be cancelled after this
     * much time without receiving a heartbeat.
     * 
     * See {@link Options#HEARTBEAT_TIMEOUT}
     */
    final protected long heartbeatTimeout;

    /**
     * The heartbeat period (ms). The {@link HeartbeatMonitorTask} will run every N
     * milliseconds, where N is the {@link #heartbeatPeriod}.
     * 
     * See {@link Options#HEARTBEAT_PERIOD}
     */
    final protected long heartbeatPeriod;

    /**
     * A service used by the {@link HeartbeatMonitorTask} to cancel jobs.
     * The use of this services keeps down the latency of the timer
     * task.
     */
    final protected ExecutorService cancelService;
    
    /**
     * @param properties
     *            See {@link Options}
     */
    protected AbstractJobAndTaskService(Properties properties) {

        threadPoolSize = Integer.parseInt(properties.getProperty(
                Options.THREAD_POOL_SIZE, Options.DEFAULT_THREAD_POOL_SIZE));

        taskService = Executors.newFixedThreadPool(threadPoolSize,
                DaemonThreadFactory.defaultThreadFactory());

        heartbeatTimeout = Long.parseLong(properties.getProperty(
                Options.HEARTBEAT_TIMEOUT, Options.DEFAULT_HEARTBEAT_TIMEOUT));

        if (heartbeatTimeout < 0)
            throw new IllegalArgumentException(
                    Options.HEARTBEAT_TIMEOUT+"="
                    + heartbeatTimeout+", but must be non-negative.");
        
        heartbeatPeriod = Long.parseLong(properties.getProperty(
                Options.HEARTBEAT_PERIOD, Options.DEFAULT_HEARTBEAT_PERIOD));

        if (heartbeatPeriod <= 0)
            throw new IllegalArgumentException(
                    Options.HEARTBEAT_PERIOD+"="
                            + heartbeatPeriod + ", but must be positive.");

        if (heartbeatTimeout > 0) {

            heartbeatMonitor = new Timer();

            heartbeatMonitor.schedule(new HeartbeatMonitorTask(), 0/* delay(ms) */,
                    heartbeatPeriod/* period(ms) */);

            cancelService = Executors.newFixedThreadPool(5, DaemonThreadFactory
                    .defaultThreadFactory());

        } else {
            
            heartbeatMonitor = null;

            cancelService = null;
            
        }
        
    }

    public void shutdown() {

        taskService.shutdown();

        /*
         * note: the heartbeat monitor stays up during normal shutdown so that
         * we can still monitor for jobs that are no longer being maintained and
         * cancel them.
         */

        if (heartbeatMonitor != null)
            heartbeatMonitor.cancel();

        if (cancelService != null)
            cancelService.shutdown();

    }

    public void shutdownNow() {

        taskService.shutdownNow();

        if (heartbeatMonitor != null)
            heartbeatMonitor.cancel();

        if (cancelService != null)
            cancelService.shutdown();

    }

    /**
     * @deprecated The {@link IBigdataClient} should be initialized from the
     *             {@link IJobMetadata} a per job basis. This will let us: (a)
     *             reuse these services across federations; and (b) avoid mishap
     *             when the client is connected to one federation and it tries
     *             to discover services connected to another federation.
     *             <p>
     *             This approach requires a weak value hash map of the clients
     *             and a hard reference to the client in the job. Once no job
     *             references a client that client should be disconnected from
     *             its federation.  Rather than always minting a client for a
     *             job, we could also first check the hash map for an existing
     *             client for the same federation.
     *             <p>
     *             It also makes sense to allow a different client for input and
     *             output so that a map/reduce job can cross federations.
     * 
     * @return The client used to read/write data.
     */
    public abstract IBigdataClient getBigdataClient();
    
    /**
     * Factory for job state.
     * 
     * @param job
     *            The job identifier.
     * @param metadata
     *            The job metadata.
     * @param client
     *            Used to access the {@link IBigdataFederation} when reading or
     *            writing data.
     * 
     * @return A {@link JobState} object.
     */
    protected JobState<M> newJobState(UUID job,M metadata,IBigdataClient client) {
        
        return new JobState<M>(job,metadata,client);
        
    }
    
    /**
     * Factory for task workers.
     * 
     * @param jobState
     *            The job state maintained by this service.
     * @param task
     *            The task to be executed.
     * 
     * @return The worker that will execute a given task.
     */
    abstract AbstractTaskWorker<M, T> newTaskWorker(JobState<M> jobState, T task);
    
    /**
     * Running jobs.
     */
    final protected Map<UUID,JobState<M>> jobs = new ConcurrentHashMap<UUID, JobState<M>>();

    public void startJob(M jobMetadata) {
        
        if(jobMetadata==null) throw new IllegalArgumentException();
        
        final UUID job = jobMetadata.getUUID();
        
        if(job==null) throw new IllegalArgumentException(ERR_NO_JOB_IDENTIFIER);

        if(jobMetadata==null) throw new IllegalArgumentException();
        
        if(jobs.containsKey(job)) {
            
            throw new IllegalStateException(ERR_JOB_EXISTS+" : "+job);
            
        }
        
        /*
         * FIXME The bigdata client should be initialized based on the
         * jobMetadata so that different jobs map run against different
         * federations.
         */
        jobs.put(job, newJobState(job,jobMetadata,getBigdataClient()));

        log.info("job=" + job );
        
    }

    public void cancelJob(UUID job) {

        if(job==null) throw new IllegalArgumentException();

        JobState<M> jobState = jobs.remove(job);
        
        if(jobState==null) {
            
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+job);
            
        }

        log.info("job=" + job );

        jobState.cancelAll();
        
    }

    /**
     * This task runs periodically.  It reviews each running job and cancels any
     * job that has not received a heartbeat in at least N seconds.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class HeartbeatMonitorTask extends TimerTask {

        public void run() {

            if(jobs.isEmpty()) return;

            Iterator<JobState<M>> itr = jobs.values().iterator();

            final long now = System.currentTimeMillis();

            log.info("Checking for deadbeat jobs");
            
            while (itr.hasNext()) {

                try {

                    JobState<M> jobState = itr.next();

                    long elapsed = now - jobState.heartbeat;

                    long overdue = elapsed - heartbeatTimeout; 
                    
                    if (overdue > 0) {

                        log
                                .info("Scheduling deadbeat for cancellation: overdue="
                                        + overdue
                                        + "ms, job="
                                        + jobState.getUUID());

                        // schedule the job for cancellation.
                        cancelService.submit(new CancelWorker(jobState
                                .getUUID()));

                    }

                } catch (NoSuchElementException ex) {

                    // Iterator was concurrently exhausted.
                    break;

                }

            }

        }

    }

    /**
     * Cancels a job if its heartbeat has timed out.
     */
    class CancelWorker implements Callable<Object> {

        final UUID job;

        public CancelWorker(UUID job) {

            if (job == null)
                throw new IllegalArgumentException();

            this.job = job;

        }

        /**
         * Cancel the job if it has still not received a heartbeat.
         */
        public Object call() throws Exception {

            JobState<M> jobState = jobs.get(job);

            if (jobState == null) {

                log.info("Job already gone: " + job);

            } else if (System.currentTimeMillis() - jobState.heartbeat < heartbeatTimeout) {

                log.info("Last second reprieve: " + job);

            } else {

                log.warn("Cancelling job due to heartbeat timeout: " + job);

                // remove from the collection of running jobs.
                jobs.remove(job);
                
                // and cancel the job.
                jobState.cancelAll();

            }
            
            return null;

        }

    }

    public int heartbeat(UUID job) throws IOException {
        
        JobState<M> jobState = jobs.get(job);
        
        if (jobState == null)
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+job);

        // update the heartbeat.
        jobState.heartbeat = System.currentTimeMillis();

        log.info(jobState.status());
        
        // #of completed tasks for that job.
        return jobState.outcomes.size();
        
    }

    public Outcome[] drain(UUID job) {

        JobState<M> jobState = jobs.get(job);
        
        if (jobState == null)
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+job);

        // No outcomes are ready.
        if(jobState.outcomes.isEmpty()) return EMPTY;

        log.info(jobState.status());
        
        Collection<Outcome> outcomes = new LinkedList<Outcome>(); 
        
        jobState.outcomes.drainTo(outcomes);
        
        return outcomes.toArray(new Outcome[outcomes.size()]);
        
    }
    
    // Empty array used when there are no outcomes.
    private static final Outcome[] EMPTY = new Outcome[]{};

    // @todo move the input<File> into the IMapTask.
    public void submit(UUID job, T task, long timeout) {

        JobState<M> jobState = jobs.get(job);
        
        if (jobState == null)
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+job);
        
        log.info("task="+task.getUUID()+" : "+jobState.status());

        Future<Object> future = taskService
                .submit(newTaskWorker(jobState, task));

        if(timeout!=0L) {
            
            timeoutService.schedule(new CancelTaskWorker(job, task.getUUID()),
                    timeout, TimeUnit.MILLISECONDS);
            
        }
        
        jobState.futures.put(task.getUUID(), future);
        
        jobState.nstarted++;
        
    }
    
    /**
     * Cancels a task if its timeout is exceeded.
     */
    class CancelTaskWorker implements Callable<Object> {

        final UUID job;
        final UUID task;

        public CancelTaskWorker(UUID job, UUID task) {

            if (job == null)
                throw new IllegalArgumentException();

            if (task== null)
                throw new IllegalArgumentException();

            this.job = job;
            
            this.task = task;

        }

        /**
         * Cancel the task.
         */
        public Object call() throws Exception {

            JobState<M> jobState = jobs.get(job);

            if (jobState == null) {

                log.info("Job already gone: " + job);

            }
            
            log.warn("Cancelling task due to timeout: job=" + job+", task="+task);

            // and cancel the task.
            jobState.cancel(task);
            
            return null;

        }

    }

    public boolean cancel(UUID job, UUID task) {

        JobState<M> jobState = jobs.get(job);
        
        if (jobState == null)
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+job);

        log.info("job=" + job+", task="+task);
        
        return jobState.cancel(task);

    }

}
