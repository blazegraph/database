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

package com.bigdata.service.mapReduce;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.journal.BufferMode;
import com.bigdata.service.EmbeddedBigdataClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.mapred.AbstractJobAndTaskService;
import com.bigdata.service.mapred.IJobAndTaskService;
import com.bigdata.service.mapred.IJobMetadata;
import com.bigdata.service.mapred.ITask;
import com.bigdata.service.mapred.JobState;
import com.bigdata.service.mapred.Outcome;
import com.bigdata.service.mapred.Status;
import com.bigdata.service.mapred.AbstractJobAndTaskService.Options;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for the {@link AbstractJobAndTaskService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAbstractJobAndTaskService extends TestCase {

    /**
     * 
     */
    public TestAbstractJobAndTaskService() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAbstractJobAndTaskService(String arg0) {
        super(arg0);
    }

    /**
     * The client used by the unit tests.
     */
    IBigdataClient bigdataClient;
    
    public void setUp() throws Exception {
        
        Properties properties = new Properties();
        
        properties
                .setProperty(
                        com.bigdata.service.EmbeddedBigdataFederation.Options.BUFFER_MODE,
                        BufferMode.Transient.toString());
        
        bigdataClient = new EmbeddedBigdataClient(properties);
        
    }
    
    public void tearDown() throws Exception {
     
        if(bigdataClient!=null) {

            bigdataClient.terminate();
            
        }
        
    }

    public void test_ctor() {

        // default ctor.
        {
            
            MyJobAndTaskService service = new MyJobAndTaskService<MyJobMetadata, MyTask>(
                new Properties());

            System.err.println("threadPoolSize(default)="+service.threadPoolSize);
            
        }

        // override the thread pool size.
        {

            Properties properties = new Properties();
            
            final int expectedThreadPoolSize = 20;
            
            properties.setProperty(Options.THREAD_POOL_SIZE,""+expectedThreadPoolSize);
            
            MyJobAndTaskService service = new MyJobAndTaskService<MyJobMetadata, MyTask>(
                    properties);

            assertEquals("threadPoolSize",expectedThreadPoolSize,service.threadPoolSize);
            
        }
        
    }

    /**
     * Starts and ends a job and also tests correct rejection of start and
     * cancel of a job.
     */
    public void test_startJob_cancelJob() {
        
        MyJobAndTaskService<MyJobMetadata, MyTask> service = new MyJobAndTaskService<MyJobMetadata, MyTask>(
                new Properties());

        MyJobMetadata jobMetadata = new MyJobMetadata(UUID.randomUUID());

        assertEquals("#jobs",0,service.jobs.size());

        // start a job.
        service.startJob(jobMetadata);

        // correct rejection if we try to start the job twice.
        try {
            
            service.startJob(jobMetadata);
        
            fail("Expecting: "+IllegalStateException.class);
            
        } catch(IllegalStateException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
        assertEquals("#jobs",1,service.jobs.size());
        
        // cancel a job.
        service.cancelJob(jobMetadata.getUUID());
        
        assertEquals("#jobs",0,service.jobs.size());
        
        // correct rejection if we try to cancel a job twice.
        try {
            
            service.cancelJob(jobMetadata.getUUID());
        
            fail("Expecting: "+IllegalStateException.class);
            
        } catch(IllegalStateException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
    }
    
    /**
     * Interleaves the start and end of two jobs.
     */
    public void start_cancel_job2() {

        MyJobAndTaskService<MyJobMetadata, MyTask> service = new MyJobAndTaskService<MyJobMetadata, MyTask>(
                new Properties());

        MyJobMetadata jobMetadata1 = new MyJobMetadata(UUID.randomUUID());

        MyJobMetadata jobMetadata2 = new MyJobMetadata(UUID.randomUUID());

        assertEquals("#jobs",0,service.jobs.size());

        service.startJob(jobMetadata1);

        assertEquals("#jobs",1,service.jobs.size());

        service.startJob(jobMetadata2);

        assertEquals("#jobs",2,service.jobs.size());

        service.cancelJob(jobMetadata1.job);

        assertEquals("#jobs",1,service.jobs.size());

        service.cancelJob(jobMetadata2.job);
        
        assertEquals("#jobs",0,service.jobs.size());

    }

    /**
     * Starts a job, submits some tasks, and then cancels the job. Verifies that
     * all the tasks for that job are cancelled when the job is cancelled.
     * 
     * @throws InterruptedException 
     */
    public void test_run_tasks() throws InterruptedException {

        // #of tasks to run.
        final int ntasks = 20;

        Properties properties = new Properties();
        
        properties.setProperty(Options.THREAD_POOL_SIZE,""+ntasks);
        
        // use service that will cause each task to sleep endlessly.
        MyJobAndTaskService<MyJobMetadata, MyTask> service = new MySleepyJobAndTaskService<MyJobMetadata, MyTask>(
                properties );

        MyJobMetadata jobMetadata = new MyJobMetadata(UUID.randomUUID());

        // start the job on the service.
        service.startJob(jobMetadata);
        
        // the state of the job executing on the service.
        JobState jobState = service.jobs.get(jobMetadata.getUUID());
        
        assertNotNull(jobState);

        /*
         * Start some tasks on the service.
         */
        
        //RemoteTaskRunner<MyJobMetadata, MyTask>
        Collection<Callable<Object>> tasks = new LinkedList<Callable<Object>>();
        
        // setup tasks.
        for(int i=0; i<ntasks; i++) {
            
            MyTask task = new MyTask();
            
            tasks.add(new SubmitRemoteTask<MyJobMetadata, MyTask>(service,jobMetadata.getUUID(),task));
            
        }

        // used to submit the tasks in parallel.
        final ExecutorService taskService = Executors.newFixedThreadPool(ntasks,
                DaemonThreadFactory.defaultThreadFactory());

        /*
         * Submit the tasks.
         * 
         * Note: The only reason to review the futures returned here is to see
         * whether or not the submit() request was successful. The task Outcome
         * is recovered below when we drain() the completed tasks from the
         * service.
         */
        {

            List<Future<Object>> submitFutures = taskService.invokeAll(tasks);

            /*
             * Wait for the tasks to start.
             * 
             * Note: if not all tasks are running yet then you may have to sleep
             * a bit longer here.
             */
            Thread.sleep(250);
            
            // verify #of tasks running.
            assertEquals("#ntasks",ntasks,jobState.futures.size());

            /* 
             * verify the submit futures are all [null] vs an exception.
             */
            
            int n = 0;
            
            Iterator<Future<Object>> itr = submitFutures.iterator();
            
            while(itr.hasNext()) {
                
                Future<Object> submitFuture = itr.next();
                
                try {

                    assertNull("submitFuture",submitFuture.get());
                    
                } catch(ExecutionException ex) {
             
                    fail("Submit failed for task: "+ex);
                    
                }
                
                n++;
                
            }
            
            assertEquals(ntasks,n);
            
            
        }
        
        // cancel the job on the service.
        service.cancelJob(jobMetadata.getUUID());

        // verify #of tasks running.
        assertEquals("#tasks",0,jobState.futures.size());

        // verify that the outcomes are NOT available for a cancelled job.
        assertEquals("#outcomes",0,jobState.outcomes.size());
        
        taskService.shutdownNow();
        
    }

    /**
     * Verify that a job that does not receive a heartbeat will be cancelled.
     * 
     * @throws InterruptedException 
     */
    public void test_heartbeatMonitor() throws InterruptedException {

        Properties properties = new Properties();
        
        properties.setProperty(Options.HEARTBEAT_TIMEOUT,"100"); // 100ms

        properties.setProperty(Options.HEARTBEAT_PERIOD,"100"); // 100ms
        
        // use service that will cause each task to sleep endlessly.
        MyJobAndTaskService<MyJobMetadata, MyTask> service = new MySleepyJobAndTaskService<MyJobMetadata, MyTask>(
                properties );

        MyJobMetadata jobMetadata = new MyJobMetadata(UUID.randomUUID());

        // start the job on the service.
        service.startJob(jobMetadata);

        // verify that the job is running.
        assertNotNull(service.jobs.get(jobMetadata.getUUID()));
        
        // wait until the heartbeat monitor will have run at least once.
        Thread.sleep(200);
        
        // verify that the job is gone.
        assertNull(service.jobs.get(jobMetadata.getUUID()));
        
    }
    
    /**
     * Sets up a job and submits some tasks, waits for the tasks to complete,
     * and then drains the {@link Outcome}s.
     * 
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void test_run_and_drain_tasks() throws InterruptedException, IOException {
        
        // #of tasks to run.
        final int ntasks = 10;

        Properties properties = new Properties();
        
        // the thread pool is set to be much smaller than the #of tasks to be run.
        properties.setProperty(Options.THREAD_POOL_SIZE,"10");
        
        MyJobAndTaskService<MyJobMetadata, MyTask> service = new MyJobAndTaskService<MyJobMetadata, MyTask>(
                properties);

        MyJobMetadata jobMetadata = new MyJobMetadata(UUID.randomUUID());

        service.startJob(jobMetadata);
        
        for(int i=0; i<ntasks; i++) {
            
            service.submit(jobMetadata.getUUID(), new MyTask(), 0L);
            
        }
        
        // wait a bit so that the tasks can complete.
        Thread.sleep(250/*ms*/);
        
        Outcome[] outcomes = service.drain(jobMetadata.getUUID());
        
        assertNotNull(outcomes);
        
        // all tasks should complete (they are NOPs).
        assertEquals(ntasks,outcomes.length);
        
        for(int i=0; i<outcomes.length; i++) {
         
            System.err.println("outcomes["+i+"] : "+outcomes[i]);
            
        }
        
        JobState<MyJobMetadata> jobState = service.jobs.get(jobMetadata.getUUID());
        
        assertNotNull(jobState);
        
        assertEquals("#running",0,jobState.futures.size());

        assertEquals("#completed",0,jobState.outcomes.size());

        service.cancelJob(jobMetadata.getUUID());

    }

    /**
     * Test that a task with a non-zero timeout will be cancelled if that
     * timeout is exceeded.
     * 
     * @throws InterruptedException 
     */
    public void test_task_timeout() throws InterruptedException {

        Properties properties = new Properties();
        
        MyJobAndTaskService<MyJobMetadata, MyTask> service = new MySleepyJobAndTaskService<MyJobMetadata, MyTask>(
                properties );

        MyJobMetadata jobMetadata = new MyJobMetadata(UUID.randomUUID());

        service.startJob(jobMetadata);
        
        MyTask task = new MyTask();
        
        service.submit(jobMetadata.getUUID(), task, 20L/*ms*/);
        
        // wait longer than the task timeout.
        Thread.sleep(250/*ms*/);
        
        Outcome[] outcomes = service.drain(jobMetadata.getUUID());

        assertEquals("#outcomes",1,outcomes.length);
        
        Outcome outcome = outcomes[0];
        
        System.err.println("Outcome: "+outcome);
        
        assertEquals(task.getUUID(),outcome.getTask());
        
        assertEquals(Status.Cancelled,outcome.getStatus());
        
        service.cancelJob(jobMetadata.getUUID());

    }
    
    /**
     * Used to submit a single task to a {@link IJobAndTaskService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class SubmitRemoteTask<M extends IJobMetadata,T extends ITask> implements Callable<Object> {

        final IJobAndTaskService<M,T> service;
        final UUID job;
        final T task;
        
        public SubmitRemoteTask(IJobAndTaskService<M, T> service, UUID job, T task) {

            if (service == null)
                throw new IllegalArgumentException();

            if (job == null)
                throw new IllegalArgumentException();

            if (task == null)
                throw new IllegalArgumentException();

            this.service = service;

            this.job = job;
            
            this.task = task;
            
        }
        
        /**
         * The {@link IJobAndTaskService#submit(UUID, ITask)} method is
         * asynchronous so all this does is submit the method and quit.
         * 
         * @return <code>null</code>
         * 
         * @exception Exception
         *                If there is a problem when submitting the task.
         *                Exceptions thrown during the execution of the task are
         *                returned asynchronously.
         * 
         * @see IJobAndTaskService#drain(UUID)
         */
        public Object call() throws Exception {

            service.submit(job, task, 0L);
            
            return null;
            
        }
        
    }
    
    /**
     * Timer task runs as a daemon and sends heartbeats for a given job.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <M>
     * @param <T>
     */
    public static class HeartbeatTask<M extends IJobMetadata, T extends ITask> extends TimerTask {
        
        public static final transient Logger log = Logger
                .getLogger(HeartbeatTask.class);

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

            // run the timer as a daemon.
            Thread.currentThread().setDaemon(true);
            
            try {
                
                service.heartbeat(job);
                
            } catch(IOException ex) {
        
                log.warn("Could not send heartbeat: "+ex, ex);
                
            }
            
        }
        
    }
    
    /**
     * Test helper class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MyJobMetadata implements IJobMetadata {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        public final UUID job;
        
        public MyJobMetadata(UUID job) {
            
            this.job = job;
            
        }
        
        public UUID getUUID() {
            
            return job;
            
        }
        
    }
    
    /**
     * Test helper class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MyTask implements ITask {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        public final UUID uuid = UUID.randomUUID();
        
        public UUID getUUID() {

            return uuid;
            
        }
        
    }
    
    /**
     * Test helper class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class MyJobAndTaskService<M extends IJobMetadata,T extends ITask> extends AbstractJobAndTaskService<M,T> {

        private final UUID serviceUUID = UUID.randomUUID();
        
        /**
         * @param properties
         */
        protected MyJobAndTaskService(Properties properties) {
            super(properties);
        }

        public UUID getServiceUUID() throws IOException {
            return serviceUUID;
        }

        public IBigdataClient getBigdataClient() {

            return bigdataClient;
            
        }

        protected com.bigdata.service.mapred.AbstractJobAndTaskService.AbstractTaskWorker<M, T> newTaskWorker(
                com.bigdata.service.mapred.JobState<M> jobState,
                T task) {

            return new MyTaskWorker(jobState, task);
            
        }

        /**
         * Test helper.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         * @param <M>
         * @param <T>
         */
        class MyTaskWorker
            extends com.bigdata.service.mapred.AbstractJobAndTaskService.AbstractTaskWorker<M,T> {

            /**
             * @param jobState
             * @param task
             */
            protected MyTaskWorker(JobState<M> jobState, T task) {

                super(jobState, task);
                
            }

            protected void run() throws Exception {

                log.info("Running task: job="+jobState.getUUID()+", task="+task.getUUID());
                
            }
            
        }
        
    }

    /**
     * Test helper class - each task executed will wait, causing none of the tasks to progress.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class MySleepyJobAndTaskService<M extends IJobMetadata,T extends ITask> extends MyJobAndTaskService<M,T> {

        /**
         * @param properties
         */
        protected MySleepyJobAndTaskService(Properties properties) {
            super(properties);
        }

        protected com.bigdata.service.mapred.AbstractJobAndTaskService.AbstractTaskWorker<M, T> newTaskWorker(
                com.bigdata.service.mapred.JobState<M> jobState,
                T task) {

            return new MySleepyTaskWorker(jobState, task);
            
        }

        /**
         * Test helper.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         * @param <M>
         * @param <T>
         */
        class MySleepyTaskWorker
            extends com.bigdata.service.mapred.AbstractJobAndTaskService.AbstractTaskWorker<M,T> {

            /**
             * @param jobState
             * @param task
             */
            protected MySleepyTaskWorker(JobState<M> jobState, T task) {

                super(jobState, task);
                
            }

            protected void run() throws Exception {

                // wait until we are interrupted.
                try {

                    log.info("Task running - will wait.");
                    
                    wait();

                    fail("Not expecting to reach this line");
                    
                } catch(InterruptedException ex) {
                    
                    fail("Not expecting to reach this line: "+ex);
                    
                } catch(IllegalMonitorStateException t) {
                
                    /*
                     * Note: This is the exception that actually gets thrown
                     * when the Callable is cancelled.
                     */
                    
                    log.info("Cancelled: "+t);
                    
                    throw t;
                    
                } catch(Throwable t) {
                    
                    fail("Not expecting to reach this line: "+t);
                    
                }
                
            }
            
        }

    }

}
