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
 * Created on Sep 24, 2007
 */

package com.bigdata.service.mapReduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import junit.framework.TestCase;

import com.bigdata.journal.BufferMode;
import com.bigdata.service.EmbeddedBigdataClient;
import com.bigdata.service.IBigdataClient;

/**
 * Test suite for {@link RemoteTaskRunner}.
 * 
 * @todo write lots of tests for this -- this is the heart of the distributed
 *       robustness for the map/reduce operations.
 *       <p>
 *       Some kinds of tests can only be performed when the services are created
 *       and destroyed during execution of the operation, thereby testing
 *       discovery and use of new services and removal of unavailable services.
 *       <p>
 *       There are also possible dependencies between map and reduce operations
 *       where a map service failure after the map operation has completed could
 *       require re-execution of the map tasks run on that service - this case
 *       arises when the map services write their reduce partitions outputs onto
 *       local files which are later read by the reduce services.
 * 
 * @todo test correct rejection (no services, all dead, bad parameters, etc).
 * 
 * @todo test with services that are really remote (requires identification of
 *       the federation to those services, perhaps as part of the job metadata).
 *       Also look into the downloadable code issue here.
 * 
 * @todo test execution metadata that can be reported by the task runner (#of
 *       tasks submitted, start time, end time, #of tasks completed {success,
 *       error, cancel}. Since we might retry (or multiply execute) some tasks,
 *       these counters need to be defined in terms of either the final state of
 *       the tasks (which has more obvious meaning) or simply in terms of the
 *       simple sums of the outcomes (in which case some tasks will be double
 *       counted).
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRemoteTaskRunner extends TestCase {

    /**
     * 
     */
    public TestRemoteTaskRunner() {
        super();
    }

    /**
     * @param arg0
     */
    public TestRemoteTaskRunner(String arg0) {
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
    
    /**
     * Simple test creates a {@link RemoteTaskRunner}, submits a NOP task, and
     * awaits termination.
     */
    public void test_nop_run1() throws InterruptedException {
   
        Properties properties = new Properties();
        
        IJobAndTaskService<MyJobMetadata, MyTask>[] services = new IJobAndTaskService[] {
            
            new MyJobAndTaskService<MyJobMetadata, MyTask>(properties)
            
        };
        
        MyJobMetadata jobMetadata = new MyJobMetadata(UUID.randomUUID());
    
        MyTask[] tasks = new MyTask[]{
                new MyTask()
        };
        
        int maxTasks = 0;
        
        int maxConcurrency = 1;

        int maxTaskRetries = 0;
        
        long taskTimeout = 0L;
        
        RemoteTaskRunner<MyJobMetadata, MyTask> taskRunner = new RemoteTaskRunner<MyJobMetadata, MyTask>(
                services, jobMetadata, Arrays.asList(tasks).iterator(),
                maxTasks, maxConcurrency, maxTaskRetries, taskTimeout);
        
        taskRunner.awaitTermination();
        
    }
    
    /**
     * Simple test creates a {@link RemoteTaskRunner}, submits a NOP task, and
     * awaits termination.
     */
    public void test_nop_run2() throws InterruptedException {
   
        Properties properties = new Properties();
        
        IJobAndTaskService<MyJobMetadata, MyTask>[] services = new IJobAndTaskService[] {
            
            new MyJobAndTaskService<MyJobMetadata, MyTask>(properties)
            
        };
        
        MyJobMetadata jobMetadata = new MyJobMetadata(UUID.randomUUID());
    
        /*
         * Setup tasks to be executed.
         */
        final int limit = 5;
        
        MyTask[] tasks = new MyTask[limit];

        for (int i = 0; i < tasks.length; i++) {

            tasks[i] = new MyTask(1000/*ms*/);
            
        }
        
        int maxTasks = 0;
        
        int maxConcurrency = 1;

        int maxTaskRetries = 0;
        
        long taskTimeout = 0L;
        
        RemoteTaskRunner<MyJobMetadata, MyTask> taskRunner = new RemoteTaskRunner<MyJobMetadata, MyTask>(
                services, jobMetadata, Arrays.asList(tasks).iterator(),
                maxTasks, maxConcurrency, maxTaskRetries, taskTimeout);
        
        taskRunner.awaitTermination(1000/*ms*/);
        
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
    static class MyTask implements ITask, Runnable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        public final UUID uuid = UUID.randomUUID();
        
        private final long sleepMillis;
        
        public MyTask() {
            this(0L);
        }
        
        public MyTask(long sleepMillis) {
            this.sleepMillis = sleepMillis;
        }
        
        public UUID getUUID() {

            return uuid;
            
        }

        public void run() {

            if(sleepMillis!=0L) {
                
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                
            }
            
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

        com.bigdata.service.mapReduce.AbstractJobAndTaskService.AbstractTaskWorker<M, T> newTaskWorker(
                com.bigdata.service.mapReduce.JobState<M> jobState,
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
            extends com.bigdata.service.mapReduce.AbstractJobAndTaskService.AbstractTaskWorker<M,T> {

            /**
             * @param jobState
             * @param task
             */
            protected MyTaskWorker(JobState<M> jobState, T task) {

                super(jobState, task);
                
            }

            protected void run() throws Exception {

                log.info("Running task: job="+jobState.uuid+", task="+task.getUUID());
                
                if(task instanceof Runnable) {

                    ((Runnable)task).run();
                    
                }
                
            }
            
        }
        
    }

}
