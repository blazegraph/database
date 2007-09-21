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
 * Created on Mar 17, 2007
 */

package com.bigdata.service.mapReduce;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ForceEnum;
import com.bigdata.service.BigdataFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedBigdataClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.EmbeddedBigdataFederation.Options;
import com.bigdata.service.mapReduce.MapService.EmbeddedMapService;
import com.bigdata.service.mapReduce.ReduceService.EmbeddedReduceService;


/**
 * The master for running parallel map/reduce jobs distributed across a cluster.
 * <p>
 * Map/reduce is a functional programming style in which a program is broken
 * down into a <i>map</i> and a <i>reduce</i> operation. Those operations are
 * trivially parallelized and distributed across one or more worker tasks on
 * available hosts. In general, there are M map tasks and N reduce tasks for
 * each map/reduce job. The map operation, the intermediate store, and the
 * reduce tasks are distinct services. In particular, this means that the job
 * state resides with the master while the intermediate state for the reduce
 * tasks is decoupled from the reduce logic.
 * <p>
 * Logically, each map operation processes a key-value pair, writing a set of
 * intermediate key-value pairs as its output. In practice, the inputs are often
 * drawn from files in a (networked) file system but other possible sources
 * include chaining map/reduce jobs or reading from an index. It is important
 * that the keys output by the map operation capture all necessary "uniqueness"
 * distinctions. For example, the key is often formed from an "application key"
 * followed the map task identifier, and finally the map task local tuple
 * counter. The map operation outputs are buffered and automatically partitioned
 * into N temporary stores (one per reduce task) using a hash function of the
 * key module the #of reduce tasks.
 * <p>
 * Each reduce task reads the intermediate store having data for the
 * intermediate key-value partition assigned to that reduce task. The keys in
 * those partitions are essentially random since they are assigned to partitions
 * by the (user-defined) hash function. However, the intermediate store
 * guarentees that the key-value pairs will be fully ordered. The reduce task is
 * then run on the total order, writing its outputs onto a local output file or
 * loading the data into scale-out indices.
 * <p>
 * The map/reduce master selects map and reduce services on which to run a
 * map/reduce job and is responsible for re-assigning tasks that fail or that
 * take are taking too long. The master attempts to use reduce services that are
 * "close" to the intermediate data (in terms of the network topology) but not
 * necessarily on the same host - this decouples the reduce operation from the
 * input state and lets us failover to another reduce service as necessary.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Use some map/reduce jobs to drive performance testing of group commit
 *       and concurrent writes on distinct indices on the same data service.
 * 
 * @todo Do a Map/Reduce service. Map an operation over a set of files,
 *       distributing the results across a cluster using a hash function. Reduce
 *       distributes an aggregation operation over the cluster, first sorting
 *       the data and then applying some operation to that sorted data.
 *       <p>
 *       A good example of a map/reduce job would be bulk loading a large Lehigh
 *       Benchmark data set. The map job would parse the files that were local
 *       on each host, distributing triples across the cluster using a hash
 *       function. The reduce job would sort the triplets arriving at each host
 *       and then bulk load those triplets into the indices.
 *       <p>
 *       Another example would be to bulk index terms in documents, etc.
 * 
 * FIXME Explore the parameter space (m,n,# map services, # reduce services,
 * buffer mode, flush on commit, overflow, group commit, job size, # of
 * machines). Look at job completion rate, scaling, bytes read/written per
 * second, etc.
 * 
 * FIXME Explore design alternatives for reduce state and behavior.
 * 
 * A) Reduce client uses local temporary stores. These are created when a
 * map/reduce job starts and released when it ends.
 * 
 * B) Reduce client uses {@link DataService}s as the store files. A set of
 * {@link DataService}s would either be dedicated to map/reduce tasks or
 * multiplexed with other operations.
 * 
 * The advantages of (B) are (1) reuse of failover for data services; and (2)
 * reduce behavior is separated from reduce state.
 * 
 * The advantage of (A) is that it will be faster since there is no network IO
 * for the reduce task (there is always going to be network IO for the map
 * task). It will also simplify management of temporary state.
 * 
 * The disadvantage (A) is that a single host failure will cause the entire
 * map/reduce job to need re-execution. This is a fairly substantial penalty
 * (and it is true anyway unless you are using data service failover).
 * 
 * For both approaches, it may be significantly cleaner to refactor the client
 * into a map client and a reduce client. The master would then recruit a set of
 * M map clients and N reduce clients for a job. Internally, each of these would
 * have a thread pool that they use to multiplex their tasks.
 * 
 * @todo reduce tasks may begin running as soon as intermediate output files
 *       become available; the input to each reduce task is M index segments
 *       (one per map task); the data in those segments are already in sorted
 *       order, but they need to be placed into a total sorted order before
 *       running the reduce task. Given the tools on hand, the easiest way to
 *       achieve a total order over the reduce task inputs is to build a
 *       partitioned index. Since each reduce task input is already in sorted
 *       order, we can build the total sorted order by reading from the M input
 *       segments in parallel (an M-way merge). Since M can be quite high and
 *       the keys are randomly distributed across the input by the user-defined
 *       hash function, this merge operation will need to scale up to a large
 *       fan-in (100,000+).
 * 
 * @todo don't bother adding a map/reduce job schedule yet - first get the basic
 *       thing working well. we can worry about scheduling jobs (and master
 *       failover) once we are running interesting jobs. Until then, focus on a
 *       master that provides a robust mechanism for running complex jobs to
 *       completion in the face of individual failures. We can store the
 *       evolving job state in memory and use replication to secondary masters
 *       for failover (it can also be logged). We only need to persist the
 *       evolving job state if we want the job to be restart-safe across total
 *       cluster failure or without secondary masters. We do not even need to
 *       make the master a "service" until we add secondary master - it can just
 *       run as a command line utility using {@link #main(String[])}.
 * 
 * @todo the reduce output is written to an arbitrary sink. common uses are
 *       probably a NOP (the N merged indices are the outputs) a 2nd stage
 *       reduce (producing a total merge of the intermediate values) or various
 *       kinds of local flat files.
 * 
 * @todo measure the scaling factor as we add hardware for a fixed problem.
 * 
 * @todo the master needs to keep track of the state each worker task on each
 *       host.
 * 
 * @todo the master needs a queue for input jobs and should expose an HTML
 *       monitor for job status, as well as email notification of results.
 * 
 * @todo support "debugging" using a version that executes tasks for a single
 *       partition of subset of the data.
 */
public class MapReduceMaster {

    public static final transient Logger log = Logger
            .getLogger(MapReduceMaster.class);

    /**
     * @todo define lookup of the bigdata instance against which the named
     *       indices will be resolved.
     * 
     * @param properties
     */
    public MapReduceMaster(Properties properties) {

    }

    /**
     * A map reduce job.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class MapReduceJob implements IMapReduceJob {

        // the UUID of this job.
        private final UUID uuid;

        // #of map tasks to run.
        private final int m;

        // #of reduce tasks to run.
        private final int n;

        // object responsible for enumerating the inputs to the map task.

        private final IMapSource mapSource;

        private final Class<? extends IMapTask> mapTaskClass;

        private final Constructor<? extends IMapTask> mapTaskCtor;
        
        private final Class<? extends IReduceTask> reduceTaskClass;

        private final Constructor<? extends IReduceTask> reduceTaskCtor;
        
        private final IHashFunction hashFunction;

        public MapReduceJob(int m, int n,
                IMapSource mapSource,
                Class<? extends IMapTask> mapTaskClass,
                Class<? extends IReduceTask> reduceTaskClass,
                IHashFunction hashFunction
                ) {

            if (m <= 0)
                throw new IllegalArgumentException();

            if (n <= 0)
                throw new IllegalArgumentException();

            if (mapSource == null)
                throw new IllegalArgumentException();

            if (mapTaskClass == null)
                throw new IllegalArgumentException();

            if (reduceTaskClass == null)
                throw new IllegalArgumentException();

            if (hashFunction == null)
                throw new IllegalArgumentException();

            // assign a job UUID.
            this.uuid = UUID.randomUUID();

            this.m = m;

            this.n = n;

            this.mapSource = mapSource;

            this.mapTaskClass = mapTaskClass;

            try {
                this.mapTaskCtor = mapTaskClass.getConstructor(new Class[] {
                        UUID.class, Integer.class, IHashFunction.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } 
            
            this.reduceTaskClass = reduceTaskClass;

            try {
                this.reduceTaskCtor = reduceTaskClass
                        .getConstructor(new Class[] { UUID.class, UUID.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } 

            this.hashFunction = hashFunction;

        }

        public UUID getUUID() {

            return uuid;

        }

        public int getMapTaskCount() {

            return m;

        }

        public int getReduceTaskCount() {

            return n;

        }

        public IMapSource getMapSource() {

            return mapSource;

        }

        public IHashFunction getHashFunction() {

            return hashFunction;

        }

        public IMapTask getMapTask() {
            
            return getMapTask( UUID.randomUUID() );
           
        }
        
        public IMapTask getMapTask(UUID task) {

            try {

                return mapTaskCtor.newInstance(new Object[] {
                        task,
                        Integer.valueOf(getReduceTaskCount()),
                        getHashFunction() }
                );
                
            } catch (InstantiationException e) {
                
                throw new RuntimeException(e);
                
            } catch (IllegalAccessException e) {
                
                throw new RuntimeException(e);
                
            } catch (IllegalArgumentException e) {

                throw new RuntimeException(e);
                
            } catch (InvocationTargetException e) {
                
                throw new RuntimeException(e);

            }

        }

        public IReduceTask getReduceTask(UUID task, UUID dataService) {

            try {

                return reduceTaskCtor.newInstance(new Object[] {
                        task,
                        dataService}
                );
                
            } catch (InstantiationException e) {
                
                throw new RuntimeException(e);
                
            } catch (IllegalAccessException e) {
                
                throw new RuntimeException(e);
                
            } catch (IllegalArgumentException e) {

                throw new RuntimeException(e);
                
            } catch (InvocationTargetException e) {
                
                throw new RuntimeException(e);

            }

        }

    }

    /**
     * The state of a map/reduce job.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class JobState {
        
        /**
         * The job to be run.
         */
        final MapReduceJob job;
        /**
         * The client for the {@link BigdataFederation} used to store the
         * intermediate state of the map/reduce operation.
         */
        final IBigdataClient client;
        /**
         * The map services.
         */
        IMapService[] mapServices;
        /**
         * The reduce services.
         */
        IReduceService[] reduceServices;
        /**
         * The UUID assigned to each reduce task.
         */
        UUID[] reduceTasks;
        /**
         * The data services for each reduce partition. The index on the data
         * service is named by the UUID of the corresponding reduce task.
         */
        UUID[] dataServices;
        
        public JobState(MapReduceJob job, IBigdataClient client) {

            if (job == null)
                throw new IllegalArgumentException();

            if (client == null)
                throw new IllegalArgumentException();

            this.job = job;
            
            this.client = client;
        
            setUp();
            
        }

        /**
         * Select the map, reduce, and data services to be used by the job.
         * 
         * @todo When we convert to distributed map/reduce, we will need to
         *       perform service discovery for the map/reduce clients and use no
         *       more than are required by the job. If there are fewer available
         *       than the input parameters for the job then we will submit more
         *       than one task to each service. In general, a map service can
         *       run 100s of tasks in parallel while a reduce service can run
         *       10s of tasks in parallel.
         * 
         * @todo cleanly separate the notion of M and N (#of map tasks and #of
         *       reduce tasks) from the notion of the #of map services and the
         *       #of reduce services and from the notion of the number of map
         *       inputs (fan-in to the map operations). N is the fan-in to the
         *       reduce operations.
         */
        protected void setUp() {

            /**
             * Since we are running everything in process, this is the #of map
             * services to be start. The actual #of map tasks to run is based on
             * the #of input files and is discovered dynamically. The #of map
             * services to be started is based on an expectation that we will
             * distribute 100 concurrent map tasks to each map service. Since we
             * do not know the fan in (the #of input files) we are not able to
             * tell how many sets of map tasks will be run through the map
             * services before the input has been consumed.
             */
            final int numMapServicesToStart = 1; //Math.max(1, job.m / 100);

            /**
             * Since we are running everything in process, this is the #of
             * reduce services to start. The #of reduce tasks to be run is given
             * by job.n - this is a fixed input parameter from the user.
             */
            final int numReduceServicestoStart = 1;// Math.max(1, job.n/10);

            /**
             * The map services. Each services is capable of running a large #of
             * map tasks in parallel.
             */
            mapServices = new IMapService[numMapServicesToStart];
            {

                // client properties.
                Properties properties = new Properties();

                for (int i = 0; i < numMapServicesToStart; i++) {

                    mapServices[i] = new EmbeddedMapService(UUID.randomUUID(),
                            properties, client);

                }

            }

            /**
             * The reduce services. Each service is capable of running a few
             * reduce tasks in parallel.
             */
            reduceServices = new IReduceService[numReduceServicestoStart];
            {

                // client properties.
                Properties properties = new Properties();

                for (int i = 0; i < numReduceServicestoStart; i++) {

                    reduceServices[i] = new EmbeddedReduceService(UUID
                            .randomUUID(), properties, client);
                    
                }

            }

            /**
             * The reduce task identifiers.
             * 
             * Note: The #of reduce tasks is determined by the caller.
             * 
             * Note: We pre-generate the UUIDs for these tasks since they are
             * also used to name the intermediate stores.
             */
            reduceTasks = new UUID[job.n];
            {

                for(int i=0; i<reduceTasks.length; i++) {

                    reduceTasks[i] = UUID.randomUUID();
                    
                }
                
            }
            
        }
        
        /**
         * Notify everyone that the job is starting.
         * 
         * @todo parallelize notice so that we can start/end jobs faster.
         * 
         * @todo handle map service and reduce service failover (take the
         *       service off line and perhaps find a new one to use).
         * 
         * @todo handle data service failover
         */
        public void start() {
            
            /*
             * Setup intermediate indices on the data services (one per reduce
             * task). The same data service MAY be used for more than one reduce
             * task.
             * 
             * @todo in order for the map and reduce tasks to have automatic
             * failover they need to query the metadata service if the given
             * data service fails and find the replacement data service (this
             * presumes that a media redundency chain is in effect).
             */
            dataServices = new UUID[reduceTasks.length];
            {
                IMetadataService metadataService = client.getMetadataService();
                for (int i = 0; i < reduceTasks.length; i++) {
                    try {
                        dataServices[i] = metadataService.getUnderUtilizedDataService();
                    } catch (IOException e) {
                        log.warn("Metadata service not available: " + e);
                    }
                    try {
                        // the index name (local on the data service).
                        String name = reduceTasks[i].toString();
                        String className = UnisolatedBTree.class.getName();
                        Object config = null;
                        client.getDataService(dataServices[i]).registerIndex(
                                name, reduceTasks[i], className, config);
                    } catch (Exception e) {
                        log.warn("Could not create intermediate store: " + e);
                    }
                }
            }
            
            // notify map services of new job.
            {
                final int nmapServices = mapServices.length;
                for (int i = 0; i < nmapServices; i++) {
                    try {
                        mapServices[i].startJob(job.uuid, reduceTasks,
                                dataServices);
                    } catch (IOException e) {
                        log.warn("Map service not available: " + e);
                    }
                }
            }
            
            // notify reduce services of new job.
            {
                final int nreduceServices = reduceServices.length;
                for (int i = 0; i < nreduceServices; i++) {
                    try {
                        reduceServices[i].startJob(job.uuid);
//                        , reduceTasks[i], dataServices[i]);
                    } catch (IOException e) {
                        log.warn("Reduce service not available: " + e);
                    }
                }
            }
            
        }

        /**
         * Notify everyone that the job is done.
         * 
         * @todo the map services should use this notice to forceably terminate
         *       any remaining map tasks for the job.
         * 
         * @todo the reduce services should use this notice to delete the reduce
         *       store for the job.
         */
        public void end() {
            
            for(int i=0; i<mapServices.length; i++) {
                try {
                    mapServices[i].endJob(job.uuid);
                } catch(IOException e) {
                    log.warn("Map service: "+e);
                }
            }
            
            for(int i=0; i<reduceServices.length; i++) {
                try {
                    reduceServices[i].endJob(job.uuid);
                } catch(IOException e) {
                    log.warn("Reduce service: "+e);
                }
            }
            
            for(int i=0; i<dataServices.length; i++) {
                try {
                    IDataService ds = client.getDataService(dataServices[i]);
                    if(ds==null) {
                        log.warn("Could not locate data service: "+dataServices[i]);
                    }
                    // the index name (local on the data service).
                    String name = reduceTasks[i].toString();
                    ds.dropIndex(name);
                } catch(Exception e) {
                    log.warn("Data service: "+e);
                }
            }
            
        }

        /**
         * Distribute the map tasks and wait for them to complete.
         * <p>
         * Note: This presumes that we are using network storage for the input
         * sources. We simply assign each file to one map task.
         * 
         * @param delay
         *            The maximum time (in milliseconds) that we will wait for a
         *            task to complete.
         * 
         * @param maxTasks
         *            When non-zero, this is the maximum #of input files that
         *            will be mapped.
         * 
         * @param maxRetry
         *            The maximum #of times that we will retry a task that fails
         *            to complete successfully within the <i>delay</i>. A value
         *            of ZERO (0) disables retry.
         * 
         * @todo The output from map tasks that do not complete within this many
         *       retries will not participate in the reduce task.
         *       <p>
         *       Note: the promise that input files that could not be
         *       successfully processed requires that we completely buffer the
         *       tuples from the map task until it has successfully complete and
         *       then do an atomic "append" of those tuples to the reduce input
         *       store.
         * 
         * @param clients
         *            The clients on which the map tasks will be run.
         * 
         * @todo run the map task on the client that is 'closest' to the data
         *       that it will read while also making sure that we distribute the
         *       map tasks among the available clients.
         * 
         * FIXME [m] should limit the #of active map tasks for this job. This
         * helps to give the user control over the parallelism and also makes
         * the "delay" interpretable in terms of the time since the job was
         * submitted.
         * <p>
         * This requires that we monitor the state of each map task that has
         * been started so that we know when to start a new map task. For each
         * task started for a given job we need to know: input file, Task UUID,
         * client UUID, start time, #of tries. The client should give us a
         * heartbeat (when realized as a service). Until then, the client will
         * either report success or failure.
         * 
         * @todo Long running tasks should be replicated once we are "mostly"
         *       done.
         * 
         * @return The percentage tasks that completed successfully.
         * 
         * @todo Can the return value mislead the caller? E.g., is it possible
         *       for a task to succeed twice, resulting in double counting such
         *       that we can not rely on the return to indicate whether all map
         *       tasks completed successfully?
         */
        public double map(long delay, long maxTasks, int maxRetry) {

            final int nservices = mapServices.length;

            // #of input sources assigned to map tasks.
            int nstarted = 0;
            
            int nretries = 0;

            // the files to be mapped over the clients.
            Iterator<File> itr = job.getMapSource().getSources();

            /*
             * A queue of map tasks that have been submitted and on which we are
             * waiting for completion.
             */
            DelayQueue<MapTaskState> waiting = new DelayQueue<MapTaskState>();

            while (itr.hasNext()) {

                // the input source.
                File file = itr.next();
                
                // do not map more than this many input files.
                if(maxTasks!=0L && nstarted>=maxTasks) break;

                // choose a client to run this input source.
                final int index = (int) (nstarted % nservices);

                log
                        .info("Mapping file " + file.toString() + " on client #"
                                + index);

                // the client to which this input source will be assigned.
                IMapService client = mapServices[index];

                // run the map task.
                Future future;
                final IMapTask mapTask = job.getMapTask();
                try {
                    // @todo the Future will go away with a remote service refactor.
                    future = client.submit(job.uuid, file, mapTask);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                // add to the queue of tasks on which we will wait.
                waiting.add(new MapTaskState(delay, mapTask.getUUID(), 0,
                        future, file));

                nstarted++;

            }

            /*
             * Wait for the queue to drain.
             */
            int nended = 0;
            int nfailed = 0; // #of tasks that do not succeed (retries also fail).
            
            while (nended < nstarted) {

                MapTaskState taskState;
                
                try {
                    
                    taskState = waiting.poll(1, TimeUnit.SECONDS);

                    if (taskState == null) {

                        // still waiting on a task to complete.
                        continue;
                        
                    }
                    
                } catch (InterruptedException ex) {

                    log.warn("Interrupted: "+ex,ex);

                    // @todo ignore or abort processing?
                    continue;
                    
                }

                nended++;
                
                taskState.cancelIfStillRunning();

                if (taskState.error() == null) {
                    log.info("Done: "
                            + taskState.uuid
                            + " "
                                + (taskState.success() ? "success"
                                        : (taskState.future.isCancelled() ? "cancelled"
                                                : taskState.error())) + "("
                                                + nended + " of " + nstarted + ")");
                } else {
                    log.warn("Error: " + taskState.uuid + " " + "(" + nended
                            + " of " + nstarted + ")", taskState.error());

                }

                /*
                 * If the task was not successful, then re-try it up to N times.
                 */
                if (!taskState.success()) {

                    if (taskState.ntries < maxRetry) {

                        final int ntries2 = taskState.ntries + 1;

                        // choose a client to run this input source.
                        final int index = (int) (nstarted % nservices);

                        log.info("Mapping file " + taskState.file.toString()
                                + " on client #" + index + "(ntries=" + ntries2
                                + ")");

                        // the client to which this input source will be
                        // assigned.
                        IMapService client = mapServices[index];

                        // run the map task.
                        Future future;
                        final IMapTask mapTask = job.getMapTask(taskState.uuid);
                        try {
                            // @todo the Future will go away with a remote
                            // service refactor.
                            future = client.submit(job.uuid, taskState.file,
                                    mapTask);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }

                        // add to the queue of tasks on which we will wait.
                        waiting
                                .add(new MapTaskState(taskState.delay,
                                        taskState.uuid, ntries2, future,
                                        taskState.file));

                        nstarted++;

                        nretries++;

                    } else {

                        /*
                         * A task has failed more times than we are willing to
                         * retry so mark this as a permanent failure.
                         */
                        
                        log.warn("Could not map: " + taskState.file);

                        nfailed++;

                    }

                }

            }

            System.err.println("Ran " + nstarted + " map tasks ("+nretries+" retries, "+nfailed+" failed).");

            return (double)(nstarted-nfailed)/nstarted;
            
        }

        /**
         * Distribute the reduce tasks and wait for them to complete.
         * 
         * @return The percentage tasks that completed successfully.
         * 
         * @todo Can the return value mislead the caller? E.g., is it possible
         *       for a task to succeed twice, resulting in double counting such
         *       that we can not rely on the return to indicate whether all
         *       tasks completed successfully?
         */
        public double reduce(long delay, int maxRetry) {

            final int nservices = reduceServices.length;

            // #of reduce tasks run.
            int nstarted = 0;

            // #of reduce tasks that get retried.
            int nretries = 0;
            
            /*
             * A queue of map tasks that have been submitted and on which we are
             * waiting for completion.
             */
            DelayQueue<ReduceTaskState> waiting = new DelayQueue<ReduceTaskState>();

            final int n = job.getReduceTaskCount();

            for (int i = 0; i < n; i++) {

                // the service on which this reduce task will be executed.
                IReduceService reduceService = reduceServices[i % reduceServices.length];

                // the service from which it will read its data.
                UUID dataService = dataServices[i % reduceTasks.length];

                // run the task.
                // @todo the Future will go away with the remove service refactor.
                Future<Object> future;
                IReduceTask reduceTask = job.getReduceTask(reduceTasks[i],dataService);
                try {
                    future = reduceService.submit(job.getUUID(), reduceTask);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // add to the queue of tasks on which we will wait.
                waiting.add(new ReduceTaskState(delay, reduceTask.getUUID(), 0, future, dataService));

                nstarted++;

            }

            /*
             * Wait for the queue to drain.
             */
            int nended = 0;
            int nfailed = 0;
            
            while (nended < nstarted) {

                ReduceTaskState taskState;
                
                try {
                    
                    taskState = waiting.poll(1, TimeUnit.SECONDS);

                    if (taskState == null) {

                        // still waiting on a task to complete.
                        continue;
                        
                    }
                    
                } catch (InterruptedException ex) {
                    
                    log.warn("Interrupted: "+ex,ex);
                    
                    // @todo ignore or abort processing?
                    continue;
                    
                }

                nended++;
                
                taskState.cancelIfStillRunning();

                if (taskState.error() == null) {
                    log.info("Done: "
                            + taskState.uuid
                            + " "
                                + (taskState.success() ? "success"
                                        : (taskState.future.isCancelled() ? "cancelled"
                                                : taskState.error())) + "("
                                                + nended + " of " + nstarted + ")");
                } else {
                    log.warn("Error: " + taskState.uuid + " " + "(" + nended
                            + " of " + nstarted + ")", taskState.error());

                }

                /*
                 * If the task was not successful, then re-try it up to N times.
                 */
                if (!taskState.success()) {

                    if (taskState.ntries < maxRetry) {

                        final int ntries2 = taskState.ntries + 1;

                        // choose a client to run this input source.
                        final int index = (int) (nstarted % nservices);

                        log.info("Reducing " + taskState.dataService
                                + " on client #" + index + "(ntries=" + ntries2
                                + ")");

                        /*
                         * The service on which this reduce task will be
                         * executed.
                         */
                        IReduceService client = reduceServices[index];

                        // run the map task.
                        Future future;
                        IReduceTask reduceTask = job.getReduceTask(
                                taskState.uuid, taskState.dataService);
                        try {
                            // @todo the Future will go away with a remote
                            // service refactor.
                            future = client.submit(job.uuid, reduceTask);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }

                        // add to the queue of tasks on which we will wait.
                        waiting.add(new ReduceTaskState(taskState.delay,
                                taskState.uuid, ntries2, future,
                                taskState.dataService));

                        nstarted++;

                        nretries++;

                    } else {

                        /*
                         * A task has failed more times than we are willing to
                         * retry so mark this as a permanent failure.
                         */

                        log.warn("Could not map: " + taskState.uuid);

                        nfailed++;

                    }

                }

            }

            System.err.println("Ran " + nstarted + " reduce tasks (" + nretries
                    + " retries, " + nfailed + " failed).");

            return (double)(nstarted-nfailed)/nstarted;

        }

    }
    
    /**
     * Run a map/reduce task.
     * 
     * @param args
     * 
     * FIXME describe the job using an XML file and refactor
     * {@link ExtractKeywords}, {@link CountKeywords}, and
     * {@link TestCountKeywordJob} into some test suites for this class.
     * 
     * @todo Normally the #of map clients will exceed the actual number of
     *       available {@link MapService}s. In this case, each client will be
     *       expected to execute multiple map task and those tasks will be
     *       parallelized both across the clients and within each clients thread
     *       pool. Likewise, there can be more reduce tasks than
     *       {@link MapService}s. However, reduce tasks tend to be heavier so
     *       the proportion of reduce tasks to clients might be on the order of
     *       2:1 while the proportion of map tasks to clients might be on the
     *       order of 100:1. Does this suggest that we should have separate map
     *       and reduce thread pools in the clients?
     *       <p>
     *       The #of reduce tasks MUST be choosen such that the intermediate
     *       file may be handled on a single machine. (There must be enough free
     *       disk to store the file and enough resources to sort the file,
     *       producing a sorted version. So you need 2X disk plus lots of RAM.
     *       The sort should really be in C so that we can leverage more than 2G
     *       of RAM.)
     * 
     * FIXME There is confusion in reporting M vs the #of map operations
     * actually run vs the #of map services available vs the #of map services
     * used. (Likewise for reduce).
     */
    public static void main(String[] args) {

        MapReduceJob job = new TestCountKeywordJob(100/* m */, 4/* n */);
//        MapReduceJob job = new TestCountKeywordJob(1/* m */, 2/* n */);

        /**
         * Setup the client that will connect to federation on which the
         * intermediate results will be written.
         * 
         * @todo this is using embedded federation - use a distributed one but
         *       keep an option for testing that uses an embedded federation.
         *       For this we will need to specify the federation identifier so
         *       that the client can connect.
         * 
         * @todo when used for map/reduce a larger disk-only journal and no
         *       overflow would improve performance since we expect to only read
         *       back the data once (in a given reduce task) and the bulk index
         *       build is at least that expensive. There really may be a role
         *       for "temporary" data services -- data services optimized for
         *       short-term but robust data storage -- for both map/reduce and
         *       for intermediate results in data processing such as closure of
         *       an RDFS store or join results.
         */
        final IBigdataClient client;
        {
            
            Properties properties = new Properties();

            /*
             * @todo use the disk-only mode since we expect large jobs. Right
             * now this is transient since there is a problem with restart for
             * the embedded federation.
             */
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                    .toString());

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                    .toString());
          
            // #of data services to run in the federation @todo use more than one.
            properties.setProperty(Options.NDATA_SERVICES, "2");

            properties.setProperty(Options.FORCE_ON_COMMIT, ForceEnum.No.toString());

            client = new EmbeddedBigdataClient(properties);
            
            client.connect();
            
        }

        JobState jobState = new JobState(job,client);
        
        // The minimum success rate required for the map stage.
        final double minSuccessRate = .9d;
        
        try {

            /*
             * Notify everyone that this job is starting.
             */
            jobState.start();

            /*
             * Distribute the map tasks and wait for them to complete.
             */
            final long elapsedMap;
            {

                final long beginMap = System.currentTimeMillis();

                // the maximum time a map task may run.
                final long delay = 60 * 1000;

                // non-zero to run at most that many input files.
                final long maxTasks = 0L;
//                final long maxTasks = 1L; // @todo remove

                // the maximum #of times a map task will be retried (zero disables retry).
//                final int maxRetry = 3;
                final int maxRetry = 0; // @todo remove

                final double percentSuccess = jobState.map(delay, maxTasks, maxRetry);
                
                System.out.println("Map operation "+percentSuccess+" success.");
                
                if(percentSuccess<minSuccessRate) {
                    
                    log.warn("Success rate less than threshold - aborting");
                    
                    System.exit(1);
                    
                }

                elapsedMap = System.currentTimeMillis() - beginMap;
                
            }

            /*
             * Distribute the reduce tasks and wait for them to complete.
             */
            final long elapsedReduce;
            {
            
                final long beginReduce = System.currentTimeMillis();

                // the maximum time a reduce task may run.
                final long delay = 60 * 1000;

                // the maximum #of times a reduce task will be retried (zero disables retry).
//                final int maxRetry = 3;
                final int maxRetry = 0; // @todo remove

                final double percentSuccess = jobState.reduce(delay, maxRetry);
               
                System.out.println("Reduce operation "+percentSuccess+" success.");

                if(percentSuccess<minSuccessRate) {
                    
                    log.warn("Success rate less than threashold - aborting");
                    
                    System.exit(1);
                    
                }
                
                elapsedReduce = System.currentTimeMillis() - beginReduce;

            }

            System.out.println("Done: map( " + elapsedMap + " ms, " + job.m
                    + " tasks ), reduce( " + elapsedReduce + " ms" + ", "
                    + job.n + " tasks )");
            
        } catch (Throwable t) {
            
            log.error( "Problem running job: "+t, t );
            
        } finally {

            /*
             * Notify everyone that this job is over.
             */

            jobState.end();

            /*
             * Disconnect from the federation.
             */
            
            client.terminate();
            
        }

    }

    /**
     * A task that has been submitted and on which we are awaiting completion.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract static class TaskState implements Delayed {

        // maximum grace period (ms).
        final long delay;

        // task UUID.
        final UUID uuid;

        // #of times that this task has been tried.
        final int ntries;

        // start time.
        final long begin = System.currentTimeMillis();

        /*
         * @todo this needs to be handled differently for a remote service.
         */
        final Future<Object> future;

        /**
         * @param uuid
         *            The task UUID.
         * @param delay
         *            The length of time in milliseconds that we are willing
         *            to wait for this task to complete.
         * @param ntries
         *            The #of times that this task has already been
         *            attempted.
         * @param future
         */
        protected TaskState(long delay, UUID uuid, int ntries,
                Future<Object> future) {

            this.delay = delay;

            this.uuid = uuid;

            this.future = future;

            this.ntries = ntries;

        }

        /**
         * Returns the remaining delay associated with this object
         * 
         * @return 0L if the task is done (whether an error or success).
         *         Otherwise, the remaining time for this task to run.
         */
        public long getDelay(TimeUnit unit) {

            if (future.isDone()) {

                // The task is done (any kind of completion).
                return 0;

            }

            // the remaining delay (it is actually in milliseconds).
            long elapsed = System.currentTimeMillis() - begin;

            long remaining = delay - elapsed;

            return unit.convert(remaining, TimeUnit.MILLISECONDS);

        }

        public int compareTo(Delayed arg0) {

            if (this == arg0)
                return 0;

            return uuid.compareTo(((TaskState) arg0).uuid);

        }

        // /**
        // * Return true iff the task is still running at the time that this
        // * method was invoked.
        // */
        // public boolean running() {
        //                
        // return ! future.isDone();
        //                
        // }

        /**
         * Return true iff the task had completed successfully at the time
         * that this method was invoked
         */
        public boolean success() {

            if (future.isDone() && !future.isCancelled()) {

                try {

                    future.get();

                    return true;

                } catch (InterruptedException ex) {
                    // will not happen since the task is done.
                    throw new AssertionError();
                } catch (CancellationException ex) {
                    // will not happen since we checked this above.
                    throw new AssertionError();
                } catch (ExecutionException ex) {
                    return false;
                }

            }

            // not successful (might not be done yet).
            return false;

        }

        /**
         * Return the {@link Throwable} object take caused the task to fail
         * or <code>null</code> if the task has not completed or if the
         * task has completed by no error was generated.
         */
        public Throwable error() {

            if (future.isDone() && !future.isCancelled()) {

                try {

                    future.get();

                    // no exception.
                    return null;

                } catch (InterruptedException ex) {
                    // will not happen since the task is done.
                    throw new AssertionError();
                } catch (CancellationException ex) {
                    // will not happen since we checked this above.
                    throw new AssertionError();
                } catch (ExecutionException ex) {
                    // the cause of the exception.
                    return ex.getCause();
                }

            }

            // not done yet or cancelled.
            return null;

        }

        /**
         * Cancel the task if it is still running.
         * 
         * @todo this implementation is synchronous since it relies on
         *       {@link Future#cancel(boolean)}.  It will block the code
         *       that is polling the queue of waiting tasks, but it will
         *       not block the running tasks themselves.
         */
        public void cancelIfStillRunning() {

            if (!future.isDone()) {

                future.cancel(true/* mayInterruptIfRunning */);

            }

        }

    }
    
    /**
     * The state for a running {@link IMapTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MapTaskState extends TaskState {

        // the input file to the map task.
        final File file;

        /**
         * @param delay
         * @param uuid
         * @param ntries
         * @param future

         * @param file
         *            The input file for the map task.
         */
        protected MapTaskState(long delay, UUID uuid, int ntries, Future<Object> future, File file) {
            super(delay, uuid, ntries, future);

            this.file = file;
        
        }

    }

    /**
     * The state for a running {@link IReduceTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ReduceTaskState extends TaskState {

        // the data service from which the reduce task will read.
        final UUID dataService;

        /**
         * @param delay
         * @param uuid
         * @param ntries
         * @param future
         * 
         * @param dataService
         *            The data service from which the reduce task will read its
         *            input.
         */
        protected ReduceTaskState(long delay, UUID uuid, int ntries, Future<Object> future, UUID dataService) {
            
            super(delay, uuid, ntries, future);

            this.dataService = dataService;
        
        }

    }

    public static class TestCountKeywordJob extends MapReduceJob {

        public TestCountKeywordJob(int m, int n) {

            super(m, n, new FileSystemMapSource(new File("."),
                    new FileFilter() {

                        public boolean accept(File pathname) {

                            if (pathname.isDirectory()) {

                                // i.e., recursive processing of directories.

                                return true;

                            }

                            String name = pathname.getName();

                            return name.endsWith(".java");

                        }

                    }),
                    
                    // map task
//                    new NopMapTask(),
//                    new ReadOnlyMapTask(),
                    ExtractKeywords.class,
                    
                    // reduce task.
                    CountKeywords.class,
                    
                    DefaultHashFunction.INSTANCE);

        }

    }

}
