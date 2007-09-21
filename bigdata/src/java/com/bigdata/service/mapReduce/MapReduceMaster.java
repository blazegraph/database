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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.BufferMode;
import com.bigdata.service.BigdataFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedBigdataClient;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.EmbeddedBigdataFederation.Options;
import com.bigdata.service.mapReduce.MapService.EmbeddedMapService;
import com.bigdata.service.mapReduce.ReduceService.EmbeddedReduceService;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

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
     * The encoding used when serializing a filename.
     */
    static final String FILENAME_ENCODING = "UTF-8";

    /**
     * @todo define lookup of the bigdata instance against which the named
     *       indices will be resolved.
     * 
     * @param properties
     */
    public MapReduceMaster(Properties properties) {

    }

    /**
     * Interface for a map task to be executed on a map/reduce client.
     * <p>
     * Each map task will be presented with a series of records drawn from the
     * input sources for the task. When the source is an index, the key-value
     * pairs will be presented in key order. The map operator is responsible for
     * writting zero or more key value pairs on the output sink. Those key value
     * pairs will be assigned to N different reduce tasks by applying the
     * user-defined hash function to the output key.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IMapTask {
        
        /**
         * The unique identifier for the map task. If a map task is retried then
         * the new instance of that task will have the <em>same</em>
         * identifier.
         */
        public UUID getUUID();
        
        /**
         * Output a key-value pair to the appropriate reduce task. For example,
         * the key could be a token and the value could be the #of times that
         * the token was identified in the input.
         * 
         * @param key
         *            The key. The key MUST be encoded such that the keys may be
         *            placed into a total order by interpreting them as an
         *            <em>unsigned</em> byte[]. See {@link KeyBuilder}.
         * @param val
         *            The value. The value encoding is essentially arbitrary but
         *            the {@link DataOutputBuffer} may be helpful here.
         */
        public void output(byte[] key, byte[] val);

    }
    
    /**
     * Interface for a reduce task to be executed on a map/reduce client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IReduceTask {
     
        /**
         * The unique identifier for this reduce task. If a reduce task is
         * retried then the new instance of the task will have the <em>same</em>
         * identifier.
         */
        public UUID getUUID();
        
        /**
         * The {@link IDataService}  from which the reduce task will read its
         * input.
         */
        public UUID getDataService();
        
        /**
         * Each reduce task will be presented with a series of key-value pairs
         * in key order. However, the keys will be distributed across the N
         * reduce tasks by the used defined hash function, so this is NOT a
         * total ordering over the intermediate keys.
         * <p>
         * Note: This method is never invoked for a key for which there are no
         * values.
         * 
         * @param key
         *            A key.
         * @param vals
         *            An iterator that will visit the set of values for that
         *            key.
         */
        public void reduce(byte[] key, Iterator<byte[]> vals) throws Exception;
        
    }

    /**
     * Interface for a map/reduce job.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public interface IMapReduceJob {

        /**
         * The UUID for this job.
         */
        public UUID getUUID();

        /**
         * The object that knows how to supply the master with the input sources
         * to be distributed among the map tasks.
         */
        public IMapSource getMapSource();

        /**
         * The #of map tasks to run.
         */
        public int getMapTaskCount();

        /**
         * The #of reduce tasks to run.
         */
        public int getReduceTaskCount();

        /**
         * The hash function used to assign key-value pairs generated by a map
         * task to a reduce tasks.
         */
        public IHashFunction getHashFunction();

        /**
         * The map task to be executed.
         */
        public IMapTask getMapTask();

        /**
         * Used to re-execute a map task. This produces a new map task instance
         * with the same task UUID.
         * 
         * @param uuid The task identifier.
         */
        public IMapTask getMapTask(UUID uuid);

        /**
         * Used to (re-)execute a reduce task. This produces a new reduce task
         * instance with the same task UUID.
         * 
         * @param uuid
         *            The task identifier. This is assigned when the map/reduce
         *            job starts.
         * @param dataService
         *            The data service from which the reduce task will read its
         *            data. This is also assigned when the map/reduce task
         *            starts.
         */
        public IReduceTask getReduceTask(UUID uuid, UUID dataService);

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
         * @todo cleanly separate the notion of M and N (#of map tasks and #of
         *       reduce tasks) from the notion of the #of map services and the
         *       #of reduce services.
         */
        protected void setUp() {

            /**
             * FIXME Since we are running everything in process, this is the #of
             * map/reduce clients to be used. When we convert to distributed
             * map/reduce, we will need to perform service discovery for the
             * map/reduce clients.
             */
            final int numMapServices = 1;
            final int numReduceServices = 1;

            /**
             * The map services. Each services is capable of running a large #of
             * map tasks in parallel.
             */
            mapServices = new IMapService[numMapServices];
            {

                // client properties.
                Properties properties = new Properties();

                for (int i = 0; i < numMapServices; i++) {

                    mapServices[i] = new EmbeddedMapService(UUID.randomUUID(),
                            properties, client);

                }

            }

            /**
             * The reduce services. Each service is capable of running a few
             * reduce tasks in parallel.
             */
            reduceServices = new IReduceService[numReduceServices];
            {

                // client properties.
                Properties properties = new Properties();

                for (int i = 0; i < numReduceServices; i++) {

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
         * @todo could be modified to also specify the reduce store identifiers,
         *       which would then be passed to both map clients and reduce
         *       clients. At that point we do not need a startJob notice per say
         *       for the reduce clients.
         * 
         * @todo parallelize notice so that we can start/end jobs faster.
         * 
         * @todo handle remove clients that become unavailable either here
         *       (right after discovery) or during processing. Once a remote
         *       client becomes unavailable we should no longer attempt to task
         *       it, we should move its tasks to other clients, and someone
         *       should make sure that there are enough clients running on the
         *       cluster.
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
         * @todo [m] should limit the #of active map tasks for this job. this
         *       requires that we monitor the state of each map task that has
         *       been started so that we know when to start a new map task. For
         *       each task started for a given job we need to know: input file,
         *       Task UUID, client UUID, start time, #of tries. The client
         *       should give us a heartbeat (when realized as a service). Until
         *       then, the client will either report success or failure.
         * 
         * @todo Long running tasks should be replicated once we are "mostly"
         *       done.
         * 
         * @return The percentage tasks that completed successfully.
         * 
         * @todo Can the return value mislead the caller?  E.g., is it possible
         * for a task to succeed twice, resulting in double counting such that
         * we can not rely on the return to indicate whether all map tasks
         * completed successfully?
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
     * @todo the map/reduce cluster identifier - should this be the same as the
     *       bigdata federation identifier or should map/reduce clusters and
     *       scale-out indices be more indepenent of each other?
     * 
     * @todo the federation identifier (only required if reduce will write on
     *       the federation, so that could be part of the reduce task).
     * 
     * @todo describe the job using command line arguments or an XML file. The
     *       necessary data are: #of map tasks; #of reduce tasks; the class that
     *       implements the map task; and the class that implements the reduce
     *       task.
     * 
     * optional data are: override of the hash function;
     * 
     * the map task, the reduce task, and the hash function must be downloadable
     * code.
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
     * used.  (Likewise for reduce).
     */
    public static void main(String[] args) {

        MapReduceJob job = new TestCountKeywordJob(100/* m */, 4/* n */);
//        MapReduceJob job = new TestCountKeywordJob(1/* m */, 2/* n */);

        /**
         * The federation on which the intermediate stores will be written.
         * 
         * @todo this is using embedded federation - use a distributed one
         * but keep an option for testing that uses an embedded federation.
         */
        final IBigdataClient client;
        {
            
            Properties properties = new Properties();

            /*
             * @todo use the disk-only mode since we expect large jobs. Right
             * now this is transient since there is a problem with restart for
             * the embedded federation.
             */
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
//                    .toString());

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                    .toString());
          
            // #of data services to run in the federation @todo use more than one.
            properties.setProperty(Options.NDATA_SERVICES, "2");
            
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

    /**
     * Processes files in a named directory of a (network) file system.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo specify binary vs character data (using an encoding guesser).
     */
    public static class FileSystemMapSource implements IMapSource {

        private File dir;

        private FileFilter filter;

        /**
         * 
         * @param dir
         *            The top level directory.
         * @param filter
         *            The filter for files to be processed. Note: You MUST
         *            return <code>true</code> when the file is a directory if
         *            you want to recursively process subdirectories.
         */
        public FileSystemMapSource(File dir, FileFilter filter) {

            if (dir == null)
                throw new IllegalArgumentException();

            if (!dir.exists())
                throw new IllegalArgumentException("Does not exist: " + dir);

            if (!dir.isDirectory())
                throw new IllegalArgumentException("Not a directory: " + dir);

            this.dir = dir;

            this.filter = filter;

        }

        public Iterator<File> getSources() {

            return getSources(dir);

        }

        protected Iterator<File> getSources(File dir) {

            File[] files = (filter == null ? dir.listFiles() : dir
                    .listFiles(filter));

            return new Striterator(Arrays.asList(files).iterator())
                    .addFilter(new Expander() {

                        private static final long serialVersionUID = -6221565889774152076L;

                        protected Iterator expand(Object arg0) {

                            File file = (File) arg0;

                            if (file.isDirectory()) {

                                return getSources(file);

                            } else {

                                return new SingleValueIterator(file);

                            }

                        }

                    });

        }

    }

    /**
     * A key-value pair that is an output from an {@link IMapTask}. Instances
     * of this class know how to place themselves into a <em>unsigned</em>
     * byte[] order based on their {@link #key}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class Tuple implements Comparable<Tuple> {
        /**
         * The reduce input partition into which this tuple is placed by its
         * {@link #key}, the {@link IHashFunction} for the
         * {@link IMapReduceJob}, and the #of reduce tasks to be run.
         */
        final int partition;
        final byte[] key;
        final byte[] val;
        Tuple(int partition,byte[] key, byte[] val) {
            this.partition = partition;
            this.key = key;
            this.val = val;
        }
        public int compareTo(Tuple arg0) {
            return BytesUtil.compareBytes(key, arg0.key);
        }
    }
    
    /**
     * Abstract base class for {@link IMapTask}s.
     * <p>
     * Note: The presumption is that there is a distinct instance of the map
     * task for each task executed and that each task is executed within a
     * single-threaded environment.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract public static class AbstractMapTask implements IMapTask {

        protected final UUID uuid;
        protected final int nreduce;
        protected final IHashFunction hashFunction;

        private final List tuples = new ArrayList<Tuple>(1000);
        private final int[] histogram;

        /**
         * @param uuid
         *            The UUID of the map task. This MUST be the same UUID each
         *            time if a map task is re-executed for a given input. The
         *            UUID (together with the tuple counter) is used to generate
         *            a key that makes the map operation "retry safe". That is,
         *            the operation may be executed one or more times and the
         *            result will be the same. This guarentee arises because the
         *            values for identical keys are overwritten during the
         *            reduce operation.
         * @param nreduce
         *            The #of reduce tasks that are being feed by this map task.
         * @param hashFunction
         *            The hash function used to hash partition the tuples
         *            generated by the map task into the input sink for each of
         *            the reduce tasks.
         */
        protected AbstractMapTask(UUID uuid, Integer nreduce, IHashFunction hashFunction) {
            
            this.uuid = uuid;
            
            this.nreduce = nreduce;
            
            this.hashFunction = hashFunction;

            this.histogram = new int[nreduce];
            
        }

        public UUID getUUID() {
            
            return uuid;
            
        }
        
        /**
         * Return the tuples.
         * 
         * @return
         */
        public Tuple[] getTuples() {
        
            int ntuples = tuples.size();
            
            return (Tuple[]) tuples.toArray(new Tuple[ntuples]);
            
        }

        /**
         * The {@link KeyBuilder} MUST be used by the {@link IMapTask} so that
         * the generated keys will have a total ordering determined by their
         * interpretation as an <em>unsigned</em> byte[].
         * 
         * @todo does not always have to support unicode
         * @todo could configure the buffer size for some tasks.
         * @todo could choose the collation sequence for unicode.
         */
        protected final KeyBuilder keyBuilder = new UnicodeKeyBuilder();

        /**
         * The values may be formatted using this utility class. The basic
         * pattern is:
         * 
         * <pre>
         * valBuilder.reset().append(foo).toByteArray();
         * </pre>
         */
        protected final DataOutputBuffer valBuilder = new DataOutputBuffer();

        /**
         * The #of tuples written by the task.
         */
        public int getTupleCount() {

            return tuples.size();

        }

        /**
         * Forms a unique key using the data already in {@link #keyBuilder} and
         * appending the task UUID and the int32 tuple counter and then invokes
         * {@link #output(byte[], byte[])} to output the key-value pair. The
         * resulting key preserves the key order, groups all keys with the same
         * value for the same map task, and finally distinguishes individual
         * key-value pairs using the tuple counter.
         * 
         * @param val
         *            The value for the tuple.
         * 
         * @see #output(byte[], byte[])
         */
        public void output(byte[] val) {
        
            byte[] key = keyBuilder.append(uuid).append(tuples.size()).getKey();
            
            output(key,val);
            
        }
        
        /**
         * Map tasks MUST invoke this method to report key-value pairs. The data
         * will be buffered until the map task is complete.
         */
        public void output(byte[] key, byte[] val) {

            if (key == null)
                throw new IllegalArgumentException();

            if (val == null)
                throw new IllegalArgumentException();

            // Note: We have to fix up the sign when the hash code is negative!
            final int hashCode = hashFunction.hashCode(key);
            
            final int partition = (hashCode<0?-hashCode:hashCode) % nreduce;
            
            histogram[partition]++;
            
            tuples.add(new Tuple(partition,key,val));

        }

        /**
         * Return the histogram of the #of tuples in each output partition.
         */
        public int[] getHistogram() {

            return histogram;
            
        }
        
    }

    /**
     * Abstract base class for {@link IMapTask}s accepting a filename as the
     * "key" and the file contents as the "value".
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract public static class AbstractFileInputMapTask extends
            AbstractMapTask {

        protected AbstractFileInputMapTask(UUID uuid, int nreduce,
                IHashFunction hashFunction) {

            super(uuid, nreduce, hashFunction);

        }
        
        final public void input(File file) throws Exception {

            log.info("Start file: " + file);

            final InputStream is = new BufferedInputStream(new FileInputStream(
                    file));

            try {

                input(file, is);

                log.info("Done file : " + file + ", ntuples="
                                + getTupleCount());

            } finally {

                try {

                    is.close();

                } catch (Throwable t) {

                    log.warn("Problem closing input stream: " + file, t);

                }

            }

        }

        abstract protected void input(File input, InputStream is)
                throws Exception;

    }

    /**
     * Does nothing.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class NOPMapTask extends AbstractFileInputMapTask {

        public NOPMapTask(UUID uuid, Integer nreduce, IHashFunction hashFunction) {

            super(uuid, nreduce, hashFunction);

        }
        
        public void input(File file, InputStream is) throws Exception {

        }

    }

    /**
     * Reads the bytes and throws them away.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ReadOnlyMapTask extends AbstractFileInputMapTask {

        public ReadOnlyMapTask(UUID uuid, Integer nreduce, IHashFunction hashFunction) {

            super(uuid, nreduce, hashFunction);

        }
        
        public void input(File file, InputStream is) throws Exception {

            while(true) {
                
                int ch = is.read();
                
                if(ch==-1) break;
                
            }
            
        }

    }

    /**
     * Tokenizes an input file, writing <code>{key, term}</code> tuples. The
     * key is an compressed Unicode sort key. The term is a UTF-8 serialization
     * of the term (it can be deserialized to recover the exact Unicode term).
     * 
     * @see CountKeywords
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ExtractKeywords extends AbstractFileInputMapTask {

        /**
         * The encoding used to serialize the term (the value of each tuple).
         */
        public static final String UTF8 = "UTF-8";
        
//        /**
//         * A byte array representing a packed long integer whose value is ONE
//         * (1L).
//         */
//        final byte[] val;

        public ExtractKeywords(UUID uuid, Integer nreduce, IHashFunction hashFunction) {

            super(uuid, nreduce, hashFunction);

//            try {
//
//                valBuilder.reset().packLong(1L);
//
//            } catch (IOException ex) {
//
//                throw new RuntimeException(ex);
//
//            }
//
//            val = valBuilder.toByteArray();

        }

        public void input(File file, InputStream is) throws Exception {

            // @todo encoding guesser.

            Reader r = new BufferedReader(new InputStreamReader(is));

            StreamTokenizer tok = new StreamTokenizer(r);

            int nterms = 0;
            
            boolean done = false;
            
            while (!done) {

                int ttype = tok.nextToken();

                switch (ttype) {

                case StreamTokenizer.TT_EOF:
                    
                    done = true;
                    
                    break;

                case StreamTokenizer.TT_NUMBER: {

                    double d = tok.nval;

                    String s = Double.toString(d);
                    
                    keyBuilder.reset().append(s);

                    output(s.getBytes(UTF8));

                    nterms++;
                    
                    break;

                }

                case StreamTokenizer.TT_WORD: {

                    String s = tok.sval;

                    keyBuilder.reset().append(s);

                    output(s.getBytes(UTF8));

                    nterms++;
                    
                    break;

                }

                }

            }

        }

    }

    /**
     * Abstract base class for reduce tasks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract public static class AbstractReduceTask implements IReduceTask {

        /**
         * The task identifier.
         */
        final public UUID uuid;
        
        /**
         * The data service identifier. The task will read its data from the
         * index on the data service that is named by the task identifier.
         */
        final private UUID dataService;
        
        /**
         * 
         * @param uuid
         *            The task identifier.
         * @param dataService
         *            The data service identifier. The task will read its data
         *            from the index on the data service that is named by the
         *            task identifier.
         */
        protected AbstractReduceTask(UUID uuid, UUID dataService) {
            
            if(uuid==null) throw new IllegalArgumentException();
            
            if(dataService==null) throw new IllegalArgumentException();
            
            this.uuid = uuid;
            
            this.dataService = dataService;
            
        }
        
        public UUID getUUID() {
            
            return uuid;
            
        }

        public UUID getDataService() {
            
            return dataService;
            
        }
        
    }
    
    /**
     * Summarizes tuples of the form <code>{key, term}</code>.
     * <p>
     * Note that many terms may be conflated into the same Unicode sort key
     * depending on the collator that you are using. This task just deserializes
     * the 1st term entry for each distinct key. If you want some consistency in
     * the reported terms, then you should normalize the terms in your map task.
     * 
     * @see ExtractKeywords
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CountKeywords extends AbstractReduceTask {

        public CountKeywords(UUID uuid, UUID dataService) {
            
            super(uuid, dataService );
            
        }
        
        public void reduce(byte[] key, Iterator<byte[]> vals) throws Exception {

            String term = null;

            boolean first = true;

            long count = 0L;

            while (vals.hasNext()) {

                byte[] val = vals.next();

                if (first) {

                    term = new String(val, ExtractKeywords.UTF8);

                    first = false;

                }
                
                count++;

            }

            if (count == 0)
                throw new AssertionError();
            
            System.err.println(term+" : "+count);
            
        }

    }

    /**
     * This is default implementation of {@link IHashFunction} - it is based on
     * {@link Arrays#hashCode(byte[])}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DefaultHashFunction implements IHashFunction {

        public static transient final IHashFunction INSTANCE = new DefaultHashFunction();

        private DefaultHashFunction() {

        }

        public int hashCode(byte[] key) {

            return Arrays.hashCode(key);

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
