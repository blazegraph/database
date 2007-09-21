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
 * Created on Sep 20, 2007
 */

package com.bigdata.service.mapReduce;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.journal.ITx;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.mapReduce.MapReduceMaster.AbstractFileInputMapTask;
import com.bigdata.service.mapReduce.MapReduceMaster.AbstractMapTask;
import com.bigdata.service.mapReduce.MapReduceMaster.IMapTask;
import com.bigdata.service.mapReduce.MapReduceMaster.IReduceTask;
import com.bigdata.service.mapReduce.MapReduceMaster.Tuple;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * The client service for map/reduce processing.
 * <p>
 * A map/reduce client can run either {@link IMapTask}s or {@link IReduceTask}s.
 * Those tasks are distributed to the client by the
 * {@link MapReduceMaster}.
 * <p>
 * 
 * @todo Each map/reduce job has a unique job identifier. The client creates the
 *       map output file (aka reduce input file) in a temporary directory. The
 *       file is named using that unique job identifier so that it may be purged
 *       once the map/reduce job is finished.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class MapService implements IServiceShutdown, IMapService {

    public static final transient Logger log = Logger
            .getLogger(MapService.class);

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
     * Queue of executing {@link IMapTask}s.
     */
    final protected ExecutorService taskService;

    /**
     * The default is N=50 threads per CPU.
     */
    final int threadPoolSize = 50 * SystemUtil.numProcessors();

    /**
     * @param properties
     * 
     * @todo define properties, including the thread pool sizes, the directory
     *       to be used for temporary files, etc.
     */
    public MapService(Properties properties) {

        taskService = Executors.newFixedThreadPool(threadPoolSize,
                DaemonThreadFactory.defaultThreadFactory());

    }

    public void shutdown() {

        taskService.shutdown();
        
    }

    public void shutdownNow() {

        taskService.shutdownNow();
        
    }
    
    /**
     * The unique identifier for this service.
     * 
     * @return The unique service identifier.
     */
    public abstract UUID getServiceUUID() throws IOException;

    /**
     * The state of a map/reduce job that is relevant to this service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class JobState {

        // the job identifier.
        final UUID uuid;

        // the reduce task identifiers.
        final UUID[] reduceTasks;
        
        // the data services on which the map task will write.
        final UUID[] dataServices;

        // the job start time.
        final long begin = System.currentTimeMillis();
        
        public JobState(UUID uuid,UUID[] reduceTasks, UUID[] dataServices) {
       
            this.uuid = uuid;
            
            this.reduceTasks = reduceTasks;
            
            this.dataServices = dataServices;
            
        }
        
    }

    /**
     * Running jobs.
     */
    final protected Map<UUID,JobState> jobs = new ConcurrentHashMap<UUID, JobState>();

    public void startJob(UUID uuid, UUID[] reduceTasks, UUID[] dataServices) {
        
        if(uuid==null) throw new IllegalArgumentException();

        if(reduceTasks==null) throw new IllegalArgumentException();
        
        if(reduceTasks.length==0) throw new IllegalArgumentException();

        if(dataServices==null) throw new IllegalArgumentException();

        if(dataServices.length != reduceTasks.length) throw new IllegalArgumentException();

        for (int i = 0; i < reduceTasks.length; i++) {

            if (reduceTasks[i] == null)
                throw new IllegalArgumentException();

            if (dataServices[i] == null)
                throw new IllegalArgumentException();
            
        }
        
        if(jobs.containsKey(uuid)) {
            
            throw new IllegalStateException(ERR_JOB_EXISTS+" : "+uuid);
            
        }
        
        jobs.put(uuid,new JobState(uuid,reduceTasks,dataServices));

        log.info("job=" + uuid + ", nreduceServices=" + reduceTasks.length);
        
    }

    public void endJob(UUID uuid) {

        if(uuid==null) throw new IllegalArgumentException();

        if(jobs.remove(uuid)==null) {
            
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+uuid);
            
        }

        log.info("job=" + uuid );

    }

    public Future submit(UUID uuid, File input, IMapTask task) {

        JobState jobState = jobs.get(uuid);
        
        if (jobState == null)
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+uuid);

        log.info("job=" + uuid+", task="+task.getUUID());

        // @todo make this work for distributed services also.
        return taskService
                .submit(new MapTaskWorker(((EmbeddedMapService) this).client,
                        uuid, input, task, jobState));
        
    }

    /**
     * A worker for a map task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MapTaskWorker implements Callable<Object> {

        protected final IBigdataClient client;
        protected final UUID uuid;
        protected final File input;
        protected final IMapTask task;
        protected final JobState jobState;
        
        /**
         * 
         * @param uuid
         *            The task UUID.
         * @param input
         *            The input file (typically read from a network file
         *            system).
         * @param task
         *            The map task.
         */
        public MapTaskWorker(IBigdataClient client, UUID uuid, File input, IMapTask task, JobState jobState) {

            if (client == null)
                throw new IllegalArgumentException();

            if (uuid == null)
                throw new IllegalArgumentException();

            if (task == null)
                throw new IllegalArgumentException();

            if (input == null)
                throw new IllegalArgumentException();

            if (jobState == null)
                throw new IllegalArgumentException();

            this.client = client;
            
            this.uuid = uuid;

            this.input = input;

            this.task = task;
            
            this.jobState = jobState;
            
        }

        /**
         * Run the {@link IMapTask}.
         * 
         * @return <code>null</code>
         */
        public Object call() throws Exception {

            log.info("Now running: job=" + uuid+", task="+task.getUUID());
            
            final long begin1 = System.currentTimeMillis();
            
            if(task instanceof AbstractFileInputMapTask) {

                try {

                    AbstractFileInputMapTask t = ((AbstractFileInputMapTask)task);

                    /*
                     * Run the task.
                     */

                    t.input(input);

                    final long elapsed1 = System.currentTimeMillis() - begin1;

                    log.info("Ran map operation in "+elapsed1+"ms");
                    
                    /*
                     * Bulk insert the ordered tuples into the reduce index.
                     */
                    
                    final long begin2 = System.currentTimeMillis();
                    
                    bulkLoad(t);
                    
                    final long elapsed2 = System.currentTimeMillis() - begin2;

                    log.info("Wrote buffered tuples in " + elapsed2
                            + "ms - total operation time is "
                            + (elapsed1 + elapsed2) + "ms");

                    return null;

                } catch (Throwable t) {

                    final long elapsed = System.currentTimeMillis() - begin1;

                    log.warn("Map task failed after "+elapsed+"ms : "+t, t);
                    
                    throw new RuntimeException( t );

                }
                
            } else {
                
                throw new RuntimeException("Do not know how to run this task: "
                        + task.getClass());
                
            }
            
        }
        
        /**
         * Divide the buffered output data for the map task into hash
         * partitioned buckets, sort the into increasing order by key, and
         * perform an atomic write onto N reduce output files.
         * <p>
         * Note: The map tasks must be able to write on the reduce task input
         * file without contention. The reduce task input files are typically
         * remote. This means that we either need an "atomic append" for the
         * network file system or a service that lets us write on the reduce
         * input file with the same semantics.
         * <p>
         * We use a {@link DataService} to solve this problem by writing on a
         * index that is local (or near) to the reduce client. The index will
         * automatically place the records into a total order. Since the index
         * will naturally remove duplicates, it is important to append a unique
         * suffix to each key. That suffix should include a map task local
         * counter and the map task UUID (that is, a global unique identifier
         * correlated with a specific input file as processed by a specific map
         * operation). The records are buffered and sorted by the map task so
         * that the B+Tree performance on insert is quite good.
         * 
         * @param task
         *            The map task.
         * 
         * <p>
         * @throws IOException 
         * @throws ExecutionException 
         * @throws InterruptedException 
         * 
         * @todo As an alternative, buffer the intermediate results for each map
         *       task on M buffers (indexed by the user-defined hash of the
         *       intermediate key); the buffers are btrees multiplexed on a
         *       journal; on the overflow, evict one index segment per btree
         *       resulting in N index segments per map task (so that the reduce
         *       task will start with each input partition in sorted order);
         * 
         * @todo Iff the write on the reduce output files is to be atomic (so
         *       the map task has no effect unless it completes successfully),
         *       then we need to use a transaction (since we are actually
         *       writing onto N distinct reduce input files). (The writes on the
         *       individual reduce input files will always be atomic, but a
         *       failure of some operations would leave part of the effect of
         *       the map task in some of the reduce input files.)
         */
        public void bulkLoad(AbstractMapTask task) throws InterruptedException, ExecutionException, IOException {
            
            // the tuples with the partition identifiers.
            final Tuple[] tuples = task.getTuples();

            // #of output tuples from the map task.
            final int ntuples = tuples.length;
            
            if(ntuples==0) {
                
                // No data.
                
                return;
                
            }
            
            log.info("Will output "+ntuples+" tuples for mapTask="+task.uuid);

            // histogram of #of tuples in each partition.
            final int[] histogram = task.getHistogram();
            
            // the #of reduce partitions.
            final int npartitions = task.nreduce;
            
            /*
             * Divide up the tuples into their target partitions and sort each
             * partition.
             * 
             * @todo Some of these steps could be parallelized, especially the N
             * sort and N remote write operations. Since the map/reduce
             * operations are already parallelized this might not make much
             * difference.
             */
            final Tuple[][] partitions = new Tuple[npartitions][];
            {

                // allocate the output partition buffers.
                for(int i=0; i<npartitions; i++) {
                    
                    partitions[i] = new Tuple[histogram[i]];
                    
                }

                // #of tuples that we insert into each partition.
                final int[] histogram2 = new int[npartitions];
                
                // copy data into the output partition buffers.
                for(int i=0; i<ntuples; i++) {
                
                    Tuple t = tuples[i];
                    
                    partitions[t.partition][histogram2[t.partition]++] = t;
                    
                }

                // sort each partition and write it on the corresponding reduce store.
                for( int i=0; i<npartitions; i++) {
                    
                    Tuple[] a = partitions[i];
                    
                    Arrays.sort( a );

                    write(client, jobState.reduceTasks[i],
                            jobState.dataServices[i], a);

                }
            
            }
            
        }
        
        /**
         * Write the tuples onto a reduce store.
         * 
         * @param tuples
         *            The tuples.
         * @throws IOException 
         * @throws ExecutionException 
         * @throws InterruptedException 
         */
        protected void write( IBigdataClient client, UUID reduceTask, UUID dataService, Tuple[] tuples ) throws InterruptedException, ExecutionException, IOException {
            
            IDataService ds = client.getDataService(dataService);

            if (ds == null)
                throw new RuntimeException("Could not locate dataService: "
                        + dataService);
            
            // the name of the index on which we will write the data.
            String name = reduceTask.toString();

            final int ntuples = tuples.length;
            
            byte[][] keys = new byte[ntuples][];
            
            byte[][] vals = new byte[ntuples][];
            
            for(int i=0; i<ntuples; i++) {
                
                keys[i] = tuples[i].key;

                vals[i] = tuples[i].val;

            }

            ds.batchInsert(ITx.UNISOLATED, name, -1, ntuples, keys, vals,
                            false /* returnOldValues */);

            log.info("Wrote " + ntuples + " tuples on dataService="
                    + dataService + " for reduceTask="
                    + reduceTask);

        }

    }

    /**
     * A local (in process) {@link MapService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class EmbeddedMapService extends MapService {
        
        final private UUID serviceUUID;
        
        final public IBigdataClient client;
        
        public EmbeddedMapService(UUID serviceUUID, Properties properties, IBigdataClient client) {
            
            super(properties);
            
            if (serviceUUID == null)
                throw new IllegalArgumentException();
        
            if (client == null)
                throw new IllegalArgumentException();
            
            this.serviceUUID = serviceUUID;
            
            this.client = client;
            
        }

        public UUID getServiceUUID() throws IOException {

            return serviceUUID;
            
        }
        
    }

}
