/*

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
package com.bigdata.service.mapred;

import java.io.IOException;
import java.text.Format;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.BufferMode;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.mapred.MapService.EmbeddedMapService;
import com.bigdata.service.mapred.ReduceService.EmbeddedReduceService;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * <p>
 * Abstract base class implementing the master for running map/reduce jobs.
 * </p>
 * <p>
 * Map/reduce is a functional programming style in which a program is broken
 * down into a <i>map</i> and a <i>reduce</i> operation. Those operations are
 * trivially parallelized and distributed across one or more worker tasks on
 * available hosts. In general, there are M map tasks and N reduce tasks for
 * each map/reduce job. The map operation, the intermediate store, and the
 * reduce tasks are distinct services. In particular, this means that the job
 * state resides with the master while the intermediate state for the reduce
 * tasks is decoupled from the reduce logic.
 * </p>
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
 * </p>
 * <p>
 * Each reduce task reads the intermediate store having data for the
 * intermediate key-value partition assigned to that reduce task. The keys in
 * those partitions are essentially random since they are assigned to partitions
 * by the (user-defined) hash function. However, the intermediate store
 * guarentees that the key-value pairs will be fully ordered. The reduce task is
 * then run on the total order, writing its outputs onto a local output file or
 * loading the data into scale-out indices.
 * </p>
 * <p>
 * The map/reduce master selects map and reduce services on which to run a
 * map/reduce job and is responsible for re-assigning tasks that fail or that
 * take are taking too long. The master attempts to use reduce services that are
 * "close" to the intermediate data (in terms of the network topology) but not
 * necessarily on the same host - this decouples the reduce operation from the
 * input state and lets us failover to another reduce service as necessary.
 * </p>
 * 
 * <h4>Design alternatives</h4>
 * 
 * Note: There are several design alternatives for reduce state and behavior,
 * including:
 * <ol>
 * 
 * <li>Map writes on stores local to the reduce service via RMI. The reduce
 * local stores are created when a map/reduce job starts and released when it
 * ends.</li>
 * 
 * <li>Map and reduce use {@link DataService}s as the store files. A set of
 * {@link DataService}s would either be dedicated to map/reduce tasks or
 * multiplexed with other operations. If dedicated data services are used, then
 * use {@link BufferMode#Disk} and set the overflow threshold quite high since
 * large temporary data sets are to be expected. (The advantage of this design
 * is that state and behavior are isolated: if a service fails you can just
 * re-run the specific tasks since the state is not lost; you can have state
 * failover using the existing {@link DataService} infrastructure.)</li>
 * 
 * <li>Map writes sorted data for each reduce partition on local disk; reduce
 * copies the data from the fan-in map services and either uses a merge sort or
 * a fused view to provide a total ordering. The map output stores are created
 * when a map operation starts and released when the reduce operation ends. The
 * reduce store (if any) is created when the reduce operation starts and
 * released when it ends. (The advantage here is that you mostly read and write
 * on local storage, except for copying data from the map service outputs to the
 * reduce service inputs. The disadvantage is that state is local to services
 * such that a map server failure during reduce setup could lead to the
 * re-execution of the entire job.)</li>
 * 
 * </ol>
 * </p>
 * 
 * <p>
 * Another design choice was the use of B+Trees for the intermediate stores. The
 * B+Tree automatically forces the tuples into a total order within each reduce
 * partition. The use of buffering (potentially across multiple map tasks for a
 * single map job) and group commit help to improve bulk insert performance on
 * the index. The most obvious alternative would be to use an external sort
 * program to sort the tuples in a reduce partition before the reduce job
 * executes. The keys are variable length (unsigned) byte[]s and the values are
 * variable length byte[]s, so the standard Un*x sort utilities will not work.
 * While many sort algorithms preserve duplicate keys, the B+Tree does not.
 * Therefore we make sure that the application key is extended to include a map
 * task identifier and map task local tuple counter so that key collision can
 * only arise when the same map task executes more than once - for example, when
 * retrying a failed or long running map task. In such cases the most recent run
 * of the task will simply overwrite the earlier run.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Explore the parameter space (m,n,# map services, # reduce services,
 * buffer mode, flush on commit, overflow, group commit, job size, # of
 * machines). Look at job completion rate, scaling, bytes read/written per
 * second, etc.
 * 
 * FIXME get working with a distributed federation as well.
 * 
 * @todo offer option to buffer map outputs across map tasks within the service
 *       to increase the size of write operations and improve performance.
 *       <p>
 *       Note: Any time that we buffer output tuples across tasks we are faced
 *       with the possibility that we must re-execute those tasks if the service
 *       fails since their state has not been transferred to the reduce worker.
 *       <p>
 *       If the intermediate state is not too large, then we could do a sort on
 *       each reduce partition split from each map worker (in memory) and then a
 *       merge sort of the reduce partition splits.
 * 
 * @todo Offer options for the behavior in the face of failed map tasks.
 *       <p>
 *       One option is that the task is simply retried, partial data may be
 *       present if the task never succeeds, and if the task eventually succeeds
 *       then the data will reflect the full execution of that task (a purely
 *       additive model with atomic append only on the individual reduce stores
 *       and not across the reduce stores on which a given map task writes).
 *       <p>
 *       Another alternative is that the output from map tasks that do not
 *       complete within this many retries will not participate in the reduce
 *       task. In order to guarentee this we need to do a
 *       <em>distributed transactional</em> atomic "append" of tuples across
 *       all reduce stores on which a map task writes. Since we know that there
 *       will never be collision from different map tasks (the map task UUID is
 *       part of the key), we can could optimize this distributed transaction in
 *       a number of ways. First, write-write conflicts can not arise. Second,
 *       we could avoid the use of real transactions (since there will never be
 *       conflicts) if we support the concept of a distributed "unisolated"
 *       atomic commit.
 * 
 * @todo map/reduce with suitable temporary storage could likely be reused to do
 *       distributed joins. Take a look at the client index view for how
 *       iterators are being range partitioned and see if this could be
 *       refactored for a distributed join.
 * 
 * @todo Map and reduce could be abstracted decomposed tasks reading and writing
 *       data in parallel. Map/Reduce becomes a 2-stage chain with slight
 *       specialization for its stages (e.g., hash partitioning output tuples
 *       and writing onto a sorted store for map while reduce reads from a
 *       sorted store). This generalization might require letting the user write
 *       the run() method that is executed by the task worker on the service so
 *       that more input and output options are enabled.
 * 
 * @todo Generalize {@link IMapSource} so that we can read input 2-tuples from a
 *       variety of sources, including indices, other map reduce jobs, etc. This
 *       interface currently assumes that the inputs for the map operations are
 *       files in a (networked) file system.
 * 
 * @todo Iff the write on the reduce output files is to be atomic (so the map
 *       task has no effect unless it completes successfully), then we need to
 *       use a transaction (since we are actually writing onto N distinct reduce
 *       input files). (The writes on the individual reduce input files will
 *       always be atomic, but a failure of some operations would leave part of
 *       the effect of the map task in some of the reduce input files.)
 * 
 * @todo A good example of a map/reduce job would be bulk loading a large Lehigh
 *       Benchmark data set. The map job would parse the files that were local
 *       on each host, distributing triples across the cluster using a hash
 *       function. The reduce job would sort the triplets arriving at each host
 *       and then bulk load those triplets into the indices.
 *       <p>
 *       Another example would be to bulk index terms in documents, etc.
 * 
 * @todo There may well be optimizations available for partitioned bulk index
 *       builds from the reduce operation that would then be migrated to the
 *       appropriate data service and merged into the current index view for the
 *       appropriate index partition).
 * 
 * @todo When reading from a sparse column store the definition of a row is
 *       different since the key is formed from the column name, application
 *       key, and timestamp.
 * 
 * @todo For at least GOM we need to deserialize rows from byte[]s, so we need
 *       to have the (de-)serializer to the application level value on hand.
 * 
 * @todo consider a BFS - a bigdata file system using the local file system to
 *       store files and a bigdata federation to store metadata. The file system
 *       would support atomic append, read, write, and delete operations. This
 *       should be trivial to implement over the existing infrastructure
 *       (failover would require sending the files about as well as the file
 *       metadata so that would be a little more complex). BFS could then be
 *       used as a distributed file system for fan-in to map/reduce jobs.
 *       <p>
 *       While Java clients for BFS might be easy enough, what challenges are
 *       involved in supporting BFS to Windows and Un*x clients?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractMaster {
    
    public static final transient Logger log = Logger
            .getLogger(AbstractMaster.class);

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
    IJobAndTaskService<MapJobMetadata, AbstractMapTask>[] mapServices;
    
    /**
     * The reduce services.
     */
    IJobAndTaskService<ReduceJobMetadata,AbstractReduceTask>[]reduceServices;
    
    /**
     * The UUID assigned to each reduce task.
     */
    UUID[] reduceTasks;
    
    /**
     * The data services for each reduce partition. The index on the data
     * service is named by the UUID of the corresponding reduce task.
     */
    UUID[] dataServices;

    /**
     * Metadata for the map operation.
     */
    MapJobMetadata mapJobMetadata;
    
    /**
     * Metadata for the reduce operation.
     */
    ReduceJobMetadata reduceJobMetadata;

    /**
     * The object that runs the map tasks.
     */
    RemoteTaskRunner<MapJobMetadata, AbstractMapTask> mapTaskRunner;
    
    /**
     * The object that runs the reduce tasks.
     */
    RemoteTaskRunner<ReduceJobMetadata, AbstractReduceTask> reduceTaskRunner;
    
    /**
     * The #of map tasks submitted so far (retries are not counted).
     */
    public long getMapTaskCount() {
        
        return mapTaskRunner == null ? 0 : mapTaskRunner.ntasks;
        
    }
    
    /**
     * The #of reduce tasks submitted so far (retries are not counted).
     */
    public long getReduceTaskCount() {
        
        return reduceTaskRunner == null ? 0 : reduceTaskRunner.ntasks;
        
    }
    
    /**
     * The elapsed time for the map operation (the clock stops once
     * the map operation is over).
     */
    public long getMapElapsedTime() {
        
        return mapTaskRunner == null ? 0 : mapTaskRunner.elapsed();
        
    }
    
    /**
     * The elapsed time for the reduce operation (the clock stops once the
     * reduce operation is over).
     */
    public long getReduceElapsedTime() {
        
        return reduceTaskRunner == null ? 0 : reduceTaskRunner.elapsed();
        
    }
    
    /**
     * The #of map tasks that eventually succeeded.
     */
    public long getMapSuccessCount() {
        
        return mapTaskRunner == null ? 0 : mapTaskRunner.nsuccess;
        
    }

    /**
     * The #of reduce tasks that eventually succeeded.
     */
    public long getReduceSuccessCount() {
        
        return reduceTaskRunner == null ? 0 : reduceTaskRunner.nsuccess;
        
    }

    /**
     * The #of map tasks that were retried (this counts each retry of each
     * task).
     */
    public long getMapRetryCount() {

        return mapTaskRunner == null ? 0 : mapTaskRunner.nretried;

    }

    /**
     * The #of reduce tasks that were retried (this counts each retry of
     * each task).
     */
    public long getReduceRetryCount() {
        
        return reduceTaskRunner == null ? 0 : reduceTaskRunner.nretried;
        
    }

    /**
     * The #of map tasks that permanently failed.
     */
    public long getMapFailedCount() {
        
        return mapTaskRunner == null ? 0 : mapTaskRunner.nfailed;
        
    }

    /**
     * The #of reduce tasks that permanently failed.
     */
    public long getReduceFailedCount() {
        
        return reduceTaskRunner == null ? 0 : reduceTaskRunner.nfailed;
        
    }

    /**
     * Setup the master to run a map/reduce job.
     * 
     * @param job
     *            The map/reduce job to execute.
     * @param client
     *            The client used to read/write data stored in a federation.
     */
    public AbstractMaster(MapReduceJob job, IBigdataClient client) {

        if (job == null)
            throw new IllegalArgumentException();

        if (client == null)
            throw new IllegalArgumentException();

        this.job = job;
        
        this.client = client;
    
    }
    
    /**
     * Select the map, reduce, and data services to be used by the job.
     * <p>
     * 
     * @exception IllegalStateException
     *                if {@link #mapServices} and {@link #reduceServices} have
     *                not been initialized before calling this method.
     */
    protected void setUp() {

        if(mapServices==null||reduceServices==null) {
            
            throw new IllegalStateException(
                    "map/reduce services are not initialized");
            
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

        setUpDataStores();
        
        mapJobMetadata = new MapJobMetadata(job.uuid,reduceTasks,dataServices);
        
        reduceJobMetadata = new ReduceJobMetadata(job.uuid);
        
    }
    
    /**
     * Setup intermediate indices on the data services (one per reduce
     * task). The same data service MAY be used for more than one reduce
     * task.
     * 
     * @todo in order for the map and reduce tasks to have automatic
     *       failover they need to query the metadata service if the given
     *       data service fails and find the replacement data service (this
     *       presumes that a media redundency chain is in effect).
     */
    protected void setUpDataStores() {

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
                    client.getDataService(dataServices[i])
                            .registerIndex(
                                    name,
                                    new IndexMetadata(reduceTasks[i]));
                } catch (Exception e) {
                    log.warn("Could not create intermediate store: " + e);
                }
            }
        }

    }

    /**
     * Tear down the embedded services.
     */
    protected void tearDown() {

        for(int i=0; i<mapServices.length; i++) {
            
            if(mapServices[i] instanceof EmbeddedMapService) {
            
                ((EmbeddedMapService)mapServices[i]).shutdown();
                
            }
            
        }
        
        for(int i=0; i<reduceServices.length; i++) {

            if(reduceServices[i] instanceof EmbeddedReduceService) {
                
                ((EmbeddedReduceService)reduceServices[i]).shutdown();
                
            }

        }
        
    }
    
    /**
     * Terminate the job.
     */
    protected void terminate() {

        /*
         * Release the intermediate stores.
         */

        if (dataServices != null) {

            for (int i = 0; i < dataServices.length; i++) {

                try {
                
                    IDataService ds = client.getDataService(dataServices[i]);
                    
                    if (ds == null) {
                        
                        log.warn("Could not locate data service: "
                                + dataServices[i]);
                        
                    }
                    
                    // the index name (local on the data service).
                    String name = reduceTasks[i].toString();
                    
                    ds.dropIndex(name);
                    
                } catch (Exception e) {
                    
                    log.warn("Data service: " + e);
                    
                }
                
            }

        }

        tearDown();
        
    }

    /**
     * Distribute the map tasks and wait for them to complete.
     * 
     * @return The percentage tasks that completed successfully.
     * 
     * @throws InterruptedException
     *             if the excecuting map tasks are interrupted.
     */
    @SuppressWarnings("unchecked")
    protected double map() throws InterruptedException { 

        Iterator<AbstractMapTask> tasks = new Striterator(job
                .getMapSource().getSources()).addFilter(new Resolver() {

                    private static final long serialVersionUID = 1L;

                    protected Object resolve(final Object arg0) {

                        return job.getMapTask(arg0);
                        
                    }

        });
        
        mapTaskRunner =
            new RemoteTaskRunner<MapJobMetadata, AbstractMapTask>(
                    mapServices,
                    mapJobMetadata,
                    tasks,
                    job.getMaxMapTasks(),
                    job.m, //maxConcurrency,
                    job.getMaxMapTaskRetry(),
                    job.getMapTaskTimeout()
                    );
        
        mapTaskRunner.awaitTermination(/*maxMapOpRuntime(ms)*/);

        return ((double)mapTaskRunner.nsuccess)/mapTaskRunner.ntasks;
        
    }

    /**
     * Distribute the reduce tasks and wait for them to complete.
     * 
     * @return The percentage tasks that completed successfully.
     * 
     * @exception InterruptedException
     *                if the executing tasks are interrupted.
     */
    protected double reduce() throws InterruptedException {

        /*
         * There is a fixed fan-in for the reduce operation.  It is the #of
         * reduce partitions declared by the map/reduce job.  Therefore we
         * put all of the tasks in an executor service with a maximum parallism
         * set by the caller.
         */
        
        AbstractReduceTask[] tasks = new AbstractReduceTask[job.n];
        
        for(int i=0; i<job.n; i++) {
            
            tasks[i] = (AbstractReduceTask) job.getReduceTask(reduceTasks[i], dataServices[i]);
            
        }
        
        reduceTaskRunner =
            new RemoteTaskRunner<ReduceJobMetadata, AbstractReduceTask>(
                    reduceServices,
                    reduceJobMetadata,
                    Arrays.asList(tasks).iterator(),
                    0L /* maxTasks */,
                    job.n, // maxConcurrency,
                    job.getMaxReduceTaskRetry(),
                    job.getReduceTaskTimeout()
                    );
        
        reduceTaskRunner.awaitTermination(/*timeout(ms)*/);

        return ((double)reduceTaskRunner.nsuccess)/reduceTaskRunner.ntasks;
        
    }

    /**
     * The percent success for the map operation [0:1].
     */
    public double getMapPercentSuccess() {

        return (mapTaskRunner == null ? 0d : ((double) mapTaskRunner.nsuccess)
                / mapTaskRunner.ntasks);
        
    }
    
    /**
     * The percent success for the reduce operation [0:1].
     */
    public double getReducePercentSuccess() {
        
        return (reduceTaskRunner == null ? 0d
                : ((double) reduceTaskRunner.nsuccess)
                        / reduceTaskRunner.ntasks);
    }
    
    /**
     * A summary of the current job state.
     */
    public String status() {

        final String map = "map( m=" + job.m + ", ntasks=" + getMapTaskCount()
                + ", nretried=" + getMapRetryCount() + ", success="
                + percent.format(getMapPercentSuccess()) + ", elapsed="
                + getMapElapsedTime() + "ms )";

        final String reduce = "reduce( n=" + job.n + ", ntasks="
                + getReduceTaskCount() + ", nretried=" + getReduceRetryCount()
                + ", success=" + percent.format(getReducePercentSuccess())
                + ", elapsed=" + getReduceElapsedTime() + "ms )";

        return map + "\n" + reduce;
        
    }
    
    // used to format percentages.
    final protected Format percent = NumberFormat.getPercentInstance();
    
    /**
     * Run the job.
     * 
     * @return <i>this</i>
     */
    public AbstractMaster run(double minMapSuccessRate, double minReduceSuccessRate) {
        
        /*
         * Notify everyone that this job is starting.
         */
        setUp();

        try {

            /*
             * Distribute the map tasks and wait for them to complete.
             */
            final double percentMapSuccess;
            {

                percentMapSuccess = map();
                
                System.out.println("Map operation "+percentMapSuccess+" success.");
                
                if(percentMapSuccess<minMapSuccessRate) {
                    
                    log.warn("Success rate less than threshold - aborting");
                    
                    return this;
                    
                }
                
            }

            /*
             * Distribute the reduce tasks and wait for them to complete.
             */
            final double percentReduceSuccess;
            {

                percentReduceSuccess = reduce();
               
                System.out.println("Reduce operation "+percentReduceSuccess+" success.");

                if(percentReduceSuccess<minReduceSuccessRate) {
                    
                    log.warn("Success rate less than threashold - aborting");
                    
                    return this;
                    
                }

            }

            log.info("Done:\n"+status());
            
        } catch (Throwable t) {
            
            log.error( "Problem running job: "+t, t );
            
        } finally {

            /*
             * Terminate the job.
             */

            try {
                terminate();
            } catch(Throwable t) {
                log.warn("Problem terminating master: "+t,t);
            }
            
        }

        return this;
        
    }
    
}
