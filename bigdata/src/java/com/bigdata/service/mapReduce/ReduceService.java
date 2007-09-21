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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.journal.ITx;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.RangeQuery;
import com.bigdata.service.mapReduce.MapReduceMaster.IReduceTask;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A service for {@link IReduceTask} processing. Those tasks are distributed by
 * the {@link MapReduceMaster}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ReduceService implements IServiceShutdown, IReduceService {

    public static final transient Logger log = Logger
            .getLogger(ReduceService.class);

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
     * Queue of executing {@link IReduceTask}s.
     */
    final protected ExecutorService taskService;

    /**
     * The default is N=1 threads per CPU.
     */
    final int threadPoolSize = 1 * SystemUtil.numProcessors();

    /**
     * @param properties
     * 
     * @todo define properties, including the thread pool sizes, the directory
     *       to be used for temporary files, etc.
     */
    public ReduceService(Properties properties) {
        
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
     * The state of a map/reduce job that is relevant to this service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class JobState {

        // the job identifier.
        final UUID uuid;
//
//        // the reduce task identifier.
//        final UUID reduceTask;
//        
//        // the data service identifier from which the reduce task will read.
//        final UUID dataService;
        
        // the job start time.
        final long begin = System.currentTimeMillis();
        
        public JobState(UUID uuid) {//, UUID reduceTask, UUID dataService) {
       
            this.uuid = uuid;
            
//            this.reduceTask = reduceTask;
//            
//            this.dataService = dataService;
            
        }
        
    }

    /**
     * Running jobs.
     */
    final protected Map<UUID,JobState> jobs = new ConcurrentHashMap<UUID, JobState>();

    public void startJob(UUID uuid) {//, UUID reduceTask, UUID dataService) {
        
        if (uuid == null)
            throw new IllegalArgumentException();

        synchronized (jobs) {

            if (jobs.containsKey(uuid)) {

                throw new IllegalStateException(ERR_JOB_EXISTS+" : "+uuid);

            }

            jobs.put(uuid, new JobState(uuid));

        }

        log.info("job=" + uuid );
        
    }

    public void endJob(UUID uuid) {

        if(uuid==null) throw new IllegalArgumentException();

        JobState jobState = jobs.remove(uuid);
        
        if(jobState==null) {
            
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+uuid);
            
        }
        
        log.info("job="+uuid);

    }

    public Future submit(UUID uuid, IReduceTask task) {

        JobState jobState = jobs.get(uuid);
        
        if (jobState == null) {

            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+uuid);
            
        }

        log.info("job=" + uuid+", task="+task.getUUID());

        // @todo make this work for a non-embedded federation also.
        return taskService.submit(new ReduceTaskWorker(
                ((EmbeddedReduceService) this).client, uuid, task));
    
    }
    
    /**
     * A worker for a reduce task.
     * 
     * @todo reduce tasks may begin running as soon as intermediate output files
     *       become available; the input to each reduce task is M index segments
     *       (one per map task); the data in those segments are already in
     *       sorted order, but they need to be placed into a total sorted order
     *       before running the reduce task. Given the tools on hand, the
     *       easiest way to achieve a total order over the reduce task inputs is
     *       to build a partitioned index. Since each reduce task input is
     *       already in sorted order, we can build the total sorted order by
     *       reading from the M input segments in parallel (an M-way merge).
     *       Since M can be quite high and the keys are randomly distributed
     *       across the input by the user-defined hash function, this merge
     *       operation will need to scale up to a large fan-in (100,000+).
     * 
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class ReduceTaskWorker implements Callable<Object> {

        protected final IBigdataClient client;
        protected final UUID uuid;
        protected final IReduceTask task;
        
        public ReduceTaskWorker(IBigdataClient client, UUID uuid, IReduceTask task) {

            if (client == null)
                throw new IllegalArgumentException();

            if (uuid == null)
                throw new IllegalArgumentException();

            if (task == null)
                throw new IllegalArgumentException();

            this.client = client;
            
            this.uuid = uuid;

            this.task = task;

        }
        
        /**
         * Run the {@link IReduceTask}.
         * <p>
         * This does a key scan, invoking
         * {@link IReduceTask#reduce(byte[], Iterator)} for each distinct
         * application key. The key in the index are comprised of an application
         * key, followed by the map task UUID and then an int32 tuple counter.
         * Since the prefix is variable length, we have to count backwards from
         * the end of the key to extract the application key. Any sequence of
         * entries having the same application key will be processed by a single
         * invocation of {@link IReduceTask#reduce(byte[], Iterator)}
         * 
         * @return <code>null</code>
         * 
         * FIXME Implement!
         */
        public Object call() throws Exception {

            log.info("Now running: job=" + uuid+", task="+task.getUUID());

            UUID dataService = task.getDataService();

            log.info("Reading from dataService="+dataService);
            
            IDataService ds = client.getDataService(dataService);

            if (ds == null)
                throw new RuntimeException("Could not locate dataService: "
                        + dataService);
            
            // the name of the index that we will read from.
            String name = task.getUUID().toString();

            // @todo make this a configuration parameter.
            final int capacity = 1000;

            // get both keys and values.
            final int flags = IDataService.KEYS | IDataService.VALS;

            // index scan.
            IEntryIterator itr = new RangeQuery(ds, name, ITx.UNISOLATED,
                    null/* fromKey */, null/* toKey */, capacity, flags);

            long ntuples = 0L;
            
            while(itr.hasNext()) {
                
                itr.next();
                
                ntuples++;
                
            }
            
            log.info("Read " + ntuples + " tuples on dataService="
                    + dataService + " for reduceTask=" + task.getUUID());

            return null;
            
        }
        
    }

    /**
     * A local (in process) {@link ReduceService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class EmbeddedReduceService extends ReduceService {
        
        final private UUID serviceUUID;
        
        final public IBigdataClient client;
        
        public EmbeddedReduceService(UUID serviceUUID, Properties properties, IBigdataClient client) {
            
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
