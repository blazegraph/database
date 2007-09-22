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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.RangeQuery;
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
     * The default is N=4 threads per CPU.
     */
    final int threadPoolSize = 4 * SystemUtil.numProcessors();

    /**
     * @param properties
     * 
     * @todo define properties (the thread pool size).
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
    static class JobState extends AbstractJobState {

        public JobState(UUID uuid) {
       
            super(uuid);
            
        }
        
    }

    /**
     * Running jobs.
     */
    final protected Map<UUID,JobState> jobs = new ConcurrentHashMap<UUID, JobState>();

    public void startJob(UUID uuid) {
        
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

        jobState.cancelAll();
        
        log.info("job="+uuid);

    }

    public Future submit(UUID uuid, IReduceTask task) {

        JobState jobState = jobs.get(uuid);
        
        if (jobState == null) {

            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+uuid);
            
        }

        log.info("job=" + uuid+", task="+task.getUUID());

        // @todo make this work for a non-embedded federation also.
        Future<Object> future = taskService.submit(new ReduceTaskWorker(
                ((EmbeddedReduceService) this).client, uuid, task));
        
        jobState.futures.put(task.getUUID(), future);
        
        return future;
    
    }
    
    public boolean cancel(UUID job, UUID task) {

        JobState jobState = jobs.get(job);
        
        if (jobState == null)
            throw new IllegalStateException(ERR_NO_SUCH_JOB+" : "+job);

        log.info("job=" + job+", task="+task);
        
        return jobState.cancel(task);

    }

    /**
     * A worker for a reduce task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class ReduceTaskWorker extends AbstractTaskWorker {

        protected final IReduceTask task;
        
        public ReduceTaskWorker(IBigdataClient client, UUID uuid, IReduceTask task) {

            super(client,uuid);

            if (task == null)
                throw new IllegalArgumentException();

             this.task = task;

        }
        
        /**
         * Run the {@link IReduceTask}.
         * <p>
         * This does a key scan, invoking
         * {@link IReduceTask#reduce(byte[], Iterator)} for each distinct
         * application key. The keys in the index are comprised of an
         * application key, followed by the map task UUID and then an int32
         * tuple counter. Since the prefix is variable length, we have to count
         * backwards from the end of the key to extract the application key. Any
         * sequence of entries having the same application key will be processed
         * by a single invocation of
         * {@link IReduceTask#reduce(byte[], Iterator)}
         * 
         * @return <code>null</code>
         * 
         * @exception Exception
         *                if something goes wrong.
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
            
            // the last distinct key prefix.
            byte[] lastDistinctKey = null;
            
            // buffers all values for a distinct key prefix.
            List<byte[]> vals = new LinkedList<byte[]>();
            
            while(itr.hasNext()) {
                
                byte[] val = (byte[])itr.next();
                
                byte[] key = itr.getKey();
                
                int len = key.length;
                
                // last byte of the application key.
                int end = len - Bytes.SIZEOF_UUID - Bytes.SIZEOF_INT;
                
                if (lastDistinctKey == null
                        || BytesUtil.compareBytesWithLenAndOffset(0,
                                lastDistinctKey.length, lastDistinctKey, 0, end, key) != 0) {
                    
                    if(!vals.isEmpty()) {
                        
                        // emit to the reduce task.
                        task.reduce(lastDistinctKey, vals.iterator());
                        
                        vals.clear();
                        
                    }
                    
                    // copy the new distinct prefix.
                    lastDistinctKey = new byte[end];
                    
                    System.arraycopy(key, 0, lastDistinctKey, 0, end);
                    
                }

                // buffer values.
                vals.add(val);

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
