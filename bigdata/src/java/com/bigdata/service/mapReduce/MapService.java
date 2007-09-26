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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.bigdata.journal.ITx;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IDataService;
import com.bigdata.service.IServiceShutdown;

/**
 * A service for {@link IMapTask} processing. Those tasks are distributed by the
 * {@link Master}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class MapService extends AbstractJobAndTaskService<MapJobMetadata, AbstractMapTask> implements IServiceShutdown {

    public static final transient Logger log = Logger
            .getLogger(MapService.class);

    /**
     * @param properties
     */
    public MapService(Properties properties) {

        super(properties);

    }

    /**
     * Returns instances of {@link MapTaskWorker}.
     */
    AbstractTaskWorker<MapJobMetadata, AbstractMapTask> 
        newTaskWorker(JobState<MapJobMetadata> jobState, AbstractMapTask task) {
        
        return new MapTaskWorker(jobState, task);
        
    }

    /**
     * A worker for a map task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MapTaskWorker extends AbstractTaskWorker<MapJobMetadata, AbstractMapTask> {

        public MapTaskWorker(JobState<MapJobMetadata> jobState, AbstractMapTask task) {

            super(jobState,task);

        }

        /**
         * Run the {@link IMapTask}.
         */
        protected void run() throws Exception {
            
            log.info("Now running: job=" + jobState.getUUID() + ", task="
                    + task.getUUID());
            
            final long begin1 = System.currentTimeMillis();
            
            if(task instanceof AbstractFileInputMapTask) {

                try {

                    AbstractFileInputMapTask t = ((AbstractFileInputMapTask)task);

                    /*
                     * Run the task.
                     * 
                     * @todo assumes that we are reading from a file.
                     */

                    t.input((File)t.getSource());

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

                    return;

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

                    write( jobState.client,
                           jobState.metadata.getReduceTasks()[i],
                           jobState.metadata.getDataServices()[i],
                           a );

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

            if(ntuples==0) {
                
                // No tuples for this reduce partition.
                
                return;
                
            }
            
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
        
        final public IBigdataClient bigdataClient;
        
        public EmbeddedMapService(UUID serviceUUID, Properties properties, IBigdataClient bigdataClient) {
            
            super(properties);
            
            if (serviceUUID == null)
                throw new IllegalArgumentException();
        
            if (bigdataClient == null)
                throw new IllegalArgumentException();
            
            this.serviceUUID = serviceUUID;
            
            this.bigdataClient = bigdataClient;
            
        }

        public UUID getServiceUUID() throws IOException {

            return serviceUUID;
            
        }

        public IBigdataClient getBigdataClient() {

            return bigdataClient;
            
        }
        
    }

}
