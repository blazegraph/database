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

package com.bigdata.service;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * A draft implementation of a map/reduce service. Map/reduce is a functional
 * programming style in which a program is broken down into a <i>map</i> and a
 * <i>reduce</i> operation. Those operations are trivially parallelized and
 * distributed across one or more worker tasks on available hosts. There will be
 * M map tasks and N reduce tasks for each map/reduce operation.
 * <p>
 * The inputs to the map operation are key-value pairs. Logicall, each map
 * operation processes a key-value pair, writing a set of intermediate key-value
 * pairs as its output. The outputs are automatically partitioned into N local
 * temporary files (one per reduce task) using a user-defined hash function.
 * <p>
 * Each reduce task reads from the M distinct files (one per map operation)
 * having data for the intermediate key-value partition assigned to that reduce
 * task. The keys in those partitions are essentially random since they are
 * assigned to partitions by the (user-defined) hash function. Before execution,
 * the reduce task inputs are placed into a total order, e.g., using a sort or
 * bulk index load. The reduce task is then run on the total order, writing its
 * outputs onto a single output file. There will be one such output file per
 * reduce task.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
 * @todo The Apache Hadoop project provides a MapReduce implementation. Explore
 *       possible ways in which an integration could be achieved.
 * 
 * @todo support "debugging" using a version that executes tasks for a single
 *       partition of subset of the data.
 */
public class MapReduceService implements IServiceShutdown {

    /**
     * Queue of executing jobs.
     */
    final protected ExecutorService jobService = Executors
            .newSingleThreadExecutor(DaemonThreadFactory
            .defaultThreadFactory());

    /**
     * @todo define lookup of the bigdata instance against which the named
     *       indices will be resolved.
     *       
     * @param properties
     */
    public MapReduceService(Properties properties) {
        
    }


    public void shutdown() {

        jobService.shutdown();
        
    }

    public void shutdownNow() {

        jobService.shutdownNow();
        
    }
    
    /**
     * Submit a job.
     * @param m
     * @param n
     * @param tx
     * @param name
     * @param fromKey
     * @param toKey
     * @param op
     */
    public Future submit(int m, int n, long tx, String name, byte[] fromKey,
            byte[] toKey, IMapOp op) {

        return jobService.submit(new Job(m,n,tx,name,fromKey,toKey,op));
        
    }
    
    /**
     * A scheduled map/reduce task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Job implements Callable<Object> {
 
        final int m;
        final int n;
        final long tx;
        final String name;
        final byte[] fromKey;
        final byte[] toKey;
        final IMapOp op;
        
        /*
         * @todo status for each of the M map tasks and N reduce tasks,
         * including where those tasks are running.
         */
        final Object status = null;

        /*
         * @todo these of course need to be remote tasks that are tracked with
         * a heartbeat.
         */
        final ExecutorService mapService;
        final ExecutorService reduceService;
        
        /**
         * A map/reduce job whose inputs are a key range of a named index.
         * 
         * @param m
         * @param n
         * @param tx
         * @param fromKey
         * @param toKey
         * @param op
         * 
         * @todo verify named index exists.
         * 
         * @todo by passing in the isolation level rather than the start time
         *       (or 0L) we can use the most recent data and do not retain old
         *       resources (journals and index segments) however, the downside
         *       is that you can create a multi-job transaction. by passing in
         *       the tx time, you can create a multi-job transaction if that is
         *       desired.
         * 
         * @todo supporting unisolated reads requires traversal under concurrent
         *       modification, e.g., cursor restart after each "batch" read.
         */
        public Job(int m, int n, long tx, String name, byte[] fromKey,
                byte[] toKey, IMapOp op) {
            assert m > 0;
            assert n > 0;
            assert name != null;

            this.m = m;
            this.n = n;
            this.tx = tx;
            this.name = name;
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.op = op;

            mapService = Executors.newFixedThreadPool(m,
                    DaemonThreadFactory.defaultThreadFactory());

            reduceService = Executors.newFixedThreadPool(n,
                    DaemonThreadFactory.defaultThreadFactory());

        }

        /**
         * Resolve the metadata service for the named index.
         * 
         * @param name
         *            The index name.
         * 
         * @return The metadata service for the named index.
         * 
         * @todo change the return type.
         */
        protected IMetadataService getMetadataService(String name) {
            
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * @todo assuming the data is in indices, partition the input data. this
         *       can be done with high accuracy by doing a rangeCount for the
         *       index, and then requesting the M-1 keys at the index entry
         *       positions that evenly divide the key space. since M can be
         *       rather large, we can start tasks as we go. if we have to
         *       restart a task, we can get its (approximate) key range by
         *       issuing new queries against the index.
         * 
         * @todo an alternative would assign map tasks to the hosts on which the
         *       data resides (this can apply with index inputs or with flat
         *       file inputs). if we are aware of the network topology, then we
         *       can assign the map tasks to hosts "near" the hosts on which the
         *       data resides.
         * 
         * @todo start up M map tasks; there is no point starting reduce tasks
         *       until the map tasks have completed since we need to provide a
         *       total ordering into the reduce tasks.
         */
        public Object call() throws Exception {
            
            IMetadataService mds = getMetadataService(name);
            
            final int nentries = mds.rangeCount(fromKey,toKey);  
            
            return null;
            
        }

    }

    /**
     * A worker for a map task.
     * 
     * @todo buffer the intermediate results for each map task on M buffers
     *       (indexed by the user-defined hash of the intermediate key); the
     *       buffers are btrees multiplexed on a journal; on the overflow, evict
     *       one index segment per btree resulting in N index segments per map
     *       task (so that the reduce task will start with each input partition
     *       in sorted order);
     * 
     * @todo the map task should be run on the data service (i.e., define a
     *       procedure and submit it to the data service).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MapWorker implements Callable<Object> {
        
        protected final long tx;
        protected final String name;
        protected final byte[] fromKey;
        protected final byte[] toKey;
        protected final IMapOp op;
        protected final int taskId;
        protected final int numReduce;
        
        public MapWorker(long tx, String name, byte[] fromKey, byte[] toKey,
                IMapOp op, int taskId, int numReduce) {
            
            this.tx = tx;
            this.name = name;
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.op = op;
            this.taskId = taskId;
            this.numReduce = numReduce;
            
        }

        public Object call() throws Exception {
            // TODO Auto-generated method stub
            return null;
        }
        
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
    public static class ReduceWorker implements Callable<Object> {

        public Object call() throws Exception {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
}
