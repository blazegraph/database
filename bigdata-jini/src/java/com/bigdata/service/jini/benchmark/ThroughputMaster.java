/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Mar 6, 2009
 */

package com.bigdata.service.jini.benchmark;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.KeeperException;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.counters.httpd.CounterSetHTTPDServer;
import com.bigdata.counters.render.XHTMLRenderer;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.accesspath.IRunnableBuffer;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.LoadBalancerService;
import com.bigdata.service.jini.DataServer;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.TaskMaster;
import com.bigdata.service.ndx.IScaleOutClientIndex;
import com.bigdata.service.ndx.pipeline.IDuplicateRemover;

/**
 * Utility class for benchmarking index operations on a federation. This test
 * harness can be parameterized for at least the following:
 * <dl>
 * <dt>general purpose scale-out index performance tests</dt>
 * <dd>These tests should focus on bulk index read, write, range count, and
 * key-range scan operations. general purpose scale-out indices do not constrain
 * the choice of separator keys for index partitions so any given read or write
 * MAY be split across one or more index partitions. The workload can be shaped
 * by the #of clients, by the relative proportion of reads and writes (including
 * inserts, updates, and deletes), by the distribution of the center and spread
 * of each read and write operation, the key type, the value type, and by the
 * frequency and key range of range count and key range scan operations.</dt>
 * <dt> sparse row store performance tests</dt>
 * <dd>These tests should excercise atomic row read, atomic row write, and
 * atomic row scans. by design the sparse row store restricts any given logical
 * row to a single index partition so each row read and row write operation will
 * be directed to ONE index partition. The row scan is atomic for each row read,
 * and may read multiple rows atomically. The workload can be shaped by the #of
 * clients, the degree of scatter of the primary keys for the logical row, the
 * #of non-null columns in each row, the #of distinct timestamped values for
 * each column, the key type, the value types, the mixture of reads and writes
 * (including inserts, updates, and deletes) (probably 90/10), and the frequency
 * and range of row scans. It is sufficient to test with a single schema (aka
 * column family) since that is realized as a simple prefix on the primary key.
 * validation should ensure that row reads and row writes are never split across
 * index partition boundaries. That can be done by a read on the metadata index
 * in which we examine the separator keys or by asserts in the code for read,
 * write and scan. It is the responsibility of the {@link ISimpleSplitHandler}
 * to ensure that separator keys are chosen for index partitions that fall on
 * logical row boundaries.</dd>
 * <dt> bigdata file system performance tests</dt>
 * <dd>These tests should exercise block append, read, and update and stream
 * oriented read and write. the workload can be shaped by the #of clients, by
 * the #of and distribution of files read and written, by the #of blocks in a
 * file, etc.</dd>
 * </dl>
 * These tests share several common features:
 * <ol>
 * <li>Operations are independent</li>
 * <li>Operations are UNISOLATED</li>
 * </ol>
 * Operations are local for the sparse row store and the bigdata file system,
 * and MAY or MAY NOT be local for generalized scale-out indices depending on
 * the workload. To the extent that the operations are independent and local the
 * system should scale linearly. Applications that concentrate operations on a
 * few key ranges will create hot spots for those key ranges which may or may
 * not translate into bottlenecks. Likewise, cache may be defeated to the extent
 * that operations rarely revisit recently visited parts of the key space.
 * <p>
 * Higher-level applications (such as the RDF DB) can introduce dependencies in
 * the scale-out indices. For example, a write on the RDF DB must first write on
 * TERM2ID, then on ID2TERM, and finally may write on the SPO, POS, and OSP
 * indices in parallel.
 * <p>
 * You can examine the performance on the system at several levels:
 * <dl>
 * <dt>Operating System</dt>
 * <dd>The {@link AbstractFederation} collects performance counters from the
 * underlying operating system using <code>vmstat</code> or the equivalent and
 * relays them to the {@link LoadBalancerService}. This is done (by default)
 * for all machines participating in a federation, including those running
 * clients.</dd>
 * <dt>Client</dt>
 * <dd>The {@link AbstractFederation} collects statistics on its work queues,
 * including the moving average of the queue length and the moving average of
 * the queueing time. This information is collected for all clients (including
 * the {@link DataService}s) and is relayed to the {@link LoadBalancerService}.
 * This mechanism may be extended by an application to report additional
 * performance counters. Clients MAY also report their own {@link Event}s.</dd>
 * <dt>Data services</dt>
 * <dd>The {@link DataServer}s collects statistics on its internal work
 * queues, including the moving average of the various queue lengths and the
 * moving average of the queueing time for those queues. The most interesting
 * queue is generally the "write service", which is where tasks which will write
 * on an index are queued. The {@link DataServer} also reports a variety of
 * {@link Event}s to the {@link LoadBalancerService}, most of which deal with
 * synchronous and asynchronous overflow handling.</dd>
 * </dl>
 * The {@link LoadBalancerService} normally exposes an {@link CounterSetHTTPD}
 * instance which may be used to examine the aggregated performance counters
 * using a navigational model or as correlated timeseries and pivot tables
 * suitable for import into a worksheet program. The {@link CounterSetHTTPD}
 * also exposes an {@link Event} viewer. See {@link XHTMLRenderer}. In
 * addition, any client or service MAY optionally expose a
 * {@link CounterSetHTTPD} instance giving a "live" look at the performance
 * counters for that host and service, but only the current value for each
 * counter will be available (no timeseries). Finally, the
 * {@link LoadBalancerService} logs both the performance counters and the
 * {@link Event}s. The performance counter logs are available for post-mortem
 * analysis using {@link CounterSetHTTPDServer}. The {@link Event}s are logged
 * in a tab-delimited format and may be imported into a worksheet for further
 * analysis.
 * <p>
 * While bigdata does not report metrics via JMX, you can enable remote JMX and
 * examine JMX counters for JVMs throughout the federation. This can be done
 * trivially if you are running one {@link DataServer} per host since you can
 * pre-assign the port number for remote JMX in the {@link Configuration} file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThroughputMaster
        extends
        TaskMaster<ThroughputMaster.JobState, ThroughputMaster.ClientTask, Void> {

    /**
     * {@link Configuration} options for the {@link ThroughputMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions extends TaskMaster.ConfigurationOptions {

//        /**
//         * #of scale-out indices to register.
//         */
//        String INDEX_COUNT = "indexCount";

        /**
         * The #of index operations to execute per client.
         */
        String OPERATION_COUNT = "operationCount";

        /**
         * The seed used for random data generation (default is 0, which uses a
         * different seed for each run).
         */
        String SEED = "seed";

        /**
         * The namespace for the indices registered by this job.
         */
        String NAMESPACE = "namespace";
        
        /**
         * When <code>true</code> the client writes will use the asynchronous
         * API. Otherwise they will use the synchronous RPC API.
         */
        String ASYNCHRONOUS = "asynchronous";
        
        /**
         * The #of index partitions (pre-splits the index). ZERO (0) which is
         * interpreted as NOT pre-splitting the index. There is no default.
         */
        String NPARTITIONS = "npartitions";

        /**
         * Boolean option specifies whether the key ranges for client operations
         * are disjoint or shared. When <code>true</code>, each client is
         * assigned a key-range when it is created and the client will choose
         * its startKey for each operations from that key-range. When
         * <code>false</code>, each client chooses a startKey randomly from
         * the entire key range for each task. There is no default.
         */
        String START_KEY_PARTITIONS = "startKeyPartitions";
        
        /**
         * The maximum #of keys to be used in each operation submitted by a
         * client. The actual number of keys is selected randomly from within
         * [1:maxKeys].
         */
        String MAX_KEYS_PER_OP = "maxKeysPerOp";
        
        /**
         * The keys for insert operations are selected by choosing a random key
         * from the legal key range as a starting key and then advancing by a
         * random number selected from [1:incRange].
         * <p>
         * This directly controls the maximum distance between keys in a batch
         * operations. In turn, that translates into the "sparsity" of the
         * operation. A small value (~10) can show 4x higher throughput than a
         * value of 1000. This is because the btree cache is more or less being
         * defeated as the spacing between the keys touched in any operation
         * grows.
         * <p>
         * When incRange is small, the randomly generately keys will be tightly
         * clustered and are likely to be not only within the same index
         * partition by in the same region of the B+Tree on that index
         * partition. As incRange is becomes larger, the keys are more likely to
         * be distributed within a given index partition.
         * 
         * @todo Even {@link Integer#MAX_VALUE} is not sufficient to cause
         *       scattering across index partitions since the range for
         *       {@link Long} is so much larger than the range {@link Integer}
         *       (it is restricted to Integer by {@link Random#nextInt(int)} but
         *       we could find a way to work around that).
         * 
         * @todo update incRange =&gt; operations per second (Disk, no sync on
         *       commit, laptop, 5.23.07).
         * 
         * <pre>
         * 
         * 10       463
         * 
         * 100      222
         * 
         * 1000     132
         * 
         * 10000    114
         * 
         * 100000   116
         * </pre>
         */
        String INC_RANGE = "incRange";
        
        /**
         * After every N operations, where N is specified by this property, the
         * state of the client will be updated in zookeeper. The interval may be
         * varied in order to determine when zookeeper becomes a bottleneck for
         * the database. The default is ZERO (0) which disables client updates
         * in zookeeper entirely.
         * 
         * @todo an expired session will end the run which makes this not very
         *       safe.
         */
        String ZOOKEEPER_UPDATE_INTERVAL = "zookeeperUpdateInterval";
        
    }

    /**
     * State describing the job to be executed. The various properties are all
     * defined by {@link ConfigurationOptions}.
     */
    public static class JobState extends TaskMaster.JobState {
        
        /**
         * 
         */
        private static final long serialVersionUID = 7663471973121918154L;

//        final int indexCount;

        /**
         * @see ConfigurationOptions#OPERATION_COUNT
         */
        public final long operationCount;
        
        /**
         * @see ConfigurationOptions#SEED
         */
        public final long seed;
        
        /**
         * @see ConfigurationOptions#NAMESPACE
         */
        public final String namespace;

        /**
         * @see ConfigurationOptions#ASYNCHRONOUS
         */
        public final boolean asynchronous;
        
        /**
         * @see ConfigurationOptions#NPARTITIONS
         */
        public final int npartitions;
        
        /**
         * @see ConfigurationOptions#START_KEY_PARTITIONS
         */
        public final boolean startKeyPartitions;

        /**
         * @see ConfigurationOptions#MAX_KEYS_PER_OP
         */
        public final int maxKeysPerOp;
        
        /**
         * @see ConfigurationOptions#INC_RANGE
         */
        public final int incRange;
        
        /**
         * @see ConfigurationOptions#ZOOKEEPER_UPDATE_INTERVAL 
         */
        public final long zookeeperUpdateInterval;
        
        @Override
        protected void toString(StringBuilder sb) {
        
//            sb.append(", " + ConfigurationOptions.INDEX_COUNT + "="
//                    + indexCount);

            sb.append(", " + ConfigurationOptions.OPERATION_COUNT + "="
                    + operationCount);
            
            sb.append(", " + ConfigurationOptions.SEED + "=" + seed);

            sb.append(", " + ConfigurationOptions.NAMESPACE + "=" + namespace);

            sb.append(", " + ConfigurationOptions.ASYNCHRONOUS + "="
                    + asynchronous);
            
            sb.append(", " + ConfigurationOptions.NPARTITIONS + "="
                    + npartitions);

            sb.append(", " + ConfigurationOptions.START_KEY_PARTITIONS + "="
                    + startKeyPartitions);

            sb.append(", " + ConfigurationOptions.MAX_KEYS_PER_OP + "="
                    + maxKeysPerOp);

            sb.append(", " + ConfigurationOptions.INC_RANGE+ "="
                    + incRange);

            sb.append(", " + ConfigurationOptions.ZOOKEEPER_UPDATE_INTERVAL
                    + "=" + zookeeperUpdateInterval);
            
        }

        public JobState(final String component, final Configuration config)
                throws ConfigurationException {

            super(component, config);

            // indexCount = (Integer) config.getEntry(component,
            // ConfigurationOptions.INDEX_COUNT, Integer.TYPE);

            operationCount = (Long) config.getEntry(component,
                    ConfigurationOptions.OPERATION_COUNT, Long.TYPE);

            seed = (Long) config.getEntry(component, ConfigurationOptions.SEED,
                    Long.TYPE, Long.valueOf(0));

            namespace = (String) config.getEntry(component,
                    ConfigurationOptions.NAMESPACE, String.class);

            asynchronous = (Boolean) config.getEntry(component,
                    ConfigurationOptions.ASYNCHRONOUS, Boolean.TYPE);

            npartitions = (Integer) config.getEntry(component,
                    ConfigurationOptions.NPARTITIONS, Integer.TYPE);

            startKeyPartitions = (Boolean) config.getEntry(component,
                    ConfigurationOptions.START_KEY_PARTITIONS, Boolean.TYPE);

            maxKeysPerOp = (Integer) config.getEntry(component,
                    ConfigurationOptions.MAX_KEYS_PER_OP, Integer.TYPE);

            incRange = (Integer) config.getEntry(component,
                    ConfigurationOptions.INC_RANGE, Integer.TYPE);
            
            zookeeperUpdateInterval = (Long) config.getEntry(component,
                    ConfigurationOptions.ZOOKEEPER_UPDATE_INTERVAL, Long.TYPE,
                    Long.valueOf(0));

        }

    }

    /**
     * @param fed
     * 
     * @throws ConfigurationException
     */
    protected ThroughputMaster(JiniFederation fed)
            throws ConfigurationException {

        super(fed);

    }

    /**
     * Runs the master. SIGTERM (normal kill or ^C) will cancel the job,
     * including any running clients.
     * 
     * @param args
     *            The {@link Configuration} and any overrides.
     * 
     * @throws ConfigurationException
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws KeeperException
     * 
     * @todo clients could report a throughput measure such as operations per
     *       second and the master should aggregate and report that back on the
     *       console (this data is available via the LBS).
     * 
     * @todo could report as tasks complete (#running, outcome).
     */
    static public void main(final String[] args) throws ConfigurationException,
            ExecutionException, InterruptedException, KeeperException {

        final JiniFederation fed = new JiniClient(args).connect();

        try {

            final TaskMaster task = new ThroughputMaster(fed);

            // execute master wait for it to finish.
            task.execute();

        } finally {

            fed.shutdown();
        }
        
    }

    /**
     * Extended to register a scale-out index in the specified namespace.
     */
    @Override
    protected void beginJob(final JobState jobState) throws Exception {

        super.beginJob(jobState);

        final String name = jobState.namespace;

        if (fed.getIndex(name, ITx.UNISOLATED) == null) {

            if (log.isInfoEnabled())
                log.info("Registering index: " + name);

            final int npartitions = getJobState().npartitions;

            if (npartitions > 0) {

                /*
                 * Pre-split the index partition when it is registered. The
                 * separator keys are choosen so as to evenly divide the
                 * workload. The choice is made based on the key generation
                 * (uniform generation of long integer keys, represented as
                 * unsigned byte[]s).
                 */
                
                final IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

                final byte[][] separatorKeys = new byte[npartitions][];

                separatorKeys[0] = new byte[] {};

                long key = 0;

                for (int i = 1; i < npartitions; i++) {

                    separatorKeys[i] = keyBuilder.reset().append(key).getKey();

                    key += Long.MAX_VALUE / npartitions;

                    if (log.isInfoEnabled())
                        log.info("separatorKey="
                                + BytesUtil.toString(separatorKeys[i]));

                }

                // use whatever is discovered by the MDS.
                final UUID[] dataServiceUUIDs = null;

                fed.registerIndex(new IndexMetadata(name, UUID.randomUUID()),
                        separatorKeys, dataServiceUUIDs);

            } else {
            
                fed.registerIndex(new IndexMetadata(name, UUID.randomUUID()));
                
            }

        }

    }

    @Override
    protected JobState newJobState(String component, Configuration config)
            throws ConfigurationException {

        return new JobState(component, config);

    }

    /**
     * The client task run on the {@link DataService}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo test when the task writes on a local index partition (simulates
     *       load of local data into the index) and when it writes on the
     *       scale-out index (simulates writes onto a distributed index).
     */
    static class ClientTask extends
            com.bigdata.service.jini.master.AbstractClientTask<JobState, Void, ClientState> {

        /**
         * 
         */
        private static final long serialVersionUID = 5950450307665619854L;

        /**
         * #of operations executed by this task thus far.
         */
        private transient long nops = 0L;
        private transient long lastNops = 0L;
        
        /**
         * Random number generator - not initialized until the task begins to
         * execute on the target {@link DataService}.
         */
        private transient Random r = null;
        
        protected ClientTask(final JobState jobState, final int clientNum) {

            super(jobState, clientNum);

        }

        @Override
        protected ClientState newClientState() {

            return new ClientState(nops);
            
        }

        @Override
        protected Void runWithZLock(final ClientState clientState) throws Exception,
                KeeperException, InterruptedException {
            
            if (r == null) {

                r = (jobState.seed == 0L) ? new Random() : new Random(
                        jobState.seed);

            }

            // unisolated view of the scale-out index.
            final IScaleOutClientIndex ndx = getFederation().getIndex(jobState.namespace,
                    ITx.UNISOLATED);

            final IDuplicateRemover<Void> duplicateRemover;
            final IRunnableBuffer<KVO<Void>[]> insertBuffer;
            final IRunnableBuffer<KVO<Void>[]> removeBuffer;
            if (jobState.asynchronous) {
                /*
                 * @todo enable optional duplicate removal and see what impact
                 * it has.
                 * 
                 * @todo support the AggregatorTask.
                 */
                duplicateRemover = null;
                // for inserts.
                insertBuffer = ndx.newWriteBuffer(//
                        (IResultHandler<Void, Void>) null,// resultHandler
                        duplicateRemover,
                        BatchInsertConstructor.RETURN_NO_VALUES);
                // for deletes.
                removeBuffer = ndx.newWriteBuffer(//
                        (IResultHandler<Void, Void>) null,// resultHandler
                        duplicateRemover,//
                        BatchRemoveConstructor.RETURN_MUTATION_COUNT);
            } else {
                duplicateRemover = null;
                insertBuffer = removeBuffer = null;
            }
            
            while (nops < jobState.operationCount) {

                // [1:maxKeysPerOp+1]
                final int nkeys = r.nextInt(jobState.maxKeysPerOp) + 1;

                /*
                 * Choose the startKey for this task.
                 * 
                 * Note: The startKey is restricted to be non-negative because
                 * of how we pre-split the index.
                 */
                
                final long firstKey;
                
                if (jobState.startKeyPartitions) {
                    
                    /*
                     * Random across the entire key range.
                     */

                    firstKey = Math.abs(r.nextLong());
                    
                } else {

                    /*
                     * Random within the pre-assigned key range.
                     */

                    // key range (1/Nth of the total key range).
                    final long keyRange = (Long.MAX_VALUE / jobState.nclients);
                    
                    // base constant for this client.
                    final long base = clientNum * keyRange;

                    // random value within the key range for this trial.
                    final long offset = (long) r.nextDouble() * keyRange;

                    // place client's start key within its assigned key range.
                    firstKey = base + offset;
                    
                }
                
                /*
                 * Note: balances inserts against deletes but probably more
                 * interesting to balance inserts against reads and mix up
                 * different kinds of isolation levels.
                 */
                final double insertRate = 1d;

                new Task(ndx, insertBuffer, removeBuffer, r, nkeys, firstKey,
                        jobState.incRange, insertRate).call();

                nops += nkeys;
                
                /*
                 * @todo experiment with this and see how much it limits
                 * throughput. Does zookeeper was choke the load rate if we do
                 * this for each operation? I could very easily see how it might
                 * since all writes on zookeeper are serialized and all tasks
                 * would be writing on zookeeper pretty much all the time!
                 * 
                 * @todo in fact, zookeeper can just die (session expired) so
                 * doing this is not very safe :-!
                 */
                if (jobState.zookeeperUpdateInterval != 0
                        && (nops - lastNops) >= jobState.zookeeperUpdateInterval) {

                    writeClientState(new ClientState(nops));

                    lastNops = nops;

                }
                
            } // next Task
            
            if(jobState.asynchronous) {
                
                // close the asynchronous write buffers.
                insertBuffer.close();
                removeBuffer.close();
                
                // await their futures.
                insertBuffer.getFuture().get();
                
                removeBuffer.getFuture().get();
                
            }
            
            /*
             * Note: All operations (synchronous and asynchronous) are reported
             * out on the same index counters. However, these data reflect only
             * the client side of the index operations and only for the specific
             * client. These counters are all reported to the LBS also so you
             * can get them from there and then aggregate them across the
             * clients.
             */
            System.err.println(getFederation().getIndexCounters(ndx.getName()));
            
            return null;
            
        }

    }

    /**
     * Run an unisolated operation.
     */
    public static class Task implements Callable<Void> {

        private final IIndex ndx;
        private final IRunnableBuffer<KVO<Void>[]> insert;
        private final IRunnableBuffer<KVO<Void>[]> remove;
        private final int nops;
        private final double insertRate;
        private final int incRange;
        private final Random r;
        private final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);

        private long lastKey;
        
        final private byte[] nextKey() {

            /*
             * Note: Must be +1 so that the keys are strictly increasing!
             * 
             * @todo change ClientIndexView test for keys that are out of order
             * since that can be a difficult error to detect and it violates the
             * split() principles when they are out of order.
             */
            final long key = lastKey + r.nextInt(incRange - 1) + 1;

            final byte[] data = keyBuilder.reset().append(key).getKey();
            
            lastKey = key;

            return data;

        }
        
        /**
         * @param ndx
         *            The index under test.
         * 
         * @todo parameterize for operation type (insert, remove, read,
         *       contains). let the caller determine the profile of operations
         *       to be executed against the service.
         */
        public Task(final IScaleOutClientIndex ndx,
                final IRunnableBuffer<KVO<Void>[]> insert,
                final IRunnableBuffer<KVO<Void>[]> remove, final Random r,
                final int nops, final long firstKey, final int incRange,
                final double insertRate) {

            this.ndx = ndx;

            this.insert = insert;
            
            this.remove = remove;
            
            this.r = r;

            if (insertRate < 0d || insertRate > 1d)
                throw new IllegalArgumentException();

            this.lastKey = firstKey;

            this.incRange = incRange;

            this.insertRate = insertRate;

            this.nops = nops;

        }

        /**
         * Executes a random batch operation with keys presented in sorted
         * order.
         * <p>
         * Note: Batch operations with sorted keys have twice the performance of
         * the corresponding operation with unsorted keys due to improved
         * locality of the lookups performed on the index.
         * 
         * @todo configure the value size distribution.
         */
        public Void call() throws Exception {

            if (r.nextDouble() <= insertRate) {

                /*
                 * Insert
                 */

                // log.info("insert: nops=" + nops);
                final KVO<Void>[] a = new KVO[nops];

                for (int i = 0; i < nops; i++) {

                    final byte[] key = nextKey();

                    final byte[] val = new byte[5];

                    r.nextBytes(val);

                    a[i] = new KVO<Void>(key, val);

                }

                if (insert == null) {

                    // synchronous RPC
                    ndx.submit(0/* fromIndex */, nops/* toIndex */, KVO
                            .getKeys(a), KVO.getVals(a), //
                            BatchInsertConstructor.RETURN_NO_VALUES, //
                            null// handler
                            );

                } else {

                    // asynchronous write.
                    insert.add(a);

                }

            } else {

                /*
                 * Remove.
                 * 
                 * @todo this is not terribly likely to encounter keys. instead
                 * this should be rewritten as an index procedure that samples
                 * the index for keys that do exist and deletes those tuples. In
                 * order to be scaleable, the keys would have to be selected by
                 * a rangeIterator().
                 */

                // log.info("remove: nops=" + nops);
                final KVO<Void>[] a = new KVO[nops];

                for (int i = 0; i < nops; i++) {

                    a[i] = new KVO<Void>(nextKey(), null/* val */);

                }

                if (remove == null) {

                    // synchronous RPC
                    ndx.submit(0/* fromIndex */, nops/* toIndex */, KVO
                            .getKeys(a), KVO.getVals(a),
                            BatchRemoveConstructor.RETURN_MUTATION_COUNT,//
                            null// handler
                            );

                } else {

                    // asynchronous write.
                    remove.add(a);

                }

            }
            
            return null;
            
        }
        
    }

    /**
     * State for the {@link ClientTask}. It stores the #of operations that have
     * been processed so that loosing the zlock does not cause us to restart
     * from scratch.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class ClientState implements Serializable {
        
        /**
         * 
         */
        private static final long serialVersionUID = -6780200715236939550L;
       
        public final long nops;
        
        public ClientState(final long nops) {
            
            this.nops = nops;
            
        }
        
    }
    
    @Override
    protected ClientTask newClientTask(final int clientNum) {

        return new ClientTask(getJobState(), clientNum);

    }
    
}
