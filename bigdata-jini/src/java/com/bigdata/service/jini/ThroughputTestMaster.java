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

package com.bigdata.service.jini;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.zookeeper.KeeperException;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.journal.ITx;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.DataService;

/**
 * Utility class for benchmarking index operations on a federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThroughputTestMaster
        extends
        TaskMaster<ThroughputTestMaster.JobState, ThroughputTestMaster.ClientTask> {

    /**
     * {@link Configuration} options for the {@link ThroughputTestMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions {

//        /**
//         * #of scale-out indices to register.
//         */
//        String INDEX_COUNT = "indexCount";

        /**
         * #of index operations to execute per client.
         */
        String OPERATION_COUNT = "operationCount";

        /**
         * seed used for random data generation (default is 0).
         */
        String SEED = "seed";

        /**
         * The namespace for the indices registered by this job.
         */
        String NAMESPACE = "namespace";

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
        
        final long operationCount;
        
        final long seed;
        
        final String namespace;

        @Override
        protected void toString(StringBuilder sb) {
        
//            sb.append(", " + ConfigurationOptions.INDEX_COUNT + "="
//                    + indexCount);

            sb.append(", " + ConfigurationOptions.OPERATION_COUNT + "="
                    + operationCount);
            
            sb.append(", " + ConfigurationOptions.SEED + "=" + seed);
            
            sb.append(", " + ConfigurationOptions.NAMESPACE + "=" + namespace);
            
        }

        public JobState(final String component, final Configuration config)
                throws ConfigurationException {

            super(component, config);

//            indexCount = (Integer) config.getEntry(component,
//                    ConfigurationOptions.INDEX_COUNT, Integer.TYPE);

            operationCount = (Long) config.getEntry(component,
                    ConfigurationOptions.OPERATION_COUNT, Long.TYPE);

            seed = (Long) config.getEntry(component, ConfigurationOptions.SEED,
                    Long.TYPE, Long.valueOf(0));

            namespace = (String) config.getEntry(component,
                    ConfigurationOptions.NAMESPACE, String.class);

        }

    }

    /**
     * @param fed
     * @throws ConfigurationException
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected ThroughputTestMaster(JiniFederation fed)
            throws ConfigurationException, KeeperException,
            InterruptedException {

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
     */
    static public void main(final String[] args) throws ConfigurationException,
            ExecutionException, InterruptedException, KeeperException {

        final JiniFederation fed = new JiniClient(args).connect();

        final TaskMaster task = new ThroughputTestMaster(fed);

        // execute master wait for it to finish.
        task.innerMain().get();
        
    }

    /**
     * Extended to register a scale-out index in the specified namespace. 
     */
    @Override
    protected void beginJob() throws Exception {

        super.beginJob();

        fed.registerIndex(new IndexMetadata(getJobState().namespace, UUID
                .randomUUID()));

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
            com.bigdata.service.jini.TaskMaster.AbstractClientTask<JobState, Void, Serializable> {

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
        
        protected ClientTask(JobState jobState, int clientNum) {

            super(jobState, clientNum);

        }

        @Override
        protected Serializable newClientState() {

            return new ClientState(nops);
            
        }

        @Override
        protected Void runWithZLock() throws Exception, KeeperException,
                InterruptedException {
            
            if (r == null) {

                r = (jobState.seed == 0L) ? new Random() : new Random(
                        jobState.seed);

            }

            // unisolated view of the scale-out index.
            final IIndex ndx = fed.getIndex(jobState.namespace, ITx.UNISOLATED);

            while (nops < jobState.operationCount) {

                // @todo config and use normal distribution.
                final int nkeys = r.nextInt(1000) + 1;

                // random choice.
                final long firstKey = r.nextLong();
                
                final int incRange = 100;
                
                /*
                 * Note: balances inserts against deletes but probably more
                 * interesting to balance inserts against reads and mix up
                 * different kinds of isolation levels.
                 */
                final double insertRate = 1d;
                
                new Task(ndx, r, nkeys, firstKey, incRange, insertRate).call();
                
                nops += nkeys;
                
                /*
                 * @todo experiment with this and see how much it limits
                 * throughput. Does zookeeper was choke the load rate if we do
                 * this for each operation? I could very easily see how it might
                 * since all writes on zookeeper are serialized and all tasks
                 * would be writing on zookeeper pretty much all the time!
                 */
                if (nops - lastNops > 100000) {

                    writeClientState(new ClientState(nops));

                    lastNops = nops;
                    
                }
                
            }
            
            return null;
            
        }

    }

    /**
     * Run an unisolated operation.
     */
    public static class Task implements Callable<Void> {

        private final IIndex ndx;
        private final int nops;
        private final double insertRate;
        
        /*
         * @todo This has a very large impact on the throughput. It directly
         * controls the maximum distance between keys in a batch operations. In
         * turn, that translates into the "sparsity" of the operation. A small
         * value (~10) can show 4x higher throughput than a value of 1000. This
         * is because the btree cache is more or less being defeated as the
         * spacing between the keys touched in any operation grows.
         * 
         * The other effect of this parameter is to change the #of possible keys
         * in the index. A larger value allows more distinct keys to be
         * generated, which in turn increases the #of entries that are permitted
         * into the index.
         * 
         * incRange => operations per second (Disk, no sync on commit, laptop,
         * 5.23.07).
         * 
         * 10 => 463
         * 
         * 100 => 222
         * 
         * 1000 => 132
         * 
         * 10000 => 114
         * 
         * 100000 => 116
         * 
         * @todo Tease apart the sparsity effect from the #of entries effect, or
         * at least report the #of entries and height of the index at the end of
         * the overall run.
         * 
         * @todo when the incRange is smallish, it is very likely that the
         * operations will all fall into the same index partition.
         */
        final int incRange;
        
        long lastKey;

        final Random r;
        
        final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG);
        
        final private byte[] nextKey() {

            final long key = lastKey + r.nextInt(incRange);

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
         * 
         * @todo keyLen is ignored. It could be replaced by an increment value
         *       that would govern the distribution of the keys.
         */
        public Task(IIndex ndx, Random r, int nops, long firstKey,
                int incRange, double insertRate) {

            this.ndx = ndx;

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
         * @return The commit time of the transaction.
         */
        public Void call() throws Exception {

            final byte[][] keys = new byte[nops][];

            if (r.nextDouble() <= insertRate) {

                /*
                 * Insert
                 */

                // log.info("insert: nops=" + nops);

                final byte[][] vals = new byte[nops][];
                
                for (int i = 0; i < nops; i++) {

                    keys[i] = nextKey();

                    // @todo configure the value size distribution.
                    vals[i] = new byte[5];

                    r.nextBytes(vals[i]);

                }

                ndx.submit(0/* fromIndex */, nops/* toIndex */, keys, vals, //
                        BatchInsertConstructor.RETURN_NO_VALUES, //
                        null// handler
                        );

            } else {

                /*
                 * Remove.
                 */

                // log.info("remove: nops=" + nops);
                for (int i = 0; i < nops; i++) {

                    keys[i] = nextKey();

                }

                ndx.submit(0/* fromIndex */, nops/* toIndex */, keys,
                        null/* vals */,//
                        BatchRemoveConstructor.RETURN_NO_VALUES,//
                        null// handler
                        );

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
    protected ClientTask newClientTask(int clientNum) {

        return new ClientTask(getJobState(), clientNum);

    }
    
}
