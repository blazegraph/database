/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Oct 9, 2007
 */

package com.bigdata.journal;

import java.io.Writer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.util.ConcurrentHashSet;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.testutil.ExperimentDriver.Result;

/**
 * Stress tests for concurrent processing of operations on named unisolated
 * indices where the concurrency is managed by an
 * {@link UnisolatedReadWriteIndex} rather than the {@link ConcurrencyManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see UnisolatedReadWriteIndex
 */
public class StressTestUnisolatedReadWriteIndex extends ProxyTestCase<Journal> {

    public StressTestUnisolatedReadWriteIndex() {
    }

    public StressTestUnisolatedReadWriteIndex(final String name) {

        super(name);
        
    }

    private Journal journal;

    public void setUpComparisonTest(final Properties properties)
            throws Exception {

        journal = new Journal(properties);

    }

    public void tearDownComparisonTest() throws Exception {

        if (journal != null) {
            
            if (journal.isOpen()) {

                journal.shutdownNow();

            }
            
            journal.deleteResources();
            
        }

        // release reference.
        journal = null;
        
    }
    
    /**
     * A stress test with a small pool of concurrent clients.
     */
    public void test_concurrentClients() throws InterruptedException {

        final Properties properties = getProperties();
        
        final Journal journal = new Journal(properties);

        try {

//        if(journal.getBufferStrategy() instanceof MappedBufferStrategy) {
//            
//            /*
//             * @todo the mapped buffer strategy has become cpu bound w/o
//             * termination when used with concurrent clients - this needs to be
//             * looked into further.
//             */
//            
//            fail("Mapped buffer strategy may have problem with tx concurrency");
//            
//        }

            /*
             * Note: Using a timeout will cause any tasks still running when the
             * timeout expires to be interrupted. The code is clearly stable
             * when the timeout is Long.MAX_VALUE, even with the presence of a
             * number of spurious extensions from the failureRate. However,
             * there are clearly problems which emerge when the timeout is less
             * than the time required to complete the scheduled tasks. A variety
             * of errors can emerged when the scheduled tasks are all
             * cancelled. It is difficult to say whether any of those problems
             * could be observed by an application outside of a shutdownNow()
             * scenario.
             * 
             * TODO We could write this using a task queue feeding an executor
             * pool so we could make it into a longer running test. If I simply
             * increase the number of trials, it attempts to schedule them all
             * concurrently and hits an out of memory error (too many native threads).
             */
            doConcurrentClientTest(journal,//
                Long.MAX_VALUE,// timeout : MUST BE INFINITE OR WILL HIT FALSE ERRORS.
                3, // 3,// nresources // 20
                1, // minLocks
                2, // 5 // maxLocks // 3
                500, // ntrials // Note: fails in CI @ 1000 (java.lang.OutOfMemoryError: unable to create new native thread)
                3, // keyLen
                1000, // nops
                0.02d,// failureRate
                0.10d // commitRate
        );
        
        } finally {

            journal.destroy();
            
        }
        
    }

    /**
     * A stress test of concurrent writers on one or more named indices.
     * 
     * @param journal
     *            The database.
     * 
     * @param timeout
     *            The #of seconds before the test will terminate.
     * 
     * @param nresources
     *            The #of named indices that will be used by the tasks.
     * 
     * @param minLocks
     *            The minimum #of resources in which a writer will obtain a lock
     *            in [<i>0</i>:<i>nresources</i>].
     * 
     * @param maxLocks
     *            The maximum #of resources in which a writer will obtain a lock
     *            in [<i>minLocks</i>:<i>nresources</i>].
     * 
     * @param ntrials
     *            The #of transactions to execute.
     * 
     * @param keyLen
     *            The length of the random unsigned byte[] keys used in the
     *            operations. The longer the keys the less likely it is that
     *            there will be a write-write conflict (that concurrent txs will
     *            write on the same key).
     * 
     * @param nops
     *            The #of operations to be performed in each transaction.
     * 
     * @param failureRate
     *            The percentage of {@link Writer}s that will throw a
     *            {@link SpuriousException} rather than completing normally.
     * 
     * @todo factor out the operation to be run as a test parameter?
     */
    static public Result doConcurrentClientTest(final Journal journal,
            final long timeout, final int nresources, final int minLocks,
            final int maxLocks, final int ntrials, final int keyLen,
            final int nops, final double failureRate, final double commitRate)
            throws InterruptedException {

        if (journal == null)
            throw new IllegalArgumentException();

        if (timeout <= 0)
            throw new IllegalArgumentException();

        if (nresources <= 0)
            throw new IllegalArgumentException();

        if (minLocks < 0)
            throw new IllegalArgumentException();

        if (maxLocks < minLocks || maxLocks > nresources)
            throw new IllegalArgumentException();

        if (ntrials < 1)
            throw new IllegalArgumentException();

        if (keyLen < 1)
            throw new IllegalArgumentException();

        if (nops < 0)
            throw new IllegalArgumentException();

        if (failureRate < 0.0 || failureRate > 1.0)
            throw new IllegalArgumentException();
        
        if (commitRate < 0.0 || commitRate > 1.0)
            throw new IllegalArgumentException();
        
        final Random r = new Random();

        /*
         * Setup the named resources/indices.
         */
        final String[] resources = new String[nresources];
        {

            for (int i = 0; i < nresources; i++) {

                resources[i] = "index#" + i;

                journal.registerIndex(resources[i], BTree.create(journal,
                        new IndexMetadata(resources[i], UUID.randomUUID())));
                
            }

            journal.commit();

        }

        if (log.isInfoEnabled())
            log.info("Created indices: " + Arrays.toString(resources));

        /*
         * Setup the tasks that we will submit.
         */

        final Collection<Callable<Void>> tasks = new HashSet<Callable<Void>>();

        final ConcurrentHashSet<Thread> threads = new ConcurrentHashSet<Thread>();

        for (int i = 0; i < ntrials; i++) {

            // choose nlocks and indices to use.
            
            final int nlocks = r.nextInt(maxLocks + 1 - minLocks) + minLocks;

            assert nlocks >= minLocks && nlocks <= maxLocks;

            final Collection<String> tmp = new HashSet<String>(nlocks);

            while (tmp.size() < nlocks) {

                tmp.add(resources[r.nextInt(nresources)]);

            }

            final String[] resource = tmp.toArray(new String[nlocks]);

            tasks.add(new WriteTask(journal, resource, i, keyLen, nops,
                    failureRate, commitRate, threads));

        }

        /*
         * Run all tasks and wait for up to the timeout for them to complete.
         */

        if (log.isInfoEnabled())
            log.info("Submitting " + tasks.size() + " tasks");

        final long begin = System.currentTimeMillis();

        final List<Future<Void>> results = journal.getExecutorService().invokeAll(
                tasks, timeout, TimeUnit.SECONDS);

        final long elapsed = System.currentTimeMillis() - begin;

        /*
         * Examine the futures to see how things went.
         */
        final Iterator<Future<Void>> itr = results.iterator();
        
        int nfailed = 0; // #of tasks that failed.
//        int nretry = 0; // #of tasks that threw RetryException
        int ninterrupt = 0; // #of interrupted tasks.
        int ncompleted = 0; // #of tasks that successfully completed.
        int nuncompleted = 0; // #of tasks that did not complete in time.

        while (itr.hasNext()) {

            final Future<Void> future = itr.next();

            if (future.isCancelled()) {

                nuncompleted++;

                continue;

            }

            try {

                future.get();
                
                ncompleted++;
                
            } catch(ExecutionException ex ) {

				if (isInnerCause(ex, InterruptedException.class)
						|| isInnerCause(ex, ClosedByInterruptException.class)) {

                    /*
                     * Note: Tasks will be interrupted if a timeout occurs when
                     * attempting to run the submitted tasks - this is normal.
                     */

                    log.warn("Interrupted: " + ex);

                    ninterrupt++;
                    
                } else if(isInnerCause(ex, SpuriousException.class)) {
                    
                    nfailed++;
                    
//                } else if(isInnerCause(ex, RetryException.class)) {
//                    
//                    nretry++;
                    
                } else {
                
                    // Other kinds of exceptions are errors.

                    fail("Not expecting: " + ex, ex);

                }
                
            }
            
        }

        final WriteExecutorService writeService = journal.getConcurrencyManager().getWriteService();
        
        journal.shutdownNow();
        
        /*
         * Compute bytes written per second.
         */
        
        final long seconds = TimeUnit.SECONDS.convert(elapsed,
                TimeUnit.MILLISECONDS);

        final long bytesWrittenPerSecond = journal.getRootBlockView()
                .getNextOffset()
                / (seconds == 0 ? 1 : seconds);

        final Result ret = new Result();

        // these are the results.
        ret.put("nfailed",""+nfailed);
//        ret.put("nretry",""+nretry);
        ret.put("ncompleted",""+ncompleted);
        ret.put("ninterrupt",""+ninterrupt);
        ret.put("nuncompleted", ""+nuncompleted);
        ret.put("elapsed(ms)", ""+elapsed);
        ret.put("bytesWrittenPerSec", ""+bytesWrittenPerSecond);
        ret.put("tasks/sec", ""+(ncompleted * 1000 / elapsed));
        ret.put("maxRunning", ""+writeService.getMaxRunning());
        ret.put("maxPoolSize", ""+writeService.getMaxPoolSize());
        ret.put("maxLatencyUntilCommit", ""+writeService.getMaxCommitWaitingTime());
        ret.put("maxCommitLatency", ""+writeService.getMaxCommitServiceTime());

        System.err.println(ret.toString(true/*newline*/));
        
        journal.deleteResources();

        return ret;
       
    }
    
    static private final Random r = new Random();
    
    /**
     * A task that writes on named unisolated index(s).
     */
    public static class WriteTask implements Callable<Void> {

        private final Journal journal;
        private final String[] resource;
        private final int trial;
        private final int keyLen;
        private final int nops;
        private final double failureRate;
        private final double commitRate;
        private final ConcurrentHashSet<Thread> threads;
        
        public WriteTask(final Journal journal,
                final String[] resource, final int trial, final int keyLen,
                final int nops, final double failureRate, final double commitRate,
                final ConcurrentHashSet<Thread> threads) {

            this.journal = journal;
            
            this.resource = resource;

            this.trial = trial;
            
            this.keyLen = keyLen;
            
            this.nops = nops;
            
            this.failureRate = failureRate;
            
            this.commitRate = commitRate;
            
            this.threads = threads;
            
        }

        @Override
        public String toString() {

            return getClass().getName() + "#" + trial;

        }
        
        /**
         * Executes random operation on a named unisolated index.
         * 
         * @return null
         */
        @Override
        public Void call() throws Exception {

            final UnisolatedReadWriteIndex[] indices = new UnisolatedReadWriteIndex[resource.length];

            final Thread t = Thread.currentThread();

            // a thread that is currently executing for some writer.
            threads.add(t);

            try {

                /*
                 * Get the index objects. This will create the ReadWriteLock if
                 * it does not exist, but it will not acquire either the
                 * ReadLock or the WriteLock.
                 */
                for (int i = 0; i < resource.length; i++) {

                    final String name = resource[i];

                    final BTree btree = journal.getIndex(name);

                    indices[i] = new UnisolatedReadWriteIndex(btree);

                }

                /*
                 * Random write operations on the named index(s).
                 */
                for (int i = 0; i < nops; i++) {

                    final IIndex ndx = indices[i % resource.length];

                    final byte[] key = new byte[keyLen];

                    r.nextBytes(key);

                    if (r.nextInt(100) > 10) {

                        final byte[] val = new byte[5];

                        r.nextBytes(val);

                        ndx.insert(key, val);

                    } else {

                        ndx.remove(key);

                    } 
                    /**
                     * FIXME Add a probability of a read-only operation, e.g.,
                     * lookup() or rangeIterator(key,val). The latter can also
                     * do chunked resolution. This will provide test coverage
                     * for the case where the close() of the iterator interrupts
                     * the producer. This happens especially in the case where a
                     * range iterator is used and the iterator is closed after
                     * the first result since the producer could still be
                     * running. If it is in the middle of evicting a dirty node
                     * from the write retention queue then that can leave the
                     * nodes and the queue in an inconsistent state. (We might
                     * need to write that test at the SAIL level since it is the
                     * AbstractedChunkedResolverator pattern for which we have
                     * observed the issue.  Perhaps through a modification of the
                     * csem stress test.)
                     * 
                     * @see <a href="http://trac.blazegraph.com/ticket/855">
                     *      AssertionError: Child does not have persistent
                     *      identity </a>
                     */

                } // for( i : nops )

                if (r.nextDouble() < failureRate) {

                    throw new SpuriousException();

                } else if (r.nextDouble() < commitRate) {

                    /*
                     * Checkpoint the indices.
                     * 
                     * Note: writeCheckpoint() will also acquire the writeLock.
                     * Thus, it contends with the other operations on the
                     * UnisolatedReadWriteIndex.
                     */
                    
                    for (String name : resource) {

                        final BTree btree = journal.getIndex(name);
                        
                        btree.writeCheckpoint();
                    
                    }

                    /*
                     * Note: This can not be safely done since we do not have
                     * any protocol to either (a) ensure that the Thread is
                     * executing in a WriteTask by the time we do
                     * Thread.interrupt() for that thread; (b) ensure that the
                     * B+Tree is reloaded from its last checkpoint after an
                     * interrupt and before the next operation on that BTree.
                     */
//                } else if (r.nextDouble() < .10d) {
//
//                    final Thread[] a = threads.toArray(new Thread[]{});
//                    
//                    final int i = r.nextInt(a.length);
//                    
//                    final Thread aThread = a[i];
//                    
//                    log.warn("Interrupting some thread");
//                    
//                    aThread.interrupt();
                    
                }

                return null;

            } finally {

                // This thread is no longer executing.
                threads.remove(t);

            }

        }
        
    } // class WriteTask
    
    /**
     * Thrown by a {@link Writer} if it is selected for abort based on the
     * {@link TestOptions#FAILURE_RATE}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    private static class SpuriousException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = 5032559382234334218L;
        
    }

    /**
     * Additional properties understood by this test.
     */
    public static interface TestOptions extends ConcurrencyManager.Options {

        /**
         * The timeout for the test (seconds).
         */
        public static final String TIMEOUT = "timeout";
        
        /**
         * The #of named resources from which {@link Writer}s may choosen the
         * indices on which they will write.
         */
        public static final String NRESOURCES = "nresources";

        /**
         * The minimum #of locks that a writer will obtain (0 or more, but a
         * writer with zero locks will not write on anything).
         */
        public static final String MIN_LOCKS = "minLocks";

        /**
         * The maximum #of locks that a writer will obtain (LTE
         * {@link #NRESOURCES}). A writer will write on each resource that it
         * locks.
         */
        public static final String MAX_LOCKS = "maxLocks";

        /**
         * The #of trials (aka transactions) to run.
         */
        public static final String NTRIALS = "ntrials";
        
        /**
         * The length of the keys used in the test. This directly impacts the
         * likelyhood of a write-write conflict. Shorter keys mean more
         * conflicts. However, note that conflicts are only possible when there
         * are at least two concurrent clients running.
         */
        public static final String KEYLEN = "keyLen";
        
        /**
         * The #of operations in each trial.
         */
        public static final String NOPS = "nops";

        /**
         * The failure rate [0.0:1.0]. A {@link Writer} aborts by throwing a
         * {@link SpuriousException}.
         */
        public static final String FAILURE_RATE = "failureRate";
        
    }

}
