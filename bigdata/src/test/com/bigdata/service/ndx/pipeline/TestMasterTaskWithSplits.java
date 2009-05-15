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
 * Created on May 6, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.openmbean.OpenDataException;

import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Stress test using key-range partitioned index ({@link IMetadataIndex}), which
 * allows us to test the {@link AbstractMasterTask} under split, move, join and
 * other kinds of index partition operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Finish this stress test and enable in {@link TestAll}
 */
public class TestMasterTaskWithSplits extends AbstractKeyRangeMasterTestCase {

    /**
     * 
     */
    public TestMasterTaskWithSplits() {
    }

    /**
     * @param arg0
     */
    public TestMasterTaskWithSplits(String arg0) {
        super(arg0);
    }

    /**
     * Method returns a separator key which lies 1/2 between the given separator
     * keys. This test suite uses long (64 bit) keys. An empty byte[]
     * corresponds to ZERO (0L). A <code>null</code>, which may only appear as
     * the right separator, corresponds to <code>2^64</code>. The math is
     * performed using {@link BigInteger}.
     * 
     * @param leftSeparator
     *            The left separator key. The left-most separator key is always
     *            an empty byte[] (<code>byte[0]</code>).
     * @param rightSeparator
     *            The right separator key. The right-most separator key is
     *            always <code>null</code>.
     * 
     * @return A separator key which lies 1/2-way between the given keys.
     */
    protected byte[] getSeparatorKey(final byte[] leftSeparator,
            final byte[] rightSeparator) {
        
        final BigInteger v1 = decodeKey(leftSeparator);

        final BigInteger v2 = decodeKey(rightSeparator);
        
        final BigInteger vm = v1.add(v2).divide(BigInteger.valueOf(2));
        
        return vm.toByteArray();
        
//      final long leftKey = KeyBuilder.decodeLong(oldLocator
//      .getLeftSeparatorKey(), 0);

//final long leftKey = new BigInteger(oldLocator.getLeftSeparatorKey()).longValue();
//
//final long rightKey = Long.MAX_VALUE;
//
//// divide the range in 1/2.
//final byte[] separatorKey = KeyBuilder
//      .asSortKey((long)(rightKey - leftKey / 2));

    }

    /**
     * <code>2^64</code>
     */
    private final static BigInteger MAX_KEY = BigInteger
            .valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2));
    
    /**
     * Convert an unsigned byte[] into a {@link BigInteger}. A <code>null</code>
     * is understood as <code>2^64</code>. An empty byte[] is understood as a
     * ZERO (0).
     * 
     * @param key
     *            The bytes.
     *            
     * @return The big integer value.
     */
    private BigInteger decodeKey(final byte[] key) {

        if (key == null) {
         
            return MAX_KEY;
            
        }

        if (key.length == 0)
            return BigInteger.ZERO;
        
        return new BigInteger(key);
        
    }

    /**
     * Unit tests to verify the math used to compute the separator keys.
     * 
     * FIXME This test passes because the assertions are correct, but it only
     * lays out some known points and does not go further to verify that a
     * desired translation between signed longs, unsigned byte[] keys, and
     * {@link BigInteger} values is being carried out.
     */
    public void test_decodeKey() {

        // zero
        assertEquals(BigInteger.valueOf(0), decodeKey(new byte[0]));

        assertEquals(BigInteger.valueOf(0), decodeKey(new byte[] { 0 }));

        assertEquals(BigInteger.valueOf(1), decodeKey(new byte[] { 1 }));

        assertEquals(BigInteger.valueOf(-1), decodeKey(new byte[] { -1 }));

        assertEquals(BigInteger.valueOf(Long.MIN_VALUE + 1),
                decodeKey(KeyBuilder.asSortKey(1L)));

        assertEquals(Long.MAX_VALUE, decodeKey(KeyBuilder.asSortKey(-1L))
                .longValue());

        assertEquals(MAX_KEY, decodeKey(null));

    }

    /**
     * FIXME Verify that the separator keys are properly ordered.
     */
    public void test_getSeparatorKey() {

        assertEquals(
                Long.MAX_VALUE,
                decodeKey(
                        getSeparatorKey(new byte[0]/* leftSeparator */, null/* rightSeparator */))
                        .longValue());
        
    }
    
    /**
     * Type-safe enumeration of index partition operations for this test.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static enum OpCode {

        /**
         * Scatter-split an index partition (takes one index partition and
         * produces N index partitions, where N is on the order of 1x to 4x the
         * #of data services in a cluster.
         */
        ScatterSplit,
        /**
         * Split an index partition into two index partitions.
         */
        Split,
        /**
         * Join two index partitions.
         */
        Join,
        /**
         * Move an index partition (changes its locator but does not change its
         * key range).
         */
        Move,
        /**
         * This is not an index partition operation but rather is used to
         * signal the end of the test.
         */
        Done;
        
    };

    /**
     * Class models an operation and the delay until it occurs. A sequence of
     * such operations forms a schedule for the test.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class Op {

        /**
         * The operation.
         */
        public final OpCode code;

        /**
         * The delay until that operation.
         */
        public final long delay;

        /**
         * The unit for that delay.
         */
        public final TimeUnit unit;
        
        public Op(OpCode code, long delay, TimeUnit unit) {
            
            this.code = code;
            
            this.delay = delay;
            
            this.unit = unit;
            
        }
        
    }

    /**
     * Stress test for redirects.
     * <p>
     * Redirects are stored in an {@link IMetadataIndex} so we may test the
     * behavior under SPLITs, MOVEs, or JOINs. The test writes {@link KVO}
     * tuples on a {@link M master}. The master allocates the tuples to output
     * buffers based on the {@link IMetadataIndex} mapping. A single thread
     * executes an {@link Op}[] schedule while N concurrent producer threads
     * write on the master.  The test ends when the schedule is done.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_redirectStressTestWithSplits() throws InterruptedException,
            ExecutionException {

        /*
         * Configuration for the stress test.
         */

        /*
         * #of concurrent producers.
         * 
         * Note: Testing with GTE 150 threads is necessary to model a realistic
         * use cases.
         */
        final int nproducers = 200;

        /*
         * The minimum and maximum delay for writing a chunk. The actual write
         * delays will have a uniform distribution within this range.
         * 
         * Note: These values are based on observed delays for an RDF bulk data
         * load operation on a 16 node cluster.
         */
        final long minWriteDelay = 1000;
        final long maxWriteDelay = 3000;

        /*
         * The minimum and maximum delay for producing a new chunk. The actual
         * delays will have a uniform distribution within this range.
         * 
         * Note: These values are SWAGs.
         */
        final long minProducerDelay = 150;
        final long maxProducerDelay = 750;

        /*
         * The size of a chunk generated by a producer (10k is typical of a
         * deployed system).
         */
        final int producerChunkSize = 1000; // vs 10000

        /*
         * Note: We should be able to use smaller chunks on the master and
         * larger chunks on the client. The semantics of the chunk size are such
         * that it controls the size of the chunks READ from the buffer, not the
         * size of the chunks on the buffer. Therefore a 10k chunk producer
         * feeding a master will cause 10k chunks to appear on the master. If
         * the master has a 10k chunk size, then it will never need to combine
         * chunks for its consumer (the sink). So the sink gets 10k chunks in
         * its buffer. However, the chunkSize of the sink controls how large the
         * writes will be on the index partition. So a 20k sink chunk size will
         * cause 2 x 10k chunks to be combined and merge sorted before it writes
         * on the index partition.
         * 
         * @todo update the bigdataCluster.config appropriately.
         * 
         * @todo when I made the subtaskChunkSize large enough that the code was
         * actually combining chunks it uncovered a problem with the
         * asynchronous writes which do not inherently protect against the
         * presence of duplicate keys in the KVO[] stream. This was frowned upon
         * for synchronous RPC, but for asynchronous writes it makes more sense
         * to permit duplicates while still restricting the producers to
         * generate ordered data. Therefore I am writing a series of unit tests
         * for the ISplitter and then I will allow this case to be valid.
         */
        final int masterQueueCapacity = 10;// vs 1000 (cluster config value).
        final int masterChunkSize = 10000;
        final long masterChunkTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(50);

        final int subtaskQueueCapacity = 50; // vs 500 (cluster config value).
        final int subtaskChunkSize = 20000;
        final long subtaskChunkTimeoutNanos = TimeUnit.MILLISECONDS
                .toNanos(Long.MAX_VALUE);

        /*
         * The idle timeout for the sink (generally infinite unless using a
         * KVOLatch to coordinate notification of results such as for the
         * TERM2ID index).
         */
        final long sinkIdleTimeout = Long.MAX_VALUE;

        final long sinkPollTimeout = TimeUnit.MILLISECONDS.toNanos(50);
        
        // The #of data services.
        final int ndataServices = 40;
        {
            
            // Setup the mock data services.
            for (int i = 0; i < ndataServices; i++) {

                final UUID uuid = UUID.randomUUID();

                dataServices.put(uuid, new DS(uuid) {

                    private final Random r = new Random();
                    
                    /**
                     * Overridden to simulate the latency of the write operation.
                     */
                    @Override
                    protected void acceptWrite(final L locator, final KVO<O>[] chunk) {

                        final long delayMillis = (long) (r.nextDouble() * (maxWriteDelay - minWriteDelay))
                                + minWriteDelay;

                        System.err.println("Writing on " + chunk.length
                                + " elements on " + locator + " (delay="
                                + delayMillis + ") ...");

                        try {
                            Thread.sleep(delayMillis/* ms */);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }

                        System.err.println("Wrote on " + this + ".");

                    }
                    
                });
                
            }
            
        }

        final TimeUnit scheduleUnit = TimeUnit.SECONDS;// seconds or minutes.
        final Op[] schedule = new Op[] {
          
                // @todo include move & join as well.
//                new Op(OpCode.ScatterSplit, 5, scheduleUnit), // FIXME test scatter split.
//                new Op(OpCode.Split, 1, scheduleUnit),
//                new Op(OpCode.Split, 1, scheduleUnit),
//                new Op(OpCode.Split, 1, scheduleUnit),
                /*
                 * Note: Always include this as the last operation or the test
                 * WILL NOT terminate!
                 */
                new Op(OpCode.Done, 10, scheduleUnit)
                
        };
        
        // duration of the stress test.
//        final long timeoutMillis;
        {
            assert schedule[schedule.length - 1].code == OpCode.Done;
            long t = 0L;
            for (Op op : schedule) {

                t += op.unit.toMillis(op.delay);

            }
//            timeoutMillis = t;
            if (log.isInfoEnabled())
                log.info("Test will run for " + t + "ms");
        }

        /*
         * Stress test impl.
         */

        // used to halt the producer(s) when the test is done.
        final AtomicBoolean halt = new AtomicBoolean(false);
        
        // the #of producers that are currently running.
        final AtomicInteger producerCount = new AtomicInteger(0);

        /**
         * Writes on a master.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class ProducerTask implements Callable<Void> {

            private final BlockingBuffer<KVO<O>[]> buffer;

            public ProducerTask(final BlockingBuffer<KVO<O>[]> buffer) {

                this.buffer = buffer;

            }

            public Void call() throws Exception {

                producerCount.incrementAndGet();

                try {

                    final KeyBuilder keyBuilder = new KeyBuilder(
                            Bytes.SIZEOF_LONG);

                    final Random r = new Random();

                    while (true) {

                        // Sleep to simulate latency in the production of new
                        // chunks.
                        Thread
                                .sleep(r
                                        .nextInt((int) (maxProducerDelay - minProducerDelay))
                                        + minProducerDelay);

                        if (halt.get()
                                || Thread.currentThread().isInterrupted()) {

                            if (log.isInfoEnabled())
                                log.info("Producer halting.");

                            return null;

                        }

                        /*
                         * Note: keys have uniform distribution.
                         */
                        final KVO<O>[] a = new KVO[producerChunkSize];

                        for (int i = 0; i < a.length; i++) {

                            final byte[] key = keyBuilder.reset().append(
                                    r.nextLong()).getKey();

                            final byte[] val = new byte[2];

                            r.nextBytes(val);

                            a[i] = new KVO(key, val);

                        }

                        // ensure sorted order for the chunk.
                        Arrays.sort(a);

                        buffer.add(a);

                    }

                } finally {

                    producerCount.decrementAndGet();

                }
                
            }
            
        }

        /**
         * Issues redirects.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class RedirectTask implements Callable<Void> {

            private final M master;

            private final Op[] schedule;
            
            final Random r = new Random();

            /**
             * 
             * @param master
             * @param times
             *            The delay times between redirects. The delay until the
             *            next redirect is chosen randomly from among the
             *            specified times.
             */
            public RedirectTask(final M master, final Op[] schedule) {
            
                this.master = master;
                
                this.schedule = schedule;
                
            }
            
            public Void call() throws Exception {

                for (Op op : schedule) {

                    if (halt.get() || Thread.currentThread().isInterrupted()) {

                        if (log.isInfoEnabled())
                            log.info("Redirecter halting.");

                        // Done.
                        return null;

                    }

                    final long delayMillis = op.unit.toMillis(op.delay);

                    if (log.isInfoEnabled())
                        log.info("Will wait " + delayMillis
                                + "ms before executing: " + op.code);

                    Thread.sleep(delayMillis);

                    if (halt.get() || Thread.currentThread().isInterrupted()) {

                        if (log.isInfoEnabled())
                            log.info("Redirecter halting.");

                        // Done.
                        return null;

                    }

                    switch (op.code) {
                    case ScatterSplit:
                        scatterSplit(op);
                        break;
                    case Split:
                        split(op);
                        break;
                    case Join:
                        join(op);
                        break;
                    case Move:
                        move(op);
                        break;
                    case Done:
                        done(op);
                        break;
                    default:
                        throw new UnsupportedOperationException(op.code
                                .toString());
                    }

                }

                if(log.isInfoEnabled())
                    log.info("Redirecter halting.");
                
                return null;

            }

            /**
             * Handles scatterSplit. One index partition is selected. It is then
             * scattered by dividing its key range into 2N equal parts, where N
             * is the #of of data services. The locators for the index partition
             * in the metadata index are updated to reflect the scatter split.
             * 
             * @param op
             */
            protected void scatterSplit(Op op) {
                master.mdiLock.lock();
                try {
                    
                    // the #of existing partitions.
                    final int npartitions = master.mdi.getEntryCount();
                    
                    assert npartitions != 0;
                    
                    // choose which one to split.
                    final int index = r.nextInt(npartitions);
                    
                    // lookup that locator.
                    final L locator = (L) master.mdi.valueAt(index,
                            master.mdi.getLookupTuple()).getObject();

                    /*
                     * Evenly divide the key range of the locator into N key
                     * ranges. If the rightSeparator is null, then we divide the
                     * keys based on the a-priori knowledge that the keys are
                     * 8-bytes long so the maximum key is formed by encoding
                     * Long.MAX_VALUE using a KeyBuilder. However, when the
                     * rightSeparator is null on input, the rightSeparator of
                     * the last output index partition will always be null as
                     * well.
                     */
                    
                    // FIXME Finish scatter-split
                    if(true)
                        throw new UnsupportedOperationException();
                    
                    // Notify DS so it will issue stale locator response.
                    final DS oldDS = dataServices.get(locator.getDataServiceUUID());
                    oldDS.notifyGone(locator);
                    
                } finally {
                    master.mdiLock.unlock();
                }
            }

            /**
             * Handle split of a randomly chosen index partition into two new
             * index partitions.
             * 
             * @param op
             */
            protected void split(final Op op) {
                master.mdiLock.lock();
                try {
                    
                    // the #of existing partitions.
                    final int npartitions = master.mdi.getEntryCount();
                    
                    assert npartitions != 0;
                    
                    // choose which one to split.
                    final int index = r.nextInt(npartitions);
                    
                    // lookup that locator.
                    final L oldLocator = (L) master.mdi.valueAt(index,
                            master.mdi.getLookupTuple()).getObject();

                    /*
                     * Divide the key range of the locator into 2 key ranges. If
                     * the rightSeparator is null, then we divide the keys based
                     * on the a-priori knowledge that the keys are 8-bytes long
                     * so the maximum key is formed by encoding Long.MAX_VALUE
                     * using a KeyBuilder. However, when the rightSeparator is
                     * null on input, the rightSeparator of the last output
                     * index partition will always be null as well.
                     */

                    final byte[] separatorKey = getSeparatorKey(oldLocator
                            .getLeftSeparatorKey(), oldLocator
                            .getRightSeparatorKey());

                    final L newLeftSibling = new L(master.mdi
                            .incrementAndGetNextPartitionId(),
                            getRandomDataService().uuid, oldLocator
                                    .getLeftSeparatorKey(), separatorKey);

                    final L newRightSibling = new L(master.mdi
                            .incrementAndGetNextPartitionId(),
                            getRandomDataService().uuid, separatorKey,
                            oldLocator.getRightSeparatorKey());

                    // remove old locator.
                    assertNotNull(master.mdi.remove(oldLocator
                            .getLeftSeparatorKey()));

                    // add new locators covering the same key-range.
                    master.mdi.insert(newLeftSibling.getLeftSeparatorKey(),
                            newLeftSibling);
                    master.mdi.insert(newRightSibling.getLeftSeparatorKey(),
                            newRightSibling);

                    // Notify DS so it will issue stale locator response.
                    final DS oldDS = dataServices.get(oldLocator
                            .getDataServiceUUID());

                    oldDS.notifyGone(oldLocator);
                    
                } finally {
                    master.mdiLock.unlock();
                }
            }

            // FIXME handle join
            protected void join(Op op) {
                throw new UnsupportedOperationException();
            }

            // FIXME handle move
            protected void move(Op op) {
                throw new UnsupportedOperationException();
            }

            /**
             * Cause the test to halt.
             * @param op
             */
            protected void done(Op op) {
                
                // set flag - will cause producers and redirector to halt.
                halt.set(true);
                
            }
            
        }

        /*
         * The master under test.
         */
        final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
                masterQueueCapacity, masterChunkSize, masterChunkTimeoutNanos,
                TimeUnit.NANOSECONDS);

        final M master = new M(masterStats, masterBuffer, executorService,
                sinkIdleTimeout, sinkPollTimeout) {
            
            protected BlockingBuffer<KVO<O>[]> newSubtaskBuffer() {

                return new BlockingBuffer<KVO<O>[]>(
                        new ArrayBlockingQueue<KVO<O>[]>(subtaskQueueCapacity), //
                        subtaskChunkSize,// 
                        subtaskChunkTimeoutNanos,//
                        TimeUnit.NANOSECONDS,//
                        true // ordered
                );

            }

        };
        
        /*
         * Setup the initial index partition.
         * 
         * Note: The mdiLock is not required here since no other threads are
         * accessing the MDI until we start them below.
         */
        master.mdiLock.lock();
        try {
            
            // choose initial data service randomly.
            final UUID dataServiceUUID = dataServices.keySet().iterator().next();
            
            final DS dataService = dataServices.get(dataServiceUUID);
            
            final L locator = new L(//
                    // the initial partitionId
                    master.mdi.incrementAndGetNextPartitionId(),
                    // the initial data service.
                    dataServiceUUID,
                    // leftSeparator is initially an empty byte[].
                    new byte[0],
                    // rightSeparator is initially null.
                    null
            );

            // add to the MDI
            master.mdi.insert(locator.getLeftSeparatorKey(), locator);

            // and inform the DS.
            dataService.notifyLocator(locator);
            
        } finally {
            
            master.mdiLock.unlock();
            
        }
        
        /*
         * Setup redirector with its schedule of operations.
         */
        final RedirectTask redirecter = new RedirectTask(master, schedule);
        
        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        // start writing data.
        final List<Future> producerFutures = new LinkedList<Future>();
        for (int i = 0; i < nproducers; i++) {

            producerFutures.add(executorService.submit(new ProducerTask(
                    masterBuffer)));
            
        }
        
        // start redirects.
        final Future redirecterFuture = executorService.submit(redirecter);  

        try {

            // periodically verify no errors in running tasks.
            while (!halt.get()) {

                /*
                 * End the test if anything is done.
                 */

                // check master.
                if (masterBuffer.getFuture().isDone()) {
                    break;
                }

                // check redirecter
                if (redirecterFuture.isDone()) {
                    break;
                }

                // check producers.
                for (Future f : producerFutures) {
                    if (f.isDone()) {
                        break;
                    }
                }

                // sleep in 1/4 second intervals.
                Thread.sleep(250/*ms*/);

            }

            /*
             * Set [halt] (it may already be set) so that the redirector and the
             * producers will all halt and their check their Futures for errors.
             */
            if (log.isInfoEnabled())
                log.info("Halting redirector and producers.");

            // set flag causing tasks to halt.
            halt.set(true);

            // await termination and check redirector future for errors.
            redirecterFuture.get();

            // await termination and check producer futures for errors.
            for (Future f : producerFutures) {

                f.get();

            }

            if (log.isInfoEnabled())
                log.info("Closing master buffer: " + masterBuffer);

            // close the master : queued data should be drained by sinks.
            masterBuffer.close();

            // await termination and check future for errors in master.
            while (true) {
                try {
                    masterBuffer.getFuture().get(1000, TimeUnit.MILLISECONDS);
                    break;
                } catch (TimeoutException e) {
                    if (log.isInfoEnabled())
                        log
                                .info("Waiting on master: ~subtaskCount="
                                        + (masterStats.subtaskStartCount - masterStats.subtaskEndCount)
                                        + ", ~elementsRemaining="
                                        + (masterStats.elementsIn - masterStats.elementsOut));
                }
            }

        } finally {

            {
                // show the subtask stats using an ordered map.
                final Map<L, HS> subStats = new TreeMap<L, HS>(master.stats
                        .getSubtaskStats());

                for (Map.Entry<L, HS> e : subStats.entrySet()) {

                    System.out.println(e.getKey() + " : " + e.getValue());

                }

            }

            // show the master stats
            System.out.println(master.stats.toString());

        }

    }

}
