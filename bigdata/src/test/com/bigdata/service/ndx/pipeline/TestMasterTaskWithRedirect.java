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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Test ability to handle a redirect (subtask learns that the target service no
 * longer accepts data for some locator and instead must send the data somewhere
 * else).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMasterTaskWithRedirect extends AbstractMasterTestCase {

    public TestMasterTaskWithRedirect() {
    }

    public TestMasterTaskWithRedirect(String name) {
        super(name);
    }

    /**
     * Unit test verifies correct redirect of a write.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteRedirectStop() throws InterruptedException,
            ExecutionException {

        /*
         * Note: The master is overriden so that the 1st chunk written onto
         * locator(13) will cause an StaleLocatorException to be thrown.
         */
        
        final M master = new M(masterStats, masterBuffer, executorService) {

            @Override
            protected S newSubtask(L locator, BlockingBuffer<KVO<O>[]> out) {

                if (locator.locator == 13) {

                    return new S(this, locator, out) {

                        @Override
                        protected boolean handleChunk(final KVO<O>[] chunk)
                                throws Exception {

                            // the write will be redirected into partition#14.
                            redirects.put(13, 14);
                            
                            lock.lockInterruptibly();
                            try {
                            handleRedirect(this, chunk);
                            } finally {
                                lock.unlock();
                            }
                            
                            // stop processing.
                            return false;
                            
                        }

                    };
                    
                }

                return super.newSubtask(locator, out);
                
            }
            
        };
        
        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[] {
                new KVO<O>(new byte[]{1},new byte[]{2},null/*val*/),
                new KVO<O>(new byte[]{13},new byte[]{3},null/*val*/)
        };

        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", a.length, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        assertEquals("elementsOut", a.length, masterStats.elementsOut);
        assertEquals("chunksOut", 2, masterStats.chunksOut);
        assertEquals("partitionCount", 3, masterStats.partitionCount);

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 1, subtaskStats.elementsOut);
            
        }
        
        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(13));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 0, subtaskStats.chunksOut);
            assertEquals("elementsOut", 0, subtaskStats.elementsOut);
            
        }

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(14));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 1, subtaskStats.elementsOut);
            
        }

    }

    /**
     * Unit test verifies correct redirect of a write arising during awaitAll()
     * in the master and occuring after there has already been a write on the
     * partition which is the target of the redirect. This explores the ability
     * of the master to correctly re-open a sink which had been closed.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteRedirectWithReopenStop() throws InterruptedException,
            ExecutionException {

        /*
         * Note: The set of conditions that we want to test here are quite
         * tricky. Therefore I have put a state machine into the fixture for the
         * purposes of this test using a lock and a variety of conditions to
         * ensure that the things happen in the desired sequence.
         */

        final ReentrantLock lck = new ReentrantLock(true/*fair*/);
        
        /*
         * The redirect has to wait until this condition is signalled. It is
         * signaled from within master#awaitAll() once the master has closed the
         * buffers for the existing output sinks.
         */
        final Condition c1 = lck.newCondition();
        /*
         * Set true when L(14) is closed (this is not cleared when it is
         * reopened).
         */
        final AtomicBoolean L14WasClosed = new AtomicBoolean(false);
        
        final M master = new M(masterStats, masterBuffer, executorService) {

            @Override
            protected void removeOutputBuffer(final L locator,
                    final AbstractSubtask sink) throws InterruptedException {

                super.removeOutputBuffer(locator, sink);

                if (locator.locator == 14) {

                    /*
                     * Signal when L(14) is removed. That should happen in
                     * master.awaitAll() when it closes the output buffers for
                     * the existing sinks.
                     */
                    lck.lock();
                    try {
                        if(log.isInfoEnabled())
                            log.info("Signaling now.");
                        c1.signal();
                        L14WasClosed.set(true);
                    } finally {
                        lck.unlock();
                    }

                }

            }
            
            @Override
            protected S newSubtask(L locator, BlockingBuffer<KVO<O>[]> out) {

                if (locator.locator == 13) {

                    /*
                     * The L(13) sink will wait until it is signalled before
                     * issueing an L(13) => L(14) redirect.
                     */
                    
                    return new S(this, locator, out) {

                        @Override
                        protected boolean handleChunk(final KVO<O>[] chunk)
                                throws Exception {

                            lck.lock();
                            try {
                                if (!L14WasClosed.get()) {
                                    // wait up to 1s for the signal and then
                                    // die.
                                    if (!c1.await(1000, TimeUnit.MILLISECONDS))
                                        fail("Not signaled?");
                                }
                            } finally {
                                lck.unlock();
                            }

                            // the write will be redirected into partition#14.
                            redirects.put(13, 14);
                            
                            lock.lockInterruptibly();
                            try {
                                handleRedirect(this, chunk);
                            } finally {
                                lock.unlock();
                            }
                            
                            // stop processing.
                            return false;
                            
                        }

                    };
                    
                }

                return super.newSubtask(locator, out);
                
            }
            
        };
        
        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        // write on L(1) and L(14).
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 2 }, null/* val */),
                    new KVO<O>(new byte[] { 14 }, new byte[] { 3 }, null/* val */) };

            masterBuffer.add(a);
        }

        /*
         * Sleep for a bit so that the first chunk gets pulled out of the
         * master's buffer. This way chunksIn will be reported as (2). Otherwise
         * it is quite likely to be reported as (1) because the asynchronous
         * iterator will generally combine the two input chunks.
         */
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(masterBuffer
                .getChunkTimeout() * 2));
        
        /*
         * Write on L(13). This will be redirected to L(14). The redirect will
         * not be issured until the master has CLOSED the output buffer for
         * L(14). This will force the master to re-open that output buffer.
         */
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 13 }, new byte[] { 3 }, null/* val */) };

            masterBuffer.add(a);
        }
        
        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", 3, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        assertEquals("elementsOut", 3, masterStats.elementsOut);
        assertEquals("chunksOut", 3, masterStats.chunksOut);
        assertEquals("partitionCount", 3, masterStats.partitionCount);

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut);
            assertEquals("elementsOut", 1, subtaskStats.elementsOut);
            
        }
        
        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(13));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 0, subtaskStats.chunksOut);
            assertEquals("elementsOut", 0, subtaskStats.elementsOut);
            
        }

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(14));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 2, subtaskStats.chunksOut);
            assertEquals("elementsOut", 2, subtaskStats.elementsOut);
            
        }

    }

    /**
     * Stress test for redirects.
     * <p>
     * Redirects are stored in a map whose key is effectively the first byte of
     * the {@link KVO} key. This map is pre-populated so that all bytes are
     * mapped randomly assigned to N distinct locators, L(0..N-1). The test
     * writes {@link KVO} tuples on a {@link M master}. The master allocates
     * the tuples to output buffers based on the redirects mapping.
     * <p>
     * The test periodically simulates MOVEs by the atomic update of an entry in
     * the {@link M#redirects} map. Note that we can not simulate SPLIT or JOIN
     * since the indirection is by the first byte from the key rather than a key
     * range (fixed granularity).
     * <p>
     * For simplicity, the keys are N bytes in length and are generated using a
     * uniform distribution. The set of "valid" locators is maintained by the
     * test. The redirects choose a byte at random and redirect it to the next
     * available locator. For example, the first redirect chooses a byte at
     * random in [0:255] and the new target for that locator is L(N), where N is
     * the index of the next locator to be assigned. A thread issues redirects
     * at random intervals.
     * <p>
     * The test ends when either {@link AbstractMasterStats#elementsOut} or
     * {@link AbstractMasterStats#redirectCount} exceeds some threshold or if
     * there is an error.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_redirectStressTest() throws InterruptedException,
            ExecutionException {

        /*
         * Configuration for the stress test.
         */
        
        // #of concurrent producers.
        final int nproducers = 60;

        // #of locators onto which the writes will initially be mapped.
        final int initialLocatorCount = 10;
        
        final long[] redirectDelays = new long[]{
                10, // ms
                100, // ms
                1000, // ms
        };
        
        // maximum delay for writing a chunk (uniform distribution up to this max).
        final long maxWriteDelay = 1000;
        
        // duration of the stress test.
        final long timeout = TimeUnit.SECONDS.toNanos(20/* seconds to run */);

        /*
         * Stress test impl.
         */

        // used to halt the redirecter and the producer(s) when the test is done.
        final AtomicBoolean halt = new AtomicBoolean(false);
        
        /**
         * Writes on a master.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
         * @version $Id$
         */
        class ProducerTask implements Callable<Void> {

            private final BlockingBuffer<KVO<O>[]> buffer;
            
            public ProducerTask(final BlockingBuffer<KVO<O>[]> buffer) {
            
                this.buffer = buffer;
                
            }

            public Void call() throws Exception {

                final KeyBuilder keyBuilder = new KeyBuilder(4);
                
                final Random r = new Random();
                
                final int incRange = 300;

                while (!halt.get()) {

                    final int ntuples = r.nextInt(1000);

                    final KVO<O>[] a = new KVO[ntuples];

                    final int firstKey = r.nextInt();

                    int k = firstKey;

                    for (int i = 0; i < a.length; i++) {

                        final byte[] key = keyBuilder.reset().append(k)
                                .getKey();

                        final byte[] val = new byte[2];

                        r.nextBytes(val);

                        a[i] = new KVO(key, val);

                        k += r.nextInt(incRange);

                    }

                    if (Thread.currentThread().isInterrupted()) {

                        if (log.isInfoEnabled())
                            log.info("Producer interrupted.");
                        
                        return null;
                        
                    }
                    
                    buffer.add(a);
                    
                }
                
                if(log.isInfoEnabled())
                    log.info("Producer halting.");
                
                return null;
                
            }
            
        }

        /**
         * Issues redirects at random intervals of one or more key ranges (based
         * on the first byte) to new locators. The target locators are choosen
         * in a strict sequence.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class RedirectTask implements Callable<Void> {

            private final M master;

            private final long[] times;
            
            // the next locator to be assigned.
            final AtomicInteger nextLocator = new AtomicInteger(0);
            
            final Random r = new Random();

            /**
             * 
             * @param master
             * @param times
             *            The delay times between redirects. The delay until the
             *            next redirect is choosen randomly from among the
             *            specified times.
             */
            public RedirectTask(final M master, final long[] times) {
            
                this.master = master;
                
                this.times = times;
                
            }
            
            public Void call() throws Exception {

                while(!halt.get()) {
                
                    if(Thread.currentThread().isInterrupted()) {
                        
                        if(log.isInfoEnabled())
                            log.info("Redirecter interrupted.");

                        // Done.
                        return null;
                        
                    }
                    
                    final long delayMillis = times[r.nextInt(times.length)];

                    if (log.isInfoEnabled())
                        log.info("Will wait " + delayMillis
                                + "ms for the next redirect");
                    
                    Thread.sleep(delayMillis);
                    
                    if(!halt.get()) {

                        final int n = r.nextInt(10) + 1;

                        final int m = r.nextInt(n) + 1;

                        redirect(n, m);
                        
                    }
                    
                }

                if(log.isInfoEnabled())
                    log.info("Redirecter halting.");
                
                return null;

            }

            /**
             * Redirect one or more key ranges (based on the first byte of the
             * key) to one or more new locators. The locators are assigned in
             * strict sequence.
             * 
             * @param n
             *            The #of key ranges (first bytes) to be redirected.
             * @param m
             *            The #of locators onto which those key ranges will be
             *            redirected (m LTE n).
             */
            protected void redirect(final int n, final int m) {

                assert m <= n : "n=" + n + ", m=" + m;

                if (log.isInfoEnabled())
                    log.info("Redirecting " + n + " key ranges onto " + m
                            + " new locators");
                
                for (int i = 0; i < n; i++) {
                    
                    // random choice of the byte (key-range) to redirect.
                    final int keyRange = r.nextInt(255);
                    
                    // random choice of new locator in [nextLocator:nextLocator+m-1]
                    final int locator = r.nextInt(m) + nextLocator.get();

                    if (log.isInfoEnabled())
                        log.info("Redirecting: keyRange=" + keyRange
                                + " to locator=" + locator);
                    
                    // redirect key-range to locator.
                    master.redirects.put(keyRange, locator);
                    
                }
                
                // increment by the #of locators which were (potentially)
                // assigned.
                nextLocator.addAndGet(m);

            }

            /**
             * Assign each key range (based on the first byte) to a locator. The
             * locators are choosen from [0:n-1]. The {@link #nextLocator} is
             * set as a post-condition to <i>n</i>.
             * 
             * @param n
             *            The #of locators onto which the key ranges will be
             *            mapped.
             */
            protected void init(int n) {

                for (int i = 0; i <= 255; i++) {

                    master.redirects.put(i, r.nextInt(n));

                }

                nextLocator.set(n);

            }
            
        }

        final M master = new M(masterStats, masterBuffer, executorService) {
          
            final private Random r = new Random();
            
            @Override
            protected S newSubtask(L locator, BlockingBuffer<KVO<O>[]> out) {

                return new S(this, locator, out) {

                    /**
                     * Overridden to simulate the latency of the write operation.
                     */
                    @Override
                    protected void writeData(final KVO<O>[] chunk) throws Exception {

                        final long delayMillis = (long) (r.nextDouble() * maxWriteDelay);
                        
                        System.err.println("Writing on " + locator + " (delay="+delayMillis+") ...");
                        
                        Thread.sleep(delayMillis/* ms */);
                        
                        System.err.println("Wrote on " + locator + ".");
                    }

                };
                
            }
            
        };
        
        /*
         * Setup the initial redirects. Each byte is directed to one of the N
         * initially defined locators.
         */
        final RedirectTask redirecter = new RedirectTask(master, redirectDelays);
        
        redirecter.init(initialLocatorCount);
        
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
            boolean done = false;
            final long begin = System.nanoTime();
            while (true) {

                /*
                 * verify no errors.
                 */

                // check master.
                if (masterBuffer.getFuture().isDone()) {
                    // error - will be identifed below.
                    break;
                }

                // check redirecter
                if (redirecterFuture.isDone()) {
                    // error - will be identifed below.
                    break;
                }

                // check producers.
                for (Future f : producerFutures) {
                    if (f.isDone()) {
                        // error - will be identifed below.
                        break;
                    }
                }

                /*
                 * Check termination conditions.
                 */

                final long elapsed = System.nanoTime() - begin;

                if ((timeout - elapsed) <= 0) {

                    if (log.isInfoEnabled())
                        log
                                .info("Ending run: elapsed="
                                        + TimeUnit.NANOSECONDS
                                                .toMillis(elapsed) + "ms");

                    done = true;

                    break;

                }

                // sleep in 1/4 second intervals up to the timeout.
                Thread.sleep(Math.min(TimeUnit.NANOSECONDS.toMillis(timeout
                        - elapsed)/* remaining */, TimeUnit.MILLISECONDS
                        .toNanos(250))/* sleep */
                );

            }

            if (!done) {

                /*
                 * Something did not end normally. We will stop all the tasks and
                 * check their futures and something will throw an exception.
                 */

                log.error("Aborting test.");

            }

            if (log.isInfoEnabled())
                log.info("Halting redirector and producers.");

            // cause the producer and redirecter to halt.
            halt.set(true);

            // await termination and check redirector future for errors.
            redirecterFuture.get();

            // await termination and check producer futures for errors.
            for (Future f : producerFutures) {

                f.get();

            }

            if (log.isInfoEnabled())
                log.info("Closing master buffer.");

            // close the master : queued data should be drained by sinks.
            masterBuffer.close();

            // await termination and check future for errors in master.
            masterBuffer.getFuture().get();

        } finally {

            if(false) {
                // show the redirects using an ordered map.
                final Map<Integer, Integer> redirects = new TreeMap<Integer, Integer>(
                        master.redirects);

                for (Map.Entry<Integer, Integer> e : redirects.entrySet()) {

                    System.out.println("key: " + e.getKey() + " => L("
                            + e.getValue() + ")");

                }

            }
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
