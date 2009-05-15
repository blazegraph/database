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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.keys.KVO;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Unit tests of the idle timeout behavior for {@link AbstractMasterTask} and
 * friends.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMasterTaskIdleTimeout extends AbstractMasterTestCase {

    public TestMasterTaskIdleTimeout() {
    }

    public TestMasterTaskIdleTimeout(String name) {
        super(name);
    }
    
    /**
     * Unit test to verify that output buffers are not closed too quickly. This
     * also verifies that a buffer automatically re-opened if it was closed by a
     * timeout.
     */
    public void test_idleTimeout() throws InterruptedException,
            ExecutionException {

        final M master = new M(masterStats, masterBuffer, executorService,
                TimeUnit.MILLISECONDS.toNanos(2000)/* sinkIdleTimeout */,
                M.DEFAULT_SINK_POLL_TIMEOUT) {
          
            protected BlockingBuffer<KVO<O>[]> newSubtaskBuffer() {

                return new BlockingBuffer<KVO<O>[]>(
                        new ArrayBlockingQueue<KVO<O>[]>(subtaskQueueCapacity), //
                        10,// chunkSize
                        20,// chunkTimeout
                        TimeUnit.MILLISECONDS,// chunkTimeoutUnit
                        true // ordered
                );

            }

        };

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        /*
         * write a chunk on the buffer. this will cause an output buffer to be
         * created.
         */
        final long beforeWrite = System.nanoTime();
        {
            final KVO<O>[] a = new KVO[] { //
                    new KVO<O>(new byte[] { 1 }, new byte[] { 2 }, null/* val */), };

            masterBuffer.add(a);

        }

        // wait just a bit for that chunk to be output by the sink.
        awaitChunksOut(master, 1, 100/* MUST be GT sink's chunkTimeout */,
                TimeUnit.MILLISECONDS);

        // verify chunk was output by the sink.
        assertEquals("elementsIn", 1, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        assertEquals("elementsOut", 1, masterStats.elementsOut);
        assertEquals("chunksOut", 1, masterStats.chunksOut);
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());
        
        // make sure that the subtask is still running.
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 0, masterStats.subtaskEndCount);

        final long elapsed1 = System.nanoTime() - beforeWrite;
        
        /*
         * Sleep for up to 1/2 of the remaining idle timeout.
         * 
         * Note: If the idle timeout is too short then Thread#sleep() will not
         * return before the timeout has expired.
         */
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(master.sinkIdleTimeoutNanos
                / 2 - elapsed1));

        final long elapsed2 = System.nanoTime() - beforeWrite;

        if (elapsed2 > master.sinkIdleTimeoutNanos) {

            fail("Sleep too long - idle timeout may have expired.");

        }

        // the original subtask should still be running.
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 0, masterStats.subtaskEndCount);

        // now sleep for the entire idle timeout (this give us some padding).
        Thread
                .sleep(TimeUnit.NANOSECONDS
                        .toMillis(master.sinkIdleTimeoutNanos));

        // the subtask should have terminated and no other subtask should have started.
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 1, masterStats.subtaskEndCount);
        assertEquals("subtaskIdleTimeout", 1, masterStats.subtaskIdleTimeout);

        /*
         * write another chunk onto the same output buffer.
         */
        {
            final KVO<O>[] a = new KVO[] { //
                    new KVO<O>(new byte[] { 1 }, new byte[] { 3 }, null/* val */), };

            masterBuffer.add(a);

        }
        
        // close the master
        masterBuffer.close();

        // and await its future.
        masterBuffer.getFuture().get();

        // verify 2nd chunk was output by the sink.
        assertEquals("elementsIn", 2, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        assertEquals("elementsOut", 2, masterStats.elementsOut);
        assertEquals("chunksOut", 2, masterStats.chunksOut);
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());

        // verify that another subtask was started by the 2nd write.
        assertEquals("subtaskStartCount", 2, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 2, masterStats.subtaskEndCount);

        /*
         * The idle timeout count should still be one since the 2nd instance of
         * the sink should have been closed when the master was exhausted not by
         * an idle timeout.
         */
        assertEquals("subtaskIdleTimeout", 1, masterStats.subtaskIdleTimeout);

        // verify writes on each expected partition.
        {

            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);

            assertEquals("chunksOut", 2, subtaskStats.chunksOut);
            assertEquals("elementsOut", 2, subtaskStats.elementsOut);

        }

    }

    /**
     * Unit test verifies that an idle timeout may be LT the chunk timeout and
     * that the sink will be closed by the idle timeout if chunks do not appear
     * in a timely manner.

     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_idleTimeout_LT_chunkTimeout() throws InterruptedException, ExecutionException {

        final long sinkIdleTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(50);

        final long sinkChunkTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(100);
        
        // make sure there is enough room for the test to succeed.
        assertTrue(sinkIdleTimeoutNanos * 2 >= sinkChunkTimeoutNanos);
        
        final M master = new M(masterStats, masterBuffer, executorService,
                sinkIdleTimeoutNanos,
                M.DEFAULT_SINK_POLL_TIMEOUT) {
          
            protected BlockingBuffer<KVO<O>[]> newSubtaskBuffer() {

                return new BlockingBuffer<KVO<O>[]>(
                        new ArrayBlockingQueue<KVO<O>[]>(subtaskQueueCapacity), //
                        10,// chunkSize
                        sinkChunkTimeoutNanos, TimeUnit.NANOSECONDS,// chunkTimeoutUnit
                        true // ordered
                );

            }

        };

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        /*
         * write a chunk on the buffer. this will cause an output buffer to be
         * created.
         */
        final long beginWrite = System.nanoTime();
        {
            final KVO<O>[] a = new KVO[] { //
                    new KVO<O>(new byte[] { 1 }, new byte[] { 2 }, null/* val */), };

            masterBuffer.add(a);

        }

        // wait just a bit for that chunk to be output by the sink.
        awaitChunksOut(master, 1, sinkIdleTimeoutNanos + sinkIdleTimeoutNanos
                / 2, TimeUnit.NANOSECONDS);

        final long elapsed = System.nanoTime() - beginWrite;
        
        // verify chunk was output by the sink.
        assertEquals("elementsIn", 1, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        assertEquals("elementsOut", 1, masterStats.elementsOut);
        assertEquals("chunksOut", 1, masterStats.chunksOut);
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());
        
        // verify the sink was closed by an idle timeout.
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 1, masterStats.subtaskEndCount);
        assertEquals("subtaskIdleTimeout", 1, masterStats.subtaskIdleTimeout);

        /*
         * Verify that the sink was closed before a chunk timeout would have
         * occurred.
         * 
         * Note: if this assertion is triggered then you might have to adjust
         * the chunk timeout to be longer. For example, this could be triggered
         * if the host was swapping since the timings would be far off their
         * nominal values.
         */
        assertTrue("test did not complete before chunk timeout.",
                elapsed <= sinkChunkTimeoutNanos);
        
        // close the master
        masterBuffer.close();

        // and await its future.
        masterBuffer.getFuture().get();

    }

    /**
     * Used to recognize the a scheduled task which halts based on our signal
     * rather than for some error condition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class HaltedException extends RuntimeException {

        private static final long serialVersionUID = 1L;
        
        public HaltedException() {
            super();
        }
        
    }

    /**
     * Unit test verifies that a sink whose source iterator is not empty always
     * resets the idle timeout (that is, if chunks continue to appear in a
     * timely manner than the sink will not be closed by an idle timeout).
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * 
     * @todo test when {@link AbstractSubtask#handleChunk(Object[])} has a
     *       latency in excess of the idle timeout and verify that we test for
     *       an available chunk before concluding that the sink is idle.
     */
    public void test_idleTimeout_LT_chunkTimeout2() throws InterruptedException, ExecutionException {

        // how long to run this test.
        final long testDurationMillis = 5000;
        
        /*
         * The ratio of sinkChunkTimeout to sinkIdleTimeout.
         */
        final int N = 10;

        /*
         * The fraction of the idle timeout between chunks written onto the
         * master's input buffer.
         */
        final double O = 1.5;
        
        final long sinkIdleTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(50);

        final long sinkChunkTimeoutNanos = sinkIdleTimeoutNanos * N;

        /*
         * The fixed delay between chunks written onto the master.
         * 
         * Note: Unless the master blocks (it should not if the sink is running)
         * this will determine the rate of arrival of chunks at the sink. E.g.,
         * if the idle timeout is 50ms and M = 2, then one chunk arrives every
         * 50ms/2 = 25ms.
         * 
         * Note: For simplicity, this test was designed such that each chunk
         * written onto the master's input buffer had exactly one element.
         * 
         * Note: This MUST be LT the sink idle timeout be a margin sufficient
         * for new chunks to arrive at the sink before an idle timeout is
         * reported.
         */
        final long scheduledDelayNanos = (long) (sinkIdleTimeoutNanos / O);

        /*
         * The expected sink output chunk size (assuming that the sink can
         * combine elements together into chunks of this size without reaching
         * its chunkSize limit).
         */
        final double expectedAverageOutputChunkSize = N * O;
        
        /*
         * Override the [masterBuffer] so that we can disable its chunk
         * combiner. This way only the sink will be combining chunks together
         * which makes the behavior of the system more predictable.
         * 
         * Note: this hides the field by the same name on our base class!
         */
        final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
                masterQueueCapacity,// 
                1, // chunkSize
                0L/*
                   * Note: a chunkTimeout of ZERO (0) disables the chunk
                   * combiner for the master.
                   */, TimeUnit.NANOSECONDS);

        final M master = new M(masterStats, masterBuffer, executorService,
                sinkIdleTimeoutNanos,
                M.DEFAULT_SINK_POLL_TIMEOUT) {
          
            protected BlockingBuffer<KVO<O>[]> newSubtaskBuffer() {

                return new BlockingBuffer<KVO<O>[]>(
                        new ArrayBlockingQueue<KVO<O>[]>(subtaskQueueCapacity), //
                        100, // chunkSize
                        sinkChunkTimeoutNanos, TimeUnit.NANOSECONDS,// chunkTimeoutUnit
                        true // ordered
                );

            }

        };

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        // scheduled service used to write on the master.
        final ScheduledExecutorService scheduledExecutorService = Executors
                .newScheduledThreadPool(1, DaemonThreadFactory
                        .defaultThreadFactory());
        
        try {

            /*
             * Submit scheduled task which writes a chunk on the master each
             * time it runs. The task is scheduled to run with a frequency 2x
             * that of the idle timeout so the sink SHOULD NOT timeout.
             * 
             * Note: The master/sink we are testing DO NOT perform duplicate
             * elimination so the #of chunks/elements written onto the master
             * MUST directly correspond to the #of chunks/elements written onto
             * the sink(s). For this test we direct all element to the same sink
             * by virtue of using the same key for all elements.
             */

            // #of chunks written on the master.
            final AtomicInteger counter = new AtomicInteger(0);

            /*
             * This is used to halt execution without interrupting the task
             * (interrupts make the behavior of master#add() less predictable since
             * we can not know whether a chunk was fully written before the task was
             * interrupted and so the counters can be off by one).
             */ 
            final AtomicBoolean halt = new AtomicBoolean(false);
            
            // start scheduled task.
            final ScheduledFuture<?> f = scheduledExecutorService
                    .scheduleWithFixedDelay(
                            new Runnable() {
                                public void run() {
                                    
                                    // halt?
                                    if(halt.get())
                                        throw new HaltedException();
                                    
                                    // verify the sink WAS NOT closed by an idle timeout.
                                    assertEquals("subtaskIdleTimeout", 0,
                                            masterStats.subtaskIdleTimeout);
                                    
                                    // add chunk to master's input buffer.
                                    final KVO<O>[] a = new KVO[] { //
                                            new KVO<O>(new byte[] { 1 },
                                                    new byte[] { 2 }, null/* val */),//
                                            };
                                    masterBuffer.add(a);
                                    
                                    // increment counter.
                                    counter.incrementAndGet();
                                }
                            }, 0L/* initialDelay */,
                            scheduledDelayNanos,
                            TimeUnit.NANOSECONDS);

            /*
             * Sleep a while giving the scheduled task time to write data onto
             * the master, the master time to move chunks onto the sink, and the
             * sink time to output those chunks.
             */
            Thread.sleep(testDurationMillis);

            // should not be done.
            if(f.isDone()) {
                // should throw out the exception.
                f.get();
                // otherwise something very weird.
                fail("Scheduled task aborted? : counter=" + counter + " : "
                        + masterStats);
            }

            // cancel the scheduled future.
            halt.set(true);

            /*
             * wait for the scheduled task to terminate and verify that it
             * terminated in response to our signal and not for any other
             * reason.
             */
            try {
                // wait for the future.
                f.get();
                // should have thrown an exception.
                fail("Expecting: " + ExecutionException.class + " wrapping "
                        + HaltedException.class);
            } catch (ExecutionException ex) {
                // verify correct exception was thrown.
                final Throwable cause = ex.getCause();
                if (cause == null || (!(cause instanceof HaltedException))) {
                    fail("Expecting: " + HaltedException.class);
                }
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

            // close the master
            masterBuffer.close();

            // and await its future.
            masterBuffer.getFuture().get();
            
            // verify data in/out.
            
            // chunks/elements in for the buffer.
            assertEquals("masterBuffer.elementsIn", counter.get(), masterBuffer
                    .getElementCount());
            assertEquals("masterBuffer.chunksIn", counter.get(), masterBuffer
                    .getChunkCount());

            /*
             * Note: chunks/elements in for the master can not be known since
             * the master can combine chunks when reading from its buffer.
             */
//            assertEquals("elementsIn", counter.get(), masterStats.elementsIn);
//            assertEquals("chunksIn", counter.get(), masterStats.chunksIn);
            
            // elements out for the master (aggregated across all sinks).
            assertEquals("elementsOut", counter.get(), masterStats.elementsOut);

            // only a single sink key was used (one index partition).
            assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());

            /*
             * verify that the sink was not closed by an idle timeout before the
             * master was closed (this verifies that we only had a single sink
             * running for the entire test).
             */
            assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
            assertEquals("subtaskEndCount", 1, masterStats.subtaskEndCount);
            
            /*
             * verify that chunks were combined until they had on average ~ N
             * elements per sink since the rate of chunk arrival should have
             * been ~ N times the chunk timeout.
             */
            final double actualAverageOutputChunkSize = (double) masterStats.elementsOut
                    / (double) masterStats.chunksOut;

            final double r = actualAverageOutputChunkSize
                    / expectedAverageOutputChunkSize;

            final String msg = "average elements per output chunk: "
                + actualAverageOutputChunkSize + ", N=" + N + ", O=" + O
                + ", " + expectedAverageOutputChunkSize+", ratio="+r;
        
            System.err.println(msg);
        
            if (r < .8 || r > 1.1) {

                /*
                 * The ration between the expected and observed average chunk
                 * sizes is not within some reasonable bounds on its
                 * performance. [It is more likely that this will report an
                 * error for a short run, but the values should be quite close
                 * on a longer run.]
                 */
                fail(msg);
                
            }

            //            assertTrue(elementsPerChunk>masterState.chunks)
            
        } finally {

            scheduledExecutorService.shutdownNow();

        }

    }

    /**
     * Unit test verifies correct shutdown when writing chunks onto a master
     * whose subtask has an infinite chunk timeout and an infinite idle timeout.
     * In this situation the subtask must notice that the master's input buffer
     * has been closed and close its input buffer so that it will process any
     * elements remaining in that buffer and then terminate.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_idleTimeoutInfinite_chunkTimeoutInfinite() throws InterruptedException,
            ExecutionException {

        final M master = new M(masterStats, masterBuffer, executorService,
                Long.MAX_VALUE/* sinkIdleTimeout */, M.DEFAULT_SINK_POLL_TIMEOUT) {

            /**
             * The subtask will use a buffer with an infinite chunk timeout.
             */
            @Override
            protected BlockingBuffer<KVO<O>[]> newSubtaskBuffer() {

                return new BlockingBuffer<KVO<O>[]>(
                        new ArrayBlockingQueue<KVO<O>[]>(subtaskQueueCapacity), //
                        BlockingBuffer.DEFAULT_CONSUMER_CHUNK_SIZE,// 
                        Long.MAX_VALUE, TimeUnit.SECONDS,// chunkTimeout
                        true // ordered
                );

            }
            
        };
        
        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        // write a chunk on the master.
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 1 }, null/* val */)
            };

            masterBuffer.add(a);

        }
        
        /*
         * Sleep long enough that we may assume that the chunk has been
         * propagated by the master to the subtask.
         */ 
        Thread.sleep(1000/* ms */);

        // verify that the master has accepted the 1st chunk. 
        assertEquals("elementsIn", 1, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        
        // verify that nothing has been output yet.
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);

        // write another chunk on the master (distinct values).
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 2 }, null/* val */)
            };

            masterBuffer.add(a);
            
        }

        // sleep some more.
        Thread.sleep(1000/* ms */);

        // verify that the master has accepted the 2nd chunk. 
        assertEquals("elementsIn", 2, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        
        // verify that nothing has been output yet.
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);

        // close the master - the output sink should now emit a chunk.
        masterBuffer.close();

        // await the future.
        masterBuffer.getFuture().get();
        
        // verify elements in/out; chunks in/out
        assertEquals("elementsIn", 2, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        
        assertEquals("elementsOut", 2, masterStats.elementsOut);
        assertEquals("chunksOut", 1, masterStats.chunksOut);
        
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());

    }

    /**
     * Unit test verifies that an idle timeout will cause a sink to flush its
     * buffer without waiting to combine additional chunks even though it has an
     * infinite chunkTimeout.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public void test_idleTimeout_with_infiniteChunkTimeout()
            throws InterruptedException, ExecutionException, TimeoutException {

        final long sinkIdleTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(100);

        final M master = new M(masterStats, masterBuffer, executorService,
                sinkIdleTimeoutNanos, M.DEFAULT_SINK_POLL_TIMEOUT) {

            /**
             * The subtask will use a buffer with an infinite chunk timeout.
             */
            @Override
            protected BlockingBuffer<KVO<O>[]> newSubtaskBuffer() {

                return new BlockingBuffer<KVO<O>[]>(
                        new ArrayBlockingQueue<KVO<O>[]>(subtaskQueueCapacity), //
                        BlockingBuffer.DEFAULT_CONSUMER_CHUNK_SIZE,// 
                        Long.MAX_VALUE, TimeUnit.SECONDS,// chunkTimeout (infinite)
                        true // ordered
                );

            }
            
        };
        
        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        // write a chunk on the master.
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 1 }, null/* val */)
            };

            masterBuffer.add(a);

        }

        /*
         * Sleep long enough that we may assume that the chunk has been
         * propagated by the master to the subtask but less than the subtask
         * idle timeout.
         */ 
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(sinkIdleTimeoutNanos) / 2);

        // verify that the master has accepted the 1st chunk. 
        assertEquals("elementsIn", 1, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        
        // verify that nothing has been output yet.
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);

        // verify subtask has started but not yet ended.
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 0, masterStats.subtaskEndCount);
        assertEquals("subtaskIdleTimeout", 0, masterStats.subtaskIdleTimeout);
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());

        // write another chunk on the master (distinct values).
        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 2 }, null/* val */)
            };

            masterBuffer.add(a);
            
        }

        // sleep some more, again not exceeding the subtask idle timeout.
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(sinkIdleTimeoutNanos) / 2);

        // verify that the master has accepted the 2nd chunk. 
        assertEquals("elementsIn", 2, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        
        // verify that nothing has been output yet.
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);

        // verify the same subtask is still running.
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 0, masterStats.subtaskEndCount);
        assertEquals("subtaskIdleTimeout", 0, masterStats.subtaskIdleTimeout);
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());

        /*
         * Sleep long enough for the subtask idle timeout to be exceeded. The
         * subtask should emit a chunk which combines the two chunks written on
         * it by the master and then close itself.
         */
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(sinkIdleTimeoutNanos) * 2);
        
        // verify elements in/out; chunks in/out
        assertEquals("elementsIn", 2, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        
        assertEquals("elementsOut", 2, masterStats.elementsOut);
        assertEquals("chunksOut", 1, masterStats.chunksOut);

        /*
         * Verify only one subtask started, that it is now ended, and that it
         * ended via an idle timeout.
         */
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 1, masterStats.subtaskEndCount);
        assertEquals("subtaskIdleTimeout", 1, masterStats.subtaskIdleTimeout);
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());

        // close the master.
        masterBuffer.close();

        /*
         * await the future, but only for a little while in case the subtask
         * does not terminate correctly.
         */
        masterBuffer.getFuture().get(1, TimeUnit.SECONDS);

        /*
         * Note: These tests are the same as those before we close the master.
         * None of these asserts should be violated by closing the master.
         */
        
        // verify elements in/out; chunks in/out
        assertEquals("elementsIn", 2, masterStats.elementsIn);
        assertEquals("chunksIn", 2, masterStats.chunksIn);
        
        assertEquals("elementsOut", 2, masterStats.elementsOut);
        assertEquals("chunksOut", 1, masterStats.chunksOut);

        /*
         * Verify only one subtask started, that it is now ended, and that it
         * ended via an idle timeout.
         */
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount);
        assertEquals("subtaskEndCount", 1, masterStats.subtaskEndCount);
        assertEquals("subtaskIdleTimeout", 1, masterStats.subtaskIdleTimeout);
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());

    }
    
}
