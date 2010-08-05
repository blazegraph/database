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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.keys.KVO;
import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Unit tests for the control logic used by {@link AbstractMasterTask} and
 * friends.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMasterTask extends AbstractMasterTestCase {

    public TestMasterTask() {
    }

    public TestMasterTask(String name) {
        super(name);
    }
        
    /**
     * Test verifies start/stop of the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startStop() throws InterruptedException,
            ExecutionException {

        final H masterStats = new H();

        final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
                masterQueueCapacity);

        final M master = new M(masterStats, masterBuffer, executorService);

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", 0, masterStats.elementsIn.get());
        assertEquals("chunksIn", 0, masterStats.chunksIn.get());
        assertEquals("elementsOut", 0, masterStats.elementsOut.get());
        assertEquals("chunksOut", 0, masterStats.chunksOut.get());
        assertEquals("partitionCount", 0, masterStats
                .getMaximumPartitionCount());

    }

    /**
     * Unit test writes an empty chunk and then stops the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startEmptyWriteStop() throws InterruptedException,
            ExecutionException {

        final H masterStats = new H();

        final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
                masterQueueCapacity);

        final M master = new M(masterStats, masterBuffer, executorService);

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[0];
        
        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", 0, masterStats.elementsIn.get());
        assertEquals("chunksIn", 0, masterStats.chunksIn.get());
        assertEquals("elementsOut", 0, masterStats.elementsOut.get());
        assertEquals("chunksOut", 0, masterStats.chunksOut.get());
        assertEquals("partitionCount", 0, masterStats.getMaximumPartitionCount());

    }

    /**
     * Unit test writes a chunk and then stops the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteStop1() throws InterruptedException,
            ExecutionException {

        final H masterStats = new H();

        final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
                masterQueueCapacity);

        final M master = new M(masterStats, masterBuffer, executorService);

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[] {
                new KVO<O>(new byte[]{1},new byte[]{2},null/*val*/),
                new KVO<O>(new byte[]{1},new byte[]{3},null/*val*/)
        };

        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", a.length, masterStats.elementsIn.get());
        assertEquals("chunksIn", 1, masterStats.chunksIn.get());
        assertEquals("elementsOut", a.length, masterStats.elementsOut.get());
        assertEquals("chunksOut", 1, masterStats.chunksOut.get());
        assertEquals("partitionCount", 1, masterStats.getMaximumPartitionCount());

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 1, subtaskStats.chunksOut.get());
            assertEquals("elementsOut", 2, subtaskStats.elementsOut.get());
            
        }

        // make sure that these counters were updated.
        assertEquals("subtaskStartCount", 1, masterStats.subtaskStartCount.get());
        assertEquals("subtaskEndCount", 1, masterStats.subtaskEndCount.get());

    }

    /**
     * Unit test writes a chunk that is split onto two subtasks and then stops
     * the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteStop2() throws InterruptedException,
            ExecutionException {

        doStartWriteStop2Test();

    }

    /**
     * Stress test for the atomic termination condition.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_stress_startWriteStop2() throws InterruptedException,
            ExecutionException {

        for (int i = 0; i < 10000; i++) {

            try {
                doStartWriteStop2Test();
            } catch (Throwable t) {
                fail("Pass#=" + i, t);
            }

        }

    }

    /**
     * The logic for {@link #test_startWriteWriteStop2()}, which is reused by a
     * stress test.
     * <p>
     * 
     * @todo There is still one fence post in some of the unit tests. The
     *       symptom is that the unit test fails due to an unexpected value for
     *       either masterStats.subtaskStartCount or masterStats.subtaskEndCount
     *       or for an unexpected value for either subtaskStats.chunksOut or
     *       subtaskStats.chunksIn. I suspect that the underlying issue is the
     *       expectation of the tests with respect to when a sink is retired by
     *       the master and whether or not a new subtaskStats object is
     *       allocated or an old one reused (e.g., because the subtask was not
     *       closed by an idle timeout or the like).
     *       <p>
     *       This test failures can be cleared up if you uncomment the [tmp]
     *       list and its use in
     *       {@link AbstractMasterStats#getSubtaskStats(Object)}. This forces
     *       the internal map to hold a hard reference to the subtask statistics
     *       objects. This demonstrates that the problem is not with the
     *       termination conditions for the {@link AbstractMasterTask}. However,
     *       the unit test are still broken until I track down the underlying
     *       assumption within them which is being violated.
     * 
     * @throws InterruptedException
     */
    private void doStartWriteStop2Test() throws InterruptedException,
            ExecutionException  {

        final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
                masterQueueCapacity);

        final H masterStats = new H();

        final M master = new M(masterStats, masterBuffer, executorService);

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[] {
                new KVO<O>(new byte[]{1},new byte[]{2},null/*val*/),
                new KVO<O>(new byte[]{2},new byte[]{3},null/*val*/),
                new KVO<O>(new byte[]{2},new byte[]{4},null/*val*/)
        };

        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", a.length, masterStats.elementsIn.get());
        assertEquals("chunksIn", 1, masterStats.chunksIn.get());
        assertEquals("elementsOut", a.length, masterStats.elementsOut.get());
        assertEquals("chunksOut", 2, masterStats.chunksOut.get());
        assertEquals("partitionCount", 2, masterStats.getMaximumPartitionCount());

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);

            assertEquals("chunksOut", 1, subtaskStats.chunksOut.get());
            assertEquals("elementsOut", 1, subtaskStats.elementsOut.get());
            
        }
        
        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(2));

            assertNotNull(subtaskStats);
            
            // @todo this assert fails stochastically.
            assertEquals("chunksOut", 1, subtaskStats.chunksOut.get());
            assertEquals("elementsOut", 2, subtaskStats.elementsOut.get());
            
        }

        /*
         * @todo make sure that these counters were updated? I need to verify
         * the assumptions behind these asserts. The conditions for start/end of
         * a subtask may now be subtly different.
         */
//        assertEquals("subtaskStartCount", 2, masterStats.subtaskStartCount.get());
//        assertEquals("subtaskEndCount", 2, masterStats.subtaskEndCount.get());

    }

    /**
     * Unit test writes 2 chunks that are each split onto two subtasks and then
     * stops the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteWriteStop2() throws InterruptedException,
            ExecutionException {

        final H masterStats = new H();

        final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
                masterQueueCapacity);

        final M master = new M(masterStats, masterBuffer, executorService);

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        {
        final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 2 }, null/* val */),
                    new KVO<O>(new byte[] { 2 }, new byte[] { 3 }, null/* val */),
                    new KVO<O>(new byte[] { 2 }, new byte[] { 4 }, null/* val */) };

            masterBuffer.add(a);
        }

        /*
         * Sleep until the first two chunks are output (this simplifies the
         * post-condition tests).
         */
        awaitChunksOut(master, 2, 1000, TimeUnit.MILLISECONDS);

        {
            final KVO<O>[] a = new KVO[] {
                    new KVO<O>(new byte[] { 1 }, new byte[] { 3 }, null/* val */),
                    new KVO<O>(new byte[] { 2 }, new byte[] { 5 }, null/* val */) };

            masterBuffer.add(a);
        }

        masterBuffer.close();

        assertTrue(masterStats == masterBuffer.getFuture().get());

        assertEquals("elementsIn", 5, masterStats.elementsIn.get());
        assertEquals("chunksIn", 2, masterStats.chunksIn.get());
        assertEquals("elementsOut", 5, masterStats.elementsOut.get());
        assertEquals("chunksOut", 4, masterStats.chunksOut.get());
        assertEquals("partitionCount", 2, masterStats
                .getMaximumPartitionCount());

        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(1));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 2, subtaskStats.chunksOut.get());
            assertEquals("elementsOut", 2, subtaskStats.elementsOut.get());
            
        }
        
        // verify writes on each expected partition.
        {
            
            final HS subtaskStats = masterStats.getSubtaskStats(new L(2));

            assertNotNull(subtaskStats);
            
            assertEquals("chunksOut", 2, subtaskStats.chunksOut.get());
            assertEquals("elementsOut", 3, subtaskStats.elementsOut.get());
            
        }

        // verify right #of tasks executed.
        assertEquals("subtaskStartCount", 2, masterStats.subtaskStartCount.get());
        assertEquals("subtaskEndCount", 2, masterStats.subtaskEndCount.get());

    }

}
