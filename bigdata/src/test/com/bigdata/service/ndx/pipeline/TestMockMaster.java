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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.KVO;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Unit tests for the control logic used by {@link AbstractMasterTask} and
 * friends.
 * <p>
 * Note: There are a bunch of inner classes which have the same names as the
 * generic types used by the master and subtask classes. This makes it much
 * easier to instantiate these things since all of the generic variety has been
 * factored out.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test ability to halt when one of the subtasks has an error.
 * 
 * @todo test ability to handle a redirect (subtask learns that the target
 *       service no longer accepts data for some locator and instead must send
 *       the data somewhere else).
 * 
 * @todo test ability to close and reopen (well, open a new subtask for the same
 *       locator) during master.awaitAll().
 */
public class TestMockMaster extends TestCase2 {

    public TestMockMaster() {
    }

    public TestMockMaster(String name) {
        super(name);
    }
    
    static class L {
        
        private final int locator;
        
        public L(int locator) {
            this.locator = locator;
        }
        
        public int hashCode() {
            return locator;
        }
        
        public boolean equals(Object o) {

            return ((L) o).locator == locator;
            
        }
        
    }

    static class HS extends MockSubtaskStats {
        
    }
    
    static class O extends Object {
        
    }
    
    static class H extends MockMasterStats<L, HS> {
        
    }
    
    static class M extends MockMaster<H, O, KVO<O>, S, L, HS> {

        private final ExecutorService executorService;
        
        public M(H stats, BlockingBuffer<KVO<O>[]> buffer,
                ExecutorService executorService) {

            super(stats, buffer);

            this.executorService = executorService;
            
        }
        
        @Override
        protected S newSubtask(L locator, BlockingBuffer<KVO<O>[]> out) {

            return new S(this, locator, out);
            
        }

        /**
         * Hash partitions the elements in the chunk using the hash of the key
         * into a fixed population of N partitions.
         */
        protected void hashPartition(final KVO<O>[] chunk)
                throws InterruptedException {

            // #of partitions.
            final int N = 10;
            
            // array of ordered containers for each partition.
            final List<KVO<O>>[] v = new List[N];

            for (KVO<O> e : chunk) {

                // Note: Could have hashed on the Object value as easily as the
                // key, which would make sense for some applications.
                final int i = e.key.hashCode() % N;

                if (v[i] == null) {

                    v[i] = new LinkedList<KVO<O>>();
                    
                }

                v[i].add(e);
                
            }

            for (int i = 0; i < v.length; i++) {

                final List<KVO<O>> t = v[i];

                if (t == null) {
                    // no data for this partition.
                    continue;
                }

                final KVO<O>[] a = t.toArray(new KVO[t.size()]);

                addToOutputBuffer(new L(i), a, 0/* fromIndex */,
                        a.length/* toIndex */, false/* reopen */);
                
            }
            
        }

        /**
         * Assigns elements from an ordered chunk to key-range partitions by
         * interpreting the first byte of the key as the partition identifier
         * (does not work if the key is empty).
         */
        protected void keyRangePartition(final KVO<O>[] chunk)
                throws InterruptedException {

            // #of partitions (one per value that a byte can take on).
            final int N = 255;
            
            // array of ordered containers for each partition.
            final List<KVO<O>>[] v = new List[N];

            for (KVO<O> e : chunk) {

                final int i = e.key[0];

                if (v[i] == null) {

                    v[i] = new LinkedList<KVO<O>>();
                    
                }

                v[i].add(e);
                
            }

            for (int i = 0; i < v.length; i++) {

                final List<KVO<O>> t = v[i];

                if (t == null) {
                    // no data for this partition.
                    continue;
                }

                final KVO<O>[] a = t.toArray(new KVO[t.size()]);

                addToOutputBuffer(new L(i), a, 0/* fromIndex */,
                        a.length/* toIndex */, false/* reopen */);

            }

        }

        /**
         * Adds the entire chunk to the sole partition.
         */
        protected void onePartition(final KVO<O>[] chunk)
                throws InterruptedException {

            addToOutputBuffer(new L(1), chunk, 0, chunk.length, false/* reopen */);

        }

        @Override
        protected void nextChunk(final KVO<O>[] chunk) throws InterruptedException {

            keyRangePartition(chunk);
            
        }

        protected BlockingBuffer<KVO<O>[]> newSubtaskBuffer() {

            return new BlockingBuffer<KVO<O>[]>(
                    new ArrayBlockingQueue<KVO<O>[]>(subtaskQueueCapacity), //
                    BlockingBuffer.DEFAULT_CONSUMER_CHUNK_SIZE,// 
                    BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT,//
                    BlockingBuffer.DEFAULT_CONSUMER_CHUNK_TIMEOUT_UNIT,//
                    true // ordered
            );

        }

        @Override
        protected Future<? extends AbstractSubtaskStats> submitSubtask(S subtask) {

            return executorService.submit(subtask);
            
        }

    }

    static class S extends MockSubtask<H, O, KVO<O>, L, S, HS, M> {

        public S(M master, L locator, BlockingBuffer<KVO<O>[]> buffer) {

            super(master, locator, buffer);

        }
        
        @Override
        protected boolean nextChunk(KVO<O>[] chunk) throws Exception {

            synchronized (master.stats) {
             
                master.stats.chunksOut++;
                master.stats.elementsOut += chunk.length;
                
            }
            
            // keep processing.
            return false;
            
        }

    }

    final int masterQueueCapacity = 100;

    static final int subtaskQueueCapacity = 100;

    final H masterStats = new H();

    final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
            masterQueueCapacity);

    final ExecutorService executorService = Executors.newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());
    
    final M master = new M(masterStats, masterBuffer, executorService);

    protected void tearDown() {
        
        executorService.shutdownNow();
        
    }
    
    /**
     * Test verifies start/stop of the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startStop() throws InterruptedException, ExecutionException {

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        //        masterBuffer.add(null);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsOut", 0, masterStats.elementsOut);

    }

    /**
     * Unit test writes an empty chunk and then stops the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startEmptyWriteStop() throws InterruptedException, ExecutionException {

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[0];
        
        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", 0, masterStats.elementsIn);
        assertEquals("chunksIn", 0, masterStats.chunksIn);
        assertEquals("elementsOut", 0, masterStats.elementsOut);
        assertEquals("chunksOut", 0, masterStats.chunksOut);

    }

    /**
     * Unit test writes a chunk and then stops the master.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_startWriteStop1() throws InterruptedException,
            ExecutionException {

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

        assertEquals("elementsIn", a.length, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        assertEquals("elementsOut", a.length, masterStats.elementsOut);
        assertEquals("chunksOut", 1, masterStats.chunksOut);
        
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

        // start the consumer.
        final Future<H> future = executorService.submit(master);
        masterBuffer.setFuture(future);

        final KVO<O>[] a = new KVO[] {
                new KVO<O>(new byte[]{1},new byte[]{2},null/*val*/),
                new KVO<O>(new byte[]{2},new byte[]{3},null/*val*/)
        };

        masterBuffer.add(a);

        masterBuffer.close();

        masterBuffer.getFuture().get();

        assertEquals("elementsIn", a.length, masterStats.elementsIn);
        assertEquals("chunksIn", 1, masterStats.chunksIn);
        assertEquals("elementsOut", a.length, masterStats.elementsOut);
        assertEquals("chunksOut", 2, masterStats.chunksOut);
        
    }

}
