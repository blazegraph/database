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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.btree.keys.KVO;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for test suites for the {@link AbstractMasterTask} and
 * friends.
 * <p>
 * Note: There are a bunch of inner classes which have the same names as the
 * generic types used by the master and subtask classes. This makes it much
 * easier to instantiate these things since all of the generic variety has been
 * factored out.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractMasterTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractMasterTestCase() {
       
    }

    /**
     * @param arg0
     */
    public AbstractMasterTestCase(String arg0) {
        super(arg0);
       
    }

    /**
     * The locator is a simple integer - you can think of this as being similar
     * to the index partition identifier. Since the unit tests are not concerned
     * with the real indices we do not need to differentiate between indices,
     * just their partitions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class L implements Comparable<L> {
        
        protected final int locator;

        public L(int locator) {
            
            this.locator = locator;
            
        }

        public int hashCode() {
            
            return locator;
            
        }
        
        public boolean equals(Object o) {

            return ((L) o).locator == locator;
            
        }
        
        public String toString() {
            
            return "L{locator=" + locator + "}";
            
        }

        public int compareTo(final L o) {
            if (locator > o.locator)
                return 1;
            if (locator == o.locator)
                return 0;
            return -1;
        }
        
    }

    static class HS extends MockSubtaskStats {
        
    }
    
    static class O extends Object {
        
    }
    
    static class H extends MockMasterStats<L, HS> {
        
        @Override
        protected HS newSubtaskStats(L locator) {

            return new HS();
            
        }

    }
    
    /**
     * Concrete master impl w/o generic types.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class M extends MockMaster<H, O, KVO<O>, S, L, HS> {

        private final ExecutorService executorService;

        static final long DEFAULT_SINK_IDLE_TIMEOUT = TimeUnit.MILLISECONDS
                .toNanos(2000);

        static final long DEFAULT_SINK_POLL_TIMEOUT = TimeUnit.MILLISECONDS
                .toNanos(50);

        public M(final H stats, final BlockingBuffer<KVO<O>[]> buffer,
                final ExecutorService executorService) {

            this(stats, buffer, executorService, DEFAULT_SINK_IDLE_TIMEOUT,
                    DEFAULT_SINK_POLL_TIMEOUT);
            
        }

        public M(final H stats, final BlockingBuffer<KVO<O>[]> buffer,
                final ExecutorService executorService, final long sinkIdleTimeout,
                final long sinkPollTimeout) {

            super(stats, buffer, sinkIdleTimeout, sinkPollTimeout);

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
        protected void hashPartition(final KVO<O>[] chunk,final boolean reopen)
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
         * A map used by {@link #keyRangePartition(KVO[], boolean)}. If there
         * is an entry in the map corresponding to the integer value of the
         * first byte of the key (which is interpreted as the partition locator)
         * then the value stored under that entry is the integer value for the
         * partition locator to which the tuple will be directed.
         * <p>
         * The map is empty by default. Some unit tests populate it as they
         * force redirects.
         */
        final protected ConcurrentHashMap<Integer, Integer> redirects = new ConcurrentHashMap<Integer, Integer>();
        
        /**
         * Assigns elements from an ordered chunk to key-range partitions by
         * interpreting the first byte of the key as the partition identifier
         * (does not work if the key is empty).
         */
        protected void keyRangePartition(final KVO<O>[] chunk,
                final boolean reopen) throws InterruptedException {

            /*
             * Split the tuples.
             */
            // #of partitions (one per value that a byte can take on).
            final int N = 255;
            // array of ordered containers for each partition.
            final List<KVO<O>>[] v = new List[N];
            for (KVO<O> e : chunk) {

                // adjust to [0:255] (somewhat arbitrary).
                final byte b = e.key[0];
                final int j = (b < 0 ? 255 + b : b);

                final Integer redirect = redirects.get(j);

                final int i = redirect == null ? j : redirect.intValue();

                if (v[i] == null) {

                    v[i] = new LinkedList<KVO<O>>();

                }

                v[i].add(e);

            }

            /*
             * Assign tuples to output buffers based on those splits.
             */
            for (int i = 0; i < v.length; i++) {

                final List<KVO<O>> t = v[i];

                if (t == null) {
                    // no data for this partition.
                    continue;
                }

                final KVO<O>[] a = t.toArray(new KVO[t.size()]);

                addToOutputBuffer(new L(i), a, 0/* fromIndex */,
                        a.length/* toIndex */, reopen);

            }

        }

        /**
         * Adds the entire chunk to the sole partition.
         */
        protected void onePartition(final KVO<O>[] chunk, final boolean reopen)
                throws InterruptedException {

            addToOutputBuffer(new L(1), chunk, 0, chunk.length, reopen );

        }

        /**
         * This applies {@link #keyRangePartition(KVO[])} to map the chunk
         * across the output buffers for the subtasks.
         */
        @Override
        protected void handleChunk(final KVO<O>[] chunk, final boolean reopen)
                throws InterruptedException {

            keyRangePartition(chunk, reopen);

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

    /**
     * Concrete subtask impl w/o generic types.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class S extends MockSubtask<H, O, KVO<O>, L, S, HS, M> {

        public S(final M master, final L locator,
                final BlockingBuffer<KVO<O>[]> buffer) {

            super(master, locator, buffer);

        }
        
        /**
         * This method may be overridden to simulate the latency of the
         * write operation.  The default is a NOP.
         */
        protected void writeData(final KVO<O>[] chunk) throws Exception {
            
        }
        
        @Override
        protected boolean handleChunk(final KVO<O>[] chunk) throws Exception {

            final long begin = System.nanoTime();
            
            writeData(chunk);
            
            final long elapsed = System.nanoTime() - begin;
            
            synchronized (master.stats) {

                master.stats.chunksOut++;
                master.stats.elementsOut += chunk.length;
                master.stats.elapsedChunkWritingNanos += elapsed;

            }

            stats.chunksOut++;
            stats.elementsOut += chunk.length;
            stats.elapsedChunkWritingNanos += elapsed;

            if (log.isInfoEnabled())
                log.info("wrote chunk: " + this + ", #elements="
                                + chunk.length);
            
            // keep processing.
            return false;
            
        }

    }

    final int masterQueueCapacity = 100;

    static final int subtaskQueueCapacity = 100;

    final H masterStats = new H();

    final BlockingBuffer<KVO<O>[]> masterBuffer = new BlockingBuffer<KVO<O>[]>(
            masterQueueCapacity);

    final ExecutorService executorService = Executors
            .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

    protected void tearDown() {

        executorService.shutdownNow();

    }

    /**
     * Sleep up to the timeout or until the chunksOut takes on the specified
     * value.
     * 
     * @param master
     * @param expectedChunksOut
     * @param timeout
     * @param unit
     * 
     * @throws InterruptedException
     * @throws AssertionFailedError
     */
    protected void awaitChunksOut(final M master, final int expectedChunksOut,
            final long timeout, final TimeUnit unit)
            throws InterruptedException {

        long nanos = unit.toNanos(timeout);

        while (nanos > 0) {

            if (master.stats.chunksOut >= expectedChunksOut) {

                return;

            }

            Thread.sleep(1/* ms */);

        }

        fail("Timeout");

    }

}
