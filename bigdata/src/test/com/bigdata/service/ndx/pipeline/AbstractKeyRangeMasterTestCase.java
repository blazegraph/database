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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KVO;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.AbstractSplitter;
import com.bigdata.service.ndx.ISplitter;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Abstract base class for test suites for the {@link AbstractMasterTask} and
 * friends using {@link IPartitionMetadata} locators.
 * <p>
 * Note: There are a bunch of inner classes which have the same names as the
 * generic types used by the master and subtask classes. This makes it much
 * easier to instantiate these things since all of the generic variety has been
 * factored out.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractKeyRangeMasterTestCase extends TestCase2 {

    /**
     * 
     */
    public AbstractKeyRangeMasterTestCase() {
       
    }

    /**
     * @param arg0
     */
    public AbstractKeyRangeMasterTestCase(String arg0) {
        super(arg0);
       
    }

    /**
     * Mock stale locator exception.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MockStaleLocatorException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public MockStaleLocatorException(L locator) {
            
            super(locator.toString());
            
        }

    }
    
    /**
     * Mapping from {@link UUID}s onto {@link DS data services}.
     */
    protected final Map<UUID, DS> dataServices = new ConcurrentHashMap<UUID, DS>();

    /**
     * Choose a data service at random.
     */
    protected DS getRandomDataService() {

        final DS[] a = dataServices.values().toArray(new DS[] {});

        final Random r = new Random();

        return a[r.nextInt(a.length)];

    }
    
    /**
     * A mock "data service". This class may be overridden to control the latency
     * of the writes.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class DS {

        /**
         * The UUID for this data service.
         */
        protected final UUID uuid;

        /**
         * A collection of index partition identifiers which are no longer
         * located on this data service.
         * <p>
         * Note: Explicit synchronization is required across accesses to
         * {@link #staleLocators}, {@link #knownLocators}, and across
         * {@link #writeChunk(L, KVO[])}.
         */
        private final Set<Integer/* partitionId */> staleLocators = new HashSet<Integer>();

        /**
         * A collection of index partitions known to exist on this data service.
         */
        private final Set<Integer/* partitionId */> knownLocators = new HashSet<Integer>();

        public DS(UUID uuid) {

            this.uuid = uuid;

        }

        /**
         * Write a chunk onto the data service.
         * 
         * @param locator
         *            Identifies the index partition to which the write request
         *            is directed.
         * @param chunk
         *            The chunk to be written.
         */
        final public void writeChunk(L locator, KVO<O>[] chunk)
                throws MockStaleLocatorException {

            synchronized (this) {

                if (staleLocators.contains(locator.getPartitionId())) {

                    throw new MockStaleLocatorException(locator);

                }

                if (!knownLocators.contains(locator.getPartitionId())) {

                    throw new RuntimeException("Locator not registered on DS: "
                            + locator);

                }

            }

            // Note: outside of synchronized block.
            acceptWrite(locator, chunk);
            
        }

        /**
         * Accept a write (NOP). This may be overridden to impose latency. The
         * caller has already determined that the chunk may be written on this
         * data service for the given locator.
         * 
         * @param locator
         *            The locator.
         * @param chunk
         *            The chunk.
         */
        protected void acceptWrite(L locator, KVO<O>[] chunk) {
            
        }
        
        /**
         * Notify the data service that an index partition is now located on
         * that data service.
         * 
         * @param locator
         *            Identifies the index partition which is located on that
         *            data service.
         */
        protected void notifyLocator(L locator) {

            synchronized (this) {

                if (!knownLocators.add(locator.getPartitionId())) {

                    throw new IllegalStateException(
                            "Already located on this DS: " + locator);

                }

            }
            
        }
        
        /**
         * Notify the data service that an index partition is no longer located
         * on that data service.
         * 
         * @param locator
         *            The locator of the index partition which has been split,
         *            moved, or joined.
         */
        protected void notifyGone(L locator) {

            synchronized (this) {

                if (!staleLocators.add(locator.getPartitionId())) {

                    fail("Locator already in stale locators collection? "
                            + locator);
                
                }

            }
            
        }
        
    }

    /**
     * The locator maps an unsigned byte[] key-range onto a {@link UUID} which
     * identifies a "data service" and an index partition identifier. The index
     * partition is presumed to be located on that data service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class L extends PartitionLocator {
        
        /**
         * De-serialization ctor.
         */
        public L() {
            
        }
        
        /**
         * Core ctor impl.
         * 
         * @param partitionId
         * @param logicalDataServiceUUID
         * @param leftSeparatorKey
         * @param rightSeparatorKey
         */
        public L(final int partitionId, final UUID logicalDataServiceUUID,
                final byte[] leftSeparatorKey, final byte[] rightSeparatorKey) {

            super(partitionId, logicalDataServiceUUID, leftSeparatorKey,
                    rightSeparatorKey);
            
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
    //static
    abstract class M extends MockMaster<H, O, KVO<O>, S, L, HS> {

        private final ExecutorService executorService;

        /** The metadata for the scale-out index. */
        final IndexMetadata managedIndexMetadata = new IndexMetadata(
                "test-ndx", UUID.randomUUID());

        /**
         * The metadata index for the scale-out index.
         * */
        final MetadataIndex mdi = MetadataIndex.create(
                new SimpleMemoryRawStore(), UUID.randomUUID(),
                managedIndexMetadata);

        /**
         * Lock used to provide atomic views on MDI for read (finding splits)
         * and write (updating the split definitions).
         */
        final ReentrantLock mdiLock = new ReentrantLock();

        /**
         * Splitter based on unsigned byte[] key-ranges using the {@link #mdi}.
         */
        final ISplitter splitter = new AbstractSplitter() {

            protected IMetadataIndex getMetadataIndex(long ts) {
              
                return mdi;
                
            };
            
            @Override
            public LinkedList<Split> splitKeys(final long ts, final int fromIndex,
                    final int toIndex, final byte[][] keys) {
                mdiLock.lock();
                try {
                    return super.splitKeys(ts, fromIndex, toIndex, keys);
                } finally {
                    mdiLock.unlock();
                }
            }

            @Override
            public LinkedList<Split> splitKeys(final long ts, final int fromIndex,
                    final int toIndex, final KVO[] a) {
                mdiLock.lock();
                try {
                    return super.splitKeys(ts, fromIndex, toIndex, a);
                } finally {
                    mdiLock.unlock();
                }
            }
            
        };
        
        public M(final H stats, final BlockingBuffer<KVO<O>[]> buffer,
                final ExecutorService executorService, final long sinkIdleTimeout,
                final long sinkPollTimeout) {

            super(stats, buffer, sinkIdleTimeout, sinkPollTimeout);

            this.executorService = executorService;
            
        }

        /**
         * Overrides the subtask to write data on the "data service". The data
         * service may be overridden to control the latency of the operation.
         */
        @Override
        protected S newSubtask(L locator, BlockingBuffer<KVO<O>[]> out) {

            return new S(this, locator, out) {

                protected void writeData(KVO<O>[] chunk) {

                    dataServices.get(locator.getDataServiceUUID())
                            .writeChunk(locator, chunk);
                
                }
                
            };
            
        }

//        /**
//         * Handle a stale locator per {@link IndexWriteTask}.
//         * 
//         * @deprecated by the use of the redirect queue.
//         */
//        @SuppressWarnings("unchecked")
//        protected void handleStaleLocator(final S sink, final KVO<O>[] chunk,
//                final MockStaleLocatorException cause) throws InterruptedException {
//
//            // FIXME remove this method and rewrite the tests.
//            throw new UnsupportedOperationException();
//            
////            if (sink == null)
////                throw new IllegalArgumentException();
////            
////            if (chunk == null)
////                throw new IllegalArgumentException();
////            
////            if (cause == null)
////                throw new IllegalArgumentException();
////
////            lock.lockInterruptibly();
////            try {
////
////                stats.redirectCount++;
////
////                /*
////                 * Note: We do not need to notify the "client" because the
////                 * master is directly accessing the MDI object.
////                 */
//////                /*
//////                 * Notify the client so it can refresh the information for this
//////                 * locator.
//////                 */
//////                ndx.staleLocator(ndx.getTimestamp(), (L) sink.locator, cause);
////
////                /*
////                 * Redirect the chunk and anything in the buffer to the appropriate
////                 * output sinks.
////                 */
////                handleRedirect(sink, chunk);
////
////                /*
////                 * Remove the buffer from the map
////                 * 
////                 * Note: This could cause a concurrent modification error if we are
////                 * awaiting the various output buffers to be closed. In order to
////                 * handle that code that modifies or traverses the [buffers] map
////                 * MUST be MUTEX or synchronized.
////                 */
////                removeOutputBuffer((L) sink.locator, sink);
////
////            } finally {
////
////                lock.unlock();
////                
////            }
//
//        }

        /**
         * Identifies splits using the {@link IMetadataIndex} and assigns those
         * splits to the appropriate output sinks.
         */
        protected void keyRangePartition(final KVO<O>[] chunk,
                final boolean reopen) throws InterruptedException {

            /*
             * Split the tuples.
             * 
             * Note: The Splitter uses the mdiLock to avoid concurrent
             * modification of the MDI while it is querying that index to
             * determine how to split the chunk.
             */
            final LinkedList<Split> splits = splitter.splitKeys(
                    0L/* timestampIsIgnored */, 0/* fromIndex */,
                    chunk.length/* toIndex */, chunk);

            /*
             * Assign tuples to output buffers based on those splits.
             */
            for (Split split : splits) {

                addToOutputBuffer((L) split.pmd, chunk, split.fromIndex,
                        split.toIndex, reopen);

            }

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
        protected void writeData(KVO<O>[] chunk) throws Exception {
            
        }
        
        @Override
        protected boolean handleChunk(final KVO<O>[] chunk) throws Exception {

            final long begin = System.nanoTime();

            try {
                
                writeData(chunk);
                
            } catch (MockStaleLocatorException ex) {

                log.warn("Stale locator: "+ex);

                handleRedirect(chunk, ex);

                // done.
                return true;
                
            }
            
            final long elapsed = System.nanoTime() - begin;
            
            synchronized (master.stats) {

                master.stats.chunksOut++;
                master.stats.elementsOut += chunk.length;
                master.stats.elapsedSinkChunkWritingNanos += elapsed;

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

    final H masterStats = new H();

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
