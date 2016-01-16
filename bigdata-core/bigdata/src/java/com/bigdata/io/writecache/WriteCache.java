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
 * Created on Feb 10, 2010
 */

package com.bigdata.io.writecache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.counters.CounterSet;
import com.bigdata.ha.msg.HAWriteMessage;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.compression.CompressorRegistry;
import com.bigdata.io.compression.IRecordCompressor;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.Bytes;
import com.bigdata.util.ChecksumError;

/**
 * This class provides a write cache with read-through for NIO writes on a
 * {@link FileChannel} (and potentially on a remote service). This class is
 * designed to maximize the opportunity for efficient NIO by combining many
 * writes onto a single direct {@link ByteBuffer} and then efficiently
 * transferring those writes onto the backing channel in a channel dependent
 * manner. In general, there are three use cases for a {@link WriteCache}:
 * <ol>
 * <li>Gathered writes. This case is used by the {@link RWStore}.</li>
 * <li>Pure append of sequentially allocated records. This case is used by the
 * {@link WORMStrategy} (WORM) and by the {@link IndexSegmentBuilder}.</li>
 * <li>Write of a single large buffer owned by the caller. This case may be used
 * when the caller wants to manage the buffers or when the caller's buffer is
 * larger than the write cache.</li>
 * </ol>
 * The caller is responsible for managing which buffers are being written on and
 * read on, when they are flushed, and when they are reset. It is perfectly
 * reasonable to have more than one {@link WriteCache} and to read through on
 * any {@link WriteCache} until it has been recycled. A {@link WriteCache} must
 * be reset before it is put into play again for new writes.
 * <p>
 * Note: For an append-only model (WORM), the caller MUST serialize writes onto
 * the {@link IRawStore} and the {@link WriteCache}. This is required in order
 * to ensure that the records are laid out in a dense linear fashion on the
 * {@link WriteCache} and permits the backing buffer to be transferred in a
 * single IO to the backing file.
 * <p>
 * Note: For a {@link RWStore}, the caller must take more responsibility for
 * managing the {@link WriteCache}(s) which are in play and scheduling their
 * eviction onto the backing store. The caller can track the space remaining in
 * each {@link WriteCache} and decide when to flush a {@link WriteCache} based
 * on that information.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class WriteCache implements IWriteCache {

    protected static final Logger log = Logger.getLogger(WriteCache.class);

    /**
     * <code>true</code> iff per-record checksums are being maintained.
     */
    private final boolean useChecksum;

    /**
     * <code>true</code> iff the buffer contents directly contain the recordMap data.
     */
    private final boolean prefixWrites;

    /**
     * The size of the header for a prefix write.
     */
    static final int SIZEOF_PREFIX_WRITE_METADATA = 8/* offset */+ 4/* size */+ 4/* latchedAddr */;
    static final int PREFIX_OFFSET_POS = 0;
    static final int PREFIX_SIZE_POS = 8;
    
    /**
     * The buffer used to absorb writes that are destined for some channel.
     * <p>
     * Note: This is an {@link AtomicReference} since we want to clear this
     * field in {@link #close()}.
     */
    final private AtomicReference<IBufferAccess> buf;

    /**
     * The read lock allows concurrent {@link #acquire()}s and permits both
     * reads and writes on the acquired buffer, while the write lock prevents
     * {@link #acquire()} during critical sections such as
     * {@link #flush(boolean, long, TimeUnit)}, {@link #reset()},
     * {@link #close()}.
     */
    final private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Return the backing {@link ByteBuffer}. The caller may read or write on
     * the buffer, but MUST NOT have a side effect on the
     * {@link ByteBuffer#position()} without first synchronizing on the
     * {@link ByteBuffer}. Once they are done, the caller MUST call
     * {@link #release()}.
     * <p>
     * Note: This uses the read lock to allow concurrent read/write operations
     * on the backing buffer.
     * <p>
     * Note: <strong>At most one write operation may execute concurrently in
     * order to avoid side effects on the buffers position when copying data
     * onto the buffer. This constraint must be imposed by the caller using a
     * <code>synchronized(buf){}</code> block during the critical sections where
     * the buffer position will be updated by a write. </strong>
     * 
     * @return The {@link ByteBuffer}.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             if the {@link WriteCache} is closed.
     */
    private ByteBuffer acquire() throws InterruptedException, IllegalStateException {

        final Lock readLock = lock.readLock();

        readLock.lockInterruptibly();

        try {

            // latch.inc();

            final IBufferAccess tmp = buf.get();

            if (tmp == null) {

                // latch.dec();

                throw new IllegalStateException();

            }

            // Note: The ReadLock is still held!
            return tmp.buffer();

        } catch (Throwable t) {

            // Release the lock only on the error path.
            readLock.unlock();

            if (t instanceof InterruptedException)
                throw (InterruptedException) t;

            if (t instanceof IllegalStateException)
                throw (IllegalStateException) t;

            throw new RuntimeException(t);

        }

    }

    /**
     * Release the read lock on an acquired {@link ByteBuffer}.
     */
    private void release() {

        lock.readLock().unlock();

        // latch.dec();

    }

    /**
     * Return a read-only view of the backing {@link ByteBuffer}.
     * 
     * @return The read-only view -or- <code>null</code> if the
     *         {@link WriteCache} has been closed.
     */
    ByteBuffer peek() {

        final ByteBuffer b = buf.get().buffer();

        return b == null ? null : b.asReadOnlyBuffer();

    }

    // /**
    // * Return the buffer. No other thread will have access to the buffer. No
    // * latch is established and there is no protocol for releasing the buffer
    // * back. Instead, the buffer will become available again if the caller
    // * releases the write lock.
    // *
    // * @throws IllegalMonitorStateException
    // * unless the caller is holding the write lock.
    // * @throws IllegalStateException
    // * if the buffer reference has been cleared.
    // */
    // protected ByteBuffer getExclusiveBuffer() {
    //
    // if (!lock.writeLock().isHeldByCurrentThread())
    // throw new IllegalMonitorStateException();
    //
    // final ByteBuffer tmp = buf.get();
    //
    // if (tmp == null)
    // throw new IllegalStateException();
    //
    // return tmp;
    //        
    // }

    /**
     * Lock used to make
     * {@link #transferTo(WriteCache, WriteCache, ConcurrentMap)} mutex with
     * {@link WriteCacheService#clearWrite(long, int)} for a specific
     * {@link WriteCache} instance.
     */
    // Note: Exposed to WriteCacheService.clearWrite().
    final /*private*/ ReentrantLock transferLock = new ReentrantLock();
    
    /**
     * The metadata associated with a record in the {@link WriteCache}.
     */
    public static class RecordMetadata {

        /**
         * The offset of the record in the file. The offset may be relative to a
         * base offset known to the writeOnChannel() implementation.
         */
        public final long fileOffset;

        /**
         * The offset within the {@link WriteCache}'s backing {@link ByteBuffer}
         * of the start of the record.
         */
        public final int bufferOffset;

        /**
         * The length of the record in bytes as it will be written on the
         * channel. If checksums are being written, then the length of the
         * record has already been incorporated into this value.
         */
        public final int recordLength;

        /**
         * The RWStore latched address for the record. This can be used to
         * recover the FixedAllocator. This field is only required for the
         * RWStore and then only for HA.
         */
        public final int latchedAddr;
        
        /**
         * Set <code>true</code> when the record is deleted.
         * <p>
         * Note: The {@link RecordMetadata} is removed from the
         * {@link WriteCache#recordMap} when the record is deleted. This flag is
         * only visible if the {@link RecordMetadata} was entered onto the
         * {@link WriteCache#orderedRecords} list.
         */
        private volatile boolean deleted;
        
        /**
         * When a record is used as a read cache then the readCount is
         * maintained as a metric on its access. This could be used to
         * determine eviction/compaction.
         * <p>
         * Note: volatile to guarantee visibility of updates. Might do better
         * with synchronized(this), synchronized(cache), or CAS.
         */
        private volatile int hitCount;
        
        public RecordMetadata(final long fileOffset, final int bufferOffset,
                final int recordLength, final int latchedAddr) {

            this.fileOffset = fileOffset;

            this.bufferOffset = bufferOffset;

            this.recordLength = recordLength;
            
            this.latchedAddr = latchedAddr;

            this.deleted = false;
            
        }

        public String toString() {

            return getClass().getSimpleName() + "{fileOffset=" + fileOffset
                    + ",bufferOffset=" + bufferOffset + ",len=" + recordLength
                    + ",delete=" + deleted + "}";

        }

		final int getHitCount() {
			return hitCount;
		}

    } // class RecordMetadata

    /**
     * An index into the write cache used for read through on the cache. The
     * keys are the file offsets that would be used to read the corresponding
     * record. The values describe the position in buffer where that record is
     * found and the length of the record.
     * <p>
     * Note: Exposed to inner classes.
     */
    final protected ConcurrentMap<Long/* fileOffset */, RecordMetadata> recordMap;

    /**
     * An ordered list of the {@link RecordMetadata} in the order in which those
     * records were created. This is maintained only for HA. It is used to
     * communicate the allocations and deletes to a downstream RWS HA follower.
     * The RWS follower relies on the ordered presentation of the addresses to
     * infer the order in which the allocators were created, the size of the
     * regions managed by those allocators, and the order in which the
     * allocators appear in the allocator list (this is the same as the order of
     * the creation of those allocators).
     * <p>
     * Note: The RWS must have the actual order in which the addresses are
     * created. The actual address allocations are serialized by the RWStore
     * using its allocationLock. Therefore the calls to WriteCache.write() must
     * also be serialized. However, it might be possible for a clear of an
     * address to be concurrent with an allocation (I need to check this with
     * Martyn) - for example, when releasing an allocation context. In any case,
     * it is wise to guard updates to {@link #orderedRecords} both to ensure
     * that the allocation order is maintained and to ensure that the data
     * structure remains consistent (since it can be updated by multiple
     * threads).
     * <p>
     * Note: This data structure is guarded by the object monitor for the
     * {@link ByteBuffer}. (This is the same thing that is used to serialize the
     * writes on the {@link ByteBuffer}). Make sure that you are using the
     * {@link ByteBuffer} and not a dup() of that {@link ByteBuffer}.
     */
    final private List<RecordMetadata> orderedRecords;
    
    /**
     * The offset of the first record written onto the {@link WriteCache}. This
     * information is used when {@link #appendOnly} is <code>true</code> as it
     * gives the starting offset at which the entire {@link ByteBuffer} may be
     * written in a single IO. When {@link #appendOnly} is <code>false</code>
     * this is basically meaningless. This is initialized to <code>-1L</code> as
     * a clear indicator that there is no valid record written yet onto the
     * cache.
     */
    final private AtomicLong firstOffset = new AtomicLong(-1L);

    /**
     * Exposed to the WORM for HA support.
     * 
     * @param firstOffset
     *            The first offset (from the HA message).
     */
    protected void setFirstOffset(final long firstOffset) {

        this.firstOffset.set(firstOffset);
        
    }
    
    /**
     * The capacity of the backing buffer.
     */
    final private int capacity;

    /**
     * When <code>true</code> {@link #close()} will release the
     * {@link ByteBuffer} back to the {@link DirectBufferPool}.
     */
    final private boolean releaseBuffer;

    /**
     * A private instance used to compute the checksum of all data in the
     * current {@link #buf}. This is enabled for the high availability write
     * replication pipeline. The checksum over the entire {@link #buf} is
     * necessary in this context to ensure that the receiver can verify the
     * contents of the {@link #buf}. The per-record checksums CAN NOT be used
     * for this purpose since large records may be broken across
     */
    final private ChecksumHelper checker;

    /**
     * The then current extent of the backing file as of the last record written
     * onto the cache before it was written onto the write replication pipeline.
     * The receiver is responsible for adjusting its local file size to match.
     * 
     * @see WriteCacheService#setExtent(long)
     */
    private final AtomicLong fileExtent = new AtomicLong();

    /**
     * m_closedForWrites is set when the buffer is about to be flushed and ensures that
     * nothing will be appended to the buffer until it is reset for reuse.  This
     * fixes a problem in the HA Pipeline where deletes could append to the buffer resulting
     * in a reported buffer length in the HAMessage greater than the data sent.
     */
    private volatile boolean m_closedForWrites = false;

//    /**
//     * The sequence must be set when the cache is ready to be flushed.  In HA this
//     * is sent down the pipeline to ensure correct synchronization when processing
//     * logged messages.
//     */
//    private long sequence = -1;
//
//    /**
//     * The sequence #of this {@link WriteCache} block within the current write
//     * set (origin ZERO(0)). This must be set when the cache is ready to be
//     * flushed. In HA this is sent down the pipeline to ensure correct
//     * synchronization when processing logged messages. This also winds up in
//     * the {@link IRootBlockView} as a summary of the #of {@link WriteCache}
//     * blocks transmitted during the write set for a specific commit point.
//     */
//    void setSequence(final long i) {
//        sequence = i;
//    }
//
//    /**
//     * The sequence #of this {@link WriteCache} block within the current write
//     * set (origin ZERO(0)).
//     */
//    long getSequence() {
//        return sequence;
//    }

    /**
     * Create a {@link WriteCache} from either a caller supplied buffer or a
     * direct {@link ByteBuffer} allocated from the {@link DirectBufferPool}.
     * <p>
     * Note: The application MUST ensure that it {@link #close()}s the
     * {@link WriteCache} or it can leak direct {@link ByteBuffer}s!
     * <p>
     * Note: NIO operations are performed using a direct {@link ByteBuffer}
     * (that is, one use backing bytes are allocated on the C heap). When the
     * caller supplies a {@link ByteBuffer} that is allocated on the Java heap
     * as opposed to in native memory, a temporary direct {@link ByteBuffer}
     * will be allocated for the IO operation by Java. The JVM can fail to
     * release this temporary direct {@link ByteBuffer}, resulting in a memory
     * leak. For this reason, the {@link WriteCache} SHOULD use a direct
     * {@link ByteBuffer}.
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=8f
     *      ab76d1d4479fffffffffa5abfb09c719a30?bug_id=6210541
     * 
     * @param buf
     *            A {@link ByteBuffer} to be used as the write cache (optional).
     *            When <code>null</code> a buffer will be allocated for you from
     *            the {@link DirectBufferPool}. Buffers allocated on your behalf
     *            will be automatically released by {@link #close()}.
     * @param prefixWrites
     *            <code>true</code> iff the implementation uses scattered
     *            writes. The RW store uses scattered writes since its updates
     *            are written to different parts of the backing file. The WORM
     *            store does not since all updates are written to the end of the
     *            user extent in the backing file.
     * @param useChecksum
     *            <code>true</code> iff the write cache will store the caller's
     *            checksum for a record and validate it on read.
     * @param isHighlyAvailable
     *            when <code>true</code> the whole record checksum is maintained
     *            for use when replicating the write cache along the write
     *            pipeline.  This needs to be <code>true</code> for HA1 as well
     *            since we need to write the HALog.
     * @param bufferHasData
     *            when <code>true</code> the caller asserts that the buffer has
     *            data (from a replicated write), in which case the position
     *            should be the start of the data in the buffer and the limit
     *            the #of bytes with valid data. when <code>false</code>, the
     *            caller's buffer will be cleared. The code presumes that the
     *            {@link WriteCache} instance will be used to lay down a single
     *            buffer worth of data onto the backing file.
     *  @param fileExtent
     *            The then current extent of the backing file.
     *
     * @throws InterruptedException
     */
    public WriteCache(IBufferAccess buf, final boolean prefixWrites,
            final boolean useChecksum, final boolean isHighlyAvailable,
            final boolean bufferHasData, final long fileExtent)
            throws InterruptedException {

        if (bufferHasData && buf == null)
            throw new IllegalArgumentException();

        if (buf == null) {

            buf = DirectBufferPool.INSTANCE.acquire();

            this.releaseBuffer = true;

        } else {

            this.releaseBuffer = false;

        }

        // if (quorumManager == null)
        // throw new IllegalArgumentException();

        // this.quorumManager = quorumManager;

        this.useChecksum = useChecksum;
        this.prefixWrites = prefixWrites;

        if (isHighlyAvailable && !bufferHasData) {
            // Note: No checker if buffer has data.
            checker = new ChecksumHelper();
        } else {
            checker = null;
        }

        // save reference to the write cache.
        this.buf = new AtomicReference<IBufferAccess>(buf);

        // the capacity of the buffer in bytes.
        this.capacity = buf.buffer().capacity();

        // apply the then current file extent.
        this.fileExtent.set(fileExtent);

        /*
         * Discard anything in the buffer, resetting the position to zero, the
         * mark to zero, and the limit to the capacity.
         */
        if (!bufferHasData) {
            buf.buffer().clear();
        }

        /*
         * An estimate of the #of records that might fit within the write cache.
         * This is based on an assumption that the "average" record is 1k. This
         * is used solely to assign the initial capacity of this map.
         */
        final int indexDefaultCapacity = capacity / (1 * Bytes.kilobyte32);

        /*
         * allocate and initialize the write cache index.
         * 
         * For scattered writes we choose to use a sorted map so that we can
         * easily flush writes to the file channel in order. This may not be
         * important depending on the caching strategy of the underlying system
         * but it cannot be a bad thing.
         * 
         * If we do not need to support scattered writes then we have the option
         * to use the ConcurrentHashMap which has the advantage of constant
         * access time for read through support.
         * 
         * Note: some literature indicates the ConcurrentSkipListMap scales
         * better with concurrency, so we should benchmark this option for
         * non-scattered writes as well.
         */
        if (prefixWrites) {
            recordMap = new ConcurrentSkipListMap<Long, RecordMetadata>();
        } else {
            recordMap = new ConcurrentHashMap<Long, RecordMetadata>(indexDefaultCapacity);
        }

        if (isHighlyAvailable && !bufferHasData) {

            /*
             * Only in HA mode, and not when we are processing a raw write cache
             * buffer replicated from the leader.
             */
            
            orderedRecords = new LinkedList<WriteCache.RecordMetadata>();
            
        } else {
            
            orderedRecords = null;
            
        }
        
        if (bufferHasData) {
            /*
             * Populate the record map from the record.
             */
            resetRecordMapFromBuffer();
        }
        
    }

    /**
     * Adds some debugging information.
     */
    public String toString() {

        return super.toString()//
                + "{recordCount=" + recordMap.size()//
                + ",firstOffset=" + firstOffset//
                + ",releaseBuffer=" + releaseBuffer//
                + ",prefixWrites=" + prefixWrites//
                + ",useChecksum=" + useChecksum//
                + ",bytesWritten=" + bytesWritten()//
                + ",bytesRemaining=" + remaining()//
                + ",bytesRemoved=" + m_removed//
                + "}";

    }

    /**
     * The offset of the first record written onto the {@link WriteCache}. This
     * information is used when {@link #appendOnly} is <code>true</code> as it
     * gives the starting offset at which the entire {@link ByteBuffer} may be
     * written in a single IO. When {@link #appendOnly} is <code>false</code>
     * this is basically meaningless.
     * <p>
     * Note: This has been raised into the
     * {@link #writeOnChannel(ByteBuffer, long, Map, long)} method signature. It
     * has been reduced to a package private method so it will remain visible to
     * the unit tests, otherwise it could become private.
     * 
     * @return The first offset written into the {@link WriteCache} since it was
     *         last {@link #reset()} and <code>-1L</code> if nothing has been
     *         written since the {@link WriteCache} was created or was last
     *         {@link #reset()}.
     */
    final long getFirstOffset() {

        return firstOffset.get();

    }

    /**
     * The maximum length of a record which could be inserted into the buffer.
     * <p>
     * Note: When checksums are enabled, this is 4 bytes less than the actual
     * capacity of the underlying buffer since each record requires an
     * additional four bytes for the checksum field.
     */
    final public int capacity() {

        return capacity - (useChecksum ? 4 : 0) - (prefixWrites ? SIZEOF_PREFIX_WRITE_METADATA : 0);

    }

    /**
     * Return the #of bytes remaining in the buffer.
     * <p>
     * Note: in order to rely on this value the caller MUST have exclusive
     * access to the buffer. This API does not provide the means for acquiring
     * that exclusive access. This is something that the caller has to arrange
     * for themselves, which is why this is a package private method.
     */
    final int remaining() {

        final int remaining = capacity - bytesWritten();//buf.get().buffer().position();

        return remaining;

    }

    /**
     * The #of bytes written on the backing buffer.
     * <p>
     * Note: in order to rely on this value the caller MUST have exclusive
     * access to the buffer. This API does not provide the means for acquiring
     * that exclusive access. This is something that the caller has to arrange
     * for themselves, which is why this is a package private method.
     */
    public final int bytesWritten() {

        return buf.get().buffer().position();

    }

    /**
     * Return <code>true</code> if there are no records buffered on the cache.
     * Note: The caller MUST be holding a lock for this to be value. Probably
     * the write lock.
     * 
     * @todo This currently tests the {@link #recordMap}. In fact, for at least
     *       the {@link RWStore} the record map COULD be empty with cleared
     *       writes on the backing {@link ByteBuffer}. Therefore this tests
     *       whether the {@link WriteCache} has data to be written but does not
     *       clearly report whether or not some data has been written onto the
     *       buffer (and hence it has fewer bytes remaining than might otherwise
     *       be expected).
     */
    final boolean isEmpty() {

        return recordMap.isEmpty();

    }

    /**
     * Set the current extent of the backing file on the {@link WriteCache}
     * object. When used as part of an HA write pipeline, the receiver is
     * responsible for adjusting its local file size to match the file extent in
     * each {@link WriteCache} message.
     * 
     * @param fileExtent
     *            The current extent of the file.
     * 
     * @throws IllegalArgumentException
     *             if the file extent is negative.
     * 
     * @see WriteCacheService#setExtent(long)
     */
    public void setFileExtent(final long fileExtent) {

        if (fileExtent < 0L)
            throw new IllegalArgumentException();

        this.fileExtent.set(fileExtent);

    }

    public long getFileExtent() {

        return fileExtent.get();

    }

    /**
     * Return the checksum of all data written into the backing buffer for this
     * {@link WriteCache} instance since it was last {@link #reset()}.
     * 
     * @return The running checksum of the data written into the backing buffer.
     * 
     * @throws UnsupportedOperationException
     *             if the {@link WriteCache} is not maintaining this checksum
     *             (i.e., if <code>isHighlyAvailable := false</code> was
     *             specified to the constructor).
     */
    // package private : exposed to WriteTask.call().
//    int getWholeBufferChecksum(final ByteBuffer checksumBuffer) {
//
//        final ByteBuffer src = peek().duplicate();
//        // flip(limit=pos;pos=0)
//        src.flip();
//
//        return getWholeBufferChecksum(checksumBuffer, src, false);
//
//    }

    int getWholeBufferChecksum(final ByteBuffer checksumBuffer, final ByteBuffer src, final boolean isCompressed) {

        if (checker == null)
            throw new UnsupportedOperationException();
        
        if (isCompressed || prefixWrites) {
            /*
             * Recalculate whole buffer checksum.
             * 
             * Note: When using prefix writes, we mark deleted records by
             * flipping the sign on the fileOffset in the pre-record header.
             * This means that we can not use an incrementally computed
             * checksum.
             * 
             * Note: With the introduction of HALog compression (compress /
             * expand), the target ByteBuffer may be sized for the message
             * rather than drawn from a pool. Therefore, the assert has been
             * modified such to ensure that the buffer has sufficient capacity
             * for the transfer - as defined by limit().
             */

            assert checksumBuffer.capacity() >= src.limit() : "b.limit="
                    + src.limit() + ", checksumBuffer.capacity="
                    + checksumBuffer.capacity();
//            assert checksumBuffer.capacity() == src.capacity() : "b.capacity="
//                    + src.capacity() + ", checksumBuffer.capacity="
//                    + checksumBuffer.capacity();

            // checksumBuffer.limit(checksumBuffer.capacity());
            checksumBuffer.limit(src.limit());
            checksumBuffer.position(0);
            checksumBuffer.put(src);
            checksumBuffer.flip();

            checker.reset();
            checker.checksum(checksumBuffer);
        }

        return checker.getChecksum();

    }

    /**
     * {@inheritDoc}
     * 
     * @throws IllegalStateException
     *             If the buffer is closed.
     * @throws IllegalArgumentException
     *             If the caller's record is larger than the maximum capacity of
     *             cache (the record could not fit within the cache). The caller
     *             should check for this and provide special handling for such
     *             large records. For example, they can be written directly onto
     *             the backing channel.
     */
    public boolean write(final long offset, final ByteBuffer data, final int chk) throws InterruptedException {

        return write(offset, data, chk, true/* writeChecksum */,0/*latchedAddr*/);

    }

    /**
     * 
     * @param offset
     * @param data
     * @param chk
     * @param writeChecksum
     *            The checksum is appended to the record IFF this argument is
     *            <code>true</code> and checksums are in use.
     * @return
     * @throws InterruptedException
     */
    boolean write(final long offset, final ByteBuffer data, final int chk, boolean writeChecksum, final int latchedAddr)
            throws InterruptedException {

        // Note: The offset MAY be zero. This allows for stores without any
        // header block.

        assert !m_closedForWrites;

        if (m_written) { // should be clean, NO WAY should this be written to!
            log.error("Writing to CLEAN cache: " + hashCode());
            throw new IllegalStateException("Writing to CLEAN cache: " + hashCode());
        }

        if (data == null)
            throw new IllegalArgumentException(AbstractBufferStrategy.ERR_BUFFER_NULL);

        final WriteCacheCounters counters = this.counters.get();

        final ByteBuffer tmp = acquire();

        try {

            final int remaining = data.remaining();

            // The #of bytes to transfer into the write cache.
            final int datalen = remaining + (writeChecksum && useChecksum ? 4 : 0);
            final int nwrite = datalen + (prefixWrites ? SIZEOF_PREFIX_WRITE_METADATA : 0);

            if (nwrite > capacity) {
                // This is more bytes than the total capacity of the buffer.
                throw new IllegalArgumentException(AbstractBufferStrategy.ERR_BUFFER_OVERRUN);

            }

            if (remaining == 0)
                throw new IllegalArgumentException(AbstractBufferStrategy.ERR_BUFFER_EMPTY);

            /*
             * Note: We need to be synchronized on the ByteBuffer here since
             * this operation relies on the position() being stable.
             * 
             * Note: Also see clearAddrMap(long) which is synchronized on the
             * acquired ByteBuffer in the same manner to protect it during
             * critical sections which have a side effect on the buffer
             * position.
             */
            final int pos;
            synchronized (tmp) {

                // the position() at which the record is cached in the buffer.
                final int spos = tmp.position();

                if (spos + nwrite > capacity) {

                    /*
                     * There is not enough room left in the write cache for this
                     * record.
                     */

                    return false;

                }

                // add prefix data if required and set data position in buffer
                if (prefixWrites) {
                    tmp.putLong(offset);
                    tmp.putInt(datalen);
                    tmp.putInt(latchedAddr);
                    pos = spos + SIZEOF_PREFIX_WRITE_METADATA;
                } else {
                    pos = spos;
                }

                tmp.put(data);

                /*
                 * Copy the record into the cache, updating position() as we go.
                 * 
                 * Note that the checker must be invalidated if a RWCache
                 * "deletes" an entry by zeroing an address. Hence, the code no
                 * longer updates the checksum when [prefixWrites:=true].
                 */
                if (checker != null && !prefixWrites) {
                    // update the checksum (no side-effects on [data])
                    final ByteBuffer chkBuf = tmp.asReadOnlyBuffer();
                    chkBuf.position(spos);
                    chkBuf.limit(tmp.position());
                    checker.update(chkBuf);
                }

                // write checksum - if any
                if (writeChecksum && useChecksum) {
                    tmp.putInt(chk);
                    if (checker != null && !prefixWrites) {
                        // update the running checksum to include this too.
                        checker.update(chk);
                    }
                }

                // set while synchronized since no contention.
                firstOffset.compareAndSet(-1L/* expect */, offset/* update */);

                // update counters while holding the lock.
                counters.naccept++;
                counters.bytesAccepted += nwrite;

                /*
                 * Add metadata for the record so it can be read back from the
                 * cache.
                 */

                final RecordMetadata md = new RecordMetadata(offset, pos,
                        datalen, latchedAddr);

                if (recordMap.put(Long.valueOf(offset), md) != null) {
                    /*
                     * Note: This exception indicates that the abort protocol
                     * did not reset() the current write cache before new writes
                     * were laid down onto the buffer.
                     */
                    throw new AssertionError(
                            "Record exists for offset in cache: offset="
                                    + offset);
                }

                if (orderedRecords != null) {

                    /*
                     * Note: insert into this collection is guarded by the
                     * object monitor for the ByteBuffer. This ensures that the
                     * LinkedList data structure remains coherent when it is
                     * updated by multiple threads. It also ensures that the
                     * order of this list is the same as the ordinal position
                     * order assigned within the ByteBuffer
                     * 
                     * Note: The real necessary ordering is the allocation
                     * ordering - any address allocation before another address
                     * MUST appear in the list before that other address. Since
                     * some addresses are recycled while others are newly
                     * allocated the latchedAddr values are not strictly
                     * ascending.
                     */
           
                    orderedRecords.add(md);

                }

            } // synchronized(tmp)

            if (log.isTraceEnabled()) { // @todo rather than hashCode() set a
                                        // buffer# on each WriteCache instance.
                log.trace("offset=" + offset + ", pos=" + pos + ", nwrite=" + nwrite + ", writeChecksum="
                        + writeChecksum + ", useChecksum=" + useChecksum + ", nrecords=" + recordMap.size()
                        + ", hashCode=" + hashCode());
            }

            return true;

        } finally {

            release();

        }

    }

    /**
     * {@inheritDoc}
     * 
     * @throws IllegalStateException
     *             If the buffer is closed.
     */
    public ByteBuffer read(final long offset, final int nbytes) throws InterruptedException, ChecksumError {

        final WriteCacheCounters counters = this.counters.get();

        // takes readLock returning buffer
        final ByteBuffer tmp = acquire();

        try {

            // Look up the metadata for that record in the cache.
            final RecordMetadata md;
            if ((md = recordMap.get(offset)) == null) {

                // The record is not in this write cache.
                counters.nmiss.increment();

                return null;
            }

            // length of the record w/o checksum field.
            final int reclen = md.recordLength - (useChecksum ? 4 : 0);

            // the start of the record in writeCache.
            final int pos = md.bufferOffset;

            // create a view with same offset, limit and position.
            final ByteBuffer view = tmp.duplicate();

            // adjust the view to just the record of interest.
            view.limit(pos + reclen);
            view.position(pos);

            // System.out.println("WriteCache, addr: " + offset + ", from: " +
            // pos + ", " + md.recordLength + ", thread: " +
            // Thread.currentThread().getId());
            /*
             * Copy the data into a newly allocated buffer. This is necessary
             * because our hold on the backing ByteBuffer for the WriteCache is
             * only momentary. As soon as we release() the buffer the data in
             * the buffer could be changed.
             */

            final byte[] b = new byte[reclen];

            final ByteBuffer dst = ByteBuffer.wrap(b);

            // copy the data into [dst] (and the backing byte[]).
            dst.put(view);

            // flip buffer for reading.
            dst.flip();

            if (useChecksum && !(this instanceof ReadCache)) { // don't check if HIRS

                final int chk = tmp.getInt(pos + reclen);

                if (chk != ChecksumUtility.threadChk.get().checksum(b, 0/* offset */, reclen)) {

                    // Note: [offset] is a (possibly relative) file offset.
                    throw new ChecksumError(checkdata());

                }

            }

            counters.nhit.increment();

            if (log.isTraceEnabled()) {
                log.trace(show(dst, "read bytes"));
            }
            
            // Increment cache read count
            final int nhits = ++md.hitCount;

			if (log.isTraceEnabled()) {
				if (nhits > 2) {
					log.trace("Cache read ");
				}
			}

			return dst;

		} finally {

			release();

		}

    }

    /**
     * Dump some metadata and leading bytes from the buffer onto a
     * {@link String}.
     * 
     * @param buf
     *            The buffer.
     * @param prefix
     *            A prefix for the dump.
     * 
     * @return The {@link String}.
     */
    private String show(final ByteBuffer buf, final String prefix) {
        final StringBuffer str = new StringBuffer();
        int tpos = buf.position();
        if (tpos == 0) {
            tpos = buf.limit();
        }
        str.append(prefix + ", length: " + tpos + " : ");
        for (int tb = 0; tb < tpos && tb < 20; tb++) {
            str.append(Integer.toString(buf.get(tb)) + ",");
        }
        // log.trace(str.toString());
        return str.toString();
    }

    // private String show(final byte[] buf, int len, final String prefix) {
    // final StringBuffer str = new StringBuffer();
    // str.append(prefix + ": ");
    // int tpos = len;
    // str.append(prefix + ", length: " + tpos + " : ");
    // for (int tb = 0; tb < tpos && tb < 20; tb++) {
    // str.append(Integer.toString(buf[tb]) + ",");
    // }
    // // log.trace(str.toString());
    // return str.toString();
    // }

    /**
     * Flush the writes to the backing channel but DOES NOT sync the channel and
     * DOES NOT {@link #reset()} the {@link WriteCache}. {@link #reset()} is a
     * separate operation because a common use is to retain recently flushed
     * instances for read-back.
     * 
     * @param force
     *            When <code>true</code>, the data will be forced to stable
     *            media.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void flush(final boolean force) throws IOException, InterruptedException {

        try {

            if (!flush(force, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {

                throw new RuntimeException();

            }

        } catch (TimeoutException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * Flush the writes to the backing channel but DOES NOT sync the channel and
     * DOES NOT {@link #reset()} the {@link WriteCache}. {@link #reset()} is a
     * separate operation because a common use is to retain recently flushed
     * instances for read-back.
     * 
     * @param force
     *            When <code>true</code>, the data will be forced to stable
     *            media.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public boolean flush(final boolean force, final long timeout,
            final TimeUnit unit) throws IOException, TimeoutException,
            InterruptedException {

        if(!m_closedForWrites)
            closeForWrites();

        // start time
        final long begin = System.nanoTime();

        // total nanoseconds to wait.
        final long nanos = unit.toNanos(timeout);

        // remaining nanoseconds to wait.
        long remaining = nanos;

//        final WriteCacheCounters counters = this.counters.get();

        final Lock writeLock = lock.writeLock();

        if (!writeLock.tryLock(remaining, TimeUnit.NANOSECONDS)) {

            return false;

        }

        try {

            final ByteBuffer tmp = this.buf.get().buffer();

            if (tmp == null)
                throw new IllegalStateException();

            // #of bytes to write on the disk.
            final int nbytes = tmp.position();

            if (log.isTraceEnabled())
                log.trace("nbytes=" + nbytes + ", firstOffset="
                        + getFirstOffset());// + ", nflush=" + counters.nflush);

            if (nbytes == 0) {

                // NOP.
                return true;

            }

            /*
             * Create a view with same offset, limit and position.
             * 
             * Note: The writeOnChannel method is given the view. This prevents
             * it from adjusting the position() on the backing buffer.
             */
            {

                final ByteBuffer view = tmp.duplicate();

                // adjust the view to just the dirty record.
                view.limit(nbytes);
                view.position(0);

                // remaining := (total - elapsed).
                remaining = nanos - (System.nanoTime() - begin);

                // write the data on the disk file.
                final boolean ret = writeOnChannel(view, getFirstOffset(),
                        Collections.unmodifiableMap(recordMap), remaining);
                
                if (!ret) {
                    throw new TimeoutException("Unable to flush WriteCache");
                }

//                counters.nflush++;

                return ret;

            }

        } finally {

            writeLock.unlock();

        }

    }

    /**
     * Debug routine logs @ ERROR additional information when a checksum error
     * has been encountered.
     * 
     * @return An informative error message.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    private String checkdata() throws IllegalStateException, InterruptedException {
        
        if (!useChecksum) {
            return "Unable to check since checksums are not enabled";
        }
        
        ByteBuffer tmp = acquire();
        try {
            int nerrors = 0;
            int nrecords = recordMap.size();
            
            for (Entry<Long, RecordMetadata> ent : recordMap.entrySet()) {
                
                final RecordMetadata md = ent.getValue();
                
                // length of the record w/o checksum field.
                final int reclen = md.recordLength - 4;

                // the start of the record in writeCache.
                final int pos = md.bufferOffset;
                
                final int chk = tmp.getInt(pos + reclen);

                // create a view with same offset, limit and position.
                final ByteBuffer view = tmp.duplicate();

                // adjust the view to just the record of interest.
                view.limit(pos + reclen);
                view.position(pos);
                
                final byte[] b = new byte[reclen];

                final ByteBuffer dst = ByteBuffer.wrap(b);

                // copy the data into [dst] (and the backing byte[]).
                dst.put(view);
                if (chk != ChecksumUtility.threadChk.get().checksum(b, 0/* offset */, reclen)) {
                    log.error("Bad data for address: " + ent.getKey());
                    nerrors++;
                }

            }
            return "WriteCache checkdata - records: " + nrecords + ", errors: " + nerrors;  
        } finally {
            release();
        }
    }
    
    /**
     * Write the data from the buffer onto the channel. This method provides a
     * uniform means to request that the buffer write itself onto the backing
     * channel, regardless of whether the channel is backed by a file, a socket,
     * etc.
     * <p>
     * Implementations of this method MAY support gathered writes, depending on
     * the channel. The necessary information to perform a gathered write is
     * present in the <i>recordMap</i>. On the other hand, the implementation
     * MAY require that the records in the cache are laid out for a WORM, in
     * which case {@link #getFirstOffset()} provides the starting offset for the
     * data to be written. The application MUST coordinate the requirements for
     * a R/W or WORM store with the use of the {@link WriteCache} and the means
     * to write on the backing channel.
     * 
     * @param buf
     *            The data to be written. Only the dirty bytes are visible in
     *            this view. The implementation should write all bytes from the
     *            current position to the limit.
     * @param firstOffset
     *            The offset of the first record in the recordMap into the file
     *            (may be relative to a base offset within the file). This is
     *            provided as an optimization for the WORM which writes its
     *            records contiguously on the backing store.
     * @param recordMap
     *            The mapping of record offsets onto metadata about those
     *            records.
     * @param nanos
     *            The timeout for the operation in nanoseconds.
     * 
     * @return <code>true</code> if the operation was completed successfully
     *         within the time alloted.
     * 
     * @throws InterruptedException
     *             if the thread was interrupted.
     * @throws IOException
     *             if there was an IO problem.
     */
    abstract protected boolean writeOnChannel(final ByteBuffer buf, final long firstOffset,
            final Map<Long, RecordMetadata> recordMap, final long nanos) throws InterruptedException, TimeoutException,
            IOException;

    /**
     * {@inheritDoc}.
     * <p>
     * This implementation clears the buffer, the record map, and other internal
     * metadata such that the {@link WriteCache} is prepared to receive new
     * writes.
     * 
     * @throws IllegalStateException
     *             if the write cache is closed.
     */
    public void reset() throws InterruptedException {

        final Lock writeLock = lock.writeLock();

        writeLock.lockInterruptibly();

        try {

            // // wait until there are no readers using the buffer.
            // latch.await();

            final ByteBuffer tmp = buf.get().buffer();

            if (tmp == null) {

                // Already closed.
                throw new IllegalStateException();

            }

            // reset all state.
            _resetState(tmp);

        } finally {

            writeLock.unlock();

        }

    }

    /**
     * Permanently take the {@link WriteCache} instance out of service. If the
     * buffer was allocated by the {@link WriteCache} then it is released back
     * to the {@link DirectBufferPool}. After this method is called, records can
     * no longer be read from nor written onto the {@link WriteCache}. It is
     * safe to invoke this method more than once.
     * <p>
     * Concurrent {@link #read(long, int)} requests will be serviced if the
     * already hold the the read lock but requests will fail once the
     * 
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {

        final Lock writeLock = lock.writeLock();

        writeLock.lockInterruptibly();

        try {

            // // wait until there are no readers using the buffer.
            // latch.await();

            /*
             * Note: This method is thread safe. Only one thread will manage to
             * clear the AtomicReference and it will do the rest of the work as
             * well.
             */

            // position := 0; limit := capacity.
            final IBufferAccess tmp = buf.get();

            if (tmp == null) {

                // Already closed.
                return;

            }

            if (buf.compareAndSet(tmp/* expected */, null/* update */)) {

                try {

                    _resetState(tmp.buffer());

                } finally {

                    if (releaseBuffer) {

                        tmp.release();

                    }

                }

            }

        } finally {

            writeLock.unlock();

        }

    }

    /**
     * Reset the internal state of the {@link WriteCache} in preparation to
     * reuse it to receive more writes.
     * <p>
     * Note: Keep private unless strong need for override since you can not call
     * this method without holding the write lock
     * 
     * @param tmp
     */
    private void _resetState(final ByteBuffer tmp) {

        if (tmp == null)
            throw new IllegalArgumentException();

        if (!lock.writeLock().isHeldByCurrentThread()) {
            // The caller must be holding the write lock.
            throw new IllegalMonitorStateException();
        }
        
        // clear the index since all records were flushed to disk.
        if (!recordMap.isEmpty())
        	recordMap.clear();
        
        if (orderedRecords != null) {
            synchronized (tmp) {
                orderedRecords.clear();
            }
        }

        // clear to well known invalid offset.
        firstOffset.set(-1L);

        // position := 0; limit := capacity.
        tmp.clear();

        if (checker != null) {

            // reset the running checksum of the data written onto the backing
            // buffer.
            checker.reset();

        }

        // Martyn: I moved your debug flag here so it is always cleared by
        // reset().
        m_written = false;
        
        m_closedForWrites = false;
        
        m_removed = 0;
        
        // leave to WCS to manage referenceCount for cache
        m_referenceCount.set(0);

    }

//    /**
//     * Return the RMI message object that will accompany the payload from the
//     * {@link WriteCache} when it is replicated along the write pipeline.
//     * 
//     * @return cache A {@link WriteCache} to be replicated.
//     */
//    final IHAWriteMessage newHAWriteMessage(//
//            final UUID storeUUID,
//            final long quorumToken,
//            final long lastCommitCounter,//
//            final long lastCommitTime,//
//            final long sequence,
//            final ByteBuffer tmp
//            ) {
//
//        return new HAWriteMessage(//
//                storeUUID,//
//                lastCommitCounter,//
//                lastCommitTime,//
//                sequence, //
//                bytesWritten(), getWholeBufferChecksum(tmp),
//                prefixWrites ? StoreTypeEnum.RW : StoreTypeEnum.WORM,
//                quorumToken, fileExtent.get(), firstOffset.get());
//
//    }
    
    /**
     * Used to retrieve the {@link HAWriteMessage} AND the associated
     * {@link ByteBuffer}.
     * <p>
     * This allows the {@link WriteCache} to compress the data and create the
     * correct {@link HAWriteMessage}.
     */
    static public class HAPackage {

        /**
         * The message as it will be sent.
         */
        private final IHAWriteMessage m_msg;
        /**
         * The data as it will be sent, with compression already applied if
         * compression will be used.
         */
        private final ByteBuffer m_data;

        /**
         * 
         * @param msg
         *            The message as it will be sent.
         * @param data
         *            The data as it will be sent, with compression already
         *            applied if compression will be used.
         */
        HAPackage(final IHAWriteMessage msg, final ByteBuffer data) {
            m_msg = msg;
            m_data = data;
        }

        public IHAWriteMessage getMessage() {
            return m_msg;
        }

        public ByteBuffer getData() {
            return m_data;
        }
    }
    
    /**
     * Return the optional key for the {@link CompressorRegistry} which
     * identifies the {@link IRecordCompressor} to be applied.
     */
    protected String getCompressorKey() {

        // Default is NO compression.
        return null;
        
    }
    
    /**
     * Return the RMI message object plus the payload (the payload has been
     * optionally compressed, depending on the configuration).
     */
    final HAPackage newHAPackage(//
            final UUID storeUUID,//
            final long quorumToken,//
            final long lastCommitCounter,//
            final long lastCommitTime,//
            final long sequence,//
            final int replicationFactor,//
            final ByteBuffer checksumBuffer
            ) {
    	
        final ByteBuffer b = peek().duplicate();
        b.flip();

        final ByteBuffer send;

        final String compressorKey  = getCompressorKey();
        
        final IRecordCompressor compressor = CompressorRegistry.getInstance()
                .get(compressorKey);

        if (compressor != null) {
        
            // Compress current buffer
            send = compressor.compress(b);

        } else {
            
            send = b;
            
        }
    	
    	final int chksum = getWholeBufferChecksum(checksumBuffer, send.duplicate(), b != send /*isCompressed*/);
        final HAWriteMessage msg = new HAWriteMessage(//
                storeUUID,//
                lastCommitCounter,//
                lastCommitTime,//
                sequence, //
                send.limit(), chksum,
                prefixWrites ? StoreTypeEnum.RW : StoreTypeEnum.WORM,
                quorumToken, replicationFactor,
                fileExtent.get(), firstOffset.get(),
                compressorKey);

        if (log.isTraceEnabled()) {
            log.trace("Original buffer: " + b.limit() + ", final buffer: " + send.limit() + ", compressorKey: " + compressorKey + ", checksum: " + chksum);
        }
        
        return new HAPackage(msg, send);
    	
    }

    /**
     * The current performance counters.
     */
    protected final AtomicReference<WriteCacheCounters> counters = new AtomicReference<WriteCacheCounters>(
            new WriteCacheCounters());

    /**
     * Stores the number of bytes removed from this {@link WriteCache}.
     * <p>
     * This can be used to determine whether the {@link WriteCache} should be
     * flushed to disk or compacted to an aggregation buffer, avoiding writes
     * and maximizing the chance of a read cache hit.
     * <p>
     * Note: <code>volatile</code> since not guarded by any lock.
     */
    private volatile int m_removed;

    /**
     * Sets the performance counters to be used by the write cache. A service
     * should do this if you want to aggregate the performance counters across
     * multiple {@link WriteCache} instances.
     * 
     * @param newVal
     *            The shared performance counters.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    void setCounters(final WriteCacheCounters newVal) {

        if (newVal == null)
            return;

        this.counters.set(newVal);

    }

    /**
     * Return the performance counters for the write cacher.
     */
    public CounterSet getCounters() {

        return counters.get().getCounters();

    }

    /**
     * A {@link WriteCache} implementation suitable for an append-only file such
     * as the {@link WORMStrategy} or the output file of the
     * {@link IndexSegmentBuilder}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class FileChannelWriteCache extends WriteCache {

        /**
         * An offset which will be applied to each record written onto the
         * backing {@link FileChannel}. The offset is generally the size of the
         * root blocks for a journal or the checkpoint record for an index
         * segment. It can be zero if you do not have anything at the head of
         * the file.
         * <p>
         * Note: This implies that writing the root blocks is done separately in
         * the protocol since you can't write below this offset otherwise.
         */
        final protected long baseOffset;

        /**
         * Used to re-open the {@link FileChannel} in this class.
         */
        public final IReopenChannel<FileChannel> opener;

        /**
         * @param baseOffset
         *            An offset
         * @param buf
         * @param opener
         * 
         * @throws InterruptedException
         */
        public FileChannelWriteCache(final long baseOffset,
                final IBufferAccess buf, final boolean useChecksum,
                final boolean isHighlyAvailable, final boolean bufferHasData,
                final IReopenChannel<FileChannel> opener,
                final long fileExtent)
                throws InterruptedException {

            super(buf, false/* scatteredWrites */, useChecksum,
                    isHighlyAvailable, bufferHasData, fileExtent);

            if (baseOffset < 0)
                throw new IllegalArgumentException();

            if (opener == null)
                throw new IllegalArgumentException();

            this.baseOffset = baseOffset;

            this.opener = opener;

        }

        @Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffset,
                final Map<Long, RecordMetadata> recordMap, final long nanos)
                throws InterruptedException, IOException {

            final long begin = System.nanoTime();

            final int nbytes = data.remaining();

            /*
             * The position in the file at which the record will be written.
             */
            final long pos = baseOffset + firstOffset;

            /*
             * Write bytes in [data] from position to limit onto the channel.
             * 
             * @todo This ignores the timeout.
             */
            final int nwrites = FileChannelUtility.writeAll(opener, data, pos);

            final WriteCacheCounters counters = this.counters.get();
            counters.nchannelWrite += nwrites;
            counters.bytesWritten += nbytes;
            counters.elapsedWriteNanos += (System.nanoTime() - begin);

            return true;

        }

    }

    /**
     * The scattered write cache is used by the {@link RWStore} since the writes
     * can be made to any part of the file assigned for data allocation.
     * <p>
     * The writeonChannel must therefore utilize the {@link RecordMetadata} to
     * write each update separately.
     * <p>
     * To support HA, we prefix each write with the file position and buffer
     * length in the cache. This enables the cache buffer to be sent as a single
     * stream and the RecordMap rebuilt downstream.
     */
    public static class FileChannelScatteredWriteCache extends WriteCache {

        /**
         * Used to re-open the {@link FileChannel} in this class.
         */
        private final IReopenChannel<FileChannel> opener;

        private final BufferedWrite m_bufferedWrite;
        /**
         * @param baseOffset
         *            An offset
         * @param buf
         * @param opener
         * 
         * @throws InterruptedException
         */
        public FileChannelScatteredWriteCache(final IBufferAccess buf,
                final boolean useChecksum, final boolean isHighlyAvailable,
                final boolean bufferHasData,
                final IReopenChannel<FileChannel> opener,
                final long fileExtent, final BufferedWrite bufferedWrite)
                throws InterruptedException {

            super(buf, true/* scatteredWrites */, useChecksum,
                    isHighlyAvailable, bufferHasData, fileExtent);

            if (opener == null)
                throw new IllegalArgumentException();

            this.opener = opener;
            
            m_bufferedWrite = bufferedWrite;

        }

        /**
         * Called by WriteCacheService to process a direct write for large
         * blocks and also to flush data from dirty caches.
         * 
         * TODO The [nanos] parameter is ignored.
         */
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffsetIgnored,
                final Map<Long, RecordMetadata> recordMap, final long nanos)
                throws InterruptedException, IOException {

            final long begin = System.nanoTime();

            final int nbytes = data.remaining();

            if (m_written) {
                log.warn("DUPLICATE writeOnChannel for : " + this.hashCode());
            } else {
                // Can be empty if reset!
                // assert !this.isEmpty();

                m_written = true;
            }

            /*
             * Retrieve the sorted write iterator and write each block to the
             * file.
             * 
             * If there is a BufferedWrite then ensure it is reset.
             */
            if (m_bufferedWrite != null) {
                m_bufferedWrite.reset();
            }
            
            int nwrites = 0;
            final Iterator<Entry<Long, RecordMetadata>> entries = recordMap.entrySet().iterator();
            while (entries.hasNext()) {

                final Entry<Long, RecordMetadata> entry = entries.next();

                final RecordMetadata md = entry.getValue();

                // create a view on record of interest.
                final ByteBuffer view = data.duplicate();
                final int pos = md.bufferOffset;
                view.limit(pos + md.recordLength);
                view.position(pos);

                final long offset = entry.getKey(); // offset in file to update
                if (m_bufferedWrite == null) {
                    nwrites += FileChannelUtility.writeAll(opener, view, offset);
                } else {
                    nwrites += m_bufferedWrite.write(offset, view, opener);
                }
                // if (log.isInfoEnabled())
                // log.info("writing to: " + offset);
                registerWriteStatus(offset, md.recordLength, 'W');
            }

            if (m_bufferedWrite != null) {
                nwrites += m_bufferedWrite.flush(opener);
                
                if (log.isTraceEnabled())
                    log.trace(m_bufferedWrite.getStats(null, true));
            }

            final WriteCacheCounters counters = this.counters.get();
            counters.nchannelWrite += nwrites;
            counters.bytesWritten += nbytes;
            counters.elapsedWriteNanos += (System.nanoTime() - begin);
            
            if (log.isTraceEnabled())
            	log.trace("WRITTEN ON CHANNEL");

            return true;

        }

        /**
         * Hook to rebuild {@link RecordMetadata} after buffer has been
         * transferred. For the {@link FileChannelScatteredWriteCache} this
         * means hopping trough the buffer marking offsets and data size into
         * the {@link RecordMetadata} map, and ignoring any zero address entries
         * that indicate a "freed" allocation.
         * <p>
         * Update: This has now been changed to avoid problems with incremental
         * checksums by indicating removal by appending a new prefix where the
         * data length is zero.
         * 
         * @throws InterruptedException
         */
        @Override
        public void resetRecordMapFromBuffer(final ByteBuffer buf,
                final Map<Long, RecordMetadata> recordMap) {

            recordMap.clear();
            final int limit = buf.limit(); // end position.
            int pos = buf.position(); // start position
            
            // log.trace("position: " + pos + ", limit: " + limit);
            while (pos < limit) {
                buf.position(pos);
                // 8 bytes (negative iff record is deleted)
                final long fileOffset = buf.getLong(); 
                assert fileOffset != 0L;
                // 4 bytes (negative iff no data follows)
                final int recordLength = buf.getInt();
                assert recordLength != 0;
                // 4 bytes
                final int latchedAddr = buf.getInt();  

                // log.trace("Record fileOffset: " + fileOffset + ", length: " + recordLength + ", latchedAddr: " + latchedAddr);

                //                if (sze == 0 /* old style deleted */) {
//                    /*
//                     * Should only happen if a previous write was already made
//                     * to the buffer but the allocation has since been freed.
//                     */
//                    recordMap.remove(addr);
//                    removeAddress(latchedAddr);
                if (fileOffset < 0 /* new style deleted */) {
                    if (recordMap.get(fileOffset) != null) {
                        // Should have been removed already.
                        throw new AssertionError();
                    }
                    /*
                     * Make sure that the address is declared. This covers the
                     * case where a record is allocated and then recycled before
                     * the WriteCache in which it was recorded is evicted from
                     * the dirtyList. This can happen when we are not
                     * compacting, as well as when we are compacting.
                     * 
                     * Note: RWS will interpret a -recordLength as notification
                     * of the existence of an allocator for that address but
                     * will not create an actual allocation for that address at
                     * this time.
                     */
                    // Ensure allocator exists (allocation may or may not be
                    // created).
                    addAddress(latchedAddr, recordLength);
                    if (recordLength > 0) {
                        // Delete allocation.
                        removeAddress(latchedAddr);
                    }
                } else {
                    /*
                     * Note: Do not enter things into [orderedRecords] on the
                     * follower.
                     */
                    if (recordLength < 0) {
                        /*
                         * Notice of allocation.
                         * 
                         * Note: recordLength is always negative for this code
                         * path. The RWS will interpret the -recordLength as
                         * notification of the existence of an allocator for
                         * that address but will not create an actual allocation
                         * for that address at this time.
                         */
                        addAddress(latchedAddr, recordLength);
                    } else {
                        /*
                         * Actual allocation with data.
                         */
                        final RecordMetadata md = new RecordMetadata(
                                fileOffset, pos + SIZEOF_PREFIX_WRITE_METADATA,
                                recordLength, latchedAddr);
                        recordMap.put(fileOffset, md);
                        addAddress(latchedAddr, recordLength);
                    }
                }
                // skip header (addr + sze + latchedAddr) and data (if any)
                pos += (SIZEOF_PREFIX_WRITE_METADATA + (recordLength > 0 ? recordLength
                        : 0));
            }
        }

        /**
         * A record add has been decoded.
         * 
         * @param latchedAddr
         *            The latched address.
         * @param size
         *            The size of the allocation in bytes.
         */
        protected void addAddress(int latchedAddr, int size) {}

        /**
         * A record delete has been decoded.
         * 
         * @param latchedAddr
         *            The latched address.
         */
        protected void removeAddress(int latchedAddr) {}

   } // class FileChannelScatteredWriteCache

    public static class ReadCache extends WriteCache {

		public ReadCache(IBufferAccess buf) throws InterruptedException {
			super(buf, false/* prefixWrites */, true/* useChecksum */,
					false/* isHighlyAvailable */, false/* bufferHasData */, 0/* fileExtent */);
		}

		@Override
		protected boolean writeOnChannel(ByteBuffer buf, long firstOffset,
				Map<Long, RecordMetadata> recordMap, long nanos)
				throws InterruptedException, TimeoutException, IOException {
			throw new UnsupportedOperationException();
		}

		/**
		 * Overide clearAddrMap for read cache to always remove from the record map.
		 */
		@Override
		/* public */boolean clearAddrMap(final long addr, final int latchedAddr)
				throws IllegalStateException, InterruptedException {

			// Remove record from this cache.
			final RecordMetadata removed = recordMap.remove(addr);

			// might be null if concurrent transfer has taken place
			return removed != null;
		}
		
		/**
		 * ReadCache is always closedForWrites
		 */
	    @Override
	    public boolean isClosedForWrites() {

	        return true;
	        
	    }
	    
	    @Override
	    public void closeForWrites() {

	        throw new UnsupportedOperationException();
	        
	    }

        @Override
        boolean write(final long offset, final ByteBuffer data, final int chk,
                boolean writeChecksum, final int latchedAddr)
                throws InterruptedException {

            throw new UnsupportedOperationException();
            
        }

        /**
         * Called from WCS when moving from hotList to ReadList.
         * 
         * The hitCounts must be reset or full cache will always
         * be copied.
         * 
         * @return this ReadCache
         */
		ReadCache resetHitCounts() {
			Iterator<RecordMetadata> mds = recordMap.values().iterator();
			
			while (mds.hasNext()) {
				mds.next().hitCount = 0;
			}
			
			return this;
		}

	}
    
    /**
     * To support deletion we will remove any entries for the provided address.
     * This is just to yank something out of the cache which was created and
     * then immediately deleted on the RW store before it could be written
     * through to the disk. This does not reclaim any space in the write cache
     * since allocations are strictly sequential within the cache and can only
     * be used with the RW store. The RW store uses write prefixes in the cache
     * buffer so we must zero the long address element as well to indicate that
     * the record was removed from the buffer.
     * 
     * This approach has now been refined to avoid problems with incremental
     * checksums which otherwise would invalidate the buffer checksum to date.
     * Rather than zeroing the address of the deleted block a new zero-length
     * prefix is written that when processed will ensure any current recordMap
     * entry is removed.
     * 
     * TODO: An issue to be worked through is whether there remains a problem
     * with a full buffer where there is not room for the dummy "remove" prefix.
     * Whilst we could of course ensure that a buffer with less than the space
     * required for prefixWrites should be moved immediately to the dirtlyList,
     * there would still exist the possibility that the clear could be requested
     * on a buffer already on the dirtyList. It looks like this should not
     * matter, since each buffer update can be considered as an atomic update
     * even if the set of writes are individually not atomic (the updates from a
     * previous buffer will always have been completed before the next buffer is
     * processed).
     * 
     * In that case it appears we could ignore the situation where there is no
     * room for the dummy "remove" prefix, since there will be no room for a new
     * write also and the buffer will be flushed either on commit or a
     * subsequent write.
     * 
     * A problem previously existed with unsynchronized access to the
     * ByteBuffer. Resulting in a conflict over the position() and buffer
     * corruption.
     * 
     * If the WriteCache is closed then it must not be modified at all otherwise
     * any HA replication will not be binary compatible.
     * 
     * @param addr
     *            The address of a cache entry.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    /* public */boolean clearAddrMap(final long addr, final int latchedAddr)
            throws IllegalStateException, InterruptedException {

        // Note: Invoked from unit test w/o lock.
//        if (!transferLock.isHeldByCurrentThread())
//            throw new IllegalMonitorStateException();

        /*
         * Note: acquire() is mutex with the writeLock. clearAddrMap() will take
         * the writeLock in order to ensure that this operation is atomic with
         * respect to closeForWrites().
         */
        final ByteBuffer tmp = acquire();
        try {

            if (m_closedForWrites) {
                /*
                 * Neither the buffer nor the record map may be modified. The
                 * WriteCacheService is in the process of writing this buffer to
                 * the disk and replicating it to the downstream nodes (HA).
                 * 
                 * The record exists, but we can not remove it. Return false.
                 */
                return false;
            }

            // Remove record from this cache.
            final RecordMetadata removed = recordMap.remove(addr);

            if (removed == null) {
                /*
                 * Must be present.
                 * 
                 * Buffer not closed for writes, but record moved. Mayhaps
                 * compacted to another?
                 */
                throw new AssertionError();
            }

            removed.deleted = true;
            
            if (!prefixWrites) {
                /*
                 * We will not record a deleted record. We are not in HA mode.
                 */
                m_removed += removed.recordLength;
                return true;
            }

            // overwrite buffer, changing file offset to negative
            final int addr_offset = removed.bufferOffset
                    - SIZEOF_PREFIX_WRITE_METADATA;
            tmp.putLong(addr_offset, -removed.fileOffset); // abs write.

            /*
             * Fix up the debug flag when last address is cleared.
             */
            if (m_written && recordMap.isEmpty()) {
                m_written = false;
            }

            m_removed += removed.recordLength;

            return true;

        } finally {

            release();

        }

    }

    protected void registerWriteStatus(long offset, int length, char action) {
        // NOP to be overridden for debug if required
    }

    boolean m_written = false;
    
    private long lastOffset;

    /**
     * Called to clear the WriteCacheService map of references to this
     * WriteCache.
     * 
     * @param serviceRecordMap
     *            the map of the WriteCacheService that associates an address
     *            with a WriteCache
     * @throws InterruptedException
     */
//    * @param fileExtent
//    *            the current extent of the backing file.
    void resetWith(final ConcurrentMap<Long, WriteCache> serviceRecordMap
//            final long fileExtentIsIgnored
            ) throws InterruptedException {

        final Iterator<Long> entries = recordMap.keySet().iterator();
        
        if (serviceRecordMap != null && entries.hasNext()) {
        	transferLock.lock();
        	try {
	            if (log.isInfoEnabled())
	                log.info("resetting existing WriteCache: nrecords=" + recordMap.size() + ", hashCode=" + hashCode());
	
	            while (entries.hasNext()) {
	                
	                final Long fileOffset = entries.next();
	
	                /*
	                 * We need to guard against the possibility that the entry in
	                 * the service record map has been updated concurrently such
	                 * that it now points to a different WriteCache instance. This
	                 * is possible (for the RWStore) if a recently freed record has
	                 * been subsequently reallocated on a different WriteCache.
	                 * Using the conditional remove on ConcurrentMap guards against
	                 * this.
	                 */
	                final boolean removed = serviceRecordMap.remove(fileOffset, this);
	                
	                registerWriteStatus(fileOffset, 0, removed ? 'R' : 'L');
	
	            }
        	} finally {
        		transferLock.unlock();
        	}

        } else {
            if (log.isInfoEnabled()) {
                // debug to see recycling
                log.info("clean WriteCache: hashCode=" + hashCode());
            }
            // cache is written but also transfererd to readCache
//            if (m_written) {
//                log.warn("Written WriteCache but with no records");
//            }
        }       
        reset(); // must ensure reset state even if cache already empty

//        setFileExtent(fileExtent);

    }

    public void setRecordMap(Collection<RecordMetadata> map) {
        throw new RuntimeException("setRecordMap NotImplemented");
    }

    /**
     * Checksum helper computes the running checksum from series of
     * {@link ByteBuffer}s and <code>int</code> checksum values as written onto
     * the backing byte buffer for a {@link WriteCache} instance.
     */
    private static class ChecksumHelper extends ChecksumUtility {

        // /**
        // * Private helper object.
        // */
        // private final Adler32 chk = new Adler32();

        /**
         * A private buffer used to format the per-record checksums when they
         * need to be combined with the records written onto the write cache for
         * a total checksum over the write cache contents.
         */
        final private ByteBuffer chkbuf = ByteBuffer.allocate(4);

        /**
         * Update the running checksum to reflect the 4 byte integer.
         * 
         * @param v
         *            The integer.
         */
        public void update(final int v) {

            chkbuf.clear();
            chkbuf.putInt(v);
            chk.update(chkbuf.array(), 0/* off */, 4/* len */);

        }

        public int getChecksum() {
            return super.getChecksum();
        }

        public void reset() {
            super.reset();
        }

        public void update(final ByteBuffer buf) {
            super.update(buf);
        }

        // /**
        // * Update the {@link Adler32} checksum from the data in the buffer.
        // The
        // * position, mark, and limit are unchanged by this operation. The
        // * operation is optimized when the buffer is backed by an array.
        // *
        // * @param buf
        // * The buffer.
        // *
        // * @return The checksum.
        // */
        // public void update(final ByteBuffer buf) {
        // assert buf != null;
        //
        // final int pos = buf.position();
        // final int limit = buf.limit();
        //          
        // assert pos >= 0;
        // assert limit > pos;
        //
        // if (buf.hasArray()) {
        //
        // /*
        // * Optimized when the buffer is backed by an array.
        // */
        //              
        // final byte[] bytes = buf.array();
        //              
        // final int len = limit - pos;
        //              
        // if (pos > bytes.length - len) {
        //                  
        // throw new BufferUnderflowException();
        //              
        // }
        //                  
        // chk.update(bytes, pos + buf.arrayOffset(), len);
        //              
        // } else {
        //              
        // for (int i = pos; i < limit; i++) {
        //                  
        // chk.update(buf.get(i));
        //                  
        // }
        //              
        // }
        //                      
        // }

    }

    /**
     * Used by the HAWriteMessage to retrieve the nextOffset as implied by the
     * recordMap
     * 
     * @return the last offset value
     */
    public long getLastOffset() {
        return lastOffset;
    }

    /**
     * Hook to rebuild RecordMetadata after buffer has been transferred. For the
     * default {@link WriteCache} this is a single entry using firstOffset and
     * current position. For scattered writes, it uses a map with the addr,
     * size, and data inlined.
     * 
     * @see FileChannelScatteredWriteCache
     * 
     * @throws InterruptedException
     */
    public void resetRecordMapFromBuffer() throws InterruptedException {
        
        final Lock writeLock = lock.writeLock();

        writeLock.lockInterruptibly();

        try {
        
            resetRecordMapFromBuffer(buf.get().buffer().duplicate(), recordMap);

        } finally {
            
            writeLock.unlock();
            
        }
    }

    /**
     * Low-level routine copies the data from the caller's buffer into this
     * buffer.
     * 
     * @param bin
     *            The caller's buffer.
     * 
     * @throws InterruptedException
     */
    void copyRawBuffer(final ByteBuffer bin) throws InterruptedException {

        final Lock writeLock = lock.writeLock();

        writeLock.lockInterruptibly();

        try {

            final ByteBuffer buf = this.buf.get().buffer();

            /*
             * Copy the data from the caller's buffer into our own.
             * 
             * Note: We receive the buffer with pos=0, limit=#ofbyteswritten. As
             * a post-condition, pos will be advanced to the limit.
             */
            buf.limit(bin.limit());
            buf.position(0);
            buf.put(bin); // copy the caller's buffer.

            /*
             * Rebuild the record map
             * 
             * Note: rebuild reads from position to limit, advancing the
             * position.
             * 
             * Note: flush() expects pos=limit, so we are good to go after we
             * rebuild the record map.
             */

            buf.position(0); // reset the position.

            resetRecordMapFromBuffer(buf, recordMap);

            buf.position(buf.limit());

        } finally {

            writeLock.unlock();

        }

    }
    
    /**
     * Transfers records from this {@link WriteCache} to the destination
     * {@link WriteCache}, updating the record map in the
     * {@link WriteCacheService} as it goes.
     * <p>
     * This method handles prefixWrites and useChecksum to transfer the correct
     * bytes for the associated {@link RecordMetadata}.
     * 
     * @param src
     *            The source buffer.
     * @param dst
     *            Records are transferred into the <i>dst</i> {@link WriteCache}
     *            .
     * @param serviceRecordMap
     *            The {@link WriteCacheService}'s record map.
     * @param threshold
     * 				The hitCount at which the record is transferred
     * 
     * @return Returns true if the transfer is complete, or false if the
     *         destination runs out of room.
     * 
     * @throws InterruptedException
     */
    // package private
    static boolean transferTo(final WriteCache src, final WriteCache dst,
            final ConcurrentMap<Long, WriteCache> serviceRecordMap, final int threshold)
            throws InterruptedException {

        if (src == null)
            throw new IllegalArgumentException();
        if (dst == null)
            throw new IllegalArgumentException();
        if (src == dst)
            throw new IllegalArgumentException();
        
        // FIXME: check assumptions for transferTo, closedForWrites vs ReadCache etc
//        if (src.m_closedForWrites) {
//            // the source buffer must not be closed for writes.
//            throw new IllegalStateException();
//        }
//        if (dst.m_closedForWrites) {
//            // the dst buffer must not be closed for writes.
//            throw new IllegalStateException();
//        }
        /*
         * Note: This method is only invoked during critical code in
         * WriteTask.call(). No other thread can write on [src] (because it is
         * closed for writes) and no other thread can write on [dst] (because it
         * is not visible to any other thread). Therefore we DO NOT need a lock
         * here to prevent concurrent writers on [src] or [dst].
         * 
         * However, there is a problem where concurrent clears of an addr are
         * permitted. Those clears are not able to effect [src] since it is
         * closed for writes. But they are also not being applied to the [dst]
         * buffer since the serviceRecordMap is incrementally updated as we copy
         * the records from [src] to [dst].
         * 
         * Therefore, WriteCacheService.clearWrite() must be MUTEX with this
         * method for the scope of the [src] buffer or we can lose that
         * clearWrite(). I suggest that we add a distinct lock for this purpose
         * so the only contention for WriteCacheService.clearWrite() is with
         * transferTo() for the buffer that is currently being compacted (the
         * [src] buffer).
         * 
         * Note: For HA, replicated WriteCache buffers MUST set closeForWrites()
         * to ensure that compaction DOES NOT take place on the followers!!!
         */
        final Lock lock = src.transferLock;
        lock.lock();
        try {
            /*
             * Use a lock to ensure that the current state of the ByteBuffer is
             * visible to this thread. This can be the readLock because (a) we
             * are only reading on [src]; and (b) this method is invoked from a
             * critical section in WriteTask.call(), writes are closed on [src],
             * and nobody can modify [src] (it could also be the writeLock, but
             * it does not really matter which we use here as far as I can tell
             * since both closedForWrites() and the critical section are
             * guaranteeing that no writes will be applied to [src]).
             */
            final ByteBuffer bb = src.acquire().duplicate();
            ByteBuffer dd = null;
//            final int srcSize = src.recordMap.size();
//            int notTransferred = 0;
//            int transferred = 0;
            try {
                // Setup destination
                dd = dst.acquire();
                // Note: md.recordLength includes the checksum (suffix)
                // check *destination* for prefixWrites - which will be zero for ReadCache
                final int prefixlen = dst.prefixWrites ? SIZEOF_PREFIX_WRITE_METADATA : 0;
    
                final Set<Entry<Long, RecordMetadata>> es = src.recordMap.entrySet();
                final Iterator<Entry<Long, RecordMetadata>> entries = es.iterator();
                while (entries.hasNext()) {
                    final Entry<Long, RecordMetadata> entry = entries.next();
                    final long fileOffset = entry.getKey(); // file offset.
                    
                    final RecordMetadata md = entry.getValue();
                    if (serviceRecordMap != null) {
                        final WriteCache tmp = serviceRecordMap.get(fileOffset);
                        if (tmp != src) {
                        	assert !(tmp instanceof ReadCache);
                        	
                        	entries.remove();
                        	
                        	continue;
//                            throw new AssertionError(
//                                    "Record not owned by this cache: src="
//                                            + src + ", owner=" + tmp
//                                            + ", offset=" + fileOffset + ", md="
//                                            + md);
                        }
                    }
                    assert !md.deleted; // not deleted (deleted entries should not be in the recordMap).
                    // only copy records >= to threshold
					if (md.hitCount < threshold) {
//						notTransferred++;
						
						serviceRecordMap.remove(fileOffset);
					} else {
						final int len = prefixlen + md.recordLength;
						final int dstremaining = dst.remaining();
						if (len > dstremaining) {
							// Not enough room in destination for this record.
							if (dstremaining >= 512) {
								// Destination still has room, keep looking.
//								notTransferred++;
								continue;
							}
							// Destination is full (or full enough).
							return false;
						}

						// final ByteBuffer dup = bb;//bb.duplicate(); (dup'd
						// above).
						final int pos = md.bufferOffset - prefixlen;// include
																	// prefix
						final int limit = pos + len; // and any postfix
						final int dstoff; // offset in the destination buffer.
						synchronized (bb) {
							bb.limit(limit);
							bb.position(pos);
							// dst.writeRaw(fileOffset, dup, md.latchedAddr);

							// Copy to destination.
							synchronized (dd) {
								dstoff = dd.position() + prefixlen;
								dd.put(bb);
								
//								transferred++;
								
								assert dst.remaining() == (dstremaining - len) : "dst.remaining(): "
										+ dst.remaining()
										+ " expected: "
										+ dstremaining;
							}
						}
						/*
						 * Insert record into destination.
						 * 
						 * Note: The [orderedList] on the target buffer is not
						 * updated because we handle the propagation of the
						 * address allocation/clear notices separately and
						 * synchronously using prepareAddressMetadataForHA().
						 */
						{
							final RecordMetadata old = dst.recordMap.put(Long
									.valueOf(fileOffset), new RecordMetadata(
									fileOffset, dstoff/* bufferOffset */,
									md.recordLength, md.latchedAddr));

							assert old == null : "Write already found: " + old;
						}

						if (serviceRecordMap != null) {
							/*
							 * Note: As soon as we update the service record map
							 * it is possible that
							 * WriteCacheService.clearWrite() will clear the
							 * record from [dst]. We can not rely on the record
							 * remaining in [dst] after this method call!
							 */
							final WriteCache tmp = serviceRecordMap.put(
									fileOffset, dst);
							assert src == tmp : "tmp=" + tmp + ",src=" + src
									+ ", offset=" + fileOffset + ", md=" + md;
						}
					}

                    // Clear entry from src recordMap.
                    entries.remove();
                    
                }
                
                // true iff all records were transfered out.
                final boolean isEmpty = src.recordMap.isEmpty();
                
                return isEmpty;
                
            } finally {
                try {
                	// FIXME: check assumptions re closedForWrites and ReadCache
//                    if (src.m_closedForWrites) {
//                        // the source buffer must not be closed for writes.
//                        throw new IllegalStateException();
//                    }
//                    if (dst.m_closedForWrites) {
//                        // the dst buffer must not be closed for writes.
//                        throw new IllegalStateException();
//                    }
                } finally {
                    if (dd != null)
                        dst.release();
                    src.release();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Apply the {@link #orderedRecords} to create a dense {@link WriteCache}
     * buffer that presents the addresses from the {@link #recordMap} along with
     * enough metadata to decide whether this is a delete or merely an address
     * declaration. Address declarations are modeled by setting the record size
     * to a negative value. Address deletes are modeled by setting the
     * fileOffset to a negative value. Actual address writes are not
     * communicated through this method, but their data will eventually make it
     * to the follower if the address is not recycled before the
     * {@link WriteCache} holding that data is communicated to the follower (in
     * which case the follower will eventually see the delete marker for the
     * address instead of the application data for the address).
     * 
     * @return true unless there is nothing in the {@link WriteCache}.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    boolean prepareAddressMetadataForHA() throws IllegalStateException,
            InterruptedException {

        if (!prefixWrites)
            throw new IllegalStateException();

        if (orderedRecords == null)
            throw new IllegalStateException();

        final ByteBuffer tmp = acquire();

        try {

            /*
             * Note: We need to be synchronized on the ByteBuffer here
             * since this operation relies on the position() being
             * stable.
             * 
             * Note: Also see clearAddrMap(long) which is synchronized
             * on the acquired ByteBuffer in the same manner to protect
             * it during critical sections which have a side effect on
             * the buffer position.
             */

            synchronized (tmp) {

                // Note: guarded by synchronized(tmp)!
                if (orderedRecords.isEmpty()) {

                    return false;

                }
                
                tmp.position(0);
                tmp.limit(tmp.capacity());

                for (RecordMetadata md : orderedRecords) {

                    if (md.deleted) {
                        /*
                         * Entry is address of deleted record. No application
                         * data follows the entry (the next thing in the buffer
                         * will be another entry).
                         */
                        tmp.putLong(-md.fileOffset);
                        tmp.putInt(-md.recordLength);
                    } else {
                        /*
                         * Entry is notice of non-deleted address. No
                         * application data follows the entry (the next thing in
                         * the buffer will be another entry).
                         */
                        tmp.putLong(md.fileOffset);
                        tmp.putInt(-md.recordLength);
                    }
                    tmp.putInt(md.latchedAddr);

                } // next RecordMetadata

                // Note: Guarded by synchronized(tmp)
                orderedRecords.clear();

            } // synchronized(tmp)

            return true;
            
        } finally {

            release();

        }

    }
    
    /**
     * Overridden by
     * {@link FileChannelScatteredWriteCache#resetRecordMapFromBuffer(ByteBuffer, Map)}
     * .
     * 
     * @param buf
     * @param recordMap
     */
    protected void resetRecordMapFromBuffer(final ByteBuffer buf,
            final Map<Long, RecordMetadata> recordMap) {

        recordMap.clear();

        // put a single empty entry into the buffer.
        recordMap.put(firstOffset.get(), new RecordMetadata(firstOffset.get(),
                0, buf.limit(), 0/* latchedAddr */));

    }

    /**
     * Called from {@link WriteCacheService} to lock buffer content immediately
     * prior to flushing and HA pipline replication. Neither the internal buffer
     * state nor the {@link #recordMap} may be changed once the
     * {@link WriteCache} has been closed for writes. This is necessary to
     * provide 100% binary replication. Otherwise the stores can differ in the
     * data in freed allocation slots.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    /*
     * Note: exposed to IHAJournalStrategy.writeRawBuffer(). Implementations of
     * that method must close the mock WriteCache against writes to prevent
     * compaction of replicated WriteCache buffers on the receiver (HA).
     */
    public void closeForWrites() throws IllegalStateException, InterruptedException {

        /*
         * Note: clearAddrMap() uses acquire() to operate on the recordMap and
         * the buffer. This method must be mutex with clearAddrMap(), so we take
         * the writeLock.
         */
        
        final Lock lock = this.lock.writeLock();

        lock.lockInterruptibly();

        try {

            if (m_closedForWrites)
                throw new AssertionError();

            m_closedForWrites = true;

        } finally {

            lock.unlock();

        }

    }
    
    public boolean isClosedForWrites() {

        return m_closedForWrites;
        
    }

    /**
     * Return the percentage of space that has been removed through the
     * application of {@link #clearAddrMap(long, int)} and hence could be
     * recovered through compaction of the {@link WriteCache}.
     * 
     * @return The percentage of recoverable space in [0:100].
     */
    final int potentialCompaction() {

        final int percentEmpty = (m_removed * 100) / bytesWritten();
        
        assert percentEmpty >= 0 && percentEmpty <= 100;

        return percentEmpty;
        
    }

    /*
     * Managing reference counts for the memoizer pattern for the ReadCache.
     */
    
    /**
     * Allocate space for a record of the given length on this
     * {@link WriteCache}.
     * 
     * @param nbytes
     *            The size of the record.
     * 
     * @return A view of the allocation on the {@link WriteCache} -or-
     *         <code>null</code> if there is not enough room in this
     *         {@link WriteCache} for the allocation.
     * 
     * @throws IllegalStateException
     *             if the {@link WriteCache} has been {@link #close() closed}.
     * @throws InterruptedException
     *             if the lock could not be acquired.
     */
    public ByteBuffer allocate(final int nbytes) throws IllegalStateException,
            InterruptedException {

        final ByteBuffer tmp = acquire();
        
        try {
        
            synchronized (tmp) {
            
                if (remaining() > nbytes) {
                
                    // [pos] the position of the new allocation.
                    final int pos = tmp.position();
                    
                    // Advance position() beyond the new allocation.
                    tmp.position(pos + nbytes);

                    // Dup the buffer for independent pos and limit.
                    final ByteBuffer ret = tmp.duplicate();

                    // Setup view onto new allocation.
                    ret.position(pos);
                    ret.limit(pos + nbytes);

                    // Return view.
                    return ret;

                } else {
                    
                    return null;

                }
                
            }

        } finally {
        
            release();
            
        }
        
    }
	
	void commitToMap(final long offset, final int position, final int nbytes) {
        final RecordMetadata md = new RecordMetadata(offset, position,
                nbytes, -1/*latchedAddr*/);

        if (recordMap.put(offset, md) != null) {
        	log.warn("Record already in cache");
        }
	}

	/**
	 * The referenceCount is used to protect as early resetting to the clean
	 * list. It is incremented by the WCS when used as a readCache and
	 * thereafter by the memoizer when the cache is used for an installation.
	 * When decremented to zero, it should be returned to the clean list. 
	 */
	final AtomicInteger m_referenceCount = new AtomicInteger(0);
	
	public int getReferenceCount() {
		return m_referenceCount.get();
	}

	/**
	 * Called when a new reference is acquired
	 * 
	 * @return current reference count
	 */
	public int incrementReferenceCount() {
	    // Note: Used from critical sections. Nothing interruptable here!
		return m_referenceCount.incrementAndGet();
	}

	/**
	 * Although public, it is designed to be used by the WriteCacheService
	 * with a memoizer pattern to support concurrent reads to
	 * read cache buffers.
	 * 
	 * Called when a reference is released.  The return
	 * value should be tested and if zero the cache should
	 * be returned to the clean list.
	 * 
	 * @return current reference count
	 */
	public int decrementReferenceCount() {
		return m_referenceCount.decrementAndGet();
	}

	/**
	 * Checks if cache recordMap contains address offset
	 * 
	 * @param offset
	 * @return
	 */
	public boolean contains(final long offset) {
		return recordMap.containsKey(offset);
	}

}
