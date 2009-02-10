/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.BTree;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.IUpdateStore;
import com.bigdata.resources.StoreManager;
import com.bigdata.resources.StoreManager.ManagedJournal;

/**
 * A disk-based strategy where a large buffer is used to minimize the chance
 * that a read will read through to the disk (under normal circumstances the
 * on-disk file will be fully buffered). This is especially important during
 * asynchronous overflow processing as the data written onto {@link BTree}s has
 * been appended onto the store and more or less random reads are required to
 * traverse the {@link BTree} tuples in index order.
 * <p>
 * This strategy is designed for use with {@link StoreManager}. The expectation
 * is that the store will be <em>fully</em> buffered MOST of the time.
 * Typically the {@link Options#INITIAL_EXTENT} will be set equal to the
 * {@link Options#MAXIMUM_EXTENT} using a value on the order of 200M. Normally,
 * overflow will be triggered before the user extent is saturated and the disk
 * file will remain fully buffered. In these cases there will be NO reads
 * through to the disk. Note that neither the {@link DirectBufferStrategy} nor
 * the {@link MappedBufferStrategy} are suitable for asynchronous overflow
 * precisely because the JVM does not handle extending a mapped file or correct
 * release of direct {@link ByteBuffer}s.
 * <p>
 * There are a variety of reasons why overflow processing might not be initiated
 * before the user extent overflows (asynchronous overflow may still be running
 * on the old journal, the last set of tasks executing may have written more
 * data that remains in the user extent, etc.). Regardless, in any of these
 * situations the backing file on the disk will be extended BUT NOT the buffer.
 * The buffer itself IS NOT extended for reasons that mostly have to do with
 * memory leaks in the JVM for direct {@link ByteBuffer}s (in fact, the caller
 * must provide the buffer via the ctor, in a manner very similar to how the
 * write cache is managed for the {@link DiskOnlyStrategy}).
 * <p>
 * The buffer provides both a write cache and a read cache, but only until it is
 * full. On commit, all bytes from the last byte flushed to the backing file
 * will be transferred from the buffer to the backing file. On restart, as much
 * data in the user extent as will fit is read from the backing file into the
 * buffer.
 * <p>
 * Once the buffer is full, a small {@link WriteCache} is allocated using the
 * {@link DirectBufferPool#INSTANCE} and reads beyond the extent covered by the
 * buffer go straight through to the disk. Writes are buffered in a write cache.
 * The cache is flushed when it would overflow. As a result only large
 * sequential writes are performed on the store. Reads read through the write
 * cache for consistency.
 * <p>
 * One other advantage of this strategy is that interrupts of NIO operations are
 * much less likely to cause the backing {@link FileChannel} to be closed
 * asynchronously since most reads will be serviced by the buffer rather than
 * touching the disk. Also, for reads which are serviced by the buffer, we can
 * offer higher concurrency (reads through to the disk are serialized).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#BufferedDisk
 * @see TestBufferedDiskJournal
 * 
 * @todo modify to accept the {@link DirectBufferPool} instance to be used as a
 *       ctor parameter. that keeps the (de-)allocation local and allows us to
 *       configure the size of the backing buffer when we setup the
 *       {@link StoreManager} and to use small buffers for the unit tests (there
 *       can be a distinct {@link DirectBufferPool} for
 *       {@link TestBufferedDiskJournal}). The {@link AbstractJournal} will
 *       need another parameter for the {@link DirectBufferPool}. That argument
 *       should be optional and default to <code>null</code> for all of the
 *       other buffer modes. We would still pass in the writeCache.
 * 
 * @todo add a test suite for this variant.
 * 
 * @todo test out on a cluster. examine behavior of a series of overflow
 *       operations and see how much this does to improve throughput.
 * 
 * @todo modify asynchronous overflow to always "finish up" (handle each index
 *       before it ends). explicitly track the indices that have to be processed
 *       and those not yet finished.
 *       <p>
 *       What to do about indices that will not overflow? This is a pretty
 *       critical issue as we otherwise could wind up keeping a number of
 *       historical journals on hand. Normally I would expect such issues mainly
 *       with new applications that are being tested, in which case (a) test on
 *       a test federation and (b) the indices can be dropped.
 * 
 * @todo test correct {@link #force(boolean)} (must transfer bytes written since
 *       the last force).
 * 
 * @todo verify that reads return an immutable view of the {@link #buffer} and
 *       that high concurrency for reads is allowed when the record to be read
 *       lies within the buffered region.
 * 
 * @todo test correct transition from the buffered extent onto the unbuffered
 *       extent.
 * 
 * @todo verify that records which extend across the {@link #buffer} are NOT
 *       stored in the {@link #buffer} (no split reads for sanity's sake).
 * 
 * @todo report whether or not the on-disk write cache is enabled for each
 *       platform in {@link AbstractStatisticsCollector}. offer guidence on how
 *       to disable that write cache.
 * 
 * @todo test verifying that the write cache comes online atomically.
 * 
 * @todo test verifying that {@link #writeCache} is restored iff necessary on
 *       restart.
 * 
 * @todo test verifying {@link #writeCacheOffset} is restored correctly on
 *       restart (ie., you can continue to append to the store after restart and
 *       the result is valid).
 * 
 * @todo test verifying that the buffer position and limit are updated correctly
 *       by {@link #write(ByteBuffer)} regardless of the code path.
 * 
 * @todo If possible, refactor to share a common base class with the
 *       {@link DiskOnlyStrategy}. The main points of departure are the lack of
 *       an option for a read cache in this class and the differences in how the
 *       buffer is layered in.
 * 
 * @todo due to the high memory burden, this variant might not be the default
 *       for the unit tests of the services. however, it should be the default
 *       for deployed distributed federations.
 * 
 * FIXME Examine behavior when write caching is enabled/disabled for the OS.
 * This has a profound impact. Asynchronous writes of multiple buffers, and the
 * use of smaller buffers, may be absolutely when the write cache is disabled.
 * It may be that swapping sets in because the Windows write cache is being
 * overworked, in which case doing incremental and async IO would help. Compare
 * with behavior on server platforms. See
 * http://support.microsoft.com/kb/259716,
 * http://www.accucadd.com/TechNotes/Cache/WriteBehindCache.htm,
 * http://msdn2.microsoft.com/en-us/library/aa365165.aspx,
 * http://www.jasonbrome.com/blog/archives/2004/04/03/writecache_enabled.html,
 * http://support.microsoft.com/kb/811392,
 * http://mail-archives.apache.org/mod_mbox/db-derby-dev/200609.mbox/%3C44F820A8.6000000@sun.com%3E
 * 
 * <pre>
 *                /sbin/hdparm -W 0 /dev/hda 0 Disable write caching
 *                /sbin/hdparm -W 1 /dev/hda 1 Enable write caching
 * </pre>
 */
public class BufferedDiskStrategy extends AbstractBufferStrategy implements
        IDiskBasedStrategy, IUpdateStore {
    
    /**
     * Options for the {@link BufferedDiskStrategy}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
        
        /**
         * The capacity of the {@link DirectBufferPool} is the maximum #of
         * direct {@link ByteBuffer} instances that may reside in the pool
         * (default {@value #DEFAULT_POOL_CAPACITY}).
         * <p>
         * Note: Placing a limit on the pool size could cause threads to
         * deadlock awaiting a direct buffer from the pool. For this reason is
         * it good practice to use
         * {@link DirectBufferPool#acquire(long, TimeUnit)} with a timeout.
         */
        String POOL_CAPACITY = BufferedDiskStrategy.class.getName()
                + ".poolCapacity";

        /**
         * The default pool capacity (no limit).
         */
        String DEFAULT_POOL_CAPACITY = "" + Integer.MAX_VALUE;

        /**
         * The capacity in bytes of the direct {@link ByteBuffer} instances
         * allocated and managed by the {@link DirectBufferPool} ({@link #DEFAULT_BUFFER_CAPACITY}).
         */
        String BUFFER_CAPACITY = BufferedDiskStrategy.class.getName()
                + ".bufferSize";

        /**
         * The default capacity of the allocated buffers.
         */
        String DEFAULT_BUFFER_CAPACITY = "" + Bytes.megabyte32 * 200;

    }
    
    /**
     * The file.
     */
    private final File file;
    
    /**
     * The mode used to open that file.
     */
    private final String fileMode;
    
    /**
     * The IO interface for the file - <strong>use
     * {@link #getRandomAccessFile()} rather than this field</strong>.
     */
    private RandomAccessFile raf;

    /**
     * The size of the journal header, including MAGIC, version, and both root
     * blocks. This is used as an offset when computing the address of a record
     * in an underlying file and is ignored by buffer modes that are not backed
     * by a file (e.g., transient) or that are memory mapped (since the map is
     * setup to skip over the header)
     */
    private final int headerSize;
    
    /**
     * Extent of the file. This value should be valid since we obtain an
     * exclusive lock on the file when we open it.
     */
    private long extent;

    private long userExtent;

    /**
     * The buffer.
     * 
     * FIXME make provisions for filling this buffer from the disk on restart.
     * For the other modes, that is happening in {@link FileMetadata} right now.
     */
    private ByteBuffer buffer;
    
    /**
     * The size of the {@link #buffer} in bytes (a constant).
     */
    private final int bufferSize;
    
    /**
     * {@link WriteCache} (allocated iff we need to extend the backing file on
     * disk beyond the fully buffered extent).
     */
    private WriteCache writeCache = null;
    
    /**
     * The next offset at which data in the {@link #writeCache} will be written
     * on the disk. The offset is relative to the start of the user data space.
     * Offset zero(0) addresses the first byte after the root blocks.
     * 
     * @see FileMetadata#nextOffset
     * @see AbstractBufferStrategy#nextOffset
     */
    private long writeCacheOffset;
    
    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class WriteCache {
        
        /**
         * The buffer used to absorb writes that are destined for the disk. Writes
         * are simply appended into this buffer until it would overflow. On
         * overflow, {@link #flushWriteCache()} is invoked to flush the data to the
         * disk (without synchronizing the disk). If a record is too large to fit
         * into this buffer, then the write cache is flushed and the record is
         * written directly on the disk.
         * <p>
         * Note: We must clone the data since the
         * {@link IRawStore#write(ByteBuffer)} contract says that the caller can
         * reuse the buffer once we return. In order minimize heap churn we simply
         * copy the data into {@link #buf}, a {@link ByteBuffer} that
         * buffers recently written records. Writes are deferred until the buffer is
         * would overflow and then all buffered are written at once onto the disk.
         * <p>
         * In order to ensure consistency we read through the {@link #buf} in
         * {@link #read(long)}. Otherwise a {@link #write(ByteBuffer)} could return
         * and a subsequent read on the record while it is in the
         * {@link #buf} would "miss" causing us to read through to the disk
         * (which would not have the correct data).
         * <p>
         * Note: The write cache design assumes an "append only" store. In
         * particular, it assumes that the application data records are written in
         * are purely sequential manner on the end of the file (the root blocks are
         * outside of the application data). Either the write cache must be disabled
         * or a different design must be used if you are using a store where records
         * may be deleted and recycled.
         * <p>
         * The write cache offers a 27% performance gain when compared to the same
         * condition without the write cache as measured by
         * {@link AbstractMRMWTestCase}.
         */
        final private ByteBuffer buf;
        
        /**
         * An index into the write cache used for read through on the cache. The
         * keys are the addresses that would be used to read the corresponding
         * record. The values are the position in {@link #buf} where that record
         * is buffered. A cache miss means that you need to read the record from
         * the disk.
         */
        final private Map<Long,Integer> writeCacheIndex;
        
        /**
         * Create a {@link WriteCache} from a caller supplied buffer.
         * <p>
         * Note: {@link FileChannel} IO is fact perform using a direct
         * {@link ByteBuffer}. When the caller supplies a {@link ByteBuffer}
         * that is allocated on the Java heap as opposed to in native memory a
         * temporary direct {@link ByteBuffer} will be allocated for the IO
         * operation. The JVM can fail to release this temporary direct
         * {@link ByteBuffer}, resulting in a memory leak. For this reason, the
         * write cache should be a direct {@link ByteBuffer} and the same direct
         * {@link ByteBuffer} instance should be shared when overflow causes the
         * live journal overflow, being replaced by a new live journal. This bug
         * forces us to pass in the write cache {@link ByteBuffer} directly via
         * the {@link ManagedJournal} constructor.
         * 
         * @see http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=8fab76d1d4479fffffffffa5abfb09c719a30?bug_id=6210541
         * 
         * @param writeCache
         *            A {@link ByteBuffer} to be used as the write cache
         *            (optional).
         * 
         * @param capacity
         */
        public WriteCache(final ByteBuffer writeCache) {
            
            if (writeCache == null)
                throw new IllegalArgumentException();

            // save reference to the write cache.
            this.buf = writeCache;
            
            // the capacity of the buffer in bytes.
            final int capacity = writeCache.capacity();
            
            /*
             * Discard anything in the buffer, resetting the position to zero,
             * the mark to zero, and the limit to the capacity.
             */
            writeCache.clear();
            
            /*
             * An estimate of the #of records that might fit within the write
             * cache. This is based on an assumption that the "average" record
             * is 1k. This is used solely to assign the initial capacity to the
             * writeCacheIndex.
             */
            final int indexDefaultCapacity = capacity / (1 * Bytes.kilobyte32);
            
            // allocate and initialize the write cache index.
            writeCacheIndex = new ConcurrentHashMap<Long, Integer>(indexDefaultCapacity);

        }
        
        /**
         * The current position in the buffer.
         */
        final int position() {
            
            return buf.position();
            
        }

        /**
         * The capacity of the buffer.
         */
        final int capacity() {
            
            return buf.capacity();
            
        }
        
        void flush() {
            
            // #of bytes to write on the disk.
            final int nbytes = buf.position();

            if (nbytes == 0) return;

            // limit := position; position := 0;
            buf.flip();

            // write the data on the disk file.
            writeOnDisk(buf, writeCacheOffset, true/*append*/);

            // position := 0; limit := capacity.
            buf.clear();

            // clear the index since all records were flushed to disk.
            writeCacheIndex.clear();
            
        }

        /**
         * Write the record on the cache.
         * 
         * @param addr
         *            The address assigned to that record in the journal.
         * 
         * @param data
         *            The record.
         */
        void write(final long addr, final ByteBuffer data) {

            // the position() at which the record is cached.
            final int position = buf.position();

            // copy the record into the cache.
            buf.put(data);

            // add the record to the write cache index for read(addr).
            writeCacheIndex.put(new Long(addr), new Integer(position));

        }

        /**
         * Read a record from the write cache.
         * 
         * @param addr
         *            The address assigned to that record in the journal.
         * @param nbytes
         *            The length of the record (decoded from the address by the
         *            caller).
         * 
         * @return A read-write view onto the record in the write cache buffer
         *         -or- <code>null</code> iff the record does not lie within
         *         this {@link WriteCache}.
         *         <p>
         *         Note: The caller MUST copy the data from the view since
         *         concurrent operations may result in the write cache being
         *         flushed and the view overwritten with new data.
         *         <p>
         *         Note: A read-write view is returned in order to support
         *         {@link BufferedDiskStrategy#update(long, int, ByteBuffer)}
         *         for those cases when the record to be updated in still in
         *         the {@link WriteCache}.
         */
        ByteBuffer read(final long addr, final int nbytes) {
                
            /*
             * The return value is the position in the writeCache where that
             * record starts and [null] if the record is not in the writeCache.
             */
            final Integer writeCachePosition = writeCacheIndex.get(addr);

            if (writeCachePosition == null) {
                
                // The record is not in this write cache.
                
                return null;
                
            }

            // the start of the record in writeCache.
            final int pos = writeCachePosition;

            // create a view with same offset, limit and position.
            final ByteBuffer tmp = buf.duplicate();

            // adjust the view to just the record of interest.
            tmp.limit(pos + nbytes);
            tmp.position(pos);
            
            /*
             * Return a slice using that view - this restrict the caller to only
             * those bytes exposed by the slice.
             */
            return tmp.slice();

        }
        
    }
    
    /**
     * Writes the {@link #writeCache} through to the disk and its position is
     * reset to zero.
     * <p>
     * The caller MUST be synchronized on <i>this</i>.
     */
    void flushWriteCache() {
        
        if (writeCache == null) return;

        writeCache.flush();
        
        counters.ncacheFlush++;

    }
    
    final public int getHeaderSize() {
        
        return headerSize;
        
    }
    
    final public File getFile() {
        
        return file;
        
    }

    final public RandomAccessFile getRandomAccessFile() {

        return raf;

    }

    final public FileChannel getChannel() {

        final RandomAccessFile raf = getRandomAccessFile();

        if (raf == null)
            return null;

        return raf.getChannel();

    }

    /**
     * Counters on {@link IRawStore} and disk access.
     */
    final public DiskOnlyStrategy.Counters counters = new DiskOnlyStrategy.Counters();
    
    /**
     * Return interesting information about the write cache and file operations.
     */
    synchronized public CounterSet getCounters() {
        
        if (root == null) {
            
            root = new CounterSet();

            root.addCounter("nextOffset", new Instrument<Long>() {
                public void sample() {
                    setValue(nextOffset);
                }
            });

            root.addCounter("extent", new Instrument<Long>() {
                public void sample() {
                    setValue(extent);
                }
            });

            root.attach(counters.getCounters());
            
            /*
             * The BUFFER_POOL.
             */
            {

                final CounterSet tmp = root.makePath("BufferPool");

                tmp.addCounter("poolCapacity", new OneShotInstrument<Integer>(
                        DirectBufferPool.INSTANCE.getPoolCapacity()));

                tmp.addCounter("bufferCapacity",
                        new OneShotInstrument<Integer>(
                                DirectBufferPool.INSTANCE.getBufferCapacity()));

                tmp.addCounter("poolSize", new Instrument<Integer>() {
                    public void sample() {
                        setValue(DirectBufferPool.INSTANCE.getPoolSize());
                    }
                });
                
            }

            /*
             * other.
             */
            {
                final CounterSet writeCache = root.makePath("writeCache");

                {
                 
                    final WriteCache tmp = BufferedDiskStrategy.this.writeCache;

                    // add counter for the write cache capacity.
                    writeCache.addCounter("capacity",
                            new OneShotInstrument<Long>(tmp == null ? 0L : tmp
                                    .capacity()));
                    
                }
                
            }

//            sb.append("file="+file);
//            sb.append(", mode="+fileMode);
//            sb.append(", readOnly=" + isReadOnly());
            
        }
        
        return root;
        
    }
    private CounterSet root;

    /**
     * A JVM-wide pool of direct {@link ByteBuffer}s used for
     * {@link BufferMode#BufferedDisk}.
     * <p>
     * Note: {@link #acquire()} requests will block once the pool capacity has
     * been reached until a buffer is {@link #release(ByteBuffer)}ed.
     */
    private final static BufferPool BUFFER_POOL;

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class BufferPool extends DirectBufferPool {

        public BufferPool(final int poolCapacity, final int bufferCapacity) {

            super(poolCapacity, bufferCapacity);

        }
        
    }
    
    static {
        
        final int poolCapacity = Integer.parseInt(System.getProperty(
                Options.POOL_CAPACITY, Options.DEFAULT_POOL_CAPACITY));

        if(INFO)
            log.info(Options.POOL_CAPACITY + "=" + poolCapacity);

        final int bufferCapacity = Integer.parseInt(System.getProperty(
                Options.BUFFER_CAPACITY, Options.DEFAULT_BUFFER_CAPACITY));

        if (INFO)
            log.info(Options.BUFFER_CAPACITY + "=" + bufferCapacity);

        BUFFER_POOL = new BufferPool(
                poolCapacity,
                bufferCapacity
                );

    }
    
    /**
     * 
     * @param maximumExtent
     * @param fileMetadata
     */
    BufferedDiskStrategy(final long maximumExtent,
            final FileMetadata fileMetadata) {

        super(fileMetadata.extent, maximumExtent, fileMetadata.offsetBits,
                fileMetadata.nextOffset, fileMetadata.bufferMode,
                fileMetadata.readOnly);
        
        this.file = fileMetadata.file;

        this.fileMode = fileMetadata.fileMode;
        
        if (fileMetadata.bufferMode != BufferMode.BufferedDisk)
            throw new IllegalArgumentException();
        
        this.raf = fileMetadata.raf;
        
        if (raf == null)
            throw new IllegalArgumentException();
        
        this.extent = fileMetadata.extent;

        this.headerSize = FileMetadata.headerSize0;
        
        this.userExtent = extent - headerSize;

        // allocate the buffer.
        {
            
            try {

                this.buffer = BUFFER_POOL.acquire(1000, TimeUnit.MILLISECONDS);
                
                this.bufferSize = buffer.capacity();
                
            } catch (Throwable t) {
                
                throw new RuntimeException(t);
                
            }

        }
        
        /*
         * Enable the write cache?
         * 
         * Note: Do NOT enable the write cache if the file is being opened in a
         * read-only mode.
         * 
         * Note: If the file has been closed for writes (closeTime != 0L), then
         * the file is read-only regardless of the mode in which it was opened.
         * 
         * Note: NIO always (at least up to Java 6) allocates a "temporary"
         * direct byte buffer for disk read/write operations on a heap buffer
         * AND there is a bug in the release of those buffers. Therefore do NOT
         * pass in a heap byte buffer for the write cache!!!
         */
        if (fileMetadata.writeCache != null && !fileMetadata.readOnly
                && fileMetadata.closeTime == 0L) {

            if(INFO)
                log.info("Enabling writeCache: capacity="
                    + fileMetadata.writeCache.capacity());

            writeCache = new WriteCache( fileMetadata.writeCache );
            
        } else {
            
            writeCache = null;
            
        }

        // the offset at which the next record would be written on the file.
        writeCacheOffset = fileMetadata.nextOffset;

    }
   
    final public boolean isStable() {
        
        return true;
        
    }

    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    /**
     * {@link #flushWriteCache() flushes} the optional {@link #writeCache}
     * before syncing the disk.
     */
    public void force(boolean metadata) {

        assertOpen();
        
        synchronized(this) {

            // flush all pending writes to disk.
            flushWriteCache();
            
        }

        try {

            // sync the disk.
            getChannel().force(metadata);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
        counters.nforce++;
        
    }

    /**
     * Closes the file immediately (without flushing any pending writes).
     */
    public void close() {

        /*
         * Note: this clears the [open] flag. It is important to do this first
         * so that we do not re-open the channel once it has been closed.
         */

        super.close();

        try {
        
        // Release the write cache.
        writeCache = null;
        
        try {

            if (raf != null) {

//                FileLockUtility.closeFile(file, raf);
                synchronized (this) {
                    if (raf != null && raf.getChannel().isOpen()) {
                        raf.close();
                    }
                }

            }
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        } finally {

            /*
             * Ensure that we release the buffer.
             */
            final ByteBuffer tmp = buffer;

            if (tmp != null) {

                buffer = null;

                try {

                    BUFFER_POOL.release(tmp);
                    
                } catch (Throwable t) {
                    
                    throw new RuntimeException(t);
                    
                }
                
            }

        }
        
    }

    public void deleteResources() {
        
        if (isOpen()) {

            throw new IllegalStateException();

        }
        
        if (!file.delete()) {

            throw new RuntimeException("Could not delete file: "
                    + file.getAbsoluteFile());

        }
        
    }

    final public long getExtent() {

        return extent;

    }

    final public long getUserExtent() {
        
        return userExtent;
        
    }

    /**
     * Note: {@link ClosedChannelException} and
     * {@link AsynchronousCloseException} can get thrown out of this method
     * (wrapped as {@link RuntimeException}s) if a reader task is interrupted.
     */
    public ByteBuffer read(final long addr) {

        final long begin = System.nanoTime();
        
        if (addr == 0L)
            throw new IllegalArgumentException(ERR_ADDRESS_IS_NULL);

        final long offset = getOffset(addr);

        final int nbytes = getByteCount(addr);

        if (nbytes == 0) {

            throw new IllegalArgumentException(ERR_RECORD_LENGTH_ZERO);

        }

        if (offset + nbytes > nextOffset) {

            throw new IllegalArgumentException(ERR_ADDRESS_NOT_WRITTEN);

        }

        if (offset + nbytes < bufferSize) {
            
            /*
             * Create a view onto the same buffer with distinct position, limit
             * and mark. This allows us to perform concurrent reads on the
             * buffer which in turn supports concurrent execution of concurrent
             * transactions reading from various historical states.
             */
            final ByteBuffer view;
            synchronized (this) {
                /*
                 * Note: Synchronization is required to have asReadOnlyBuffer()
                 * construct a consistent view with concurrent writers. If you
                 * do not synchronize then an IllegalArgumentException can get
                 * tossed out of here. The problem is that the operation is not
                 * atomic without synchronization (concurrent operations can
                 * modify the limit and position rendering the result
                 * inconsistent). This problem was revealed by the
                 * AbstractMRMWTestCase, but it does not show up on every run.
                 */
                view = buffer.asReadOnlyBuffer();
            }

            // return a read-only view onto the data in the store.

            view.limit((int) offset + nbytes);
            view.position((int) offset);

            // the slice will show only the record being read.

            return view.slice();

        }
        
        /*
         * Allocate a new buffer of the exact capacity.
         * 
         * Note: we do this even if we are reading from the writeCache since the
         * writeCache may be flushed and re-written while the caller is holding
         * onto the returned buffer. If the buffer were a view onto the
         * writeCache, then this would cause the data in the returned view to
         * change!
         */

        final ByteBuffer dst = ByteBuffer.allocate(nbytes);

        /*
         * We need to synchronize before we test the write cache since otherwise
         * the cache contents could change asynchronously.
         * 
         * FIXME The synchronization block also covers the case when we have to
         * read through to the disk. Ideally we would not have to remain
         * synchronized for that operation, but inconsistencies have been
         * observed when synchronization is not maintained during the read. I am
         * not sure why this is required, but corrupt data can otherwise be
         * demonstrated by AbstractMRMWTestCase.
         * 
         * Note: this issue appears to be an interaction with the OS or hardware
         * disk cache as the problem is generally demonstrated only after the
         * cache has been given some time to "clear". I have seen this problem
         * using Java 1.5.0_07 (-server -Xms1g -Xmx1g
         * -XX:MaxDirectMemorySize=256M) and Windows/XP service pack 2 but I
         * have not tested on other platforms yet.
         * 
         * Performance is somewhat better if you do not synchronize this block
         * of code. However, the differences are not that extreme. As measured
         * by AbstractMRMWTestCase (timeout=10, nclients=20, percentReaders=.8)
         * the performance is:
         * 
         * write 3.3, read 11.4 mb/s with sychronized(this)
         * 
         * write 3.6, read 13.2 mb/s without sychronized(this)
         * 
         * FIXME Also of interest, the JRockit VM corresponding to 1.5.0_06
         * performs significantly worse on the same test. Check out some other
         * VM and OS versions and see what is going on here!
         * 
         * @todo If you are NOT synchronized here then NIO READ operations can
         * be concurrent with WRITEs on the channel and there are methods on
         * this class that DO NOT retry writes if the channel is concurrently
         * closed! Those methods would need to be modified to retry in order for
         * this class to remain thread-safe. ( @todo DirectBuffer probably has
         * the same problem.)
         */

        synchronized (this) 
        {

            if (nbytes > counters.maxReadSize) {

                counters.maxReadSize = nbytes;

            }
            
            /*
             * Check the write cache for this address.
             */

            if (writeCache != null) {

                final long beginCache = System.nanoTime();
                
                final ByteBuffer tmp = writeCache.read(addr, nbytes);
                
                if (tmp != null) {
                 
                    /*
                     * Copy the data into the newly allocated buffer.
                     */

                    // copy the data into [dst].
                    dst.put(tmp);

                    // flip buffer for reading.
                    dst.flip();

                    /*
                     * Update counters while synchronized.
                     */
                    counters.nreads++;
                    counters.bytesRead+=nbytes;
                    counters.ncacheRead++;
                    counters.elapsedReadNanos+=(System.nanoTime()-begin);

                    // return the new buffer.
                    return dst;

                } else {
                    
                    counters.elapsedCacheReadNanos+=(System.nanoTime()-beginCache);
                    
                }
                
            }

            /*
             * read through to the disk.
             */

            final long beginDisk = System.nanoTime();
            
            // the offset into the disk file.
            final long pos = offset + headerSize;

            try {

                counters.ndiskRead += FileChannelUtility.readAll(opener, dst,
                        pos);

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            // flip for reading.
            dst.flip();

            /*
             * Update counters while synchronized.
             */
            counters.nreads++;
            counters.bytesRead+=nbytes;
            counters.bytesReadFromDisk+=nbytes;
            counters.elapsedReadNanos+=(System.nanoTime()-begin);
            counters.elapsedDiskReadNanos+=(System.nanoTime()-beginDisk);
            
            // return the buffer.
            return dst;

        } // synchronized(this)

    }

    /**
     * Used to re-open the {@link FileChannel} in this class.
     */
    private final IReopenChannel opener = new IReopenChannel() {

        public String toString() {
            
            return file.toString();
            
        }
        
        public FileChannel reopenChannel() throws IOException {

            return BufferedDiskStrategy.this.reopenChannel();

        }
        
    };
    
    /**
     * This method transparently re-opens the channel for the backing file.
     * <p>
     * Note: This method is synchronized so that concurrent readers do not try
     * to all open the store at the same time.
     * 
     * @todo This method is ONLY invoked by readers. It should be used for
     *       writers as well. Note that this method WILL NOT be invoked by
     *       {@link FileChannelUtility} if the channel was closed by an
     *       interrupt in the current thread (a different exception is thrown).
     */
    synchronized private FileChannel reopenChannel() throws IOException {

        assertOpen();

        if (raf != null && raf.getChannel().isOpen()) {
            
            /* The channel is still open.  If you are allowing concurrent reads
             * on the channel, then this could indicate that two readers each 
             * found the channel closed and that one was able to re-open the
             * channel before the other such that the channel was open again
             * by the time the 2nd reader got here.
             */
            
            return raf.getChannel();
            
        }
        
        // open the file.
        this.raf = new RandomAccessFile(file, fileMode);

        if (INFO)
            log.info("(Re-)opened file: " + file);

        try {

            /*
             * Request a shared file lock.
             */

            final boolean readOnly = "r".equals(fileMode);

            if (raf.getChannel()
                    .tryLock(0, Long.MAX_VALUE, readOnly/* shared */) == null) {

                /*
                 * Note: A null return indicates that someone else holds the
                 * lock. This can happen if the platform does not support shared
                 * locks or if someone requested an exclusive file lock.
                 */

                try {
                    raf.close();
                } catch (Throwable t) {
                    // ignore.
                }

                throw new IOException("File already locked? file=" + file);

            }

        } catch (IOException ex) {

            /*
             * Note: This is true of NFS volumes. This is Ok and should be
             * ignored. However the backing file is not protected against
             * accidental deletes or overwrites.
             */

            if (INFO)
                log.info("FileLock not supported: file=" + file, ex);

        }

        counters.nreopen++;
        
        return raf.getChannel();
        
    }
    
    public long allocate(final int nbytes) {
        
        if (isReadOnly())
            throw new IllegalStateException(ERR_READ_ONLY);
        
        if (nbytes <= 0)
            throw new IllegalArgumentException("Bad record size");
        
        final long addr; // address in the store.
        
        synchronized(this) {
            
            /*
             * The offset at which the record will be written on the disk file
             * (not adjusted for the root blocks).
             */
            final long offset = nextOffset;

            /*
             * Make sure that the allocated region of the file exists.
             */
            overflow(offset, nbytes);
            
            /* 
             * Formulate the address that can be used to recover that record.
             */
            addr = toAddr(nbytes, offset);
            
            /*
             * Increment the offset of the next address to be assigned by the
             * #of bytes in the record.
             */
            nextOffset += nbytes;
        
        }

        return addr;
        
    }
    
    public void update(final long addr, final int off, final ByteBuffer data) {

        if (addr == 0L)
            throw new IllegalArgumentException(ERR_ADDRESS_IS_NULL);

        if (off < 0)
            throw new IllegalArgumentException("Offset is negative");
        
        if (data == null)
            throw new IllegalArgumentException(ERR_BUFFER_NULL);

        if (isReadOnly())
            throw new IllegalStateException(ERR_READ_ONLY);
        
        // The offset of the record in the store (not adjusted for the root blocks).
        final long addrOffset = getOffset(addr);

        // The size of the record (NOT the #of bytes to be written).
        final int addrByteCount = getByteCount(addr);
        
        if (addrOffset + addrByteCount > nextOffset) {

            throw new IllegalArgumentException(ERR_ADDRESS_NOT_WRITTEN);

        }

        // #of bytes to be updated on the pre-existing record.
        final int nbytes = data.remaining();
        
        if (nbytes == 0)
            throw new IllegalArgumentException(ERR_BUFFER_EMPTY);
        
        if (off + nbytes > addrByteCount) {

            throw new IllegalArgumentException("Would overrun record");

        }
        
        final long begin = System.nanoTime();
        
        synchronized(this) {

            try {

                if (writeCache != null) {

                    /*
                     * Check the writeCache. If the record is found in the write
                     * cache then we just update the slice of the record
                     * corresponding to the caller's request. This is a common
                     * use case and results in no IO.
                     */

                    final long beginCache = System.nanoTime();

                    try {

                        final ByteBuffer view = writeCache.read(addr,addrByteCount);

                        if (view != null) {

                            // adjust the limit on the record in the write
                            // cache.
                            view.limit(off + nbytes);

                            // adjust the position on the record in the write
                            // cache.
                            view.position(off);

                            // copy the caller's data onto the record in the
                            // write
                            // cache.
                            view.put(data);

                            // count this as a cache write.
                            counters.ncacheWrite++;

                            // Done.
                            return;

                        }

                    } finally {

                        // track the write cache time.
                        counters.elapsedCacheWriteNanos += (System.nanoTime() - beginCache);

                    }

                }

                /*
                 * Either the writeCache is disabled or the record was not found
                 * in the write cache so just write the record directly on the
                 * disk.
                 * 
                 * Note: for this case we might be able to move the write
                 * outside of the synchronized() block IFF we also cloned the
                 * data (since the caller is allowed to modify the buffer as
                 * soon as write() returns).
                 * 
                 * Note: We MUST NOT update the writeCacheOffset since we are
                 * probably writing behind the end of the file (this is contrary
                 * to a normal write write is an append at the end of the file).
                 */

                writeOnDisk(data, addrOffset + off/* adjustedOffset */, false/* append */);

            } finally {

                /*
                 * Update counters while we are synchronized. If done outside of
                 * the synchronization block then we need to use AtomicLongs
                 * rather than primitive longs.
                 */

                counters.nwrites++;
                counters.bytesWritten += nbytes;
                counters.elapsedWriteNanos += (System.nanoTime() - begin);

                if(nbytes > counters.maxWriteSize) {
                    
                    counters.maxWriteSize = nbytes;
                    
                }
                
            }
            
        } // synchronized
        
    }
    
    public long write(final ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException(ERR_BUFFER_NULL);

        if (isReadOnly())
            throw new IllegalStateException(ERR_READ_ONLY);
        
        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException(ERR_BUFFER_EMPTY);

        final long begin = System.nanoTime();
        
        final long addr; // address in the store.
        synchronized(this) {

            /*
             * Allocate address for a new record with [nbytes] of data.
             */
            addr = allocate(nbytes);
            
            /*
             * The offset at which the record will be written on the disk file
             * (not adjusted for the root blocks).
             */
            final long offset = getOffset(addr);

            if (offset + nbytes > bufferSize && writeCache != null) {

                /*
                 * The data to be written lies at least in part beyond the end
                 * of the [buffer]. In this case we consider the [writeCache]
                 * (there is no need to use the writeCache when the data lies
                 * fully within the [buffer]).
                 */
                
                if (nbytes + writeCache.position() > writeCache.capacity()) {

                    /*
                     * Flush the writeCache if the record would cause it to
                     * overflow.
                     */
                    flushWriteCache();

                }

                /*
                 * This record is to big for the write cache so we write the
                 * record directly on the disk.
                 */

                if (nbytes > writeCache.capacity()) {

                    writeOnDisk(data, offset, true/*append*/);

                } else {

                    /*
                     * Queue up the write in the writeCache.
                     */
                    
                    final long beginCache = System.nanoTime();
                    
                    writeCache.write(addr, data);

                    counters.ncacheWrite++;

                    counters.elapsedCacheWriteNanos+=(System.nanoTime()-beginCache);

                }
                
            } else {

                /*
                 * The writeCache is disabled so just write the record directly
                 * on the disk.
                 * 
                 * Note: for this case we might be able to move the write
                 * outside of the synchronized() block IFF we also cloned the
                 * data (since the caller is allowed to modify the buffer as
                 * soon as write() returns).
                 * 
                 * Note: We update the writeCacheOffset even when the writeCache
                 * is disabled just to keep it consistent. This allows for the
                 * possibility that the writeCache could be enabled and disabled
                 * at will.
                 */
                
                writeOnDisk(data, offset, true/* append */);

            }

            /*
             * Update counters while we are synchronized. If done outside of the
             * synchronization block then we need to use AtomicLongs rather than
             * primitive longs.
             */

            counters.nwrites++;
            counters.bytesWritten+=nbytes;
            counters.elapsedWriteNanos+=(System.nanoTime() - begin);

            if(nbytes > counters.maxWriteSize) {
                
                counters.maxWriteSize = nbytes;
                
            }

        } // synchronized
        
        return addr;

    }

    /**
     * Make sure that the file is large enough to accept a write of <i>nbytes</i>
     * starting at <i>offset</i> bytes into the file.
     * <p>
     * Note: The caller MUST be synchronized on <i>this</i>.
     * 
     * @param offset
     *            The offset into the file (NOT adjusted for the root blocks).
     * @param nbytes
     *            The #of bytes to be written at that offset.
     */
    private void overflow(final long offset, final int nbytes) {

        final long needed = (offset + nbytes) - userExtent;

        if (needed > 0) {
            
            if (!overflow(needed)) {

                throw new OverflowException();

            }

        }

    }
    
    /**
     * Write the data on the disk (synchronous).
     * <p>
     * Note: The caller MUST be synchronized on <i>this</i>.
     * <p>
     * Note: This updates {@link #writeCacheOffset} as well (but only if the
     * write is an append).
     * <p>
     * Note: It is possible for {@link #update(long, int, ByteBuffer)} to force
     * a non-append write that is beyond the {@link #writeCacheOffset}. This
     * will occur if the record that is being updated is too large for the
     * {@link #writeCache} while there are also records buffered by this write
     * cache.
     * 
     * @param data
     *            The data. The bytes from the current
     *            {@link ByteBuffer#position()} to the
     *            {@link ByteBuffer#limit()} will be written and the
     *            {@link ByteBuffer#position()} will be advanced to the
     *            {@link ByteBuffer#limit()} . The caller may subsequently
     *            modify the contents of the buffer without side effects (i.e.,
     *            the data are copied onto the disk).
     * @param offset
     *            The offset in the file at which the data will be written.
     * @param append
     *            <code>true</code> iff the write is an append (most record
     *            writes are appends).
     */
    void writeOnDisk(final ByteBuffer data, final long offset, final boolean append) {

        final long begin = System.nanoTime();
        
        final int nbytes = data.remaining();

        // make sure that the file is large enough.
        overflow(offset, nbytes);

        /* 
         * The position in the file at which the record will be written
         * (this is adjusted for the root blocks).
         */

        final long pos = offset + headerSize;

        try {

            /*
             * Write bytes in [data] from position to limit onto the channel.
             * 
             * Note: Since the caller is synchronized on [this] it SHOULD NOT be
             * possible for a reader is to be interrupted during a concurrent
             * NIO operation and thus the channel SHOULD NOT be asynchronously
             * closed while we are writing on it.
             */

            counters.ndiskWrite += FileChannelUtility.writeAll(getChannel(),
                    data, pos);

            if (offset + nbytes < bufferSize) {

                /*
                 * Copy the data into the [buffer].
                 * 
                 * Note: The data MUST be copied within the synchronized() block
                 * since otherwise overflow() could cause the buffer reference
                 * to be invalidated. (Likewise, we do not "own" the limit and
                 * position on the [buffer] unless everyone is synchronized.)
                 */

                buffer.limit((int) offset + nbytes);
                buffer.position((int) offset);
                buffer.put(data);
                
            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        // update the next offset at which data will be written on the disk.
        if(append) {
            
            writeCacheOffset += nbytes;
            
        }

        counters.bytesWrittenOnDisk += nbytes;
        counters.elapsedDiskWriteNanos += (System.nanoTime() - begin);

    }

    public ByteBuffer readRootBlock(final boolean rootBlock0) {
        
        if(!isOpen()) throw new IllegalStateException();

        final ByteBuffer tmp = ByteBuffer
                .allocate(RootBlockView.SIZEOF_ROOT_BLOCK);

        try {

            /*
             * Note: Synchronized on [this] to pervent concurrent NIO requests
             * which might lead to the channel being closed asynchronously.
             */
//            synchronized (this) {

                FileChannelUtility.readAll(opener, tmp,
                        rootBlock0 ? FileMetadata.OFFSET_ROOT_BLOCK0
                                : FileMetadata.OFFSET_ROOT_BLOCK1);
                
//            }
            
            tmp.position(0); // resets the position.

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        return tmp;

    }
    
    public void writeRootBlock(final IRootBlockView rootBlock,
            final ForceEnum forceOnCommit) {

        /*
         * Note: Root blocks are written for a temporary store in support of
         * rollback().
         */
//        if(temporaryStore) {
//            
//            /*
//             * Note: There are NO ROOT BLOCKS for a temporary store. Root blocks
//             * are only useful for stores that can be re-opened, and you can not
//             * re-open a temporary store - the backing file is always deleted
//             * when the store is closed. The AbstractJournal still formats the
//             * root blocks and retains a reference to the current root block,
//             * but it is NOT written onto the file.
//             */
//            
//            return;
//            
//        }
        
        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        try {
            
            final ByteBuffer data = rootBlock.asReadOnlyBuffer();
            
            final long pos = rootBlock.isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
                    : FileMetadata.OFFSET_ROOT_BLOCK1;

            /*
             * Note: Synchronized on [this] to pervent concurrent NIO requests
             * which might lead to the channel being closed asynchronously.
             */
            synchronized(this) {

                FileChannelUtility.writeAll(getChannel(), data, pos);
                
            }

            if (forceOnCommit != ForceEnum.No) {

                force(forceOnCommit == ForceEnum.ForceMetadata);

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        if (DEBUG)
            log.debug("wrote root block: "+rootBlock);
        
        counters.nwriteRootBlock++;
        
    }

    synchronized public void truncate(final long newExtent) {

        final long newUserExtent =  newExtent - headerSize;
        
        if (newUserExtent < getNextOffset() ) {
           
            throw new IllegalArgumentException(ERR_TRUNCATE);
            
        }

        if(newUserExtent == getUserExtent()) {
            
            // NOP.
            return;
            
        }
        
        try {

            // extend (or truncate) the file.
            getRandomAccessFile().setLength(newExtent);
            
            /*
             * Since we just changed the file length we force the data to disk
             * and update the file metadata. this is a relatively expensive
             * operation but we want to make sure that we do not loose track of
             * a change in the length of the file.
             * 
             * @todo an alternative would be to set a marker on the buffer such
             * that the next force() also forced the metadata to disk.
             */
            
            force(true);

            counters.ntruncate++;
            
            if(WARN)
                log.warn("newLength=" + cf.format(newExtent) + ", file="
                                + file);
            
            if(INFO)
                log.info(getCounters().toString());
            
        } catch(IOException ex) {
         
            /*
             * I've see an IOException "The handle is invalid" tossed here (just
             * once). A bit of searching around suggests that perhaps the
             * RandomAccessFile was concurrently closed? Something to look out
             * for if it happens again. [@todo probably a concurrent reader was
             * interrupted, in which case this method should just try the
             * setLength() operation again.]
             */
            throw new RuntimeException(ex);
            
        }
 
        this.userExtent = newUserExtent;
        
        this.extent = newExtent;
        
    }

    synchronized public long transferTo(RandomAccessFile out)
            throws IOException {
        
        if (out == null)
            throw new IllegalArgumentException();
        
        /*
         * Note: Force the write cache to the disk so that all the data we want
         * to transfer from channel to channel are actually on the source
         * channel!
         * 
         * Note: This also handles the case for a Temporary store where the
         * backing file has not even been created yet.
         */
        
        flushWriteCache();
        
        return super.transferFromDiskTo(this, out);
        
    }

    /**
     * Extended to discard the write cache.
     * <p>
     * Note: The file is NOT closed and re-opened in a read-only mode in order
     * to avoid causing difficulties for concurrent readers.
     */
    public void closeForWrites() {

        // sets the [readOnly] flag.
        super.closeForWrites();

        // discard the write cache.
        writeCache = null;
        
    }
    
}
