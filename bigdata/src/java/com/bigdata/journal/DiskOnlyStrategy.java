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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Disk-based journal strategy.
 * <p>
 * Writes are buffered in a write cache. The cache is flushed when it would
 * overflow. As a result only large sequential writes are performed on the
 * store. Reads read through the write cache for consistency.
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
 *    /sbin/hdparm -W 0 /dev/hda 0 Disable write caching
 *    /sbin/hdparm -W 1 /dev/hda 1 Enable write caching
 * </pre>
 * 
 * @todo The flush of the write cache could be made asynchronous if we had two
 *       write buffers, but that increases the complexity significantly. It
 *       would have to be synchronous if invoked from {@link #force(boolean)} in
 *       any case.
 *       <p>
 *       Reconsider a 2nd buffer so that we can avoid waiting on the writes to
 *       disk. Use
 *       {@link Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)
 *       to obtain the 2nd (daemon) thread and an. {@link Exchanger}.
 *       <p>
 *       Consider the generalization where a WriteCache encapulates the logic
 *       that exists in this class and where we have a {@link BlockingQueue} of
 *       available write caches. There is one "writable" writeCache object at
 *       any given time, unless we are blocked waiting for one to show up on the
 *       availableQueue. When a WriteCache is full it is placed onto a
 *       writeQueue. A thread reads from the writeQueue and performs writes,
 *       placing empty WriteCache objects onto the availableQueue. Sync places
 *       the current writeCache on the writeQueue and then waits on the
 *       writeQueue to be empty.
 *       <p>
 *       Consider that a WriteCache also doubles as a read cache IF we create
 *       write cache objects encapsulating reads that we read directly from the
 *       disk rather than from a WriteCache. In this case we might do a larger
 *       read so as to populate more of the WriteCache object in the hope that
 *       we will have more hits in that part of the journal.
 *       <p>
 *       Note: Most reads are nodes and leaves of {@link BTree}s which are
 *       already cached by the btree and should not stay long in a read cache in
 *       the journal itself - they will only be re-read if we reload the
 *       {@link BTree} from its {@link BTreeMetadata}. Some reads are
 *       {@link BTreeMetadata} records and there are a few {@link ICommitRecord}s.
 *       That is all the different kinds of objects in the store at this time.
 *       If we develop a read/write variant of the store then the metadata for
 *       the allocation blocks will need to be cached.
 *       <p>
 *       Currently, GC of nodes in the btree is driving reads.
 *       <p>
 *       Async read ahead should be explored for rangeIterators in the
 *       {@link BTree}. This could reduce latency waiting on the disk to zero
 *       for range scans.
 * 
 * FIXME Add lazy creation of the backing file so that we can use the
 * {@link DiskOnlyStrategy} for temporary stores as well. The backing file will
 * never be created unless the write cache overflows. Since we write the root
 * blocks when we create the file, we have to be careful and make sure that the
 * store is initialized properly. Note that this will also mean that the temp
 * store can support commits, which it does not really need to do.
 * 
 * @todo A separate read cache could be used for hot records, but the B+Tree
 *       implementations already buffer nodes and leaves so this is unlike to
 *       help anything using indices rather than just the {@link IRawStore}
 *       interface. For this reason you can expect performance tests such as
 *       {@link AbstractMRMWTestCase} to under-report the effective read
 *       performance of the store. Tests written at the B+Tree level are a
 *       better guide here.
 * 
 * @todo test verifying that large records are written directly and that the
 *       write cache is properly flush beforehand.
 * 
 * @todo test verifying that the write cache can be disabled.
 * 
 * @todo test verifying that {@link #writeCacheOffset} is restored correctly on
 *       restart (ie., you can continue to append to the store after restart and
 *       the result is valid).
 * 
 * @todo test verifying that the buffer position and limit are updated correctly
 *       by {@link #write(ByteBuffer)} regardless of the code path.
 * 
 * @todo config parameter for buffer size, direct vs heap allocation of the
 *       {@link #writeCache}, and whether or not the {@link #writeCache} is
 *       enabled.
 * 
 * @todo Retro fit the concept of a write cache into the
 *       {@link DirectBufferStrategy} so that we defer writes onto the disk
 *       until (a) a threshold of data has been buffered; or (b)
 *       {@link #force(boolean)} is invoked. Note that the implementation will
 *       be a bit different since the Direct mode is already fully buffered so
 *       we do not need to allocate a separate writeCache. However, we will
 *       still need to track the {@link #writeCacheOffset} and maintain a
 *       {@link #writeCacheIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Disk
 */
public class DiskOnlyStrategy extends AbstractBufferStrategy implements
        IDiskBasedStrategy {
    
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
    /*private*/ /*final*/ RandomAccessFile raf;

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
     * Optional {@link WriteCache}.
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
        private ByteBuffer buf;
        
        /**
         * An index into the write cache used for read through on the cache. The
         * keys are the addresses that would be used to read the corresponding
         * record. The values are the position in {@link #buffer} where that record
         * is buffered. A cache miss means that you need to read the record from the
         * disk.
         */
        final private Map<Long,Integer> writeCacheIndex;
        
        /**
         * The starting position in the buffer for data that has not been
         * written to the disk.
         * 
         * FIXME not used yet.
         * 
         * @see Task
         */
        private int start = 0;
        
        public WriteCache(int capacity) {
            
            if(capacity<=0) throw new IllegalArgumentException();
            
            /*
             * An estimate of the #of records that might fit within the write
             * cache. This is based on an assumption that the "average" record
             * is 4k. This is used solely to assign the initial capacity to the
             * writeCacheIndex.
             */
            final int indexDefaultCapacity = capacity / (4 * Bytes.kilobyte32);

            // allocate the write cache : @todo make using a direct buffer an
            // option.
            this.buf = ByteBuffer.allocate(capacity);

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
            writeOnDisk(buf, writeCacheOffset);

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
        void write(long addr, ByteBuffer data) {

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
         * @return A view onto the record in the write cache buffer -or-
         *         <code>null</code> iff the record does not lie within this
         *         {@link WriteCache}.
         *         <p>
         *         Note: The caller MUST copy the data from the view since
         *         concurrent operations may result in the write cache being
         *         flushed and the view overwritten with new data.
         */
        ByteBuffer read(long addr,int nbytes) {
                
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

            // view onto the writeCache with its own limit and position.
            ByteBuffer tmp = buf.asReadOnlyBuffer();

            // adjust the view to just the record of interest.
            tmp.limit(pos + nbytes);
            
            tmp.position(pos);
            
            // return the view.
            return tmp;

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
        
        return getRandomAccessFile().getChannel();
        
    }

    /**
     * Counters for {@link IRawStore} access, including operations that read or
     * write through to the underlying media.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo force counter.  force elapsed time counter.
     * 
     * @todo reopenChannel() counter. re-open elapsed time counter.
     * 
     * @todo refactor so that all buffer strategies support counters.
     */
    public static class Counters {
        
        /**
         * #of read requests.
         */
        long nreads;

        /**
         * #of read requests that are satisified by our write cache (vs the
         * OS or disk level write cache).
         */
        long ncacheRead;

        /**
         * #of read requests that read through to the backing file.
         */
        long ndiskRead;
        
        /**
         * #of bytes read.
         */
        long bytesRead;

        /**
         * The size of the largest record read.
         */
        long maxReadSize;
        
        /**
         * Total elapsed time for reads.
         */
        long elapsedReadNanos;

        /**
         * Total elapsed time for reading on the disk.
         */
        long elapsedDiskReadNanos;
        
        /**
         * #of write requests.
         */
        long nwrites;

        /**
         * #of write requests that are absorbed by our write cache (vs the OS or
         * disk level write cache).
         */
        long ncacheWrite;
        
        /**
         * #of write requests that write through to the backing file.
         */
        long ndiskWrite;

        /**
         * The size of the largest record written.
         */
        long maxWriteSize;
        
        /**
         * #of bytes written.
         */
        long bytesWritten;
        
        /**
         * Total elapsed time for writes.
         */
        long elapsedWriteNanos;

        /**
         * Total elapsed time for writing on the disk.
         */
        long elapsedDiskWriteNanos;
        
        /**
         * #of times the data were forced to the disk.
         */
        long nforce;
        
        /**
         * #of times the length of the file was changed (typically, extended).
         */
        long ntruncate;
        
        /**
         * #of times the file has been reopened after it was closed by an
         * interrupt.
         */
        long nreopen;
        
        /**
         * #of times one of the root blocks has been written.
         */
        long nwriteRootBlock;
        
        /**
         * Human readable representation of the counters.
         */
        public String toString() {
            
            StringBuilder sb = new StringBuilder();

            // IRawStore statistics.
            {

                final double elapsedReadSecs = (elapsedReadNanos / 1000000000.);

                final String bytesReadPerSec = (elapsedReadSecs == 0L ? "N/A" : ""
                        + commaFormat.format(bytesRead / elapsedReadSecs));

                sb.append("store(read): #read=" + commaFormat.format(nreads)
                        + ", bytesRead=" + commaFormat.format(bytesRead)
                        + ", secs="+secondsFormat.format(elapsedReadSecs)
                        + ", bytesPerSec=" + bytesReadPerSec
                        + ", maxRecordSize=" + commaFormat.format(maxReadSize)
                        + "\n");
                
                final double elapsedWriteSecs = (elapsedWriteNanos / 1000000000.);

                final String bytesWrittenPerSec = (elapsedWriteSecs == 0. ? "N/A"
                        : ""+ commaFormat.format(bytesWritten
                                        / elapsedWriteSecs));

                sb.append("store(write): #write=" + commaFormat.format(nwrites)
                        + ", bytesWritten=" + commaFormat.format(bytesWritten)
                        + ", secs="+secondsFormat.format(elapsedWriteSecs)
                        + ", bytesPerSec=" + bytesWrittenPerSec  
                        + ", maxRecordSize=" + commaFormat.format(maxWriteSize)
                        + "\n");
                
            }

            // cache statisitics.
            if (ncacheRead > 0 || ncacheWrite > 0) {

                String cacheReadRate = (nreads == 0 ? "N/A" : ""
                        + percentFormat.format((double) ncacheRead / nreads));

                String cacheWriteRate = (nwrites == 0 ? "N/A" : ""
                        + percentFormat.format((double) ncacheWrite / nwrites));

                sb.append("cache(read): " + cacheReadRate + " (#cache="
                        + ncacheRead + ", #disk=" + ndiskRead + ", total="
                        + (ncacheRead + ndiskRead) + ")\n");

                sb.append("cache(write): " + cacheWriteRate + " (#cache="
                        + ncacheWrite + ", #disk=" + ndiskWrite + ", total="
                        + (ncacheWrite + ndiskWrite) + ")\n");
                
            }
            
            // disk statistics.
            {
            
                String bytesPerDiskRead = (ndiskRead == 0 ? "N/A" : commaFormat
                        .format(bytesRead / ndiskRead));

                String bytesPerDiskWrite = (ndiskWrite == 0 ? "N/A"
                        : commaFormat.format(bytesWritten / ndiskWrite));

                double elapsedDiskReadSecs = (elapsedDiskReadNanos / 1000000000.);

                double elapsedDiskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);

                String readLatency = (elapsedDiskReadSecs == 0 ? "N/A"
                        : secondsFormat.format(elapsedDiskReadSecs/ndiskRead));

                String writeLatency = (elapsedDiskWriteSecs == 0 ? "N/A"
                        : secondsFormat.format(elapsedDiskWriteSecs/ndiskWrite));

                sb.append("disk(read): #read=" + ndiskRead + ", bytesPerRead="
                        + bytesPerDiskRead + ", secs="
                        + secondsFormat.format(elapsedDiskReadSecs)
                        + "s, secs/read=" + readLatency + "\n");

                sb.append("disk(write): #write=" + ndiskWrite
                        + ", bytesPerWrite=" + bytesPerDiskWrite + ", secs="
                        + secondsFormat.format(elapsedDiskWriteSecs)
                        + "s, secs/write=" + writeLatency + "\n");

                sb.append("disk(other): #force=" + nforce + ", #extend="
                        + ntruncate + ", #reopen=" + nreopen + ", #rootBlocks="
                        + nwriteRootBlock);
                
            }

            return sb.toString();
            
        }

        static private final NumberFormat commaFormat = NumberFormat.getInstance();
        static private final NumberFormat percentFormat = NumberFormat.getPercentInstance();
        static private final NumberFormat secondsFormat = NumberFormat.getInstance();
        
        static
        {
    
            commaFormat.setGroupingUsed(true);
            commaFormat.setMaximumFractionDigits(0);
            
            secondsFormat.setMinimumFractionDigits(3);
            secondsFormat.setMaximumFractionDigits(3);
            
        }
        
    }
    
    /**
     * Counters on {@link IRawStore} and disk access.
     */
    final public Counters counters = new Counters();
    
    /**
     * Return interesting information about the write cache and file operations.
     * 
     * @todo add information about the write cache configuration.
     */
    public String getStatistics() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("file="+file);

        sb.append(", mode="+fileMode);
        
        sb.append(", nextOffset="+Counters.commaFormat.format(nextOffset));

        sb.append(", extent="+Counters.commaFormat.format(extent));
        
        sb.append("\n");

        sb.append(counters.toString());
        
        return sb.toString();
        
    }
    
    /**
     * 
     * @param maximumExtent
     * @param fileMetadata
     * @param writeCacheCapacity
     *            The #of bytes that can be buffered by the write cache and also
     *            the maximum length of a sequential write operation when
     *            flushing the write cache to disk. Large sequential writes far
     *            outperform a set of smaller writes since the alliviate the
     *            need to repeatedly seek on the disk.
     */
    DiskOnlyStrategy(long maximumExtent, FileMetadata fileMetadata, int writeCacheCapacity) {

        super(fileMetadata.extent, maximumExtent, fileMetadata.offsetBits,
                fileMetadata.nextOffset, BufferMode.Disk);

        this.file = fileMetadata.file;

        this.fileMode = fileMetadata.fileMode;
        
        this.raf = fileMetadata.raf;

        this.extent = fileMetadata.extent;

        this.headerSize = FileMetadata.headerSize0;
        
        this.userExtent = extent - headerSize;

        if (writeCacheCapacity > 0) {

            // Enable the write cache.

            log.info("Enabling writeCache: capacity="+writeCacheCapacity);

            writeCache = new WriteCache( writeCacheCapacity );
            
            /*
             * Start a thread that will be used to asynchronously drive data in
             * the write cache to the disk.
             */

            if(false) {
                
                writeService = Executors
                        .newSingleThreadExecutor(DaemonThreadFactory
                                .defaultThreadFactory());
                
            }

        } else {
            
            writeCache = null;
            
        }

        // the offset at which the next record would be written on the file.
        writeCacheOffset = fileMetadata.nextOffset;
        
    }
    private ExecutorService writeService = null;

    /**
     * Writes all data in the {@link WriteCache} onto the disk file.
     * 
     * FIXME In order to be able to asynchronously drive data in the write cache
     * to the disk we need to track the offset of the 1st byte in {@link #buf}
     * that has not been written onto the disk. {@link #flush()} will have to be
     * modified so that it awaits the current task writing out data (if any) and
     * then synchronously writes out the remaining data itself.
     * <P>
     * We can submit a new task each time a record is written, but we do not
     * want to have more than one task on the queue. Alternatively, just make
     * the thread runnable and coordinate with a {@link Lock} and
     * {@link Condition}s.
     * 
     * in order to handle the end of the buffer gracefully we might have to just
     * and the application a new one when the current one is full. we can then
     * trigger a write once the buffer is at least X% (or #M) and if the next
     * record would cause an overflow in anycase. the write can be a partial
     * write of the buffer or not.
     * 
     * using multiple buffers means that we have to search the chain of buffers
     * still holding data as our write cache. if we make the buffer change over
     * point synchronous then this will only be a single buffer. else a chain.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class Task implements Runnable {

        public void run() {

            throw new UnsupportedOperationException();
            
        }
        
    }
    
    final public boolean isStable() {
        
        return true;
        
    }

    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    /**
     * {@link #flushWriteCache() flushs} the {@link #writeCache} before syncing
     * the disk.
     */
    public void force(boolean metadata) {

        // flush all pending writes to disk.
        flushWriteCache();

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

        if (writeService != null) {
            
            /*
             * Shutdown the write service.
             */
            
            writeService.shutdownNow();
            
            try {
            
                if (!writeService.awaitTermination(2, TimeUnit.SECONDS)) {
                
                    log.warn("Timeout awaiting termination");
                    
                }
                
            } catch (InterruptedException ex) {
                
                log.warn("Interrupted awaiting termination: " + ex, ex);
                
            }
            
        }
        
        // Release the write cache.
        writeCache = null;
        
        try {

            raf.close();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public void delete() {
        
        if (isOpen()) {

            throw new IllegalStateException();

        }
        
        if( ! file.delete() ) {
            
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
    public ByteBuffer read(long addr) {

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

        /*
         * Allocate a new buffer of the exact capacity.
         * 
         * Note: we do this even if we are reading from the writeCache since the
         * writeCache may be flushed and re-written while the caller is holding
         * onto the returned buffer. If the buffer were a view onto the
         * writeCache, then this would cause the data in returned view to
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
         */

        synchronized (this) {

            if (nbytes > counters.maxReadSize) {

                counters.maxReadSize = nbytes;

            }
            
            /*
             * Check the write cache for this address.
             */

            if (writeCache != null) {
                
                ByteBuffer tmp = writeCache.read(addr, nbytes);
                
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

                }
                
            }

            /*
             * read through to the disk.
             */

            final long beginDisk = System.nanoTime();
            
            // the offset into the disk file.
            final long pos = offset + headerSize;

            for(int ntries=0; ntries<3; ntries++) {

                if(ntries>0) {

                    /*
                     * Note: clear if we are retrying since the buffer may have
                     * been modified by a partial read.
                     */ 

                    dst.clear();
                    
                }
                
                try {

                    // copy the data into the buffer.
                    final int nread = getChannel().read(dst, pos);

                    if (nread != nbytes) {

                        throw new RuntimeException("Expected to read " + nbytes
                                + " bytes but read " + nread);

                    }

                } catch (ClosedByInterruptException ex) {
                    
                    /*
                     * This indicates that this thread was interrupted. We
                     * always abort in this case.
                     */
                    
                    throw new RuntimeException(ex);

                } catch (AsynchronousCloseException ex) {
                    
                    /*
                     * The channel was closed asynchronously while blocking
                     * during the read. If the buffer strategy still thinks that
                     * it is open then we re-open the channel and re-read.
                     */
                    
                    if(reopenChannel()) continue;
                    
                    throw new RuntimeException(ex);
                    
                } catch (ClosedChannelException ex) {
                    
                    /*
                     * The channel is closed. If the buffer strategy still
                     * thinks that it is open then we re-open the channel and
                     * re-read.
                     */

                    if(reopenChannel()) continue;

                    throw new RuntimeException(ex);
                    
                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

            }

            // flip for reading.
            dst.flip();

            /*
             * Update counters while synchronized.
             */
            counters.nreads++;
            counters.bytesRead+=nbytes;
            counters.ndiskRead++;
            counters.elapsedReadNanos+=(System.nanoTime()-begin);
            counters.elapsedDiskReadNanos+=(System.nanoTime()-beginDisk);
            
            // return the buffer.
            return dst;

        } // synchronized(this)

    }

    /**
     * This method transparently re-opens the channel for the backing file.
     * <p>
     * Note: This method is synchronized so that concurrent readers do not try
     * to all open the store at the same time.
     * <p>
     * Note: This method is ONLY invoked by readers. This helps to ensure that a
     * writer that has been interrupted can not regain access to the channel (it
     * does not prevent it, but re-opening for writers is asking for trouble).
     * 
     * @return true iff the channel was re-opened.
     * 
     * @throws IllegalStateException
     *             if the buffer strategy has been closed.
     *             
     * @throws RuntimeException
     *             if the backing file can not be opened (can not be found or
     *             can not acquire a lock).
     */
    synchronized private boolean reopenChannel() {
        
        if (raf != null && raf.getChannel().isOpen()) {
            
            /* The channel is still open.  If you are allowing concurrent reads
             * on the channel, then this could indicate that two readers each 
             * found the channel closed and that one was able to re-open the
             * channel before the other such that the channel was open again
             * by the time the 2nd reader got here.
             */
            
            return true;
            
        }
        
        if(!isOpen()) {

            // the buffer strategy has been closed.
            
            return false;
            
        }

        try {

            raf = FileMetadata.openFile(file, fileMode, bufferMode);
        
            log.warn("Re-opened file: "+file);
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        counters.nreopen++;
        
        return true;
        
    }
    
    public long write(ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException(ERR_BUFFER_NULL);

        // #of bytes to store.
        final int nbytes = data.remaining();

        if (nbytes == 0)
            throw new IllegalArgumentException(ERR_BUFFER_EMPTY);

        final long begin = System.nanoTime();
        
        final long addr; // address in the store.
        synchronized(this) {
            
            if(nbytes > counters.maxWriteSize) {
                
                counters.maxWriteSize = nbytes;
                
            }
            
            /*
             * The offset at which the record will be written on the disk file
             * (not adjusted for the root blocks).
             */
            final long offset = nextOffset;
            
            // formulate the address that can be used to recover that record.
            addr = toAddr(nbytes, offset);

            if (writeCache != null) {

                /*
                 * Flush the writeCache if the record would cause it to
                 * overflow.
                 */

                if (nbytes + writeCache.position() > writeCache.capacity()) {

                    flushWriteCache();

                }

                /*
                 * This record is to big for the write cache so we write the
                 * record directly on the disk.
                 */

                if (nbytes > writeCache.capacity()) {

                    writeOnDisk(data,nextOffset);

                } else {

                    /*
                     * Queue up the write in the writeCache.
                     */
                    
                    writeCache.write(addr, data);

                    counters.ncacheWrite++;

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
                
                writeOnDisk(data,nextOffset);

            }
            
            /*
             * Whether we wrote the record on the cache or on the disk, we now
             * increment the offset of the next address to be assigned by the
             * #of bytes in the record.
             */
            
            nextOffset += nbytes;

            /*
             * Update counters while we are synchronized. If done outside of the
             * synchronization block then we need to use AtomicLongs rather than
             * primitive longs.
             */

            counters.nwrites++;
            counters.bytesWritten+=nbytes;
            counters.elapsedWriteNanos+=(System.nanoTime() - begin);

        } // synchronized
        
        return addr;

    }

    /**
     * Write the data on the disk (synchronous).
     * <p>
     * Note: This updates {@link #writeCacheOffset} as well.
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
     */
    void writeOnDisk(ByteBuffer data, long offset) {

        final long begin = System.nanoTime();
        
        final int nbytes = data.limit();
        
        /* 
         * The position in the file at which the record will be written
         * (this is adjusted for the root blocks).
         */

        final long pos = offset + headerSize;

        final long needed = (offset + nbytes) - userExtent;

        if (needed > 0) {

            if (!overflow(needed)) {

                throw new OverflowException();

            }

        }

        try {

            /*
             * Write the data onto the channel.
             */

            // writes bytes from position to limit on the channel at pos.
            final int count = getChannel().write(data, pos);

            if (count != nbytes) {

                throw new IOException("Expecting to write " + nbytes
                        + " bytes, but wrote " + count + " bytes.");

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        // update the next offset at which data will be written on the disk.
        writeCacheOffset += nbytes;
                
        counters.ndiskWrite++;
        counters.elapsedDiskWriteNanos+=(System.nanoTime()-begin);

    }
    
    public void writeRootBlock(IRootBlockView rootBlock,ForceEnum forceOnCommit) {

        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        try {

            FileChannel channel = getChannel();

            final int count = channel.write(rootBlock.asReadOnlyBuffer(),
                    rootBlock.isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
                            : FileMetadata.OFFSET_ROOT_BLOCK1);
            
            if(count != RootBlockView.SIZEOF_ROOT_BLOCK) {
                
                throw new IOException("Expecting to write "
                        + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but wrote"
                        + count + " bytes.");
                
            }

            if( forceOnCommit != ForceEnum.No ) {

                force(forceOnCommit==ForceEnum.ForceMetadata);
            
            }

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        counters.nwriteRootBlock++;
        
    }

    synchronized public void truncate(long newExtent) {

        long newUserExtent =  newExtent - headerSize;
        
        if (newUserExtent < getNextOffset() ) {
           
            throw new IllegalArgumentException(ERR_TRUNCATE);
            
        }

        if(newUserExtent == getUserExtent()) {
            
            // NOP.
            return;
            
        }
        
        try {

            // extend the file.
            getRandomAccessFile().setLength(newExtent);
            
            /*
             * since we just changed the file length we force the data to disk
             * and update the file metadata. this is a relatively expensive
             * operation but we want to make sure that we do not loose track of
             * a change in the length of the file.
             * 
             * @todo an alternative would be to set a marker on the buffer such
             * that the next force() also forced the metadata to disk.
             */
            
            force(true);

            counters.ntruncate++;
            
            log.warn("Disk file: newLength="+cf.format(newExtent));
            
            log.info(getStatistics());
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
 
        this.userExtent = newUserExtent;
        
        this.extent = newExtent;
        
    }

    synchronized public long transferTo(RandomAccessFile out)
            throws IOException {
        
        return super.transferFromDiskTo(this, out);
        
    }
    
}
