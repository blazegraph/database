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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.Executors;

import com.bigdata.cache.LRUCache;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.FileLockUtility;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.IUpdateStore;
import com.bigdata.resources.StoreManager.ManagedJournal;

/**
 * Disk-based journal strategy.
 * <p>
 * Writes are buffered in a write cache. The cache is flushed when it would
 * overflow. As a result only large sequential writes are performed on the
 * store. Reads read through the write cache for consistency.
 * <p>
 * Note: This is used to realize both the {@link BufferMode#Disk} and the
 * {@link BufferMode#Temporary} {@link BufferMode}s. When configured for the
 * {@link BufferMode#Temporary} mode: the root blocks will not be written onto
 * the disk, writes will not be forced, and the backing file will be created the
 * first time the {@link DiskOnlyStrategy} attempts to write through to the
 * disk. For many scenarios, the backing file will never be created unless the
 * write cache overflows. This provides very low latency on start-up, the same
 * MRMW capability, and allows very large temporary stores.
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
 * 
 * @todo report whether or not the on-disk write cache is enabled for each
 *       platform in {@link AbstractStatisticsCollector}. offer guidence on how
 *       to disable that write cache.
 * 
 * @todo The flush of the write cache could be made asynchronous if we had two
 *       write buffers, but that increases the complexity significantly. It
 *       would have to be synchronous if invoked from {@link #force(boolean)} in
 *       any case (or rather force would have to flush all buffers).
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
 *       writeQueue to be empty. Large objects could be wrapped and written out
 *       using the same mechansims but should not become "available" again after
 *       they are written.
 *       <p>
 *       Consider that a WriteCache also doubles as a read cache IF we create
 *       write cache objects encapsulating reads that we read directly from the
 *       disk rather than from a WriteCache. In this case we might do a larger
 *       read so as to populate more of the WriteCache object in the hope that
 *       we will have more hits in that part of the journal.
 *       <p>
 *       modify force to use an atomic handoff of the write cache so that the
 *       net result is atomic from the perspective of the caller. This may
 *       require locking on the write cache so that we wait until concurrent
 *       writes have finished before flushing to the disk or I may be able to
 *       use nextOffset to make an atomic determination of the range of the
 *       buffer to be forced, create a view of that range, and use the view to
 *       force to disk so that the position and limits are not changed by force
 *       nor by concurrent writers - this may also be a problem for the Direct
 *       mode and the Mapped mode, at least if they use a write cache.
 *       <p>
 *       Async cache writes are also useful if the disk cache is turned off and
 *       could gain importance in offering tighter control over IO guarentees.
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
 * @see BufferMode#Temporary
 */
public class DiskOnlyStrategy extends AbstractBufferStrategy implements
        IDiskBasedStrategy, IUpdateStore {
    
    /**
     * The file.
     */
    private final File file;
    
    /**
     * The mode used to open that file.
     */
    private final String fileMode;

    /**
     * <code>true</code> iff configured as a {@link BufferMode#Temporary} store.
     */
    private final boolean temporaryStore;

    /**
     * The backing file for a {@link BufferMode#Temporary} store is not opened
     * until the {@link #writeCache} is flushed to disk for the first time. In
     * these scenarios this field will be <code>false</code> until the
     * {@link #writeCache} is flushed and <code>true</code> thereafter. For
     * {@link BufferMode#Disk}, this field is initially <code>true</code>.
     * <p>
     * The value of this field determines the behavior of
     * {@link #reopenChannel()}.
     */
    private boolean fileOpened;
    
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
     * Optional read cache.
     * <p>
     * Note: When enabled, records are entered iff there is a miss on a read.
     * Written records are NOT entered into the read cache since (when the
     * {@link #writeCache} is enabled), recently written records are already in
     * the {@link #writeCache}.
     * 
     * @todo purge old entries based on last touched time?
     */
    private LRUCache<Long, byte[]> readCache = null;
    
    /**
     * The maximum size of a record that may enter the {@link #readCache}.
     * Records larger than this are not cached.
     */
    private int readCacheMaxRecordSize = 0;
    
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
         * record. The values are the position in {@link #buf} where that record
         * is buffered. A cache miss means that you need to read the record from
         * the disk.
         */
        final private Map<Long,Integer> writeCacheIndex;
        
//        /**
//         * The starting position in the buffer for data that has not been
//         * written to the disk.
//         * 
//         * @see Task
//         */
//        private int start = 0;

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
        public WriteCache(ByteBuffer writeCache) {
            
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
         * @return A read-write view onto the record in the write cache buffer
         *         -or- <code>null</code> iff the record does not lie within
         *         this {@link WriteCache}.
         *         <p>
         *         Note: The caller MUST copy the data from the view since
         *         concurrent operations may result in the write cache being
         *         flushed and the view overwritten with new data.
         *         <p>
         *         Note: A read-write view is returned in order to support
         *         {@link DiskOnlyStrategy#update(long, int, ByteBuffer)}
         *         for those cases when the record to be updated in still in
         *         the {@link WriteCache}.
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

    /**
     * Note: This MAY be <code>null</code>. If {@link BufferMode#Temporary}
     * is used then it WILL be <code>null</code> until the {@link #writeCache}
     * is flushed to disk for the first time.
     */
    final public RandomAccessFile getRandomAccessFile() {

        return raf;

    }

    /**
     * Note: This MAY be <code>null</code>. If {@link BufferMode#Temporary}
     * is used then it WILL be <code>null</code> until the {@link #writeCache}
     * is flushed to disk for the first time.
     */
    final public FileChannel getChannel() {

        final RandomAccessFile raf = getRandomAccessFile();

        if (raf == null)
            return null;

        return raf.getChannel();

    }

    /**
     * Counters for {@link IRawStore} access, including operations that read or
     * write through to the underlying media.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo report elapsed time and average latency for force, reopen, and
     *       writeRootBlock.
     * 
     * @todo counters need to be atomic if we want to avoid the possibility of
     *       concurrent <code>x++</code> operations failing to correctly
     *       increment <code>x</code> for each request.
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
         * #of bytes that have been read from the disk.
         */
        long bytesReadFromDisk;
        
        /**
         * The size of the largest record read.
         */
        long maxReadSize;
        
        /**
         * Total elapsed time for reads.
         */
        long elapsedReadNanos;

        /**
         * Total elapsed time checking the disk write cache for records to be
         * read.
         */
        long elapsedCacheReadNanos;
        
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
         * #of times the write cache was flushed to disk.
         */
        long ncacheFlush;
        
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
         * #of bytes that have been written on the disk.
         */
        long bytesWrittenOnDisk;
        
        /**
         * Total elapsed time for writes.
         */
        long elapsedWriteNanos;
        
        /**
         * Total elapsed time writing records into the cache (does not count
         * time to flush the cache when it is full or to write records that do
         * not fit in the cache directly to the disk).
         */
        long elapsedCacheWriteNanos;
        
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
        
        synchronized public CounterSet getCounters() {

            if (root == null) {

                root = new CounterSet();

                // IRawStore API
                {

                    /*
                     * reads
                     */

                    root.addCounter("nreads", new Instrument<Long>() {
                        public void sample() {
                            setValue(nreads);
                        }
                    });

                    root.addCounter("bytesRead", new Instrument<Long>() {
                        public void sample() {
                            setValue(bytesRead);
                        }
                    });

                    root.addCounter("readSecs", new Instrument<Double>() {
                        public void sample() {
                            final double elapsedReadSecs = (elapsedReadNanos / 1000000000.);
                            setValue(elapsedReadSecs);
                        }
                    });

                    root.addCounter("bytesReadPerSec",
                            new Instrument<Double>() {
                                public void sample() {
                                    final double readSecs = (elapsedReadNanos / 1000000000.);
                                    final double bytesReadPerSec = (readSecs == 0L ? 0d
                                            : (bytesRead / readSecs));
                                    setValue(bytesReadPerSec);
                                }
                            });

                    root.addCounter("maxReadSize", new Instrument<Long>() {
                        public void sample() {
                            setValue(maxReadSize);
                        }
                    });

                    /*
                     * writes
                     */

                    root.addCounter("nwrites", new Instrument<Long>() {
                        public void sample() {
                            setValue(nwrites);
                        }
                    });

                    root.addCounter("bytesWritten", new Instrument<Long>() {
                        public void sample() {
                            setValue(bytesWritten);
                        }
                    });

                    root.addCounter("writeSecs", new Instrument<Double>() {
                        public void sample() {
                            final double writeSecs = (elapsedWriteNanos / 1000000000.);
                            setValue(writeSecs);
                        }
                    });

                    root.addCounter("bytesWrittenPerSec",
                            new Instrument<Double>() {
                                public void sample() {
                                    final double writeSecs = (elapsedWriteNanos / 1000000000.);
                                    final double bytesWrittenPerSec = (writeSecs == 0L ? 0d
                                            : (bytesWritten / writeSecs));
                                    setValue(bytesWrittenPerSec);
                                }
                            });

                    root.addCounter("maxWriteSize", new Instrument<Long>() {
                        public void sample() {
                            setValue(maxWriteSize);
                        }
                    });

                }

                /*
                 * write cache statistics
                 */
                {

                    final CounterSet writeCache = root.makePath("writeCache");

                    /*
                     * read
                     */
                    writeCache.addCounter("nread", new Instrument<Long>() {
                        public void sample() {
                            setValue(ncacheRead);
                        }
                    });

                    writeCache.addCounter("readHitRate", new Instrument<Double>() {
                        public void sample() {
                            setValue(nreads == 0L ? 0d : (double) ncacheRead
                                    / nreads);
                        }
                    });

                    writeCache.addCounter("readSecs", new Instrument<Double>() {
                        public void sample() {
                            setValue(elapsedCacheReadNanos / 1000000000.);
                        }
                    });

                    /*
                     * write
                     */
                    
                    // #of writes on the write cache.
                    writeCache.addCounter("nwrite", new Instrument<Long>() {
                        public void sample() {
                            setValue(ncacheWrite);
                        }
                    });

                    /*
                     * % of writes that are buffered vs writing through to the
                     * disk.
                     * 
                     * Note: This will be 1.0 unless you are writing large
                     * records. Large records are written directly to the disk
                     * rather than first into the write cache. When this happens
                     * the writeHitRate on the cache can be less than one.
                     */
                    writeCache.addCounter("writeHitRate", new Instrument<Double>() {
                        public void sample() {
                            setValue(nwrites == 0L ? 0d : (double) ncacheWrite
                                    / nwrites);
                        }
                    });

                    writeCache.addCounter("writeSecs", new Instrument<Double>() {
                        public void sample() {
                            setValue(elapsedCacheWriteNanos / 1000000000.);
                        }
                    });

                    // #of times the write cache was flushed to the disk.
                    writeCache.addCounter("nflush", new Instrument<Long>() {
                        public void sample() {
                            setValue(ncacheFlush);
                        }
                    });
                    
                }

                // disk statistics
                {
                    final CounterSet disk = root.makePath("disk");

                    /*
                     * read
                     */

                    disk.addCounter("nreads", new Instrument<Long>() {
                        public void sample() {
                            setValue(ndiskRead);
                        }
                    });

                    disk.addCounter("bytesRead", new Instrument<Long>() {
                        public void sample() {
                            setValue(bytesReadFromDisk);
                        }
                    });

                    disk.addCounter("bytesPerRead", new Instrument<Double>() {
                        public void sample() {
                            final double bytesPerDiskRead = (ndiskRead == 0 ? 0d
                                    : (bytesReadFromDisk / ndiskRead));
                            setValue(bytesPerDiskRead);
                        }
                    });

                    disk.addCounter("readSecs", new Instrument<Double>() {
                        public void sample() {
                            final double diskReadSecs = (elapsedDiskReadNanos / 1000000000.);
                            setValue(diskReadSecs);
                        }
                    });

                    disk.addCounter("bytesReadPerSec",
                            new Instrument<Double>() {
                                public void sample() {
                                    final double diskReadSecs = (elapsedDiskReadNanos / 1000000000.);
                                    final double bytesReadPerSec = (diskReadSecs == 0L ? 0d
                                            : bytesReadFromDisk / diskReadSecs);
                                    setValue(bytesReadPerSec);
                                }
                            });

                    disk.addCounter("secsPerRead", new Instrument<Double>() {
                        public void sample() {
                            final double diskReadSecs = (elapsedDiskReadNanos / 1000000000.);
                            final double readLatency = (diskReadSecs == 0 ? 0d
                                    : diskReadSecs / ndiskRead);
                            setValue(readLatency);
                        }
                    });

                    /*
                     * write
                     */

                    disk.addCounter("nwrites", new Instrument<Long>() {
                        public void sample() {
                            setValue(ndiskWrite);
                        }
                    });

                    disk.addCounter("bytesWritten", new Instrument<Long>() {
                        public void sample() {
                            setValue(bytesWrittenOnDisk);
                        }
                    });

                    disk.addCounter("bytesPerWrite", new Instrument<Double>() {
                        public void sample() {
                            final double bytesPerDiskWrite = (ndiskWrite == 0 ? 0d
                                    : (bytesWrittenOnDisk / ndiskWrite));
                            setValue(bytesPerDiskWrite);
                        }
                    });

                    disk.addCounter("writeSecs", new Instrument<Double>() {
                        public void sample() {
                            final double diskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);
                            setValue(diskWriteSecs);
                        }
                    });

                    disk.addCounter("bytesWrittenPerSec",
                            new Instrument<Double>() {
                                public void sample() {
                                    final double diskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);
                                    final double bytesWrittenPerSec = (diskWriteSecs == 0L ? 0d
                                            : bytesWrittenOnDisk
                                                    / diskWriteSecs);
                                    setValue(bytesWrittenPerSec);
                                }
                            });

                    disk.addCounter("secsPerWrite", new Instrument<Double>() {
                        public void sample() {
                            final double diskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);
                            final double writeLatency = (diskWriteSecs == 0 ? 0d
                                    : diskWriteSecs / ndiskWrite);
                            setValue(writeLatency);
                        }
                    });

                    /*
                     * other
                     */

                    disk.addCounter("nforce", new Instrument<Long>() {
                        public void sample() {
                            setValue(nforce);
                        }
                    });

                    disk.addCounter("nextend", new Instrument<Long>() {
                        public void sample() {
                            setValue(ntruncate);
                        }
                    });

                    disk.addCounter("nreopen", new Instrument<Long>() {
                        public void sample() {
                            setValue(nreopen);
                        }
                    });

                    disk.addCounter("rootBlockWrites", new Instrument<Long>() {
                        public void sample() {
                            setValue(nwriteRootBlock);
                        }
                    });

                }

            }

            return root;

        }
        private CounterSet root;
        
        /**
         * Human readable representation of the counters.
         */
        public String toString() {

            return getCounters().toString();
            
        }
        
    }
    
    /**
     * Counters on {@link IRawStore} and disk access.
     */
    final public Counters counters = new Counters();
    
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
             * other.
             */
            {
                final CounterSet writeCache = root.makePath("writeCache");

                {
                 
                    final WriteCache tmp = DiskOnlyStrategy.this.writeCache;

                    // add counter for the write cache capacity.
                    writeCache.addCounter("capacity",
                            new OneShotInstrument<Long>(tmp == null ? 0L : tmp
                                    .capacity()));
                    
                }
                
            }
            
            /*
             * read cache.
             */
            {

                final CounterSet readCache = root.makePath("readCache");

                {
                    
                    final LRUCache tmp = DiskOnlyStrategy.this.readCache;

                    readCache.addCounter("capacity",
                            new OneShotInstrument<Long>((long) (tmp == null ? 0
                                    : tmp.capacity())));
                    
                }
                
                readCache.addCounter("testCount",new Instrument<Long>(){

                    @Override
                    protected void sample() {

                        final LRUCache tmp = DiskOnlyStrategy.this.readCache;
                        
                        if(tmp==null) return;
                        
                        setValue(tmp.getTestCount());

                    }});
                
                readCache.addCounter("successCount",new Instrument<Long>(){

                    @Override
                    protected void sample() {

                        final LRUCache tmp = DiskOnlyStrategy.this.readCache;
                        
                        if(tmp==null) return;
                        
                        setValue(tmp.getSuccessCount());

                    }});
                
                readCache.addCounter("insertCount",new Instrument<Long>(){

                    @Override
                    protected void sample() {

                        final LRUCache tmp = DiskOnlyStrategy.this.readCache;
                        
                        if(tmp==null) return;
                        
                        setValue(tmp.getInsertCount());

                    }});
                
                readCache.addCounter("hitRatio",new Instrument<Double>(){

                    @Override
                    protected void sample() {

                        final LRUCache tmp = DiskOnlyStrategy.this.readCache;
                        
                        if(tmp==null) return;
                        
                        setValue(tmp.getHitRatio());

                    }});
                
            }
            
//            sb.append("file="+file);
//            sb.append(", mode="+fileMode);
//            sb.append(", readOnly=" + isReadOnly());
            
        }
        
        return root;
        
    }
    private CounterSet root;
    
    /**
     * 
     * @param maximumExtent
     * @param fileMetadata
     */
    DiskOnlyStrategy(long maximumExtent, FileMetadata fileMetadata) {

        super(fileMetadata.extent, maximumExtent, fileMetadata.offsetBits,
                fileMetadata.nextOffset, fileMetadata.bufferMode,
                fileMetadata.readOnly);

        this.file = fileMetadata.file;

        this.fileMode = fileMetadata.fileMode;
        
        this.temporaryStore = (fileMetadata.bufferMode==BufferMode.Temporary);
        
        this.raf = fileMetadata.raf;
        
        this.fileOpened = raf != null;
        
        if (!temporaryStore && !fileOpened) {
            
            throw new RuntimeException(
                    "File not open and not a temporary store");
            
        }

        this.extent = fileMetadata.extent;

        this.headerSize = FileMetadata.headerSize0;
        
        this.userExtent = extent - headerSize;

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

        if (fileMetadata.readCacheCapacity > 0) {

            if(INFO)
                log.info("Enabling read cache: capacity="
                    + fileMetadata.readCacheCapacity + ", maxRecordSize="
                    + fileMetadata.readCacheMaxRecordSize);

            if (fileMetadata.readCacheMaxRecordSize <= 0)
                throw new IllegalArgumentException();

            this.readCacheMaxRecordSize = fileMetadata.readCacheMaxRecordSize;
            
            this.readCache = new LRUCache<Long, byte[]>(
                    fileMetadata.readCacheCapacity);
            
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

        assertOpen();
        
        synchronized(this) {

            // flush all pending writes to disk.
            flushWriteCache();
            
        }

        try {

            if(!temporaryStore) {

                // sync the disk.
                getChannel().force(metadata);
                
            }

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

        // Release the write cache.
        writeCache = null;
        
        if(readCache != null) {

            if (INFO)
                log.info("readCache: " + readCache.getStatistics());
            
            // Discard the LRU cache.
            readCache = null;
            
        }
        
        try {

            if (raf != null) {

                FileLockUtility.closeFile(file,raf);
                
            }
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public void deleteResources() {
        
        if (isOpen()) {

            throw new IllegalStateException();

        }
        
        if( fileOpened && ! file.delete() ) {
            
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

        if (readCache != null) {
            
            /*
             * Test the read cache first and return the record from the read
             * cache if it is found there.
             */
            
            final byte[] data = readCache.get(addr);

            if (data != null) {

                return ByteBuffer.wrap(data).asReadOnlyBuffer();
                
            }
            
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

            for (int ntries = 0; ntries < 3; ntries++) {

                if (ntries > 0) {

                    /*
                     * Note: clear if we are retrying since the buffer may have
                     * been modified by a partial read.
                     */ 

                    dst.clear();
                    
                }
                
                try {

                    counters.ndiskRead += FileChannelUtility.readAll(
                            getChannel(), dst, pos);

                    // successful read - exit the loop.
                    break;

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
            counters.bytesReadFromDisk+=nbytes;
            counters.elapsedReadNanos+=(System.nanoTime()-begin);
            counters.elapsedDiskReadNanos+=(System.nanoTime()-beginDisk);

            if (readCache != null && nbytes < readCacheMaxRecordSize) {

                /*
                 * Note: make sure that the record is not in the cache (we have
                 * to do this again since we were not synchronized on [this]
                 * when we tested at the start of this method).
                 */
                if (readCache.get(addr) == null) {

                    /*
                     * Put a copy of the record in the read cache.
                     */

                    // new byte[] for the read cache.
                    final byte[] data = new byte[nbytes];

                    // copy contents into the new byte[].
                    dst.get(data);

                    // flip the buffer again so that it is read for re-reading.
                    dst.flip();

                    // put the record into the read cache.
                    readCache.put(addr, data, false/* dirty */);

                }
                
            }
            
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

        if(temporaryStore && !fileOpened) {

            /*
             * The backing file has not been opened.
             * 
             * Note: Without this case this method would create the backing
             * store for a Temporary store if anyone happened to invoke it. In
             * fact, this method will never get invoked for a Temporary store
             * without a backing store since the reads never read against the
             * channel because it does not exist. So, really, this is just here
             * to be paranoid.
             */
            
            return false;
        
        }
        
        try {

            raf = FileLockUtility.openFile(file, fileMode, true/*tryFileLock*/);
        
            if(WARN) log.warn("Re-opened file: "+file);
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        counters.nreopen++;
        
        return true;
        
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
                
                writeOnDisk(data,offset, true/*append*/);

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
     * Create/open the backing file for a {@link BufferMode#Temporary} store iff
     * it has not been created/opened.
     */
    final private void createBackingFile() {
        
        if (!fileOpened && temporaryStore) {
            
            try {
                
                // open the file for the first time (create).
                raf = FileLockUtility.openFile(file, fileMode,
                        bufferMode != BufferMode.Mapped/*useTryLock*/);
                
                // note that it has been opened.
                fileOpened = true;

                if (INFO)
                    log.info("Opened backing file for temporary store: "
                                    + file);
                
            } catch (IOException e) {
                
                throw new RuntimeException("Could not open temp file: file="
                        + file, e);
                
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

        createBackingFile();
        
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
            synchronized (this) {

                FileChannelUtility.readAll(getChannel(), tmp,
                        rootBlock0 ? FileMetadata.OFFSET_ROOT_BLOCK0
                                : FileMetadata.OFFSET_ROOT_BLOCK1);
                
            }
            
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
        
        /*
         * Note: This handles the case for a Temporary store where the write
         * cache is the same size as the initial extent and everything written
         * so far has been absorbed by the write cache.
         */
        createBackingFile();
        
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
            
            if (!temporaryStore) {

                force(true);
                
            }

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
