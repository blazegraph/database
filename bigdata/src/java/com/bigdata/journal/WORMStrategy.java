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
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.bigdata.LRUNexus;
import com.bigdata.btree.BTree.Counter;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.striped.StripedCounters;
import com.bigdata.ha.QuorumRead;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;

/**
 * Disk-based Write Once Read Many (WORM) journal strategy. The phsyical layout
 * on the disk is the journal header, the root blocks, and then the user extent.
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
 * http://mail-archives.apache.org/mod_mbox
 * /db-derby-dev/200609.mbox/%3C44F820A8.6000000@sun.com%3E
 * 
 * <pre>
 *                /sbin/hdparm -W 0 /dev/hda 0 Disable write caching
 *                /sbin/hdparm -W 1 /dev/hda 1 Enable write caching
 * </pre>
 * 
 * @todo report whether or not the on-disk write cache is enabled for each
 *       platform in {@link AbstractStatisticsCollector}. offer guidance on how
 *       to disable that write cache.
 * 
 * @todo test verifying that the write cache can be disabled.
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
public class WORMStrategy extends AbstractBufferStrategy implements
        IDiskBasedStrategy, IHABufferStrategy {
    
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
     * 
     * @todo Atomic long to ensure visiblility of changes?
     */
    private long extent;

    private long userExtent;

    private final long minimumExtension;

    private final Quorum<?,?> quorum;
//    private final AtomicReference<Quorum<?,?>> quorumRef;

    /**
     * This lock is used to exclude readers when the extent of the backing file
     * is about to be changed. This is a workaround for an old (an unresolved as
     * of February 2010) Sun bug.
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
     */
    final private ReentrantReadWriteLock extensionLock = new ReentrantReadWriteLock();

    /**
     * The service responsible for migrating dirty records onto the backing file
     * and (for HA) onto the other members of the {@link Quorum}.
     * 
     * @todo This MAY be <code>null</code> for a read-only store or if the write
     *       cache is disabled.
     * 
     * @todo Is HA read-only allowed? If so, then since the
     *       {@link WriteCacheService} handles failover reads it should be
     *       enabled for HA read-only.
     */
    private final WriteCacheService writeCacheService;

    /**
     * <code>true</code> iff the backing store has record level checksums.
     */
    private final boolean useChecksums;

    /**
     * <code>true</code> if the backing store will be used in an HA
     * {@link Quorum} (this is passed through to the {@link WriteCache} objects
     * which use this flag to conditionally track the checksum of the entire
     * write cache buffer).
     */
    private final boolean isHighlyAvailable;
    
    /**
     * The {@link UUID} which identifies the journal (this is the same for each
     * replicated journal is a quorum, so it is really a logical store UUID).
     */
    private final UUID storeUUID;
    
    @Override
    public boolean useChecksums() {
        return useChecksums;
    }
    
    /**
     * Issues the disk writes for the write cache and recycles the write cache
     * to receive new writes.
     */
    private void flushWriteCache() {

        if (writeCacheService != null) {

            try {

                /*
                 * Issue the disk writes (does not force to the disk).
                 * 
                 * Note: This will wind up calling writeOnDisk().
                 * 
                 * Note: It is critical that this operation is atomic with
                 * regard to writes on the cache. Otherwise new writes can enter
                 * the cache after it was flushed to the backing channel but
                 * before it is reset. Those writes will then be lost. This
                 * issue does not arise for the {@link WriteCacheService} since
                 * it atomically moves the full buffer onto a dirty list.
                 */

                writeCacheService.flush(false/* force */);
                
            } catch (InterruptedException e) {
                
                throw new RuntimeException(e);
                
            }

        }
        
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
     * Striped performance counters for {@link IRawStore} access, including
     * operations that read or write through to the underlying media.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @param <T>
     * 
     * @todo report elapsed time and average latency for force, reopen, and
     *       writeRootBlock.
     *       
     * FIXME  CAT may be much faster than striped locks (2-3x faster).
     */
    static public class StoreCounters<T extends StoreCounters<T>> extends
            StripedCounters<T> {

        /**
         * #of read requests.
         */
        public volatile long nreads;

        /**
         * #of read requests that read through to the backing file.
         */
        public volatile long ndiskRead;
        
        /**
         * #of bytes read.
         */
        public volatile long bytesRead;

        /**
         * #of bytes that have been read from the disk.
         */
        public volatile long bytesReadFromDisk;
        
        /**
         * Total elapsed time for reads.
         */
        public volatile long elapsedReadNanos;

        /**
         * Total elapsed time for reading on the disk.
         */
        public volatile long elapsedDiskReadNanos;

        /**
         * The #of checksum errors while reading on the local disk.
         */
        public volatile long checksumErrorCount;
        
        /**
         * #of write requests.
         */
        public volatile long nwrites;
        
        /**
         * #of write requests that write through to the backing file.
         */
        public volatile long ndiskWrite;

        /**
         * The size of the largest record read.
         */
        public volatile long maxReadSize;
        
        /**
         * The size of the largest record written.
         */
        public volatile long maxWriteSize;
        
        /**
         * #of bytes written.
         */
        public volatile long bytesWritten;
        
        /**
         * #of bytes that have been written on the disk.
         */
        public volatile long bytesWrittenOnDisk;
        
        /**
         * Total elapsed time for writes.
         */
        public volatile long elapsedWriteNanos;
        
        /**
         * Total elapsed time for writing on the disk.
         */
        public volatile long elapsedDiskWriteNanos;
        
        /**
         * #of times the data were forced to the disk.
         */
        public volatile long nforce;
        
        /**
         * #of times the length of the file was changed (typically, extended).
         */
        public volatile long ntruncate;
        
        /**
         * #of times the file has been reopened after it was closed by an
         * interrupt.
         */
        public volatile long nreopen;
        
        /**
         * #of times one of the root blocks has been written.
         */
        public volatile long nwriteRootBlock;

        /**
         * {@inheritDoc}
         */
        public StoreCounters() {
            super();
        }

        /**
         * {@inheritDoc}
         */
        public StoreCounters(final int batchSize) {
            super(batchSize);
        }

        /**
         * {@inheritDoc}
         */
        public StoreCounters(final int nstripes, final int batchSize) {
            super(nstripes, batchSize);
        }

        @Override
        public void add(final T o) {

            super.add(o);
            
            nreads += o.nreads;
            ndiskRead += o.ndiskRead;
            bytesRead += o.bytesRead;
            bytesReadFromDisk += o.bytesReadFromDisk;
            maxReadSize = Math.max(maxReadSize, o.maxReadSize);
            elapsedReadNanos += o.elapsedReadNanos;
            elapsedDiskReadNanos += o.elapsedDiskReadNanos;
            checksumErrorCount += o.checksumErrorCount;

            nwrites += o.nwrites;
            ndiskWrite += o.ndiskWrite;
            maxWriteSize = Math.max(maxWriteSize, o.maxWriteSize);
            bytesWritten += o.bytesWritten;
            bytesWrittenOnDisk += o.bytesWrittenOnDisk;
            elapsedWriteNanos += o.elapsedWriteNanos;
            elapsedDiskWriteNanos += o.elapsedDiskWriteNanos;

            nforce += o.nforce;
            ntruncate += o.ntruncate;
            nreopen += o.nreopen;
            nwriteRootBlock += o.nwriteRootBlock;
            
        }

        @Override
        public T subtract(final T o) {

            // make a copy of the current counters.
            final T t = super.subtract(o);
            
            // subtract out the given counters.
            t.nreads -= o.nreads;
            t.ndiskRead -= o.ndiskRead;
            t.bytesRead -= o.bytesRead;
            t.bytesReadFromDisk -= o.bytesReadFromDisk;
            t.maxReadSize -= o.maxReadSize; // @todo report max? min?
            t.elapsedReadNanos -= o.elapsedReadNanos;
            t.elapsedDiskReadNanos -= o.elapsedDiskReadNanos;
            t.checksumErrorCount -= o.checksumErrorCount;

            t.nwrites -= o.nwrites;
            t.ndiskWrite -= o.ndiskWrite;
            t.maxWriteSize -= o.maxWriteSize; // @todo report max? min?
            t.bytesWritten -= o.bytesWritten;
            t.bytesWrittenOnDisk -= o.bytesWrittenOnDisk;
            t.elapsedWriteNanos -= o.elapsedWriteNanos;
            t.elapsedDiskWriteNanos -= o.elapsedDiskWriteNanos;

            t.nforce -= o.nforce;
            t.ntruncate -= o.ntruncate;
            t.nreopen -= o.nreopen;
            t.nwriteRootBlock -= o.nwriteRootBlock;

            return t;
            
        }
        
        @Override
        public void clear() {

            // subtract out the given counters.
            nreads = 0;
            ndiskRead = 0;
            bytesRead = 0;
            bytesReadFromDisk = 0;
            maxReadSize = 0;
            elapsedReadNanos = 0;
            elapsedDiskReadNanos = 0;
            checksumErrorCount = 0;

            nwrites = 0;
            ndiskWrite = 0;
            maxWriteSize = 0;
            bytesWritten = 0;
            bytesWrittenOnDisk = 0;
            elapsedWriteNanos = 0;
            elapsedDiskWriteNanos = 0;

            nforce = 0;
            ntruncate = 0;
            nreopen = 0;
            nwriteRootBlock = 0;

        }
        
        @Override
        public CounterSet getCounters() {

            final CounterSet root = super.getCounters();

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

                root.addCounter("bytesReadPerSec", new Instrument<Double>() {
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

                root.addCounter("checksumErrorCount", new Instrument<Long>() {
                    public void sample() {
                        setValue(checksumErrorCount);
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

                root.addCounter("bytesWrittenPerSec", new Instrument<Double>() {
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

            } // IRawStore

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
                                : (bytesReadFromDisk / (double) ndiskRead));
                        setValue(bytesPerDiskRead);
                    }
                });

                disk.addCounter("readSecs", new Instrument<Double>() {
                    public void sample() {
                        final double diskReadSecs = (elapsedDiskReadNanos / 1000000000.);
                        setValue(diskReadSecs);
                    }
                });

                disk.addCounter("bytesReadPerSec", new Instrument<Double>() {
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
                                : (bytesWrittenOnDisk / (double) ndiskWrite));
                        setValue(bytesPerDiskWrite);
                    }
                });

                disk.addCounter("writeSecs", new Instrument<Double>() {
                    public void sample() {
                        final double diskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);
                        setValue(diskWriteSecs);
                    }
                });

                disk.addCounter("bytesWrittenPerSec", new Instrument<Double>() {
                    public void sample() {
                        final double diskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);
                        final double bytesWrittenPerSec = (diskWriteSecs == 0L ? 0d
                                : bytesWrittenOnDisk / diskWriteSecs);
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

            } // disk
            
            return root;

        } // getCounters()
        
    } // class StoreCounters
    
    /**
     * Striped performance counters for this class.
     */
    private final AtomicReference<StoreCounters> storeCounters = new AtomicReference<StoreCounters>();

    /**
     * Returns the striped performance counters for the store.
     */
    public StoreCounters<?> getStoreCounters() {

        return storeCounters.get();

    }

    /**
     * Replaces the {@link StoreCounters} object.
     * 
     * @param storeCounters
     *            The new {@link Counter}s.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    public void setStoreCounters(final StoreCounters<?> storeCounters) {

        if (storeCounters == null)
            throw new IllegalArgumentException();

        this.storeCounters.set(storeCounters);
        
    }

    /**
     * Return interesting information about the write cache and file operations.
     */
    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

        root.addCounter("nextOffset", new Instrument<Long>() {
            public void sample() {
                setValue(nextOffset.get());
            }
        });

        root.addCounter("extent", new Instrument<Long>() {
            public void sample() {
                setValue(extent);
            }
        });

        // attach the most recently updated values from the striped counters.
        root.attach(storeCounters.get().getCounters());

        if (writeCacheService != null) {

            final CounterSet tmp = root.makePath("writeCache");

            tmp.attach(writeCacheService.getCounters());

        }
        
        return root;

    }

    /**
     * 
     * @param maximumExtent
     * @param fileMetadata
     */
    WORMStrategy(final long maximumExtent, final long minimumExtension,
            final FileMetadata fileMetadata,
            final Quorum<?, ?> quorum) {

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

        this.minimumExtension = minimumExtension;

        this.quorum = quorum;

        this.useChecksums = fileMetadata.useChecksums;

        this.storeUUID = fileMetadata.rootBlock.getUUID();
        
        // initialize striped performance counters for this store.
        this.storeCounters.set(new StoreCounters(10/* batchSize */));
        
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
         * 
         * Note: HA MUST use a write cache service (the write cache service
         * handles the write pipeline to the downstream quorum members).
         */
//        final Quorum<?,?> quorum = quorumRef.get();
        
        isHighlyAvailable = quorum != null && quorum.isHighlyAvailable();

        final boolean useWriteCacheService = fileMetadata.writeCacheEnabled
                && !fileMetadata.readOnly && fileMetadata.closeTime == 0L
                || isHighlyAvailable;
        
        if (useWriteCacheService) {
            /*
             * WriteCacheService.
             */
            try {
                this.writeCacheService = new WriteCacheService(
                        fileMetadata.writeCacheBufferCount, useChecksums,
                        extent, opener, quorum) {
                    @Override
                    public WriteCache newWriteCache(final ByteBuffer buf,
                            final boolean useChecksum,
                            final boolean bufferHasData,
                            final IReopenChannel<? extends Channel> opener)
                            throws InterruptedException {
                        return new WriteCacheImpl(0/* baseOffset */, buf,
                                useChecksum, bufferHasData,
                                (IReopenChannel<FileChannel>) opener);
                    }
                };
                this._checkbuf = null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            this.writeCacheService = null;
            this._checkbuf = useChecksums ? ByteBuffer.allocateDirect(4) : null;
        }

//        System.err.println("WARNING: alpha impl: "
//                + this.getClass().getName()
//                + (writeCacheService != null ? " : writeCacheBuffers="
//                        + fileMetadata.writeCacheBufferCount : " : No cache")
//                + ", useChecksums=" + useChecksums);

    }

    /**
     * Implementation coordinates writes using the read lock of the
     * {@link DiskOnlyStrategy#extensionLock}. This is necessary in order to
     * avoid corrupt reads or writes with concurrent changes to the file size.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class WriteCacheImpl extends WriteCache.FileChannelWriteCache {

        public WriteCacheImpl(final long baseOffset, final ByteBuffer buf,
                final boolean useChecksum,
                final boolean bufferHasData,
                final IReopenChannel<FileChannel> opener)
                throws InterruptedException {

            super(baseOffset, buf, useChecksum, isHighlyAvailable,
                    bufferHasData, opener);

        }

        @Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffset,
                final Map<Long, RecordMetadata> recordMapIsIgnored,
                final long nanos) throws InterruptedException, IOException {

            final long begin = System.nanoTime();
            
            long remaining = nanos;
            
            final Lock readLock = extensionLock.readLock();

            if (!readLock.tryLock(remaining, TimeUnit.NANOSECONDS)) {

                return false;
                
            }
            
            try {

                remaining -= (System.nanoTime() - begin);

                final int dpos = data.position();
                final int nbytes = data.remaining();

                /*
                 * Note: We are holding the readLock (above). This is Ok since
                 * file extension occurs when the record is accepted for write
                 * while only the readLock is required to actually write on the
                 * file.
                 */
                final int nwrites = writeOnDisk(data, firstOffset);

                final WriteCacheCounters counters = this.counters.get();
                counters.nwrite += nwrites;
                counters.bytesWritten += nbytes;
                counters.elapsedWriteNanos += (System.nanoTime() - begin);

                if (WriteCache.log.isTraceEnabled()) {
                    WriteCache.log.trace("wroteOnDisk: dpos=" + dpos
                            + ", nbytes=" + nbytes + ", firstOffset="
                            + firstOffset + ", nrecords="
                            + recordMapIsIgnored.size());
                }
                
                return true;
                
            } finally {
                
                readLock.unlock();
                
            }
            
        }
        
    }
    
    final public boolean isStable() {
        
        return true;
        
    }

    public boolean isFullyBuffered() {
        
        return false;
        
    }

    public void force(final boolean metadata) {

        assertOpen();

        try {

            // flush all pending writes to disk.
            flushWriteCache();

            if (!temporaryStore) {

                // sync the disk.
                getChannel().force(metadata);

            }

            final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                    .acquire();
            try {
                c.nforce++;
            } finally {
                c.release();
            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
    }

    /**
     * Flushes the write cache (if enabled).
     * 
     * @todo Should be a NOP for the WORM? Check
     *       {@link AbstractJournal#commitNow(long)}
     */
    @Override
    public void commit(IJournal journal) {

        flushWriteCache();

    }

    /**
     * Resets the {@link WriteCacheService} (if enabled).
     *<p>
     * Note: This assumes the caller is synchronized appropriately otherwise
     * writes belonging to other threads will be discarded from the cache!
     */
    @Override
    public void abort() {

        if (writeCacheService != null) {
            try {
                writeCacheService.reset();
                writeCacheService.setExtent(extent);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    /**
     * Closes the file immediately (without flushing any pending writes).
     * 
     * @todo This is synchronized solely to coordinate the service shutdown
     *       state, which SHOULD use a different lock.
     */
    synchronized public void close() {

        /*
         * Note: this clears the [open] flag. It is important to do this first
         * so that we do not re-open the channel once it has been closed.
         */

        super.close();

        // Release the write cache.
        releaseWriteCache();
        
        try {

            if (raf != null) {
                synchronized (opener) {
                    if (raf != null && raf.getChannel().isOpen()) {
                        raf.close();
                    }
                }
            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        if (!bufferMode.isStable() && file.exists()) {

            if (!file.delete()) {

                log.warn("Unable to delete temporary file: " + file);
                
            }
            
        }
        
    }

    public void deleteResources() {
        
        if (isOpen()) {

            throw new IllegalStateException();

        }
        
        if (fileOpened && file.exists() && !file.delete()) {
            
            log.warn("Could not delete file: " + file.getAbsoluteFile());
            
        }
        
    }

    final public long getExtent() {

        return extent;

    }

    final public long getUserExtent() {
        
        return userExtent;
        
    }

    /**
     * Extended to handle {@link ChecksumError}s by reading on another node when
     * the {@link Quorum} (iff the quorum is highly available).
     * <p>
     * {@inheritDoc}
     * 
     * @todo hook for monitoring (nagios, etc). bad reads indicate a problem
     *       with the disk which should be tracked over time.
     * 
     * @todo If we see a read error from a checksum and want to update the
     *       record on the backing file then we would have to go around the
     *       write cache to do a direct disk write since (at least for the WORM)
     *       the assumption is pure append for the write cache.
     *       <p>
     *       An attempt to overwrite a bad record on the disk could itself be a
     *       bad idea. If it was just a high write, then it might be Ok. But
     *       many other kinds of errors are likely to have long pauses while the
     *       OS attempts to get a good read/write from the file system.
     * 
     * @todo If the record can be successfully read from the remote quorum, then
     *       it will generally be inserted into the {@link LRUNexus} which will
     *       reduce the likelihood that we will attempt to read it from the
     *       backing file "soon.
     *       <p>
     *       We might want to maintain a set of known bad records and fail the
     *       node when the size of that set grows too large. That would also
     *       help us to avoid "hanging" on a bad read when we know that we have
     *       to get the data from another node based on past experience for that
     *       record.
     */
    public ByteBuffer read(final long addr) {

        try {
            // Try reading from the local store.
            return readFromLocalStore(addr);
        } catch (InterruptedException e) {
            // wrap and rethrow.
            throw new RuntimeException(e);
        } catch (ChecksumError e) {
            /*
             * Note: This assumes that the ChecksumError is not wrapped by
             * another exception. If it is, then the ChecksumError would not be
             * caught.
             */
            // log the error.
            try {
                log.error(e + " : addr=" + toString(addr), e);
            } catch (Throwable ignored) {
                // ignore error in logging system.
            }
            // update the performance counters.
            final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                    .acquire();
            try {
                c.checksumErrorCount++;
            } finally {
                c.release();
            }
//            final Quorum<?, ?> quorum = quorumRef.get();
            if (quorum != null && quorum.isHighlyAvailable()) {
                if (quorum.isQuorumMet()) {
                    try {
                        // Read on another node in the quorum.
                        final byte[] a = ((QuorumRead<?>) quorum.getMember())
                                .readFromQuorum(storeUUID, addr);
                        return ByteBuffer.wrap(a);
                    } catch (Throwable t) {
                        throw new RuntimeException("While handling: " + e, t);
                    }
                }
            }
            // Otherwise rethrow the checksum error.
            throw e;
        }
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation tests the {@link WriteCacheService} first
     * and then reads through to the local disk on a cache miss. This is
     * automatically invoked by {@link #read(long)}. 
     */
    public ByteBuffer readFromLocalStore(final long addr)
            throws InterruptedException {
        
        final long begin = System.nanoTime();
        
        if (addr == 0L)
            throw new IllegalArgumentException(ERR_ADDRESS_IS_NULL);

        final long offset = getOffset(addr);

        final int nbytes = getByteCount(addr);

        if (nbytes == 0) {

            throw new IllegalArgumentException(ERR_RECORD_LENGTH_ZERO);

        }

        if (offset + nbytes > nextOffset.get()) {

            throw new IllegalArgumentException(ERR_ADDRESS_NOT_WRITTEN);

        }

        {
            final StoreCounters<?> storeCounters = (StoreCounters<?>) this.storeCounters
                    .get().acquire();
            try {
                if (nbytes > storeCounters.maxReadSize) {
                    storeCounters.maxReadSize = nbytes;
                }
            } finally {
                storeCounters.release();
            }
        }

        if (writeCacheService != null) {

            /*
             * Test the write cache for a hit. The WriteCacheService handles
             * synchronization internally.
             * 
             * Note: WriteCacheService#read(long) DOES NOT throw an
             * IllegalStateException for an asynchronous close. However, it will
             * throw a RuntimeException if there is a checksum error on the
             * record.
             */
            // Note: Can throw ChecksumError, InterruptedException
            ByteBuffer tmp = writeCacheService.read(offset);
            if (tmp != null) {
                /*
                 * Hit on the write cache.
                 * 
                 * Update the store counters.
                 */
                final StoreCounters<?> c = (StoreCounters<?>) storeCounters
                        .get().acquire();
                try {
                    c.nreads++;
                    c.bytesRead += nbytes;
                    c.elapsedReadNanos += (System.nanoTime() - begin);
                } finally {
                    c.release();
                }
//                if (log.isTraceEnabled())
//                    log.trace("cacheRead: addr=" + toString(addr));
                return tmp;
            }
            
        } // if(writeCacheService!=null)

        /*
         * Read through to the disk.
         * 
         * Note: Strip off the checksum from the end of the record and validate
         * it.
         */
        final Lock readLock = extensionLock.readLock();
        readLock.lock();
        try {

            // Allocate a new buffer of the exact capacity.
            final ByteBuffer dst = ByteBuffer.allocate(nbytes);

            // Read through to the disk.
            final long beginDisk = System.nanoTime();

            try {

                // the offset into the disk file.
                final long pos = headerSize + offset;

                // read on the disk.
                final int ndiskRead = FileChannelUtility.readAll(opener, dst,
                        pos);

                // update performance counters.
                final StoreCounters<?> c = (StoreCounters<?>) storeCounters
                        .get().acquire();
                try {
                    c.ndiskRead += ndiskRead;
                } finally {
                    c.release();
                }

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            // flip for reading.
            dst.flip();

            if(useChecksums) {

                // extract the checksum.
                final int chk = dst.getInt(nbytes - 4);

                // adjust the record length to exclude the checksum.
                dst.limit(nbytes - 4);
                
                if (chk != ChecksumUtility.threadChk.get().checksum(dst)) {
                    
                    throw new ChecksumError("offset=" + offset + ", nbytes="
                            + nbytes);
                
                }

            }
            
            // Update counters.
            final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                    .acquire();
            try {
                c.nreads++;
                c.bytesRead += nbytes;
                c.bytesReadFromDisk += nbytes;
                c.elapsedReadNanos += (System.nanoTime() - begin);
                c.elapsedDiskReadNanos += (System.nanoTime() - beginDisk);
            } finally {
                c.release();
            }

            if (log.isTraceEnabled())
                log.trace("diskRead: addr=" + toString(addr));
            
            // return the buffer.
            return dst;

        } finally {

            readLock.unlock();
            
        }

    }
    
    /**
     * Used to re-open the {@link FileChannel} in this class.
     */
    private final IReopenChannel<FileChannel> opener = new IReopenChannel<FileChannel>() {

        public String toString() {
            
            return file.toString();
            
        }
        
        public FileChannel reopenChannel() throws IOException {

            return WORMStrategy.this.reopenChannel();

        }
        
    };

    /**
     * This method transparently re-opens the channel for the backing file.
     * <p>
     * Note: This method is synchronized (internally) so that concurrent readers
     * do not try to all open the store at the same time.
     * <p>
     * Note: This method is invoked by both readers and writers. It WILL NOT be
     * invoked by {@link FileChannelUtility} if the channel was closed by an
     * interrupt in the current thread (a different exception is thrown).
     */
    private FileChannel reopenChannel() throws IOException {

        synchronized (opener) {

            assertOpen();

            if (raf != null && raf.getChannel().isOpen()) {

                /*
                 * The channel is still open. If you are allowing concurrent
                 * reads on the channel, then this could indicate that two
                 * readers each found the channel closed and that one was able
                 * to re-open the channel before the other such that the channel
                 * was open again by the time the 2nd reader got here.
                 */

                return raf.getChannel();

            }

            if (temporaryStore && !fileOpened) {

                /*
                 * The backing file has not been opened.
                 * 
                 * Note: Without this case this method would create the backing
                 * store for a Temporary store if anyone happened to invoke it.
                 * In fact, this method will never get invoked for a Temporary
                 * store without a backing store since the reads never read
                 * against the channel because it does not exist. So, really,
                 * this is just here to be paranoid.
                 */

                throw new AssertionError("TemporaryStore not yet open: " + file);

            }

            // open the file.
            this.raf = new RandomAccessFile(file, fileMode);

            if (log.isInfoEnabled())
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
                     * lock. This can happen if the platform does not support
                     * shared locks or if someone requested an exclusive file
                     * lock.
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

                if (log.isInfoEnabled())
                    log.info("FileLock not supported: file=" + file, ex);

            }

            // Update counters.
            final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                    .acquire();
            try {
                c.nreopen++;
            } finally {
                c.release();
            }

            return raf.getChannel();

        }

    }

    /**
     * Note: Synchronization is provided by the caller (
     * {@link #write(ByteBuffer)}
     * 
     * @param nbytes
     *            The #of bytes in the record.
     *            
     * @return The address at which that record may be written.
     */
    private long allocate(final int nbytes) {
        
//        if (isReadOnly())
//            throw new IllegalStateException(ERR_READ_ONLY);
//        
//        if (nbytes <= 0)
//            throw new IllegalArgumentException(ERR_BAD_RECORD_SIZE);
        
//        final long addr; // address in the store.
        
//        synchronized(nextOffset) {
            
        /*
         * The offset at which the record will be written on the disk file
         * (not adjusted for the root blocks).
         */
        final long offset = nextOffset.get();

        /*
         * Make sure that the allocated region of the file exists (this will
         * acquire the extensionLock's write lock in truncate(long) if it
         * has to change the size of the file).
         */
        overflow(offset, nbytes);
        
        /* 
         * Formulate the address that can be used to recover that record.
         */
        final long addr = toAddr(nbytes, offset);
        
        /*
         * Increment the offset of the next address to be assigned by the
         * #of bytes in the record.
         */
        nextOffset.addAndGet(nbytes);
        
//        }

        return addr;
        
    }

    public long write(final ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException(ERR_BUFFER_NULL);

        if (isReadOnly())
            throw new IllegalStateException(ERR_READ_ONLY);
        
        // #of bytes in the record.
        final int remaining = data.remaining();
        
        // #of bytes to write onto the file (includes the optional checksum).
        final int nwrite = remaining + (useChecksums ? 4 : 0);

        if (remaining == 0)
            throw new IllegalArgumentException(ERR_BUFFER_EMPTY);

        final long begin = System.nanoTime();
        
//        final StoreCounters storeCounters = this.storeCounters.get();

        // get checksum for the buffer contents.
        final int chk = useChecksums ? ChecksumUtility.threadChk.get()
                .checksum(data) : 0;

        final long addr; // address in the store.
        try {

            /*
             * The offset at which the record will be written on the disk file
             * (not adjusted for the root blocks).
             */
            final long offset;
            synchronized (writeOnCacheLock) {

                /*
                 * Allocate address for a new record with [nbytes] of data.
                 * 
                 * Note: [writeOnCacheLock] imposes a total ordering over the
                 * allocations AND also over those records which get written
                 * through to the write cache.
                 * 
                 * Note: Records written onto the cache for the WORM store must
                 * be single threaded in order to ensure that the records are
                 * laid out on the cache buffer in their allocation order.
                 * Otherwise they could be in a jumbled order. We could of
                 * course use the ordered write version of the cache to work
                 * around that, but writes on the cache should be very fast
                 * (especially if the cache is a service since it will hand us a
                 * new buffer if the old one is full).
                 * 
                 * Note: We do not need to protect against concurrent close of
                 * the write cache since it is only closed by close() and
                 * closeForWrites() and the caller is responsible for ensuring
                 * that there are no concurrent writes during those methods
                 * (concurrent readers are allowed, but will be interrupted by
                 * close()).
                 */
                // Note: allocation must include the optional checksum.
                addr = allocate(nwrite);

                offset = getOffset(addr);

                boolean wroteOnCache = false;
                if (writeCacheService != null) {
                    if (!writeCacheService.write(offset, data, chk))
                        throw new AssertionError();
                    wroteOnCache = true;
                }
                if (!wroteOnCache) {

                    /*
                     * The writeCache is disabled or the record is too large for
                     * the write cache, so just write the record directly on the
                     * disk.
                     * 
                     * Note: At this point the backing file is already extended.
                     * 
                     * Note: Unlike writes on the cache, the order in which we
                     * lay down this write onto the disk does not matter. We
                     * have already made the allocation and now the caller will
                     * block until the record is on the disk.
                     */

                    final Lock readLock = extensionLock.readLock();
                    readLock.lock();
                    try {

                        writeOnDisk(data, offset);

                        if (useChecksums) {
                            /*
                             * Note: If [useChecksums] is enabled but we are not
                             * using the WriteCacheService then we also need to
                             * write the checksum on the file here.
                             */
                            final ByteBuffer b = _checkbuf;
                            b.clear();
                            b.putInt(chk);
                            b.flip();
                            writeOnDisk(b, offset + remaining);
                        }

                    } finally {

                        readLock.unlock();

                    }

                } // if(!wroteOnCache)

            } // synchronized(writeOnCacheLock)

            // Update counters.
            final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                    .acquire();
            try {
                c.nwrites++;
                c.bytesWritten += nwrite;
                c.elapsedWriteNanos += (System.nanoTime() - begin);
                if (nwrite > c.maxWriteSize) {
                    c.maxWriteSize = nwrite;
                }
            } finally {
                c.release();
            }

        } catch(InterruptedException ex) {
            
            throw new RuntimeException(ex);

        }

        return addr;

    }

    /**
     * An object used to serialize the allocation of record addresses and their
     * writes onto the cache. See write() above for why. This is also relied on
     * to make {@link #flushWriteCache()} atomic.
     */
    private final Object writeOnCacheLock = new Object();

    /**
     * A small direct {@link ByteBuffer} used if we need to write the checksum
     * on the backing file directly because the {@link WriteCacheService} is not
     * in use.
     */
    private final ByteBuffer _checkbuf;
    
    /**
     * Make sure that the file is large enough to accept a write of
     * <i>nbytes</i> starting at <i>offset</i> bytes into the file. This is only
     * invoked from {@link #allocate(int)}, which is responsible for ensuring
     * that the store file is large enough on the disk for any address which it
     * returns to its caller. This way {@link #writeOnDisk(ByteBuffer, long)}
     * does not have to check the extent on the disk.
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
     * Overridden to use the value specified to the constructor.
     */
    @Override
    protected long getMinimumExtension() {
        
        return minimumExtension;
        
    }
    
    /**
     * Create/open the backing file for a {@link BufferMode#Temporary} store iff
     * it has not been created/opened.
     */
    final private void createBackingFile() {
        
        if (!fileOpened && temporaryStore) {
            
            try {
                
//                // open the file for the first time (create).
//                raf = FileLockUtility.openFile(file, fileMode,
//                        bufferMode != BufferMode.Mapped/*useTryLock*/);

                // note: set true so that reopenChannel will create the file.
                fileOpened = true;
                
                reopenChannel();
                
                if (log.isInfoEnabled())
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
     * Note: The caller MUST hold either the read lock or the write lock on the
     * {@link #extensionLock}. Normal writes should hold the read lock. File
     * size changes MUST hold the write lock.
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
     *            The offset in the user extent at which the data will be
     *            written (it is automatically adjusted for the size of the root
     *            blocks).
     * 
     * @return The #of write operations against the disk.
     */
    private int writeOnDisk(final ByteBuffer data, final long offset) {
        
        assert offset >= 0 : "offset=" + offset;
        
        // Thread MUST have either the read or write lock.
        assert extensionLock.getReadHoldCount() > 0
                || extensionLock.getWriteHoldCount() > 0;
        
        final long begin = System.nanoTime();

//        final StoreCounters storeCounters = this.storeCounters.get();

        createBackingFile();
        
        final int nbytes = data.remaining();

        /* 
         * The position in the file at which the record will be written
         * (this is adjusted for the root blocks).
         */

        final long pos = headerSize + offset;

        final int nwrites;
        try {

            /*
             * Write bytes in [data] from position to limit onto the channel.
             * 
             * Note: Since readers ARE NOT synchronized it is possible for a
             * reader is to be interrupted during a concurrent NIO operation and
             * thus the channel MAY be asynchronously closed while we are
             * writing on it, in which case we need to retry the write until it
             * succeeds.
             */

            nwrites = FileChannelUtility.writeAll(opener, data, pos);

            // Update counters.
            final long elapsed = (System.nanoTime() - begin);
            final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                    .acquire();
            try {
                c.ndiskWrite += nwrites;
                c.bytesWrittenOnDisk += nbytes;
                c.elapsedDiskWriteNanos += elapsed;
            } finally {
                c.release();
            }
            
            if (log.isTraceEnabled()) {
                /*
                 * Note: There are only two places where the journal writes on the
                 * disk using this backing buffer implementation. Here and when it
                 * updates the root blocks. It only syncs the disk at the commit.
                 */
                log.trace("wrote on disk: bytes="
                        + nbytes
                        + ", elapsed="
                        + TimeUnit.NANOSECONDS.toMillis(elapsed)
                        + "ms; totals: write="
                        + TimeUnit.NANOSECONDS
                                .toMillis(storeCounters.get().elapsedDiskWriteNanos)
                        + "ms, read="
                        + TimeUnit.NANOSECONDS
                                .toMillis(storeCounters.get().elapsedDiskReadNanos)
                        + "ms");
            }

            return nwrites;

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public ByteBuffer readRootBlock(final boolean rootBlock0) {
        
        if(!isOpen()) throw new IllegalStateException();

        final ByteBuffer tmp = ByteBuffer
                .allocate(RootBlockView.SIZEOF_ROOT_BLOCK);

        try {

            FileChannelUtility.readAll(opener, tmp,
                rootBlock0 ? FileMetadata.OFFSET_ROOT_BLOCK0
                        : FileMetadata.OFFSET_ROOT_BLOCK1);
            
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

            final long pos = rootBlock.isRootBlock0()
                    ? FileMetadata.OFFSET_ROOT_BLOCK0
                    : FileMetadata.OFFSET_ROOT_BLOCK1;

            /*
             * Note: This uses the [opener] to automatically retry the operation
             * in case concurrent readers are interrupting, causing an
             * asynchronous close of the backing channel.
             * 
             * @todo Consider using the read lock vs the write lock of the
             * extensionLock here. The advantage of the read lock is higher
             * concurrency. The advantage of the write lock is that it locks out
             * readers when we are writing the root blocks, which could help to
             * ensure timely updates of the root blocks even if readers are
             * behaving badly (lots of interrupts).
             * 
             * FIXME Modify AbstractInterruptsTestCase to test for correct
             * handling of root block writes where concurrent readers cause the
             * backing store to be closed asynchronously. This code block SHOULD
             * cause the root block write to eventually succeed.
             */
            final Lock lock = extensionLock.readLock();
            lock.lock();
            try {

                // Update the root block.
                FileChannelUtility.writeAll(opener, data, pos);

                if (!temporaryStore) {

                    /*
                     * Generally, you want to force the file data to the disk
                     * here. The file metadata MIGHT not matter since we always
                     * force it to the disk when we change the file size (unless
                     * the file system updates other aspects of file metadata
                     * during normal writes).
                     * 
                     * @todo make sure the journal has already forced the
                     * writes, that forcing an empty cache buffer is a NOP, and
                     * that we want to just force the channel after we write the
                     * root blocks since writes were already forced on each node
                     * in the quorum before we wrote the root blocks and the
                     * root blocks are transmitted using RMI not the write
                     * pipeline.
                     */
                    
                    // sync the disk.
                    getChannel().force(forceOnCommit == ForceEnum.ForceMetadata);
//                    force(forceOnCommit == ForceEnum.ForceMetadata);
                    
                }

                // Update counters.
                final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                        .acquire();
                try {
                    c.nwriteRootBlock++;
                } finally {
                    c.release();
                }
                
            } finally {

                lock.unlock();
                
            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        if (log.isDebugEnabled())
            log.debug("wrote root block: "+rootBlock);
        
    }

    public void truncate(final long newExtent) {

        final long newUserExtent = newExtent - headerSize;

        if (newUserExtent < getNextOffset()) {

            throw new IllegalArgumentException(ERR_TRUNCATE);

        }

        if (newUserExtent == getUserExtent()) {

            // NOP.
            return;

        }

        final Lock writeLock = extensionLock.writeLock();
        writeLock.lock();
        try {

            /*
             * Note: This handles the case for a Temporary store where the write
             * cache is the same size as the initial extent and everything
             * written so far has been absorbed by the write cache.
             */
            createBackingFile();

            /*
             * Extend (or truncate) the file.
             * 
             * FIXME I could see how this might fail with a concurrent interrupt
             * of a reader. This "extend" needs to be robust just writeAll() on
             * FileChannelUtility. It must use the opener and retry if there is
             * a ClosedByInterruptException. [See the notes below in the catch
             * clause.]
             */
            getRandomAccessFile().setLength(newExtent);

            if (writeCacheService != null) {
                /*
                 * Inform the write cache service that the file extent has
                 * changed. It will propagate this message along the write
                 * pipeline when HA is enabled.
                 */
                try {
                    writeCacheService.setExtent(newExtent);
                } catch (InterruptedException t) {
                    throw new RuntimeException(t);
                }
            }

            // Update fields and counters while holding the lock.
            this.userExtent = newUserExtent;
            this.extent = newExtent;
            storeCounters.get().ntruncate++;

            /*
             * Since we just changed the file length we force the data to disk
             * and update the file metadata. This is a relatively expensive
             * operation but we want to make sure that we do not loose track of
             * a change in the length of the file.
             * 
             * @todo Per the link below, consider modifying this to force for a
             * temporary store as well. At issue is whether the Java bug is
             * linked to the update of the file metadata or just a concurrency
             * issue. If it is only a concurrency issue, then there is no reason
             * to force the file metadata for a temporary store.
             * 
             * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
             */
            if (!temporaryStore) {

                /*
                 * We need to force the file data and metadata to the disk. When
                 * integrated with the WriteCacheService the FileChannel#force()
                 * request will be executed in a different thread and would
                 * deadlock unless we first release the WriteLock since
                 * writeOnChannel needs to acquire the ReadLock to proceed.
                 * 
                 * We address this by doing acquiring the ReadLock (we are
                 * already holding the WriteLock so this will not block) and
                 * then releasing the WriteLock so other threads may now also
                 * acquire the ReadLock.
                 * 
                 * Note: An alternative would be to directly invoke force(true)
                 * on the FileChannel.
                 */
                extensionLock.readLock().lock();
                extensionLock.writeLock().unlock();
                force(true/*metadata*/);
//              opener.reopenChannel().force(true/*metadata*/);

            }

            if (WARN)
                log.warn("newLength=" + cf.format(newExtent) + ", file="+ file);

            if (log.isInfoEnabled())
                log.info(getCounters().toString());

        } catch (IOException ex) {

            /*
             * FIXME I've see an IOException "The handle is invalid" tossed here
             * (just once). A bit of searching around suggests that perhaps the
             * RandomAccessFile was concurrently closed? Something to look out
             * for if it happens again. [@todo probably a concurrent reader was
             * interrupted, in which case this method should just try the
             * setLength() operation again.] [@todo the MRMW test can throw this
             * during test shutdown, which simulates interrupt of NIO
             * operations].
             */
            throw new RuntimeException(ex);

        } finally {

//            writeLock.unlock();
            extensionLock.readLock().unlock();

        }

    }

    /*
     * @todo why is this synchronized? the operation should be safe. maybe
     * against a concurrent close?
     */
    synchronized public long transferTo(final RandomAccessFile out)
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
     * Extended to discard the write cache (we will depend on the
     * {@link LRUNexus} to buffer recent writes on the journal since we can't
     * read through the write cache after this).
     * <p>
     * Note: The file is NOT closed and re-opened in a read-only mode in order
     * to avoid causing difficulties for concurrent readers.
     */
    public void closeForWrites() {

        // sets the [readOnly] flag.
        super.closeForWrites();

        // discard the write cache.
        releaseWriteCache();
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: {@link #read(long)} has logic to handle the concurrent close of the
     * {@link WriteCacheService}, passing through the operation to the disk.
     * 
     * @todo Should this get invoked from {@link #closeForWrites()} for HA? If
     *       read failover is handled by the {@link WriteCacheService} then we
     *       can not close it out here.
     */
    private final void releaseWriteCache() {

        if (writeCacheService != null) {
            try {
                writeCacheService.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
            
    }

    /**
     * This implementation can not release storage allocations and invocations
     * of this method are ignored.
     */
	public void delete(long addr) {
		// NOP
	}

    public void writeRawBuffer(final HAWriteMessage msg, final ByteBuffer b)
            throws IOException, InterruptedException {

        writeCacheService.newWriteCache(b, useChecksums,
                true/* bufferHasData */, opener).flush(false/* force */);

    }

    /**
     * Extend file if required for HAWriteMessage - just call through to truncate
     */
    public void setExtentForLocalStore(final long extent) throws IOException,
            InterruptedException {

        truncate(extent);

    }

}
