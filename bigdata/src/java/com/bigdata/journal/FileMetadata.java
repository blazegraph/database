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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.io.FileChannelUtility;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.resources.StoreManager;
import com.bigdata.util.ChecksumUtility;

/**
 * Helper object used when opening or creating journal file in any of the
 * file-based modes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileMetadata {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(FileMetadata.class);
    protected static final boolean INFO = log.isInfoEnabled();
    protected static final boolean DEBUG = log.isDebugEnabled();

    static final int SIZE_MAGIC = Bytes.SIZEOF_INT;
    static final int SIZE_VERSION = Bytes.SIZEOF_INT;
    static final int SIZEOF_ROOT_BLOCK = RootBlockView.SIZEOF_ROOT_BLOCK;

    /**
     * Magic value for journal (the root blocks have their own magic value).
     */
    final int MAGIC = 0xe6b4c275;
    
    /**
     * Journal version number (version 1).
     */
    final int VERSION1 = 0x1;
    
    /**
     * The file that was opened.
     */
    final File file;

    /**
     * The mode used to open the file.
     */
    final String fileMode;

    /**
     * The buffer mode used to open that file.
     */
    final BufferMode bufferMode;
    
    /**
     * The interface for IO performed on that file.
     */
    final RandomAccessFile raf;
    
    /**
     * The 32-bit magic value at offset 0L in the file.
     * 
     * @see #MAGIC
     */
    final int magic;
    
    /**
     * The 32-bit version number at offset 4 in the file.
     */
    final int version;
    
    /**
     * The extent of the file in bytes.
     */
    final long extent;
    
    /**
     * The extent of the user data space (everything after the root blocks).
     */
    final long userExtent;

    /**
     * The #of bits out of a 64-bit long integer that are used to encode the
     * byte offset as an unsigned integer.  The remaining bits are used to
     * encode the byte count (aka record length) as an unsigned integer.
     */
    final int offsetBits;

    /**
     * The #of records to be buffered in an optional read cache -or- ZERO(0) to
     * disable the read cache.
     */
    final int readCacheCapacity;
    
    /**
     * The maximum size of a record that will be allowed into the read cache.
     */
    final int readCacheMaxRecordSize;
    
    /**
     * The optional {@link ByteBuffer} to be used as the write cache.
     */
    final ByteBuffer writeCache;
    
    /**
     * The next offset at which a record would be written on the store.  The
     * offset is relative to the start of the user data space.  Offset zero(0)
     * addresses the first byte after the root blocks.
     */
    final long nextOffset;
    
    /**
     * True iff the file was opened in a read-only mode.
     */
    final boolean readOnly;

    /**
     * The timestamp from the createTime field in the root block.
     */
    final long createTime;
    
    /**
     * The timestamp from the closeTime field in the root block.
     */
    final long closeTime;
    
    /**
     * Offset of the first root block in the file.
     */
    static final int OFFSET_ROOT_BLOCK0 = SIZE_MAGIC + SIZE_VERSION;
    
    /**
     * Offset of the second root block in the file.
     */
    static final int OFFSET_ROOT_BLOCK1 = SIZE_MAGIC + SIZE_VERSION + (SIZEOF_ROOT_BLOCK * 1);
    
    /**
     * The size of the journal header, including MAGIC, version, and both root
     * blocks. This is used as an offset when computing the index of a record in
     * the journal.
     */
    static final int headerSize0 = SIZE_MAGIC + SIZE_VERSION + (SIZEOF_ROOT_BLOCK * 2);
    
    /**
     * Depending on the mode, this will be either a direct buffer, a mapped
     * buffer or [null] if no buffer is being used.
     */
    final ByteBuffer buffer;

    /**
     * True iff the file was pre-existing.
     */
    final boolean exists;

    /**
     * The 1st root block.
     */
    IRootBlockView rootBlock0;

    /**
     * The 2nd root block.
     */
    IRootBlockView rootBlock1;
    
    /**
     * The current root block. For a new file, this is "rootBlock0". For an
     * existing file it is based on an examination of both root blocks.
     */
    final IRootBlockView rootBlock;
    
    /**
     * Prepare a journal file for use by an {@link IBufferStrategy}.
     * 
     * @param file
     *            The name of the file to be opened.
     * @param bufferMode
     *            The {@link BufferMode}.
     * @param useDirectBuffers
     *            true if a buffer should be allocated using
     *            {@link ByteBuffer#allocateDirect(int)} rather than
     *            {@link ByteBuffer#allocate(int)}. This has no effect for the
     *            {@link BufferMode#Disk} and {@link BufferMode#Mapped} modes.
     * @param initialExtent
     *            The initial extent of the journal. The size of the journal is
     *            automatically increased up to the <i>maximumExtent</i> on an
     *            as necessary basis.
     * @param maximumExtent
     *            The maximum extent of the journal before it will
     *            {@link Journal#overflow()}.
     * @param create
     *            When true, the file is created if it does not exist (this is
     *            ignored for {@link BufferMode#Temporary} files since they are
     *            created lazily if at all).
     * @param isEmptyFile
     *            This flag must be set when the temporary file mechanism is
     *            used to create a new temporary file otherwise an empty file is
     *            treated as an error since it does not contain valid root
     *            blocks.
     * @param deleteOnExit
     *            When set, a <em>new</em> file will be marked for deletion
     *            when the VM exits. This may be used as part of a temporary
     *            store strategy.
     * @param readOnly
     *            When true, the file is opened in a read-only mode and it is an
     *            error if the file does not exist.
     * @param forceWrites
     *            When true, the file is opened in "rwd" mode and individual IOs
     *            are forced to disk. This option SHOULD be false since we only
     *            need to write through to disk on commit, not on each IO.
     * @param offsetBits
     *            The #of bits out of a 64-bit long integer that are used to
     *            encode the byte offset as an unsigned integer. The remaining
     *            bits are used to encode the byte count (aka record length) as
     *            an unsigned integer.
     * @param readCacheCapacity
     *            The capacity of an optional read cache. When ZERO(0) the read
     *            cache will be disabled. The capacity specifies the #of records
     *            that will be retained by an LRU style cache. Note that this
     *            option is only supported by the {@link DiskOnlyStrategy}.
     *            Further note that most of the other {@link IBufferStrategy}s
     *            are already fully buffered and hence can not benefit from a
     *            read cache.
     * @param readCacheMaxRecordSize
     *            The maximum size of a record that will be allowed into the
     *            optional read cache.
     * @param writeCache
     *            A direct {@link ByteBuffer} to be used as the write cache
     *            (optional). Note that this write cache is only used by the
     *            {@link DiskOnlyStrategy}. Further note that this MUST be a
     *            direct {@link ByteBuffer} in order to avoid potential memory
     *            leaks since NIO will otherwise force the allocation of a
     *            "temporary" direct {@link ByteBuffer} and there is a known bug
     *            from 1.4 through at least 1.6 with the release of such
     *            temporary buffers.
     * @param createTime
     *            The create time to be assigned to the root block iff a new
     *            file is created.
     * @param validateChecksum
     *            When <code>true</code>, the checksum stored in the root
     *            blocks of an existing file will be validated when the file is
     *            opened. See {@link Options#VALIDATE_CHECKSUM}.
     * @param checker
     *            The object used to compute the checksum of the root blocks.
     * @throws RuntimeException
     *             if there is a problem preparing the file for use by the
     *             journal.
     */
    FileMetadata(File file, BufferMode bufferMode, boolean useDirectBuffers,
            long initialExtent, long maximumExtent, boolean create,
            boolean isEmptyFile, boolean deleteOnExit, boolean readOnly,
            ForceEnum forceWrites, int offsetBits, int readCacheCapacity,
            int readCacheMaxRecordSize, ByteBuffer writeCache,
            boolean validateChecksum, final long createTime,
            ChecksumUtility checker) throws RuntimeException {

        if (file == null)
            throw new IllegalArgumentException();

        if (bufferMode == null)
            throw new IllegalArgumentException();

        if( bufferMode == BufferMode.Transient ) {
            
            // This mode is not a valid option in this context.
            
            throw new IllegalArgumentException();
            
        }

        if (readOnly && create) {

            throw new IllegalArgumentException("'" + Options.CREATE
                    + "' may not be used with '" + Options.READ_ONLY + "'");

        }

        if (readOnly && forceWrites != ForceEnum.No) {

            throw new IllegalArgumentException("'" + Options.FORCE_WRITES
                    + "'='" + forceWrites + "' may not be used with '"
                    + Options.READ_ONLY + "'");

        }

        WormAddressManager.assertOffsetBits(offsetBits);
        
        this.bufferMode = bufferMode;

        this.offsetBits = offsetBits;

        this.readCacheCapacity = readCacheCapacity;
        
        this.readCacheMaxRecordSize = readCacheMaxRecordSize;
        
        this.writeCache = writeCache;
        
        this.fileMode = (readOnly ?"r" :forceWrites.asFileMode());

        this.readOnly = readOnly;
        
        this.exists = !isEmptyFile && file.exists();
        
        // true for a temporary file
        final boolean temporary = bufferMode.equals(BufferMode.Temporary);

        if(temporary) {
            
            // override for temporary files.
            deleteOnExit = true;
            
        }
        
        this.file = file;
        
        if (exists && !temporary) {

            if(INFO) log.info("Opening existing file: "
                    + file.getAbsoluteFile());

        } else {

            if (readOnly) {

                throw new RuntimeException("File does not exist and '"
                        + Options.READ_ONLY + "' was specified: "
                        + file.getAbsoluteFile());

            }

            if ( ! create && ! isEmptyFile ) {

                throw new RuntimeException("File does not exist and '"
                        + Options.CREATE + "' was not specified: "
                        + file.getAbsoluteFile());

            }

            /*
             * Note: a temporary file that does not exist is created _lazily_.
             * See below and DiskOnlyStrategy.
             */
            if(INFO)
                log.info("Backing file: exists=" + exists + ", temporary="
                    + temporary + ", create=" + create + ", readOnly="
                    + readOnly + ", file=" + file.getAbsoluteFile());

        }
        
        try {
            
            /*
             * Open/create the file (temporary files are not opened/created eagerly).
             */
            this.raf = temporary ? null : openFile(file, fileMode,
                    bufferMode != BufferMode.Mapped); 
                
            if (exists && !temporary) {
    
                /*
                 * The file already exists (but not for temporary files).
                 */
    
                this.extent = raf.length();
                
                this.userExtent = extent - headerSize0;
                
                if( this.extent <= headerSize0 ) {
    
                    /*
                     * By throwing an exception for files that are not large enough
                     * to contain the MAGIC, VERSION, and both root blocks we avoid
                     * IO errors when trying to read those data and are able to
                     * reject files based on whether they have bad magic, version,
                     * or root blocks.
                     */
                    
                    throw new RuntimeException(
                            "File too small to contain a valid journal: "
                                    + file.getAbsoluteFile());
                    
                }
    
                if( bufferMode != BufferMode.Disk ) {
    
                    /*
                     * Verify that we can address this many bytes with this
                     * strategy. The strategies that rely on an in-memory buffer
                     * are all limited to the #of bytes that can be addressed by
                     * an int32.
                     */
    
                    AbstractBufferStrategy.assertNonDiskExtent(userExtent);
                    
                }
    
                /*
                 * Note: The code to read the MAGIC, VERSION, and root blocks is
                 * shared by DumpJournal (code is copy by value).
                 */
                
                /*
                 * Read the MAGIC and VERSION.
                 */
                raf.seek(0L);
                // Note: this next line will throw IOException if there is a file lock contention.
                magic = raf.readInt();
                if (magic != MAGIC)
                    throw new RuntimeException("Bad journal magic: expected="
                            + MAGIC + ", actual=" + magic);
                version = raf.readInt();
                if (version != VERSION1)
                    throw new RuntimeException("Bad journal version: expected="
                            + VERSION1 + ", actual=" + version);
    
                /*
                 * Check root blocks (magic, timestamps), choose root block, read
                 * constants (slotSize, segmentId).
                 */
                
                FileChannel channel = raf.getChannel();
                ByteBuffer tmp0 = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
                ByteBuffer tmp1 = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
                FileChannelUtility.readAll(channel, tmp0, OFFSET_ROOT_BLOCK0);
                FileChannelUtility.readAll(channel, tmp1, OFFSET_ROOT_BLOCK1);
//                int nread;
//                if ((nread = channel.read(tmp0, OFFSET_ROOT_BLOCK0)) != RootBlockView.SIZEOF_ROOT_BLOCK) {
//                    throw new IOException("Expected to read "
//                            + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but read "
//                            + nread + " bytes");
//                }
//                if ((nread = channel.read(tmp1, OFFSET_ROOT_BLOCK1)) != RootBlockView.SIZEOF_ROOT_BLOCK) {
//                    throw new IOException("Expected to read "
//                            + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but read "
//                            + nread + " bytes");
//                }
                tmp0.position(0); // resets the position.
                tmp1.position(0);
                try {
                    rootBlock0 = new RootBlockView(true,tmp0,validateChecksum?checker:null);
                } catch(RootBlockException ex ) {
                    log.warn("Bad root block zero: "+ex);
                }
                try {
                    rootBlock1 = new RootBlockView(false,tmp1,validateChecksum?checker:null);
                } catch(RootBlockException ex ) {
                    log.warn("Bad root block one: "+ex);
                }
                if( rootBlock0 == null && rootBlock1 == null ) {
                    throw new RuntimeException("Both root blocks are bad - journal is not usable: "+file);
                }
                // Choose the root block based on the commit counter.
                this.rootBlock =
                    ( rootBlock0.getCommitCounter() > rootBlock1.getCommitCounter()
                        ? rootBlock0
                        : rootBlock1
                        );
                
                /*
                 * The offset into the user extent at which the next record will be
                 * written.
                 */
                this.nextOffset = rootBlock.getNextOffset();
                
                this.createTime = rootBlock.getCreateTime();
                
                this.closeTime = rootBlock.getCloseTime();
                
                if (closeTime != 0L && !readOnly) {

                    throw new RuntimeException(
                            "Journal is closed for writes: closedTime="
                                    + closeTime);
                    
                }
                
                switch (bufferMode) {
                case Direct: {
                    // Allocate the buffer buffer.
                    buffer = (useDirectBuffers ? ByteBuffer
                            .allocateDirect((int) userExtent) : ByteBuffer
                            .allocate((int) userExtent));
                    // Setup to read data from file into the buffer.
                    if (nextOffset > Integer.MAX_VALUE) {
                        throw new RuntimeException(
                                "This file is too large for a buffered mode: use "
                                        + BufferMode.Disk);
                    }
                    buffer.limit((int)nextOffset);
                    buffer.position(0);
                    // Read the file image into the direct buffer.
                    FileChannelUtility.readAll(raf.getChannel(), buffer, headerSize0);
//                    final int nbytes = raf.getChannel().read(buffer,
//                            headerSize0);
//                    if (nbytes != nextOffset) {
//                        throw new IOException("Expected to read " + nextOffset
//                                + " bytes, but read " + nbytes + " bytes");
//                    }
                    break;
                }
                case Mapped: {
                    // Map the file.
                    boolean loadMappedFile = false; // @todo expose as property.
                    buffer = raf.getChannel().map(
                            FileChannel.MapMode.READ_WRITE, headerSize0, extent);
                    if( loadMappedFile ) {
                        /*
                         * Load the image into mapped memory. Generally, I would
                         * think that you are better off NOT loading the image.
                         * When you want the image in memory, use the Direct
                         * mode instead. It should be MUCH faster and has better
                         * control over the amount and timing of the IO.
                         */
                        ((MappedByteBuffer)buffer).load();
                    }
                    break;
                }
                case Disk:
                    buffer = null;
                    break;
                default:
                    throw new AssertionError();
                }
    
                /*
                 * Note: there should be no processing required on restart since the
                 * intention of transactions that did not commit will not be
                 * visible.
                 */
                
            } else {
    
                /*
                 * Create a new journal.
                 */
    
                if (deleteOnExit) {
                    
                    // Mark the file for deletion on exit.
                    file.deleteOnExit();
                    
                }
                
                /*
                 * Set the initial extent.
                 * 
                 * Note: since a mapped file CAN NOT be extended, we pre-extend
                 * it to its maximum extent here.
                 */
    
                this.extent = (bufferMode == BufferMode.Mapped ? maximumExtent
                        : initialExtent);
    
                this.userExtent = extent - headerSize0;
                
                if (bufferMode != BufferMode.Disk
                        && bufferMode != BufferMode.Temporary ) {
    
                    /*
                     * Verify that we can address this many bytes with this
                     * strategy. The strategies that rely on an in-memory buffer
                     * are all limited to the #of bytes that can be addressed by
                     * an int32.
                     */
    
                    AbstractBufferStrategy.assertNonDiskExtent(userExtent);
                    
                }

                /*
                 * The offset at which the first record will be written. This is
                 * zero(0) since the buffer offset (0) is the first byte after the
                 * root blocks.
                 */
                nextOffset = 0;

                magic = MAGIC;

                version = VERSION1;
                
                if (!temporary) {
                
                    /*
                     * Extend the file. We do this eagerly in an attempt to
                     * convince the OS to place the data into a contiguous
                     * region on the disk.
                     */
                    raf.setLength(extent);

                    /*
                     * Write the MAGIC and version on the file.
                     */
                    raf.seek(0);
                    raf.writeInt(MAGIC);
                    raf.writeInt(VERSION1);
                
                }

                /*
                 * Generate the root blocks. They are for all practical purposes
                 * identical (in fact, their timestamps will be distict). The root
                 * block are then written into their locations in the file.
                 */
                final long commitCounter = 0L;
                final long firstCommitTime = 0L;
                final long lastCommitTime = 0L;
                final long commitRecordAddr = 0L;
                final long commitRecordIndexAddr = 0L;
                final UUID uuid = UUID.randomUUID(); // journal's UUID.
                if(createTime == 0L) {
                    throw new IllegalArgumentException("Create time may not be zero.");
                }
                this.createTime = createTime;
                this.closeTime = 0L;
                final IRootBlockView rootBlock0 = new RootBlockView(true, offsetBits,
                        nextOffset, firstCommitTime, lastCommitTime,
                        commitCounter, commitRecordAddr, commitRecordIndexAddr,
                        uuid, createTime, closeTime, checker);
                final IRootBlockView rootBlock1 = new RootBlockView(false,
                        offsetBits, nextOffset, firstCommitTime,
                        lastCommitTime, commitCounter, commitRecordAddr,
                        commitRecordIndexAddr, uuid, createTime, closeTime,
                        checker);
                
                if(!temporary) {
                
                    FileChannel channel = raf.getChannel();
                    
                    FileChannelUtility.writeAll(channel, rootBlock0.asReadOnlyBuffer(), OFFSET_ROOT_BLOCK0);
                    
                    FileChannelUtility.writeAll(channel, rootBlock1.asReadOnlyBuffer(), OFFSET_ROOT_BLOCK1);
                    
                    /*
                     * Force the changes to disk. We also force the file metadata to
                     * disk since we just changed the file size and we do not want to
                     * loose track of that.
                     */
                    channel.force(true);
        
                }

                this.rootBlock = rootBlock0;
                
                switch (bufferMode) {
                case Direct:
                    /*
                     * Allocate the buffer.
                     * 
                     * Note that we do not read in any data since no user data has
                     * been written and the root blocks are not cached in the buffer
                     * to avoid possible overwrites.
                     */
                    buffer = (useDirectBuffers ? ByteBuffer
                            .allocateDirect((int) userExtent) : ByteBuffer
                            .allocate((int) userExtent));
                    break;
                case Mapped:
                    /*
                     * Map the file starting from the first byte of the user space
                     * and continuing through the entire user extent.
                     */
                    if(INFO)
                        log.info("Mapping file="+file);
                    buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE,
                            headerSize0, userExtent);
                    break;
                case Disk:
                    buffer = null;
                    break;
                case Temporary:
                    buffer = null;
                    break;
                default:
                    throw new AssertionError();
                }

            }

        } catch (IOException ex) {

            throw new RuntimeException("file=" + file, ex);
            
        }
        
    }
    
    /**
     * Create/open the file and obtain an exclusive lock.
     * <p>
     * A {@link FileLock} will be used when supported and requested. An advisory
     * lock will be used if <i>useFileLock == false</code>, if the <i>fileMode</i>
     * is read-only. If tryLock() returns <code>null</code> then the lock
     * exists and this request will fail. However, if
     * {@link FileChannel#tryLock()} throws an {@link IOException} then the
     * underlying platform does not support {@link FileLock} for the named file
     * (memory mapped files, read-only files, and NFS mounted files can all have
     * this problem) and we will attempt to acquire an advisory lock instead.
     * 
     * <strong>Advisory locks are NOT visible to other applications</strong>
     * 
     * <p>
     * Do NOT request a {@link FileLock} if you are going to use a memory-mapped
     * buffer. The JDK cautions that these things do not play well together on
     * some platforms.
     * 
     * @param file
     *            The file.
     * @param fileMode
     *            The file mode for
     *            {@link RandomAccessFile#RandomAccessFile(File, String)}
     * @param useFileLock
     *            <code>true</code> if {@link FileChannel#tryLock()} should be
     *            attempted. when <code>false</code> only an advisory lock
     *            will be sought.
     * 
     * @return The {@link RandomAccessFile}
     * 
     * @throws IOException
     *             If the file could not be opened or someone already holds a
     *             lock for that file.
     * 
     * @see #acquireAdvisoryLock(File)
     * 
     * @see Options#FILE_LOCK_ENABLED
     * 
     * @todo we really don't need locks for temporary files.
     * 
     * @todo handle lock files during {@link StoreManager} startup
     * 
     * @todo write unit tests for the advisory locks.
     */
    public static RandomAccessFile openFile(File file, String fileMode,
            boolean useFileLock) throws IOException {
        
        final boolean readOnly = "r".equals(fileMode)
                || Boolean.parseBoolean(System.getProperty(
                        Options.FILE_LOCK_ENABLED,
                        Options.DEFAULT_FILE_LOCK_ENABLED));
        
        final RandomAccessFile raf = new RandomAccessFile(file, fileMode);
        
        if (!readOnly && useFileLock) {//bufferMode != BufferMode.Mapped) {

            if (INFO)
                log.info("Seeking exclusive lock: " + file);
            
            try {
                
                // seek a native platform exclusive file lock.
                if (raf.getChannel().tryLock() != null) {

                    // got it.
                    return raf;
                    
                } else {
                    
                    /*
                     * A null return indicates that someone else holds the lock.
                     */
                    try {
                        raf.close();
                    } catch (Throwable t) {
                        // log and ignore.
                        log.error(t, t);
                    }

                    /*
                     * We were not able to get a lock on the file.
                     */
                    throw new RuntimeException("Already locked: "
                            + file.getAbsoluteFile());

                }
                
            } catch (IOException ex) {
                
                /*
                 * The platform does not support FileLock (memory mapped files,
                 * read-only files, NFS mounted files all have this problem).
                 */
                log.warn("FileLock not supported: file=" + file + " : " + ex);

                return _acquireAdvisoryLock(raf,file);
                
            }

        } else {
            
            /*
             * Either read only or FileLock was explicitly disabled.
             */

            return _acquireAdvisoryLock(raf, file);
            
        }

    }

    private static RandomAccessFile _acquireAdvisoryLock(RandomAccessFile raf,
            File file) throws IOException {

        try {
            
            // seek an advisory lock.
            if (acquireAdvisoryLock(file)) {

                // obtained advisory lock.
                return raf;
            }
            
            // someone else holds the advisory lock.
            try {
                raf.close();
            } catch (IOException t) {
                // log and ignore.
                log.error(t, t);
            }

            throw new IOException("Advisory lock exists: " + file);
            
        } catch (IOException ex2) {

            log.error("Error while seeking advisory lock: file=" + file, ex2);
            
            try {
                raf.close();
            } catch (IOException t) {
                // log and ignore.
                log.error(t, t);
            }
            throw ex2;
            
        }

    }
    
    /**
     * Close the file and automatically releases the {@link FileLock} (if any)
     * and removes the advisory lock for that file (if any).
     * <p>
     * Note: This method should be used in combination with
     * {@link #openFile(File, String, boolean)} in order to ensure that the
     * optional advisory lock file is deleted when the file is closed. The
     * purpose of the advisory lock file is to provide advisory locking file
     * modes (read-only), platforms, or file systems (NFS) that do not support
     * {@link FileLock}.
     * 
     * @param file
     *            The file.
     * @param raf
     *            The {@link RandomAccessFile}.
     * 
     * @throws IOException
     */
    public static void closeFile(File file, RandomAccessFile raf)
            throws IOException {

        if (file == null)
            throw new IllegalArgumentException();
        
        if (raf == null)
            throw new IllegalArgumentException();
        
        if(!raf.getChannel().isOpen()) {

            throw new IllegalStateException();
            
        }
        
        try {

            // close the file.
            raf.close();
        
        } finally {

            removeAdvisoryLock(file);
            
        }

    }

    /**
     * Creates an advisory lock file having the same basename as the given file
     * with a <code>.lock</code> extension.
     * <p>
     * Note: This uses {@link File#createNewFile()} which is NOT advised for
     * this purpose. However, {@link FileLock} does not work in some contexts so
     * this is used as a fallback mechanism. We write a {@link UUID} into the
     * advisory lock since Java does not have platform independent PIDs. That
     * {@link UUID} allows us to tell whether the advisory lock file was created
     * by this process or by another process.
     * <p>
     * Note: If a {@link Thread} is interrupted during an NIO operation then the
     * {@link FileChannel} will be closed asynchronously. While this correctly
     * releases a {@link FileLock} it does NOT cause our advisory lock file to
     * be deleted. During a normal shutdown of an {@link AbstractJournal}, the
     * advisory lock file is deleted by
     * {@link #closeFile(File, RandomAccessFile)}. However, following an
     * abnormal shutdown the advisory lock file MAY still exist and (assuming
     * that {@link FileLock} is not working since we created an advisory lock in
     * the first place) it MUST be removed by hand before the
     * {@link AbstractJournal} can be reopened.
     * 
     * @param file
     *            The given file.
     * 
     * @return <code>true</code> if the advisory lock was created or exists
     *         and was created by this process. <code>false</code> if the
     *         advisory lock already exists and was created by another process.
     * 
     * @throws IOException
     *             If there is a problem.
     * 
     * @see #pid
     */
    synchronized public static boolean acquireAdvisoryLock(File file)
            throws IOException {

        if (INFO)
            log.info("Seeking advisory lock: " + file);
        
        final File lockFile = new File(file + ".lock");

        if(lockFile.exists()) {
            
            // check the signature in the lock file.
            return isOurLockFile(lockFile);
            
        }
        
        if(!lockFile.createNewFile()) {
            
            // someone else got there first.
            return false;
            
        }
        
        {
            
            final BufferedWriter w = new BufferedWriter(
                    new FileWriter(lockFile));

            try {

                w.write(pid);

                w.write('\n');

                w.flush();
                
            } finally {

                w.close();

            }
            
        }
        
        if (INFO)
            log.info("Created advisory lock: " + file);
        
        return true;

    }

    static public boolean isOurLockFile(File lockFile) throws IOException {

        final BufferedReader r = new BufferedReader(new FileReader(lockFile));

        try {

            final String str = r.readLine();

            if (pid.equals(str))
                return true;

            return false;

        } finally {

            r.close();
            
        }
        
    }
    
    /**
     * Removes the advisory lock for the file if it exists.
     * 
     * @param file
     *            The file whose <code>.lock</code> file will be removed.
     * 
     * @throws IOException
     *             if the lock file exists but does not belong to this process
     *             or can not be removed.
     * 
     * @see #acquireAdvisoryLock(File)
     */
    synchronized public static void removeAdvisoryLock(File file)
            throws IOException {

        final File lockFile = new File(file + ".lock");

        // no advisory lock file.
        if (!lockFile.exists()) return;
        
        if (!isOurLockFile(lockFile)) {
            
            throw new IOException("Not our lock file: " + lockFile);
            
        }

        if (!lockFile.delete()) {

            throw new IOException("Could not delete lock file: " + lockFile);

        }


    }

    /**
     * Since Java does not have platform independent PIDs we use a static
     * {@link UUID} to identify this process. This {@link UUID} gets written
     * into all advisory lock files that the process creates. Another process
     * should check the {@link UUID} in the advisory lock file and refuse to
     * open the file if the {@link UUID} is not its own {@link UUID}.
     */
    static String pid = UUID.randomUUID().toString();
    
}
