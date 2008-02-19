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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;
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
    public static final Logger log = Logger.getLogger(FileMetadata.class);

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
     *            When true, the file is created if it does not exist.
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
     * @param createTime
     *            The create time to be assigned to the root block iff a new
     *            file is created.
     * @param validateChecksum
     *            When true, the checksum stored in the root blocks of an
     *            existing file will be validated when the file is opened. See
     *            {@link Options}
     * @param checker
     *            The object used to compute the checksum of the root blocks.
     * @throws RuntimeException
     *             if there is a problem preparing the file for use by the
     *             journal.
     */
    FileMetadata(File file, BufferMode bufferMode, boolean useDirectBuffers,
            long initialExtent, long maximumExtent, boolean create,
            boolean isEmptyFile, boolean deleteOnExit, boolean readOnly,
            ForceEnum forceWrites, int offsetBits, boolean validateChecksum,
            long createTime, ChecksumUtility checker) throws RuntimeException {

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
        
        this.fileMode = (readOnly ?"r" :forceWrites.asFileMode());

        this.readOnly = readOnly;
        
        this.exists = !isEmptyFile && file.exists();
        
        this.file = file;
        
        if (exists) {

            log.info("Opening existing file: "
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

            log.info("Will create file: " + file.getAbsoluteFile());

        }
        
        try {
            
            /*
             * Open/create the file.
             */
            this.raf = openFile(file, fileMode, bufferMode); 
                
            if (exists) {
    
                /*
                 * The file already exists.
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
                int nread;
                if ((nread = channel.read(tmp0, OFFSET_ROOT_BLOCK0)) != RootBlockView.SIZEOF_ROOT_BLOCK) {
                    throw new IOException("Expected to read "
                            + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but read "
                            + nread + " bytes");
                }
                if ((nread = channel.read(tmp1, OFFSET_ROOT_BLOCK1)) != RootBlockView.SIZEOF_ROOT_BLOCK) {
                    throw new IOException("Expected to read "
                            + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but read "
                            + nread + " bytes");
                }
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
                    throw new RuntimeException("Both root blocks are bad - journal is not usable.");
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
                    final int nbytes = raf.getChannel().read(buffer,
                            headerSize0);
                    if (nbytes != nextOffset) {
                        throw new IOException("Expected to read " + nextOffset
                                + " bytes, but read " + nbytes + " bytes");
                    }
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
    
                // Mark the file for deletion on exit.
                if(deleteOnExit) file.deleteOnExit();
                
                /*
                 * Set the initial extent.
                 * 
                 * Note: since a mapped file CAN NOT be extended, we pre-extend
                 * it to its maximum extent here.
                 */
    
                this.extent = (bufferMode == BufferMode.Mapped ? maximumExtent
                        : initialExtent);
    
                this.userExtent = extent - headerSize0;
                
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
                 * Extend the file.  We do this eagerly in an attempt to convince
                 * the OS to place the data into a contiguous region on the disk.
                 */
                raf.setLength(extent);
    
                /*
                 * Write the MAGIC and version on the file.
                 */
                raf.seek(0);
                raf.writeInt(magic = MAGIC);
                raf.writeInt(version = VERSION1);
    
                /*
                 * Generate the root blocks. They are for all practical purposes
                 * identical (in fact, their timestamps will be distict). The root
                 * block are then written into their locations in the file.
                 */
                
                /*
                 * The offset at which the first record will be written. This is
                 * zero(0) since the buffer offset (0) is the first byte after the
                 * root blocks.
                 */
                nextOffset = 0;
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
                IRootBlockView rootBlock0 = new RootBlockView(true, offsetBits,
                        nextOffset, firstCommitTime, lastCommitTime,
                        commitCounter, commitRecordAddr, commitRecordIndexAddr,
                        uuid, createTime, closeTime, checker);
                IRootBlockView rootBlock1 = new RootBlockView(false,
                        offsetBits, nextOffset, firstCommitTime,
                        lastCommitTime, commitCounter, commitRecordAddr,
                        commitRecordIndexAddr, uuid, createTime, closeTime,
                        checker);
                FileChannel channel = raf.getChannel();
                channel.write(rootBlock0.asReadOnlyBuffer(), OFFSET_ROOT_BLOCK0);
                channel.write(rootBlock1.asReadOnlyBuffer(), OFFSET_ROOT_BLOCK1);
                this.rootBlock = rootBlock0;
                
                /*
                 * Force the changes to disk. We also force the file metadata to
                 * disk since we just changed the file size and we do not want to
                 * loose track of that.
                 */
                channel.force(true);
    
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
                    log.info("Mapping file="+file);
                    buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE,
                            headerSize0, userExtent);
                    break;
                case Disk:
                    buffer = null;
                    break;
                default:
                    throw new AssertionError();
                }
    
            }

        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
    /**
     * Create/open the file and obtain an exclusive lock where warranted and
     * supported.
     * 
     * @param file
     *            The file.
     * @param fileMode
     *            The file mode for
     *            {@link RandomAccessFile#RandomAccessFile(File, String)}
     * @param bufferMode
     *            The {@link BufferMode} (mapped files do not support exclusive
     *            locks).
     * 
     * @return The {@link RandomAccessFile}
     * 
     * @throws IOException
     *             If the file could not be opened or the lock could not be
     *             obtained.
     */
    public static RandomAccessFile openFile(File file, String fileMode, BufferMode bufferMode) throws IOException {
        
        final boolean readOnly = "r".equals(fileMode);
        
        RandomAccessFile raf = new RandomAccessFile(file, fileMode);
        
        if (!readOnly && bufferMode != BufferMode.Mapped) {

            /*
             * Obtain exclusive lock on the file. This is a non-blocking
             * request. The lock is released automatically when the channel
             * is closed.
             * 
             * Note: Do not attempt to gain a lock on the file if you are
             * going to use a memory-mapped buffer. The JDK cautions that
             * these things do not play well together on some platforms.
             * 
             * Note: tryLock() will fail if the file channel was opened in a
             * read-only mode, so we do not attempt to lock files that are
             * not being written.
             * 
             * @todo we should write a semaphore lock file to avoid
             * concurrent processes operating on the same journal, including
             * the case of a read-only process and a read-write process. The
             * JDK tryLock() mechanism is only suited to control access to a
             * file from within the same JVM, not across processes. Further,
             * tryLock() can not be used when the file channel is read-only.
             */

            final FileLock fileLock = raf.getChannel().tryLock();

            if (fileLock == null) {

                try {

                    raf.close();
                    
                } catch (Throwable t) {
                    
                    log.warn(t);
                    
                }

                /*
                 * We were not able to get a lock on the file.
                 */

                throw new RuntimeException("Could not lock file: "
                        + file.getAbsoluteFile());

            }

        }

        return raf;

    }

}
