/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import com.bigdata.rawstore.Bytes;

/**
 * Helper object used when opening or creating journal file in any of the
 * file-based modes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileMetadata {

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
     * The unique segment identifier.
     */
    final int segment;
    
    /**
     * The file that was opened.
     */
    final File file;
    
    /**
     * The buffer mode used to open that file.
     */
    final BufferMode bufferMode;
    
    /**
     * The interface for IO performed on that file.
     */
    final RandomAccessFile raf;
    
    /**
     * The extent of the file in bytes.
     */
    final long extent;
    
    /**
     * The extent of the user data space (everything after the root blocks).
     */
    final long userExtent;

    /**
     * The next offset at which a record would be written on the store.  The
     * offset is relative to the start of the user data space.  Offset zero(0)
     * addresses the first byte after the root blocks.
     */
    final int nextOffset;
    
    /**
     * True iff the file was opened in a read-only mode.
     */
    final boolean readOnly;

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
     * blocks. This is as an offset when computing the index of a slot on the
     * journal.
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
     * The current root block. For a new file, this is "rootBlock0". For an
     * existing file it is based on an examination of both root blocks.
     */
    final IRootBlockView rootBlock;
    
    /**
     * Prepare a journal file for use by an {@link IBufferStrategy}.
     * 
     * @param segmentId
     *            The unique segment identifier.
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
     * 
     * @throws RuntimeException
     *             if there is a problem preparing the file for use by the
     *             journal.
     */
    FileMetadata(int segmentId, File file, BufferMode bufferMode,
            boolean useDirectBuffers, long initialExtent, long maximumExtent,
            boolean create, boolean isEmptyFile, boolean deleteOnExit,
            boolean readOnly, ForceEnum forceWrites) throws RuntimeException {

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

        this.segment = segmentId;
        
        this.bufferMode = bufferMode;

        final String fileMode = (readOnly ?"r" :forceWrites.asFileMode());

        this.readOnly = readOnly;
        
        this.exists = !isEmptyFile && file.exists();
        
        this.file = file;
        
        if (exists) {

            System.err.println("Opening existing file: "
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

            System.err.println("Will create file: " + file.getAbsoluteFile());

        }
        
        try {
            
            /*
             * Open/create the file.
             */
            this.raf = new RandomAccessFile(file, fileMode);
    
            if (bufferMode != BufferMode.Mapped) {
    
                /*
                 * Obtain exclusive lock on the file. This is a non-blocking
                 * request. The lock is released automatically when the channel
                 * is closed.
                 * 
                 * Note: Do not attempt to gain a lock on the file if you are
                 * going to use a memory-mapped buffer.  The JDK cautions that
                 * these things do not play well together on some platforms.
                 */
    
                FileLock fileLock = this.raf.getChannel().tryLock();
    
                if (fileLock == null) {
    
                    /*
                     * We were not able to get a lock on the file.
                     */
    
                    throw new RuntimeException("Could not lock file: "
                            + file.getAbsoluteFile());
    
                }
    
            }
            
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
                 * Read the MAGIC and VERSION.
                 */
                raf.seek(0L);
                final int magic = raf.readInt();
                if (magic != MAGIC)
                    throw new RuntimeException("Bad journal magic: expected="
                            + MAGIC + ", actual=" + magic);
                final int version = raf.readInt();
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
                IRootBlockView rootBlock0 = null;
                IRootBlockView rootBlock1 = null;
                try {
                    rootBlock0 = new RootBlockView(true,tmp0);
                } catch(RootBlockException ex ) {
                    System.err.println("Bad root block zero: "+ex);
                }
                try {
                    rootBlock1 = new RootBlockView(false,tmp1);
                } catch(RootBlockException ex ) {
                    System.err.println("Bad root block one: "+ex);
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
                
                switch (bufferMode) {
                case Direct: {
                    // Allocate the buffer buffer.
                    buffer = (useDirectBuffers ? ByteBuffer
                            .allocateDirect((int) userExtent) : ByteBuffer
                            .allocate((int) userExtent));
                    // Setup to read data from file into the buffer.
                    buffer.limit(nextOffset);
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
                raf.writeInt(MAGIC);
                raf.writeInt(VERSION1);
    
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
                final long commitTimestamp = 0L;
                final long firstTxId = 0L;
                final long lastTxId = 0L;
                final long commitRecordAddr = 0L;
                final long commitRecordIndexAddr = 0L;
                IRootBlockView rootBlock0 = new RootBlockView(true, segmentId,
                        nextOffset, firstTxId, lastTxId, commitTimestamp,
                        commitCounter, commitRecordAddr, commitRecordIndexAddr);
                IRootBlockView rootBlock1 = new RootBlockView(false, segmentId,
                        nextOffset, firstTxId, lastTxId, commitTimestamp,
                        commitCounter, commitRecordAddr, commitRecordIndexAddr);
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
    
}
