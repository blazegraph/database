package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;


/**
 * Helper object used when opening or creating journal file in any of the
 * file-based modes.
 * 
 * FIXME Write tests that verify the correct initialization of a new journal
 * file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

class FileMetadata {

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
    final long segment;
    
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
     * The actual slot size for the file (which may differ from the given slot
     * size when opening a pre-existing file).
     */
    final int slotSize;

    /**
     * The actual or computed slot limit for the file.
     */
    final int slotLimit;
    
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
    final int journalHeaderSize  = SIZE_MAGIC + SIZE_VERSION + (SIZEOF_ROOT_BLOCK * 2);
    
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
     * @param segment
     *            The unique segment identifier.
     * @param file
     *            The name of the file to be opened.
     * @param bufferMode
     *            The {@link BufferMode}.
     * @param initialExtent
     *            The initial extent of the file iff a new file is created.
     * @param slotSize
     *            The slot size iff a new file is created.
     * @param objectIndexSize
     *            The #of keys in a node of the object index (aka branching
     *            factor). This value must be even, and positive. Typically it
     *            is a power of two. E.g., 64, 128, 256.
     * @param create
     *            When true, the file is created if it does not exist.
     * @param readOnly
     *            When true, the file is opened in a read-only mode and it is an
     *            error if the file does not exist.
     * @param forceWrites
     *            When true, the file is opened in "rwd" mode and individual IOs
     *            are forced to disk. This option SHOULD be false since we only
     *            need to write through to disk on commit, not on each IO.
     * 
     * @throws IOException
     */

    FileMetadata(long segment, File file, BufferMode bufferMode,
            long initialExtent, int slotSize, int objectIndexSize,
            boolean create, boolean readOnly, ForceEnum forceWrites)
            throws IOException {

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

        this.segment = segment;
        
        this.file = file;
        
        this.bufferMode = bufferMode;

        final String fileMode = (readOnly ?"r" :forceWrites.asFileMode());

        this.readOnly = readOnly;
        
        exists = file.exists();

        if (exists) {

            System.err.println("Opening existing file: "
                    + file.getAbsoluteFile());

        } else {

            if (readOnly) {

                throw new RuntimeException("File does not exist and '"
                        + Options.READ_ONLY + "' was specified: "
                        + file.getAbsoluteFile());

            }

            if ( ! create ) {

                throw new RuntimeException("File does not exist and '"
                        + Options.CREATE + "' was not specified: "
                        + file.getAbsoluteFile());

            }

            System.err.println("Will create file: " + file.getAbsoluteFile());

        }

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

                throw new RuntimeException("Could not lock file: " + file.getAbsoluteFile());

            }

        }
        
        if (exists) {

            /*
             * The file already exists.
             */

            this.extent = raf.length();
            
            if( this.extent <= journalHeaderSize ) {

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

                AbstractBufferStrategy.assertNonDiskExtent(extent);
                
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
             * 
             * @todo figure out whether the journal is empty or not. If it is
             * then it could be simply discarded (this decision really needs to
             * be at a high level).
             * 
             * @todo make decision whether to compact and truncate the journal
             * 
             * FIXME read the slot allocation index (how is this passed along to
             * the Journal; what about the transient journal?)
             * 
             * FIXME read the object index (how is this passed along to the
             * Journal; what about the transient journal?)
             */
            
            FileChannel channel = raf.getChannel();
            ByteBuffer tmp0 = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
            ByteBuffer tmp1 = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
            channel.read(tmp0, OFFSET_ROOT_BLOCK0);
            channel.read(tmp1, OFFSET_ROOT_BLOCK1);
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
            
            this.slotSize = rootBlock.getSlotSize();
            this.slotLimit = rootBlock.getSlotLimit();
            
            switch (bufferMode) {
            case Direct:
                // Allocate a direct buffer.
                buffer = ByteBuffer.allocateDirect((int) extent);
                // Read the file image into the direct buffer.
                raf.getChannel().read(buffer, 0L);
                break;
            case Mapped:
                // Map the file.
                boolean loadMappedFile = false; // @todo expose as property.
                buffer = raf.getChannel().map(
                        FileChannel.MapMode.READ_WRITE, 0L, extent);
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
            case Disk:
                buffer = null;
                break;
            default:
                throw new AssertionError();
            }

            /*
             * @todo Review requirements for restart processing. I believe that
             * we only need to deallocate all slots that are marked as allocated
             * but not committed.
             * 
             * Other than that there should be no processing required on restart
             * since the intention of transactions that did not commit will not
             * be visible. However, that may change once we nail down the
             * multi-phase commit strategy.
             */

            throw new UnsupportedOperationException("Restart not supported");
            
        } else {

            /*
             * Create a new journal.
             */

            /*
             * Set the initial extent.
             */

            this.extent = initialExtent;

            if( bufferMode != BufferMode.Disk ) {

                /*
                 * Verify that we can address this many bytes with this
                 * strategy. The strategies that rely on an in-memory buffer
                 * are all limited to the #of bytes that can be addressed by
                 * an int32.
                 */

                AbstractBufferStrategy.assertNonDiskExtent(extent);
                
            }

            /* 
             * Extend the file.
             */
            raf.setLength(extent);

            /*
             * Write the MAGIC and version on the file.
             */
            raf.seek(0);
            raf.writeInt(MAGIC);
            raf.writeInt(VERSION1);

            /*
             * FIXME bootstrap the slot allocation and object indices. These
             * data structures need to be written on the buffer (where one is
             * used) and then flushed to disk. For at least the slot allocation
             * index we will maintain a resident data structure for instant
             * access.
             */
            
            final int slotChain = 0; // @todo bootstrap slot allocation index.
            
            final int objectIndex = 0; // @todo bootstrap object index.
            
            this.slotSize = slotSize;
            
            /*
             * The first slot index that MUST NOT be addressed.
             * 
             * Note: The same computation occurs in DiskOnlyStrategy and BasicBufferStrategy.
             */
            this.slotLimit = (int) (extent - journalHeaderSize) / slotSize;

            /*
             * Generate the root blocks. The are for all practical purposes
             * identical (in fact, their timestamps will be distict). The root
             * block are then written into their locations in the file.
             */
            final long commitCounter = 0L;
            int[] rootIds = new int[ RootBlockView.MAX_ROOT_ID ];
            IRootBlockView rootBlock0 = new RootBlockView(true, segment,
                    slotSize, slotLimit, objectIndexSize, slotChain,
                    objectIndex, commitCounter, rootIds);
            IRootBlockView rootBlock1 = new RootBlockView(false, segment,
                    slotSize, slotLimit, objectIndexSize, slotChain,
                    objectIndex, commitCounter, rootIds);
            FileChannel channel = raf.getChannel();
            channel.write(rootBlock0.asReadOnlyBuffer(), OFFSET_ROOT_BLOCK0);
            channel.write(rootBlock1.asReadOnlyBuffer(), OFFSET_ROOT_BLOCK1);
            this.rootBlock = rootBlock0;
            
            // Force the changes to disk.
            channel.force(false);

            switch (bufferMode) {
            case Direct:
                // Allocate a direct buffer.
                buffer = ByteBuffer.allocateDirect((int) extent);
                /*
                 * Read in the journal header, including the root blocks since
                 * those are not-zeroed.
                 * 
                 * FIXME It might be safer to not read the journal header into
                 * the buffer since that would mean that we could not write on
                 * it by mistake since it was not in the buffer. That could be
                 * consistent with the transient journal not having a header at
                 * all. The memory-mapped mode would have to be changed to not
                 * map the header either.
                 */
                buffer.position(0);
                buffer.limit(journalHeaderSize);
                channel.read(buffer, 0);
                break;
            case Mapped:
                // Map the file.
                buffer = raf.getChannel().map(
                        FileChannel.MapMode.READ_WRITE, 0L, extent);
                break;
            case Disk:
                buffer = null;
                break;
            default:
                throw new AssertionError();
            }

        }

    }
    
}
