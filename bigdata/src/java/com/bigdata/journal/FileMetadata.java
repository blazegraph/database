package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Helper object used when opening or creating an {@link DirectBufferStrategy}.
 * This takes care of the basics for creating or opening the file, preparing
 * the buffer image, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

class FileMetadata {

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
     * Depending on the mode, this will be either a direct buffer, a mapped
     * buffer or [null] if no buffer is being used.
     */
    final ByteBuffer buffer;

    /**
     * True iff the file was pre-existing.
     */
    final boolean exists;

    FileMetadata(File file, BufferMode bufferMode, long initialExtent)
            throws IOException {

        if (file == null)
            throw new IllegalArgumentException();

        if (bufferMode == null)
            throw new IllegalArgumentException();

        if( bufferMode == BufferMode.Transient ) {
            
            // This mode is not a valid option in this context.
            
            throw new IllegalArgumentException();
            
        }
        
        this.file = file;
        
        this.bufferMode = bufferMode;

        /*
         * Note: We do not choose the options for writing synchronously to
         * the underlying storage device since we only need to write through
         * to disk on commit, not on incremental write.
         */

        final String fileMode = "rw";

        exists = file.exists();

        if (exists) {

            System.err.println("Opening existing file: " + file);

        } else {

            System.err.println("Will create file: " + file);

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

                throw new RuntimeException("Could not lock file: " + file);

            }

        }
        
        if (exists) {

            /*
             * The file already exists.
             */

            this.extent = raf.length();

            if( bufferMode != BufferMode.Disk ) {

                /*
                 * Verify that we can address this many bytes with this
                 * strategy. The strategies that rely on an in-memory buffer
                 * are all limited to the #of bytes that can be addressed by
                 * an int32.
                 */

                AbstractBufferStrategy.assertNonDiskExtent(extent);
                
            }

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
             * FIXME Check root blocks (magic, timestamps), choose root
             * block, figure out whether the journal is empty or not, read
             * constants (slotSize, segmentId), make decision whether to
             * compact and truncate the journal, read root nodes of indices,
             * etc.
             * 
             * @todo Check the magic and other things that can be used to
             * quickly detect a corrupt file or a file that is not a journal
             * before reading the image from the disk. This can be done with
             * a little helper method.
             * 
             * @todo Review requirements for restart processing. Off hand,
             * there should be no processing required on restart since the
             * intention of transactions that did not commit will not be
             * visible. However, that may change once we nail down the
             * multi-phase commit strategy.
             */

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

            switch (bufferMode) {
            case Direct:
                // Allocate a direct buffer.
                buffer = ByteBuffer.allocateDirect((int) extent);
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

            /*
             * FIXME Format the journal, e.g., write the root blocks and the
             * root index and allocation nodes.
             */

        }

    }
    
}