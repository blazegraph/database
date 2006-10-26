package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/**
 * Disk-based journal strategy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Disk
 */
public class DiskOnlyStrategy extends AbstractBufferStrategy {

    /**
     * The file.
     */
    final File file;

    /**
     * The IO interface for the file.
     */
    final RandomAccessFile raf;

    /**
     * Extent of the file. This value should be valid since we obtain an
     * exclusive lock on the file when we open it.
     */
    final long extent;

    /**
     * The index of the first slot that MUST NOT be addressed (e.g., nslots).
     */
    final int slotLimit;

    /**
     * The backing channel.
     */
    final FileChannel channel;

    private boolean open;

    /**
     * @param slotMath
     */
    DiskOnlyStrategy(FileMetadata fileMetadata, SlotMath slotMath) {

        super(fileMetadata.journalHeaderSize,BufferMode.Disk, slotMath);

        this.file = fileMetadata.file;

        this.raf = fileMetadata.raf;

        this.extent = fileMetadata.extent;

        /*
         * The first slot index that MUST NOT be addressed.
         * 
         * Note: The same computation occurs in BasicBufferStrategy and FileMetadata.
         */

        this.slotLimit = (int) (extent - journalHeaderSize) / slotSize;

        System.err.println("slotLimit=" + slotLimit);

        this.channel = raf.getChannel();
        
        open = true;

    }

    public boolean isOpen() {

        return open;

    }

    /**
     * Forces the data to disk.
     */
    public void force(boolean metadata) {

        try {

            raf.getChannel().force(metadata);

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Closes the file.
     */
    public void close() {

        if (!isOpen())
            throw new IllegalStateException();

        try {

            force(false);

            raf.close();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        open = false;

    }

    public void deleteFile() {
        
        if( open ) throw new IllegalStateException();
        
        if( ! file.delete() ) {
            
            throw new RuntimeException("Could not delete file: "
                    + file.getAbsoluteFile());
            
        }
        
    }
    
    public long getExtent() {

        return extent;

    }

    public int getSlotLimit() {

        return slotLimit;

    }

    /**
     * Read the slot from disk.
     * 
     * @todo modify to use a page cache.
     */    
    public ByteBuffer readSlot(int slot, ByteBuffer dst ) {

        // #of bytes to copy.
        final int remaining = dst.remaining();
        
        assert remaining <= slotSize;
        
        try {

            // Position the buffer on the current slot.
            final long pos = journalHeaderSize + slotSize * (long) slot;

            // Read the data from the disk.
            final int nbytes = channel.read(dst,pos);
            
            assert nbytes == remaining;

            return dst;
            
        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Write the slot on disk.
     * 
     * @todo Modify to use a page cache.
     */
    public void writeSlot(int slot, ByteBuffer data) {

        // #of bytes of data to be written.
        final int remaining = data.remaining();

        // may not exceed one slot.
        assert remaining <= slotSize;
        
        /*
         * Write onto the disk.
         */

        try {

            // Seek to the current slot.
            final long pos = journalHeaderSize + slotSize * (long) slot;
            
            final int nwritten = channel.write(data, pos);
            
            assert nwritten == remaining;
             
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

    }

    public void writeRootBlock(IRootBlockView rootBlock) {

        if( rootBlock == null ) throw new IllegalArgumentException();
        
        try {

            FileChannel channel = raf.getChannel();

            channel.write(rootBlock.asReadOnlyBuffer(), rootBlock
                    .isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
                    : FileMetadata.OFFSET_ROOT_BLOCK1);

            force(false);

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

}
