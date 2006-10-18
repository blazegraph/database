package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.bigdata.journal.Journal.SlotHeader;

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

    /**
     * <p>
     * A buffer used to read/write a slot at a time. On read, the slot is read
     * from disk into this buffer and then the buffer is unpacked. On write, the
     * header and data for a slot are first written into this buffer and then
     * the buffer is written in a single operation to disk. This minimizes the
     * #of IOs that actually read/write through to disk to one per slot.
     * </p>
     */
    final private ByteBuffer buf;
    
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
        
        this.buf = ByteBuffer.allocateDirect(slotSize);
        
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

    public long getExtent() {

        return extent;

    }

    public int getSlotLimit() {

        return slotLimit;

    }

    /**
     * Note: This always reads the full slot into a temporary buffer since we
     * (a) do not know a priori how much data is in the slot; and (b) we do not
     * want to perform more than one read from the disk.
     */
    public ByteBuffer readFirstSlot(int firstSlot, boolean readData,
            SlotHeader slotHeader, ByteBuffer dst ) {

        assert slotHeader != null;

        try {

            /*
             * Read the slot from disk into a temporary buffer.
             */

            // Position the buffer on the current slot.
            final long pos = journalHeaderSize + slotSize * (long) firstSlot;

            buf.clear();
            
            // Read the data from the disk.
            channel.read(buf,pos);

            // Flip to read the data from the temporary buffer.
            buf.flip();

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        /*
         * Unpack the slot header.
         */
        final int nextSlot = buf.getInt();
        final int size = -buf.getInt();
        if (size <= 0) {
            
            // dumpSlot( firstSlot, true );
            throw new RuntimeException("Journal is corrupt"
                    + ": firstSlot=" + firstSlot + " reports size=" + size);

        }

        // Copy out the header fields.
        slotHeader.nextSlot = nextSlot;
        slotHeader.priorSlot = -size;

        if (!readData) return null;

        /*
         * Verify that the destination buffer exists and has sufficient
         * remaining capacity.
         */
        if (dst == null || dst.remaining() < size) {

            // Allocate a destination buffer to size.
            dst = ByteBuffer.allocate(size);

        }

        /*
         * We copy no more than the remaining bytes and no more than the data
         * available in the slot.
         */

        final int thisCopy = (size > slotDataSize ? slotDataSize : size);

        // Set limit on source for copy.
        buf.limit(slotHeaderSize + thisCopy);

        // Copy data from slot.
        dst.limit( dst.position() + thisCopy );
        dst.put(buf);

        return dst;

    }

    public int readNextSlot(int thisSlot, int priorSlot, int slotsRead,
            int remainingToRead, ByteBuffer dst) {

        // #of bytes to read from t
        final int thisCopy;
        
        if (dst != null) {

//            final int size = dst.capacity();
//
//            final int remaining = size - dst.position();

            assert remainingToRead > 0;
            
            // #of bytes to read from this slot (header + data).
            thisCopy = slotHeaderSize
                    + (remainingToRead > slotDataSize ? slotDataSize
                            : remainingToRead);

        } else {

            thisCopy = slotHeaderSize;

        }

        try {

            /*
             * Read the slot from disk into a temporary buffer.
             */

            // Position the buffer on the current slot.
            final long pos = journalHeaderSize + slotSize * (long) thisSlot;

            buf.clear();

            // #of bytes to read from the disk.
            buf.limit( thisCopy );
            
            // Read the data from the disk.
            channel.read(buf,pos);

            // Flip to read the data from the temporary buffer.
            buf.flip();

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        // read the header, advancing buf.position().
        final int nextSlot = buf.getInt();
        final int priorSlot2 = buf.getInt();
        if (priorSlot != priorSlot2) {

            // dumpSlot( thisSlot, true );
            throw new RuntimeException("Journal is corrupt" + ": slotsRead="
                    + slotsRead + ", slot=" + thisSlot
                    + ", expected priorSlot=" + priorSlot
                    + ", actual priorSlot=" + priorSlot2);

        }

        // Copy data from slot.
        if (dst != null) {

            dst.limit(dst.position() + thisCopy - slotHeaderSize);

            dst.put( buf );

        }

        return nextSlot;

    }

    public void writeSlot(int thisSlot, int priorSlot, int nextSlot,
            ByteBuffer data) {

        // #of bytes of data to be written (does not include header).
        final int remaining = data.remaining();

        /*
         * Write the header and data onto an intermediate buffer.
         */

        // Reset the buffer.
        buf.clear();
        
        // Write the slot header.
        buf.putInt(nextSlot); // nextSlot or -1 iff last
        buf.putInt(priorSlot); // priorSlot or -size iff first

        // Write the slot data, advances data.position().
        buf.put(data);
        
        // Flip before writing onto the disk.
        buf.flip();

        /*
         * Write the intermedite buffer onto the disk.
         */

        try {

            // Seek to the current slot.
            final long pos = journalHeaderSize + slotSize * (long) thisSlot;
            
            final int nwritten = channel.write(buf, pos);
            
            assert nwritten == remaining + slotHeaderSize;
            
//            raf.seek(pos);

//            // Write the slot header.
//            raf.writeInt(nextSlot); // nextSlot or -1 iff last
//            raf.writeInt(priorSlot); // priorSlot or -size iff first
//
//            // Write the slot data, advances data.position().
//            final int nwritten = channel.write(data, pos + slotHeaderSize);
//            
//            assert nwritten == remaining;
 
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

    }

}
