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
 * 
 * @todo Consider whether a page caching layer makes any sense. The basic access
 *       patterns are sequential writes with random reads. It probably makes
 *       sense to just let the OS and disk subsystems handle buffering. However,
 *       object index and allocation index nodes SHOULD be cached for this as
 *       well as all other {@link BufferMode}s.
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

    private boolean open;

    /**
     * @param slotMath
     */
    DiskOnlyStrategy(FileMetadata fileMetadata, SlotMath slotMath) {

        super(BufferMode.Disk, slotMath);

        this.file = fileMetadata.file;

        this.raf = fileMetadata.raf;

        this.extent = fileMetadata.extent;

        /*
         * The first slot index that MUST NOT be addressed.
         * 
         * Note: The same computation occurs in BasicBufferStrategy.
         */

        this.slotLimit = (int) (extent - SIZE_JOURNAL_HEADER) / slotSize;

        System.err.println("slotLimit=" + slotLimit);

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

    // @todo vet this method against the RandomAccessFile, FileChannel, and
    // ReadableChannel APIs
    public ByteBuffer readFirstSlot(long id, int firstSlot, boolean readData,
            SlotHeader slotHeader) {

        assert slotHeader != null;

        try {

            final long pos = SIZE_JOURNAL_HEADER + slotSize * (long) firstSlot;
            raf.seek(pos);

            int nextSlot = raf.readInt();
            final int size = -raf.readInt();
            if (size <= 0) {

                // dumpSlot( firstSlot, true );
                throw new RuntimeException("Journal is corrupt: id=" + id
                        + ", firstSlot=" + firstSlot + " reports size=" + size);

            }

            // Copy out the header fields.
            slotHeader.nextSlot = nextSlot;
            slotHeader.priorSlot = size;

            if (!readData)
                return null;

            // Allocate destination buffer to size.
            ByteBuffer dst = ByteBuffer.allocate(size);

            /*
             * We copy no more than the remaining bytes and no more than the
             * data available in the slot.
             */

            final int dataSize = slotDataSize;

            int thisCopy = (size > dataSize ? dataSize : size);

            // Set limit on dst for copy.
            dst.limit(thisCopy);

            // Copy data from slot.
            raf.getChannel().read(dst,pos+slotHeaderSize);

            return dst;

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    // @todo vet this method against the RandomAccessFile, FileChannel, and ReadableChannel APIs
    public int readNextSlot(long id, int thisSlot, int priorSlot, int slotsRead, ByteBuffer dst) {

        try {

            // Position the buffer on the current slot and set limit for copy.
            final long pos = SIZE_JOURNAL_HEADER + slotSize * (long) thisSlot;
            raf.seek(pos);

            // read the header.
            final int nextSlot = raf.readInt();
            final int priorSlot2 = raf.readInt();
            if (priorSlot != priorSlot2) {

                // dumpSlot( thisSlot, true );
                throw new RuntimeException("Journal is corrupt:  id=" + id
                        + ", slotsRead=" + slotsRead + ", slot=" + thisSlot
                        + ", expected priorSlot=" + priorSlot
                        + ", actual priorSlot=" + priorSlot2);

            }

            // Copy data from slot.
            if (dst != null) {

                final int size = dst.capacity();

                final int remaining = size - dst.position();

                // #of bytes to read from this slot (header + data).
                final int thisCopy = (remaining > slotDataSize ? slotDataSize
                        : remaining);

                dst.limit(dst.position() + thisCopy);

                raf.getChannel().read(dst,pos+slotHeaderSize);

            }

            return nextSlot;

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public void writeSlot(int thisSlot, int priorSlot, int nextSlot,
            ByteBuffer data) {

        FileChannel channel = raf.getChannel();

        try {

            // Seek to the current slot.
            final long pos = SIZE_JOURNAL_HEADER + slotSize * (long) thisSlot;
            raf.seek(pos);

            // Write the slot header.
            raf.writeInt(nextSlot); // nextSlot or -1 iff last
            raf.writeInt(priorSlot); // priorSlot or -size iff first

            // Write the slot data, advances data.position().
            channel.write(data, pos + slotHeaderSize);
 
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

    }

}
