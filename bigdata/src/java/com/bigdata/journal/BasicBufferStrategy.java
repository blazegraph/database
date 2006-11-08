package com.bigdata.journal;

import java.nio.ByteBuffer;


/**
 * Implements logic to read from and write on a buffer. This is sufficient
 * for a {@link BufferMode#Transient} implementation or a
 * {@link BufferMode#Mapped} implementation, but the
 * {@link BufferMode#Direct} implementation needs to also implement write
 * through to the disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class BasicBufferStrategy extends AbstractBufferStrategy {

    /**
     * A direct buffer containing a write through image of the backing file.
     */
    final ByteBuffer directBuffer;

    /**
     * The current length of the backing file in bytes.
     */
    final long extent;

    public long getExtent() {

        return extent;

    }

    /**
     * The index of the first slot that MUST NOT be addressed (e.g., nslots).
     */
    final int slotLimit;

    public int getSlotLimit() {
        return slotLimit;
    }

    /**
     * Asserts that the slot index is in the legal range for the journal
     * <code>[1:slotLimit-1]</code>
     * 
     * @param slot
     *            The slot index.
     */

    void assertSlot(int slot) {

        if (slot >= 1 && slot < slotLimit)
            return;

        throw new AssertionError("slot=" + slot + " is not in [1:" + slotLimit
                + ")");

    }

    BasicBufferStrategy(int journalHeaderSize, BufferMode bufferMode,
            SlotMath slotMath, ByteBuffer buffer) {

        super(journalHeaderSize, bufferMode, slotMath);

        this.directBuffer = buffer;

        this.extent = buffer.capacity();

        /*
         * The first slot index that MUST NOT be addressed.
         * 
         * Note: The same computation occurs in DiskOnlyStrategy and
         * FileMetadata.
         */

        this.slotLimit = (int) (extent - journalHeaderSize) / slotSize;

        System.err.println("slotLimit=" + slotLimit);

    }

    /**
     * Return a read-only slice of the direct buffer.
     * 
     * @param slots
     *            The slot allocation (MUST be contiguous).
     * 
     * @return The slice.
     */
    public ByteBuffer getSlice(ISlotAllocation slots) {

        assert slots != null;
        
        assert slots.isContiguous();

        int firstSlot = slots.firstSlot();

        int nbytes = slots.getByteCount();

        final int pos = journalHeaderSize + slotSize * firstSlot;
        directBuffer.limit(pos + nbytes);
        directBuffer.position(pos);

        return directBuffer.slice().asReadOnlyBuffer();

    }
    
    /**
     * Write the data onto a mutable slice of the buffer corresponding to the
     * specified allocation.
     * 
     * @param slots
     *            The slot allocation (MUST be contiguous).
     * 
     * @param data
     *            The data (required, MUST be no larger than the capacity of the
     *            slots).
     */
    public void writeSlice(ISlotAllocation slots, ByteBuffer data ) {

        assert slots != null;
        
        assert slots.isContiguous();

        int firstSlot = slots.firstSlot();

        int nbytes = slots.getByteCount();

        final int pos = journalHeaderSize + slotSize * firstSlot;
        directBuffer.limit(pos + nbytes);
        directBuffer.position(pos);

        ByteBuffer slice = directBuffer.slice();
        
        slice.put(data);

    }

    public void writeSlot(int slot, ByteBuffer data) {

        assertSlot(slot);
        assert data != null;

        // Position the buffer on the current slot.
        final int pos = journalHeaderSize + slotSize * slot;
        directBuffer.limit(pos + slotSize);
        directBuffer.position(pos);

        // Write the slot data, advances data.position().
        directBuffer.put(data);

    }

    public ByteBuffer readSlot(int slot, ByteBuffer dst) {

        assertSlot(slot);
        assert dst != null;

        final int remaining = dst.remaining();

        assert remaining <= slotSize;

        /*
         * Setup the source (limit and position).
         */
        final int pos = journalHeaderSize + slotSize * slot;
        directBuffer.limit(pos + remaining);
        directBuffer.position(pos);

        /*
         * Copy data from slot.
         */
        // dst.limit(dst.position() + thisCopy);
        dst.put(directBuffer);

        return dst;

    }
    
}
