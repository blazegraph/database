package com.bigdata.journal;

import java.nio.ByteBuffer;

import com.bigdata.journal.Journal.SlotHeader;

/**
 * <p>
 * Interface for implementations of a buffer strategy as identified by a
 * {@link BufferMode}. This interface is designed to encapsulate the
 * specifics of reading and writing slots and performing operations to make
 * an atomic commit.
 * </p>
 * <p>
 * Note: Implementations of this interface SHOULD NOT provide thread-safety.
 * This interface is always used within a single-threaded context.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBufferStrategy {
    
    /**
     * The buffer mode supported by the implementation
     * 
     * @return The implemented buffer mode.
     */
    public BufferMode getBufferMode();

    /**
     * True iff the journal is open.
     */
    public boolean isOpen();
    
    /**
     * The index of the first slot that MUST NOT be addressed (e.g., nslots).
     */
    public int getSlotLimit();
    
    /**
     * The current size of the journal in bytes.
     */
    public long getExtent();

    /**
     * Either truncates or extends the journal.  The caller MUST insure that
     * the journal is compact up to the specified extent when used to truncate
     * a journal.  {@link BufferMode}s that use an in-memory buffer MAY NOT
     * be increased beyond {@link Integer#MAX_VALUE} bytes in extent.
     * 
     * @param extent
     */
    public void truncate(long extent);
    
    /**
     * Close the journal.
     * 
     * @throws IllegalStateException if the journal is not open.
     */
    public void close();
    
    /**
     * Force the data to stable storage. This method MUST be invoked during
     * {@link #close()} and MAY be invoked during commit processing.
     * 
     * @param metadata
     *            If true, then force both the file contents and the file
     *            metadata to disk.
     */
    public void force(boolean metadata);
    
    /**
     * Read the first slot for some data version.
     * 
     * @param id
     *            The persistent identifier.
     * @param firstSlot
     *            The first slot for that data version.
     * @param readData
     *            When true, a buffer will be allocated sized to exactly hold
     *            the data version and the data from the first slot will be read
     *            into that buffer. The buffer is returned to the caller.
     * @param slotHeader
     *            The structure into which the header data will be copied.
     * 
     * @return A newly allocated buffer containing the data from the first slot
     *         or <code>null</code> iff <i>readData</i> is false. The
     *         {@link ByteBuffer#position()} will be the #of bytes read from the
     *         slot. The limit will be equal to the position. Data from
     *         remaining slots for this data version should be appended starting
     *         at the current position. You must examine
     *         {@link SlotHeader#nextSlot} to determine if more slots should be
     *         read.
     * 
     * @exception RuntimeException
     *                if the slot is corrupt.
     */

    public ByteBuffer readFirstSlot(long id, int firstSlot, boolean readData,
            SlotHeader slotHeader);
    
    /**
     * Read another slot in a chain of slots for some data version.
     * 
     * @param id
     *            The persistent identifier.
     * @param thisSlot
     *            The slot being read.
     * @param priorSlot
     *            The previous slot read.
     * @param slotsRead
     *            The #of slots read so far in the chain for the data version.
     * @param dst
     *            When non-null, the data from the slot is appended into this
     *            buffer starting at the current position.
     * @return The next slot to be read or {@link #LAST_SLOT_MARKER} iff this
     *         was the last slot in the chain.
     */
    public int readNextSlot(long id,int thisSlot,int priorSlot,int slotsRead,ByteBuffer dst );

    /**
     * Write a slot.
     * 
     * @param thisSlot
     *            The slot index.
     * @param priorSlot
     *            The value to be written into the priorSlot header field.
     * @param nextSlot
     *            The value to be written into the nextSlot header field.
     * @param data
     *            The data to be written on the slot.
     */
    public void writeSlot(int thisSlot,int priorSlot,int nextSlot, ByteBuffer data);

}