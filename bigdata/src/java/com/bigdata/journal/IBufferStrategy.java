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
     * 
     * FIXME This must be a long integer for the {@link BufferMode#Disk}
     * strategy. The other strategies all use an int limit. That means that we
     * need to hide the actual field a little more, but we also need to change
     * the APIs to pass a long slot value everywhere.
     * 
     * The problem with changing to a long integer is that the priorSlot and
     * nextSlot fields in the slot header then need to be changed from int32 to
     * int64 fields. That will have a significiant impact on the journal size.
     * 
     * My preference is to have two {@link SlotMath} implementations, one of
     * which supports int32 slot indices and the other of which supports int64
     * slot indices. The APIs could then use int64 (long) throughout, and it
     * will be downcast to int32 (int) when using a disk-based journal.
     * 
     * However, this introduces another complication - the disk-based journal
     * would have to have a different binary format when the intention was to
     * address more than {@link Integer#MAX_VALUE} slots. Since that is
     * definately NOT the sweet spot for the journal or bigdata, the "right"
     * thing may be to keep the slot index as an int32. Hence, I am not changing
     * anything right now. The {@link BufferMode#Disk} SHOULD still be able to
     * address files with more than {@link Integer#MAX_VALUE} bytes, just not
     * files with more than {@link Integer#MAX_VALUE} slots.
     * 
     * @todo Add test cases that verify correct fence post semantics for
     *       {@link BufferMode#Disk} as outlined above.
     */
    public int getSlotLimit();
    
    /**
     * The #of bytes of data that fit in each slot.
     */
    public int getSlotDataSize();
    
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

    public ByteBuffer readFirstSlot(int firstSlot, boolean readData,
            SlotHeader slotHeader);
    
    /**
     * Read another slot in a chain of slots for some data version.
     * 
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
    public int readNextSlot(int thisSlot,int priorSlot,int slotsRead,ByteBuffer dst );

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
     *            The data to be written on the slot. Bytes are written from the
     *            current position up to the limit (exclusive). The position is
     *            updated as a side effect. The post-condition is that the
     *            position is equal to the pre-condition limit.
     */
    public void writeSlot(int thisSlot,int priorSlot,int nextSlot, ByteBuffer data);

}
