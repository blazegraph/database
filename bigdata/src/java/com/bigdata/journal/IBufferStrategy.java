package com.bigdata.journal;

import java.nio.ByteBuffer;


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
     * Delete the backing file for the journal if any.
     * 
     * @exception IllegalStateException
     *                if the journal is not closed.
     */
    public void deleteFile();

    /**
     * Close the journal. If the journal is backed by disk, then the data are
     * forced to disk first.
     * 
     * @throws IllegalStateException
     *             if the journal is not open.
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
     * Read data from a slot, appending the data into the provided buffer.
     * Invoking this method for each slot in a {@link ISlotAllocation} in turn
     * will cause the all data for that allocation to be assembled in the buffer
     * in order.
     * 
     * @param slot
     *            The slot index.
     * @param dst
     *            The data version will be read into this buffer beginning with
     *            the current position up to the last byte of data in the slot
     *            or the limit (exclusive) on the buffer, whichever comes first.
     * 
     * @return The buffer. The {@link ByteBuffer#position()} will be advanced by
     *         the #of bytes read from the slot.
     */
    public ByteBuffer readSlot(int slot, ByteBuffer dst);
    
    /**
     * Write data on a slot.
     * 
     * @param slot
     *            The slot index.
     * @param data
     *            The data to be written on the slot. Bytes are written from the
     *            current position up to the limit (exclusive). The position is
     *            updated as a side effect. The post-condition is that the
     *            position is equal to the pre-condition limit.
     * 
     * @exception IllegalArgumentException
     *                if <i>slot</i> is invalid.
     * @exception IllegalArgumentException
     *                if data is null.
     * @exception IllegalArgumentException
     *                if the #of bytes remaining in data exceeds the size of a
     *                single slot.
     */
    public void writeSlot(int slot, ByteBuffer data);

    /**
     * Write the root block onto stable storage (ie, flush it through to disk).
     * 
     * @param rootBlock
     *            The root block. Which root block is indicated by
     *            {@link IRootBlockView#isRootBlock0()}.
     * 
     * @todo It is up in the air whether the root blocks and file header need to
     *       appear in the buffer. I rather think not. The buffer can simply
     *       begin after the root blocks. That approach makes accidental
     *       overwrite less likely, but it also means that the transient mode
     *       does not have the notion of root blocks since it has nothing to
     *       write through to.
     */
    public void writeRootBlock(IRootBlockView rootBlock);
    
}
