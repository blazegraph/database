package com.bigdata.journal;

import com.bigdata.rawstore.Addr;


/**
 * Abstract base class for {@link IBufferStrategy} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractBufferStrategy implements IBufferStrategy {
    
    /**
     * The buffer strategy implemented by this class.
     */
    protected final BufferMode bufferMode;
    
    /**
     * The next offset at which a data item would be written on the store. This
     * is updated each time a new record is written on the store. On restart,
     * the value is initialized from the current root block. The current value
     * is written as part of the new root block during each commit.
     * <p>
     * Note: It is NOT safe to reload the current root block and therefore reset
     * this to an earlier offset unless all transactions are discarded. The
     * reason is that transactions may use objects (btrees) to provide
     * isolation. Those objects write on the store but do not register as
     * {@link ICommitter}s and therefore never make themselves restart safe.
     * However, you can not discard the writes of those objects unless the
     * entire store is being restarted, e.g., after a shutdown or a crash.
     */
    protected int nextOffset;

    final public BufferMode getBufferMode() {

        return bufferMode;
        
    }

    final public int getNextOffset() {

        return nextOffset;
        
    }
    
    /**
     * (Re-)open a buffer.
     * 
     * @param nextOffset
     *            The next offset within the buffer on which a record will be
     *            written. Note that the buffer begins _after_ the root blocks
     *            and offset zero is always the first byte in the buffer.
     * @param bufferMode
     *            The {@link BufferMode}.
     */
    AbstractBufferStrategy(int nextOffset, BufferMode bufferMode) {

        assert nextOffset >= 0;
        
        if( bufferMode == null ) throw new IllegalArgumentException();
        
        this.nextOffset = nextOffset;
        
        this.bufferMode = bufferMode;
        
    }

    /**
     * Throws an exception if the extent is too large for an in-memory buffer.
     * 
     * @param extent
     *            The extent.
     * 
     * @return The extent.
     * 
     * @deprecated All buffer modes are now limited to int32 offsets since the
     *             offset is encoded in the high word of an {@link Addr}. It is
     *             possible that a disk-only mode could address a longer extent
     *             simply because the offset + nbytes could run beyond the int32
     *             boundary, but that is not worth it.
     */
    static long assertNonDiskExtent(long extent) {

        if( extent > Integer.MAX_VALUE ) {
            
            /*
             * The file image is too large to address with an int32. This
             * rules out both the use of a direct buffer image and the use
             * of a memory-mapped file. Therefore, the journal must use a
             * disk-based strategy.
             */
           
            throw new RuntimeException(
                    "The extent requires the 'disk' mode: extent=" + extent);
            
        }

        return extent;
        
    }

}
