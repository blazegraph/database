package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Direct buffer strategy uses a direct {@link ByteBuffer} as a write through
 * cache and writes through to disk for persistence.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Direct
 */

public class DirectBufferStrategy extends DiskBackedBufferStrategy {

    DirectBufferStrategy(FileMetadata fileMetadata, SlotMath slotMath) {

        super(BufferMode.Direct,fileMetadata,slotMath);

    }

    /**
     * Extends the basic behavior to write through to the backing file.
     */
    public void writeSlice(ISlotAllocation slots,ByteBuffer data) {
        
        /*
         * The #of bytes to be written (this is modified as a side effect by the
         * call to our superclass to we have to get it before we make that
         * call).
         */
        final int remaining = data.remaining();
        
        // Write on the buffer image.
        super.writeSlice(slots, data);

        // Position the buffer on the current slot.
        final int pos = journalHeaderSize + slotSize * slots.firstSlot();

        /*
         * Set limit to write just those bytes that were written on the buffer.
         * This includes the slot header in addition to the data written onto
         * the slot.
         */
        directBuffer.limit( pos + remaining );
        
        // Set position on the buffer.
        directBuffer.position( pos );

        try {

            /*
             * Write on the backing file.
             * 
             * Note: We use the direct buffer as the source since it is a native
             * memory buffer so that transfer to the disk cache should be
             * optimized by Java and the OS.
             */
            final int nwritten = raf.getChannel().write(directBuffer,pos);
            
            assert nwritten == remaining;
            
        }

        catch( IOException ex ) {
            
            throw new RuntimeException( ex );
            
        }

    }
    
    /**
     * Extends the basic behavior to write through to the backing file.
     */
    public void writeSlot(int slot, ByteBuffer data) {

        /*
         * The #of bytes to be written (this is modified as a side effect by the
         * call to our superclass to we have to get it before we make that
         * call).
         */
        final int remaining = data.remaining();
        
        // Write on the buffer image.
        super.writeSlot(slot, data);

        // Position the buffer on the current slot.
        final int pos = journalHeaderSize + slotSize * slot;

        /*
         * Set limit to write just those bytes that were written on the buffer.
         * This includes the slot header in addition to the data written onto
         * the slot.
         */
        directBuffer.limit( pos + remaining );
        
        // Set position on the buffer.
        directBuffer.position( pos );

        try {

            /*
             * Write on the backing file.
             * 
             * Note: We use the direct buffer as the source since it is a native
             * memory buffer so that transfer to the disk cache should be
             * optimized by Java and the OS.
             */
            final int nwritten = raf.getChannel().write(directBuffer,pos);
            
            assert nwritten == remaining;
            
        }

        catch( IOException ex ) {
            
            throw new RuntimeException( ex );
            
        }
        
    }
    
}
