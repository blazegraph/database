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
    public void writeSlot(int thisSlot,int priorSlot,int nextSlot, ByteBuffer data) {

        // #of bytes to be written.
        final int remaining = data.remaining();
        
        // Write on the buffer image.
        super.writeSlot(thisSlot,priorSlot,nextSlot,data);
        
        // Position the buffer on the current slot.
        final int pos = journalHeaderSize + slotSize * thisSlot;

        /*
         * Set limit to write just those bytes that were written on the buffer.
         * This includes the slot header in addition to the data written onto
         * the slot.
         */
        directBuffer.limit( pos + slotHeaderSize + remaining );
        
        // Set position on the buffer.
        directBuffer.position( pos );

        try {

            // Write on the backing file.
            final int nwritten = raf.getChannel().write(directBuffer,pos);
            
            assert nwritten == remaining + slotHeaderSize;
            
        }

        catch( IOException ex ) {
            
            throw new RuntimeException( ex );
            
        }
        
    }
    
}
