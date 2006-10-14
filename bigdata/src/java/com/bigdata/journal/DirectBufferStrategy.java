package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Direct buffer strategy uses a direct {@link ByteBuffer} as a write
 * through cache and writes through to disk for persistence.
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
     * 
     * @todo This always writes the entire slot, even when only part of the
     *       slot was written on by the super implementation. This edge case
     *       only occurs on the last slot of a chain, but it is common
     *       enough since we expect many objects to fit into a single slot.
     */
    public void writeSlot(int thisSlot,int priorSlot,int nextSlot, ByteBuffer data) {

        // Write on the buffer image.
        super.writeSlot(thisSlot,priorSlot,nextSlot,data);
        
        // Position the buffer on the current slot.
        final int pos = SIZE_JOURNAL_HEADER + slotSize * thisSlot;
        directBuffer.limit( pos + slotSize );
        directBuffer.position( pos );

        try {

            // Write on the backing file.
            raf.getChannel().write(directBuffer,pos);
            
        }

        catch( IOException ex ) {
            
            throw new RuntimeException( ex );
            
        }
        
    }
    
}