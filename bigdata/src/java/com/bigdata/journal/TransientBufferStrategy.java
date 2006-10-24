package com.bigdata.journal;

import java.nio.ByteBuffer;


/**
 * Transient buffer strategy uses a direct buffer but never writes on disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Transient
 */
public class TransientBufferStrategy extends BasicBufferStrategy {
    
    /**
     * The transient journal does not have a header (no magic, no version, and
     * no root blocks).
     * 
     * FIXME Verify that we can (and want to) operate without root blocks.
     */
    static private final int journalHeaderSize = 0;
    
    private boolean open = false;
    
    TransientBufferStrategy(SlotMath slotMath,long initialExtent) {
        
        super(journalHeaderSize,BufferMode.Transient, slotMath, ByteBuffer
                .allocateDirect((int) assertNonDiskExtent(initialExtent)));
    
        open = true;
        
    }
    
    public void deleteFile() {
        
        if( open ) throw new IllegalStateException();

        // NOP.
        
    }
    
    public void force(boolean metadata) {
        
        // NOP.
        
    }

    public void close() {
        
        if( ! isOpen() ) {
            
            throw new IllegalStateException();
            
        }

        force(true);
        
        open = false;
        
    }

    public boolean isOpen() {

        return open;
        
    }
    
}