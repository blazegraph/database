package com.bigdata.journal;


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
    final BufferMode bufferMode;
    
    /**
     * The size of the journal header, including MAGIC, version, and both root
     * blocks. This is as an offset when computing the index of a slot on the
     * journal.
     */
    final int journalHeaderSize;

    /**
     * The size of a slot. 
     */
    final int slotSize;
    
    /**
     * The size of the per-slot header.
     */
    final int slotHeaderSize;

    /**
     * The size of the per-slot data region.
     */
    final int slotDataSize;

    public int getSlotDataSize() {
        
        return slotDataSize;
        
    }
    
    public BufferMode getBufferMode() {return bufferMode;}

    AbstractBufferStrategy(int journalHeaderSize, BufferMode bufferMode, SlotMath slotMath) {

        assert journalHeaderSize >= 0;
        
        if( bufferMode == null ) throw new IllegalArgumentException();
        
        if( slotMath == null ) throw new IllegalArgumentException();
        
        this.journalHeaderSize = journalHeaderSize;
        
        this.bufferMode = bufferMode;
        
        this.slotSize = slotMath.slotSize;
        
        this.slotHeaderSize = slotMath.headerSize;
        
        this.slotDataSize = slotMath.dataSize;
        
    }

    /**
     * Throws an exception if the extent is too large for an in-memory
     * buffer.
     * 
     * @param extent The extent.
     * 
     * @return The extent.
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

    /**
     * FIXME Implement the ability to extent or truncate the buffer.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public void truncate(long extent) {
        
        throw new UnsupportedOperationException();
        
    }
    
}