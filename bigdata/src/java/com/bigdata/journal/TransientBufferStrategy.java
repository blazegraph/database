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
    
    private boolean open = false;

    /**
     * The root blocks.
     */
    final private IRootBlockView rootBlocks[] = new IRootBlockView[2];
    
    /**
     * Either zero (0) or one (1).
     */
    private int currentRootBlock = 0;
    
    TransientBufferStrategy(boolean useDirectBuffers,long extent) {
        
        /*
         * Note: I have not observed much performance gain from the use of
         * a direct buffer for the transient mode.
         */
        super(  0/* nextOffset */, //
                0/*headerSize*/, //
                extent, //
                BufferMode.Transient, //
                (useDirectBuffers ? ByteBuffer
                        .allocateDirect((int) assertNonDiskExtent(extent))
                        : ByteBuffer
                                .allocate((int) assertNonDiskExtent(extent)))
                );
    
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

//        force(true);
        
        open = false;
        
    }

    public boolean isOpen() {

        return open;
        
    }

    public boolean isStable() {
        
        return true;
        
    }

    public void writeRootBlock(IRootBlockView rootBlock, ForceEnum forceOnCommit) {
        
        if(rootBlock == null) throw new IllegalArgumentException();
        
        currentRootBlock = rootBlock.isRootBlock0() ? 0 : 1;

        rootBlocks[currentRootBlock] = rootBlock;
        
    }

}
