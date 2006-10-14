package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * Abstract base class for implementations that use a direct buffer as a
 * write through cache to an image on the disk. This covers both the
 * {@link BufferMode#Direct}, where we use explicit IO operations, and the
 * {@link BufferMode#Mapped}, where we memory-map the image. Common
 * features shared by these implementations deal mainly with initialization
 * of a new disk image.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class DiskBackedBufferStrategy extends BasicBufferStrategy {

    /**
     * The name of the backing file.
     */
    final File file;

    /**
     * Interface for random access on the backing file.
     */
    final RandomAccessFile raf;

    /**
     * True iff the channel is open.
     */
    private boolean open = false;

    public boolean isOpen() {
        
        return open;
        
    }

    /**
     * Forces the data to disk.
     */
    public void force( boolean metadata ) {
        
        try {

            raf.getChannel().force( metadata );
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }           
        
    }
    
    /**
     * Closes the file.
     */
    public void close() {
        
        if( ! open ) throw new IllegalStateException();

        try {

            force( false );
            
            raf.close();
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }
        
        open = false;
        
    }

    DiskBackedBufferStrategy(BufferMode bufferMode, FileMetadata fileMetadata, SlotMath slotMath) {

        super(bufferMode,slotMath,fileMetadata.buffer);

        this.file = fileMetadata.file;
        
        this.raf = fileMetadata.raf;
        
        this.open = true;

    }

}