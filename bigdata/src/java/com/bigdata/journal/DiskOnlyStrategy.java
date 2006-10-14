package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.bigdata.journal.Journal.SlotHeader;

/**
 * Disk-based journal strategy.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Disk
 * 
 * @todo Implement.
 */
public class DiskOnlyStrategy extends AbstractBufferStrategy {

  final File file;
  final RandomAccessFile raf;
  final long extent;
  private boolean open;
    
    /**
     * @param slotMath
     */
    DiskOnlyStrategy(FileMetadata fileMetadata, SlotMath slotMath) {

        super(BufferMode.Disk,slotMath);

        this.file = fileMetadata.file;
        
        this.raf = fileMetadata.raf;
        
        this.extent = fileMetadata.extent;
        
        open = true;

    }


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
        
        if( ! isOpen() ) throw new IllegalStateException();

        try {

            force( false );
            
            raf.close();
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }
        
        open = false;
        
    }

    public long getExtent() {

        return extent;
        
    }

    public int getSlotLimit() {
        // TODO Auto-generated method stub
        return 0;
    }

    public ByteBuffer readFirstSlot(long id, int firstSlot, boolean readData, SlotHeader slotHeader) {
        // TODO Auto-generated method stub
        return null;
    }

    public int readNextSlot(long id, int thisSlot, int priorSlot, int slotsRead, ByteBuffer dst) {
        // TODO Auto-generated method stub
        return 0;
    }

    public void writeSlot(int thisSlot, int priorSlot, int nextSlot, ByteBuffer data) {
        // TODO Auto-generated method stub
        
    }

}