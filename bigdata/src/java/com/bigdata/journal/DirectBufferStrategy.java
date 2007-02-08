package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.rawstore.Addr;

/**
 * Direct buffer strategy uses a direct {@link ByteBuffer} as a write through
 * cache and writes through to disk for persistence.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Direct
 * 
 * @todo modify to support aio (asynchronous io). aio can be supported with a
 *       2nd thread that writes behind from the cache to the file. force() will
 *       need to be modified to wait until the aio thread has caught up to the
 *       nextOffset (i.e., has written all data that is dirty on the buffer).
 */
public class DirectBufferStrategy extends DiskBackedBufferStrategy {

    DirectBufferStrategy(long maximumExtent, FileMetadata fileMetadata) {

        super(maximumExtent, BufferMode.Direct,fileMetadata);

    }

    /**
     * Extends the basic behavior to write through to the backing file.
     */
    public long write(ByteBuffer data) {

        if (data == null)
            throw new IllegalArgumentException("Buffer is null");

        /*
         * The #of bytes to be written (this is modified as a side effect by the
         * call to our superclass to we have to get it before we make that
         * call).
         */
        final int remaining = data.remaining();

        // write on the buffer - this also detects and handles overflow.
        final long addr = super.write(data);

        // Position the buffer on the current slot.
        final int offset = Addr.getOffset(addr);

        /*
         * Set limit to write just those bytes that were written on the buffer.
         */
        directBuffer.limit( offset + remaining );
        
        // Set position on the buffer.
        directBuffer.position( offset );

        try {

            /*
             * Write on the backing file.
             * 
             * Note: We use the direct buffer as the source since it is a native
             * memory buffer so that transfer to the disk cache should be
             * optimized by Java and the OS.
             */
            final int nwritten = raf.getChannel().write(directBuffer,
                    headerSize + offset);
            
            assert nwritten == remaining;
            
        }

        catch( IOException ex ) {
            
            throw new RuntimeException( ex );
            
        }
        
        return addr;

    }
    
    public void truncate(long newExtent) {
        
        super.truncate(newExtent);
        
        try {

            raf.setLength(newExtent);
            
            System.err.println("Disk file: newLength="+newExtent);
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
}
