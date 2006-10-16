package com.bigdata.journal;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileLock;


/**
 * <p>
 * Memory-mapped journal strategy (this mode is NOT recommended).
 * </p>
 * <p>
 * Note: the use of {@link FileLock} with a memory-mapped file is NOT
 * recommended by the JDK as this combination is not permitted on some
 * platforms.
 * </p>
 * <p>
 * Note: Extension and truncation of a mapped file are not possible with the JDK
 * since there is no way to guarentee that the mapped file will be unmapped in a
 * timely manner.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
 * @see BufferMode#Mapped
 */
public class MappedBufferStrategy extends DiskBackedBufferStrategy {

    /**
     * A strongly typed reference to the buffer exposing memory-map specific
     * operations.
     */
    final MappedByteBuffer mappedBuffer;
    
    MappedBufferStrategy(FileMetadata fileMetadata, SlotMath slotMath) {
        
        super(BufferMode.Mapped, fileMetadata, slotMath);
        
        this.mappedBuffer = (MappedByteBuffer) fileMetadata.buffer;
        
    }
    
    /**
     * Force the data to disk.
     * 
     * @see MappedByteBuffer#force()
     */
    public void force(boolean metadata) {

        mappedBuffer.force();

        // Note: This super class invocation is probably not required.
        super.force(metadata );
        
    }
 
    /**
     * The file channel is closed, but according to
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038 there is no
     * way to guarentee when the mapped file will be released.
     */
    
    public void close() {
        
        super.close();
        
    }
    
}