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
     * Overrides the default behavior so that an exception is NOT thrown if the
     * file can not be deleted. Since Java can not provide for synchronous unmap
     * of memory-mapped files, we can not delete the backing file immediately.
     * Instead, we mark the file for "deleteOnExit" and let the VM attempt to
     * clean it up when it exits.
     */
    public void deleteFile() {
        
        if( isOpen() ) throw new IllegalStateException();
        
        if( ! file.delete() ) {
            
            System.err.println("Could not delete memory-mapped file: "
                    + file.getAbsoluteFile());

            file.deleteOnExit();
            
        }
        
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