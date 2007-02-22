package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;


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
abstract public class DiskBackedBufferStrategy extends BasicBufferStrategy
        implements IDiskBasedStrategy {

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

    final public int getHeaderSize() {
        
        return headerSize;
        
    }

    final public File getFile() {
        
        return file;
        
    }
    
    final public RandomAccessFile getRandomAccessFile() {
        
        return raf;
        
    }
    
    final public boolean isOpen() {
        
        return open;
        
    }

    final public boolean isStable() {
        
        return true;
        
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

            raf.close();
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }
        
        open = false;
        
    }

    public void closeAndDelete() {
        
        close();
        
        if(!file.delete()) {
            
            System.err.println("WARN: Could not delete: "+file.getAbsolutePath());
            
        }
        
    }

    public void deleteFile() {
        
        if( open ) throw new IllegalStateException();
        
        if( ! file.delete() ) {
            
            throw new RuntimeException("Could not delete file: "
                    + file.getAbsoluteFile());
            
        }
        
    }
    
    DiskBackedBufferStrategy(long maximumExtent, BufferMode bufferMode,
            FileMetadata fileMetadata) {

        super(maximumExtent, fileMetadata.nextOffset, fileMetadata.headerSize0,
                fileMetadata.extent, bufferMode, fileMetadata.buffer);

        this.file = fileMetadata.file;
        
        this.raf = fileMetadata.raf;
        
        this.open = true;

    }
   
    public void writeRootBlock(IRootBlockView rootBlock,ForceEnum forceOnCommit) {

        if( rootBlock == null ) throw new IllegalArgumentException();

        try {

            FileChannel channel = raf.getChannel();

            final int count = channel.write(rootBlock.asReadOnlyBuffer(),
                    rootBlock.isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
                            : FileMetadata.OFFSET_ROOT_BLOCK1);

            if(count != RootBlockView.SIZEOF_ROOT_BLOCK) {
                
                throw new IOException("Expecting to write "
                        + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but wrote"
                        + count + " bytes.");
                
            }

            if( forceOnCommit != ForceEnum.No ) {

                force(forceOnCommit == ForceEnum.ForceMetadata);
            
            }

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public long transferTo(RandomAccessFile out) throws IOException {
        
        return super.transferFromDiskTo(this, out);
        
    }

}
