/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.bigdata.io.FileChannelUtility;

/**
 * Abstract base class for implementations that use a direct buffer as a write
 * through cache to an image on the disk. This covers both the
 * {@link BufferMode#Direct}, where we use explicit IO operations, and the
 * {@link BufferMode#Mapped}, where we memory-map the image. Common features
 * shared by these implementations deal mainly with initialization of a new disk
 * image.
 * 
 * @todo write tests of the disk-only mode operations when overflowing an int32
 *       extent.
 * 
 * @todo consider a read buffer that uses a weak value cache backed by an LRU
 *       hard reference cache, which in turn evicts buffers back to a pool for
 *       reuse. The pool would bin buffers by size, keep no more than some #of
 *       buffers of a given size, and offer a service to allocate buffers of no
 *       less than a given capacity. This might reduce heap churn by recycling
 *       buffers in addition to providing minimizing the need to hit the OS disk
 *       cache, the disk buffers, or the disk platter.
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
     * The mode used to open the backing file.
     */
    final String fileMode;
    
    /**
     * Interface for random access on the backing file.
     */
    /*final*/ RandomAccessFile raf;

    final public int getHeaderSize() {
        
        return headerSize;
        
    }

    final public File getFile() {
        
        return file;
        
    }
    
    final public RandomAccessFile getRandomAccessFile() {
        
        return raf;
        
    }
    
    final public FileChannel getChannel() {

        return getRandomAccessFile().getChannel();
        
    }
    
    final public boolean isStable() {
        
        return true;
        
    }

    /**
     * Forces the data to disk.
     */
    public void force( boolean metadata ) {
        
        try {

            getChannel().force( metadata );
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }           
        
    }
    
    /**
     * Closes the file.
     */
    public void close() {

        /*
         * Note: this clears the [open] flag. It is important to do this first
         * so that we do not re-open the channel once it has been closed.
         */

        super.close();

        try {

            FileMetadata.closeFile(file, raf);
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    public void deleteResources() {

        if(isOpen()) throw new IllegalStateException();
        
        if(!file.delete()) {
            
            throw new RuntimeException("Could not delete: "
                    + file.getAbsolutePath());
            
        }
        
    }

    DiskBackedBufferStrategy(long maximumExtent, BufferMode bufferMode,
            FileMetadata fileMetadata) {

        super(maximumExtent, fileMetadata.offsetBits, fileMetadata.nextOffset,
                FileMetadata.headerSize0, fileMetadata.extent, bufferMode,
                fileMetadata.buffer, fileMetadata.readOnly);

        this.file = fileMetadata.file;
        
        this.fileMode = fileMetadata.fileMode;
        
        this.raf = fileMetadata.raf;
        
    }
   
    public void writeRootBlock(IRootBlockView rootBlock,ForceEnum forceOnCommit) {

        if (rootBlock == null)
            throw new IllegalArgumentException();

        try {

            final ByteBuffer data = rootBlock.asReadOnlyBuffer();
            
            final long pos = rootBlock.isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
                    : FileMetadata.OFFSET_ROOT_BLOCK1;
            
            FileChannelUtility.writeAll(getChannel(), data, pos);

//            final int count = getChannel().write(rootBlock.asReadOnlyBuffer(),
//                    rootBlock.isRootBlock0() ? FileMetadata.OFFSET_ROOT_BLOCK0
//                            : FileMetadata.OFFSET_ROOT_BLOCK1);
//
//            if(count != RootBlockView.SIZEOF_ROOT_BLOCK) {
//                
//                throw new IOException("Expecting to write "
//                        + RootBlockView.SIZEOF_ROOT_BLOCK + " bytes, but wrote"
//                        + count + " bytes.");
//                
//            }

            if( forceOnCommit != ForceEnum.No ) {

                force(forceOnCommit == ForceEnum.ForceMetadata);
            
            }

        }

        catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    synchronized public long transferTo(RandomAccessFile out)
            throws IOException {
        
        return super.transferFromDiskTo(this, out);
        
    }

    /**
     * The backing file is fully buffered so it does not need to be open for a
     * read to succeed. However, we use this as an opportunity to transparently
     * re-open the {@link FileChannel} if it has been closed asynchronously or
     * in response to an interrupt (that is, if we discover that the channel is
     * closed but {@link #isOpen()} still returns true).
     */
    public ByteBuffer read(long addr) {
        
        if(isOpen() && !raf.getChannel().isOpen()) {
            
            reopenChannel();
            
        }
        
        return super.read(addr);
        
    }
        
    /**
     * This method transparently re-opens the channel for the backing file.
     * <p>
     * Note: This method is synchronized so that concurrent readers do not try
     * to all open the store at the same time.
     * <p>
     * Note: This method is ONLY invoked by readers. This helps to ensure that a
     * writer that has been interrupted can not regain access to the channel (it
     * does not prevent it, but re-opening for writers is asking for trouble).
     * 
     * @return true iff the channel was re-opened.
     * 
     * @throws IllegalStateException
     *             if the buffer strategy has been closed.
     *             
     * @throws RuntimeException
     *             if the backing file can not be opened (can not be found or
     *             can not acquire a lock).
     */
    synchronized private boolean reopenChannel() {
        
        if(raf.getChannel().isOpen()) {
            
            /* The channel is still open.  If you are allowing concurrent reads
             * on the channel, then this could indicate that two readers each 
             * found the channel closed and that one was able to re-open the
             * channel before the other such that the channel was open again
             * by the time the 2nd reader got here.
             */
            
            return true;
            
        }
        
        if(!isOpen()) {

            // the buffer strategy has been closed.
            
            return false;
            
        }

        try {

            raf = FileMetadata.openFile(file, fileMode, true/*tryFileLock*/);
        
            log.warn("Re-opened file: "+file);
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        return true;
        
    }
    
}
