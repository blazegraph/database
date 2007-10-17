/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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

            raf.close();
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    public void delete() {

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
                fileMetadata.buffer);

        this.file = fileMetadata.file;
        
        this.fileMode = fileMetadata.fileMode;
        
        this.raf = fileMetadata.raf;
        
    }
   
    public void writeRootBlock(IRootBlockView rootBlock,ForceEnum forceOnCommit) {

        if( rootBlock == null ) throw new IllegalArgumentException();

        try {

            FileChannel channel = getChannel();

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

            raf = FileMetadata.openFile(file, fileMode, bufferMode);
        
            log.warn("Re-opened file: "+file);
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }

        return true;
        
    }
    
}
