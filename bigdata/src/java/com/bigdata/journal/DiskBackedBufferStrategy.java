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

    final public int getHeaderSize() {
        
        return headerSize;
        
    }

    final public File getFile() {
        
        return file;
        
    }
    
    final public RandomAccessFile getRandomAccessFile() {
        
        return raf;
        
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

        super.close();

        try {

            raf.close();
            
        } catch( IOException ex ) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    public void closeAndDelete() {
        
        close();
        
        if(!file.delete()) {
            
            System.err.println("WARN: Could not delete: "+file.getAbsolutePath());
            
        }
        
    }

    public void deleteFile() {
        
        if( isOpen() ) {
            
            throw new IllegalStateException();
            
        }
        
        if( ! file.delete() ) {
            
            throw new RuntimeException("Could not delete file: "
                    + file.getAbsoluteFile());
            
        }
        
    }
    
    DiskBackedBufferStrategy(long maximumExtent, BufferMode bufferMode,
            FileMetadata fileMetadata) {

        super(maximumExtent, fileMetadata.offsetBits, fileMetadata.nextOffset,
                fileMetadata.headerSize0, fileMetadata.extent, bufferMode,
                fileMetadata.buffer);

        this.file = fileMetadata.file;
        
        this.raf = fileMetadata.raf;
        
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
