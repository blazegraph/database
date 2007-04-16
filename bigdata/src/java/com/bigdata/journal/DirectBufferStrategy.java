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

        super(maximumExtent, BufferMode.Direct, fileMetadata);

    }

    public boolean isFullyBuffered() {
        
        return true;
        
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

            // extend the file.
            raf.setLength(newExtent);
            
            /*
             * since we just changed the file length we force the data to disk
             * and update the file metadata. this is a relatively expensive
             * operation but we want to make sure that we do not loose track of
             * a change in the length of the file.
             * 
             * @todo an alternative would be to set a marker on the buffer such
             * that the next force() also forced the metadata to disk.
             */
            force(true);
            
            System.err.println("Disk file: newLength="+cf.format(newExtent));
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
}
