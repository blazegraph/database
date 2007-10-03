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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;

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
     * <p>
     * Note: {@link ClosedChannelException} can be thrown out of this method.
     * This exception is an indication that the backing channel was closed while
     * a writer was still running.
     * <p>
     * Note: {@link ClosedByInterruptException} can be thrown out of this method
     * (wrapped as a {@link RuntimeException}). This exception is an indication
     * that a writer was interrupted. This will occur if you are using
     * {@link ExecutorService#shutdownNow()} on a service that is running one or
     * more writers. In such cases this should not be considered an error but
     * the expected result of interrupting the writer.
     * <p>
     * However, note that {@link ClosedByInterruptException} means that the
     * channel was actually <strong>closed</strong> when the writer was
     * interrupted. This means that you basically can not interrupt running
     * writers without having to re-open the channel.
     */
    public long write(ByteBuffer data) {

        /*
         * write the record on the buffer - this also detects and handles
         * overflow, error checks the address range, etc.
         */
        final long addr = super.write(data);

        // The offset into the buffer for this record.
        final long offset = getOffset(addr);

        // The length of the record.
        final int nbytes = getByteCount(addr);
        
        // obtain a view in case there are concurrent writes on the buffer.
        final ByteBuffer buffer = getBufferView(false/* readOnly */);
        
        // Set limit to just those bytes that were written on the buffer.
        buffer.limit( (int) offset + nbytes );
        
        // Set position on the buffer.
        buffer.position( (int) offset );

        try {

            /*
             * Write on the backing file.
             * 
             * Note: We use the direct buffer as the source since it is a native
             * memory buffer so that transfer to the disk cache should be
             * optimized by Java and the OS.
             */

            final int count = raf.getChannel().write(buffer,
                    headerSize + offset);
            
            if (count != nbytes) {

                throw new RuntimeException("Expected to write " + nbytes
                        + " bytes but wrote " + count);

            }
            
        } catch( IOException ex ) {
            
            throw new RuntimeException( ex );
            
        }
        
        return addr;

    }
    
    synchronized public void truncate(long newExtent) {
        
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
