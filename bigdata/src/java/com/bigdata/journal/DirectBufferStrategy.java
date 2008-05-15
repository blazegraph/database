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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;

import com.bigdata.io.FileChannelUtility;

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

            FileChannelUtility.writeAll(getChannel(), buffer, headerSize + offset);
            
//            final int count = getChannel().write(buffer, headerSize + offset);
//            
//            if (count != nbytes) {
//
//                throw new RuntimeException("Expected to write " + nbytes
//                        + " bytes but wrote " + count);
//
//            }
            
        } catch( IOException ex ) {
            
            throw new RuntimeException( ex );
            
        }
        
        return addr;

    }
    
    synchronized public void truncate(long newExtent) {
        
        super.truncate(newExtent);
        
        try {

            // extend the file.
            getRandomAccessFile().setLength(newExtent);
            
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
