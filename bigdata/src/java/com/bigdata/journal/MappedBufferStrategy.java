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
 * timely manner. Journals that handle {@link IJournal#overflow()} should
 * trigger overflow just a bit earlier for a {@link MappedByteBuffer} in an
 * attempt to avoid running out of space in the journal. If a transaction can
 * not be committed due to overflow, it could be re-committed <em>after</em>
 * handling the overflow event (e.g., throw a "CommitRetryException").
 * </p>
 * <p>
 * The mapped mode has nearly the performance of the transient mode in some
 * tests. However the use of mapped files might not prove worth the candle due
 * to the difficulties with resource deallocation for this strategy and the good
 * performance of some alternative strategies.  There are also some other issues
 * that have shown up in some of the tests suites -- look carefully if you are
 * going to pursue this!
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
     * <p>
     * Note: The buffer is mapped onto the user extent from the first byte after
     * the root blocks through the last byte of the file. This means that we do
     * not need to translate the offset of an address when writing onto the
     * buffer.
     */
    final MappedByteBuffer mappedBuffer;

    public boolean isFullyBuffered() {
        
        return false;
        
    }
    
    MappedBufferStrategy(long maximumExtent, FileMetadata fileMetadata) {
        
        super(maximumExtent, BufferMode.Mapped, fileMetadata);
        
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

    /**
     * Overrides the default behavior so that an exception is NOT thrown if the
     * file can not be deleted. Since Java can not provide for synchronous unmap
     * of memory-mapped files, we can not delete the backing file immediately.
     * Instead, we mark the file for "deleteOnExit" and let the VM attempt to
     * clean it up when it exits.
     */
    public void deleteResources() {
        
        if( isOpen() ) {
            
            throw new IllegalStateException();
            
        }
        
        if( ! file.delete() ) {
            
            log.warn("Could not delete memory-mapped file: "
                    + file.getAbsoluteFile()
                    + " - marked for deletion on exit");

            file.deleteOnExit();
            
        }
        
    }

    /**
     * Note: Extension and truncation of a mapped file are not possible with the
     * JDK since there is no way to guarentee that the mapped file will be
     * unmapped in a timely manner.
     * 
     * @exception UnsupportedOperationException
     *                Always thrown.
     */
    public void truncate(long newExtent) {

        throw new UnsupportedOperationException(
                "Mapped file may not be extended or truncated.");
        
    }
    
}
