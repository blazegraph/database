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
 * Note that the use of mapped files might not prove worth the candle due to the
 * difficulties with resource deallocation for this strategy and the good
 * performance of some alternative strategies.
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
    public void delete() {
        
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
     * @exception UnsupportedOperationException Always thrown.
     */
    public void truncate(long newExtent) {

        throw new UnsupportedOperationException(
                "Mapped file may not be extended or truncated.");
        
    }
    
}
