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

import java.nio.ByteBuffer;

/**
 * <p>
 * The buffer mode in which the journal is opened.
 * </p>
 * <p>
 * This {@link #Direct} and {@link #Mapped} options may not be used for
 * files exceeding {@link Integer#MAX_VALUE} bytes in length since a
 * {@link ByteBuffer} is indexed with an <code>int</code>. However the
 * common use case for a journal is a small file on the order of 10s of
 * megabytes that absorbs writes for a read-optimized database file.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

public enum BufferMode {

    /**
     * <p>
     * A variant on the {@link #Direct} mode that is not restart-safe. This mode
     * is useful for segments whose contents do not require persistence, applets,
     * etc.
     * </p>
     */
    Transient("Transient"),
    
    /**
     * <p>
     * A direct buffer is allocated for the file image. Writes are applied
     * to the buffer. The buffer tracks dirty slots regardless of the
     * transaction that wrote them and periodically writes dirty slots
     * through to disk. On commit, any dirty index or allocation nodes are
     * written onto the buffer and all dirty slots on the buffer. Dirty
     * slots in the buffer are then synchronously written to disk, the
     * appropriate root block is updated, and the file is (optionally)
     * flushed to disk.
     * </p>
     * <p>
     * This option offers wires an image of the journal file into memory and
     * allows the journal to optimize IO operations.
     * </p>
     */
    Direct("Direct"),
    
    /**
     * <p>
     * A memory-mapped buffer is allocated for the file image. Writes are
     * applied to the buffer. Reads read from the buffer. On commit, the map is
     * forced disk disk.
     * </p>
     * <p>
     * This option yields control over IO and memory resources to the OS.
     * However, there is currently no way to force release of the mapped memory
     * per the bug described below. This means (a) that the mapped file might
     * not be deletable; and (b) that native memory can be exhausted. While
     * performance is good on at least some benchmarks, it is difficult to
     * recommend this solution given its downsides.
     * </p>
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
     */
    Mapped("Mapped"),
    
    /**
     * <p>
     * The journal is managed on disk. This option may be used with files of
     * more than {@link Integer#MAX_VALUE} bytes in extent, but no more than
     * {@link Integer#MAX_VAlUE} slots. Large files are NOT the normal use case
     * for bigdata. Journal performance for large files should be fair on write,
     * but performance will degrade as the the object and allocation indices
     * grow and the journal is NOT optimized for random reads (poor locality).
     * </p>
     */
    Disk("Disk");
    
    private final String name;
    
    BufferMode(String name) {

        this.name = name;
        
    }
    
    public String toString() {
        
        return name;
        
    }

    /**
     * Parse a string whose contents must be "Transient", "Direct", "Mapped", or
     * "Disk".
     * 
     * @param s
     *            The string.
     * 
     * @return The named {@link BufferMode}.
     */
    public static BufferMode parse(String s) {
        if( s == null ) throw new IllegalArgumentException();
        if( s.equals(Transient.name)) return Transient;
        if( s.equals(Direct.name)) return Direct;
        if( s.equals(Mapped.name)) return Mapped;
        if( s.equals(Disk.name)) return Disk;
        throw new IllegalArgumentException();
    }
    
}
