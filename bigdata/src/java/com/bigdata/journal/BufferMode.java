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

import java.nio.ByteBuffer;

/**
 * <p>
 * The buffer mode in which the journal is opened.
 * </p>
 * <p>
 * The {@link #Direct} and {@link #Mapped} options may not be used for files
 * exceeding {@link Integer#MAX_VALUE} bytes in length since a
 * {@link ByteBuffer} is indexed with an <code>int</code> (the pragmatic limit
 * is much lower since a JVM does not have access to more than 2G of RAM).
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
    Transient(false/*stable*/),
    
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
    Direct(true/*stable*/),
    
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
    Mapped(true/*stable*/),
    
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
    Disk(true/*stable*/),
    
    /**
     * <p>
     * A variant on the {@link #Disk} mode that is not restart-safe. This mode
     * is useful for all manners of temporary data with full concurrency control
     * and scales-up to very large temporary files. The backing file (if any) is
     * always destroyed when the store is closed.
     * </p>
     */
    Temporary(false/*stable*/);
    
    private final boolean stable;
    
    private BufferMode(boolean stable) {
        
        this.stable = stable;
        
    }
    
    /**
     * <code>true</code> iff this {@link BufferMode} uses a stable media
     * (disk).
     */
    public boolean isStable() {
       
        return stable;
        
    }
    
}
