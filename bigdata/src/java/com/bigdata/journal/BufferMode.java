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

import com.bigdata.btree.BTree;

/**
 * <p>
 * The buffer mode in which the journal is opened.
 * </p>
 * <p>
 * The {@link #Direct} and {@link #Mapped} options may not be used for files
 * exceeding {@link Integer#MAX_VALUE} bytes in length since a
 * {@link ByteBuffer} is indexed with an <code>int</code> (the pragmatic limit
 * is typically much lower and depends on the size of the JVM heap for the
 * {@link #Direct} mode).
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum BufferMode {

    /**
     * <p>
     * A variant on the {@link #Direct} mode that is not restart-safe. This mode
     * is useful for temporary stores which can reside entirely in memory and do
     * not require disk.  It can be used in environments, such as an applet,
     * where you can not access the disk (however, you can also use a transient
     * {@link BTree} with much the same effect).  The {@link #Temporary} mode
     * is much more scalable.
     * </p>
     * 
     * @see TransientBufferStrategy
     */
    Transient(false/* stable */, true/* fullyBuffered */),

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
     * 
     * @see DirectBufferStrategy
     */
    Direct(true/* stable */, true/* fullyBuffered */),

    /**
     * <p>
     * A memory-mapped buffer is allocated for the file image. Writes are
     * applied to the buffer. Reads read from the buffer. On commit, the map is
     * forced disk disk.
     * </p>
     * <p>
     * This option yields control over IO and memory resources to the OS.
     * However, there is currently no way to force release of the mapped memory
     * per the bug described below. This means (a) you might not be able to
     * delete the mapped file; and (b) that native memory can be exhausted.
     * While performance is good on at least some benchmarks, it is difficult to
     * recommend this solution given its downsides.
     * </p>
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
     * @see MappedBufferStrategy
     */
    Mapped(true/* stable */, false/* fullyBuffered */),

    /**
     * <p>
     * The journal is managed on disk. This option may be used with files of
     * more than {@link Integer#MAX_VALUE} bytes in extent. Journal performance
     * for large files should be fair on write, but performance will degrade as
     * the journal is NOT optimized for random reads (poor locality).
     * </p>
     * 
     * @see DiskOnlyStrategy
     */
    Disk(true/* stable */, false/* fullyBuffered */),

    /**
     * <p>
     * The journal is managed on disk. This option may be used with files of
     * more than {@link Integer#MAX_VALUE} bytes in extent. Journal performance
     * for large files should be fair on write, but performance will degrade as
     * the journal is NOT optimized for random reads (poor locality).
     * </p>
     * 
     * @see WORMStrategy
     * 
     * @deprecated This mode exists to test a new version of the
     *             {@link DiskOnlyStrategy} which does not require
     *             synchronization for disk read/write operations except when
     *             the file size is being changed. Once this is working properly
     *             it will replace the {@link DiskOnlyStrategy}.
     */
    DiskWORM(true/* stable */, false/* fullyBuffered */),

    /**
     * <p>
     * The journal is managed on disk. This option may be used with files of
     * more than {@link Integer#MAX_VALUE} bytes in extent. RW indicates that
     * it is not a WORM with append only semantics but rather a disk alloc/realloc
     * mechanism that supports updates to values.  In general the store locality
     * may be poor but should normally benefit in comparison to a WORM with
     * smaller disk size.
     * </p>
     * 
     * @see RWStrategy
     */
    DiskRW(true/* stable */, false/* fullyBuffered */),

    /**
     * <p>
     * A variant on the {@link #Disk} mode that is not restart-safe. This mode
     * is useful for all manners of temporary data with full concurrency control
     * and scales-up to very large temporary files. The backing file (if any) is
     * always destroyed when the store is closed. This is much more scalable
     * than the {@link #Transient} mode.
     * </p>
     * 
     * @see DiskOnlyStrategy
     */
    Temporary(false/* stable */, false/* fullyBuffered */);

    private final boolean stable;
    private final boolean fullyBuffered;
    
    private BufferMode(final boolean stable, final boolean fullyBuffered) {
        
        this.stable = stable;
        
        this.fullyBuffered = fullyBuffered;
        
    }
    
    /**
     * <code>true</code> iff this {@link BufferMode} uses a stable media
     * (disk).
     */
    public boolean isStable() {
       
        return stable;
        
    }
    
    /**
     * <code>true</code> iff this {@link BufferMode} is fully buffered 
     * in memory - this implies that there is an absolute upper bound
     * of {@link Integer#MAX_VALUE} bytes in the store since that is
     * the limit on a byte[] in Java.
     */
    public boolean isFullyBuffered() {
       
        return fullyBuffered;
        
    }
    
}
