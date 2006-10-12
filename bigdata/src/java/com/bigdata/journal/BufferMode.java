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
    Direct("direct"),
    
    /**
     * <p>
     * A memory-mapped buffer is allocated for the file image.  Writes are
     * applied to the buffer.  Reads read from the buffer.  On commit, the
     * map is forced disk disk.
     * </p>
     * <p>
     * This option yields control over IO and memory resources to the OS.
     * </p> 
     */
    Mapped("mapped"),
    
    /**
     * <p>
     * The journal is managed on disk. Disk pages are cached to support
     * random access patterns. The primary use cases for random access are
     * reading index or allocation nodes; reading objects to support
     * state-based validation or application data fetches; reading objects
     * whose data is being migrated to the database.
     * </p>
     * <p>
     * This option may be used with very large files. Large files are NOT
     * the normal use case for bigdata. Journal performance for very large
     * files should be fair on write, but performance does degrade as the
     * the object and allocation indices grow and the journal is NOT
     * optimized for random reads (poor locality).
     * </p>
     */
    Disk("disk");
    
    private final String name;
    
    BufferMode(String name) {
        this.name = name;
    }
    
    public String toString() { return name; }

    /**
     * Parse a string whose contents must be "direct", "mapped", or "disk".
     * 
     * @param s
     *            The string.
     *            
     * @return The named {@link BufferMode}.
     */
    public static BufferMode parse(String s) {
        if( s == null ) throw new IllegalArgumentException();
        if( s.equals(Direct.name)) return Direct;
        if( s.equals(Mapped.name)) return Mapped;
        if( s.equals(Disk.name)) return Disk;
        throw new IllegalArgumentException();
    }
    
}