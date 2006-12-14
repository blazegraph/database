package com.bigdata.objndx;

/**
 * An address encodes both an int32 length and an int32 offset into a
 * single long integer. This limits the addressable size of a file to
 * int32 bytes, but that limit far exceeds the envisoned capacity of a
 * single file in the bigdata architecture. Note that the long integer
 * ZERO (0L) is reserved and always has the semantics of a <em>null</em>
 * reference.  In practice this means that there must be some non-zero
 * offset to the start of the persistent data, e.g., a header record on
 * the file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
final public class Addr {

    /**
     * Converts a length and offset into a long integer.
     * 
     * @param nbytes
     *            The #of bytes.
     * @param offset
     *            The offset.
     * 
     * @return The long integer.
     */
    public static long toLong(int nbytes,int offset) {
        
        assert nbytes >= 0;

        assert offset >= 0;
        
        return ((long) offset) << 32 | nbytes ;
        
    }
    
    /**
     * Extracts the byte count from a long integer formed by
     * {@link #toLong(int, int)}.
     * 
     * @param addr
     *            The long integer.
     * 
     * @return The byte count in the corresponding slot allocation.
     */
    public static int getByteCount(long addr) {

        return (int) (NBYTES_MASK & addr);

    }

    /**
     * Extracts the offset from a long integer formed by
     * {@link #toLong(int, int)}.
     * 
     * @param addr
     *            The long integer.
     *            
     * @return The offset.
     */
    public static int getOffset(long addr) {

        return (int) ((OFFSET_MASK & addr) >>> 32);

    }

    private static final transient long NBYTES_MASK = 0x00000000ffffffffL;
    private static final transient long OFFSET_MASK = 0xffffffff00000000L;
    
}