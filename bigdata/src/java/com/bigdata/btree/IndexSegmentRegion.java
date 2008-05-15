package com.bigdata.btree;

/**
 * Type-safe enumeration of the regions to which relative offsets may be
 * constructed for an {@link IndexSegmentStore}.
 * 
 * @see IndexSegmentBuilder
 * @see IndexSegmentStore
 * @see IndexSegmentCheckpoint
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum IndexSegmentRegion {

    /**
     * Offset is relative to the start of the backing file.
     * <p>
     * Note: The {@link #BASE} region runs from the start of the file until the
     * end of the file and thus overlaps both the {@link #NODE} and
     * {@link #BLOB} regions. <em>Any</em> address may be expressed within the
     * {@link #BASE} region.
     * <p>
     * Note: The leaves are not really their own region, but rather are found
     * within the {@link #BASE} region starting at [leavesOffset] and running
     * for [leavesExtent] bytes.
     */
    BASE(0x00),
    /**
     * Offset is relative to the start of the node region.  This region contains
     * only the nodes.
     */
    NODE(0x01),
    /**
     * Offset is relative to the start of the blob region. This region contains
     * only raw records used for blob references.
     */
    BLOB(0x02);
    
    private final int code;
    
    private IndexSegmentRegion(int code) {
        this.code = code;
    }
    
    public int code() {
        return code;
    }

    /**
     * The #of bits that are used to indicate the {@link IndexSegmentRegion} on an
     * encoded address. Since the address is encoded in a 64-bit long, the
     * #of bits available to encode the offset and byteCount of the address
     * is reduced by this many bits.
     */
    final static protected long NBITS = 0x02L;
    
    /**
     * A mask that shows only the lower {@link #NBITS}.
     */
    final static private long MASK = 0x03;
    
    /**
     * Encode an offset within a region. The address is left shifted by one
     * {@link #NBITS} and the low bits are set to indicate the region that
     * identifies the base for the offset.
     * 
     * @param offset
     *            The offset of the allocation.
     * 
     * @return The encoded offset.
     */
    public long encodeOffset(long offset) {

//        if(offset==0L) return 0L;
        
        return (offset << NBITS) | code;

    }

    /**
     * Return the decoded region from an encoded offset.
     * 
     * @param encodedOffset
     *            The encoded offset.
     *            
     * @return The decoded region.
     */
    public static IndexSegmentRegion decodeRegion(long encodedOffset) {
        
//        if(offset==0L) return BASE;
        
        final int code = (int) (MASK & encodedOffset);

        switch (code) {
        case 0x00:
            return BASE;
        case 0x01:
            return NODE;
        case 0x02:
            return BLOB;
        default:
            throw new IllegalArgumentException();
        }

    }

    /**
     * Return the decoded offset (right-shifts by {@link #NBITS} in
     * order to strip off the bits used to encode the region). The
     * returned offset is relative to the encoded region. The caller
     * MUST adjust the offset appropriately in order to de-reference
     * the record in the store.
     * 
     * @param encodedOffset
     *            The encoded offset.
     * 
     * @return The decoded offset.
     */
    public static long decodeOffset(long encodedOffset) {
        
//        if(encodedOffset==0L) return 0L;
        
        return encodedOffset >>> NBITS;
        
    }
    
}
