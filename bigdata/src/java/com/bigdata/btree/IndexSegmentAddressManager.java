package com.bigdata.btree;

import com.bigdata.rawstore.WormAddressManager;

/**
 * <p>
 * Address manager supporting offsets that are encoded for one of several
 * regions in an {@link IndexSegmentFileStore}. The regions are identified
 * by a {@link IndexSegmentRegion}, which gets encoded into the offset component of
 * the address. The offsets are relative to the start of the identified
 * regions. The {@link IndexSegmentCheckpoint} record gives the start of
 * each region.
 * </p>
 * <p>
 * Together with {@link IndexSegmentRegion}, this class class provides a workaround
 * for node offsets (which are relative to the start of the nodes block) in
 * contrast to leaf offsets (which are relative to a known offset from the
 * start of the index segment file). This condition arises as a side effect
 * of serializing nodes at the same time that the
 * {@link IndexSegmentBuilder} is serializing leaves such that we can not
 * both group the nodes and leaves into distinct regions and know the
 * absolute offset to each node or leaf as it is serialized.
 * </p>
 * <p>
 * The offsets for blobs are likewise relative to the start of a
 * {@link IndexSegmentRegion#BLOB} region. The requirement for a blob region arises
 * in a similar manner: blobs are serialized during the
 * {@link IndexSegmentBuilder} operation onto a buffer and then bulk copied
 * onto the output file. This means that only the relative offset into the
 * blob region is available at the time that the blob's address is written
 * in an index entry's value.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentAddressManager extends WormAddressManager {

    /**
     * The offset within the file of the start of the node region. All nodes
     * are written densely on the file beginning at this offset. The child
     * addresses for a node are relative to this offset and are
     * automatically adjusted during decoding by this class.
     */
    protected final long offsetNodes;
    
    /**
     * #of bytes in the nodes region.
     */
    protected final int sizeNodes;
    
    /**
     * The offset within the file of the start of the blob region. All blob
     * records are written densely on the file beginning at this offset. The
     * blob addresses stored in a leaf are relative to this offset and are
     * automatically adjusted during decoding by this class.
     */
    protected final long offsetBlobs;
    
    /**
     * #of bytes in the blobs region.
     */
    protected final int sizeBlobs;
    
    /**
     * The maximum offset (aka the #of bytes in the file).
     */
    protected final long maxOffset;
    
    /**
     * @param checkpoint
     */
    public IndexSegmentAddressManager(IndexSegmentCheckpoint checkpoint) {

        super(checkpoint.offsetBits);

        /*
         * Note: we have to bootstrap these fields without relying on our
         * own getOffset(...) implementation since it has a dependency on
         * the correct initialization of these fields.
         */
        
        this.offsetNodes = IndexSegmentRegion.decodeOffset(super
                .getOffset(checkpoint.addrNodes));

        this.sizeNodes = super.getByteCount(checkpoint.addrNodes);

        this.offsetBlobs = IndexSegmentRegion.decodeOffset(super
                .getOffset(checkpoint.addrBlobs));

        this.sizeBlobs = super.getByteCount(checkpoint.addrBlobs);

        this.maxOffset = checkpoint.length;
        
    }
    
    /**
     * Decodes the offset to extract the {@link IndexSegmentRegion} and then applies
     * the appropriate offset for that region in order to convert the offset
     * into an absolute offset into the store.
     */
    final public long getOffset(long addr) {
    
        if (addr == 0L)
            return 0L;
        
        // the encoded offset (the region is encoded in this value).
        final long encodedOffset = super.getOffset(addr);
        
        // the region.
        final IndexSegmentRegion region = IndexSegmentRegion.decodeRegion(encodedOffset);
        
        // the decoded offset (relative to the region).
        long offset = IndexSegmentRegion.decodeOffset(encodedOffset);
        
        switch (region) {
        
        case BASE:
            
            // range check address.
            assert offset + getByteCount(addr) <= maxOffset : "Region="
                    + region + ", addr=" + toString(addr) + ", offset="
                    + offset + ", byteCount=" + getByteCount(addr)
                    + ", maxOffset=" + maxOffset;
            
            break;
            
        case NODE:
        
            // adjust offset.
            offset += offsetNodes;
            
            // range check address.
            assert getByteCount(addr) <= sizeNodes : "Region=" + region
                    + ", addr=" + toString(addr) + ", offset=" + offset
                    + ", byteCount=" + getByteCount(addr) + ", sizeNodes="
                    + sizeNodes;
            
            break;
            
        case BLOB:

            // adjust offset.
            offset += offsetBlobs;
            
            // range check address.
            assert getByteCount(addr) <= sizeBlobs : "Region=" + region
                    + ", addr=" + toString(addr) + ", offset=" + offset
                    + ", byteCount=" + getByteCount(addr) + ", sizeBlobs="
                    + sizeBlobs;
            
            break;
            
        default:
            throw new AssertionError();
        
        }
        
        return offset;
        
    }

    /**
     * Returns a representation of the address with the decoded offset and
     * the region to which that offset is relative.
     */
    public String toString(long addr) {
        
        if (addr == 0L) return _NULL_;
        
        final long encodedOffset = super.getOffset(addr);
        
        final int nbytes = getByteCount(addr);
        
        return "{nbytes=" + nbytes + ",offset="
                + IndexSegmentRegion.decodeOffset(encodedOffset) + ",region="
                + IndexSegmentRegion.decodeRegion(encodedOffset) + "}";

    }

}
