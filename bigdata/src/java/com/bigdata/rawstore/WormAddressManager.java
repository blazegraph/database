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
/*
 * Created on Sep 4, 2007
 */

package com.bigdata.rawstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.text.NumberFormat;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.IndexSegmentAddressManager;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * Encapsulates logic for operations on an opaque long integer comprising an
 * byte offset and a byte count suitable for use in a WORM (Write Once, Read
 * Many) {@link IRawStore}. Both the byte offset and the byte count of the
 * record are stored directly in the opaque identifier. Note that the maximum
 * byte offset only indirectly governs the maximum #of records that can be
 * written on a store - the maximum #of records also depends on the actual
 * lengths of the individual records that are written since the byte offset of
 * the next record increases by the size of the last record written.
 * <p>
 * The constructor defines where the split is between the high and the low bits
 * and therefore the maximum byte offset and the maximum byte count. This allows
 * an {@link IRawStore} implementation to parameterize its handling of addresses
 * to trade off the #of distinct offsets at which it can store records against
 * the size of those records.
 * <p>
 * The offset is stored in the high bits of the long integer while the byte
 * count is stored in the low bits. This means that two addresses encoded by an
 * {@link WormAddressManager} with the same split point can be placed into a
 * total ordering by their offset without being decoded.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WormAddressManager implements IAddressManager {

    /**
     * Used to represent a null reference by {@link #toString(long)}.
     */
    protected static final String _NULL_ = "NULL";

    /**
     * The minimum #of bits that may be used to encode an offset as an unsigned
     * integer (31). This value MUST be used when the {@link IRawStore}
     * implementation is backed by an in-memory array since an array index may
     * not have more than 31 unsigned bits (the equivalent of 32 signed bits).
     */
    public static final int MIN_OFFSET_BITS = 31;
    
    /**
     * The maximum #of bits that may be used to encode an offset (this leaves 4
     * bits for the byte count, so the maximum record size is only 16 bytes).
     * This is not a useful record size for BTree data, but it might be useful
     * for some custom data structures.
     */
    public static final int MAX_OFFSET_BITS = 60;
    
    /**
     * The #of offset bits that allows byte offsets of up to 4,398,046,511,103
     * (4 terabytes minus one) and a maximum record size of 4,194,303 (4
     * megabytes minus one).
     * <p>
     * This is a good value when deploying a scale-up solution. For the scale-out
     * deployment scenario you will have monolithic indices on a journal,
     * providing effectively a WORM or "immortal" database. It is highly
     * unlikely to have records as large as 4M with the modest branching factors
     * used on a BTree backed by a Journal. At the same time, 4T is as much
     * storage as we could reasonable expect to be able to address on a single
     * file system.
     */
    public static final int SCALE_UP_OFFSET_BITS = 42;
    
    /**
     * The #of offset bits that must be used in order to support 64M (67,108,864
     * bytes) blobs (38).
     * <p>
     * This is a good value when deploying a scale-out solution. For the
     * scale-out deployment scenario you will have key-range partitioned
     * indices automatically distributed among data services available on a
     * cluster. The journal files are never permitted to grow very large for
     * scale-out deployments. Instead, the journal periodically overflows,
     * generating index segments which capture historical views. The larger
     * record size (64M) also supports the distributed repository and map/reduce
     * processing models.
     */
    public static final int SCALE_OUT_OFFSET_BITS = 38;
    
    /**
     * The #of bits allocated to the byte offset (this is the sole input to the
     * constructor).
     */
    final int offsetBits;
    
    /**
     * The #of bits allocate to the byte count (64 - {@link #offsetBits}).
     */
    final int byteCountBits;
    
    /**
     * The maximum offset that can be stored in a 64-bit integer given the
     * {@link #offsetBits}.
     */
    final long maxOffset;
    
    /**
     * The maximum byte count that can be stored in a 64-bit integer given the
     * {@link #byteCountBits}.
     * <p>
     * Note that this is a long since we are treating the stored values as
     * _unsigned_ so 32-bits can actually store a larger value than can be
     * represented in a 32-bit signed integer. Of course, the API declares the
     * byte count as an <code>int</code> so you can not actually have a byte
     * count that exceeds {@link Integer#MAX_VALUE}.
     */
    final long maxByteCount;
    
    /**
     * The mask that is used to extract the offset from the long integer.
     */
    final long offsetMask;
    
    /**
     * The mask that is used to extract the byte count from the long integer.
     */
    final long byteCountMask;

    /**
     * Return the #of bits that are allocated to the offset.
     */
    final public int getOffsetBits() {
        
        return offsetBits;
        
    }

    /**
     * The maximum byte offset that may be represented.
     */
    final public long getMaxOffset() {

        return maxOffset;
        
    }
    
    /**
     * The maximum byte count that may be represented.
     */
    final public int getMaxByteCount() {
        
        if(maxByteCount > Integer.MAX_VALUE ) {
            
            // limit imposed by the API.
            return Integer.MAX_VALUE;
            
        } else {
        
            return (int) maxByteCount;
            
        }

    }

    /**
     * Range checks the #of offset bits.
     * 
     * @param offsetBits
     *            The #of offset bits.
     * 
     * @exception IllegalArgumentException
     *                if the parameter is out of range.
     * 
     * @return true otherwise.
     */
    final public static boolean assertOffsetBits(final int offsetBits) {
        
        if (offsetBits < MIN_OFFSET_BITS || offsetBits > MAX_OFFSET_BITS) {
            
            throw new IllegalArgumentException("offsetBits must be in ["
                    + MIN_OFFSET_BITS + ":" + MAX_OFFSET_BITS + "], not: "
                    + offsetBits);

        }
        
        return true;
        
    }
    
    /**
     * Compute the maximum byte count (aka record size) allowed for a given #of
     * bits dedicated to the byte offset.
     * 
     * @param offsetBits
     *            The #of bits to be used to represent the byte offset.
     * 
     * @return The maximum byte count that can be represented.
     */
    final public static int getMaxByteCount(final int offsetBits) {

        assertOffsetBits( offsetBits );
        
        final int byteCountBits = 64 - offsetBits;

        final long maxByteCount = BigInteger.valueOf(2).pow(byteCountBits).longValue();
        
//        final long maxByteCount = ((Double)Math.pow(2,byteCountBits)).longValue();
        
        if(maxByteCount > Integer.MAX_VALUE ) {
            
            // limit imposed by the API.
            return Integer.MAX_VALUE;
            
        } else {
        
            return (int) maxByteCount;
            
        }
        
    }
    
//    /**
//     * Allows byte offsets of up to 4T and record lengths of up to 4M (it
//     * allocates {@link #DEFAULT_OFFSET_BITS} to the offset).
//     */
//    protected WormAddressManager() {
//
//        this( DEFAULT_OFFSET_BITS );
//        
//    }
    
    /**
     * Construct an {@link IAddressManager} that will allocate a specified #of
     * bits to the offset and use the remaining bits for the byte count
     * component.
     * 
     * @param offsetBits
     *            An integer defining how many bits will be used for the offset
     *            component and thereby determines the maximum #of records that
     *            may be stored. The remaining bits are used for the byte count,
     *            so this indirectly determines the maximum #of bytes that may
     *            be stored in a record.
     */
    public WormAddressManager(final int offsetBits) {
        
        assertOffsetBits( offsetBits );
        
        this.offsetBits = offsetBits;
        
        this.byteCountBits = 64 - offsetBits;

        /*
         * Construct the byte count bit mask - this will have zeros in the high
         * bits that correspond to the offset and ones in the low bits that
         * correspond to the byte count.
         */
        {

            long mask = 0;
            
            long bit;
            
            for (int i = 0; i < byteCountBits; i++) {

                bit = (1L << i);
                
                mask |= bit;
                
            }

            this.byteCountMask = mask;
            
        }

        /*
         * The offset bit mask is the complement of the byte count bit mask. It
         * has ones in the high bits that correspond to the offset and zeros in
         * the low bits that correspond to the byte count.
         */
        this.offsetMask = ~byteCountMask;

        /*
         * The offset mask shifted down to get rid of the trailing zeros is the
         * maximum value for an offset.
         */
        
        this.maxOffset = offsetMask >>> byteCountBits;
        
        // sanity check.
        assert maxOffset > 0L;

        /*
         * The byte count mask is also the maximum value for a byte count.
         */

        this.maxByteCount = byteCountMask;
        
        // sanity check.
        assert maxByteCount > 0L;

//        System.err.println(this.toString());
        
    }

    /**
     * Range check the byte count.
     * 
     * @param nbytes
     *            The byte count.
     * 
     * @exception IllegalArgumentException
     *                if the byte count is out of range.
     * 
     * @return true otherwise.
     */
    public final boolean assertByteCount(final int nbytes) {

        if (nbytes < 0 || nbytes > maxByteCount) {

            throw new IllegalArgumentException(
                    "Maximum record length exceeded: nbytes must be in [0:"
                            + maxByteCount + "], but was " + nbytes);
            
        }
        
        return true;
        
    }

    /**
     * Range check the byte offset.
     * 
     * @param offset
     *            The byte offset.
     * 
     * @exception IllegalArgumentException
     *                if the offset is out of range.
     * 
     * @return true otherwise.
     */
    public final boolean assertOffset(final long offset) {

        if (offset < 0 || offset > maxOffset) {

            throw new IllegalArgumentException(
                    "Maximum offset exceeded: offset must be in [0:"
                            + maxOffset + "], but was " + offset);
            
        }
        
        return true;

    }

    final public long toAddr(final int nbytes, final long offset) {

        // range check the byte count.
        /*assert*/assertByteCount(nbytes);
        
        // range check the offset.
        /*assert*/ assertOffset(offset);
        
//        if(nbytes==0 && offset==0L) {
//            
//            throw new IllegalArgumentException("Offset and byte count are both null.");
//            
//        }
        
        return ((long) offset) << byteCountBits | nbytes ;
        
    }
    
    final public int getByteCount(final long addr) {

        return (int) (byteCountMask & addr);

    }

    /**
     * Note: overridden by {@link IndexSegmentAddressManager}.
     */
    public long getOffset(final long addr) {

        return (offsetMask & addr) >>> byteCountBits;

    }

//    /**
//     * Breaks an address into its offset and size and packs each component
//     * separately. This provides much better packing then writing the entire
//     * address as a long integer since each component tends to be a small
//     * positive integer value. When the byte count will fit into a non-negative
//     * short integer, it is packed as such for better storage efficiency.
//     * 
//     * @param os
//     *            The output stream.
//     * 
//     * @param addr
//     *            The opaque identifier that is the within store locator for
//     *            some datum.
//     * 
//     * @throws IOException
//     */
//    final public void packAddr(final DataOutput os,final long addr) throws IOException {
//        
//        final long offset = getOffset(addr);
//        
//        final int nbytes = getByteCount(addr);
//
//        if(os instanceof DataOutputBuffer){
//        
//            DataOutputBuffer buf = (DataOutputBuffer)os;
//            
//            buf.packLong(offset);
//
//            if (byteCountBits <= 15) {
//
//                /*
//                 * 15 unsigned bits can be packed as a non-negative short integer.
//                 */
//
//                buf.packShort((short)nbytes);
//                
//            } else {
//
//                /*
//                 * Otherwise use the long integer packer.
//                 */
//                
//                buf.packLong(nbytes);
//                
//            }
//            
//        } else {
//            
//            LongPacker.packLong(os, offset);
//
//            if (byteCountBits <= 15) {
//
//                /*
//                 * 15 unsigned bits can be packed as a non-negative short integer.
//                 */
//
//                ShortPacker.packShort(os, (short)nbytes);
//                
//            } else {
//
//                /*
//                 * Otherwise use the long integer packer.
//                 */
//                
//                LongPacker.packLong(os, nbytes);
//                
//            }
//            
//        }
//        
//    }
//    
//    final public long unpackAddr(final DataInput is) throws IOException {
//    
//        final long offset;
//
//        final int nbytes;
//
//        if(is instanceof DataInputBuffer) {
//
//            DataInputBuffer in = (DataInputBuffer)is;
//            
//            offset = in.unpackLong();
//            
//            if (byteCountBits <= 15) {
//
//                nbytes = in.unpackShort();
//                
//            } else {
//
//                final long v = in.unpackLong();
//
//                assert v <= Integer.MAX_VALUE;
//            
//                nbytes = (int) v;
//                
//            }
//
//        } else {
//            
//            offset = LongPacker.unpackLong(is);
//            
//            if (byteCountBits <= 15) {
//
//                nbytes = ShortPacker.unpackShort(is);
//
//            } else {
//
//                final long v = LongPacker.unpackLong(is);
//
//                assert v <= Integer.MAX_VALUE;
//            
//                nbytes = (int) v;
//                
//            }
//
//        }
//
//        return toAddr(nbytes, offset);
//        
//    }

    public String toString(final long addr) {
        
        if(addr==0L) return _NULL_;
        
        final long offset = getOffset(addr);
        
        final int nbytes = getByteCount(addr);
        
        return "{off="+offset+",len="+nbytes+"}";
//        return "{nbytes="+nbytes+",offset="+offset+"}";
        
    }

    /**
     * A human readable representation of the state of the
     * {@link WormAddressManager}.
     */
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(super.toString());
        
        sb.append("{ offsetBits="+offsetBits);
        
        sb.append(", byteCountBits="+byteCountBits);
        
        sb.append(", maxOffset="+Long.toHexString(maxOffset));

        sb.append(", maxByteCount="+Long.toHexString(maxByteCount));
                
        sb.append(", offsetMask="+Long.toHexString(offsetMask));

        sb.append(", byteCountMask="+Long.toHexString(byteCountMask));
                
        sb.append("}");
        
        return sb.toString();
        
    }
    
    /**
     * Displays a table of offset bits and the corresponding maximum byte offset
     * and maximum byte count (aka record size) that a store may address for a
     * given #of offset bits. This table may be used to choose how to
     * parameterize the {@link WormAddressManager} and hence a {@link IRawStore}
     * using that {@link WormAddressManager} so as to best leverage the 64-bit long
     * integer as a persistent locator into the store.
     * 
     * @param args
     *            unused.
     */
    public static void main(final String[] args) {

        final NumberFormat nf = NumberFormat.getInstance();
        
        nf.setGroupingUsed(true);
        
        System.out.println("#offsetBits\tmaxOffset\tmaxByteCount");

        for (int offsetBits = MIN_OFFSET_BITS; offsetBits <= MAX_OFFSET_BITS; offsetBits++) {
            
            final WormAddressManager am = new WormAddressManager( offsetBits );
            
            final long maxRecords = am.getMaxOffset();
            
            final int maxRecordSize = am.getMaxByteCount();
            
            System.out.println("" + offsetBits + "\t" + nf.format(maxRecords)
                    + "\t" + nf.format(maxRecordSize));
            
        }
        
    }

}
