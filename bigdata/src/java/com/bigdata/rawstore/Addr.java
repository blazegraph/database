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
package com.bigdata.rawstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.io.SerializerUtil;

/**
 * An address encodes both an int32 length and an int32 offset into a single
 * long integer. This limits the addressable size of a file to int32 bytes
 * (actually, only 2^31 bytes, e.g., 2G, since Java is using signed integers),
 * but that limit far exceeds the envisoned capacity of a single file in the
 * bigdata architecture. Note that the long integer ZERO (0L) is reserved and
 * always has the semantics of a <em>null</em> reference. Writes at offset
 * zero are allowed, depending on the store, by writes of zero length are
 * disallowed and hence no address will ever be ZERO (0L).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This is now an interface that is extended by {@link IRawStore}
 *             such that each store may be provisioned for a different split
 *             between the bits dedicated to the offset and the bits dedicated
 *             to the length of the data record. Only {@link #toLong(int, int)},
 *             {@link #getOffset(long)}, and {@link #getByteCount(long)} will
 *             need to become instance methods. The metadata required to recover
 *             the provisioned bit split must be stored in the root block of a
 *             persistence store so that it may be recovered without record to
 *             decoding an {@link Addr}. (This is also going to cause a lot of
 *             javadoc comments to refer to the "Addr" interface, which will be
 *             the base interface for {@link IRawStore} and that will be
 *             confusing and should be cleaned up.)
 *             <p>
 *             Other consequences are that offset and nextOffset are always long
 *             integers and that packing and unpacking of addresses requires
 *             knowledge of the #offset bits (so {@link SerializerUtil} should
 *             not be used for those cases). This change to serialization also
 *             effects how btree nodes and leaves are stored.
 */
final public class Addr {

    /**
     * A null reference (0L).
     */
    public static final long NULL = 0L;
    
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
     * A human readable representation showing the offset and length components
     * of the address.
     * 
     * @param addr
     *            An address.
     * 
     * @return The representation.
     */
    public static String toString(long addr) {
        
        if(addr==0L) return "NULL";
        
        int offset = getOffset(addr);
        
        int nbytes = getByteCount(addr);
        
        return "{nbytes="+nbytes+",offset="+offset+"}";
        
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
 
    /**
     * Breaks an {@link Addr} into its offset and size and packs each component
     * separately. This provides much better packing then packing the entire
     * {@link Addr} as a long integer since each component tends to be a small
     * positive integer value.
     * 
     * @param os The output stream.
     * 
     * @param addr The {@link Addr}.
     * 
     * @throws IOException
     */
    public static void pack(DataOutput os,long addr) throws IOException {
        
        final int offset = Addr.getOffset(addr);
        
        final int nbytes = Addr.getByteCount(addr);
        
        LongPacker.packLong(os, offset);
        
        LongPacker.packLong(os, nbytes);
        
    }
    
    /**
     * Unpacks an {@link Addr}.
     * 
     * @param is The input stream.
     * 
     * @return The addr.
     * 
     * @throws IOException
     */
    public static long unpack(DataInput is) throws IOException {
    
        long v = LongPacker.unpackLong(is);
        
        assert v <= Integer.MAX_VALUE;
        
        final int offset = (int) v;

        v = LongPacker.unpackLong(is);
        
        assert v <= Integer.MAX_VALUE;
        
        final int nbytes = (int) v;
        
        return Addr.toLong(nbytes, offset);
        
    }

}
