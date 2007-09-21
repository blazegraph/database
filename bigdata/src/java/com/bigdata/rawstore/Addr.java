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
