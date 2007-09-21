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
/*
 * Created on Sep 4, 2007
 */

package com.bigdata.rawstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.bigdata.btree.IndexSegmentBuilder;

/**
 * An interface that encapsulates operations on opaque identifiers used to
 * locate data within an {@link IRawStore}.
 * 
 * @todo consider address segments to support fast combination of buffers each
 *       containing its own address space. A prime candidate for this is the
 *       {@link IndexSegmentBuilder} which currently jumps through hoops in
 *       order to make the nodes resolvable. When considering segments, note
 *       that addresses may currently be directly tested for order since the
 *       offset is in the high int32 word.
 *       
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAddressManager {

    /**
     * A null reference (0L).
     * <P>
     * Note: It is a requirement that all implementations understand the value
     * <code>0L</code> as a null reference.
     */
    public static final long NULL = 0L;
    
    /**
     * Converts a byte count and offset into a long integer.
     * 
     * @param nbytes
     *            The byte count.
     * @param offset
     *            The byte offset.
     * 
     * @return The long integer.
     * 
     * @exception IllegalArgumentException
     *                if the byte count is larger than can be represented by the
     *                address manager.
     * 
     * @exception IllegalArgumentException
     *                if the byte offset is larger than can be represented by
     *                the address manager.
     */
    public long toAddr(int nbytes,long offset);

    /**
     * The offset on the store at which the datum is stored. While this is often
     * the offset of a byte into a file on disk, implementations MAY assign
     * offsets using other strategies.
     * 
     * @param addr
     *            The opaque identifier that is the within store locator for
     *            some datum.
     * 
     * @return The offset of that datum.
     */
    public long getOffset(long addr);

    /**
     * The length of the datum in bytes.
     * 
     * @param addr
     *            The opaque identifier that is the within store locator for
     *            some datum.
     *            
     * @return The offset of that datum.
     */
    public int getByteCount(long addr);

    /**
     * Pack the address onto the output stream.
     * 
     * @param out
     *            The output stream.
     * 
     * @param addr
     *            The opaque identifier that is the within store locator for
     *            some datum.
     */
    public void packAddr(DataOutput out, long addr) throws IOException;
    
    /**
     * Unpack the address from the input stream.
     * 
     * @param in
     *            The input stream.
     * 
     * @return The opaque identifier that is the within store locator for some
     *         datum.
     */
    public long unpackAddr(DataInput in) throws IOException;

//    /**
//     * Pack a vector of addresses onto the output stream.
//     * 
//     * @param out
//     *            The output stream.
//     * @param addrs
//     *            The vector of addresses.
//     * @param off
//     *            The index of the first address to pack.
//     * @param len
//     *            The #of addresses to pack.
//     */
//    public void packAddr(DataOutput out, long[] addrs, int off, int len)
//            throws IOException;
//    
//    /**
//     * Unpack a vector of addresses from the input stream onto the provided
//     * array.
//     * 
//     * @param out
//     *            The input stream.
//     * @param addrs
//     *            The array onto which the unpacked addresses will be written.
//     * @param off
//     *            The index into the array at which the first address will be
//     *            written.
//     * @param len
//     *            The #of addresses to unpack.
//     */
//    public void unpackAddr(DataInput in, long[] addrs, int off, int len)
//            throws IOException;
    
    /**
     * A human readable representation of the address.
     * 
     * @param addr
     *            The opaque identifier that is the within store locator for
     *            some datum.
     * 
     * @return A human readable representation.
     */
    public String toString(long addr);

}
