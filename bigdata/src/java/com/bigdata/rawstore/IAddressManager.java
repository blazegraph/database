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


/**
 * An interface that encapsulates operations on opaque identifiers used to
 * locate data within an {@link IRawStore}.
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
     * The length of the datum in bytes. This must be the actual length of the
     * record on the disk, not the length of the caller's byte[]. This is
     * necessary in order to support transparent checksums and/or compression
     * for records in the {@link IRawStore}.
     * 
     * @param addr
     *            The opaque identifier that is the within store locator for
     *            some datum.
     * 
     * @return The offset of that datum.
     */
    public int getByteCount(long addr);

//    /**
//     * Pack the address onto the output stream.
//     * 
//     * @param out
//     *            The output stream.
//     * 
//     * @param addr
//     *            The opaque identifier that is the within store locator for
//     *            some datum.
//     * 
//     * @deprecated Address packing is not currently used by anything. While it
//     *             can produce somewhat more compact representations, it is
//     *             problematic in that it requires an awareness of the
//     *             {@link IAddressManager} at the point in the code where the
//     *             address is to be decoded. This is not always feasible and
//     *             other techniques are available to code the addresses more
//     *             efficiently, for example, the child addresses of a
//     *             {@link Node}.
//     */
//    public void packAddr(DataOutput out, long addr) throws IOException;
//    
//    /**
//     * Unpack the address from the input stream.
//     * 
//     * @param in
//     *            The input stream.
//     * 
//     * @return The opaque identifier that is the within store locator for some
//     *         datum.
//     * 
//     * @deprecated Address packing is not currently used by anything. While it
//     *             can produce somewhat more compact representations, it is
//     *             problematic in that it requires an awareness of the
//     *             {@link IAddressManager} at the point in the code where the
//     *             address is to be decoded. This is not always feasible and
//     *             other techniques are available to code the addresses more
//     *             efficiently, for example, the child addresses of a
//     *             {@link Node}.
//     */
//    public long unpackAddr(DataInput in) throws IOException;

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
