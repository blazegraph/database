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
 * Created on Dec 19, 2006
 */

package com.bigdata.objndx;

import java.nio.ByteBuffer;

import com.bigdata.journal.IRawStore;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.Journal;

/**
 * <p>
 * A low-level interface for reading and writing data. This interface is not
 * isolated and operations do not possess ACID semantics. Implementations may or
 * may not be durable. All operations are expressed in terms of long integers,
 * constructed and examined using {@link Addr}, that encode both the offset on
 * the store at which the data exists and the length of the data in bytes.
 * </p>
 * <p>
 * The {@link Journal} is the principle implementation of this interface and
 * provides both transient and durable options and the facilities for atomic
 * commit. {@link BTree} provides a higher level interface for operations on a
 * {@link Journal}. The {@link BTree} uses a copy-on-write policy designed to
 * support transactional semantics by making it possible to read from a
 * consistent historical state.
 * </p>
 * 
 * FIXME This is a transitional interface bridging {@link IRawStore} based on
 * fixed size slot allocations to a new interface drops the concept of
 * {@link ISlotAllocation} and encodes the #of bytes (vs #of slots) directly
 * into the long {@link Addr}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see Addr
 * @see IRawStore
 * @see Journal
 * @see BTree
 */
public interface IRawStore2 {

    /**
     * Write the data (unisolated).
     * 
     * @param data
     *            The data.
     * 
     * @return A long integer formed using {@link Addr} that encodes both the
     *         offset from which the data may be read and the #of bytes to be
     *         read.
     * 
     * @todo document whether the buffer position is advanced to the limit or
     *       whether it is unmodified by this call.
     */
    public long write(ByteBuffer data);
    
    /**
     * Delete the data (unisolated).
     * 
     * @param addr
     *            A long integer formed using {@link Addr} that encodes both the
     *            offset at which the data was written and the #of bytes that
     *            were written.
     */
    public void delete(long addr);
    
    /**
     * Read the data (unisolated).
     * 
     * @param addr
     *            A long integer formed using {@link Addr} that encodes both the
     *            offset from which the data will be read and the #of bytes to
     *            be read.
     * @param dst
     *            The destination buffer (optional). When <code>null</code> or
     *            too small then a new buffer MAY be allocated. However, an
     *            implementation is encouraged to return a read-only slice
     *            containing the data.
     * 
     * @return The data read. The buffer will be flipped to prepare for reading
     *         (the position will be zero and the limit will be the #of bytes
     *         read).
     */
    public ByteBuffer read(long addr, ByteBuffer dst);

}
