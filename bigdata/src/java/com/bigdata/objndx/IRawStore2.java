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
     *            The data. The bytes from the current
     *            {@link ByteBuffer#position()} to the
     *            {@link ByteBuffer#limit()} will be written and the
     *            {@link ByteBuffer#position()} will be advanced to the
     *            {@link ByteBuffer#limit()} . The caller may subsequently
     *            modify the contents of the buffer without changing the state
     *            of the store (i.e., the data are copied into the store).
     * 
     * @return A long integer formed using {@link Addr} that encodes both the
     *         offset from which the data may be read and the #of bytes to be
     *         read.
     * 
     * @exception IllegalArgumentException
     *                if <i>data</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if <i>data</i> has zero bytes
     *                {@link ByteBuffer#remaining()}.
     * 
     * @todo the addresses need to reflect the ascending offset at which the
     *       data are written, at least for a class of append only store. some
     *       stores, such as the Journal, also have an offset from the start of
     *       the file to the start of the data region (in the case of the Journal
     *       it is used to hold the root blocks).
     */
    public long write(ByteBuffer data);
    
    /**
     * Delete the data (unisolated).
     * <p>
     * After this operation subsequent reads on the address MAY fail and the
     * caller MUST NOT depend on the ability to read at that address.
     * 
     * @param addr
     *            A long integer formed using {@link Addr} that encodes both the
     *            offset at which the data was written and the #of bytes that
     *            were written.
     * 
     * @exception IllegalArgumentException
     *                If the address is known to be invalid (never written or
     *                deleted). Note that the address 0L is always invalid.
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
     *            The destination buffer (optional). This buffer will be used if
     *            it has sufficient {@link ByteBuffer#remaining()} capacity.
     *            When used, the data will be written into the offered buffer
     *            starting at {@link ByteBuffer#position()} and the
     *            {@link ByteBuffer#position()} will be advanced by the #of
     *            bytes read.<br>
     *            When <code>null</code> or when a buffer is offered without
     *            sufficient {@link ByteBuffer#remaining()} capacity the
     *            implementation is encouraged to return a read-only slice
     *            containing the data or, failing that, to allocate a new buffer
     *            with sufficient capacity.<br>
     *            Note that it is not an error to offer a buffer that is too
     *            small - it will simply be ignored.<br>
     *            Note that it is not an error to offer a buffer with excess
     *            remaining capacity.
     * 
     * @return The data read. The buffer will be flipped to prepare for reading
     *         (the position will be zero and the limit will be the #of bytes
     *         read).
     * 
     * @exception IllegalArgumentException
     *                If the address is known to be invalid (never written or
     *                deleted). Note that the address 0L is always invalid.
     */
    public ByteBuffer read(long addr, ByteBuffer dst);

    /**
     * True iff the store is open.
     * 
     * @return <code>true</code> iff the store is open.
     */
    public boolean isOpen();
    
    /**
     * Closes the store without invoking {@link #commit()}. Any uncommitted
     * changes will be lost.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     */
    public void close();

    /**
     * Request an atomic commit.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     * @exception IllegalStateException
     *                if the store is not writable.
     */
    public void commit();

    /**
     * Register a persistence capable data structure for callback during the
     * commit protocol.
     * 
     * @param rootSlot
     *            The slot in the root block where the {@link Addr address} of
     *            the {@link ICommitter} will be recorded.
     * 
     * @param committer
     *            The commiter.
     * 
     * @todo for named btrees, there will be one btree that registers with this
     *       method and the named btrees will register with that btree rather
     *       than directly with the store.
     */
    public void registerCommitter(int rootSlot, ICommitter committer);

    /**
     * The last address stored in the specified root slot as of the last
     * committed state of the store.
     * 
     * @param rootSlot
     *            The root slot identifier.
     *            
     * @return The {@link Addr address} written in that slot.
     */
    public long getAddr(int rootSlot);
    
    /**
     * An interface implemented by a persistence capable data structure such as
     * a btree so that it can participate in the commit protocol for the store.
     * <p>
     * This interface is invoked by {@link #commit()} for each registered
     * committer. The {@link Addr} returned by {@link #commit()} will be saved
     * on the root block in the slot identified to the committer when it
     * registered itself.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see IRawStore2#registerCommitter(int,
     *      com.bigdata.objndx.IRawStore2.ICommitter)
     * 
     * @todo Note that {@link Addr}s are assigned in strict sequence.
     *       Therefore even in the face of concurrent writers, the last
     *       {@link Addr} written by a given transactional context is the
     *       necessary and sufficient limit on the data written on the store
     *       that must be stable for the commit to be valid.
     */
    public static interface ICommitter {

        /**
         * Flush all dirty records to disk in preparation for an atomic commit.
         * 
         * @return The {@link Addr address} of the record from which the
         *         persistence capable data structure can load itself. If no
         *         changes have been made then the previous address should be
         *         returned as it is still valid.
         */
        public long commit();
        
    }
    
}
