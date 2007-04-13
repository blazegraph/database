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

package com.bigdata.rawstore;

import java.io.File;
import java.nio.ByteBuffer;

import com.bigdata.btree.BTree;
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see Addr
 * @see Journal
 * @see BTree
 * 
 * @todo It is possible to implement this interface using an int64 address space
 *       without embedding the record length into the address. this will require
 *       a means to translate from logical addresses to physical addresses. we
 *       could use a copy-on-write btree for that purpose. delete() should be
 *       defined so that we can release storage that is no longer required for
 *       data versions that can be accessed by any current transaction
 *       (supporting gc). a free list should offer fast best-fit or good fit
 *       access to chunks in the store that are available for reuse. this would
 *       make it possible for us to implement a scale up store based on a single
 *       monolithic file.
 */
public interface IRawStore {

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

//    /**
//     * Delete the data (unisolated).
//     * <p>
//     * After this operation subsequent reads on the address MAY fail and the
//     * caller MUST NOT depend on the ability to read at that address.
//     * 
//     * @param addr
//     *            A long integer formed using {@link Addr} that encodes both the
//     *            offset at which the data was written and the #of bytes that
//     *            were written.
//     * 
//     * @exception IllegalArgumentException
//     *                If the address is known to be invalid (never written or
//     *                deleted). Note that the address 0L is always invalid.
//     * 
//     * @deprecated This method will be discarded. It is only applicable in the
//     *             context of a garbage collection strategy. With an append only
//     *             store and with eviction of btrees into index segments there
//     *             is no reason to delete anything on the store - and nothing to
//     *             keep track of the delete.
//     */
//    public void delete(long addr);
    
    /**
     * Read the data (unisolated).
     * 
     * @param addr
     *            A long integer formed using {@link Addr} that encodes both the
     *            offset from which the data will be read and the #of bytes to
     *            be read.
     * 
     * @return The data read. The buffer will be flipped to prepare for reading
     *         (the position will be zero and the limit will be the #of bytes
     *         read).
     * 
     * @exception IllegalArgumentException
     *                If the address is known to be invalid (never written or
     *                deleted). Note that the address 0L is always invalid.
     */
    public ByteBuffer read(long addr);

//    /**
//     * Read the data (unisolated).
//     * 
//     * @param addr
//     *            A long integer formed using {@link Addr} that encodes both the
//     *            offset from which the data will be read and the #of bytes to
//     *            be read.
//     * @param dst
//     *            The destination buffer (optional). This buffer will be used if
//     *            it has sufficient {@link ByteBuffer#remaining()} capacity.
//     *            When used, the data will be written into the offered buffer
//     *            starting at {@link ByteBuffer#position()} and the
//     *            {@link ByteBuffer#position()} will be advanced by the #of
//     *            bytes read.<br>
//     *            When <code>null</code> or when a buffer is offered without
//     *            sufficient {@link ByteBuffer#remaining()} capacity the
//     *            implementation is encouraged to return a read-only slice
//     *            containing the data or, failing that, to allocate a new buffer
//     *            with sufficient capacity.<br>
//     *            Note that it is not an error to offer a buffer that is too
//     *            small - it will simply be ignored.<br>
//     *            Note that it is not an error to offer a buffer with excess
//     *            remaining capacity.
//     * 
//     * @return The data read. The buffer will be flipped to prepare for reading
//     *         (the position will be zero and the limit will be the #of bytes
//     *         read).
//     * 
//     * @exception IllegalArgumentException
//     *                If the address is known to be invalid (never written or
//     *                deleted). Note that the address 0L is always invalid.
//     */
//    public ByteBuffer read(long addr, ByteBuffer dst);

    /**
     * True iff the store is open.
     * 
     * @return <code>true</code> iff the store is open.
     */
    public boolean isOpen();
    
    /**
     * Close the store immediately.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     */
    public void close();
    
    /**
     * Closes the store immediately and releases its persistent resources.
     */
    public void closeAndDelete();
    
    /**
     * The backing file -or- <code>null</code> if there is no backing file
     * for the store.
     */
    public File getFile();
    
    /**
     * True iff backed by stable storage.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     */
    public boolean isStable();
   
    /**
     * True iff the store is fully buffered (all reads are against memory).
     * Implementations MAY change the value returned by this method over the
     * life cycle of the store, e.g., to conserve memory a store may drop or
     * decrease its buffer if it is backed by disk.
     * <p>
     * Note: This does not guarentee that the OS will not swap the buffer onto
     * disk.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     */
    public boolean isFullyBuffered();
    
    /**
     * Force the data to stable storage. While this is NOT sufficient to
     * guarentee an atomic commit, the data must be forced to disk as part of an
     * atomic commit protocol.
     * 
     * @param metadata
     *            If true, then force both the file contents and the file
     *            metadata to disk.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     */
    public void force(boolean metadata);
    
    /**
     * The #of application data bytes written on the store (does not count any
     * headers or root blocks that may exist for the store).
     */
    public long size();
    
}
