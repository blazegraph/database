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
 * Created on Dec 19, 2006
 */

package com.bigdata.rawstore;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.LRUNexus;
import com.bigdata.btree.BTree;
import com.bigdata.cache.IGlobalLRU;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.mdi.IResourceMetadata;

/**
 * <p>
 * A low-level interface for reading and writing data. This interface is not
 * isolated and operations do not possess ACID semantics. Implementations may or
 * may not be durable. All operations are expressed in terms of long integers
 * that are often called "addresses" but which should be understood as opaque
 * identifiers.
 * </p>
 * <p>
 * The {@link AbstractJournal} is the principal implementation of this interface
 * and provides both transient and durable options and the facilities for atomic
 * commit. {@link BTree} provides a higher level interface for operations on an
 * {@link IRawStore} and uses a copy-on-write policy designed to support
 * transactional semantics by making it possible to read from a consistent
 * historical state. The {@link AbstractJournal} provides the necessary
 * mechanisms to support transactions based on the copy-on-write semantics of
 * the {@link BTree}.
 * </p>
 * <p>
 * The {@link IRawStore} supports write and read back on immutable addresses and
 * does NOT directly support update of data for an immutable address once that
 * data has been written. Applications seeking Create, Read, Update, Delete
 * (CRUD) semantics should use a {@link BTree} to store their application data.
 * Unlike this interface, the {@link BTree} can correctly support update and
 * delete of application data using a persistent identifier (the key).
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see AbstractJournal
 * @see BTree
 * 
 * @todo The existing implementations of this interface do NOT support the
 *       release and reuse of allocated records - that is, they are all
 *       {@link IWORM Write Once Read Many} stores rather than read/write
 *       stores. A read-write implementation of this interface is of course
 *       possible and will require a means to allocate, release, and reallocate
 *       chunks of varying sizes within the store. Those chunks may either be
 *       drawn from a set of fixed sized allocation pools spanning a variety of
 *       useful record sizes or can use a strategy where records are allocated on
 *       an exact fit basis the first time and reallocated on a best/good fit
 *       basis. A free list can offer fast best-fit or good fit access to chunks
 *       in the store that are available for reuse. Any such approach must
 *       serialize allocations by one means or another, even though different
 *       allocation pools might be used concurrently. A delete() operation on
 *       {@link IRawStore} should be defined so that we can release storage that
 *       is no longer required for data versions that can not be accessed by any
 *       current transaction (a gc strategy). This would make it possible for us
 *       to implement a scale up store based on a single monolithic file.
 * 
 *       FIXME The use of the {@link ByteBuffer} in this API has become
 *       limiting. It would be nicer to specify a byte[] with optional offset
 *       and length parameters. This would make it easier to wrap the byte[] in
 *       different ways for reading and writing. The {@link ByteBuffer} has an
 *       advantage when reading since it allows us to return a read-only view of
 *       the record, but in practice that is only effective for an in-memory
 *       store (with or without a backing file). The byte[] is more flexible.
 *       The caller is free to reuse the byte[] and the store always copies the
 *       data.
 */
public interface IRawStore extends IAddressManager {
    
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
     * @return A long integer formed that encodes both the offset from which the
     *         data may be read and the #of bytes to be read. See
     *         {@link IAddressManager}.
     * 
     * @throws IllegalArgumentException
     *             if <i>data</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>data</i> has zero bytes {@link ByteBuffer#remaining()}.
     * @throws IllegalStateException
     *             if the store is not open.
     * @throws IllegalStateException
     *             if the store does not allow writes.
     * 
     * @todo define exception if the maximum extent would be exceeded.
     * 
     * @todo the addresses need to reflect the ascending offset at which the
     *       data are written, at least for a class of append only store. some
     *       stores, such as the Journal, also have an offset from the start of
     *       the file to the start of the data region (in the case of the
     *       Journal it is used to hold the root blocks).
     */
    public long write(ByteBuffer data);
    
    /**
     * 
     * @param data
     *            The data. The bytes from the current
     *            {@link ByteBuffer#position()} to the
     *            {@link ByteBuffer#limit()} will be written and the
     *            {@link ByteBuffer#position()} will be advanced to the
     *            {@link ByteBuffer#limit()} . The caller may subsequently
     *            modify the contents of the buffer without changing the state
     *            of the store (i.e., the data are copied into the store).
     * @param oldAddr as returned from a previous write of the same object, or zero if a new write
     * 
     * @return  A long integer formed that encodes both the offset from which the
     *         data may be read and the #of bytes to be read. See
     *         {@link IAddressManager}.
     */
    public long write(ByteBuffer data, long oldAddr);

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
     * 
     * It is only applicable in the
     * context of a garbage collection strategy. With an append only
     * store and with eviction of btrees into index segments there
     * is no reason to delete anything on the store - and nothing to
     * keep track of the delete.
     * 
     * However, with a Read-Write store it is a requirement, and a void
     * implementation is provided for other stores.
     */
    public void delete(long addr);
    
    /**
     * Read the data (unisolated).
     * 
     * @param addr
     *            A long integer that encodes both the offset from which the
     *            data will be read and the #of bytes to be read. See
     *            {@link IAddressManager#toAddr(int, long)}.
     * 
     * @return The data read. The buffer will be flipped to prepare for reading
     *         (the position will be zero and the limit will be the #of bytes
     *         read).
     * 
     * @throws IllegalArgumentException
     *                If the address is known to be invalid (never written or
     *                deleted). Note that the address 0L is always invalid.
     * @throws IllegalStateException
     *             if the store is not open.
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
     * <code>true</code> iff the store is open.
     * 
     * @return <code>true</code> iff the store is open.
     */
    public boolean isOpen();
    
    /**
     * <code>true</code> iff the store does not allow writes.
     * 
     * @throws IllegalStateException
     *             if the store is not open.
     */
    public boolean isReadOnly();

    /**
     * Close the store immediately, but does not clear any records for the store
     * from the {@link IGlobalLRU}.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     */
    public void close();

    /**
     * Deletes the backing file(s) (if any) and clears any records for the store
     * from the {@link IGlobalLRU}.
     * 
     * @exception IllegalStateException
     *                if the store is open.
     * 
     * @exception RuntimeException
     *                if the backing file exists and could not be deleted.
     */
    public void deleteResources();

    /**
     * Closes the store immediately, deletes its persistent resources, and
     * clears any records for the store from the {@link IGlobalLRU}.
     * 
     * @see #deleteResources()
     */
    public void destroy();
    
    /**
     * The backing file -or- <code>null</code> if there is no backing file
     * for the store.
     */
    public File getFile();

    /**
     * Return the {@link UUID} which identifies this {@link IRawStore}. This
     * supports both {@link #getResourceMetadata()} and the {@link LRUNexus}.
     */
    public UUID getUUID();

    /**
     * A description of this store in support of the scale-out architecture.
     */
    public IResourceMetadata getResourceMetadata();
    
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
     * Note: This does not guarantee that the OS will not swap the buffer onto
     * disk.
     * 
     * @exception IllegalStateException
     *                if the store is not open.
     */
    public boolean isFullyBuffered();
    
    /**
     * Force the data to stable storage. While this is NOT sufficient to
     * guarantee an atomic commit, the data must be forced to disk as part of an
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

    /**
     * Reports performance counters.
     */
    public CounterSet getCounters();
    
}
