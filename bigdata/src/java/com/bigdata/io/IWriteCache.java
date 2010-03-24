/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Feb 10, 2010
 */

package com.bigdata.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.journal.IAtomicStore;

/**
 * Interface for a write cache with read back and the capability to update
 * records while they are still in the cache.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IWriteCache {

    /**
     * Write the records on the cache. This interface DOES NOT provide any
     * guarantee about the ordering of writes. Callers who require a specific
     * ordering must coordinate that ordering themselves, e.g., by synchronizing
     * across their writes onto the cache.
     * 
     * @param offset
     *            The offset of that record (maybe relative to a base offset).
     * @param data
     *            The record. The bytes from the current
     *            {@link ByteBuffer#position()} to the
     *            {@link ByteBuffer#limit()} will be written and the
     *            {@link ByteBuffer#position()} will be advanced to the
     *            {@link ByteBuffer#limit()} . The caller may subsequently
     *            modify the contents of the buffer without changing the state
     *            of the cache (i.e., the data are copied into the cache).
     * 
     * @return <code>true</code> iff the caller's record was transferred to the
     *         cache. When <code>false</code>, there is not enough room left in
     *         the write cache for this record.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             If the buffer is closed.
     * @throws IllegalArgumentException
     *             If the caller's record is larger than the maximum capacity of
     *             cache (the record could not fit within the cache). The caller
     *             should check for this and provide special handling for such
     *             large records. For example, they can be written directly onto
     *             the backing channel.
     */
    public boolean write(final long offset, final ByteBuffer data)
            throws InterruptedException;

    /**
     * Read a record from the write cache.
     * 
     * @param offset
     *            The offset of that record (maybe relative to a base offset).
     * @param nbytes
     *            The length of the record (decoded from the address by the
     *            caller).
     * 
     * @return The data read -or- <code>null</code> iff the record does not lie
     *         within the {@link IWriteCache}. When non-null, this will be a
     *         newly allocated exact fit mutable {@link ByteBuffer} backed by a
     *         Java <code>byte[]</code>. The buffer will be flipped to prepare
     *         for reading (the position will be zero and the limit will be the
     *         #of bytes read).
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             if the buffer is closed.
     */
    public ByteBuffer read(final long offset) throws InterruptedException;

//    /**
//     * Update a record in the write cache.
//     * 
//     * @param addr
//     *            The address of the record.
//     * @param off
//     *            The byte offset of the update.
//     * @param data
//     *            The data to be written onto the record in the cache starting
//     *            at that byte offset. The bytes from the current
//     *            {@link ByteBuffer#position()} to the
//     *            {@link ByteBuffer#limit()} will be written and the
//     *            {@link ByteBuffer#position()} will be advanced to the
//     *            {@link ByteBuffer#limit()}. The caller may subsequently modify
//     *            the contents of the buffer without changing the state of the
//     *            cache (i.e., the data are copied into the cache).
//     * 
//     * @return <code>true</code> iff the record was updated and
//     *         <code>false</code> if no record for that address was found in the
//     *         cache.
//     * 
//     * @throws InterruptedException
//     * @throws IllegalStateException
//     * 
//     * @throws IllegalStateException
//     *             if the buffer is closed.
//     */
//    public boolean update(final long addr, final int off, final ByteBuffer data)
//            throws IllegalStateException, InterruptedException;

    /**
     * Flush the writes to the backing channel but does not force anything to
     * the backing channel. The caller is responsible for managing when the
     * channel is forced to the disk (if it is backed by disk) and whether file
     * data or file data and file metadata are forced to the disk.
     * 
     * @throws IOException
     * @throws InterruptedException
     * 
     *             FIXME The [force] parameter is ignored and will be removed
     *             shortly.
     */
    public void flush(final boolean force) throws IOException,
            InterruptedException;

    /**
     * Flush the writes to the backing channel but does not force anything to
     * the backing channel. The caller is responsible for managing when the
     * channel is forced to the disk (if it is backed by disk) and whether file
     * data or file data and file metadata are forced to the disk.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     * 
     *             FIXME The [force] parameter is ignored and will be removed
     *             shortly.
     */
    public boolean flush(final boolean force, final long timeout,
            final TimeUnit unit) throws IOException, TimeoutException,
            InterruptedException;

    /**
     * Reset the write cache, discarding any writes which have not been written
     * through to the backing channel yet. This method IS NOT responsible for
     * discarding writes which have been written through since those are in
     * general available for reading directly from the backing channel. The
     * abort protocol at the {@link IAtomicStore} level is responsible for
     * ensuring that processes do not see old data after an abort. This is
     * generally handled by re-loading the appropriate root block and
     * reinitializing various things from that root block.
     * 
     * @throws InterruptedException
     */
    public void reset() throws InterruptedException;
    
    /**
     * Permanently take the {@link IWriteCache} out of service. Dirty records
     * are discarded, not flushed.
     * 
     * @throws InterruptedException
     */
    public void close() throws InterruptedException;

}
