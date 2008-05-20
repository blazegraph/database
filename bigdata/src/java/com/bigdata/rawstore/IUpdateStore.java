/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on May 20, 2008
 */

package com.bigdata.rawstore;

import java.nio.ByteBuffer;

import com.bigdata.btree.IndexSegmentBuilder;

/**
 * This interface adds methods for allocating a record without writing any data
 * and for (re-)writing a slice of a record.
 * 
 * @todo This interface can be used to perform incremental writes for the block
 *       API. The client requests a blob reference of a given size. The data
 *       service allocates the space for that block on the raw store, obtaining
 *       its addr, and encapsulates this as a "blob" for the client. The client
 *       writes on the blob and the data service collects bytes to be written in
 *       a buffer. When the buffer overflows, the data services flushes it in an
 *       incremental write to the backing store using this method.
 * 
 * @todo the other use case for this is the {@link IndexSegmentBuilder} which
 *       needs to be able to allocate the block (obtaining its address) before
 *       it writes the block, so either a "write-update" or an "alloc-update".
 *       this is required in order to get the prior/next leaf references into
 *       place in the generated store file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IUpdateStore extends IRawStore {

    /**
     * Allocate a record without writing it on the store
     * <p>
     * Note: The contents of the record having that address are <i>undefined</i>
     * unless until data is written onto the record using
     * {@link #update(long, int, ByteBuffer)} and only those bytes actually
     * written will be defined.
     * 
     * @param nbytes
     *            The #of bytes in the record.
     * 
     * @return The address of the record.
     */
    public long allocate(final int nbytes);
    
    /**
     * Updates a region of a record. The record may have been written or simply
     * allocated. The bytes in <i>data</i> from the
     * {@link ByteBuffer#position()} to the {@link ByteBuffer#limit()} will be
     * written starting at <i>off</i> bytes into the record identified by the
     * <i>addr</i>. The state of other bytes in the record are unchanged. If
     * their state was undefined (e.g., the record was {@link #allocate(int)}'d
     * but not written) then their state will remain undefined.
     * 
     * @param addr
     *            The address of an existing record.
     * @param off
     *            The offset into that record at which the data will be written.
     * @param data
     *            The data to be written.
     * 
     * @throws IllegalArgumentException
     *             if <i>addr</i> was not assigned by this store (at least for
     *             those cases where this can be detected).
     * @throws IllegalArgumentException
     *             if <i>off</i> is negative.
     * @throws IllegalArgumentException
     *             if <i>data</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if there are no bytes to be written in <i>data</i>.
     * @throws IllegalArgumentException
     *             if <i>off</i> plus the #of bytes to be written in <i>data</i>
     *             exceeds the size of the record identified by <i>addr</i>.
     * @throws IllegalStateException
     *             if the store is read-only.
     */
    public void update(final long addr, final int off, final ByteBuffer data);
    
}
