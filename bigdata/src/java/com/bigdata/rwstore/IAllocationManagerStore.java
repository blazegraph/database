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
package com.bigdata.rwstore;

import java.nio.ByteBuffer;

import com.bigdata.rawstore.IRawStore;

/**
 * Adds capability to write and delete allocations within an
 * {@link IAllocationContext}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see IAllocationManager
 * @see IAllocationContext
 */
public interface IAllocationManagerStore extends IRawStore {

    /**
     * Write the data within the allocation context. The write is not visible
     * outside of the allocation until the allocation context has been merged
     * into the parent allocation context.
     * 
     * @param data
     *            The data.
     * @param context
     *            The allocation context.
     *            
     * @return The address at which the data was written.
     */
    long write(ByteBuffer data, IAllocationContext context);

    /**
     * Delete the data associated with the address within the allocation
     * context. The delete is not visible outside of the allocation until the
     * allocation context has been merged into the parent allocation context.
     * 
     * @param addr
     *            The address whose allocation is to be deleted.
     * @param context
     *            The allocation context.
     */
    void delete(long addr, IAllocationContext context);

}
