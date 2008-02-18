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
 * Created on Feb 12, 2008
 */

package com.bigdata.rawstore;

import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.ITuple;
import com.bigdata.repo.BigdataRepository;
import com.bigdata.service.IDataService;

/**
 * An interface that supports streaming reads or writes of large blocks on a
 * store. The size of a block is limited to the maximum record size for the
 * backing store. In general, the maximum record size is a provisioning option
 * for the store.
 * <p>
 * Note: This interface does not support arbitrary length BLOBs. The reason is
 * that the architecture always limits by design the size of the artifacts that
 * have to be moved around to support a scale-out database. However, support for
 * BLOBs may be build on {@link IBlockStore}s. See {@link BigdataRepository}.
 * 
 * @deprecated This interface was never put into place. Instead I have added a
 *             means to read a block from an {@link ITuple} and an
 *             {@link IDataService}.  Writes of blocks are performed through the
 *             standard {@link IIndexProcedure} mechansims at this time, e.g., 
 *             see the {@link BigdataRepository}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBlockStore extends IRawStore {

    /**
     * Obtain an object that may be used to read a block from a store.
     * 
     * @param addr
     *            The address of the block.
     *            
     * @return The object that may be used to read the block from the store.
     */
    public IBlock readBlock(long addr);

    /**
     * Obtain an object that may be used to write a block on a store.
     * 
     * @param byteCount
     *            The size of the block to be written.
     *            
     * @return The object that may be used to write the block on the store.
     */
    public IBlock writeBlock(int byteCount);
    
}
