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

package com.bigdata.btree;

import java.io.Serializable;

import com.bigdata.journal.IResourceManager;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;

/**
 * An interface used to decided whether and where to split an index partition
 * into 2 or more index partitions.
 * <p>
 * Note: applications frequency must constrain the allowable separator keys when
 * splitting an index partition into two or more index partitions. For example,
 * the {@link SparseRowStore} must to maintain an guarentee of atomic operations
 * for a logical row, which is in turn defined as the ordered set of index
 * entries sharing the same primary key. You can use this interface to impose
 * application specific constraints such that the index partition boundaries
 * only fall on acceptable separator keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISplitHandler extends Serializable {

    /**
     * Return <code>true</code> if a cursory examination of an index partition
     * suggests that it SHOULD be split into 2 or more index partitions.
     * 
     * @param ndx
     *            An index partition.
     * 
     * @return <code>true</code> if the index partition should be split.
     */
    public boolean shouldSplit(IIndex ndx);
    

    /**
     * Choose a set of splits that completely span the key range of the index
     * view. The first split MUST use the leftSeparator of the index view as its
     * leftSeparator. The last split MUST use the rightSeparator of the index
     * view as its rightSeparator. The #of splits SHOULD be choosen such that
     * the resulting index partitions are each at least 50% full.
     * 
     * @param ndx
     *            The source index partition.
     * 
     * @return A {@link Split}[] array contains everything that we need to
     *         define the new index partitions <em>except</em> the partition
     *         identifiers -or- <code>null</code> if a more detailed
     *         examination reveals that the index SHOULD NOT be split at this
     *         time.
     */
    public Split[] getSplits(IResourceManager resourceManager, IIndex ndx);
    
}
