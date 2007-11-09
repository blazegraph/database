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
 * Created on Feb 1, 2007
 */

package com.bigdata.btree;

/**
 * <p>
 * Interface for batch operations a B+-Tree mapping variable length unsigned
 * byte[] keys to arbitrary values. Batch operations can be very efficient if
 * the keys are presented in sorted order.
 * </p>
 * <p>
 * All mutation operations on a {@link BTree} are executed in a single threaded
 * context and are therefore atomic. A batch api operation that does NOT span
 * more than one index partition is therefore atomic. However, if an operation
 * spans multiple partitions of an index then NO GUARENTEE is made that the
 * operation is atomic over the set of index partitions.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see UnicodeKeyBuilder, which may be used to encode one or more values into a
 *      variable length unsigned byte[] key.
 * 
 * @todo add batch api for rangeCount and rangeQuery.
 * 
 * @todo support batch api for indexOf(), keyAt(), valueAt()?
 * 
 * @todo add extensible operation defined by an vector
 *       {@link UserDefinedFunction}. Use this to move application logic to the
 *       indices as an alternative to scalar {@link UserDefinedFunction}s.  For
 *       example, the logic that tests an index for a key, increments a counter
 *       if the key is not found, and inserts the counter under the key.
 */
public interface IBatchBTree {

    /**
     * Apply a batch insert operation.
     */
    public void insert(BatchInsert op);

    /**
     * Apply a batch lookup operation.
     */
    public void lookup(BatchLookup op);

    /**
     * Apply a batch existence test operation.
     */
    public void contains(BatchContains op);

    /**
     * Apply a batch remove operation.
     */
    public void remove(BatchRemove op);

}
