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

import java.io.Serializable;

/**
 * <p>
 * Interface for batch operations a B+-Tree mapping variable length unsigned
 * byte[] keys to arbitrary values. Batch operations can be very efficient if
 * the keys are presented in sorted order. Custom logic can be communicated with
 * the data {@link IIndexProcedure}s and the
 * {@link #submit(int, byte[][], byte[][], IIndexProcedureConstructor, IResultHandler) submit}
 * method.
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

    /**
     * Runs a procedure against an index.
     * <p>
     * Note: This may be used to send custom logic together with the data to a
     * remote index or index partition. When the index is remote both the
     * procedure and the return value MUST be {@link Serializable}.
     * <p>
     * Note: While scale-out indices add support for auto-split of the procedure
     * such that it runs locally against each relevant index partition, the
     * procedure only has access to individual index partitions.
     * 
     * @param n
     *            The #of tuples (positive integer).
     * @param keys
     *            The keys (required).
     * @param vals
     *            The values (optional depending on the procedure).
     * @param ctor
     *            An object that can create instances of the procedure.
     * @param handler
     *            An object that is responsible for handling the results
     *            obtained from the procedure, including both when it is applied
     *            to a unitary index and when it is applied to multiple index
     *            partitions of a scale-out index.
     */
    public void submit(int n, byte[][] keys, byte[][] vals,
            IIndexProcedureConstructor ctor, IResultHandler handler);

}
