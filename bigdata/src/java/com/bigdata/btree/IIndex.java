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
package com.bigdata.btree;

import java.io.Serializable;
import java.util.UUID;

import com.bigdata.isolation.IIsolatableIndex;

/**
 * <p>
 * Interface for mutable B+-Tree mapping arbitrary non-null keys to arbitrary
 * values.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndex extends ISimpleBTree { //, IBatchBTree {
    
    /**
     * The unique identifier for the index whose data is stored in this B+Tree
     * data structure. When using a scale-out index the same <i>indexUUID</i>
     * MUST be assigned to each mutable and immutable B+Tree having data for any
     * partition of that scale-out index. This makes it possible to work
     * backwards from the B+Tree data structures and identify the index to which
     * they belong.
     */
    public UUID getIndexUUID();

    /**
     * Interesting statistics about the index.
     */
    public String getStatistics();
    
    /**
     * Return <code>true</code> iff the index supports isolation.
     * <p>
     * Note: This is declared as a method since the {@link IIsolatableIndex}
     * interface is not always detectable. For example, a remote client will use
     * a view of the index. That view does not declare {@link IIsolatableIndex}
     * but the client can use this method instead to decide whether or not the
     * index supports transactional isolation.
     * 
     * @see IIsolatableIndex
     */
    public boolean isIsolatable();

    /**
     * Runs a procedure against an index.
     * <p>
     * Note: This may be used to send custom logic together with the data to a
     * remote index or index partition. When the index is remote both the
     * procedure and the return value MUST be {@link Serializable}.
     * <p>
     * Note: The scale-out indices add support for auto-split of the procedure
     * such that it runs locally against each relevant index partition.
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
