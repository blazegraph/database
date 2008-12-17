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

import com.bigdata.btree.proc.AbstractIndexProcedureConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.counters.ICounterSet;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.Split;

/**
 * <p>
 * Interface for mutable B+-Tree mapping arbitrary non-null keys to arbitrary
 * values.
 * </p>
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IIndex extends ISimpleBTree, IAutoboxBTree, IRangeQuery {

    /**
     * The description of the resources comprising the index view.
     */
    public IResourceMetadata[] getResourceMetadata();
    
    /**
     * The metadata for the index. This is full of good stuff about the index.
     */
    public IndexMetadata getIndexMetadata();
    
    /**
     * A restart-safe counter. For an unpartitioned index, this a single counter
     * for the entire index with an initial value of zero (0) and it is stored
     * in the index {@link Checkpoint} record. For a partitioned index, there is a
     * distinct counter for each index partition, the partition identifier is
     * used as the high int32 bits of the counter, and the low int32 of the
     * counter has an initial value of zero (0) in each index partition.
     */
    public ICounter getCounter();
    
    /**
     * Interesting performance counters and other statistics about the index.
     */
    public ICounterSet getCounters();
    
    /**
     * Submits an index procedure that operations on a single key to the
     * appropriate index partition returning the result of that procedure.
     * 
     * @param key
     *            The key.
     * @param proc
     *            The procedure.
     * 
     * @return The value returned by {@link IIndexProcedure#apply(IIndex)}
     */
    public Object submit(byte[] key, ISimpleIndexProcedure proc);
    
    /**
     * The procedure will be transparently applied against each index partition
     * spanned by the given key range.
     * <p>
     * Note: Since this variant of <i>submit()</i> does not split keys the
     * <i>fromIndex</i> and <i>toIndex</i> in the {@link Split}s reported to
     * the {@link IResultHandler} will be zero (0).
     * 
     * @param fromKey
     *            The lower bound (inclusive) -or- <code>null</code> if there
     *            is no lower bound.
     * @param toKey
     *            The upper bound (exclusive) -or- <code>null</code> if there
     *            is no upper bound.
     * @param proc
     *            The procedure. If the procedure implements the
     *            {@link IParallelizableIndexProcedure} marker interface then it
     *            MAY be executed in parallel against the relevant index
     *            partition(s).
     * @param resultHandler
     *            When defined, results from each procedure application will be
     *            reported to this object.
     */
    public void submit(byte[] fromKey, byte[] toKey,
            final IKeyRangeIndexProcedure proc, final IResultHandler handler); 
    
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
     * @param fromIndex
     *            The index of the first key to be used (inclusive).
     * @param toIndex
     *            The index of the last key to be used (exclusive).
     * @param keys
     *            The keys (required).
     * @param vals
     *            The values (optional depending on the procedure).
     * @param ctor
     *            An object that can create instances of the procedure.
     * @param resultHandler
     *            When defined, results from each procedure application will be
     *            reported to this object.
     */
    public void submit(int fromIndex, int toIndex, byte[][] keys,
            byte[][] vals, AbstractIndexProcedureConstructor ctor,
            IResultHandler resultHandler);

}
