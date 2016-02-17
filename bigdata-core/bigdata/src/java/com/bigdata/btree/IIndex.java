/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.service.Split;

/**
 * <p>
 * Interface for mutable B+-Tree mapping arbitrary non-null keys to arbitrary
 * values.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IIndex extends ISimpleBTree, IAutoboxBTree, IRangeQuery,
		IIndexLocalCounter, ICounterSetAccess {

	/**
	 * The description of the resources comprising the index view.
	 */
    public IResourceMetadata[] getResourceMetadata();

	/**
	 * The metadata for the index. This is full of good stuff about the index.
	 * <p>
	 * Note: The same method is exposed by {@link ICheckpointProtocol}. It is
	 * also exposed here in order to provide access to the {@link IndexMetadata}
	 * to remote clients in the scale-out architecture.
	 * 
	 * @see ICheckpointProtocol#getIndexMetadata()
	 */
    public IndexMetadata getIndexMetadata();
    
    /**
     * {@inheritDoc}
     * <p>
     * Interesting performance counters and other statistics about the index.
     */
    @Override
    public CounterSet getCounters();
    
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
    public <T> T submit(byte[] key, ISimpleIndexProcedure<T> proc);
    
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
     * 
     * TODO In order to allow parallelization within a shard, we need to modify
     * this method signature to pass in an {@link IResultHandler} constructor
     * object. That might be something which could be pushed down onto the ctor
     * argument. It would be used in scale-out to create a DS local result handler
     * so we can locally aggregate when parallelizing against each shard and then
     * return that aggregated result to the client which would extract the aggregate
     * result across the shards from the client's result handler. See BLZG-1537.
     *            
	 * @see BLZG-1537 (Schedule more IOs when loading data)
     */
    public void submit(int fromIndex, int toIndex, byte[][] keys,
            byte[][] vals, AbstractKeyArrayIndexProcedureConstructor ctor,
            IResultHandler resultHandler);

}
