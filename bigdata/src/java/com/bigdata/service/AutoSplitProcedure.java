/*

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
 * Created on Jan 7, 2008
 */
package com.bigdata.service;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IProcedure;
import com.bigdata.service.ClientIndexView.Split;

/**
 * Abstract base class for {@link IProcedure}s that can be transparently run
 * against a local index or auto-split based on the key ranges and applied to a
 * partitioned index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo support efficient serialization using key and value compression. The
 *       application should be able to specify the (de-)compression algorithm.
 * 
 * @todo consider moving #apply() onto IIndex and then having the implementation
 *       of that method transparently use {@link AutoSplitProcedure} when the
 *       index is remote.  This would also let us handle a hash-partitioned
 *       index transparently.
 */
abstract public class AutoSplitProcedure<T extends Object> implements IProcedure {

    /**
     * The #of valid entries in {@link #keys}.
     */
    protected int n;

    /**
     * The keys (in sorted order).
     */
    protected byte[][] keys;

    /**
     * The values (optional, co-indexed with {@link #keys} when non-<code>null</code>).
     */
    protected byte[][] vals;

    /**
     * Lazily allocated iff used.
     */
    private List<Object> results;

    /**
     * @param n
     *            The #of entries in keys that contain valid data
     *            (additional entries are ignored).
     * @param keys
     *            The keys (in sorted order).
     * @param vals
     *            The values (optional, when non-<code>null</code> the
     *            vals[] must be correlated with the keys[]).
     */
    protected AutoSplitProcedure(int n, byte[][] keys, byte[][] vals) {

        if (n < 0)
            throw new IllegalArgumentException();

        if (keys == null)
            throw new IllegalArgumentException();

        if (keys.length < n)
            throw new IllegalArgumentException();

        this.n = n;

        this.keys = keys;

        this.vals = vals;

    }

    /**
     * Method is invoked for each result. The implementation is responsible for
     * combining the results in whatever manner is meaningful for the procedure.
     * The default implementation creates a {@link List} of the individual
     * results.
     * <p>
     * Note: Implementations of this method MUST be <strong>synchronized</strong>
     * since the procedure MAY be applied in parallel when it spans more than
     * one index partition.
     * <p>
     * Note: If you override this method then you probably also want to override
     * {@link #getResult()}.
     * 
     * @param result
     *            The result from applying the procedure to a single index
     *            partition.
     * @param split
     *            The {@link Split} that generated that result.
     */
    synchronized protected void aggregate(T result, Split split) {

        if (results == null) {

            // lazy allocation.
            results = new LinkedList<Object>();

        }

        results.add(results);

    }

    /**
     * Return the aggregated results as an implementation dependent object.
     * 
     * @see #aggregate(Object)
     */
    protected Object getResult() {

        return results;

    }

    /**
     * Create an instance of a procedure embodying the logic to be executed.
     * 
     * @param n
     *            The #of tuples that are input to the procedure.
     * @param keys
     *            The keys that are input to the procedure.
     * @param vals
     *            The values that are input to the procedure (optional).
     * @param split
     *            Metadata about the split, including the offsets into
     *            {@link #keys} and {@link #vals}.
     * 
     * @return The procedure.
     */
    protected abstract IProcedure newProc(Split split);

    /**
     * Makes a dense <strong>copy</strong> of the keys for the specified split.
     * <p>
     * Note: it is MUCH more efficient in terms of memory usage to index into
     * the {@link #keys} array using {@link Split#fromIndex} and
     * {@link Split#ntuples}.
     * 
     * @param split
     *            The split.
     * 
     * @return A dense copy of the keys for that split.
     */
    protected byte[][] getKeys(Split split) {

        final byte[][] _keys = new byte[split.ntuples][];

        System.arraycopy(keys, split.fromIndex, _keys, 0, split.ntuples);

        return keys;

    }

    /**
     * Makes a dense <strong>copy</strong> of the values for the specified
     * split.
     * <p>
     * Note: it is MUCH more efficient in terms of memory usage to index into
     * the {@link #vals} array using {@link Split#fromIndex} and
     * {@link Split#ntuples}.
     * 
     * @param split
     *            The split.
     * 
     * @return A dense copy of the values for that split.
     * 
     * @throws IllegalStateException
     *             if {@link #vals} is <code>null</code>.
     */
    protected byte[][] getVals(Split split) {

        if (vals == null)
            throw new IllegalStateException();

        final byte[][] _vals = new byte[split.ntuples][];

        System.arraycopy(vals, split.fromIndex, _vals, 0, split.ntuples);

        return vals;

    }
    
    /**
     * Apply a procedure to an index. If the index is a index then the procedure
     * will be transparently broken down and executed against each index
     * partitions spanned by its keys.
     * 
     * @param proc
     *            The procedure.
     * 
     * @return The result of the procedure. When the procedure is auto-split and
     *         run against multiple partitions, then the results of the
     *         individual runs are combined by {@link #aggregate(Object)} and
     *         reported by {@link #getResult()}.
     */
    final public Object apply(IIndex ndx) {

        if (!(ndx instanceof ClientIndexView)) {

            /*
             * Run the procedure locally.
             */

            IProcedure proc = newProc(null/* split */);

            return proc.apply(ndx);

        }

        /*
         * Run the procedure remotely.
         * 
         * @todo add counters for the #of procedures run and the execution time
         * for those procedures. add counters for the #of splits and the #of
         * tuples in each split, as well as the total #of tuples.
         */

        final ClientIndexView _ndx = (ClientIndexView) ndx;

        final List<Split> splits = _ndx.splitKeys(n, keys);

        final Iterator<Split> itr = splits.iterator();

        while (itr.hasNext()) {

            final Split split = itr.next();

            final IDataService dataService = _ndx.getDataService(split.pmd);

            final IProcedure proc = newProc(split);

            try {

                final T result = (T) dataService.submit(_ndx.getTx(), _ndx
                        .getName(), split.pmd.getPartitionId(), proc);

                aggregate(result, split);

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }

        }

        return getResult();

    }

}
