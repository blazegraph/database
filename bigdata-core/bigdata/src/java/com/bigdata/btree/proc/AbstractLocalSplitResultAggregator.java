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
package com.bigdata.btree.proc;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.btree.proc.SplitValuePair.PairComparator;
import com.bigdata.service.Split;

/**
 * Aggregator base class collects the individual results in an internal ordered
 * map and assembles the final result when it is requested from the individual
 * results. With this approach there is no overhead or contention when the
 * results are being produced in parallel and they can be combined efficiently
 * within a single thread in {@link #getResult()}.
 * <p>
 * Note: This implementation assumes that there is one element of the result for
 * each key in the original request. It places the {@link Split}-wise results
 * into a total ordering over the {@link Split}s and then delegates to a
 * concrete implementation to build the aggregated results out of the ordered
 * pairs of ({@link Split}, partial-result).
 *
 * @author bryan
 */
abstract public class AbstractLocalSplitResultAggregator<R> implements IResultHandler<R, R> {

	/**
	 * The #of elements in the request (which is the must be the same as the
	 * cardinality of the aggregated result).
	 */
	private final int size;

	/**
	 * Map for collecting the piece wise results.
	 */
	private final ConcurrentHashMap<Split, R> map = new ConcurrentHashMap<Split, R>();

	/**
	 * 
	 * @param size
	 *            The #of elements in the request (which is the same as the
	 *            cardinality of the aggregated result).
	 */
	public AbstractLocalSplitResultAggregator(final int size) {

		if (size < 0)
			throw new IllegalArgumentException();
		
		this.size = size;
		
	}

	@Override
	public void aggregate(final R result, final Split split) {

		map.put(split, result);

	}

	@Override
	public R getResult() {

		/*
		 * Extract the results into a key/value array.
		 */

		final int nresults = map.size();

		@SuppressWarnings("unchecked")
		final SplitValuePair<Split, R>[] a = new SplitValuePair[nresults];

		{

			int i = 0;

			for (Map.Entry<Split, R> e : map.entrySet()) {

				a[i++] = new SplitValuePair<Split, R>(e.getKey(), e.getValue());

			}

			if (a.length == 1) {

				// Do not bother aggregating a single result.
				return a[0].val;

			}

		}

		/*
		 * Sort the array by Split. This imposes a total ordering and makes the
		 * counters[] 1:1 with the original keys[][] and vals[][] provided to
		 * the index procedure.
		 */
		Arrays.sort(a, 0/* fromIndex */, a.length/* toIndex */, new PairComparator<Split, R>());

		return newResult(size, a);

	}

	/**
	 * Build the aggregated result by aggregate the individual results in the
	 * given order.
	 * 
	 * @param size
	 *            The number of keys in the original request.
	 * @param a
	 *            An array of {@link SplitValuePair}s that is ordered on
	 *            {@link Split#fromIndex}.
	 * 
	 * @return The aggregated result.
	 */
	abstract protected R newResult(final int size, final SplitValuePair<Split, R>[] a);

}
