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
package com.bigdata.service.ndx;

import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.service.Split;

/**
 * NOP aggregator does nothing and returns <code>null</code>. This is used for
 * parallelizing operations that require a result handler to stripe them against
 * the index.
 * 
 * @author bryan
 *
 * @param <R>
 * @param <A>
 * 
 * @see BLZG-1537 (Schedule more IOs when loading data)
 */
public class NopAggregator<R, A> implements IResultHandler<R, A> {

	@SuppressWarnings("rawtypes")
	public static final IResultHandler INSTANCE = new NopAggregator();

	public NopAggregator() {
	}

	@Override
	public void aggregate(final R result, final Split split) {
		// NOP
	}

	@Override
	public A getResult() {
		// NOP
		return null;
	}

}
