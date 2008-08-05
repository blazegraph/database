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
 * Created on Jan 16, 2008
 */
package com.bigdata.btree.proc;

import com.bigdata.service.Split;

/**
 * An interface for handling results obtained when an {@link IIndexProcedure} is
 * applied to either a unitary index or to partitions of a scale-out index.
 * 
 * @param <R>
 *            The type of the result from applying the procedure to a single
 *            index partition.
 * @param <A>
 *            The type of the aggregated result.
 * 
 * @todo drop {@link #getResult()} from the signature? The handler
 *       implementation can expose a custom method when an aggregated return is
 *       desirable. However some handlers will apply iterative processing to the
 *       results as they are obtained without any sense of aggregation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IResultHandler<R extends Object, A extends Object> {
    
    /**
     * Method is invoked for each result and is responsible for combining
     * the results in whatever manner is meaningful for the procedure.
     * Implementations of this method MUST be <strong>thread-safe</strong>
     * since the procedure MAY be applied in parallel when it spans more
     * than one index partition.
     * 
     * @param result
     *            The result from applying the procedure to a single index
     *            partition.
     * @param split
     *            The {@link Split} that generated that result.
     */
    public void aggregate(R result, Split split);

    /**
     * Return the aggregated results as an implementation dependent object.
     */
    public A getResult();

}
