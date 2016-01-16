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
/*
 * Created on Sep 28, 2011
 */

package com.bigdata.relation.accesspath;

import com.bigdata.bop.IPredicate;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.relation.IRelation;

import cutthecrap.utils.striterators.IFilter;

/**
 * A common interface for all access paths.
 * 
 * @param <R>
 *            The generic type of the [R]elation elements of the
 *            {@link IRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAbstractAccessPath<R> {

    /**
     * The constraints on the {@link IAccessPath}.
     */
    IPredicate<R> getPredicate();

    /**
     * True iff the access path is empty (there are no matches for the
     * {@link IPredicate}) This is more conclusive than {@link #rangeCount()}
     * since you MAY have a non-zero range count when the key range is in fact
     * empty (there may be "deleted" index entries within the key range).
     */
    boolean isEmpty();

    /**
     * Return the maximum #of elements spanned by the {@link IPredicate}.
     * <p>
     * Note: When there is an {@link IFilter} on the {@link IPredicate} the
     * exact range count MUST apply that {@link IFilter}, which means that it
     * will be required to traverse the index counting tuples which pass the
     * {@link IFilter}. However, {@link IFilter}s are ignored for the fast
     * range count.
     * 
     * @param exact
     *            When <code>true</code>, the result will be an exact count and
     *            may require a key-range scan. When <code>false</code>, the
     *            result will be an upper bound IFF delete markers are
     *            provisioned for the backing index (delete markers are required
     *            for transactions and for scale-out indices).
     * 
     * @see IRangeQuery
     */
    long rangeCount(boolean exact);

    /**
     * Remove all elements selected by the {@link IPredicate} (optional
     * operation).
     * 
     * @return The #of elements that were removed.
     */
    public long removeAll();

}
