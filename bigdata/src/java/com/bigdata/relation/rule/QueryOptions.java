/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Sep 24, 2008
 */

package com.bigdata.relation.rule;

import java.util.Arrays;

import com.bigdata.bop.aggregation.ISlice;
import com.bigdata.bop.aggregation.ISortOrder;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class QueryOptions implements IQueryOptions {

    /**
     * 
     */
    private static final long serialVersionUID = -4926866732224421937L;

    private final boolean distinct;

    private final boolean stable;

    private final ISortOrder[] orderBy;

    private final ISlice slice;

    /**
     * An instance specifying NONE of the constraints declared by
     * {@link IQueryOptions}.
     */
    public static final transient IQueryOptions NONE = new QueryOptions(
            false/* distinct */, false/* stable */, null/* orderBy */, null/* Slice */);

    /**
     * An instance specifying <code>distinct := true</code> but none of the
     * other {@link IQueryOptions}.
     */
    public static final transient IQueryOptions DISTINCT = new QueryOptions(
            true/* distinct */, false/* stable */, null/* orderBy */, null/* querySlice */);
    
    /**
     * @param distinct
     * @param stable
     * @param orderBy
     * @param slice
     * @throws IllegalArgumentException
     *             if a <i>stable</i> is <code>false</code> and a slice is
     *             specified with a non-zero offset and/or a non-{@link Long#MAX_VALUE}
     *             limit
     */
    public QueryOptions(final boolean distinct, final boolean stable,
            final ISortOrder[] orderBy, final ISlice slice) {

        this.distinct = distinct;

        this.stable = stable;
        
        // MAY be null.  @todo check elements not null when orderBy not null.
        this.orderBy = orderBy;
        
        // MAY be null.
        this.slice = slice;

        if (!stable
                && slice != null
                && (slice.getOffset() != 0L || slice.getLimit() != Long.MAX_VALUE)) {

            throw new IllegalArgumentException("slices must be stable");
            
        }
        
    }

    final public boolean isDistinct() {
        
        return distinct;
        
    }

    final public boolean isStable() {
        
        return stable;
        
    }

    final public ISortOrder[] getOrderBy() {
        
        return orderBy;
        
    }

    final public ISlice getSlice() {

        return slice;
        
    }

    public String toString() {

        return "QueryOptions" + //
            "{ distinct=" + distinct + //
            ", stable=" + stable + //
            ", orderBy=" + (orderBy == null ? "N/A" : Arrays.toString(orderBy)) + //
            ", slice=" + slice + //
            "}";

    }
    
}
