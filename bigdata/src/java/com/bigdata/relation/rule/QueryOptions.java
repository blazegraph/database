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
    
    public QueryOptions(boolean distinct, boolean stable,
            ISortOrder[] orderBy, ISlice slice) {

        this.distinct = distinct;

        this.stable = stable;
        
        // MAY be null.
        this.orderBy = orderBy;
        
        // MAY be null.
        this.slice = slice;

    }

    public boolean isDistinct() {
        
        return distinct;
        
    }

    public boolean isStable() {
        
        return stable;
        
    }

    public ISortOrder[] getOrderBy() {
        
        return orderBy;
        
    }

    public ISlice getSlice() {

        return slice;
        
    }

}
