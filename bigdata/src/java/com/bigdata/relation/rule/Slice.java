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

import java.math.BigInteger;

/**
 * Default implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Slice implements ISlice {

    /**
     * 
     */
    private static final long serialVersionUID = 5396509164843609197L;

    private final long offset;
    private final long limit;
    private final long last;
    
    /**
     * A slice corresponding to all results (offset is zero, limit is
     * {@link Long#MAX_VALUE}).
     */
    public static final transient ISlice ALL = new Slice(0, Long.MAX_VALUE);
    
    /**
     * 
     * @param offset
     * @param limit
     * 
     * @throws IllegalArgumentException
     *             if offset is negative.
     * @throws IllegalArgumentException
     *             if limit is non-positive.
     */
    public Slice(long offset,long limit) {
        
        if (offset < 0)
            throw new IllegalArgumentException();
        
        if (limit <= 0)
            throw new IllegalArgumentException();
        
        this.offset = offset;
        
        this.limit = limit;

        // @todo what is a cheaper way to do this?
        this.last = BigInteger.valueOf(offset).add(BigInteger.valueOf(limit))
                .min(BigInteger.valueOf(Long.MAX_VALUE)).longValue();
        
    }
    
    public long getOffset() {
        
        return offset;
        
    }

    public long getLimit() {
        
        return limit;
        
    }

    public long getLast() {

        return last;
        
    }
    
    public String toString() {
        
        return "Slice{offset="+offset+", limit="+limit+", last="+last+"}";
        
    }
    
}
