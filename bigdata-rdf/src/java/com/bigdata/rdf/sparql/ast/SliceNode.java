/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

/**
 * AST node for a SLICE (offset/limit).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SliceNode extends SolutionModifierBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends SolutionModifierBase.Annotations {

        final String OFFSET = "offset";

        final long DEFAULT_OFFSET = 0L;

        final String LIMIT = "limit";

        final long DEFAULT_LIMIT = Long.MAX_VALUE;
    
    }
    
    /**
     * Slice with defaults that do not impose a constraint (offset is ZERO,
     * limit is {@link Long#MAX_VALUE}).
     */
    public SliceNode() {
    }
    
    /**
     * @param offset
     *            The offset (origin ZERO).
     * @param limit
     *            The limit (use {@link Long#MAX_VALUE} if there is no limit).
     */
    public SliceNode(final long offset, final long limit) {
     
        setOffset(offset);
        
        setLimit(limit);
        
    }

    public void setOffset(final long offset) {
        
        setProperty(Annotations.OFFSET, offset);
        
    }

    public long getOffset() {
        
        return getProperty(Annotations.OFFSET, Annotations.DEFAULT_OFFSET);

    }

    public void setLimit(final long limit) {

        setProperty(Annotations.LIMIT, limit);
        
    }

    public long getLimit() {

        return getProperty(Annotations.LIMIT, Annotations.DEFAULT_LIMIT);

    }

    /**
     * Return <code>true</code> if the slice will impose a constraint (
     * <code>offset GT ZERO</code> or
     * <code>limit LT {@link Long#MAX_VALUE}</code>).
     */
    public boolean hasSlice() {

        return getOffset() > 0 || getLimit() < Long.MAX_VALUE;
        
    }
    
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(indent(indent));
        sb.append("slice(");
        final long offset = getOffset();
        final long limit = getLimit();
        if (offset != 0L) {
            sb.append("offset=" + offset);
        }
        if (limit != Long.MAX_VALUE) {
            if (offset != 0L)
                sb.append(",");
            sb.append("limit=" + limit);
        }
        sb.append(")");
        return sb.toString();

    }

//    public boolean equals(final Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof SliceNode))
//            return false;
//
//        final SliceNode t = (SliceNode) o;
//
//        if (limit != t.limit)
//            return false;
//
//        if (offset != t.offset)
//            return false;
//
//        return true;
//        
//    }
    
}
