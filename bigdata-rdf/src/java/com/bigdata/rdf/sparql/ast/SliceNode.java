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

    private long offset, limit;
    
    /**
     * Slice with defaults that do not impose a constraint (offset is ZERO,
     * limit is {@link Long#MAX_VALUE}).
     */
    public SliceNode() {
        offset = 0;
        limit = Long.MAX_VALUE;
    }
    
    /**
     * @param offset
     *            The offset (origin ZERO).
     * @param limit
     *            The limit (use {@link Long#MAX_VALUE} if there is no limit).
     */
    public SliceNode(final long offset, final long limit) {
      
        this.offset = offset;
        
        this.limit = limit;
        
    }

    public void setOffset(final long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public void setLimit(final long limit) {
        this.limit = limit;
    }

    public long getLimit() {
        return limit;
    }

    /**
     * Return <code>true</code> if the slice will impose a constraint (
     * <code>offset
     * GT ZERO</code> or <code>limit LT {@link Long#MAX_VALUE}</code>).
     */
    public boolean hasSlice() {
        return offset > 0 || limit < Long.MAX_VALUE;
    }
    
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(indent(indent));
        sb.append("slice(");
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

    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof SliceNode))
            return false;

        final SliceNode t = (SliceNode) o;

        if (limit != t.limit)
            return false;

        if (offset != t.offset)
            return false;

        return true;
        
    }
    
}
