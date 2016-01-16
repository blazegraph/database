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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;

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

    public interface Annotations extends SolutionModifierBase.Annotations {

        /**
         * The first solution to be returned to the caller (origin ZERO).
         */
        final String OFFSET = "offset";

        final long DEFAULT_OFFSET = 0L;

        /**
         * The maximum #of solutions to be returned to the caller (default is
         * all).
         */
        final String LIMIT = "limit";

        /**
         * A value of {@link Long#MAX_VALUE} is used to indicate that there is
         * no limit.
         */
        final long DEFAULT_LIMIT = Long.MAX_VALUE;
    
    }
    
    /**
     * Deep copy constructor.
     */
    public SliceNode(final SliceNode op) {

        super(op);

    }

    /**
     * Shallow copy constructor.
     */
    public SliceNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

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
    
    @Override
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
        if (getQueryHints() != null && !getQueryHints().isEmpty()) {
            sb.append("\n");
            sb.append(indent(indent + 1));
            sb.append(Annotations.QUERY_HINTS);
            sb.append("=");
            sb.append(getQueryHints().toString());
        }
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
