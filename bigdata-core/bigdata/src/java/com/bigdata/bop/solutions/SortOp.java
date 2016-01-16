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
 * Created on Sep 4, 2010
 */

package com.bigdata.bop.solutions;

import java.util.Comparator;
import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.PipelineOp;

/**
 * Base class for operators which sort binding sets.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class SortOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * An {@link ISortOrder}[] specifying an ordered list of
         * {@link IValueExpression}s on which the sort will be imposed and the
         * order (ascending or descending) for each {@link IValueExpression}.
         */
        String SORT_ORDER = SortOp.class.getName() + ".sortOrder";

        /**
         * The {@link Comparator} used to compare individual as-bound / computed
         * values within binding sets. This will be wrapped up with a comparator
         * which knows how to compare the different values within the binding
         * sets based on the declared {@link #SORT_ORDER}.
         */
        String VALUE_COMPARATOR = SortOp.class.getName() + ".valueComparator";

    }

    /**
     * @param op
     */
    public SortOp(final SortOp op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public SortOp(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * @see Annotations#SORT_ORDER
     */
    public ISortOrder[] getSortOrder() {

        return (ISortOrder[]) getRequiredProperty(Annotations.SORT_ORDER);

    }

    /**
     * @see Annotations#VALUE_COMPARATOR
     */
    public Comparator getValueComparator() {

        return (Comparator) getRequiredProperty(Annotations.VALUE_COMPARATOR);

    }

}
