/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 12, 2010
 */

package com.bigdata.bop;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An operator, such as a constant, variable, join, sort, filter, etc.
 * <p>
 * Operators are organized in a tree of operators. The <i>arity</i> of an
 * operator is the number of child operands declared by that operator class. The
 * children of an operator are themselves operators. Parents reference their
 * children, but back references to the parents are not maintained.
 * <p>
 * In addition to their arguments, operators may have a variety of annotations,
 * including those specific to an operator (such as the maximum number of
 * iterators for a closure operator), those shared by many operators (such as
 * set of variables which are selected by a join or distributed hash table), or
 * those shared by all operators (such as a cost model).
 * <p>
 * Operators are immutable, {@link Serializable} to facilitate distributed
 * computing, and {@link Cloneable} to facilitate non-destructive tree rewrites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface BOp extends Cloneable, Serializable {

    /**
     * The #of arguments to the operation.
     */
    int arity();

    /**
     * Return an argument to the operation.
     * 
     * @param index
     *            The argument index in [0:{@link #arity()}-1].
     * 
     * @return The argument.
     */
    BOp get(int index);
    
    /**
     * The operator's arguments.
     */
    List<BOp> args();

    /**
     * The operator's annotations.
     */
    Map<String,Object> annotations();

    /**
     * Deep copy clone of the operator.
     */
    BOp clone();

    /**
     * Interface declaring well known annotations.
     */
    public interface Annotations {
        
        /**
         * The unique identifier for a query. This is used to collect all
         * runtime state for a query within the session on a node.
         */
        String QUERY_ID = "queryId";

        /**
         * The unique identifier within a query for a specific {@link BOp}. The
         * {@link #QUERY_ID} and the {@link #BOP_ID} together provide a unique
         * identifier for the {@link BOp} within the context of its owning
         * query.
         */
        String BOP_ID = "bopId";
        
    }
    
}
