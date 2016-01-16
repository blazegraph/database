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
 * Created on Aug 14, 2011
 */

package com.bigdata.bop.join;

import com.bigdata.bop.IVariable;

/**
 * Annotations for hash joins.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface HashJoinAnnotations {

    /**
     * The {@link IVariable[]} specifying the join variables (required). The
     * order of the entries is used when forming the as-bound keys for the hash
     * table. Duplicate elements and null elements are not permitted.
     * <p>
     * Note: The source solutions presented to a hash join MUST have bindings
     * for the {@link #JOIN_VARS} in order to join (source solutions can still
     * be passed on as optionals, but they will not join unless the join
     * variables are not bound).
     * <p>
     * Note: If no join variables are specified, then the join will consider the
     * N x M cross product, filtering for solutions which join. This is very
     * expensive when compared to a hash join. Whenever possible you should
     * identify one or more variables which must be bound for the join and
     * specify those as the join variables.
     */
    String JOIN_VARS = HashJoinAnnotations.class.getName() + ".joinVars";

    /**
     * When non-<code>null</code>, the {@link IVariable} which will be bound to
     * <code>true</code> iff there is at least one solution for a
     * {@link JoinTypeEnum#Exists} hash join.
     * <p>
     * Note: This supports the bridge between the evaluation of the (NOT) EXISTS
     * graph pattern and the processing whether or not the "EXISTS" graph
     * pattern was successful, which is encoded on the "ask variable."
     */
    String ASK_VAR = HashJoinAnnotations.class.getName() + ".askVar";


//    /**
//     * The maximum number of solutions that will be considered before a hash
//     * join without any join variables is failed.
//     * <p>
//     * The purpose of this annotation is to identify hash joins which are doing
//     * too much work because they lack any join variables.
//     */
//    String NO_JOIN_VARS_LIMIT = HashJoinAnnotations.class.getName()
//            + ".noJoinVarsLimit";

    /**
     * TODO Annotation and query hint for this.
     * 
     * @see UnconstrainedJoinException
     */
    long DEFAULT_NO_JOIN_VARS_LIMIT = Long.MAX_VALUE;
    

    /**
     * Boolean flag to be set when we do not want to return the variables
     * defined by the {#JVMHashIndexOp.Annotations.SELECT} annotation, but
     * instead calculate the DISTINCT projection over the join variables.
     * This is the approach that we use for many subgroups, where we project
     * in the distinct variables (typically: exactly the join variables),
     * execute the inner group, and finally rejoin with the whole set of
     * variables in the end.
     */
    final String OUTPUT_DISTINCT_JVs = 
       HashJoinAnnotations.class.getName() + ".outputDistinctJVs";
    
}
