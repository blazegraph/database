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
 * Operators are effectively immutable (mutation APIs always return a deep copy
 * of the operator to which the mutation has been applied), {@link Serializable}
 * to facilitate distributed computing, and {@link Cloneable} to facilitate
 * non-destructive tree rewrites.
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

    /** A shallow copy of the operator's arguments. */
    BOp[] toArray();

    /**
     * A shallow copy of the operator's arguments using the generic type of the
     * caller's array. If the array has sufficient room, then the arguments are
     * copied into the caller's array. If there is space remaining, a
     * <code>null</code> is appended to mark the end of the data.
     */
    <T> T[] toArray(final T[] a);
    
    /**
     * The operator's annotations.
     */
    Map<String,Object> annotations();

    /**
     * Return the value of the named annotation.
     * 
     * @param name
     *            The name of the annotation.
     * @param defaultValue
     *            The default value.
     * @return The annotation value -or- the <i>defaultValue</i> if the
     *         annotation was not bound.
     * @param <T>
     *            The generic type of the annotation value.
     */
    <T> T getProperty(final String name, final T defaultValue);

    /**
     * Return the value of the named annotation.
     * 
     * @param name
     *            The name of the annotation.
     *            
     * @return The value of the annotation and <code>null</code> if the
     *         annotation is not bound.
     */
    <T> T getProperty(final String name);

    /**
     * Return the value of the named annotation.
     * 
     * @param name
     *            The name of the annotation.
     * 
     * @return The value of the annotation.
     * 
     * @throws IllegalArgumentException
     *             if the named annotation is not bound.
     */
    <T> T getRequiredProperty(final String name);
    
    /**
     * Deep copy clone of the operator.
     */
    BOp clone();

    /**
     * Return the evaluation context for the operator. The default is
     * {@link BOpEvaluationContext#ANY}. Operators which must be mapped against
     * shards, mapped against nodes, or evaluated on the query controller must
     * override this method.
     */
    BOpEvaluationContext getEvaluationContext();

    /**
     * Return <code>true</code> iff this operator is an access path which writes
     * on the database.
     * 
     * @see Annotations#MUTATION
     */
    boolean isMutation();
    
    /**
     * The timestamp or transaction identifier on which the operator will read
     * or write.
     * 
     * @see Annotations#TIMESTAMP
     */
    long getTimestamp();
    
    /**
     * Interface declaring well known annotations.
     */
    public interface Annotations {

        /**
         * The unique identifier within a query for a specific {@link BOp}. The
         * {@link #QUERY_ID} and the {@link #BOP_ID} together provide a unique
         * identifier for the {@link BOp} within the context of its owning
         * query.
         */
        String BOP_ID = "bopId";

        /**
         * The timeout for the operator evaluation (milliseconds).
         * 
         * @see #DEFAULT_TIMEOUT
         * 
         * @todo Probably support both deadlines and timeouts. A deadline
         *       expresses when the query must be done while a timeout expresses
         *       how long it may run. A deadline may be imposed as soon as the
         *       query plan is formulated and could even be communicated from a
         *       remote client (e.g., as an httpd header). A timeout will always
         *       be interpreted with respect to the time when the query began to
         *       execute.
         */
        String TIMEOUT = "timeout";
        
        /**
         * The default timeout for operator evaluation.
         */
        long DEFAULT_TIMEOUT = Long.MAX_VALUE;

        /**
         * Boolean property whose value is <code>true</code> iff this operator
         * writes on a database.
         * <p>
         * Most operators operate solely on streams of elements or binding sets.
         * Some operators read or write on the database using an access path,
         * which is typically described by an {@link IPredicate}. This property
         * MUST be <code>true</code> when access path is used to write on the
         * database.
         * <p>
         * Operators which read or write on the database must declare the
         * {@link Annotations#TIMESTAMP} associated with that operation.
         * 
         * @see #TIMESTAMP
         */
        String MUTATION = BOp.class.getName() + ".mutation";
        
        boolean DEFAULT_MUTATION = false;
        
        /**
         * The timestamp (or transaction identifier) used by this operator if it
         * reads or writes on the database.
         * 
         * @see #MUTATION
         */
        String TIMESTAMP = BOp.class.getName() + ".timestamp";

        /**
         * For hash partitioned operators, this is the set of the member nodes
         * for the operator.
         * <p>
         * This annotation is required for such operators since the set of known
         * nodes of a given type (such as all data services) can otherwise
         * change at runtime.
         * 
         * @todo Move onto an interface parallel to {@link IShardwisePipelineOp}
         */
        String MEMBER_SERVICES = "memberServices";

    }

}
