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
 * An operator, such as a constant, variable, join, sort, filter, etc. Operators
 * are organized in a tree of operators. The arity of an operator is the number
 * of children declared by that operator class. The children of an operator are
 * themselves operators and traversal is supported between a parent and its
 * children. In addition to their arguments, operators may have a variety of
 * annotations, including those specific to an operator (such as the maximum
 * number of iterators for a closure operator), those shared by many operators
 * (such as set of variables which are selected by a join or distributed hash
 * table), or those shared by all operators (such as a cost model).
 * <p>
 * Operators are mutable, thread-safe, {@link Serializable} to facilitate
 * distributed computing, and {@link Cloneable} to facilitate non-destructive
 * tree rewrites.
 * <p>
 * What follows is a summary of some of the more important kinds of operations.
 * For each type of operation, there may be several implementations. One common
 * way in which implementations of the same operator may differ is whether they
 * are designed for low-volume selective queries or high volume unselective
 * queries.
 * <dl>
 * <dt>JOINs</dt>
 * <dd></dd>
 * <dt>Mapping binding sets across shards (key-range partitions) or nodes (hash
 * partitioned)</dt>
 * <dd></dd>
 * <dt>Predicates and access paths</dt>
 * <dd></dd>
 * <dt>SORT</dt>
 * <dd></dd>
 * <dt>DISTINCT</dt>
 * <dd></dd>
 * <dt>Element filters</dt>
 * <dd></dd>
 * <dt>Rule constraints</dt>
 * <dd></dd>
 * <dt>Binding set filters (removing binding sets which are not required outside
 * of some context)</dt>
 * <dd></dd>
 * <dt>Identifiers for sinks to which binding sets can be written and
 * conditional routing of binding sets, for example based on variable value or
 * type or join success or failure</dt>
 * <dd></dd>
 * <dt>Sequential or iterative programs.</dt>
 * <dd></dd>
 * <dt>Creating or destroying transient or persistent resources (graphs, tables,
 * DHTs, etc). Such life cycle operators dominate the subtree within which the
 * resource will be utilized.</dt>
 * <dd></dd>
 * <dt>Export of proxy objects, especially for query or mutation buffers.</dt>
 * <dd></dd>
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo javadoc review and reconcile with notes.
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

//    /**
//     * Bind an argument of the operation.
//     * 
//     * @param index
//     *            The argument index in [0:{@link #arity()}-1].
//     * @param newValue
//     *            The bound value.
//     * 
//     * @return A copy of the operation having the bound value for the argument.
//     */
//    BOp<?> setArg(int index, BOp<?> newValue);

//    /**
//     * Return the value of the named operator annotation.
//     * 
//     * @param name
//     *            The annotation name.
//     *            
//     * @return The value of the named operator annotation.
//     */
//    Object getProperty(String name);

//    /**
//     * Set the value of the named operator annotation.
//     * 
//     * @param name
//     *            The annotation name.
//     * @param newValue
//     *            The new value for the named annotation,
//     * 
//     * @return The old value of the named operator annotation.
//     */
//    Object setProperty(String name,Object newValue);
    
//    /**
//     * Return the type constraint on the specified argument.
//     * 
//     * @param index
//     *            The argument index in [0:{@link #arity()}-1].
//     *            
//     * @return The type constraint on that argument.
//     */
//    Class<?> getArgType(int index);

//    /**
//     * The type of the values produced by the operation (Constant or variable,
//     * primitive?, relation, triple store, index, file, bat, ...).
//     */
//    Class<T> getResultType();

//    /**
//     * @TODO There needs to be some simple evaluation path for things such as
//     *       native SPARQL operations. This is currently
//     *       {@link IConstraint#accept(IBindingSet)}, which returns a truth
//     *       value.  This seems quite adequate.
//     */
//    boolean accept(IBindingSet bset);

//    /**
//     * The #of arguments to this operation which are variables. This method does
//     * not report on variables in child nodes nor on variables in attached
//     * {@link IConstraint}, etc.
//     */
//    int getVariableCount();

//    /**
//     * Return an iterator visiting those arguments of this operator which are
//     * variables. This method does not report on variables in child nodes nor on
//     * variables in attached {@link IConstraint}, etc.
//     */
//    Iterator<IVariable<?>> getVariables();

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
