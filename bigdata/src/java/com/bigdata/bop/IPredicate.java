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
 * Created on Jun 19, 2008
 */

package com.bigdata.bop;

import java.io.Serializable;

import com.bigdata.bop.ap.filter.BOpFilterBase;
import com.bigdata.bop.ap.filter.BOpTupleFilter;
import com.bigdata.bop.ap.filter.DistinctFilter;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.filter.TupleFilter;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.pipeline.JoinMasterTask;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.FilterBase;
import cutthecrap.utils.striterators.IFilter;

/**
 * An immutable constraint on the elements visited using an {@link IAccessPath}.
 * The slots in the predicate corresponding to variables are named and those
 * names establish binding patterns access {@link IPredicate}s. Access is
 * provided to slots by ordinal index regardless of whether or not they are
 * named variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPredicate<E> extends BOp, Cloneable, Serializable {

    /**
     * Interface declaring well known annotations.
     */
    public interface Annotations extends BOp.Annotations, BufferAnnotations {

        /**
         * The name of the relation on which the predicate will read.
         * 
         * FIXME Change this to be a scalar value. It is currently an array for
         * backwards compatibility.
         */
        String RELATION_NAME = "relationName";

        /**
         * The {@link IKeyOrder} which will be used to read on the relation.
         * <p>
         * Note: This is generally assigned by the query optimizer. The query
         * optimizer considers the possible indices for each {@link IPredicate}
         * in a query and the possible join orders and then specifies the order
         * of the joins and the index to use for each join.
         */
        String KEY_ORDER = "keyOrder";

        /**
         * <code>true</code> iff the predicate is optional (the right operand of
         * a left join).
         * 
         * @deprecated This flag is being moved to the join operator.
         */
        String OPTIONAL = "optional";

        /**
         * Constraints on the elements read from the relation.
         * 
         * @deprecated This is being replaced by two classes of filters. One
         *             which is always evaluated local to the index and one
         *             which is evaluated in the JVM in which the access path is
         *             evaluated once the {@link ITuple}s have been resolved to
         *             elements of the relation.
         */
        String CONSTRAINT = "constraint";

        /**
         * An optional {@link BOpFilterBase} that will be evaluated local to the
         * to the index. When the index is remote, the filter will be sent to
         * the node on which the index resides and evaluated there. This makes
         * it possible to efficiently filter out tuples which are not of
         * interest for a given access path.
         * <p>
         * Note: The filter MUST NOT change the type of data visited by the
         * iterator - it must remain an {@link ITupleIterator}. An attempt to
         * change the type of the visited objects will result in a runtime
         * exception.
         * <p>
         * Note: This filter must be "aware" of the reuse of tuples within tuple
         * iterators. See {@link BOpTupleFilter} and {@link TupleFilter} for
         * starting points.
         * <p>
         * You can chain {@link BOpFilterBase} filters by nesting them inside of
         * one another. You can chain {@link FilterBase} filters together as
         * well.
         * 
         * @see #ACCESS_PATH_FILTER
         * 
         * @see IRangeQuery#rangeIterator(byte[], byte[], int, int, IFilter)
         */
        String INDEX_LOCAL_FILTER = "indexLocalFilter";

        /**
         * An optional {@link BOpFilterBase} to be applied to the elements of
         * the relation as they are materialized from the index. {@link ITuple}s
         * are automatically resolved into relation "elements" before this
         * filter is applied.
         * <p>
         * Unlike {@link #INDEX_LOCAL_FILTER}, this an
         * {@link #ACCESS_PATH_FILTER} is never sent to a remote index for
         * evaluation. This makes it possible to impose {@link DistinctFilter}
         * across a {@link #REMOTE_ACCESS_PATH}.
         * <p>
         * You can chain {@link BOpFilterBase} filters by nesting them inside of
         * one another. You can chain {@link FilterBase} filters together as
         * well.
         */
        String ACCESS_PATH_FILTER = "accessPathFilter";

        /**
         * Expander pattern.
         */
        String EXPANDER = "expander";

        /**
         * The partition identifier -or- <code>-1</code> if the predicate does
         * not address a specific shard.
         */
        String PARTITION_ID = "partitionId";
        
        int DEFAULT_PARTITION_ID = -1;

        /**
         * Boolean option determines whether the predicate will use a local
         * access path or a remote access path (default
         * {@value #DEFAULT_REMOTE_ACCESS_PATH}).
         * <p>
         * <em>Local access paths</em> are much more efficient and should be
         * used for most purposes. However, it is not possible to impose certain
         * kinds of filters on a sharded or hash partitioned operations across
         * local access paths. In particular, a DISTINCT filter can not be
         * imposed using sharded or hash partitioned.
         * <p>
         * When the access path is local, the parent operator must be annotated
         * to use a {@link BOpEvaluationContext#SHARDED shard wise} or
         * {@link BOpEvaluationContext#HASHED node-wise} mapping of the binding
         * sets.
         * <p>
         * <em>Remote access paths</em> use a scale-out index view. This view
         * makes the scale-out index appear as if it were monolithic rather than
         * sharded or hash partitioned. The monolithic view of a scale-out index
         * can be used to impose a DISTINCT filter since all tuples will flow
         * back to the caller.
         * <p>
         * When the access path is remote, the parent operator should use
         * {@link BOpEvaluationContext#ANY} to prevent the binding sets from
         * being moved around when the access path is remote.
         * 
         * @see BOpEvaluationContext
         */
        String REMOTE_ACCESS_PATH = "remoteAccessPath";
        
        boolean DEFAULT_REMOTE_ACCESS_PATH = false;
        
        /**
         * If the estimated rangeCount for an {@link AccessPath#iterator()} is
         * LTE this threshold then use a fully buffered (synchronous) iterator.
         * Otherwise use an asynchronous iterator whose capacity is governed by
         * {@link #CHUNK_OF_CHUNKS_CAPACITY}.
         * 
         * @see #DEFAULT_FULLY_BUFFERED_READ_THRESHOLD
         */
        String FULLY_BUFFERED_READ_THRESHOLD = IPredicate.class.getName()
                + ".fullyBufferedReadThreshold";

        /**
         * Default for {@link #FULLY_BUFFERED_READ_THRESHOLD}.
         * 
         * @todo Experiment with this. It should probably be something close to
         *       the branching factor, e.g., 100.
         */
        int DEFAULT_FULLY_BUFFERED_READ_THRESHOLD = 100;

        /**
         * Flags for the iterator ({@link IRangeQuery#KEYS},
         * {@link IRangeQuery#VALS}, {@link IRangeQuery#PARALLEL}).
         * <p>
         * Note: The {@link IRangeQuery#PARALLEL} flag here is an indication
         * that the iterator may run in parallel across the index partitions.
         * This only effects scale-out and only for simple triple patterns since
         * the pipeline join does something different (it runs inside the index
         * partition using the local index, not the client's view of a
         * distributed index).
         * 
         * @see #DEFAULT_FLAGS
         */
        String FLAGS = IPredicate.class.getName() + ".flags";

        /**
         * The default flags will visit the keys and values of the non-deleted
         * tuples and allows parallelism in the iterator (when supported).
         * 
         * @todo consider making parallelism something that the query planner
         *       must specify explicitly.
         */
        final int DEFAULT_FLAGS = IRangeQuery.KEYS | IRangeQuery.VALS
                | IRangeQuery.PARALLEL;

    }
    
    /**
     * Resource identifier (aka namespace) identifies the {@link IRelation}
     * associated with this {@link IPredicate}.
     * 
     * @throws IllegalStateException
     *             if there is more than on element in the view.
     * 
     * @see Annotations#RELATION_NAME
     * 
     * @todo Rename as getRelationName()
     */
    public String getOnlyRelationName();

    /**
     * Return the ith element of the relation view. The view is an ordered array
     * of resource identifiers that describes the view for the relation.
     * 
     * @param index
     *            The index into the array of relation names in the view.
     * 
     * @deprecated Unions of predicates must be handled explicitly as a union of
     *             pipeline operators reading against the different predicate.
     */
    public String getRelationName(int index);
    
    /**
     * The #of elements in the relation view.
     * 
     * @deprecated per {@link #getRelationName(int)}.
     */
    public int getRelationCount();
    
    /**
     * The index partition identifier and <code>-1</code> if no partition
     * identifier was specified.
     * 
     * @return The index partition identifier -or- <code>-1</code> if the
     *         predicate is not locked to a specific index partition.
     * 
     * @see Annotations#PARTITION_ID
     * 
     * @see PartitionLocator
     * @see AccessPath
     * @see JoinMasterTask
     */
    public int getPartitionId();
    
    /**
     * Sets the index partition identifier constraint.
     * 
     * @param partitionId
     *            The index partition identifier.
     *            
     * @return The constrained {@link IPredicate}.
     * 
     * @throws IllegalArgumentException
     *             if the index partition identified is a negative integer.
     * @throws IllegalStateException
     *             if the index partition identifier was already specified.
     * @see Annotations#PARTITION_ID
     */
    public IPredicate<E> setPartitionId(int partitionId);

    /**
     * <code>true</code> iff the predicate is optional when evaluated as the
     * right-hand side of a join. An optional predicate will match once after
     * all matches in the data have been exhausted. By default, the match will
     * NOT bind any variables that have been determined to be bound by the
     * predicate based on the computed {@link IEvaluationPlan}.
     * <p>
     * For mutation, some {@link IRelation}s may require that all variables
     * appearing in the head are bound. This and similar constraints can be
     * enforced using {@link IConstraint}s on the {@link IRule}.
     * <p>
     * More control over the behavior of optionals may be gained through the use
     * of an {@link ISolutionExpander} pattern.
     * 
     * @return <code>true</code> iff this predicate is optional when evaluating
     *         a JOIN.
     * 
     * @deprecated By {@link PipelineJoin.Annotations#OPTIONAL}
     */
    public boolean isOptional();

    /**
     * Returns the object that may be used to selectively override the
     * evaluation of the predicate.
     * 
     * @return The {@link ISolutionExpander}.
     * 
     * @see Annotations#EXPANDER
     * 
     * @todo replace with {@link ISolutionExpander#getAccessPath(IAccessPath)},
     *       which is the only method declared by {@link ISolutionExpander}.
     */
    public ISolutionExpander<E> getSolutionExpander();

    /**
     * An optional constraint on the visitable elements.
     * 
     * @see Annotations#CONSTRAINT
     * 
     * @deprecated This is being replaced by two classes of filters. One which
     *             is always evaluated local to the index and one which is
     *             evaluated in the JVM in which the access path is evaluated
     *             once the {@link ITuple}s have been resolved to elements of
     *             the relation.
     */
    public IElementFilter<E> getConstraint();

    /**
     * Return the {@link IKeyOrder} assigned to this {@link IPredicate} by the
     * query optimizer.
     * 
     * @return The assigned {@link IKeyOrder} or <code>null</code> if the query
     *         optimizer has not assigned the {@link IKeyOrder} yet.
     * 
     * @see Annotations#KEY_ORDER
     */
    public IKeyOrder<E> getKeyOrder();
    
    /**
     * Set the {@link IKeyOrder} annotation on the {@link IPredicate}, returning
     * a new {@link IPredicate} in which the annotation takes on the given
     * binding.
     * 
     * @param keyOrder
     *            The {@link IKeyOrder}.
     * 
     * @return The new {@link IPredicate}.
     * 
     * @see Annotations#KEY_ORDER
     */
    public IPredicate<E> setKeyOrder(final IKeyOrder<E> keyOrder);
    
    /**
     * Figure out if all positions in the predicate which are required to form
     * the key for this access path are bound in the predicate.
     */
    public boolean isFullyBound(IKeyOrder<E> keyOrder);

    /**
     * @deprecated This is only used in a few places, which should probably use
     *             {@link BOpUtility#getArgumentVariableCount(BOp)} instead.
     */
    public int getVariableCount();
    
    /**
     * The #of arguments in the predicate required for the specified
     * {@link IKeyOrder} which are unbound.
     * 
     * @param keyOrder
     *            The key order.
     *            
     * @return The #of unbound arguments for that {@link IKeyOrder}.
     */
    public int getVariableCount(IKeyOrder<E> keyOrder);
    
    /**
     * Return <code>true</code> if this is a remote access path.
     * 
     * @see Annotations#REMOTE_ACCESS_PATH
     */
    public boolean isRemoteAccessPath();
    
    /**
     * Return the variable or constant at the specified index.
     * 
     * @param index
     *            The index.
     * 
     * @return The variable or constant at the specified index.
     * 
     * @throws IllegalArgumentException
     *             if the index is less than zero or GTE the {@link #arity()} of
     *             the {@link IPredicate}.
     */
    /*
     * Note: the return value can not be parameterized without breaking code.
     */
    @SuppressWarnings("unchecked")
    public IVariableOrConstant get(int index);

    /**
     * Return the asBound value at the specified index for the given element.
     * This method does not consider the bindings of the predicate instance.
     * <p>
     * Note: there is no general means available to implement this method of an
     * awareness of the internal structure of the element type. General purpose
     * record types, such as GOM or relation records, can generally implement
     * this method in the context of the "schema" imposed by the predicate.
     * 
     * @param e
     *            The element, which must implement {@link IElement}.
     * @param index
     *            The index.
     * 
     * @return The value.
     * 
     * @throws UnsupportedOperationException
     *             If this operation is not supported by the {@link IPredicate}
     *             implementation or for the given element type.
     * 
     * @deprecated by {@link IElement#get(int)} which does exactly what this
     *             method is trying to do.
     */
    public IConstant<?> get(E e, int index);

    /**
     * A copy of this {@link IPredicate} in which zero or more variables have
     * been bound to constants using the given {@link IBindingSet}.
     */
    public IPredicate<E> asBound(IBindingSet bindingSet);

    /**
     * Extract the as bound value from the predicate. When the predicate is not
     * bound at that index, the value of the variable is taken from the binding
     * set.
     * 
     * @param index
     *            The index into that predicate.
     * @param bindingSet
     *            The binding set.
     * 
     * @return The bound value -or- <code>null</code> if no binding is available
     *         (the predicate is not bound at that index and the variable at
     *         that index in the predicate is not bound in the binding set).
     * 
     * @throws IndexOutOfBoundsException
     *             unless the <i>index</i> is in [0:{@link #arity()-1], inclusive.
     * @throws IllegalArgumentException
     *             if the <i>bindingSet</i> is <code>null</code>.
     */
    public Object asBound(int index, IBindingSet bindingSet);
    
    /**
     * A copy of this {@link IPredicate} in which the <i>relationName</i>(s)
     * replace the existing set of relation name(s).
     * 
     * @param relationName
     *            The relation name(s).
     * 
     * @throws IllegalArgumentException
     *             if <i>relationName</i> is empty.
     * @throws IllegalArgumentException
     *             if <i>relationName</i> is <code>null</code>
     * @throws IllegalArgumentException
     *             if any element of <i>relationName</i> is <code>null</code>
     * 
     * @deprecated This will be modified to use a scalar relation name per
     *             {@link #getOnlyRelationName()}.
     */
    public IPredicate<E> setRelationName(String[] relationName);
    
    /**
     * Representation of the predicate without variable bindings.
     */
    public String toString();
    
    /**
     * Representation of the predicate with variable bindings.
     * 
     * @param bindingSet
     *            The variable bindings
     */
    public String toString(IBindingSet bindingSet);

    /**
     * Compares the bindings of two predicates for equality.
     * 
     * @param other
     *            Another predicate.
     *            
     * @return true iff the predicate have the same arity and their ordered
     *         bindings are the same. when both predicates have a variable at a
     *         given index, the names of the variables must be the same.
     */
    public boolean equals(Object other);

    /**
     * The hash code is defined as
     * 
     * <pre>
     * get(0).hashCode()*31&circ;(n-1) + get(1).hashCode()*31&circ;(n-2) + ... + get(n-1).hashCode()
     * </pre>
     * 
     * using <code>int</code> arithmetic, where <code>n</code> is the
     * {@link #arity()} of the predicate, and <code>^</code> indicates
     * exponentiation.
     * <p>
     * Note: This is similar to how {@link String#hashCode()} is defined.
     */
    public int hashCode();
    
    /**
     * Sets the {@link com.bigdata.bop.BOp.Annotations#BOP_ID} annotation.
     * 
     * @param bopId
     *            The bop id.
     *            
     * @return The newly annotated {@link IPredicate}.
     */
    public IPredicate<E> setBOpId(int bopId);

}
