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

import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.pipeline.JoinMasterTask;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.striterator.IKeyOrder;

/**
 * An immutable constraint on the elements visited using an {@link IAccessPath}.
 * The slots in the predicate corresponding to variables are named and those
 * names establish binding patterns access {@link IPredicate}s in the context
 * of a {@link IRule}. Access is provided to slots by ordinal index regardless
 * of whether or not they are named variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPredicate<E> extends BOp, Cloneable, Serializable {

    /**
     * Interface declaring well known annotations.
     */
    public interface Annotations extends BOp.Annotations {

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
         */
        String CONSTRAINT = "constraint";

        /**
         * Expander pattern.
         */
        String EXPANDER = "expander";

        /**
         * The partition identifier -or- <code>-1</code> if the predicate does
         * not address a specific shard.
         */
        String PARTITION_ID = "partitionId";
    }
    
    /**
     * Resource identifier (aka namespace) identifies the {@link IRelation}
     * associated with this {@link IPredicate}.
     * <p>
     * This is more or less ignored when the {@link IRule} is executed as a
     * query.
     * <p>
     * When the {@link IRule} is executed as an {@link ActionEnum#Insert} or
     * {@link ActionEnum#Delete} then this identifies the target
     * {@link IMutableRelation} on which the computed {@link ISolution}s will be
     * written.
     * 
     * @throws IllegalStateException
     *             if there is more than on element in the view.
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
     * <p>
     * Note: The ability to specify an index partition identifier for a
     * predicate is provided in support of scale-out JOIN strategies. The
     * {@link AccessPath} and the {@link JoinMasterTask} are both aware
     * of this property. The {@link JoinMasterTask} sets the partition
     * identifier in order to request an access path backed by the name of the
     * local index object on a {@link DataService} rather than the name of the
     * scale-out index.
     * <p>
     * The index partition can not be specified until a choice has been made
     * concerning which {@link IAccessPath} to use for a predicate without an
     * index partition constraint. The {@link IAccessPath} choice is therefore
     * made by the {@link IEvaluationPlan} using the scale-out index view and an
     * {@link AbstractScaleOutFederation#locatorScan(String, long, byte[], byte[], boolean)}
     * is used to identify the index partitions on which the {@link IAccessPath}
     * will read. The index partition is then set on a constrained
     * {@link IPredicate} for each target index partition and the JOINs are then
     * distributed to the {@link DataService}s on which those index partitions
     * reside.
     * 
     * @return The index partition identifier -or- <code>-1</code> if the
     *         predicate is not locked to a specific index partition.
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
     * @return <code>true</code> iff this predicate is optional when
     *         evaluating a JOIN.
     */
    public boolean isOptional();

    /**
     * Returns the object that may be used to selectively override the
     * evaluation of the predicate.
     * 
     * @return The {@link ISolutionExpander}.
     * 
     * @todo replace with {@link ISolutionExpander#getAccessPath(IAccessPath)},
     *       which is the only method declared by {@link ISolutionExpander}.
     */
    public ISolutionExpander<E> getSolutionExpander();
    
    /**
     * An optional constraint on the visitable elements.
     * 
     * @todo rename as get(Element)Filter().
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
    
}
