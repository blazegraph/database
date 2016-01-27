/*

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
 * Created on Jun 19, 2008
 */

package com.bigdata.bop.ap;

import java.util.Map;

import com.bigdata.bop.AbstractAccessPathOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.CoreBaseBOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IAccessPathExpander;
import com.bigdata.striterator.IKeyOrder;

import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.NOPFilter;

/**
 * A generic implementation of an immutable {@link IPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Predicate<E> extends AbstractAccessPathOp<E> implements
        IPredicate<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends IPredicate.Annotations {
        
    }

    /**
     * Required shallow copy constructor.
     */
    public Predicate(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public Predicate(final Predicate<E> op) {
        super(op);
    }

    /**
     * Variable argument version of the shallow constructor.
     * @param vars
     * @param annotations
     */
    public Predicate(final BOp[] args, final NV... annotations) {
        
        super(args, NV.asMap(annotations));
        
    }
    
    /**
     * Disallows <code>null</code> in any position.
     * @param args
     */
    @Override
	protected void checkArgs(BOp[] args) {
		for (BOp a : args) {
			if (a == null)
				throw new IllegalArgumentException();
		}
	}
    
	/**
	 * Simplified ctor (for lex predicates).
	 * 
	 * @param values
	 *            The values (order is important!).
	 * @param relationName
	 *            Identifies the relation to be queried.
	 */
	public Predicate(final IVariableOrConstant<?>[] values,
			final String relationName, final long timestamp) {

		this(values, relationName, -1/* partitionId */, false/* optional */,
				null/* constraint */, null/* expander */, ITx.READ_COMMITTED);

	}

    /**
     * 
     * @param relationName
     *            The namespace of the relation.
     * @param partitionId
     *            The index partition constraint -or- <code>-1</code> if there
     *            is no index partition constraint.
     * @param values
     *            The values (order is important!).
     * @param optional
     *            true iff the predicate is optional when evaluated in a JOIN.
     * @param constraint
     *            An optional constraint.
     * @param expander
     *            Allows selective override of the predicate evaluation.
     */
    public Predicate(final IVariableOrConstant<?>[] values,
            final String relationName, final int partitionId,
            final boolean optional, final IElementFilter<E> constraint,
            final IAccessPathExpander<E> expander, final long timestamp) {

        this(values, NV.asMap(new NV[] {//
                new NV(Annotations.RELATION_NAME,new String[]{relationName}),//
                new NV(Annotations.PARTITION_ID,partitionId),//
                new NV(Annotations.OPTIONAL,optional),//
                new NV(Annotations.INDEX_LOCAL_FILTER,
                    ElementFilter.newInstance(constraint)),//
                new NV(Annotations.ACCESS_PATH_EXPANDER,expander),//
                new NV(Annotations.TIMESTAMP, timestamp)
        }));
        
        if (relationName == null)
            throw new IllegalArgumentException();

        if (partitionId < -1)
            throw new IllegalArgumentException();

        if (values == null)
            throw new IllegalArgumentException();
        
    }
    
    public String getOnlyRelationName() {
        
        final String[] relationName = (String[]) getRequiredProperty(Annotations.RELATION_NAME);
        
        if (relationName.length != 1)
            throw new IllegalStateException();

        return relationName[0];
        
    }
    
    public String getRelationName(final int index) {

        final String[] relationName = (String[]) getRequiredProperty(Annotations.RELATION_NAME);

        return relationName[index];
        
//        throw new UnsupportedOperationException();
        
    }

    public int getRelationCount() {
        
        final String[] relationName = (String[]) getRequiredProperty(Annotations.RELATION_NAME);
      
        return relationName.length;
        
    }

    public Predicate<E> setRelationName(final String[] relationName) {
        
//      throw new UnsupportedOperationException();
      final Predicate<E> tmp = this.clone();

      tmp._setProperty(Annotations.RELATION_NAME, relationName);

      return tmp;
      
  }

    public int getPartitionId() {
        
        return (Integer) getProperty(Annotations.PARTITION_ID,
                Annotations.DEFAULT_PARTITION_ID);

    }
    
    @SuppressWarnings("rawtypes")
    public IVariableOrConstant get(final int index) {
        
        return (IVariableOrConstant<?>) super.get(index);
        
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public IConstant<?> get(final E e, final int index) {

        return new Constant(((IElement) e).get(index));

    }
    
    final public boolean isOptional() {

        return (Boolean) getProperty(Annotations.OPTIONAL,Annotations.DEFAULT_OPTIONAL);
        
    }

//    /**
//     * 
//     * @deprecated This is being replaced by two classes of filters. One which
//     *             is always evaluated local to the index and one which is
//     *             evaluated in the JVM in which the access path is evaluated
//     *             once the {@link ITuple}s have been resolved to elements of
//     *             the relation.
//     */
//    @SuppressWarnings("unchecked")
//    final public IElementFilter<E> getConstraint() {
//
//        return (IElementFilter<E>) getProperty(Annotations.CONSTRAINT);
//
//    }

//    public IConstraint[] constraints() {
//
//        return getProperty(IPredicate.Annotations.CONSTRAINTS, null/* defaultValue */);
//
//    }

    final public IFilter getIndexLocalFilter() {

        return (IFilter) getProperty(Annotations.INDEX_LOCAL_FILTER);

    }

    final public IFilter getAccessPathFilter() {

        return (IFilter) getProperty(Annotations.ACCESS_PATH_FILTER);

    }
    
    @SuppressWarnings("unchecked")
    final public IAccessPathExpander<E> getAccessPathExpander() {
        
        return (IAccessPathExpander<E>) getProperty(Annotations.ACCESS_PATH_EXPANDER);
        
    }

    final public int getVariableCount() {

        int nvars = 0;
        
        final int arity = arity();

        for (int i = 0; i < arity; i++) {

            if (get(i) instanceof IVariable<?>)
                nvars++;
            
        }
        
        return nvars;

    }

    final public boolean isFullyBound(final IKeyOrder<E> keyOrder) {
        
        return getVariableCount(keyOrder) == 0;
        
    }

    final public int getVariableCount(final IKeyOrder<E> keyOrder) {
        int nunbound = 0;
        final int keyArity = keyOrder.getKeyArity();
        for (int keyPos = 0; keyPos < keyArity; keyPos++) {
            final int index = keyOrder.getKeyOrder(keyPos);
            final IVariableOrConstant<?> t = get(index);
            if (t == null || t.isVar()) {
                nunbound++;
            }
        }
        return nunbound;
    }

    final public boolean isRemoteAccessPath() {
        return getProperty(Annotations.REMOTE_ACCESS_PATH,
                Annotations.DEFAULT_REMOTE_ACCESS_PATH);
    }
   
    public Predicate<E> asBound(final IVariable<?> var, final IConstant<?> val) {

        return asBound(new ListBindingSet(new IVariable[] { var },
                new IConstant[] { val }));
        
    }

    /**
     * Fast path for as-bound.
     * <p>
     * This reuses the annotations map since we know that the annotations will
     * not be changed by {@link #asBound(IBindingSet)}. That provides a
     * significant reduction in heap churn.
     */
    public Predicate<E> asBound(final IBindingSet bindingSet) {

    	Map<String, Object> anns = annotationsRef();
    	
        final RangeBOp rangeBOp = range();

        if (rangeBOp != null) {

	        /*
	         * Attempt to evaluate the RangeBOp.
	         */
	        final RangeBOp asBound = rangeBOp.asBound(bindingSet);
	
	        // reference test is ok here
	        if (asBound != rangeBOp) {
	        	
	        	// make a copy of the anns
	        	// set the asBound range on it
	        	// use those for the pred below
	        	
	        	anns = annotationsCopy();
	        	anns.put(Annotations.RANGE, asBound);
	        	
	        }
	        
        }
        
//		/*
//		 * If we have a range we need to modify the annotations with the asBound
//		 * range.
//		 */
//    	final Map<String, Object> anns = 
//    		range() != null ? annotationsCopy() : annotationsRef();
    	
        return new Predicate<E>(argsCopy(), anns)
                ._asBound(bindingSet);

    }

    /**
     * Override any unbound variables for which we were giving bindings.
     */
    protected final Predicate<E> _asBound(final IBindingSet bindingSet) {

        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final int arity = arity();
        
        for (int i = 0; i < arity; i++) {

            final IVariableOrConstant<?> t = (IVariableOrConstant<?>) get(i);

            if (t.isConstant())
                continue;

            final IVariable<?> var = (IVariable<?>) t;

            final IConstant<?> val = bindingSet.get(var);

            if (val == null) {
                // still unbound.
                continue;
            }

            /*
             * asBound() needs to associate the constant with the variable in
             * order for the binding to be propagated to the variable. This
             * was not true historically when we visited IElements on access
             * paths, but it is true now that we are visting IBindingSets on
             * access paths.
             * 
             * See
             * https://sourceforge.net/apps/trac/bigdata/ticket/209#comment:7.
             */
            _set(i, new Constant(var, val.get()));
//            tmp._set(i, val.clone());

        }
        
//        /*
//         * FIXME When putting the RangeBOp back into use, be very careful of the
//         * optimization in asBound(). Predicate#asBound() is NOT making a copy
//         * of the annotations map. The code below will therefore cause a
//         * modification to the source predicate's annotations, not the copy's.
//         * This violates the "effectively immutable" contract.
//         * 
//         * What the code should probably do is check in asBound() and only use
//         * the code path which avoids the annotations map copy when the RANGE is
//         * not to be set on the new Predicate instance.
//         */
//        
//        final RangeBOp rangeBOp = range();
//        
//        if (rangeBOp != null) {
//
//	        /*
//	         * Attempt to evaluate the RangeBOp.
//	         */
//	        final RangeBOp asBound = rangeBOp.asBound(bindingSet);
//	
//	        _setProperty(Annotations.RANGE, asBound);
//        }

        return this;

    }
    
    public Object asBound(final int index, final IBindingSet bindingSet) {

        return get(index).get(bindingSet);

    }

    final public RangeBOp range() {
        
        return (RangeBOp) getProperty(Annotations.RANGE);
        
    }

    @SuppressWarnings("unchecked")
    public IKeyOrder<E> getKeyOrder() {

        final Object o = getProperty(Annotations.KEY_ORDER);

        if (o == null)
            return null;
        
        if (o instanceof IKeyOrder<?>)
            return (IKeyOrder<E>) o;

        return (IKeyOrder<E>) SPOKeyOrder.fromString((String) o);

    }

//    public Predicate<E> setKeyOrder(final IKeyOrder<E> keyOrder) {
//
//        final Predicate<E> tmp = this.clone();
//
//        tmp._setProperty(Annotations.KEY_ORDER, keyOrder);
//
//        return tmp;
//
//    }

    /**
     * Overridden to provide a fast path clone(). This avoids a constructor
     * lookup via reflection in favor of simply creating a new {@link Predicate}
     * instance. However, subclasses MUST override this method. This change was
     * introduced on 11/17/2011 when a profiler showed a 13% of all time related
     * to a join intensive process in {@link CoreBaseBOp#clone()} using
     * reflection to make a copy of an {@link #asBound(IBindingSet)} predicate.
     */
    @Override
//    @SuppressWarnings("unchecked")
    public Predicate<E> clone() {

        // Fast path for clone().
        return new Predicate<E>(this);
//        return (Predicate<E>) super.clone();
        
    }

    public Predicate<E> setPartitionId(final int partitionId) {

        final Predicate<E> tmp = this.clone();

        tmp._setProperty(Annotations.PARTITION_ID, partitionId);

        return tmp;

    }

    public Predicate<E> setBOpId(final int bopId) {

        final Predicate<E> tmp = this.clone();

        tmp._setProperty(Annotations.BOP_ID, bopId);

        return tmp;

    }

    public Predicate<E> setTimestamp(final long timestamp) {

        final Predicate<E> tmp = this.clone();

        tmp._setProperty(Annotations.TIMESTAMP, timestamp);

        return tmp;

    }

    @SuppressWarnings("rawtypes")
    public Predicate<E> setArg(final int index, final IVariableOrConstant arg) {

        final Predicate<E> tmp = this.clone();

        tmp._set(index, arg);

        return tmp;

    }

    /**
     * Add an {@link Annotations#INDEX_LOCAL_FILTER}. When there is a filter for
     * the named property, the filters are combined. Otherwise the filter is
     * set.
     * 
     * @param filter
     *            The filter.
     *            
     * @return The new predicate to which that filter was added.
     */
    public Predicate<E> addIndexLocalFilter(final IFilter filter) {

        final Predicate<E> tmp = this.clone();

        tmp.addFilter(Annotations.INDEX_LOCAL_FILTER, filter);

        return tmp;

    }

    /**
     * Add an {@link Annotations#INDEX_LOCAL_FILTER}. When there is a filter for
     * the named property, the filters are combined. Otherwise the filter is
     * set.
     * 
     * @param filter
     *            The filter.
     *            
     * @return The new predicate to which that filter was added.
     */
    public Predicate<E> addAccessPathFilter(final IFilter filter) {

        final Predicate<E> tmp = this.clone();

        tmp.addFilter(Annotations.ACCESS_PATH_FILTER, filter);

        return tmp;

    }

    /**
     * Private method used to add a filter. When there is a filter for the named
     * property, the filters are combined. Otherwise the filter is set. DO NOT
     * use this outside of the copy-on-write helper methods.
     * 
     * @param name
     *            The property name.
     * @param filter
     *            The filter.
     */
    private void addFilter(final String name, final IFilter filter) {
        
        if (filter == null)
            throw new IllegalArgumentException();

        final IFilter current = (IFilter) getProperty(name);

        if (current == null) {
            
            /*
             * Set the filter.
             */
            _setProperty(name, filter);
            
        } else {

            /*
             * Wrap the filter.
             */
            _setProperty(name, new NOPFilter().addFilter(current)
                    .addFilter(filter));
        }

    }
    
    /**
     * Strengthened return type.
     * <p>
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Predicate<E> clearAnnotations(final String[] names) {
        
        return (Predicate<E>) super.clearAnnotations(names);
        
    }
    
    public String toString() {
        
        return toString(null/* bindingSet */);
        
    }
    
    public String toString(final IBindingSet bindingSet) {

        final int arity = arity();
        
        final StringBuilder sb = new StringBuilder();

        sb.append(getClass().getName());

        final Integer bopId = (Integer) getProperty(Annotations.BOP_ID);
        
        if (bopId != null) {
        
            sb.append("[" + bopId + "]");
            
        }

        sb.append("(");

        for (int i = 0; i < arity; i++) {

            if (i > 0)
                sb.append(", ");

            final IVariableOrConstant<?> v = get(i);

            sb.append(v == null ? null : v.isConstant() ? v.toString() : (v
                    + "=" + (bindingSet == null ? null : bindingSet
                    .get((IVariable<?>) v))));

        }

        sb.append(")");
        annotationsToString(sb);
        return sb.toString();

    }

//    public final boolean isMutation() {
//
//        return getProperty(IPredicate.Annotations.MUTATION,
//                IPredicate.Annotations.DEFAULT_MUTATION);
//
//    }

    public final long getTimestamp() {

        return (Long) getRequiredProperty(IPredicate.Annotations.TIMESTAMP);

    }

}
