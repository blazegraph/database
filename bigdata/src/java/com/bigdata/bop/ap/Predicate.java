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

package com.bigdata.bop.ap;

import java.util.Map;

import com.bigdata.bop.AbstractAccessPathOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.bindingSet.ArrayBindingSet;
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
    public Predicate(final BOp[] values, final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public Predicate(final Predicate<E> op) {
        super(op);
    }

    /**
     * Variable argument version of the shallow constructor.
     * @param vars
     * @param annotations
     */
    public Predicate(final BOp[] vars, final NV... annotations) {
        
        super(vars, NV.asMap(annotations));
        
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
    
//    /**
//     * Simplified ctor.
//     * 
//     * @param values
//     *            The values (order is important!).
//     * @param relationName
//     *            Identifies the relation to be queried.
//     */
//    public Predicate(final IVariableOrConstant<?>[] values,
//            final String relationName) {
//
//        this(values, relationName, -1/* partitionId */, false/* optional */,
//                null/* constraint */, null/* expander */, ITx.READ_COMMITTED);
//
//    }

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
    
    @SuppressWarnings("unchecked")
    public IVariableOrConstant get(final int index) {
        
        return (IVariableOrConstant<?>) super.get(index);
        
    }

    @SuppressWarnings("unchecked")
    public IConstant<?> get(final E e, final int index) {

        return new Constant(((IElement) e).get(index));

    }

    final public boolean isOptional() {

        return (Boolean) getProperty(Annotations.OPTIONAL,Boolean.FALSE);
        
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

        return asBound(new ArrayBindingSet(new IVariable[] { var },
                new IConstant[] { val }));
        
    }
    
    public Predicate<E> asBound(final IBindingSet bindingSet) {

        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final Predicate<E> tmp = this.clone();

        /*
         * Now override any unbound variables for which we were giving bindings.
         */

        final int arity = arity();
        
//        boolean modified = false;
        
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

            tmp.set(i, val.clone());
            
//            modified = true;

        }
        
        return tmp;
        
    }

    public Object asBound(final int index, final IBindingSet bindingSet) {

        return get(index).get(bindingSet);

    }

//    @SuppressWarnings("unchecked")
//    public IKeyOrder<E> getKeyOrder() {
//
//        return (IKeyOrder<E>) getProperty(Annotations.KEY_ORDER);
//
//    }
//
//    public Predicate<E> setKeyOrder(final IKeyOrder<E> keyOrder) {
//
//        final Predicate<E> tmp = this.clone();
//
//        tmp._setProperty(Annotations.KEY_ORDER, keyOrder);
//
//        return tmp;
//
//    }
    
    @SuppressWarnings("unchecked")
    public Predicate<E> clone() {

        return (Predicate<E>) super.clone();
        
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

	/**
	 * This class may be used to insert instances of {@link IPredicate}s into a
	 * hash map where equals is decided based solely on the pattern of variables
	 * and constants found on the {@link IPredicate}. This may be used to create
	 * access path caches or to identify and eliminate duplicate requests for
	 * the same access path.
	 */
    public static class HashedPredicate<E> {

    	/**
    	 * The predicate.
    	 */
		public final IPredicate<E> pred;
    	
		/**
		 * The cached hash code.
		 */
		final private int hash;
	
		public HashedPredicate(final IPredicate<E> pred) {

			if (pred == null)
				throw new IllegalArgumentException();
    		
    		this.pred = pred;

    		this.hash = computeHash();
    		
		}

		public boolean equals(final Object other) {

			if (this == other)
				return true;

			if (!(other instanceof HashedPredicate<?>))
				return false;

			final IPredicate<?> o = ((HashedPredicate<?>) other).pred;

			final int arity = pred.arity();

			if (arity != o.arity())
				return false;

			for (int i = 0; i < arity; i++) {

				final IVariableOrConstant<?> x = pred.get(i);

				final IVariableOrConstant<?> y = o.get(i);

				if (x != y && !(x.equals(y))) {

					return false;

				}

			}

			return true;

		}

		public int hashCode() {

			return hash;
			
		}

		private final int computeHash() {

			int h = 0;

			final int n = pred.arity();

			for (int i = 0; i < n; i++) {

				h = 31 * h + pred.get(i).hashCode();

			}

			return h;

		}
		
    }

}
