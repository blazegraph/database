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

package com.bigdata.relation.rule;

import java.util.Arrays;

import com.bigdata.relation.accesspath.IElementFilter;

/**
 * A generic implementation of an immutable {@link IPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Predicate<E> implements IPredicate<E> {

    public Predicate<E> copy() {
        
        return new Predicate(this, relationName);
        
    }
    
    /**
     * 
     */
    private static final long serialVersionUID = -790293263213387491L;

    private final int arity;
    
    /** #of unbound variables. */
    private final int nvars;

    private final String[] relationName;
    
    private final IVariableOrConstant[] values;
    
    private final boolean optional;
    
    private final IElementFilter<E> constraint;

    private final ISolutionExpander<E> expander;
    
    private final int partitionId;

    /**
     * Copy constructor creates a new instance of this class with any unbound
     * variables overriden by their bindings from the given binding set (if
     * any).
     * 
     * @param src
     *            The source predicate.
     * @param bindingSet
     *            Additional bindings.
     */
    protected Predicate(Predicate<E> src, IBindingSet bindingSet) {

        this.arity = src.arity;

        this.relationName = src.relationName;

        this.values = src.values.clone();

        this.optional = src.optional;
        
        this.constraint = src.constraint;

        this.expander = src.expander;
        
        this.partitionId = src.partitionId;
        
        /*
         * Now override any unbound variables for which we were giving bindings.
         */

        int nvars = src.nvars;
        
        for (int i = 0; i < values.length && nvars > 0; i++) {

            if (values[i].isConstant()) continue;

            final IVariable var = (IVariable) values[i];
            
            final IConstant val = bindingSet.get(var);
            
            if(val != null) {
                
                values[i] = val;
                
                nvars--;
                
            }
            
        }

        this.nvars = nvars;
        
    }
    
    /**
     * Copy constructor creates a new instance of this class replacing the
     * existing relation name(s) with the given one(s).
     * 
     * @param src
     *            The source predicate.
     * @param relationName
     *            The new relation name(s).
     */
    protected Predicate(Predicate<E> src, String[] relationName) {

        if (relationName == null) {

            throw new IllegalArgumentException();

        }

        for (int i = 0; i < relationName.length; i++) {

            if (relationName[i] == null)
                throw new IllegalArgumentException();

        }

        if (relationName.length == 0)
            throw new IllegalArgumentException();

        this.arity = src.arity;

        this.nvars = src.nvars;

        this.relationName = relationName; // override.

        this.values = src.values;

        this.optional = src.optional;
        
        this.constraint = src.constraint;
        
        this.expander = src.expander;
     
        this.partitionId = src.partitionId;
        
    }
    
    /**
     * Copy constructor creates a new instance of this class in which the index
     * partition constraint as specified.
     * 
     * @param src
     *            The source predicate.
     * @param partitionId
     *            The index partition constraint.
     * 
     * @throws IllegalArgumentException
     *             if the index partition identified is a negative integer.
     * @throws IllegalStateException
     *             if the index partition identifier was already specified.
     */
    protected Predicate(Predicate<E> src, int partitionId) {

        if (this.partitionId != -1) {
            
            throw new IllegalStateException();

        }

        if (partitionId < 0) {

            throw new IllegalArgumentException();

        }

        this.arity = src.arity;

        this.nvars = src.nvars;

        this.relationName = src.relationName;

        this.partitionId = partitionId; // override.

        this.values = src.values;

        this.optional = src.optional;

        this.constraint = src.constraint;

        this.expander = src.expander;
     
    }
    
    /**
     * Simplified ctor.
     * 
     * @param relationName
     *            Identifies the relation to be queried.
     * @param values
     *            The values (order is important!).
     */
    public Predicate(String relationName, IVariableOrConstant[] values) {
        
        this(new String[] { relationName }, -1/* partitionId */, values,
                false/* optional */, null/* constraint */, null/* expander */);
        
    }

    /**
     * Fully specified ctor.
     * 
     * @param relationName
     *            Identifies the relation(s) in the view.
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
    public Predicate(String[] relationName, int partitionId,
            IVariableOrConstant[] values, boolean optional,
            IElementFilter<E> constraint, ISolutionExpander<E> expander) {

        if (relationName == null)
            throw new IllegalArgumentException();

        if (partitionId < -1)
            throw new IllegalArgumentException();

        if (values == null)
            throw new IllegalArgumentException();

        this.relationName = relationName;

        this.partitionId = partitionId;
        
        this.arity = values.length;

        int nvars = 0;

        for (int i = 0; i < arity; i++) {

            final IVariableOrConstant value = values[i];

            if (value == null)
                throw new IllegalArgumentException();

            if (value.isVar())
                nvars++;

        }

        this.nvars = nvars;

        this.values = values;
        
        this.optional = optional;
        
        this.constraint = constraint;
        
        this.expander = expander;
        
    }
    
    public String getOnlyRelationName() {
        
        if (relationName.length != 1)
            throw new IllegalStateException();
        
        return relationName[0];
        
    }
    
    public String getRelationName(int index) {
        
        return relationName[index];
        
    }

    public int getPartitionId() {
        
        return partitionId;
        
    }
    
    public int getRelationCount() {
        
        return relationName.length;
        
    }
    
    final public int arity() {
        
        return arity;
        
    }

    public IVariableOrConstant get(int index) {
        
        return values[index];
        
    }

    final public boolean isOptional() {
        
        return optional;
        
    }
    
    final public IElementFilter<E> getConstraint() {

        return constraint;
        
    }

    final public ISolutionExpander<E> getSolutionExpander() {
        
        return expander;
        
    }
    
    final public int getVariableCount() {

        return nvars;
        
    }

    final public boolean isFullyBound() {

        return false;
        
    }

    /**
     * Returns an ordered array of the values for this predicate with the given
     * bindings overriding any unbound variables.
     * 
     * @param bindingSet
     *            The bindings (optional).
     * 
     * @return
     */
    public IVariableOrConstant[] toArray(IBindingSet bindingSet) {
        
        final IVariableOrConstant[] values = new IVariableOrConstant[this.values.length];
        
        for(int i=0; i<arity; i++) {
            
            final IVariableOrConstant v = values[i];
            
            if (v.isVar() && bindingSet != null
                    && bindingSet.isBound((IVariable) v)) {

                values[i] = new Constant(bindingSet.get((IVariable) v));
                
            } else {
                
                values[i] = this.values[i];
                
            }
            
        }

        return values;
        
    }
    
    /**
     * Note: easily implemented using {@link #toArray(IBindingSet)}.
     */
    public Predicate<E> asBound(IBindingSet bindingSet) {

        return new Predicate<E>(this, bindingSet);
        
    }

    public Predicate<E> setRelationName(String[] relationName) {
    
        return new Predicate<E>(this, relationName);
        
    }

    public Predicate<E> setPartitionId(int partitionId) {

        return new Predicate<E>(this, partitionId);

    }
    
    public String toString() {
        
        return toString(null/* bindingSet */);
        
    }
    
    public String toString(IBindingSet bindingSet) {

        StringBuilder sb = new StringBuilder();

        sb.append("(");

        sb.append(Arrays.toString(relationName));
        
        for (int i = 0; i < arity; i++) {

//            if (i > 0)
                sb.append(", ");

            final IVariableOrConstant v = values[i];

            sb.append(v.isConstant() || bindingSet == null
                    || !bindingSet.isBound((IVariable) v) ? v.toString()
                    : bindingSet.get((IVariable) v));

        }

        sb.append(")");
        
        if(isOptional()) {
            
            sb.append("[optional]");
            
        }

        return sb.toString();

    }

    public boolean equals(Object other) {
        
        if (this == other)
            return true;

        final IPredicate o = (IPredicate)other;
        
        final int arity = arity();
        
        if(arity != o.arity()) return false;
        
        for(int i=0; i<arity; i++) {
            
            final IVariableOrConstant x = get(i);
            
            final IVariableOrConstant y = o.get(i);
            
            if (x != y && !(x.equals(y))) {
                
                return false;
            
            }
            
        }
        
        return true;
        
    }
    
    public int hashCode() {
        
        int h = hash;

        if (h == 0) {

            final int n = arity();

            for (int i = 0; i < n; i++) {
        
                h = 31 * h + get(i).hashCode();
                
            }
            
            hash = h;
            
        }
        
        return h;

    }

    /**
     * Caches the hash code.
     */
    private int hash = 0;
    
}
