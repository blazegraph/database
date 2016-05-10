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
 * Created on Sep 10, 2008
 */

package com.bigdata.bop.bindingSet;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;

import cutthecrap.utils.striterators.EmptyIterator;

/**
 * An immutable empty binding set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
final public class EmptyBindingSet implements IBindingSet, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 4270590461117389862L;
    
    /**
     * Immutable singleton.
     */
    public static transient final EmptyBindingSet INSTANCE = new EmptyBindingSet();
    
    private EmptyBindingSet() {
        
    }

    /**
     * @todo Clone returns the same object, which is immutable. Since we use
     *       clone when binding, it might be better to return a mutable object.
     */
    public EmptyBindingSet clone() {
        
        return this;
        
    }
    
    public EmptyBindingSet copy(IVariable[] variablesToDrop) {
        
        return this;
        
    }
        
    /** Returns the same object. */
    @Override
    public EmptyBindingSet copyMinusErrors(final IVariable[] variablesToDrop) {
        return this;
    }
        
    
    /** 
     * @return false, always
     */
    @Override
    public final boolean containsErrorValues() {  
        return false;
    }
    
    
    public void clear(IVariable var) {
        throw new UnsupportedOperationException();
    }

    public void clearAll() {
        throw new UnsupportedOperationException();
    }

    public Iterator<Entry<IVariable, IConstant>> iterator() {
        
        return EmptyIterator.DEFAULT;
        
    }

    public void set(IVariable var, IConstant val) {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        return true;
    }
    
    public int size() {
        return 0;
    }

    public boolean equals(final Object t) {
        
        if (this == t)
            return true;

        if (!(t instanceof IBindingSet))
            return false;

        final IBindingSet o = (IBindingSet) t;

        if (o.size() == 0)
            return true;

        return false;

    }

    /**
     * The hash code of an empty binding set is always zero.
     */
    public int hashCode() {

        return 0;
        
    }
    
    public IConstant get(IVariable var) {

        if (var == null)
            throw new IllegalArgumentException();

        return null;
        
    }

    public boolean isBound(IVariable var) {
        
        if (var == null)
            throw new IllegalArgumentException();

        return false;
        
    }

    /**
     * Imposes singleton pattern during object de-serialization.
     */
    private Object readResolve() throws ObjectStreamException {

        return EmptyBindingSet.INSTANCE;

    }

    public Iterator<IVariable> vars() {

        return EmptyIterator.DEFAULT;
        
    }

//	public void push(IVariable[] vars) {
//        throw new UnsupportedOperationException();
//	}
//    
//	public void pop(IVariable[] vars) {
//        throw new IllegalStateException();
//	}

}
