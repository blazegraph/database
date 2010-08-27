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
 * Created on Sep 10, 2008
 */

package com.bigdata.bop;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;

import cutthecrap.utils.striterators.EmptyIterator;

/**
 * An immutable empty binding set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
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
    
    public void clear(IVariable var) {
        throw new UnsupportedOperationException();
    }

    public void clearAll() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public Iterator<Entry<IVariable, IConstant>> iterator() {
        
        return EmptyIterator.DEFAULT;
        
    }

    public void set(IVariable var, IConstant val) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        return 0;
    }

    public boolean equals(IBindingSet o) {
        
        if (this == o)
            return true;
        
        if (o.size() == 0)
            return true;
        
        return false;

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
    
}
