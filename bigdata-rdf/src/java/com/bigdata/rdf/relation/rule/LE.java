/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.relation.rule;

import com.bigdata.rdf.internal.AbstractDatatypeLiteralInternalValue;
import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IVariable;

/**
 * FIXME obviously not implemented correctly right now, but it works
 * 
 * @author mike
 */
public class LE implements IConstraint {

    private static final long serialVersionUID = 8104692462788944394L;
    
    public final IVariable v;
    public final double d;
    
    public LE(final IVariable<IV> v, IV iv) {
        
        if (v == null || !iv.isInline())
            throw new IllegalArgumentException();

        if (!(iv instanceof AbstractDatatypeLiteralInternalValue))
            throw new IllegalArgumentException();
        
        this.v = v;
        
        this.d = ((AbstractDatatypeLiteralInternalValue) iv).doubleValue();
        
    }
    
    public boolean accept(IBindingSet s) {
        
        // get binding for "x".
        final IConstant<IV> c = s.get(this.v);
       
        if (c == null)
            return true; // not yet bound.

        final IV term = c.get();
        
        if (term instanceof AbstractDatatypeLiteralInternalValue) {
            
            return ((AbstractDatatypeLiteralInternalValue) term).doubleValue() <= d;
            
        }
        
        throw new RuntimeException(
                "cannot apply this constraint to this type of internal value: " 
                + term);

    }
    
    public IVariable[] getVariables() {
        
        return new IVariable[] { v };
        
    }

}
