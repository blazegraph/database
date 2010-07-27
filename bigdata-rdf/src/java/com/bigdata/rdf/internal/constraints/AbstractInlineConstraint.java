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

package com.bigdata.rdf.internal.constraints;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IVariable;

/**
 * Use inline terms to perform numerical comparison operations.
 * <p>
 * @see {@link IVUtility#numericalCompare(IV, IV)}. 
 */
public abstract class AbstractInlineConstraint implements IConstraint {

    public final IVariable v;
    public final IV iv;
    
    public AbstractInlineConstraint(final IVariable<IV> v, final IV iv) {
        
        if (v == null)
            throw new IllegalArgumentException();

        if (!IVUtility.canNumericalCompare(iv))
            throw new IllegalArgumentException();
        
        this.v = v;
        
        this.iv = iv;
        
    }
    
    public boolean accept(IBindingSet s) {
        
        // get binding for "x".
        final IConstant<IV> c = s.get(this.v);
       
        if (c == null)
            return true; // not yet bound.

        final IV term = c.get();

        final int compare = IVUtility.numericalCompare(term, iv);
        
        return _accept(compare);
        
    }
    
    protected abstract boolean _accept(final int compare);
    
    public IVariable[] getVariables() {
        
        return new IVariable[] { v };
        
    }

}
