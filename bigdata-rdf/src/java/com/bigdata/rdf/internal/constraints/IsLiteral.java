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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.constraint.BOpConstraint;
import com.bigdata.rdf.internal.IV;

/**
 * Imposes the constraint <code>isLiteral(x)</code>.
 */
public class IsLiteral extends BOpConstraint {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3125106876006900339L;

    /**
     * Required shallow copy constructor.
     */
    public IsLiteral(final BOp[] values,
            final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public IsLiteral(final IsLiteral op) {
        super(op);
    }

    public IsLiteral(final IVariable<IV> x) {
        
        super(new BOp[] { x }, null/*annocations*/);
        
        if (x == null)
            throw new IllegalArgumentException();

    }
    
    public boolean accept(IBindingSet s) {
        
        // get binding for "x".
        final IConstant<IV> x = s.get((IVariable<IV>) get(0)/*x*/);
       
        if (x == null)
            return true; // not yet bound.

        final IV iv = x.get();

    	return iv.isLiteral(); 

   }

}
