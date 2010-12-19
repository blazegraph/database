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

import com.bigdata.rdf.internal.IV;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IVariable;

/**
 * Imposes the constraint <code>isLiteral(x)</code>.
 */
public class IsLiteral implements IConstraint {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3125106876006900339L;

	private final IVariable x;

	public IsLiteral(final IVariable x) {
		
        if (x == null)
            throw new IllegalArgumentException();

        this.x = x;
		
	}
	
    public boolean accept(IBindingSet s) {
        
        // get binding for "x".
        final IConstant x = s.get(this.x);
       
        if (x == null)
            return true; // not yet bound.

        final IV iv = (IV) x.get();
        
		return iv.isLiteral();
		
   }

    public IVariable[] getVariables() {
        
        return new IVariable[] { x };
        
    }

}
