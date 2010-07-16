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
 * Imposes the constraint <code>x == y</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: EQ.java 2982 2010-06-04 18:03:16Z thompsonbry $
 */
public class LT implements IConstraint {

    private static final long serialVersionUID = 8104692462788944394L;
    
    public final IVariable v;
    public final double d;
    
    public LT(final IVariable<IV> v, final double d) {
        
        if (v == null)
            throw new IllegalArgumentException();

        this.v = v;
        
        this.d = d;
        
    }
    
    public boolean accept(IBindingSet s) {
        
        // get binding for "x".
        final IConstant<IV> c = s.get(this.v);
       
        if (c == null)
            return true; // not yet bound.

        final IV term = c.get();
        
        if (term instanceof AbstractDatatypeLiteralInternalValue) {
            
            return ((AbstractDatatypeLiteralInternalValue) term).doubleValue() < d;
            
        }
        
        throw new RuntimeException(
                "cannot apply to this constraint to this type of internal value: " 
                + term);

    }
    
    public IVariable[] getVariables() {
        
        return new IVariable[] { v };
        
    }

}
