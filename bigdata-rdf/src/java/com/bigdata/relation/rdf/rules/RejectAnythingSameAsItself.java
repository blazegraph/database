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
package com.bigdata.relation.rdf.rules;

import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IVariable;

/**
 * Rejects (x y z) iff x==z and y==owl:sameAs, where x, y, and z are variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RejectAnythingSameAsItself implements IConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final IVariable<Long> s;
    private final IVariable<Long> p;
    private final IVariable<Long> o;
    private final IConstant<Long> owlSameAs;
    
    /**
     * 
     * @param s
     * @param p
     * @param o
     * @param owlSameAs
     */
    public RejectAnythingSameAsItself(IVariable<Long> s, IVariable<Long> p, IVariable<Long> o, IConstant<Long> owlSameAs) {
        
        if (s == null || p == null || o == null || owlSameAs == null)
            throw new IllegalArgumentException();

        if (s == p || p == o || s == o)
            throw new IllegalArgumentException();
        
        this.s = s;

        this.p = p;

        this.o = o;
        
        this.owlSameAs = owlSameAs;
        
    }
    
    @SuppressWarnings("unchecked")
    public boolean accept(IBindingSet bindingSet) {
        
        // get binding for "x".
        final IConstant<Long> s = bindingSet.get(this.s);
       
        if(s==null) return true; // not yet bound.

        // get binding for "y".
        final IConstant<Long> p = bindingSet.get(this.p);
    
        if(p==null) return true; // not yet bound.
    
        // get binding for "z".
        final IConstant<Long> o = bindingSet.get(this.o);
    
        if(o==null) return true; // not yet bound.
    
        if (s.equals(o) && p.equals(owlSameAs)) {

            // reject this case.
            
            return false;
            
        }

        return true;
        
   }

}
