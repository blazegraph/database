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
package com.bigdata.rdf.rules;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.constraint.BooleanValueExpression;

/**
 * Rejects (x y z) iff x==z and y==owl:sameAs, where x, y, and z are variables.
 */
public class RejectAnythingSameAsItself extends BOpBase 
		implements BooleanValueExpression {

    /**
	 * 
	 */
	private static final long serialVersionUID = -44020295153412258L;

	/**
     * Required shallow copy constructor.
     */
    public RejectAnythingSameAsItself(final BOp[] values,
            final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public RejectAnythingSameAsItself(final RejectAnythingSameAsItself op) {
        super(op);
    }

    /**
     * 
     * @param s
     * @param p
     * @param o
     * @param owlSameAs
     */
    public RejectAnythingSameAsItself(final IVariable<Long> s,
            final IVariable<Long> p, final IVariable<Long> o,
            final IConstant<Long> owlSameAs) {

        super(new BOp[] { s, p, o, owlSameAs }, null/*annotations*/);
        
        if (s == null || p == null || o == null || owlSameAs == null)
            throw new IllegalArgumentException();

    }
    
    @SuppressWarnings("unchecked")
    public Boolean get(final IBindingSet bindingSet) {

        // get binding for "x".
        final IConstant<Long> s = bindingSet.get((IVariable<?>) get(0)/* s */);

        if (s == null)
            return true; // not yet bound.

        // get binding for "y".
        final IConstant<Long> p = bindingSet.get((IVariable<?>) get(1)/* p */);

        if (p == null)
            return true; // not yet bound.

        // get binding for "z".
        final IConstant<Long> o = bindingSet.get((IVariable<?>) get(2)/* o */);

        if (o == null)
            return true; // not yet bound.

        if (s.equals(o) && p.equals(get(3)/* owlSameAs */)) {

            // reject this case.

            return false;

        }

        return true;

    }

}
