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
package com.bigdata.bop.constraint;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;

/**
 * Imposes the constraint <code>var == constant</code>.
 */
public class EQConstant extends BOpBase implements BooleanValueExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4619002304068158318L;

	public EQConstant(final IVariable<?> x, final IConstant<?> y) {

        this(new BOp[] { x, y }, null/* annotations */);

    }
    
    /**
     * Required shallow copy constructor.
     */
    public EQConstant(final BOp[] args, final Map<String, Object> annotations) {
        super(args, annotations);

        if (args.length != 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public EQConstant(final EQConstant op) {
        super(op);
    }
    
    public Boolean get(final IBindingSet bset) {
        
        final IVariable<?> var = (IVariable<?>) get(0)/* var */;
        
        // get binding for the variable.
        final IConstant<?> asBound = bset.get(var);
    
        if (asBound == null)
            return true; // not yet bound.
    
        final IConstant<?> cnst = (IConstant<?>) get(1);
        
        final boolean ret = asBound.equals(cnst);
        
        return ret;

   }

}
