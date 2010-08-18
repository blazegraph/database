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

import com.bigdata.bop.AbstractBOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;

/**
 * Imposes the constraint <code>var == constant</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EQConstant extends AbstractBOp implements IConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

//    /**
//     * 
//     */
//    private static final long serialVersionUID = -2267584965908057945L;
//    
//    private final IVariable<?> var;
//    private final IConstant<?> val;

    public EQConstant(final IVariable<?> var, final IConstant<?> val) {

        super(new BOp[] { var, val });
        
        if (var == null)
            throw new IllegalArgumentException();
        
        if (val == null)
            throw new IllegalArgumentException();

//        this.var = var;
//
//        this.val = val;
        
    }
    
    public boolean accept(final IBindingSet s) {
        
        // get binding for the variable.
        final IConstant<?> tmp = s.get((IVariable<?>) args[0]/* var */);
    
        if (tmp == null)
            return true; // not yet bound.
    
        return tmp.equals(args[1]);

   }

}
