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

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * Imposes the constraint <code>isInline(x)</code>.
 */
public class IsInline { //extends AbstractInlineConstraint implements IConstraint {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3125106876006900339L;

//	private final IVariable x;

	/**
	 * If true, only accept variable bindings for {@link #x} that have an 
	 * inline internal value {@link IV}. Otherwise only accept variable bindings
	 * that are not inline in the statement indices.
	 * <p>
	 * @see IV#isInline()
	 */
	private final boolean inline;
    
//    /**
//     * @todo Required deep copy constructor.
//     */
//    public IsInline(final IsInline op) {
//        super(op);
//    }

    public IsInline(final IVariable x, final boolean inline) {
        
        if (x == null)
            throw new IllegalArgumentException();

//        this.x = x;
		this.inline = inline;
        
    }
    
//    public boolean accept(IBindingSet s) {
//        
//        // get binding for "x".
//        final IConstant x = s.get((IVariable)get(0)/*x*/);
//       
//        if (x == null)
//            return true; // not yet bound.
//
//        final IV iv = (IV) x.get();
//        
//		return iv.isInline() == inline; 
//
//   }

}
