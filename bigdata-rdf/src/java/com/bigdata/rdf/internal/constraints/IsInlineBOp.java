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
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSDBooleanIV;

/**
 * Imposes the constraint <code>isInline(x)</code>.
 */
public class IsInlineBOp extends XSDBooleanIVValueExpression {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3125106876006900339L;

    public interface Annotations extends PipelineOp.Annotations {

    	/**
    	 * If true, only accept variable bindings for {@link #x} that have an 
    	 * inline internal value {@link IV}. Otherwise only accept variable bindings
    	 * that are not inline in the statement indices.
    	 * <p>
    	 * @see IV#isInline()
    	 */
    	String INLINE = (IsInlineBOp.class.getName() + ".inline").intern();
    	
    }

    public IsInlineBOp(final IVariable<IV> x, final boolean inline) {
        
        this(new BOp[] { x }, NV.asMap(new NV(Annotations.INLINE, inline)));
        
    }
    
    /**
     * Required shallow copy constructor.
     */
    public IsInlineBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public IsInlineBOp(final IsInlineBOp op) {
        super(op);
    }

    public boolean accept(final IBindingSet bs) {
        
        final boolean inline = 
        	(Boolean) getRequiredProperty(Annotations.INLINE); 
        
        final IV iv = get(0).get(bs);
       
        // not yet bound
        if (iv == null)
        	throw new SparqlTypeErrorException();

		return iv.isInline() == inline;

    }
    
}
