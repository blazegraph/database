/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Imposes the constraint <code>x OR y</code>. Each operand of this operator
 * must evaluate to a boolean. If the operand is not known to evaluate to a
 * boolean, it is wrapped with an {@link EBVBOp}.
 */
public class OrBOp extends XSDBooleanIVValueExpression {
	
    /**
	 * 
	 */
	private static final long serialVersionUID = 610253427197564102L;

    public OrBOp(
    		final IValueExpression<? extends IV> x,
            final IValueExpression<? extends IV> y) { 

        this(new BOp[] { wrap(x), wrap(y) }, BOp.NOANNS);

    }

    /**
     * Required shallow copy constructor.
     */
    public OrBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 2 || args[0] == null || args[1] == null)
			throw new IllegalArgumentException();
        
        if (!(args[0] instanceof XSDBooleanIVValueExpression))
			throw new IllegalArgumentException();

        if (!(args[1] instanceof XSDBooleanIVValueExpression))
			throw new IllegalArgumentException();

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public OrBOp(final OrBOp op) {
        super(op);
    }
    
    @Override
    public IValueExpression<? extends XSDBooleanIV> get(final int i) {
		return (IValueExpression<? extends XSDBooleanIV>) super.get(i);
    }

    /**
     * Follows semantics from SPARQL spec - "Testing Values".
     * <p>
     * see http://www.w3.org/TR/rdf-sparql-query/#tests section 11.2
     */
    public boolean accept(final IBindingSet bs) {

    	XSDBooleanIV left, right;
    	
    	try {
    		left = get(0).get(bs);
    	} catch (SparqlTypeErrorException ex) {
    		left = null;
    	}
    	
    	try {
    		right = get(1).get(bs);
    	} catch (SparqlTypeErrorException ex) {
    		right = null;
    	}
    	
    	// special error handling per the SPARQL spec
    	if (left == null || right == null) {
    		// if one or the other is true, return true
    		if (left != null && left.booleanValue())
    			return true;
    		if (right != null && right.booleanValue())
    			return true;
    		// all other cases, throw a type error
    		throw new SparqlTypeErrorException();
    	}
    	
    	return left.booleanValue() || right.booleanValue();

    }
    
}
