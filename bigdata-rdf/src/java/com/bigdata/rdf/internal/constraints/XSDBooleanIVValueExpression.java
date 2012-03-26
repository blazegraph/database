/*

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;

/**
 * Base class for RDF value expression BOps that happen to evaluate to an
 * {@link XSDBooleanIV}. These are operators such as Compare, Is*, And, Or, etc.
 */
public abstract class XSDBooleanIVValueExpression extends
		IVValueExpression<XSDBooleanIV> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7068219781217676085L;

    /**
     * If the operand is not known to evaluate to a boolean, wrap it with an
     * {@link EBVBOp}.
     * 
     * @return An operand which is known to evaluate to an {@link XSDBooleanIV}.
     */
    protected static final XSDBooleanIVValueExpression wrap(
            final IValueExpression<? extends IV> ve, final String lex) {

        return ve instanceof XSDBooleanIVValueExpression ? (XSDBooleanIVValueExpression) ve
                : new EBVBOp(ve, lex);

    }

	/**
     * Required shallow copy constructor.
     */
    public XSDBooleanIVValueExpression(final BOp[] args,
            final Map<String, Object> anns) {
     
        super(args, anns);
        
    }

    /**
     * Required deep copy constructor.
     */
    public XSDBooleanIVValueExpression(final XSDBooleanIVValueExpression op) {
        
        super(op);
        
    }

    /**
     * Delegates to {@link #accept(IBindingSet)}.
     */
    @SuppressWarnings("rawtypes")
    @Override
    final public XSDBooleanIV get(final IBindingSet bs) {
    	
        return accept(bs) ? XSDBooleanIV.TRUE : XSDBooleanIV.FALSE;
        
    }
    
    /**
     * Implement this method.
     * 
     * @param bs
     *            The source solution.
     * @return <code>true</code> iff the function accepts the solution.
     */
    protected abstract boolean accept(final IBindingSet bs);

}
