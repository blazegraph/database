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

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.constraint.BOpConstraint;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;

/**
 * BOpConstraint that wraps a {@link EBVBOp}, which itself computes the 
 * effective boolean value of an IValueExpression.
 */
public class ValueExpressionConstraint extends BOpConstraint {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7068219781217676085L;
	
	protected static final Logger log = Logger.getLogger(ValueExpressionConstraint.class);
	
	/**
	 * Convenience method to generate a constraint from a value expression.
	 */
	public static IConstraint wrap(final IValueExpression<IV> ve) {
		return new ValueExpressionConstraint(new EBVBOp(ve));
	}
	
	
	public ValueExpressionConstraint(final EBVBOp x) {
		
        this(new BOp[] { x }, null/*annocations*/);

    }

    /**
     * Required shallow copy constructor.
     */
    public ValueExpressionConstraint(final BOp[] args, 
    		final Map<String, Object> anns) {
    	
        super(args, anns);
        
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public ValueExpressionConstraint(final ValueExpressionConstraint op) {
        super(op);
    }

    @Override
    public EBVBOp get(final int i) {
    	return (EBVBOp) super.get(i);
    }
    
    public boolean accept(final IBindingSet bs) {
    	
    	try {
    		
    		// evaluate the EBV operator
    		return get(0).get(bs).booleanValue();
    		
    	} catch (SparqlTypeErrorException ex) {
    		
    		// trap the type error and filter out the solution
    		if (log.isInfoEnabled())
    			log.info("discarding solution due to type error: " + bs);
    		return false;
    		
    	}
    	
    }
    
}
