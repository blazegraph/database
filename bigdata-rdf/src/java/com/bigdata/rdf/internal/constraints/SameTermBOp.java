/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSDBooleanIV;

/**
 * Compare two terms for exact equality. 
 */
public class SameTermBOp extends XSDBooleanIVValueExpression {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    public SameTermBOp(final IValueExpression<? extends IV> left, 
    		final IValueExpression<? extends IV> right) {
    	
        this(new BOp[] { left, right }, null);
        
    }
    
    /**
     * Required shallow copy constructor.
     */
    public SameTermBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 2 || args[0] == null || args[1] == null)
			throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public SameTermBOp(final SameTermBOp op) {
        super(op);
    }

    public boolean accept(final IBindingSet bs) {
        
    	final IV left = get(0).get(bs);
    	final IV right = get(1).get(bs);

    	// not yet bound
    	if (left == null || right == null)
            throw new SparqlTypeErrorException(); 

		return left.equals(right);
		
    }
    
}
