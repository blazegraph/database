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

import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.constraint.BOpConstraint;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;

/**
 * Compare two terms for exact equality. 
 */
public class SameTermBOp extends BOpConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * Required shallow copy constructor.
     */
    public SameTermBOp(final BOp[] values,
            final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public SameTermBOp(final SameTermBOp op) {
        super(op);
    }

    public SameTermBOp(final IValueExpression<IV> left, 
    		final IValueExpression<IV> right) {
    	
        super(new BOp[] { left, right }, null);
        
        if (left == null || right == null)
            throw new IllegalArgumentException();

    }
    
    public boolean accept(final IBindingSet s) {
        
    	final IV left = ((IValueExpression<IV>) get(0)).get(s);
    	final IV right = ((IValueExpression<IV>) get(1)).get(s);

    	if (left == null || right == null)
            return true; // not yet bound.

		return left.equals(right);
		
    }
    
}
