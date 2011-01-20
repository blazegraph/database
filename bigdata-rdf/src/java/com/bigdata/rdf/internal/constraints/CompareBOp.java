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
 * Use inline terms to perform numerical comparison operations.
 * 
 * @see IVUtility#numericalCompare(IV, IV) 
 */
public class CompareBOp extends BOpConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The compare operator, which is a {@link CompareOp} enum value.
         */
        String OP = CompareBOp.class.getName() + ".op";

    }

    /**
     * Required shallow copy constructor.
     */
    public CompareBOp(final BOp[] values,
            final Map<String, Object> annotations) {
        super(values, annotations);
    }

    /**
     * Required deep copy constructor.
     */
    public CompareBOp(final CompareBOp op) {
        super(op);
    }

    public CompareBOp(final IValueExpression<IV> left, 
    		final IValueExpression<IV> right, final CompareOp op) {
    	
        super(new BOp[] { left, right }, NV.asMap(new NV(Annotations.OP, op)));
        
        if (left == null || right == null || op == null)
            throw new IllegalArgumentException();

    }
    
    public boolean accept(final IBindingSet s) {
        
    	final IV left = ((IValueExpression<IV>) get(0)).get(s);
    	final IV right = ((IValueExpression<IV>) get(1)).get(s);

    	if (left == null || right == null)
            return true; // not yet bound.

    	final CompareOp op = (CompareOp) getProperty(Annotations.OP);
    	
    	if (left.isTermId() && right.isTermId() &&
    			(op == CompareOp.EQ || op == CompareOp.NE)) {
    		return _accept(left.compareTo(right));
    	}
    	
    	if (!IVUtility.canNumericalCompare(left) ||
    			!IVUtility.canNumericalCompare(right)) {
    		if (op == CompareOp.EQ) {
    			return false;
    		} else if (op == CompareOp.NE) {
    			return true;
    		} else {
    			throw new NotNumericalException("cannot numerical compare: " 
    					+ left + " " + op + " " + right);
    		}
    	}
    	
		return _accept(IVUtility.numericalCompare(left, right));
    	
    }
    
    protected boolean _accept(final int compare) {
    	
    	final CompareOp op = (CompareOp) getProperty(Annotations.OP);
    	
    	switch(op) {
    	case EQ:
    		return compare == 0;
    	case NE:
    		return compare != 0;
    	case GT:
    		return compare > 0;
    	case GE:
    		return compare >= 0;
    	case LT:
    		return compare < 0;
    	case LE:
    		return compare <= 0;
    	default:
    		throw new UnsupportedOperationException();
    	}
    	
    }
    
    public static class NotNumericalException extends RuntimeException {

		/**
		 * 
		 */
		private static final long serialVersionUID = -8853739187628588335L;

		public NotNumericalException() {
			super();
		}

		public NotNumericalException(String s, Throwable t) {
			super(s, t);
		}

		public NotNumericalException(String s) {
			super(s);
		}

		public NotNumericalException(Throwable t) {
			super(t);
		}
    	
    }
    
}
