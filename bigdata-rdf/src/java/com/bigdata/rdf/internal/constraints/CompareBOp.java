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

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.XSDBooleanIV;

/**
 * Use inline terms to perform numerical comparison operations.
 * 
 * @see IVUtility#numericalCompare(IV, IV) 
 */
public class CompareBOp extends ValueExpressionBOp 
		implements IValueExpression<IV> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5661497748051783499L;
	
	protected static final Logger log = Logger.getLogger(CompareBOp.class);
	
    
    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The compare operator, which is a {@link CompareOp} enum value.
         */
        String OP = (CompareBOp.class.getName() + ".op").intern();

    }

    public CompareBOp(final IValueExpression<IV> left, 
    		final IValueExpression<IV> right, final CompareOp op) {
    	
        this(new BOp[] { left, right }, NV.asMap(new NV(Annotations.OP, op)));
        
    }
    
    /**
     * Required shallow copy constructor.
     */
    public CompareBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 2 || args[0] == null || args[1] == null
        		|| getProperty(Annotations.OP) == null) {

			throw new IllegalArgumentException();
		
		}

    }

    /**
     * Required deep copy constructor.
     */
    public CompareBOp(final CompareBOp op) {
        super(op);
    }
    
    public CompareOp op() {
    	return (CompareOp) getRequiredProperty(Annotations.OP);
    }

    public boolean accept(final IBindingSet s) {
        
    	final IV left = get(0).get(s);
    	final IV right = get(1).get(s);
    	
        // not yet bound
    	if (left == null || right == null)
        	throw new SparqlTypeErrorException();

    	final CompareOp op = op();
    	
    	if (left.isTermId() && right.isTermId()) {
    		if (op == CompareOp.EQ || op == CompareOp.NE) {
    			return _accept(left.compareTo(right));
    		} else {
    			if (log.isInfoEnabled())
    				log.info("cannot compare: " 
    					+ left + " " + op + " " + right);
    			
    			throw new SparqlTypeErrorException();
    		}
    	}
    	
    	/*
    	 * This code is bad.
    	 */
    	if (!IVUtility.canNumericalCompare(left) ||
    			!IVUtility.canNumericalCompare(right)) {
    		if (op == CompareOp.EQ) {
    			return false;
    		} else if (op == CompareOp.NE) {
    			return true;
    		} else {
    			if (log.isInfoEnabled())
    				log.info("cannot numerical compare: " 
    					+ left + " " + op + " " + right);
    			
    			throw new SparqlTypeErrorException();
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

    public IV get(final IBindingSet bs) {
    	
    	return accept(bs) ? XSDBooleanIV.TRUE : XSDBooleanIV.FALSE;        		
    	
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
