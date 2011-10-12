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
import org.openrdf.model.Literal;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Perform open-world value comparison operations per the SPARQL spec.
 */
public class CompareBOp extends XSDBooleanIVValueExpression
		implements INeedsMaterialization {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5661497748051783499L;
	
	protected static final Logger log = Logger.getLogger(CompareBOp.class);
	
    
    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The compare operator, which is a {@link CompareOp} enum value.
         */
        String OP = CompareBOp.class.getName() + ".op";

    }

    public CompareBOp(final IValueExpression<? extends IV> left, 
    		final IValueExpression<? extends IV> right, final CompareOp op) {
    	
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
        
        if (log.isDebugEnabled()) {
        	log.debug(toString());
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

    public static boolean compare( 
    		final IV<BigdataValue, ?> left,
            final IV<BigdataValue, ?> right,
            final CompareOp op) {

//    	final BigdataValue val1 = left.getValue();
//    	final BigdataValue val2 = right.getValue();
//    	
//    	try {
//    	
//    		// use the Sesame implementation directly
//    		final boolean accept = QueryEvaluationUtil.compare(val1, val2, op);
//    		
//    		if (log.isDebugEnabled()) {
//    			log.debug(accept);
//    		}
//    		
//    		return accept;
//    		
//    	} catch (Exception ex) {
//    		
//    		if (log.isDebugEnabled()) {
//    			log.debug("exception: " + ex);
//    		}
//    		
//    		throw new SparqlTypeErrorException();
//    		
//    	}

    	
		if (left.isLiteral() && right.isLiteral()) {
			// Both left and right argument is a Literal
			return compareLiterals(left, right, op);
		}
		else {
			// All other value combinations
			switch (op) {
				case EQ:
					return valuesEqual(left, right);
				case NE:
					return !valuesEqual(left, right);
				default:
					throw new SparqlTypeErrorException();
			}
		}
		
    }
    
    private static boolean compareLiterals(
    		final IV<BigdataValue, ?> left,
    		final IV<BigdataValue, ?> right,
    		final CompareOp op) {
    	
    	/*
    	 * Handle the special case where we have exact termId equality.
    	 * Probably would never hit this because of SameTermBOp 
    	 */
    	if (!left.isInline() && !right.isInline() && op == CompareOp.EQ) {

            if (!left.isNullIV() && !right.isNullIV() && left.equals(right)) {
                /*
                 * Neither may be a NullIV (or mock IV) and they are equals().
                 */
                return true;
            }
    		
    	}
    	
    	/*
    	 * We want to special case the LiteralExtensionIV, which
    	 * handles xsd:dateTime.  If we defer to Sesame for this, we will be
    	 * forced into materialization because the first step in Sesame's
    	 * evaluation is to get the datatype (requires materialization for
    	 * LiteralExtensionIV).
    	 */
    	if (left instanceof LiteralExtensionIV &&
    			right instanceof LiteralExtensionIV) {
    	
    		@SuppressWarnings("rawtypes")
            final IV leftDatatype = ((LiteralExtensionIV) left).getExtensionIV();
    		
            @SuppressWarnings("rawtypes")
    		final IV rightDatatype = ((LiteralExtensionIV) right).getExtensionIV();
    		
    		if (leftDatatype.equals(rightDatatype)) {
    		
    			return _accept(left.compareTo(right), op);
    			
    		}
    		
    	}
//    	
//    	if (left.isInline() && left.isNumeric() && right.isInline() && right.isNumeric()) {
//    		
//            final DTE dte1 = left.getDTE();
//            final DTE dte2 = right.getDTE();
//
//            // we can use the natural ordering if they have the same DTE
//            // this will naturally take care of two booleans or two numerics of the
//            // same datatype
//            if (dte1 == dte2)
//                return _accept(left.compareTo(right), op);
//            
//            // otherwise we need to try to convert them into comparable numbers
//            final AbstractLiteralIV num1 = (AbstractLiteralIV) left; 
//            final AbstractLiteralIV num2 = (AbstractLiteralIV) right; 
//            
//            // if one's a BigDecimal we should use the BigDecimal comparator for both
//            if (dte1 == DTE.XSDDecimal || dte2 == DTE.XSDDecimal) {
//                return _accept(num1.decimalValue().compareTo(num2.decimalValue()), op);
//            }
//            
//            // same for BigInteger
//            if (dte1 == DTE.XSDInteger || dte2 == DTE.XSDInteger) {
//                return _accept(num1.integerValue().compareTo(num2.integerValue()), op);
//            }
//            
//            // fixed length numerics
//            if (dte1.isFloatingPointNumeric() || dte2.isFloatingPointNumeric()) {
//                // non-BigDecimal floating points - use doubles
//                return _accept(Double.compare(num1.doubleValue(), num2.doubleValue()), op);
//            } else {
//                // non-BigInteger integers - use longs
//                final long a = num1.longValue();
//                final long b = num2.longValue();
//                return _accept(a == b ? 0 : a < b ? -1 : 1, op);
//            }
//
//    	}
    	
		/*
		 * Now that the IVs implement the right openrdf interfaces,
		 * we should be able to mix and match inline with non-inline,
		 * using either the IV directly or its materialized value.
		 */
//		final Literal l1 = left.isInline() ? (Literal) left : left.getValue();
		final Literal l1 = (Literal) left;
		
//		final Literal l2 = right.isInline() ? (Literal) right : right.getValue();
		final Literal l2 = (Literal) right;
		
        if (log.isDebugEnabled()) {
            log.debug(l1);
            log.debug(l2);
        }

        try {

    		// use the Sesame implementation directly
    		final boolean accept = 
    				QueryEvaluationUtil.compareLiterals(l1, l2, op);
    		
    		if (log.isDebugEnabled()) {
    			log.debug(accept);
    		}
    		
    		return accept;
    		
    	} catch (ValueExprEvaluationException ex) {
    		
    		if (log.isDebugEnabled()) {
    			log.debug("exception: " + ex);
    		}
    		
    		throw new SparqlTypeErrorException();
    		
    	}
    	
    }
    
	private static boolean valuesEqual(
			final IV<BigdataValue, ?> left, final IV<BigdataValue, ?> right) {
		
    	// must be the same type of value (e.g. both URIs, both BNodes, etc)
    	if (left.getVTE() != right.getVTE()) {
    		return false;
    	}

		/*
		 * We can't use IV.equals() when we have a null IV. A null IV might
		 * have a materialized value that matches a term id in the database.
		 * Different IVs, same materialized value (this can happen via the
		 * datatype or str operator, anything that mints a new Value during
		 * query evaluation). 
		 */
		if (left.isNullIV() || right.isNullIV()) {
			
			return left.getValue().equals(right.getValue());
			
		} else {
			
			return left.equals(right);
			
		}
		
	}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public boolean accept(final IBindingSet s) {
        
    	final IV left = get(0).get(s);
    	final IV right = get(1).get(s);
    	
        // not yet bound
    	if (left == null || right == null)
        	throw new SparqlTypeErrorException();
    	
    	if (log.isDebugEnabled()) {
    		log.debug("left: " + left);
    		log.debug("right: " + right);
    		
    		log.debug("left value: " + (left.hasValue() ? left.getValue() : null));
    		log.debug("right value: " + (right.hasValue() ? right.getValue() : null));
    	}
    
    	return compare(left, right, op());
    	
    }
    	
    static protected boolean _accept(final int compare, final CompareOp op) {
    	
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
    
    /**
     * The CompareBOp can work with non-materialized terms in the case of
     * inline numerical compare operations.  It is only when the bop encounters
     * non-inlined numerics or needs to compare strings that it needs
     * materialized terms.  
     */
    public Requirement getRequirement() {
    	
    	return INeedsMaterialization.Requirement.SOMETIMES;
    	
    }
    
}
