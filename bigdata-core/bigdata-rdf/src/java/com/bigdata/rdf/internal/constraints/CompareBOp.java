/**

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

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.bop.solutions.IVComparator;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.FilterNode;

/**
 * Perform open-world value comparison operations per the SPARQL spec (the LT
 * operator). This does NOT implement the broader ordering for ORDER BY. That is
 * handled by {@link IVComparator}.
 * 
 * @see <a
 *      href="http://www.w3.org/TR/2013/REC-sparql11-query-20130321/#op_lt">&lt;</a>
 * 
 * @see IVComparator
 */
public class CompareBOp extends XSDBooleanIVValueExpression
		implements INeedsMaterialization {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5661497748051783499L;
	
    private static final transient Logger log = Logger
            .getLogger(CompareBOp.class);
	
    public interface Annotations extends XSDBooleanIVValueExpression.Annotations {

        /**
         * The compare operator, which is a {@link CompareOp} enum value.
         */
        String OP = CompareBOp.class.getName() + ".op";

    }

    @SuppressWarnings("rawtypes")
    public CompareBOp(final IValueExpression<? extends IV> left,
            final IValueExpression<? extends IV> right, final CompareOp op) {

        this(new BOp[] { left, right }, NV.asMap(Annotations.OP, op));

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
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
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

        if (log.isDebugEnabled()) {
            log.debug(left);
            log.debug(right);
        }

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
    
    private static boolean isRealTermId(final IV iv) {
    	
    	return !iv.isInline() && !iv.isNullIV();
    	
    }
    
    private static boolean compareLiterals(
    		final IV<BigdataValue, ?> left,
    		final IV<BigdataValue, ?> right,
    		final CompareOp op) {
    	
    	/*
    	 * Handle the special case where we have exact termId equality.  This
    	 * only works if both are "real" termIds (i.e. not mock IVs).
    	 */
    	if (op == CompareOp.EQ && isRealTermId(left) && isRealTermId(right)) {

            if (left.equals(right)) {
            	
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
    		
        		final boolean accept = 
        			_accept(left.compareTo(right), op);
        		
	    		if (log.isDebugEnabled()) {
	    			log.debug(accept);
	    		}
	    		
	    		return accept;
    			
    		}
    		
    	}
    	
    	/**
    	 * Handle LiteralExtensionIVs that support comparison with built-in datatypes.
         * These types are handled by conversion into a built-in datatype, which can be checked
         * by Sesame's QueryEvaluationUtil.
    	 */
    	Literal l1 = null;
        Literal l2 = null;
    	if (left instanceof LiteralExtensionIV || right instanceof LiteralExtensionIV) { 
    	    
    	    /**
    	     * We caught the case where both left and right are LiteralExtensionIVs
    	     * earlier, so we can be sure that exactly one of the is a LiteralExtensionIVs.
    	     */
    	    boolean leftIsLiteralExtensionIV = left instanceof LiteralExtensionIV;
    	    
    	    @SuppressWarnings("rawtypes")
            final AbstractLiteralIV delegate = 
                leftIsLiteralExtensionIV ?
                ((LiteralExtensionIV) left).getDelegate():
                ((LiteralExtensionIV) right).getDelegate();
                    
    	    if (delegate !=null) {
    	        
    	        Literal delegateAsBuiltInLiteral = null;
    	        if (delegate instanceof PackedLongIV) {
    	            
    	            @SuppressWarnings("rawtypes")
                    final PackedLongIV iv = (PackedLongIV)delegate;
    	            delegateAsBuiltInLiteral = new XSDIntegerIV<>(iv.integerValue());

    	        } // else: add other cases here in future

    	        // set left-hand or right-hand side expression
                if (leftIsLiteralExtensionIV) {
                    l1 = delegateAsBuiltInLiteral; // may remain null
                } else {
                    l2 = delegateAsBuiltInLiteral; // may remanin null
                }    	        
    	    }
    	}

		/*
		 * Now that the IVs implement the right openrdf interfaces,
		 * we should be able to mix and match inline with non-inline,
		 * using either the IV directly or its materialized value.
		 */
		l1 = l1==null ? asLiteral(left) : l1;
		l2 = l2==null ? asLiteral(right) : l2;
		
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
			final IV<?, ?> left, final IV<?, ?> right) {
		
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
			
			return asValue(left).equals(asValue(right));
			
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
