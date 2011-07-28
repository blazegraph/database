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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.NotMaterializedException;
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

    public static boolean compare(final CompareOp op, final IV left,
            final IV right) {

        if (left.isStatement() || right.isStatement()) {
    		
    		throw new SparqlTypeErrorException();
    		
    	}
    	
    	// handle the special case where we have exact termId equality
    	// probably would never hit this because of SameTermBOp
    	if (op == CompareOp.EQ && !left.isInline() && !right.isInline()) {
    		
//            final long tid1 = left.getTermId();
//            final long tid2 = right.getTermId();
//            
//            if (tid1 == tid2 && tid1 != TermId.NULL && tid2 != TermId.NULL)
//                return true;

            if (!left.isNullIV() && !right.isNullIV() && left.equals(right)) {
                /*
                 * Neither may be a NullIV (or mock IV) and they are equals().
                 */
                return true;
            }
    		
    	}
    	
    	// handle inlines: booleans, numerics, and dateTimes (if inline) 
    	if (IVUtility.canNumericalCompare(left, right)) {
    		
            return _accept(op,IVUtility.numericalCompare(left, right));
    		
    	}

    	final BigdataValue val1 = left.getValue();
    	final BigdataValue val2 = right.getValue();
    	
        if (val1 == null || val2 == null)
        	throw new NotMaterializedException();
        
    	try {
    	
    		// use the Sesame implementation directly
    		final boolean accept = QueryEvaluationUtil.compare(val1, val2, op);
    		
    		if (log.isDebugEnabled()) {
    			log.debug(accept);
    		}
    		
    		return accept;
    		
    	} catch (Exception ex) {
    		
    		if (log.isDebugEnabled()) {
    			log.debug("exception: " + ex);
    		}
    		
    		throw new SparqlTypeErrorException();
    		
    	}
    }
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
    
    	final CompareOp op = op();
    	return compare(op,left,right);
    }
    	
    static protected boolean _accept(final CompareOp op,final int compare) {
    	
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
    
    private volatile transient Set<IVariable<IV>> terms;
    
    public Set<IVariable<IV>> getTermsToMaterialize() {
    	
    	if (terms == null) {
    		
    		terms = new LinkedHashSet<IVariable<IV>>();
    		
    		for (BOp bop : args()) {
    			
    			if (bop instanceof IVariable)
    				terms.add((IVariable<IV>) bop);
    		
    		}
    		
    	}
    	
    	return terms;
    	
    }

}
