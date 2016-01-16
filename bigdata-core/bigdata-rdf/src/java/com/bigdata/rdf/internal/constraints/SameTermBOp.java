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

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;

/**
 * Compare two terms for exact equality. 
 */
public class SameTermBOp extends XSDBooleanIVValueExpression 
		implements INeedsMaterialization {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    public interface Annotations extends
            XSDBooleanIVValueExpression.Annotations {

        /**
         * The compare operator, which is a {@link CompareOp} enum value. Must
         * be either {@link CompareOp#EQ} or {@link CompareOp#NE}.
         */
        String OP = CompareBOp.Annotations.OP;

    }
    
    /**
     * Constructor for sameTerm using {@link CompareOp#EQ}.
     */
    @SuppressWarnings("rawtypes")
    public SameTermBOp(
    		final IValueExpression<? extends IV> left,
            final IValueExpression<? extends IV> right) {

        this(left, right, CompareOp.EQ);

    }
    
    /**
     * Constructor for sameTerm using either {@link CompareOp#EQ} or
     * {@link CompareOp#NE}.
     * 
     * @param left
     * @param right
     * @param op
     */
    @SuppressWarnings("rawtypes")
    public SameTermBOp(final IValueExpression<? extends IV> left,
            final IValueExpression<? extends IV> right, final CompareOp op) {

        this(new BOp[] { left, right }, NV.asMap(Annotations.OP, op));

    }
    
    /**
     * Required shallow copy constructor.
     */
    public SameTermBOp(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);
    	
        if (args.length != 2 || args[0] == null || args[1] == null)
			throw new IllegalArgumentException();

        final CompareOp op = (CompareOp) getRequiredProperty(Annotations.OP);
        
        if (!(op == CompareOp.EQ || op == CompareOp.NE))
			throw new IllegalArgumentException();
        
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public SameTermBOp(final SameTermBOp op) {
        super(op);
    }
    
    @Override
    public INeedsMaterialization.Requirement getRequirement() {
    	
    	return INeedsMaterialization.Requirement.SOMETIMES;
    	
    }

    public boolean accept(final IBindingSet bs) {
        
    	@SuppressWarnings("rawtypes")
        final IV left = get(0).get(bs);

        // not yet bound
        if (left == null)
            throw new SparqlTypeErrorException(); 

        @SuppressWarnings("rawtypes")
        final IV right = get(1).get(bs);

    	// not yet bound
        if (right == null)
            throw new SparqlTypeErrorException();

        final CompareOp op = (CompareOp) getRequiredProperty(Annotations.OP);

        switch (op) {
        case EQ:
            return compare(left, right);
        default:
            return !compare(left, right);
        }

    }
    
    private static boolean compare(final IV iv1, final IV iv2) {
    	
    	if (iv1.isNullIV() || iv2.isNullIV()) {
    	
    		final Value val1 = asValue(iv1);
    		
    		final Value val2 = asValue(iv2);
    		
    		if (val1 instanceof URI && val2 instanceof URI) {
    			
    			return val1.stringValue().equals(val2.stringValue());
    			
    		} else if (val1 instanceof Literal && val2 instanceof Literal) {
    			
    			final Literal lit1 = (Literal) val1;
    			
    			final Literal lit2 = (Literal) val2;
    			
    			return equals(lit1.getLabel(), lit2.getLabel()) &&
	    			   equals(lit1.getDatatype(), lit2.getDatatype()) &&
	    			   equals(lit1.getLanguage(), lit2.getLanguage());
    			
    		} else {
    			
    			return false;
    			
    		}
    		
    	} else {
    		
    		return iv1.equals(iv2);
    		
    	}
    	
    }
    
    private static boolean equals(final Object o1, final Object o2) {
    	
    	if (o1 == null && o2 == null)
    		return true;
    	
    	if (o1 == null || o2 == null)
    		return false;
    	
    	return o1.equals(o2);
    	
    }
    
}
