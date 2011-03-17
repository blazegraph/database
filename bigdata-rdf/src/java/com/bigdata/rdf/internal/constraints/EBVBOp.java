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
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSDBooleanIV;

/**
 * Calculates the "effective boolean value" of an IValueExpression.  See the
 * SPARQL spec for details.
 */
public class EBVBOp extends XSDBooleanIVValueExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5701967329003122236L;

    public EBVBOp(final IValueExpression<? extends IV> x) {

        this(new BOp[] { x }, null/*Annotations*/);

    }

    /**
     * Required shallow copy constructor.
     */
    public EBVBOp(final BOp[] args, final Map<String, Object> anns) {
    	
        super(args, anns);
        
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

    }

    /**
     * Required deep copy constructor.
     */
    public EBVBOp(final EBVBOp op) {
        super(op);
    }

	/**
	 * 11.2.2 Effective Boolean Value (EBV)
	 * 
	 * Effective boolean value is used to calculate the arguments to the logical
	 * functions logical-and, logical-or, and fn:not, as well as evaluate the
	 * result of a FILTER expression.
	 * 
	 * The XQuery Effective Boolean Value rules rely on the definition of
	 * XPath's fn:boolean. The following rules reflect the rules for fn:boolean
	 * applied to the argument types present in SPARQL Queries:
	 * 
	 * The EBV of any literal whose type is xsd:boolean or numeric is false if
	 * the lexical form is not valid for that datatype (e.g.
	 * "abc"^^xsd:integer).
	 * 
	 * If the argument is a typed literal with a datatype of xsd:boolean, the
	 * EBV is the value of that argument.
	 * 
	 * If the argument is a plain literal or a typed literal with a datatype of
	 * xsd:string, the EBV is false if the operand value has zero length;
	 * otherwise the EBV is true.
	 * 
	 * If the argument is a numeric type or a typed literal with a datatype
	 * derived from a numeric type, the EBV is false if the operand value is NaN
	 * or is numerically equal to zero; otherwise the EBV is true.
	 * 
	 * All other arguments, including unbound arguments, produce a type error.
	 * 
	 * An EBV of true is represented as a typed literal with a datatype of
	 * xsd:boolean and a lexical value of "true"; an EBV of false is represented
	 * as a typed literal with a datatype of xsd:boolean and a lexical value of
	 * "false".
	 */
    public boolean accept(final IBindingSet bs) {

    	final IV iv = get(0).get(bs);
    	
    	if (iv instanceof XSDBooleanIV) {
    		return ((XSDBooleanIV) iv).booleanValue();
    	}
    	
        throw new SparqlTypeErrorException();

    }
    
}
