/*

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

import org.openrdf.model.Value;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.sparql.ast.FilterNode;

/**
 * Calculates the "effective boolean value" of an IValueExpression.  See the
 * SPARQL spec for details.
 */
public class EBVBOp extends XSDBooleanIVValueExpression 
		implements INeedsMaterialization {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5701967329003122236L;

    @SuppressWarnings("rawtypes")
    public EBVBOp(final IValueExpression<? extends IV> x) { 

        this(new BOp[] { x }, BOp.NOANNS);

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
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
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
    @Override
    @SuppressWarnings("rawtypes")
    public boolean accept(final IBindingSet bs) {

        final IV iv = getAndCheckBound(0, bs);

        if (iv instanceof XSDBooleanIV) {

            return ((XSDBooleanIV) iv).booleanValue();

        }

        final Value val = super.asValue(iv);

        try {

            return QueryEvaluationUtil.getEffectiveBooleanValue(val);

        } catch (ValueExprEvaluationException ex) {

            throw new SparqlTypeErrorException();

        }

    }

    /**
     * The {@link EBVBOp} only needs materialization if its internal value
     * expression does not evaluate to an {@link XSDBooleanIV}.
     */
    @Override
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.SOMETIMES;

    }
    
}
