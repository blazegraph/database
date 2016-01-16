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
package com.bigdata.bop.rdf.aggregate;

import java.util.Map;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.constraints.MathUtility;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.impl.literal.NumericIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Operator computes the running sum over the presented binding sets for the
 * given variable. A missing value does not contribute towards the sum.
 * 
 * @author thompsonbry
 */
public class AVERAGE extends AggregateBase<IV> implements  INeedsMaterialization {

//    private static final transient Logger log = Logger.getLogger(AVERAGE.class);

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    public AVERAGE(AVERAGE op) {
        super(op);
    }

    public AVERAGE(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    public AVERAGE(boolean distinct, IValueExpression<IV> expr) {

        super( distinct, expr);
    }

    /**
     * The running aggregate value.
     * <p>
     * Note: SUM() returns ZERO if there are no non-error solutions presented.
     * This assumes that the ZERO will be an xsd:int ZERO.
     * <p>
     * Note: This field is guarded by the monitor on the {@link AVERAGE}
     * instance.
     */
    private transient NumericIV aggregated = ZERO;

    /**
     * The #of observed values.
     */
    private transient long n = 0;

    /**
     * The first error encountered since the last {@link #reset()}.
     */
    private transient Throwable firstCause = null;

    synchronized public void reset() {

        aggregated = ZERO;
        
        n = 0;
        
        firstCause = null;
        
    }

    synchronized public IV done() {

        if (firstCause != null) {
        
            throw new RuntimeException(firstCause);
            
        }

        if(n == 0)
            return ZERO;
        
        return MathUtility.literalMath(aggregated,
                new XSDNumericIV<BigdataLiteral>(n), MathBOp.MathOp.DIVIDE);
        
    }

    synchronized public IV get(final IBindingSet bindingSet) {

        try {

            return doGet(bindingSet);

        } catch (Throwable t) {

            if (firstCause == null) {

                firstCause = t;
                
            }

            throw new RuntimeException(t);

        }

    }

    private IV doGet(final IBindingSet bindingSet) {

        final IValueExpression<IV> expr = (IValueExpression<IV>) get(0);

        final IV<?,?> iv = expr.get(bindingSet);

        if (iv != null) {

            /*
             * Aggregate non-null literal values.
             */
        	
            /*
             * Aggregate non-null literal values.
             */

        	final Literal lit = IVValueExpression.asLiteral(iv);
        	
        	if (!MathUtility.checkNumericDatatype(lit))
                throw new SparqlTypeErrorException();
        		
        	aggregated = 
        		MathUtility.literalMath(aggregated, lit, MathOp.PLUS);

//        	if (!iv.isLiteral())
//        		throw new SparqlTypeErrorException();
//        	
//            if (iv.isInline()) {
//
//                // Two IVs.
//                aggregated = IVUtility.numericalMath(iv, aggregated,
//                        MathOp.PLUS);
//
//            } else {
//
//                // One IV and one Literal.
//                final BigdataValue val1 = iv.getValue();
//
//                if (val1 == null)
//                    throw new NotMaterializedException();
//
////                if (!(val1 instanceof Literal))
////                    throw new SparqlTypeErrorException();
//
//                // Only numeric value can be used in math expressions
//                final URI dt1 = ((Literal)val1).getDatatype();
//                if (dt1 == null || !XMLDatatypeUtil.isNumericDatatype(dt1))
//                    throw new SparqlTypeErrorException();
//
//                aggregated = IVUtility.numericalMath((Literal) val1,
//                        aggregated, MathOp.PLUS);
//
//            }

            n++;

        }

        return aggregated;

    }

    /**
     * Note: {@link AVERAGE} only works on numerics. If they are inline, then that
     * is great. Otherwise it will handle a materialized numeric literal and do
     * type promotion, which always results in a signed inline number IV and
     * then operate on that.
     * 
     * FIXME MikeP: What is the right return value here?
     */
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.ALWAYS;

    }

}
