/**

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
package com.bigdata.bop.rdf.aggregate;

import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Operator computes the running sum over the presented binding sets for the
 * given variable. A missing value does not contribute towards the sum.
 * 
 * @author thompsonbry
 */
public class SUM extends AggregateBase<IV> implements IAggregate<IV>,
        INeedsMaterialization {

    private static final transient Logger log = Logger.getLogger(SUM.class);

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    public SUM(SUM op) {
        super(op);
    }

    public SUM(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    public SUM(boolean distinct, IValueExpression<IV> expr) {
        super(FunctionCode.SUM, distinct, expr);
    }

    /**
     * The running aggregate value.
     * <p>
     * Note: SUM() returns ZERO if there are no non-error solutions presented.
     * This assumes that the ZERO will be an xsd:int ZERO.
     * <p>
     * Note: This field is guarded by the monitor on the {@link SUM} instance.
     */
    private transient IV aggregated = ZERO;

    synchronized public void reset() {
        aggregated = ZERO;
    }

    synchronized public IV done() {
        return aggregated;
    }

    synchronized public IV get(final IBindingSet bindingSet) {

        final IValueExpression<IV> expr = (IValueExpression<IV>) get(0);

        final IV iv = expr.get(bindingSet);

        if (iv != null) {

            /*
             * Aggregate non-null values.
             */

            if (iv.isInline()) {

                // Two IVs.
                aggregated = IVUtility.numericalMath(iv, aggregated,
                        MathOp.PLUS);

            } else {

                // One IV and one Literal.
                final BigdataValue val1 = iv.getValue();

                if (val1 == null)
                    throw new NotMaterializedException();

                if (!(val1 instanceof Literal))
                    throw new SparqlTypeErrorException();

                // Only numeric value can be used in math expressions
                final URI dt1 = ((Literal) val1).getDatatype();
                if (dt1 == null || !XMLDatatypeUtil.isNumericDatatype(dt1))
                    throw new SparqlTypeErrorException();

                aggregated = IVUtility.numericalMath((Literal) val1,
                        aggregated, MathOp.PLUS);

            }

        }

        return aggregated;

    }

    /**
     * Note: {@link SUM} only works on numerics. If they are inline, then that
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
