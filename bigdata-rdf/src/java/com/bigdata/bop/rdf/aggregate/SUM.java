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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.util.InnerCause;

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

    public SUM(BOpBase op) {
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
	private transient IV aggregated = new XSDIntIV<BigdataLiteral>(0);
	
	synchronized
	public IV get(final IBindingSet bindingSet) {

		final IVariable<IV> var = (IVariable<IV>) get(0);

		final IConstant<IV> val = (IConstant<IV>) bindingSet.get(var);

        try {

            if (val != null) {

                /*
                 * Aggregate non-null values.
                 */

                final IV iv = val.get(bindingSet);

                if (iv == null)
                    throw new SparqlTypeErrorException.UnboundVarException();

                if (iv.isInline()) {

                    aggregated = IVUtility.numericalMath(iv, aggregated,
                            MathOp.PLUS);

                } else {

                    /*
                     * FIXME For this code path, write a version of
                     * IVUtility.numericalMath() which accepts one inline
                     * numeric IV and one BigdataLiteral (materialized value)
                     * and then does the type promotion. Because the type
                     * promotion rules are so simple, we will always wind up
                     * with an inline numeric IV.
                     */
                    final BigdataValue val1 = iv.getValue();

                    final BigdataValue val2 = aggregated.getValue();

                    if (val1 == null || val2 == null)
                        throw new NotMaterializedException();

                    if (!(val1 instanceof Literal)
                            || !(val2 instanceof Literal)) {
                        throw new SparqlTypeErrorException();
                    }

                    aggregated = IVUtility.literalMath((Literal) val1,
                            (Literal) val2, MathOp.PLUS);

                }

            }
            
            return aggregated;

        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, SparqlTypeErrorException.class)) {

                // trap the type error and filter out the solution
                if (log.isInfoEnabled())
                    log.info("discarding solution due to type error: "
                            + bindingSet + " : " + t);

                return aggregated;

            }

            throw new RuntimeException(t);

        }

	}

    /**
     * Note: {@link SUM} can work on inline numerics. It is only when the
     * operands evaluate to non-inline numerics that this bop needs
     * materialization, and that can only happen (today) with an xsd unsigned
     * datatype (we would reject non-numeric datatypes with a type error).
     * 
     * FIXME MikeP: What is the right return value here?
     * 
     * TODO Write unit tests for SUM for xsd inline {@link IV}s as well as
     * materialized {@link BigdataLiteral}s.
     */
    public Requirement getRequirement() {
        
        return INeedsMaterialization.Requirement.ALWAYS;
        
    }
    
    private volatile transient Set<IVariable<IV>> terms;
    
    public Set<IVariable<IV>> getTermsToMaterialize() {
    
        if (terms == null) {

            final IValueExpression<IV> e = getExpression();

            if (e instanceof IVariable<?>)
                terms = Collections.singleton((IVariable<IV>) e);
            else
                terms = Collections.emptySet();

        }

        return terms;

    }

}
