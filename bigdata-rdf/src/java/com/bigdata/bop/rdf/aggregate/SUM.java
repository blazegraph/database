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

import org.openrdf.query.algebra.MathExpr.MathOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.XSDLongIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Operator computes the running sum over the presented binding sets for the
 * given variable. A missing value does not contribute towards the sum.
 * 
 * @author thompsonbry
 * 
 *         FIXME This must handle non-inline IVs as type errors and just skip
 *         over them (but the operator can only handle KBs where we are inlining
 *         the numeric values - perhaps we should just get rid of the option to
 *         not inline and require people to export/import for an upgrade).
 */
public class SUM extends AggregateBase<IV> implements IAggregate<IV> {

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
		super(distinct, expr);
	}
	
	/**
	 * The running aggregate value.
	 * <p>
	 * Note: SUM() returns ZERO if there are no non-error solutions presented.
	 * This assumes that the ZERO will be xsd:long, but we could as easily
	 * return an xsd:int ZERO.
	 * <p>
	 * Note: This field is guarded by the monitor on the {@link SUM} instance.
	 */
	private transient IV aggregated = new XSDLongIV<BigdataLiteral>(0L);
	
	@Override
	synchronized
	public IV get(final IBindingSet bindingSet) {

		final IVariable<IV> var = (IVariable<IV>) get(0);

		final IV val = (IV) bindingSet.get(var);

		if (val != null) {

			/*
			 * aggregate non-null values.
			 * 
			 * FIXME We need a distinguished exception for type conversion
			 * errors. Such errors should cause the aggregate to not update, but
			 * should not cause the aggregate to throw out an exception. The
			 * other way to handle type conversion errors is to return an IV for
			 * [err], perhaps using the last of the inline data types (or a
			 * null).
			 */
			aggregated = IVUtility.numericalMath(val, aggregated, MathOp.PLUS);

		}

		return aggregated;

	}

}
