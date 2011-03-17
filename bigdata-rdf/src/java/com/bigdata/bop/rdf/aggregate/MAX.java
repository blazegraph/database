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

import org.openrdf.query.algebra.evaluation.util.ValueComparator;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.aggregate.AggregateBase.FunctionCode;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;

/**
 * Operator reports the minimum observed value over the presented binding sets
 * for the given variable using SPARQL ORDER_BY semantics. Missing values are
 * ignored.
 * 
 * @author thompsonbry
 * 
 * @todo What is reported if there are no non-null observations?
 * 
 *       FIXME MIN (and MAX) are defined in terms of the ORDER_BY semantics for
 *       SPARQL. Therefore, this must handle comparisons when the value is not
 *       an IV, e.g., using {@link ValueComparator}.
 * 
 * @deprecated I am not convinced that a concrete operator can be implemented in
 *             this manner rather than by a tight integration with the GROUP_BY
 *             operator implementation.
 */
public class MAX extends AggregateBase<IV> implements IAggregate<IV> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MAX(BOpBase op) {
		super(op);
	}

	public MAX(BOp[] args, Map<String, Object> annotations) {
		super(args, annotations);
	}

	public MAX(boolean distinct, IValueExpression<IV> expr) {
		super(FunctionCode.MAX,distinct, expr);
	}
	
	/**
	 * The maximum observed value and initially <code>null</code>.
	 * <p>
	 * Note: This field is guarded by the monitor on the {@link MAX} instance.
	 */
	private transient IV max = null;
	
//	/**
//	 * The RDF Value comparator.
//	 */
//	private final ValueComparator vc = new ValueComparator();
	
	synchronized
	public IV get(final IBindingSet bindingSet) {

		final IVariable<IV> var = (IVariable<IV>) get(0);

		final IV val = (IV) bindingSet.get(var);

		if (val != null) {

			if(max == null) {
			
				max = val;
				
			} else {

				if (IVUtility.compare(max, val) > 0) {

					max = val;

				}

			}

		}

		return max;

	}

}
