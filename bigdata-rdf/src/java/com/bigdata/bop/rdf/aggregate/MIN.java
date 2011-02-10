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
 */
public class MIN extends AggregateBase<IV> implements IAggregate<IV> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MIN(BOpBase op) {
		super(op);
	}

	public MIN(BOp[] args, Map<String, Object> annotations) {
		super(args, annotations);
	}

	public MIN(boolean distinct, IValueExpression<IV> expr) {
		super(distinct, expr);
	}
	
	/**
	 * The minimum observed value and initially <code>null</code>.
	 * <p>
	 * Note: This field is guarded by the monitor on the {@link MIN} instance.
	 */
	private transient IV min = null;
	
//	/**
//	 * The RDF Value comparator.
//	 */
//	private final ValueComparator vc = new ValueComparator();
	
	@Override
	synchronized
	public IV get(final IBindingSet bindingSet) {

		final IVariable<IV> var = (IVariable<IV>) get(0);

		final IV val = (IV) bindingSet.get(var);

		if (val != null) {

			if(min == null) {
			
				min = val;
				
			} else {

				if (IVUtility.compare(min, val) < 0) {

					min = val;

				}

			}

		}

		return min;

	}

}
