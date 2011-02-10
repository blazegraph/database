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

import org.openrdf.model.Literal;
import org.openrdf.model.impl.LiteralImpl;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;

/**
 * Operator combines the string values over the presented binding sets for the
 * given variable. Missing values are ignored. The initial value is an empty
 * plain literal.
 * 
 * @author thompsonbry
 * 
 *         FIXME This must only operate on variables which are known to be
 *         materialized RDF Values.
 */
public class GROUP_CONCAT extends AggregateBase<Literal> implements IAggregate<Literal> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public interface Annotations extends AggregateBase.Annotations {

		/**
		 * Required string property provides the separator used when combining
		 * the {@link IValueExpression} computed for each solution within the
		 * group.
		 */
		String SEPARATOR = AggregateBase.class.getName()+".separator";

	}
	
	public GROUP_CONCAT(BOpBase op) {
		super(op);
	}

	public GROUP_CONCAT(BOp[] args, Map<String, Object> annotations) {
		super(args, annotations);
	}

	/**
	 * 
	 * @param var
	 *            The variable whose values will be combined.
	 * @param sep
	 *            The separator string.
	 */
	public GROUP_CONCAT(final boolean distinct,
			final IValueExpression<Literal> expr, final IConstant<String> sep) {
		this(new BOp[] { expr }, NV.asMap(//
				new NV(Annotations.DISTINCT, distinct),//
				new NV(Annotations.SEPARATOR, sep)//
				));
	}
	
	/**
	 * The running concatenation of observed bound values.
	 * <p>
	 * Note: This field is guarded by the monitor on the {@link GROUP_CONCAT}
	 * instance.
	 */
	private transient Literal aggregated = new LiteralImpl("");
	
	@Override
	synchronized public Literal get(final IBindingSet bindingSet) {

		final IVariable<Literal> var = (IVariable<Literal>) get(0);

		final Literal val = (Literal) bindingSet.get(var);

		if (val != null) {

			final IConstant<String> sep = (IConstant<String>) get(1);

			aggregated = new LiteralImpl(aggregated.getLabel() + sep.get()
					+ val.stringValue());

		}

		return aggregated;

	}

}
