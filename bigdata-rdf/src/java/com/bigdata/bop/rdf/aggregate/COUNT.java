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

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IAggregate;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSDLongIV;

/**
 * Operator computes the number of non-null values over the presented binding
 * sets for the given variable.
 * 
 * @author thompsonbry
 */
public class COUNT extends ImmutableBOp implements IAggregate<IV> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public COUNT(BOpBase op) {
		super(op);
	}

	/**
	 * FIXME This must also accept '*' in lieu of a variable. When given a '*'
	 * (which could be modeled as a special variable name), we count all detail
	 * records without regard to their bound values.
	 * 
	 * @param var
	 */
	public COUNT(IVariable<IV> var) {
		this(new BOp[] { var }, null/* annotations */);
	}
	
	public COUNT(BOp[] args, Map<String, Object> annotations) {
		super(args, annotations);
	}

	/**
	 * The running aggregate value.
	 * <p>
	 * Note: This field is guarded by the monitor on the {@link COUNT} instance.
	 */
	private transient long aggregated = 0L;

	/**
	 * {@inheritDoc}
	 * <p>
	 * Note: COUNT() returns ZERO if there are no non-error solutions presented.
	 * This assumes that the ZERO will be an xsd:long.
	 */
	@Override
	synchronized
	public IV get(final IBindingSet bindingSet) {

		final IVariable<IV> var = (IVariable<IV>) get(0);

		final IV val = (IV) bindingSet.get(var);

		if (val != null) {

			// aggregate non-null values.
			aggregated++;

		}

		return new XSDLongIV(aggregated);

	}

}
