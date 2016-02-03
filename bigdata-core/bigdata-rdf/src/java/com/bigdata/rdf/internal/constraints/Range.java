/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2011.  All rights reserved.

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

import java.io.Serializable;

import com.bigdata.rdf.internal.IV;

/**
 * Represents a numerical range of IVs - a lower bound and an upper bound.
 * Useful for constraining predicates to a particular range of values for the
 * object.
 */
public class Range implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -706615195901299026L;

	private final IV from, to;
	
	/**
	 * Construct a numerical range using two IVs.  The range includes the from
	 * and to value (>= from && <= to).  Non-inclusive from and to must be
	 * accomplished using a filter.  The from must be less than or equal to the 
	 * to.
	 */
	public Range(final IV from, final IV to) {
		
		if (!from.isNumeric())
			throw new IllegalArgumentException("not numeric: " + from);
		if (!to.isNumeric())
			throw new IllegalArgumentException("not numeric: " + to);
		
		this.from = from;
		this.to = to;
		
	}
	
	public IV from() {
		return from;
	}
	
	public IV to() {
		return to;
	}
	
}
