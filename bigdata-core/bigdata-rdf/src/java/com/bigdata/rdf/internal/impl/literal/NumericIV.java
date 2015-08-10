/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

package com.bigdata.rdf.internal.impl.literal;

import org.openrdf.model.Literal;

import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Superclass for the inline numerics - {@link XSDNumericIV},
 * {@link XSDIntegerIV}, and {@link XSDDecimalIV}.
 */ 
public abstract class NumericIV<V extends BigdataLiteral, T> extends
        AbstractLiteralIV<V, T> implements Literal {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2878889877313783890L;

    public NumericIV(final DTE dte) {
        
        super(dte);

    }

}
