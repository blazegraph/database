/*

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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.XSDBooleanIV;

/**
 * Base class for RDF value expression BOps.  Value expressions perform some
 * evaluation on one or more value expressions as input and produce one
 * value expression as output (boolean, numeric value, etc.)
 */
public abstract class XSDBooleanIVValueExpression 
		extends IVValueExpression<XSDBooleanIV> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7068219781217676085L;

	/**
     * Required shallow copy constructor.
     */
    public XSDBooleanIVValueExpression(final BOp[] args, final Map<String, Object> anns) {
        super(args, anns);
    }

    /**
     * Required deep copy constructor.
     */
    public XSDBooleanIVValueExpression(final XSDBooleanIVValueExpression op) {
        super(op);
    }

    public boolean accept(final IBindingSet bs) {
    	return false;
    }
    
    public XSDBooleanIV get(final IBindingSet bs) {
    	return accept(bs) ? XSDBooleanIV.TRUE : XSDBooleanIV.FALSE;        		
    }

}
