/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Sep 24, 2008
 */

package com.bigdata.bop.solutions;

import java.io.Serializable;

import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;

/**
 * A variable and an order that will be imposed on the values for that variable.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISortOrder<E> extends Serializable {

	/**
	 * The variable whose values will be sorted.
	 * 
	 * FIXME ORDER_BY is defined in terms of Expressions, not just Vars. Either
	 * this will need to be an {@link IValueExpression} which is evaluated
	 * during the ordering or we will have to pre-compute a hidden variable
	 * which can be ordered directly. Presumably BrackettedExpression provides a
	 * computed RDF Value while Constraint orders based on the BEV.
	 * 
	 * <pre>
	 * [23]  	OrderCondition	  ::=  	( ( 'ASC' | 'DESC' ) BrackettedExpression ) | ( Constraint | Var )
	 * </pre>
	 */
    IVariable<E> getVariable();

    /**
     * <code>true</code> iff the values will be placed into an ascending sort
     * and <code>false</code> if the values will be placed into a descending
     * sort.
     */
    boolean isAscending();
    
}
