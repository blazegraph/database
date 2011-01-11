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
 * Created on Jun 19, 2008
 */

package com.bigdata.bop;

import java.io.Serializable;


/**
 * Abstraction models either a constant or an unbound variable.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IVariableOrConstant<E> extends IValueExpression<E> {

    /**
     * Return <code>true</code> iff this is a variable.
     */
    boolean isVar();

    /**
     * Return <code>true</code> iff this is a constant.
     */
    boolean isConstant();
    
    /**
     * Return <code>true</code> if this is the same variable or if both values
     * are {@link Constant} whose values are the same.
     */
    boolean equals(IVariableOrConstant<E> o);

    /**
     * Return the bound value.
     * 
     * @throws UnsupportedOperationException
     *             if this is a variable.
     */
    E get();

    /**
     * Return the name of a variable.
     * 
     * @throws UnsupportedOperationException
     *             if this is not a variable.
     */
    String getName();

}
