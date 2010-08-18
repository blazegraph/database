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

package com.bigdata.relation.rule;

import com.bigdata.bop.IVariable;

/**
 * Default impl.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SortOrder<E> implements ISortOrder<E> {

    /**
     * 
     */
    private static final long serialVersionUID = -669873421670514139L;

    private final IVariable<E> var;
    private final boolean asc;

    /**
     * 
     * @param var
     *            The variable.
     * @param asc
     *            <code>true</code> for an ascending sort and
     *            <code>false</code> for a descending sort.
     */
    public SortOrder(IVariable<E> var, boolean asc) {

        if (var == null)
            throw new IllegalArgumentException();

        this.var = var;
        
        this.asc = asc;
        
    }

    public IVariable<E> getVariable() {
        
        return var;
        
    }

    public boolean isAscending() {
        
        return asc;
        
    }

}
