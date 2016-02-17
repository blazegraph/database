/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

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
/*
 * Created on Sep 24, 2008
 */

package com.bigdata.bop.solutions;

import com.bigdata.bop.IValueExpression;

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

    private final IValueExpression<E> expr;
    private final boolean asc;

    /**
     * Either <code>ASC ( expr ) or DESC ( expr )</code>.
     */
    @Override
    public String toString() {

    	return (asc ? "ASC" : "DESC") + "(" + expr + ")";
    	
    }
    
    /**
     * 
     * @param expr
     *            The value expression.
     * @param asc
     *            <code>true</code> for an ascending sort and
     *            <code>false</code> for a descending sort.
     */
    public SortOrder(final IValueExpression<E> expr, final boolean asc) {

        if (expr == null)
            throw new IllegalArgumentException();

        this.expr = expr;
        
        this.asc = asc;
        
    }

    public IValueExpression<E> getExpr() {
        
        return expr;
        
    }

    public boolean isAscending() {
        
        return asc;
        
    }

}
