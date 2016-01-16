/**

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
 * Created on Sep 28, 2011
 */

package com.bigdata.relation.accesspath;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IElement;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.relation.IRelation;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * An interface for access paths which visit solutions (versus {@link IElement}
 * s).
 * 
 * @param <R>
 *            The generic type of the [R]elation elements of the
 *            {@link IRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBindingSetAccessPath<R> extends IAbstractAccessPath<R> {
    
    /**
     * Return an iterator which will visit the solutions drawn from the access
     * path with a limit on the number of solutions drawn.
     * 
     * @param limit
     *            The maximum #of solutions to visit.
     * @param stats
     *            Some statistics are updated as solutions are visited.
     * 
     * @return The iterator.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/209 (Access path
     *      should visit solutions for high level query).
     */
    ICloseableIterator<IBindingSet[]> solutions(BOpContext context, long limit, BaseJoinStats stats);
    
}
