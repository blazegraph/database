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
 * Created on Jun 24, 2008
 */

package com.bigdata.join;

import java.util.Comparator;

import com.bigdata.join.rdf.SPOKeyOrder;

/**
 * Wraps the {@link Comparator} obtained from a {@link SPOKeyOrder} such that it
 * will ordered {@link ISolution}s by the elements reported by
 * {@link ISolution#get()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SolutionComparator<E> implements Comparator<ISolution<E>> {

    private final Comparator<E> comparator;

    public SolutionComparator(IKeyOrder<E> keyOrder) {

        this.comparator = keyOrder.getComparator();

    }

    public int compare(ISolution<E> arg0, ISolution<E> arg1) {

        return comparator.compare(arg0.get(), arg1.get());

    }

}
