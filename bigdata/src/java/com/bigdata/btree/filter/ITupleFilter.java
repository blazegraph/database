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
 * Created on Jan 18, 2008
 */

package com.bigdata.btree.filter;

import java.util.Iterator;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;

import cutthecrap.utils.striterators.IFilter;

/**
 * Interface for stacked filtering iterators for {@link ITuple}s.
 * 
 * @see ITupleIterator
 * 
 * @see IRangeQuery#rangeIterator(byte[], byte[], int, int, IFilterConstructor)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the objects that can be materialized using
 *            {@link ITuple#getObject()}.
 */
public interface ITupleFilter<E> extends IFilter {

    /**
     * Strengthened return type.
     */
    public ITupleIterator<E> filter(Iterator src);

}
