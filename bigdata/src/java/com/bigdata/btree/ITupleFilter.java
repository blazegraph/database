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

package com.bigdata.btree;

import java.io.Serializable;

/**
 * Interface for filtering key range scans.
 * 
 * @see ITupleIterator
 * 
 * @see AbstractBTree#rangeIterator(byte[], byte[], int, int, EntryFilter)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITupleFilter extends Serializable {

    /**
     * Chains a filter after this one.
     * 
     * @param filter
     */
    public void add(ITupleFilter filter);
    
    /**
     * Return <code>true</code> iff the index entry should be visited.
     * 
     * @param tuple
     *            A tuple revealing data from the next index entry to be visited
     *            by the iterator.
     * 
     * @return <code>true</code> iff the index entry should be visited.
     */
    public boolean isValid(ITuple tuple);

    /**
     * Method may (re-)write the value associated with an {@link ITuple} that
     * the iterator would visit. The default implementation is a NOP. This
     * method is applied <em>after</em> {@link #isValid(ITuple)}. The
     * operation does not have a side-effect on the value stored in the index.
     * 
     * @param tuple
     *            A tuple that will be visited by the iterator.
     * 
     * @return The value that will be visited.
     */
    public void rewrite(ITuple tuple);

}
