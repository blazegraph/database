/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Dec 11, 2006
 */

package com.bigdata.btree;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Interface visits {@link ITuple}s populated with the data and metadata for
 * visited index entries.
 * 
 * @see IRangeQuery
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add isForwardScan(), isReverseScan(), #priorTuple(), #nextTuple(), and
 *       prior() methods so that we can scan in reverse key order.  (prior/next tuple
 *       are always relative to the key order while prior/next are relative to the 
 *       iterator scan order).
 */
public interface ITupleIterator extends Iterator<ITuple> {

    /**
     * Advance the iterator and return the {@link ITuple} from which you can
     * extract the data and metadata for next entry.
     * <p>
     * Note: An {@link ITupleIterator}s will generally return the <em>same</em>
     * {@link ITuple} reference on on each invocation of this method. The caller
     * is responsible for copying out any data or metadata of interest before
     * calling {@link #next()} again.
     * 
     * @return The {@link ITuple} containing the data and metadata for the
     *         current index entry.
     * 
     * @throws NoSuchElementException
     *             if there is no next entry.
     */
    public ITuple next();
    
}
