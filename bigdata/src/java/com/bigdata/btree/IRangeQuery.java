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
 * Created on Feb 14, 2007
 */

package com.bigdata.btree;


/**
 * Interface for range count and range query operations (non-batch api).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRangeQuery {
    
    /**
     * Return an iterator that visits the entries in a half-open key range.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @see #entryIterator(), which visits all entries in the btree.
     * 
     * @see SuccessorUtil, which may be used to compute the successor of a value
     *      before encoding it as a component of a key.
     * 
     * @see BytesUtil#successor(byte[]), which may be used to compute the
     *      successor of an encoded key.
     * 
     * @see EntryFilter, which may be used to filter the entries visited by the
     *      iterator.
     * 
     * @todo define behavior when the toKey is less than the fromKey.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey);

    /**
     * Return the #of entries in a half-open key range. The fromKey and toKey
     * need not be defined in the btree. This method computes the #of entries in
     * the half-open range exactly using {@link AbstractNode#indexOf(Object)}.
     * The cost is equal to the cost of lookup of the both keys.
     * 
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return The #of entries in the half-open key range. This will be zero if
     *         <i>toKey</i> is less than or equal to <i>fromKey</i> in the
     *         total ordering.
     * 
     * @todo this could overflow an int32 for a scale out database.
     */
    public int rangeCount(byte[] fromKey, byte[] toKey);

}
