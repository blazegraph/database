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
 * Created on Aug 7, 2009
 */

package com.bigdata.btree.raba;

/**
 * Immutable implementation does not allow <code>null</code>s but supports
 * search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyKeysRaba extends AbstractRaba {

    /**
     * This view is read-only.
     */
    final public boolean isReadOnly() {

        return true;

    }

    /**
     * For B+Tree keys (does not allow <code>null</code>s, is searchable).
     */
    final public boolean isKeys() {
    
        return true;
        
    }

    /**
     * Create a view of a byte[][]. All elements in the array are visible in the
     * view.
     * 
     * @param a
     *            The backing byte[][].
     */
    public ReadOnlyKeysRaba(final byte[][] a) {

        this(0/* fromIndex */, a.length/* toIndex */, a.length/* capacity */, a);

    }

    /**
     * Create a view of a byte[][]. All elements in the array are visible in the
     * view. The elements in the array from index ZERO (0) through index
     * <code>size-1</code> are assumed to have valid data.
     * 
     * @param size
     *            The #of elements with valid data.
     * @param a
     *            The backing byte[][].
     */
    public ReadOnlyKeysRaba(final int size, final byte[][] a) {

        this(0/* fromIndex */, size/* toIndex */, a.length/* capacity */, a);

    }

    /**
     * Create a view from a slice of a byte[][].
     * 
     * @param fromIndex
     *            The index of the first element in the byte[][] which is
     *            visible in the view (inclusive lower bound).
     * @param toIndex
     *            The index of the first element in the byte[][] which lies
     *            beyond the view (exclusive upper bound).
     * @param capacity
     *            The #of elements which may be used in the view.
     * @param a
     *            The backing byte[][].
     */
    public ReadOnlyKeysRaba(final int fromIndex, final int toIndex,
            final int capacity, final byte[][] a) {

        super(fromIndex, toIndex, capacity, a);

    }

}
