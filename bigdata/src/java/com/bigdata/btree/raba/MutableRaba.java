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
 * Created on Mar 6, 2008
 */

package com.bigdata.btree.raba;

import com.bigdata.btree.proc.IKeyArrayIndexProcedure;

/**
 * Flyweight implementation for wrapping a <code>byte[][]</code> with
 * fromIndex and toIndex.
 * <p>
 * Note: This implementation is used when we split an
 * {@link IKeyArrayIndexProcedure} based on a key-range partitioned index. The
 * {@link MutableKeyBuffer} will not work for this case since it is not aware of
 * a fromIndex and a toIndex.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MutableRaba extends AbstractRaba implements IRandomAccessByteArray {

    /**
     * This view is mutable.
     */
    public boolean isReadOnly() {

        return false;

    }

    /**
     * This view allows <code>null</code>s.
     */
    public boolean isNullAllowed() {
        
        return true;
        
    }

    /**
     * This view does not support search.
     */
    public boolean isSearchable() {
    
        return false;
        
    }
    
    /**
     * Create a view of a byte[][]. All elements in the array are visible in the
     * view.
     * 
     * @param a
     *            The backing byte[][].
     */
    public MutableRaba(final byte[][] a) {

        this(0/* fromIndex */, a.length/* toIndex */, a.length/* capacity */, a);

    }

    /**
     * Create a view of a byte[][].
     * 
     * @param fromIndex
     *            The index of the first visible in the view (inclusive lower
     *            bound).
     * @param toIndex
     *            The index of the first element beyond the view (exclusive
     *            upper bound). If toIndex == fromIndex then the view is empty.
     * @param a
     *            The backing byte[][].
     * 
     * @deprecated This ctor assumes
     *             <code>capacity == a.length - fromIndex</code>.
     */
    public MutableRaba(final int fromIndex, final int toIndex, final byte[][] a) {

        this(fromIndex, toIndex, a.length - fromIndex, a);

    }

    /**
     * Create a view from a slice of a byte[][].
     * 
     * @param fromIndex
     *            The index of the first element in the byte[][] which is
     *            visible in the view (inclusive lower bound).
     * @param toIndex
     *            The index of the first element in the byte[][] beyond the view
     *            (exclusive upper bound).
     * @param capacity
     *            The #of elements which may be used in the view.
     * @param a
     *            The backing byte[][].
     */
    public MutableRaba(final int fromIndex, final int toIndex,
            final int capacity, final byte[][] a) {

        super(fromIndex, toIndex, capacity, a);
        
    }
        
    /**
     * Resize the buffer, copying the references to the existing data into a new
     * view backed by a new byte[][]. fromIndex will be zero in the new view.
     * 
     * @param n
     *            The size of the new buffer.
     * 
     * @return The new view, backed by a new byte[][].
     */
    public MutableRaba resize(final int n) {

        assertNotReadOnly();
        
        if (n < 0)
            throw new IllegalArgumentException();

        // #of entries in the source.
        final int m = size();

        // #of entries to be copied into the new buffer.
        final int p = Math.min(m, n);

        // new backing array sized to [n].
        final byte[][] b = new byte[n][];

        // copy references to the new buffer.
        System.arraycopy(a, fromIndex, b, 0, p);

        return new MutableRaba(0/* fromIndex */,
                p/* toIndex */, a.length/* capacity */, a);

    }

}
