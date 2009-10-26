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

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;

/**
 * Immutable implementation does not allow <code>null</code>s but supports
 * search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyKeysRaba extends AbstractRaba {

    /**
     * A read-only empty keys raba.
     */
    public static final transient ReadOnlyKeysRaba EMPTY = new ReadOnlyKeysRaba(
            BytesUtil.EMPTY2);

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
     * Create a view of a <code>byte[][]</code> slice. The slice will include
     * only those elements between the fromIndex and the toIndex. The capacity
     * will be the #of elements. {@link #isFull()} will report <code>true</code>
     * .
     * <p>
     * Note: This constructor is used when we split an
     * {@link IKeyArrayIndexProcedure} based on a key-range partitioned index.
     * 
     * @param fromIndex
     *            The index of the first visible in the view (inclusive lower
     *            bound).
     * @param toIndex
     *            The index of the first element beyond the view (exclusive
     *            upper bound). If toIndex == fromIndex then the view is empty.
     * @param a
     *            The backing byte[][].
     */
    public ReadOnlyKeysRaba(final int fromIndex, final int toIndex,
            final byte[][] a) {

        this(fromIndex, toIndex, a.length - fromIndex, a);

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

        // FIXME This is too much overhead for normal runtime.
//        assert assertTotalOrder(fromIndex, toIndex, a);
        
    }

    /**
     * Asserts that the <code>unsigned byte[]</code>s in the logical byte[][]
     * are in a total order.
     * 
     * @param fromIndex
     *            The first index in the view (inclusive lower bound).
     * @param toIndex
     *            The first index outside the view (exclusive upper bound).
     * @param a
     *            The backing byte[][].
     *            
     * @return true if the total ordering constraint is respected by the data.
     */
    protected boolean assertTotalOrder(final int fromIndex, final int toIndex,
            final byte[][] a) {

        for (int i = fromIndex; i < toIndex; i++) {

            if (i > fromIndex) {

                final int ret = BytesUtil.compareBytes(a[i - 1], a[i]);
                
                assert ret < 0 : "unsigned byte[]s are out of order at index="
                        + i +", ret="+ret //
                        + "\na[" + (i - 1) + "]=" + BytesUtil.toString(a[i-1]) //
                        + "\na[" + (i) + "]=" + BytesUtil.toString(a[i]);

            }
            
        }
        
        return true;
        
    }

}
