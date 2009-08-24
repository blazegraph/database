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
 * Flyweight implementation for wrapping a <code>byte[][]</code> with fromIndex
 * and toIndex.
 * 
 * @todo This implementation is used when we split an
 *       {@link IKeyArrayIndexProcedure} based on a key-range partitioned index.
 *       The {@link MutableKeyBuffer} will not work for this case since it is
 *       not aware of a fromIndex and a toIndex. However,
 *       {@link ReadOnlyValuesRaba} and {@link ReadOnlyKeysRaba} would work
 *       better for those use cases.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MutableKeysRaba extends AbstractRaba implements IRaba {

    /**
     * No - this view is mutable.
     */
    public boolean isReadOnly() {

        return false;

    }

    /**
     * Yes.
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
    public MutableKeysRaba(final byte[][] a) {

        this(0/* fromIndex */, a.length/* toIndex */, a.length/* capacity */, a);

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
    public MutableKeysRaba(final int fromIndex, final int toIndex,
            final int capacity, final byte[][] a) {

        super(fromIndex, toIndex, capacity, a);

    }

    public MutableKeysRaba resize(final int n) {

        return (MutableKeysRaba) super.resize(n);

    }

}
