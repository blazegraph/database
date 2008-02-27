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
 * Created on Feb 1, 2007
 */

package com.bigdata.btree;

import com.bigdata.mdi.IResourceMetadata;

/**
 * <p>
 * A read-only fused view.
 * </p>
 * 
 * @deprecated by {@link BTree#setReadOnly(boolean)}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyFusedView extends FusedView {

    public ReadOnlyFusedView(AbstractBTree src1, AbstractBTree src2) {

        super(src1, src2);

    }

    /**
     * 
     * @param srcs
     *            The ordered sources for the fused view. The order of the
     *            elements in this array determines which value will be selected
     *            for a given key by lookup() and which value is retained by
     *            rangeQuery().
     * 
     * @exception IllegalArgumentException
     *                if a source is used more than once.
     * @exception IllegalArgumentException
     *                unless all sources have the same index UUID.
     * @exception IllegalArgumentException
     *                unless all sources support delete markers.
     */
    public ReadOnlyFusedView(final AbstractBTree[] srcs) {

        super(srcs);

    }

    final public IndexMetadata getIndexMetadata() {

        // Note: clone object to disallow modification.
        return super.getIndexMetadata().clone();

    }

    final public IResourceMetadata[] getResourceMetadata() {

        // Note: clone object to disallow modification.
        return super.getResourceMetadata().clone();
        
    }

    /**
     * Disabled.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    final public byte[] insert(byte[] key, byte[] value) {

        throw new UnsupportedOperationException();

    }

    /**
     * Disabled.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    final public byte[] remove(byte[] key) {

        throw new UnsupportedOperationException();

    }

    /**
     * Iterator is read-only.
     */
    final public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IEntryFilter filter) {

        if ((flags & REMOVEALL) != 0) {

            /*
             * Note: Must be explicitly disabled!
             */

            throw new UnsupportedOperationException();

        }

        return new ReadOnlyEntryIterator(super.rangeIterator(fromKey, toKey,
                capacity, flags, filter));

    }

}
