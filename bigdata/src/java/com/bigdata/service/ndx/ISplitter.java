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
 * Created on May 7, 2009
 */

package com.bigdata.service.ndx;

import java.util.LinkedList;

import com.bigdata.btree.keys.KVO;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.service.Split;

/**
 * Interface for finding the {@link Split}s for an ordered set of unsigned
 * byte[] keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISplitter {

    /**
     * Identify the {@link Split}s for an ordered array of keys such that there
     * is one {@link Split} per index partition spanned by the data.
     * 
     * @param ts
     *            The timestamp for the {@link IMetadataIndex} view that will be
     *            applied to choose the {@link Split}s.
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            An array of keys. Each key is an interpreted as an unsigned
     *            byte[]. All keys must be non-null. The keys must be in sorted
     *            order.
     * 
     * @return The {@link Split}s that you can use to form requests based on
     *         the identified first/last key and partition identified by this
     *         process.
     */
    LinkedList<Split> splitKeys(final long ts, final int fromIndex,
            final int toIndex, final byte[][] keys);

    /**
     * Identify the {@link Split}s for an ordered {@link KVO}[] such that
     * there is one {@link Split} per index partition spanned by the data.
     * 
     * @param ts
     *            The timestamp for the {@link IMetadataIndex} view that will be
     *            applied to choose the {@link Split}s.
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            An array of keys. Each key is an interpreted as an unsigned
     *            byte[]. All keys must be non-null. The keys must be in sorted
     *            order.
     * 
     * @return The {@link Split}s that you can use to form requests based on
     *         the identified first/last key and partition identified by this
     *         process.
     */
    LinkedList<Split> splitKeys(final long ts, final int fromIndex,
            final int toIndex, final KVO[] a);

}
