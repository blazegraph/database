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
 * Created on Apr 15, 2009
 */

package com.bigdata.service.ndx.pipeline;

import com.bigdata.btree.keys.KVO;

/**
 * Interface allowing application specific elimination of duplicates.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IDuplicateRemover<O> {

    /**
     * Accepts a dense array of key-value tuples and returns a dense array of
     * key-value tuples in which duplicates have been eliminated. The elements
     * of the given array MUST NOT be modified as a side-effect.
     * <p>
     * Implementations MUST be thread-safe.
     * <p>
     * Implementations MUST test for {@link KVOList} and obey its contract (this
     * ensures that we can map {@link KVO#done()} over the eliminated duplicates
     * once the original has been written successfully onto the database).
     * 
     * @param a
     *            A sorted array of key-value tuples.
     * 
     * @return The a dense array of key-value tuples in which any duplicates
     *         have been eliminated.
     * 
     * @todo The thread safe requirement is probably not necessary. At least as
     *       implemented, duplicates are eliminated by the
     *       {@link IndexPartitionWriteTask} within a single-threaded context.
     */
    public KVO<O>[] filter(KVO<O>[] a);

}
