/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jan 28, 2012
 */

package com.bigdata.btree;

import com.bigdata.mdi.ISeparatorKeys;
import com.bigdata.util.BytesUtil;

/**
 * Utility class to verify that a key lies within a key range.
 * <p>
 * Note: In order to be a useful check on the mapping of predicates across
 * shards, the check needs to verify that the as-bound predicate either lies
 * within or spans the shard, i.e., that the interaction of the as-bound
 * predicate and the shard is not empty.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/461">
 *      KeyAfterPartitionException </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RangeCheckUtil {

    /**
     * Verify that the key lies within the key range of an index partition.
     * 
     * @param key
     *            The key.
     * 
     * @param allowUpperBound
     *            <code>true</code> iff the <i>key</i> represents an inclusive
     *            upper bound and thus must be allowed to be LTE to the right
     *            separator key for the index partition. For example, this would
     *            be <code>true</code> for the <i>toKey</i> parameter on
     *            rangeCount or rangeIterator methods.
     * 
     * @return <code>true</code> always.
     * 
     * @throws IllegalArgumentException
     *             if the key is <code>null</code>
     * @throws KeyOutOfRangeException
     *             if the key does not lie within the index partition.
     * 
     * @see BytesUtil#rangeCheck(byte[], byte[], byte[])
     */
    static public boolean rangeCheck(final ISeparatorKeys pmd,
            final byte[] key, final boolean allowUpperBound) {

        final byte[] leftSeparatorKey = pmd.getLeftSeparatorKey();

        final byte[] rightSeparatorKey = pmd.getRightSeparatorKey();

        if (BytesUtil.compareBytes(key, leftSeparatorKey) < 0) {

            throw new KeyBeforePartitionException(key, allowUpperBound, pmd);

        }

        if (rightSeparatorKey != null) {

            final int ret = BytesUtil.compareBytes(key, rightSeparatorKey);

            if (allowUpperBound) {

                if (ret <= 0) {

                    // key less than or equal to the exclusive upper bound.

                } else {

                    throw new KeyAfterPartitionException(key, allowUpperBound,
                            pmd);
                }

            } else {

                if (ret < 0) {

                    // key strictly less than the exclusive upper bound.

                } else {

                    throw new KeyAfterPartitionException(key, allowUpperBound,
                            pmd);

                }

            }

        }
    
        // key lies within the index partition.
        return true;

    }

}
