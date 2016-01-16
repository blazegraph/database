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
 * Created on Sep 1, 2010
 */

package com.bigdata.bop.engine;

import java.util.Comparator;

/**
 * An immutable class capturing the evaluation context of an operator against a
 * shard.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BSBundle implements Comparable<BSBundle> {

    public final int bopId;

    public final int shardId;

    @Override
    public String toString() {

        return super.toString() + "{bopId=" + bopId + ",shardId=" + shardId
                + "}";

    }

    public BSBundle(final int bopId, final int shardId) {

        this.bopId = bopId;

        this.shardId = shardId;

    }

    @Override
    public int hashCode() {

        return (bopId * 31) + shardId;

    }

    @Override
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof BSBundle))
            return false;

        final BSBundle t = (BSBundle) o;

        return bopId == t.bopId && shardId == t.shardId;

    }

    /**
     * {@inheritDoc}
     * <p>
     * This orders the {@link BSBundle}s by reverse {@link #bopId} and by
     * {@link #shardId} if the {@link #bopId} is the same. This order imposes a
     * bias to draw entries with higher {@link #bopId}s from an ordered
     * collection.
     * <p>
     * Note: Query plans are assigned bopIds from 0 through N where higher
     * bopIds are assigned to operators that occur later in the query plan. This
     * is not a strict rule, but it is a strong bias. Given that bias and an
     * ordered map, this {@link Comparator} will tend to draw from operators
     * that are further along in the query plan. This emphasizes getting results
     * through the pipeline quickly. Whether or not this {@link Comparator} has
     * any effect depends on the {@link ChunkedRunningQuery#consumeChunk()}
     * method and the backing map over the operator queues. If a hash map is
     * used, then the {@link Comparator} is ignored. If a skip list map is used,
     * then the {@link Comparator} will influence the manner in which the
     * operator queues are drained.
     */
    @Override
    public int compareTo(final BSBundle o) {

        int ret = (bopId < o.bopId) ? 1 : ((bopId == o.bopId) ? 0 : -1);

        if (ret == 0) {

            ret = (shardId < o.shardId) ? 1 : ((shardId == o.shardId) ? 0 : -1);

        }

        return ret;

    }

}
