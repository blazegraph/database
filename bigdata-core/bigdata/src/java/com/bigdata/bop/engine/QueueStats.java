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
 * Created on Feb 13, 2012
 */

package com.bigdata.bop.engine;

import java.util.HashSet;
import java.util.Set;

/**
 * Statistics summary for a work queue feeding a specific operator for a query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class QueueStats {

    /**
     * The set of shard identifiers for which there are queued
     * {@link IChunkMessage}s. The size of this set is also the #of work queues
     * for the operator). The shard identifier will be <code>-1</code> unless
     * the operator is sharded and running on a cluster.
     */
    public final Set<Integer> shardSet;

    /**
     * The #of {@link IChunkMessage}s which are queued the operator across the
     * shards for which that operator has work queued.
     */
    public int chunkCount;

    /**
     * The #of solutions in those queued chunks.
     */
    public int solutionCount;

    public QueueStats() {

        this.shardSet = new HashSet<Integer>();
        
    }

}
