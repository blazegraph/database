/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 21, 2010
 */

package com.bigdata.bop.engine;

import com.bigdata.bop.PipelineOp;

/**
 * Annotations understood by the {@link QueryEngine} which are used for some
 * unit tests but which should not be used for real queries.
 * <p>
 * Note: This class is in the main source tree because {@link QueryEngine}
 * references it, but the annotations defined here should only be specified from
 * within a unit test.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QueryEngineTestAnnotations {

    /**
     * When <code>true</code>, each chunk will be sent out using its own
     * {@link IChunkMessage}. Otherwise the {@link QueryEngine} MAY (and
     * generally does) combine the chunks in the output of a given operator
     * evaluation pass into a single {@link IChunkMessage} per target query
     * peer.
     * <p>
     * Note: This annotation was introduced to make it easier to control the #of
     * {@link IChunkMessage}s output from a given operator and thereby diagnose
     * {@link RunState} termination conditions linked to having multiple
     * {@link IChunkMessage}s.
     * <p>
     * Note: Just controlling the {@link PipelineOp.Annotations#CHUNK_CAPACITY}
     * and {@link PipelineOp.Annotations#CHUNK_OF_CHUNKS_CAPACITY} is not enough
     * to force the {@link QueryEngine} to run the an operator once per source
     * chunk. The {@link QueryEngine} normally combines chunks together. You
     * MUST also specify this annotation in order for the query engine to send
     * multiple {@link IChunkMessage} rather than just one.
     * 
     * @deprecated Support for this is no longer present. It was lost when the
     *             {@link StandaloneChunkHandler} was written.
     */
    String ONE_MESSAGE_PER_CHUNK = QueryEngineTestAnnotations.class.getName()
            + ".oneMessagePerChunk";

    boolean DEFAULT_ONE_MESSAGE_PER_CHUNK = false;

	/**
	 * This option may be used to place an optional limit on the #of concurrent
	 * tasks which may run for the same (bopId,shardId) for a given query. The
	 * query is guaranteed to make progress as long as this is some positive
	 * integer. Limiting this value can limit the concurrency with which certain
	 * operators are evaluated and that can have a negative effect on the
	 * throughput for a given query.
	 */
    String MAX_CONCURRENT_TASKS_PER_OPERATOR_AND_SHARD = QueryEngineTestAnnotations.class.getName()
            + ".maxConcurrentTasksPerOperatorAndShard";

    /**
     * The default is essentially unlimited.
     */
    int DEFAULT_MAX_CONCURRENT_TASKS_PER_OPERATOR_AND_SHARD = Integer.MAX_VALUE; 
    
}
