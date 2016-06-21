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
 * Created on June 21, 2016
 */
package com.bigdata.bop.engine;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rwstore.sector.MemoryManager;

/**
 * Class providing helper methods related to the query engine.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class QueryEngineUtils {

    /**
     * Creates a native memory manager for usage from withing the query engine,
     * bounded by {@link QueryHints#DEFAULT_ANALYTIC_MAX_MEMORY_PER_QUERY}.
     * IMPORTANT: the the caller needs to take care to properly close() the 
     * memory manager in order to release allocated memory.
     * 
     * @see QueryHints#ANALYTIC_MAX_MEMORY_PER_QUERY
     * @see QueryHints#DEFAULT_ANALYTIC_MAX_MEMORY_PER_QUERY
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-42" > Per query
     *      memory limit for analytic query mode. </a>
     **/
    public static MemoryManager newBoundedNativeMemoryManager() {
        
        // The native memory pool that will be used by this query.
        final DirectBufferPool pool = DirectBufferPool.INSTANCE;
        
        // Figure out how much memory may be allocated by this query.
        long maxMemoryBytesPerQuery = QueryHints.DEFAULT_ANALYTIC_MAX_MEMORY_PER_QUERY;
        if (maxMemoryBytesPerQuery < 0) {
            // Ignore illegal values.
            maxMemoryBytesPerQuery = 0L;
        }

        final boolean blocking;
        final int nsectors;
        if (maxMemoryBytesPerQuery == 0) {
            /*
             * Allocation are blocking IFF there is no bound on the memory for
             * the query.
             */
            blocking = true; // block until allocation is satisfied.
            nsectors = Integer.MAX_VALUE; // no limit
        } else {
            /*
             * Allocations do not block if we run out of native memory for this
             * query. Instead a memory allocation exception will be thrown and
             * the query will break.
             * 
             * The #of sectors is computed by dividing through by the size of
             * the backing native ByteBuffers and then rounding up.
             */
            blocking = false; // throw exception if query uses too much RAM.

            // The capacity of the buffers in this pool.
            final int bufferCapacity = pool.getBufferCapacity();

            // Figure out the maximum #of buffers (rounding up).
            nsectors = (int) Math.ceil(maxMemoryBytesPerQuery
                    / (double) bufferCapacity);

        }

        return new MemoryManager(pool, nsectors, blocking, null/* properties */);

    }    

}
