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
 * Created on Sep 6, 2010
 */

package com.bigdata.bop.fed;

import java.nio.ByteBuffer;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.BindingSetChunk;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.service.IBigdataFederation;

/**
 * Extends {@link RunningQuery} to provide additional state and logic required
 * to support distributed query evaluation against an {@link IBigdataFederation}
 * .
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo SCALEOUT: Life cycle management of the operators and the query implies
 *       both a per-query bop:NodeList map on the query coordinator identifying
 *       the nodes on which the query has been executed and a per-query
 *       bop:ResourceList map identifying the resources associated with the
 *       execution of that bop on that node. In fact, this could be the same
 *       {@link #resourceMap} except that we would lose type information about
 *       the nature of the resource so it is better to have distinct maps for
 *       this purpose.
 * 
 * @todo HA aspects of running queries? Checkpoints for long running queries?
 * */
public class FederatedRunningQuery extends RunningQuery {

    public FederatedRunningQuery(FederatedQueryEngine queryEngine,
            long queryId, long readTimestamp, long writeTimestamp, long begin,
            long timeout, boolean controller, IQueryClient clientProxy,
            BOp query, IBlockingBuffer<IBindingSet[]> queryBuffer) {

        super(queryEngine, queryId, readTimestamp, writeTimestamp, begin,
                timeout, controller, clientProxy, query, queryBuffer);
        
    }

    @Override
    public FederatedQueryEngine getQueryEngine() {

        return (FederatedQueryEngine) super.getQueryEngine();

    }

    /**
     * Create a {@link BindingSetChunk} from a sink and add it to the queue.
     * <p>
     * Note: If we are running standalone, then we leave the data on the heap
     * rather than formatting it onto a {@link ByteBuffer}.
     * 
     * @param sinkId
     * @param sink
     * 
     * @return The #of chunks made available for consumption by the sink. This
     *         will always be ONE (1) for scale-up. For scale-out, there will be
     *         one chunk per index partition over which the intermediate results
     *         were mapped.
     * 
     * @todo <p>
     *       For selective queries in s/o, first format the data onto a list of
     *       byte[]s, one per target shard/node. Then, using a lock, obtain a
     *       ByteBuffer if there is none associated with the query yet.
     *       Otherwise, using the same lock, obtain a slice onto that ByteBuffer
     *       and put as much of the byte[] as will fit, continuing onto a newly
     *       recruited ByteBuffer if necessary. Release the lock and notify the
     *       target of the ByteBuffer slice (buffer#, off, len). Consider
     *       pushing the data proactively for selective queries.
     *       <p>
     *       For unselective queries in s/o, proceed as above but we need to get
     *       the data off the heap and onto the {@link ByteBuffer}s quickly
     *       (incrementally) and we want the consumers to impose flow control on
     *       the producers to bound the memory demand (this needs to be
     *       coordinated carefully to avoid deadlocks). Typically, large result
     *       sets should result in multiple passes over the consumer's shard
     *       rather than writing the intermediate results onto the disk.
     * 
     *       FIXME SCALEOUT: This is where we need to map the binding sets over
     *       the shards for the target operator. Once they are mapped, write the
     *       binding sets onto an NIO buffer for the target node and then send
     *       an RMI message to the node telling it that there is a chunk
     *       available for the given (queryId,bopId,partitionId).
     */
    @Override
    protected int add(final int sinkId,
            final IBlockingBuffer<IBindingSet[]> sink) {

        /*
         * Note: The partitionId will always be -1 in scale-up.
         */
        final BindingSetChunk chunk = new BindingSetChunk(getQueryId(), sinkId,
                -1/* partitionId */, sink.iterator());

        addChunkToQueryEngine(chunk);

        return 1;

    }

}
