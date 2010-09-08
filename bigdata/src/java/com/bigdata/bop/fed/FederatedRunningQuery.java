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
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.master.IAsynchronousClientTask;
import com.bigdata.striterator.IKeyOrder;

/**
 * Extends {@link RunningQuery} to provide additional state and logic required
 * to support distributed query evaluation against an {@link IBigdataFederation}
 * .
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: FederatedRunningQuery.java 3511 2010-09-06 20:45:37Z
 *          thompsonbry $
 * 
 * @todo SCALEOUT: We need to model the chunks available before they are
 *       materialized locally such that (a) they can be materialized on demand
 *       (flow control); and (b) we can run the operator when there are
 *       sufficient chunks available without taking on too much data.
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
     * {@inheritDoc}
     * 
     * @return The #of chunks made available for consumption by the sink. This
     *         will always be ONE (1) for scale-up. For scale-out, there will be
     *         one chunk per index partition over which the intermediate results
     *         were mapped.
     * 
     *         FIXME SCALEOUT: This is where we need to map the binding sets
     *         over the shards for the target operator. Once they are mapped,
     *         write the binding sets onto an NIO buffer for the target node and
     *         then send an RMI message to the node telling it that there is a
     *         chunk available for the given (queryId,bopId,partitionId).
     *         <p>
     *         For selective queries in s/o, first format the data onto a list
     *         of byte[]s, one per target shard/node. Then, using a lock, obtain
     *         a ByteBuffer if there is none associated with the query yet.
     *         Otherwise, using the same lock, obtain a slice onto that
     *         ByteBuffer and put as much of the byte[] as will fit, continuing
     *         onto a newly recruited ByteBuffer if necessary. Release the lock
     *         and notify the target of the ByteBuffer slice (buffer#, off,
     *         len). Consider pushing the data proactively for selective
     *         queries.
     *         <p>
     *         For unselective queries in s/o, proceed as above but we need to
     *         get the data off the heap and onto the {@link ByteBuffer}s
     *         quickly (incrementally) and we want the consumers to impose flow
     *         control on the producers to bound the memory demand (this needs
     *         to be coordinated carefully to avoid deadlocks). Typically, large
     *         result sets should result in multiple passes over the consumer's
     *         shard rather than writing the intermediate results onto the disk.
     * 
     * */
    @Override
    protected <E> int add(final int sinkId,
            final IBlockingBuffer<IBindingSet[]> sink) {

        if (sink == null)
            throw new IllegalArgumentException();

        final BOp bop = bopIndex.get(sinkId);

        if (bop == null)
            throw new IllegalArgumentException();

        switch (bop.getEvaluationContext()) {
        case ANY:
            return super.add(sinkId, sink);
        case HASHED: {
            /*
             * FIXME The sink self describes the nodes over which the
             * binding sets will be mapped and the hash function to be applied
             * so we look up those metadata and apply them to distributed the
             * binding sets across the nodes.
             */
            throw new UnsupportedOperationException();
        }
        case SHARDED: {
            /*
             * FIXME The sink must read or write on a shard so we map the
             * binding sets across the access path for the sink.
             * 
             * @todo For a pipeline join, the predicate is the right hand
             * operator of the sink. This might be true for INSERT and DELETE
             * operators as well.
             * 
             * @todo IKeyOrder tells us which index will be used and should be
             * set on the predicate by the join optimizer.
             * 
             * @todo Use the read or write timestamp depending on whether the
             * operator performs mutation [this must be part of the operator
             * metadata.]
             * 
             * @todo Set the capacity of the the "map" buffer to the size of the
             * data contained in the sink (in fact, we should just process the
             * sink data in place).
             */
            final IPredicate<E> pred = null; // @todo
            final IKeyOrder<E> keyOrder = null; // @todo
            final long timestamp = getReadTimestamp(); // @todo
            final int capacity = 1000;// @todo
            final MapBindingSetsOverShardsBuffer<IBindingSet, E> mapper = new MapBindingSetsOverShardsBuffer<IBindingSet, E>(
                    getFederation(), pred, keyOrder, timestamp, capacity) {

                        @Override
                        IBuffer<IBindingSet> newBuffer(PartitionLocator locator) {
                            // TODO Auto-generated method stub
                            return null;
                        }
                
            };
            /*
             * Map the binding sets over shards.
             * 
             * FIXME The buffers created above need to become associated with
             * this query as resources of the query. Once we are done mapping
             * the binding sets over the shards, the target node for each buffer
             * needs to be set an RMI message to let it know that there is a
             * chunk available for it for the target operator.
             */
            {
                final IAsynchronousIterator<IBindingSet[]> itr = sink
                        .iterator();
                try {
                    while (itr.hasNext()) {
                        final IBindingSet[] chunk = itr.next();
                        for (IBindingSet bset : chunk) {
                            mapper.add(bset);
                        }
                    }
                } finally {
                    itr.close();
                    sink.close();
                }
            }
            
            throw new UnsupportedOperationException();
        }
        case CONTROLLER: {

            final IQueryClient clientProxy = getQueryController();

//            getQueryEngine().getResourceService().port;
//            
//            clientProxy.bufferReady(clientProxy, serviceAddr, getQueryId(), sinkId);

            throw new UnsupportedOperationException();
        }
        default:
            throw new AssertionError(bop.getEvaluationContext());
        }

    }

}
