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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.fed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.IQueryDecl;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.solutions.SliceOp;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;

/**
 * An {@link IBigdataFederation} aware {@link QueryEngine}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: FederatedQueryEngine.java 3508 2010-09-05 17:02:34Z thompsonbry
 *          $
 * 
 * @todo buffer management for s/o bindingSet[] movement
 * 
 * @todo buffer management for s/o DHT element[] movement
 * 
 * @todo Compressed representations of binding sets with the ability to read
 *       them in place or materialize them onto the java heap. The
 *       representation should be amenable to processing in C since we want to
 *       use them on GPUs as well. See {@link IChunkMessage} and perhaps
 *       {@link IRaba}.
 */
public class FederatedQueryEngine extends QueryEngine {

    /**
     * The {@link IBigdataFederation} iff running in scale-out.
     * <p>
     * Note: The {@link IBigdataFederation} is required in scale-out in order to
     * perform shard locator scans when mapping binding sets across the next
     * join in a query plan.
     */
    private final IBigdataFederation<?> fed;

    /**
     * The service used to expose {@link ByteBuffer}s and managed index
     * resources for transfer to remote services in support of distributed query
     * evaluation.
     */
    private final ManagedResourceService resourceService;

    /**
     * Constructor used on a {@link DataService} (a query engine peer).
     * 
     * @param dataService
     *            The data service.
     */
    public FederatedQueryEngine(final DataService dataService) {

        this(dataService.getFederation(),
                new DelegateIndexManager(dataService), dataService
                        .getResourceManager().getResourceService());
        
    }

    /**
     * Constructor used on a non-{@link DataService} node to expose a query
     * controller. Since the query controller is not embedded within a data
     * service it needs to provide its own {@link ResourceService} and local
     * {@link IIndexManager}.
     * 
     * @param fed
     * @param indexManager
     * @param resourceService
     */
    public FederatedQueryEngine(//
            final IBigdataFederation<?> fed,//
            final IIndexManager indexManager,//
            final ManagedResourceService resourceService//
            ) {

        super(indexManager);

        if (fed == null)
            throw new IllegalArgumentException();

        if (resourceService == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.resourceService = resourceService;

    }

    @Override
    public UUID getServiceUUID() {

        return fed.getServiceUUID();

    }

    @Override
    public IBigdataFederation<?> getFederation() {

        return fed;

    }

    /**
     * The service used to expose {@link ByteBuffer}s and managed index
     * resources for transfer to remote services in support of distributed query
     * evaluation.
     */
    public ManagedResourceService getResourceService() {
    
        return resourceService;
        
    }

    /**
     * Overridden to strengthen the return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    protected FederatedRunningQuery getRunningQuery(final long queryId) {

        return (FederatedRunningQuery) super.getRunningQuery(queryId);

    }
    
    public void declareQuery(final IQueryDecl queryDecl) {

        final long queryId = queryDecl.getQueryId();
        
        putRunningQuery(queryId, newRunningQuery(this, queryId,
                false/* controller */, queryDecl.getQueryController(),
                queryDecl.getQuery()));

    }
    
    @Override
    public void bufferReady(final IChunkMessage msg) {

        if (msg == null)
            throw new IllegalArgumentException();
        
        assertRunning();
        
        final long queryId = msg.getQueryId();
        
        final FederatedRunningQuery q = getRunningQuery(queryId);
        
        if(q == null)
            throw new RuntimeException(ERR_QUERY_NOT_RUNNING + queryId);
    
        if(msg.isMaterialized()) {

            q.acceptChunk(msg);
            
        } else {

            /*
             * FIXME SCALEOUT: We need to model the chunks available before they
             * are materialized locally such that (a) they can be materialized
             * on demand (flow control); and (b) we can run the operator when
             * there are sufficient chunks available without taking on too much
             * data. [For the sort term, they can be dropped onto a queue and
             * materialized in order of arrival.]
             */
            throw new UnsupportedOperationException("FIXME");
            
        }
        
    }

    /**
     * Factory for {@link RunningQuery}s.
     */
    @Override
    protected FederatedRunningQuery newRunningQuery(
            final QueryEngine queryEngine, final long queryId,
            final boolean controller, final IQueryClient clientProxy,
            final BindingSetPipelineOp query) {

        return new FederatedRunningQuery(this, queryId, true/* controller */,
                this/* clientProxy */, query, newQueryBuffer(query));

    }

    /**
     * {@inheritDoc}
     * 
     * @todo Historically, this has been a proxy object for an {@link IBuffer}
     *       on the {@link IQueryClient query controller}. However, it would be
     *       nice if we could reuse the same NIO transfer of {@link ByteBuffer}s
     *       to move the final results back to the client rather than using a
     *       proxy object for the query buffer.
     *       <p>
     *       In scale-out we must track chunks consumed by the client so we do
     *       not release the backing {@link ByteBuffer} on which the solutions
     *       are marshalled before the client is done draining the iterator. If
     *       the solutions are generated on the peers, then the peers must
     *       retain the data until the client has consumed them or have
     *       transferred the solutions to itself.
     *       <p>
     *       The places where this can show up as a problem are {@link SliceOp},
     *       when a query deadline is reached, and when a query terminates
     *       normally. Also pay attention when the client closes the
     *       {@link IAsynchronousIterator} from which it is draining solutions
     *       early.
     */
    @Override
    protected IBlockingBuffer<IBindingSet[]> newQueryBuffer(
            final BindingSetPipelineOp query) {

        return query.newBuffer();

    }

    /**
     * Resolve an {@link IQueryPeer}.
     * <p>
     * Note: This only resolves the peers running on the {@link IDataService}s.
     * It will not resolve a query controller unless an {@link IDataService} is
     * being used as the query controller.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * 
     * @return The proxy for the query peer.
     */
    protected IQueryPeer getQueryPeer(final UUID serviceUUID) {

        IQueryPeer proxy = proxyMap.get(serviceUUID);

        if (proxy == null) {

            final IDataService dataService = getFederation().getDataService(
                    serviceUUID);

            if (dataService == null)
                throw new RuntimeException("No such service: " + serviceUUID);

            try {
                proxy = dataService.getQueryEngine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            proxyMap.put(serviceUUID, proxy);

        }

        return proxy;

    }

    /**
     * Cache for {@link #getQueryPeer(UUID)}.
     */
    private final ConcurrentHashMap<UUID, IQueryPeer> proxyMap = new ConcurrentHashMap<UUID, IQueryPeer>();
    
}
