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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.engine.RunningQuery;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;

/**
 * An {@link IBigdataFederation} aware {@link QueryEngine}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: FederatedQueryEngine.java 3508 2010-09-05 17:02:34Z thompsonbry
 *          $
 * 
 * @todo Modify the {@link FederatedQueryEngine} to actually run a distributed
 *       query. Since we are in the same JVM, the {@link IBindingSet} chunks can
 *       be used directly without being marshalled onto {@link ByteBuffer}s and
 *       transferred over the network.
 *       <p>
 *       Distributed query will fail until each {@link FederatedQueryEngine} is
 *       receiving chunks and running operators against its local
 *       {@link IIndexManager}. This requires that we map the output chunks for
 *       an operator over the shards for the next operator, that we send the
 *       appropriate messages to the query engine peers, that they demand the
 *       necessary data from their peers, etc.
 *       <p>
 *       Once distributed query is running, begin to marshall the chunks onto
 *       buffers [this might have to be done immediately to get the notification
 *       protocol working].
 * 
 * @todo buffer management for s/o, including binding sets movement, element
 *       chunk movement for DHT on access path, and on demand materialization of
 *       large query resources for large data sets, parallel closure, etc.;
 *       grouping operators which will run locally (such as a pipeline join plus
 *       a conditional routing operator) so we do not marshall binding sets
 *       between operators when they will not cross a network boundary. Also,
 *       handle mutation, programs and closure operators.
 * 
 * @todo I have not yet figured out how to mark operators to indicate when their
 *       output should be mapped across shards or handled locally. It would
 *       appear that this is a concern of their parent in the operator tree. For
 *       example, the {@link ConditionalRoutingOp} would be applied to transform
 *       the output of a {@link PipelineJoin} before mapping the output over the
 *       shards.
 *       <p>
 *       The operator themselves could carry this information either as a Java
 *       method or as an annotation.
 *       <p>
 *       This could interact with how we combine {@link RunningQuery#chunksIn}.
 * 
 * @todo Override to release buffers associated with chunks buffered for a query
 *       when it terminates (buffers may be for received chunks or chunks which
 *       are awaiting transfer to another node). [This might be handled by a
 *       {@link RunningQuery} override.]
 * 
 * @todo Override protocol hooks for moving data around among the
 *       {@link QueryEngine}s
 * 
 * @todo Compressed representations of binding sets with the ability to read
 *       them in place or materialize them onto the java heap. The
 *       representation should be ammenable to processing in C since we want to
 *       use them on GPUs as well.
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
     * A service used to expose {@link ByteBuffer}s and managed index resources
     * for transfer to remote services in support of distributed query
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
    protected UUID getServiceId() {

        return fed.getServiceUUID();

    }

    @Override
    public IBigdataFederation<?> getFederation() {

        return fed;

    }

    @Override
    public void bufferReady(IQueryClient clientProxy,
            InetSocketAddress serviceAddr, long queryId, int bopId) {
        
        // @todo notify peer when a buffer is ready.
        
    }

    /**
     * Factory for {@link RunningQuery}s.
     */
    @Override
    protected FederatedRunningQuery newRunningQuery(
            final QueryEngine queryEngine, final long queryId,
            final long readTimestamp, final long writeTimestamp,
            final long begin, final long timeout, final boolean controller,
            final IQueryClient clientProxy, final BindingSetPipelineOp query) {

        return new FederatedRunningQuery(this, queryId, readTimestamp,
                writeTimestamp, System.currentTimeMillis()/* begin */, timeout,
                true/* controller */, this/* clientProxy */, query,
                newQueryBuffer(query));

    }

}
