/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndexProcedure;
import com.bigdata.journal.CommitRecordIndex.Entry;

/**
 * Interface for clients of a {@link IBigdataFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBigdataClient {

    public static final Logger log = Logger.getLogger(IBigdataClient.class);

    /**
     * Connect to a bigdata federation. If the client is already connected, then
     * the existing connection is returned.
     * 
     * @return The federation.
     * 
     * @todo determine how a federation will be identified, e.g., by a name that
     *       is an {@link Entry} on the {@link MetadataServer} and
     *       {@link DataServer} service descriptions and provide that name
     *       attribute here. Note that a {@link MetadataService} can failover,
     *       so the {@link ServiceID} for the {@link MetadataService} is not the
     *       invariant, but rather the name attribute for the federation.
     */
    public IBigdataFederation connect();
    
    /**
     * Normal shutdown allows any existing client requests to federation
     * services to complete but does not schedule new requests, disconnects from
     * the federation, and then terminates any background processing that is
     * being performed on the behalf of the client (service discovery, etc).
     */
    public void shutdown();

    /**
     * Immediate shutdown terminates any client requests to federation services,
     * disconnects from the federation, and then terminate any background
     * processing that is being performed on the behalf of the client (service
     * discovery, etc).
     */
    public void shutdownNow();

    /**
     * Return an array UUIDs for {@link IDataService}s.
     * 
     * @param maxCount
     *            The maximum #of data services whose UUIDs will be returned.
     *            When zero (0) the UUID for all known data services will be
     *            returned.
     * 
     * @return An array of {@link UUID}s for data services.
     */
    public UUID[] getDataServiceUUIDs(int maxCount);
    
    /**
     * Resolve the service identifier to an {@link IDataService}.
     * <p>
     * Note: Whether the returned object is a proxy or the service
     * implementation depends on whether the federation is embedded (in process)
     * or distributed (networked).
     * 
     * @param serviceUUID
     *            The identifier for a {@link IDataService}.
     * 
     * @return The {@link IDataService} or <code>null</code> iff the
     *         {@link IDataService} could not be discovered from its identifier.
     */
    public IDataService getDataService(UUID serviceUUID);

    /**
     * Return the load balancer service (or a proxy for that service).
     */
    public ILoadBalancerService getLoadBalancerService();

    /**
     * Return the metadata service.
     * <p>
     * Note: Whether the returned object is a proxy or the service
     * implementation depends on whether the federation is embedded (in process)
     * or distributed (networked).
     * 
     * @return The metadata service.
     */
    public IMetadataService getMetadataService();
     
    /**
     * A thread pool that may be used by clients to parallelize operations
     * against the federation. This thread pool is automatically used by the
     * {@link ClientIndexView}.
     */
    public ExecutorService getThreadPool();

    /**
     * The default capacity when a client issues a range query request.
     * 
     * @see Options#CLIENT_RANGE_QUERY_CAPACITY
     */
    public int getDefaultRangeQueryCapacity();
    
    /**
     * When <code>true</code> requests for non-batch API operations will throw
     * exceptions.
     * 
     * @see Options#CLIENT_BATCH_API_ONLY
     */
    public boolean getBatchApiOnly();
    
    /**
     * Configuration options for {@link IBigdataClient}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options {
    
        /**
         * The #of threads in the client thread pool (default is <code>20</code>).
         * This thread pool is used to parallelize all requests issued by the
         * client and also to limit the maximum parallelism of the client with
         * respect to requests made of the federation.
         * 
         * @todo review this default.
         */
        public static final String CLIENT_THREAD_POOL_SIZE = "client.threadPoolSize";
        
        public static final String DEFAULT_CLIENT_THREAD_POOL_SIZE = "20";

        /**
         * The default capacity used when a client issues a range query request (50000).
         * 
         * @todo allow override on a per index basis as part of the index metadata?
         */
        public static final String CLIENT_RANGE_QUERY_CAPACITY = "client.rangeIteratorCapacity";

        public static final String DEFAULT_CLIENT_RANGE_QUERY_CAPACITY = "50000";

        /**
         * A boolean property which controls whether or not the non-batch API
         * will be disabled (default is <code>false</code>). This may be used
         * to disable the non-batch API, which is quite convenient for locating
         * code that needs to be re-written to use {@link IIndexProcedure}s in
         * order to obtain high performance.
         */
        public static final String CLIENT_BATCH_API_ONLY = "client.batchOnly";

        public static final String DEFAULT_CLIENT_BATCH_API_ONLY = "false";
        
    };
    
}
