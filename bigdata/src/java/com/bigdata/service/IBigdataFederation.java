/*

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
package com.bigdata.service;

import java.util.UUID;
import java.util.concurrent.ExecutorService;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.sparse.GlobalRowStoreSchema;
import com.bigdata.sparse.SparseRowStore;

/**
 * The client-facing interface to a bigdata federation. Note that each bigdata
 * service has an {@link IBigdataFederation} which it uses to communicate with
 * the other services in the federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBigdataFederation extends IIndexManager, IFederationDelegate {

    /**
     * Return the client object that was used to connect to the federation.
     * 
     * @throws IllegalStateException
     *             if the client disconnected and this object is no longer
     *             valid.
     */
    public IBigdataClient getClient();
    
    /**
     * The URL that may be used to access the local httpd service for this
     * client or service.
     * 
     * @return The URL -or- <code>null</code> if the httpd service is not
     *         running.
     */
    public String getHttpdURL();
    
    /**
     * Return the {@link ITransactionService} (or a proxy for that service).
     * 
     * @return The service -or- <code>null</code> if the service has not been
     *         discovered.
     */
    public ITransactionService getTransactionService();
    
    /**
     * Return the load balancer service (or a proxy for that service).
     * 
     * @return The service -or- <code>null</code> if the service has not been discovered.
     */
    public ILoadBalancerService getLoadBalancerService();
    
    /**
     * Return the metadata service (or a proxy for the metadata service).
     * 
     * @return The service -or- <code>null</code> if the service has not been discovered.
     */
    public IMetadataService getMetadataService();
    
    /**
     * A thread pool that may be used by clients to parallelize operations
     * against the federation. This thread pool is automatically used by the
     * {@link ClientIndexView}.
     */
    public ExecutorService getExecutorService();

    /**
     * The {@link CounterSet} which the client will use report its statistics to
     * the {@link ILoadBalancerService}.
     * <p>
     * Note: Applications MAY add their own counters (within a suitable
     * namespace) to the returned {@link CounterSet} in order to report their
     * own performance data to the {@link ILoadBalancerService}.
     * 
     * @see #getServiceCounterSet()
     * @see #getServiceCounterPathPrefix()
     */
    public CounterSet getCounterSet();
    
    /**
     * The node in {@link #getCounterSet()} corresponding to the root of the
     * client or service connected using this federation.
     */
    public CounterSet getServiceCounterSet();
    
    /**
     * The path prefix under which all of the client or service's counters are
     * located. The returned path prefix is terminated by an
     * {@link ICounterSet#pathSeparator}.
     * 
     * @see #getServiceCounterSet()
     */
    public String getServiceCounterPathPrefix();
    
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
     * Return an array of {@link IDataService} references that is correlated
     * with the given array of {@link IDataService} {@link UUID}s.
     * <p>
     * Note: This method will also resolve the {@link UUID} of an
     * {@link IMetadataService}.
     * 
     * @param uuids
     *            The (meta)data service UUIDs.
     * 
     * @return The (meta)data service proxies.
     */
    public IDataService[] getDataServices(UUID[] uuid);
    
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
     * Return ANY {@link IDataService} which has been (or could be) discovered
     * and which is part of the connected federation.
     * <p>
     * Note: This method is here as a failsafe when the
     * {@link ILoadBalancerService} is not available.
     * 
     * @return <code>null</code> if there are NO known {@link IDataService}s.
     */
    public IDataService getAnyDataService();
    
    /**
     * Return an {@link IDataService} joined with this
     * {@link IBigdataFederation} and having the specified service name.
     * Services that are not joined will not be discovered.
     * <p>
     * Note: At least some service fabrics (such as jini) do not enforce a
     * uniqueness constraint on the service name(s). In such cases an arbitrary
     * {@link IDataService} method the other requirements will be returned. It
     * is the responsibility of the administrator to ensure that each
     * {@link IDataService} is assigned a distinct service name.
     * 
     * @param name
     *            The service name.
     * 
     * @return A service assigned that name -or- <code>null</code> if none is
     *         joined with the {@link IBigdataFederation} at this time.
     * 
     * @throws IllegalArgumentException
     *             if <i>name</i> is <code>null</code>.
     */
    public IDataService getDataServiceByName(String name);
     
    /**
     * Return a read-only view of the index partitions for the named scale-out
     * index.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The {@link IMetadataIndex} for the named scale-out index -or-
     *         <code>null</code> iff there is no such scale-out index.
     */
    public IMetadataIndex getMetadataIndex(String name, long timestamp);
    
    /**
     * Register a scale-out index.
     * 
     * @param metadata
     *            The metadata template used to create component indices for
     *            {@link BTree}s this scale-out index (this also specifies the
     *            name of the scale-out index).
     * 
     * @todo compare the throws behavior of the federation with
     *       {@link AbstractJournal#registerIndex(IndexMetadata)}
     */
    public void registerIndex(IndexMetadata metadata);
    
    /**
     * Register a scale-out index and assign the initial index partition to the
     * specified data service.
     * 
     * @param metadata
     *            The metadata template used to create component indices for
     *            {@link BTree}s this scale-out index (this also specifies the
     *            name of the scale-out index).
     * @param dataServiceUUID
     *            The data service identifier (optional). When <code>null</code>,
     *            a data service will be selected automatically. If
     *            {@link IndexMetadata.Options#INITIAL_DATA_SERVICE} was
     *            specified, then the identified service will be used. Otherwise
     *            an underutilized service will be selected using the
     *            {@link ILoadBalancerService}.
     * 
     * @return The UUID of the registered index.
     * 
     * @see IndexMetadata.Options#INITIAL_DATA_SERVICE
     * 
     * @todo change to void return
     */
    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID);
    
    /**
     * Register and statically partition a scale-out index.
     * 
     * @param metadata
     *            The metadata template used to create component indices for
     *            {@link BTree}s this scale-out index (this also specifies the
     *            name of the scale-out index).
     * @param separatorKeys
     *            The array of separator keys. Each separator key is interpreted
     *            as an <em>unsigned byte[]</em>. The first entry MUST be an
     *            empty byte[]. The entries MUST be in sorted order.
     * @param dataServiceUUIDs
     *            The array of data services onto which each partition defined
     *            by a separator key will be mapped (optional). When given, the
     *            #of entries in this array MUST agree with the #of entries in
     *            the <i>separatorKeys</i> array and all entries must be non-<code>null</code>.
     *            When not given, the index partitions will be auto-assigned to
     *            the discovered data services.
     * 
     * @return The UUID of the scale-out index.
     * 
     * @todo change to void return
     */
    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs);

    /**
     * Drop a scale-out index.
     * 
     * @param name
     *            The index name.
     */
    public void dropIndex(String name);
    
    /**
     * Obtain a view on a partitioned index.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            A transaction identifier, {@link ITx#UNISOLATED} for the
     *            unisolated index view, {@link ITx#READ_COMMITTED}, or
     *            <code>timestamp</code> for a historical view no later than
     *            the specified timestamp.
     *            
     * @return The index or <code>null</code> if the index does not exist.
     */
    public IClientIndex getIndex(String name, long timestamp);
    
    /**
     * Return <code>true</code> iff the federation supports scale-out indices
     * (supports key-range partitioned indices). Note that a <code>true</code>
     * return does NOT imply that the federation is running in a distributed
     * environment, just that it uses the scale-out index architecture. A
     * <code>false</code> return indicates that {@link #getMetadataService()}
     * WILL NOT return a {@link IMetadataService} since key-range partitioned
     * indices are NOT supported.
     * 
     * @see IndexMetadata
     */
    public boolean isScaleOut();
    
    /**
     * Return <code>true</code> iff the federation is distributed (uses RMI and
     * is running, at least in principle, across more than one host/JVM).
     */
    public boolean isDistributed();

    /**
     * Return <code>true</code> iff the federation is backed by "stable" (vs
     * transient) storage. Most federation deployments are stable in this sense,
     * but it is possible to create federation instances backed solely by
     * transient storage and those instances will report <code>false</code>
     * here. This is most typically done for testing purposes using a
     * {@link LocalDataServiceFederation} or an {@link EmbeddedFederation}.
     */
    public boolean isStable();
    
    /**
     * Return the global {@link SparseRowStore} used to store named property
     * sets in the federation.
     * 
     * @see GlobalRowStoreSchema
     */
    public SparseRowStore getGlobalRowStore(/*long timestamp*/);
    
    /**
     * Destroys all discovered services belonging to the federation and their
     * persistent data and disconnects from the federation.
     */
    public void destroy();

    /**
     * Return the last commit time for the federation (the timestamp of the most
     * recent commit point across all {@link IDataService}s).
     * <p>
     * This is useful for {@link ITx#READ_COMMITTED} operations that need to use
     * a consistent timestamp across a series of {@link DataService}s or a
     * series of requests against a single {@link DataService} that must use a
     * consistent view.
     */
    public long getLastCommitTime();

}
