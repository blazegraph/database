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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSet;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.sparse.SparseRowStore;

/**
 * The client-facing interface to a bigdata federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBigdataFederation extends IIndexManager {

    public Logger log = Logger.getLogger(IBigdataFederation.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO.toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Return the client object that was used to connect to the federation.
     * 
     * @throws IllegalStateException
     *             if the client disconnected and this object is no longer
     *             valid.
     */
    public IBigdataClient getClient();
    
    /**
     * Return the {@link ITimestampService} (or a proxy for that service).
     * 
     * @return The service -or- <code>null</code> if the service has not been discovered.
     */
    public ITimestampService getTimestampService();
    
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
    public ExecutorService getThreadPool();

    /**
     * The {@link CounterSet} which the client will use report its statistics to
     * the {@link ILoadBalancerService}.
     * <p>
     * Note: Applications MAY add their own counters (within a suitable
     * namespace) to the returned {@link CounterSet} in order to report their
     * own performance data to the {@link ILoadBalancerService}.
     * 
     * @see #getClientCounterPathPrefix()
     */
    public CounterSet getCounterSet();
    
    /**
     * The path prefix under which all of the client's counters are located.
     * This includes the fully qualified hostname, the word "client', and the
     * {@link UUID} for the client. The returned path prefix is terminated by
     * an {@link ICounterSet#pathSeparator}.
     */
    public String getClientCounterPathPrefix();
    
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
     *            a data service will be selected automatically.
     * 
     * @return The UUID of the registered index.
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
     *            by a separator key will be mapped. The #of entries in this
     *            array MUST agree with the #of entries in the <i>separatorKeys</i>
     *            array.
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
     *            Either the startTime of an active transaction,
     *            {@link ITx#UNISOLATED} for the current unisolated index view,
     *            {@link ITx#READ_COMMITTED} for a read-committed view, or
     *            <code>-timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * 
     * @return The index or <code>null</code> if the index does not exist.
     */
    public IIndex getIndex(String name, long timestamp);
    
    /**
     * Return a thread-local {@link IKeyBuilder} configured using the properties
     * specified for the {@link IBigdataClient}.
     */
    public IKeyBuilder getKeyBuilder();
    
    /**
     * Return <code>true</code> iff the federation supports scale-out indices.
     * <p>
     * Note: A <code>false</code> return indicates that
     * {@link #getMetadataService()} WILL NOT return a {@link IMetadataService}
     * since key-range partitioned indices are NOT supported. Applications can
     * use this method to decide whether or not to enable delete markers when
     * registering an index since delete markers are only required for scale-out
     * indices.
     * 
     * @see IndexMetadata
     */
    public boolean isScaleOut();
    
    /**
     * Return the global {@link SparseRowStore} used to store named property
     * sets in the federation.
     * 
     * @see GlobalRowStoreSchema
     * @see #getNamedRecord(String)
     * @see #putNamedRecord(String, Object)
     */
    public SparseRowStore getGlobalRowStore();
    
    /**
     * Return a named object stored in the global namespace for the federation.
     * 
     * @param name
     *            The name under which the object is stored.
     * 
     * @return The named record together -or- <code>null</code> iff there was
     *         no record recorded under that name.
     * 
     * @throws IllegalArgumentException
     *             if <i>name</i> is <code>null</code>.
     */
    public Object getNamedRecord(String name);
    
    /**
     * Store or update a named object in the global namespace for the
     * federation.
     * 
     * @param name
     *            The name under which the object is to be stored.
     * @param value
     *            The value to be stored under that name -or- <code>null</code>
     *            to clear any value associated with that name.
     * 
     * @return The old value -or- <code>null</code> if there was no value
     *         stored under that name.
     * 
     * @throws IllegalArgumentException
     *             if <i>name</i> is <code>null</code>.
     * 
     * @todo Consider replacing with <code>getGlobalRowStore()</code> since we
     *       can use that directly to store property sets, serialized objects,
     *       etc.
     */
    public Object putNamedRecord(String name, Object value);

    /**
     * Destroys all discovered services belonging to the federation and their
     * persistent data.
     * 
     * @todo create()?
     */
    public void destroy();
    
}
