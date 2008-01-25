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
package com.bigdata.service;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadata;
import com.bigdata.mdi.UnisolatedBTreePartitionConstructor;
import com.bigdata.rawstore.IRawStore;

/**
 * This class encapsulates access to the metadata and data services for a
 * bigdata federation - it is in effect a proxy object for the distributed set
 * of services that comprise the federation.
 * 
 * @todo in order to for a {@link IPartitionMetadata} cache to remain valid we
 *       need to either not store the left and right separator keys or we need
 *       to update the right separator key of an existing partition when a new
 *       partition is created by either this client or any other client. If the
 *       data service validates that the key(s) lie within its mapped
 *       partitions, then it can issue an appropriate redirect when the client
 *       has stale information. Failure to handle this issue will result in
 *       reads or writes against the wrong data services, which will result in
 *       lost data from the perspective of the clients.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataFederation implements IBigdataFederation {

    /**
     * The client - cleared to <code>null</code> when the client
     * {@link #disconnect()}s from the federation.
     */
    private BigdataClient client;

    /**
     * A temporary store used to cache various data in the client.
     */
    private final IRawStore clientTempStore = new TemporaryRawStore();

    /**
     * A per-index partition metadata cache.
     */
    private final Map<String, MetadataIndex> partitions = new ConcurrentHashMap<String, MetadataIndex>();
    
    /**
     * @exception IllegalStateException
     *                if the client has disconnected from the federation.
     */
    private void assertOpen() {

        if (client == null) {

            throw new IllegalStateException();

        }

    }

    public BigdataFederation(BigdataClient client) {

        if (client == null)
            throw new IllegalArgumentException();

        this.client = client;

    }

    public void disconnect() {
        
        if(client==null) {
            
            // Already disconnected.
            
            return;
            
        }
        
        client = null;

        if(clientTempStore.isOpen()) {

            clientTempStore.close();
            
        }

        partitions.clear();

    }

    public IBigdataClient getClient() {
        
        return client;
        
    }
    
    /**
     * Return a read-only view of the index partitions for the named scale-out
     * index.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The partitions for that index (keys are byte[] partition
     *         separator keys, values are serialized {@link PartitionMetadata}
     *         objects).
     * 
     * @throws NoSuchIndexException
     * 
     * @todo only statically partitioned indices are supported at this time.
     * 
     * @todo refactor to make use of this cache in the various operations of
     *       this client, reading through to the metadata service iff there is a
     *       cache miss.
     * 
     * @todo Rather than synchronizing all requests, this should queue requests
     *       for a specific metadata index iff there is a cache miss for that
     *       index.
     */
    public MetadataIndex getMetadataIndex(String name) {

        assertOpen();

        MetadataIndex tmp = partitions.get(name);

        if (tmp == null) {

            try {

                tmp = cacheMetadataIndex(name);

            } catch (IOException ex) {

                throw new RuntimeException(
                        "Could not cache partition metadata", ex);

            }

            partitions.put(name, tmp);

        }

        return tmp;

    }

    public IMetadataService getMetadataService() {

        assertOpen();

        return client.getMetadataService();

    }

    /**
     * Note: This does not return an {@link IIndex} since the client does
     * not provide a transaction identifier when registering an index (
     * index registration is always unisolated).
     * 
     * @see #registerIndex(String, UUID)
     */
    public UUID registerIndex(String name,UnisolatedBTreePartitionConstructor ctor) {

        assertOpen();

        return registerIndex(name, ctor, null);

    }

    /**
     * Registers a scale-out index and assigns the initial index partition
     * to the specified data service.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @param dataServiceUUID
     *            The data service identifier (optional). When
     *            <code>null</code>, a data service will be selected
     *            automatically.
     * 
     * @return The UUID of the registered index.
     * 
     * @deprecated This method and its task on the metadataservice can be
     *             replaced by
     *             {@link #registerIndex(String, byte[][], UUID[])}
     */
    public UUID registerIndex(String name,
            UnisolatedBTreePartitionConstructor ctor, UUID dataServiceUUID) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerManagedIndex(name,
                    ctor, dataServiceUUID);

            return indexUUID;

        } catch (Exception ex) {

            BigdataClient.log.error(ex);

            throw new RuntimeException(ex);

        }

    }

    public UUID registerIndex(String name, UnisolatedBTreePartitionConstructor ctor,
            byte[][] separatorKeys, UUID[] dataServiceUUIDs) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerManagedIndex(name, ctor,
                    separatorKeys, dataServiceUUIDs);

            return indexUUID;

        } catch (Exception ex) {

            BigdataClient.log.error(ex);

            throw new RuntimeException(ex);

        }

    }

    /**
     * Drops the named scale-out index (synchronous).
     * 
     * FIXME implement. No new unisolated operation or transaction should be
     * allowed to read or write on the index. Once there are no more users
     * of the index, the index must be dropped from each data service,
     * including both the mutable B+Tree absorbing writes for the index and
     * any read-only index segments. The metadata index must be dropped on
     * the metadata service (and from the client's cache).
     * 
     * @todo A "safe" version of this operation would schedule the restart
     *       safe deletion of the mutable btrees, index segments and the
     *       metadata index so that the operation could be "discarded"
     *       before the data were actually destroyed (assuming an admin tool
     *       that would allow you to recover a dropped index before its
     *       component files were deleted).
     */
    public void dropIndex(String name) {

        assertOpen();

        throw new UnsupportedOperationException();

    }

    /**
     * @todo support isolated views, share cached data service information
     *       between isolated and unisolated views.
     */
    public IIndex getIndex(long tx, String name) {

        assertOpen();

        try {

            if (getMetadataService().getManagedIndexUUID(name) == null) {

                return null;

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        return new ClientIndexView(this, tx, name);

    }

    /**
     * Cache the index partition metadata in the client.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The cached partition metadata.
     * 
     * @throws NoSuchIndexException
     * 
     * @todo write tests to validate this method. refactor the code code
     *       into a utility class for batch index copy.
     * 
     * @todo This implementation does not handle a partitioned metadata
     *       index.
     */
    private MetadataIndex cacheMetadataIndex(String name) throws IOException {

        assertOpen();

        // The name of the metadata index.
        final String metadataName = MetadataService.getMetadataName(name);

        // The metadata service - we will use a range query on it.
        final IMetadataService metadataService = getMetadataService();

        // The UUID for the metadata index for that scale-out index.
        final UUID metadataIndexUUID = metadataService
                .getIndexUUID(metadataName);

        if (metadataIndexUUID == null) {

            throw new NoSuchIndexException(name);

        }

        // The UUID for the managed scale-out index.
        final UUID managedIndexUUID = metadataService
                .getManagedIndexUUID(metadataName);

        /*
         * Allocate a cache for the defined index partitions.
         */
        MetadataIndex mdi = new MetadataIndex(clientTempStore,
                metadataIndexUUID, managedIndexUUID, name,
                /*
                 * Note: We do not need to use the same ctor here that was
                 * declared to the real metadata index since we are not going to
                 * be creating data indices using the clients view of the
                 * metadata index.
                 */
                new UnisolatedBTreePartitionConstructor()
        );

        /*
         * Bulk copy the partition definitions for the scale-out index into the
         * client. This uses range queries to bulk copy the keys and values from
         * the metadata index on the metadata service into the client's cache.
         */
        ResultSet rset;

        byte[] nextKey = null;

        /*
         * Note: metadata index is NOT partitioned.
         * 
         * @todo Does not support partitioned metadata index.
         */

        while (true) {

            try {

                rset = metadataService.rangeIterator(ITx.UNISOLATED,
                        metadataName, nextKey, null, 1000, IRangeQuery.KEYS
                                | IRangeQuery.VALS, null/* filter */);

                BigdataClient.log.info("Fetched " + rset.getNumTuples()
                        + " partition records for " + name);

            } catch (Exception ex) {

                throw new RuntimeException(
                        "Could not cache index partition metadata", ex);

            }

            int npartitions = rset.getNumTuples();

            byte[][] separatorKeys = rset.getKeys();

            byte[][] values = rset.getValues();

            mdi.insert(new BatchInsert(npartitions, separatorKeys, values));

            if (rset.isExhausted()) {

                // No more results are available.

                break;

            }

            // @todo write test to validate fence post for successor/lastKey.
            nextKey = rset.successor();
            //                nextKey = rset.getLastKey();

        }

        return mdi;

    }

}
