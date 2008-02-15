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

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadata;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
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
 *       lost data from the perspective of the clients. (in fact, I think that I
 *       will simply change the partition identifier when the key range of a
 *       partition is changed, which will take care of the problem).
 * 
 * @todo This implementation does not handle a partitioned metadata index.
 * 
 * @todo only statically partitioned indices are supported at this time.
 * 
 * @todo refactor to make use of this cache in the various operations of this
 *       client, reading through to the metadata service iff there is a cache
 *       miss.
 * 
 * @todo Rather than synchronizing all requests, this should queue requests for
 *       a specific metadata index iff there is a cache miss for that index.
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

    public UUID registerIndex(IndexMetadata metadata) {

        assertOpen();

        return registerIndex(metadata, null);

    }

    public UUID registerIndex(IndexMetadata metadata, UUID dataServiceUUID) {

        assertOpen();

        if (dataServiceUUID == null) {
            
            try {
            
                dataServiceUUID = getMetadataService()
                        .getUnderUtilizedDataService();

            } catch (Exception ex) {

                log.error(ex);

                throw new RuntimeException(ex);

            }

        }

        return registerIndex(//
                metadata, //
                new byte[][] { new byte[] {} },//
                new UUID[] { dataServiceUUID } //
            );

    }

    public UUID registerIndex(IndexMetadata metadata, byte[][] separatorKeys,
            UUID[] dataServiceUUIDs) {

        assertOpen();

        try {

            UUID indexUUID = getMetadataService().registerScaleOutIndex(
                    metadata, separatorKeys, dataServiceUUIDs);

            return indexUUID;

        } catch (Exception ex) {

            log.error(ex);

            throw new RuntimeException(ex);

        }

    }

    public void dropIndex(String name) {

        assertOpen();

        try {
            
            getMetadataService().dropScaleOutIndex(name);
            
        } catch (Exception e) {

            throw new RuntimeException( e );
            
        }

    }

    public IIndex getIndex(long tx, String name) {

        assertOpen();

        /*
         * Verify index exists.
         */
        
        final MetadataIndexMetadata mdmd;
        try {

            mdmd = (MetadataIndexMetadata) getMetadataService()
                    .getIndexMetadata(MetadataService.getMetadataIndexName(name));
            
            if (mdmd == null) {

                // No such index.
                
                return null;

            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }
        
        // Index exists.
        
        return new ClientIndexView(this, tx, name, mdmd );

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
     */
    private MetadataIndex cacheMetadataIndex(String name) throws IOException {

        assertOpen();

        // The name of the metadata index.
        final String metadataName = MetadataService.getMetadataIndexName(name);

        // The metadata service - we will use a range query on it.
        final IMetadataService metadataService = getMetadataService();

        // The metadata for the metadata index itself.
        final MetadataIndexMetadata mdmd = (MetadataIndexMetadata)metadataService.getIndexMetadata(metadataName);
        
        if (mdmd == null) {

            throw new NoSuchIndexException(name);

        }

        // The UUID of the metadata index.
        final UUID metadataIndexUUID = mdmd.getIndexUUID();

        /*
         * Allocate a cache for the defined index partitions.
         */
        MetadataIndex mdi = MetadataIndex.create(//
                clientTempStore,//
                metadataIndexUUID,//
                mdmd.getManagedIndexMetadata()// the managed index's metadata.
        );

        /*
         * Bulk copy the partition definitions for the scale-out index into the
         * client.
         * 
         * Note: This assumes that the metadata index is NOT partitioned.
         */
        {
        
            final IEntryIterator itr = new DataServiceRangeIterator(metadataService,
                    metadataName, ITx.UNISOLATED, null/* fromKey */,
                    null/* toKey */, 0/* capacity */, IRangeQuery.KEYS
                            | IRangeQuery.VALS, null/*filter*/);
        
            while(itr.hasNext()) {
             
                ITuple tuple = itr.next();
                
                byte[] key = tuple.getKey();
                
                byte[] val = tuple.getValue();
                
                mdi.insert(key, val);
                
            }
            
        }

        return mdi;

    }

}
