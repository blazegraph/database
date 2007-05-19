package com.bigdata.service;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import net.jini.core.lookup.ServiceID;

import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.IIndex;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.scaleup.PartitionMetadata;
import com.bigdata.service.DataService.NoSuchIndexException;

/**
     * This class encapsulates access to the metadata and data services for a
     * bigdata federation.
     * 
     * @todo in order to for a {@link IPartitionMetadata} cache to remain valid
     *       we need to either not store the left and right separator keys or we
     *       need to update the right separator key of an existing partition
     *       when a new partition is created by either this client or any other
     *       client. If the data service validates that the key(s) lie within
     *       its mapped partitions, then it can issue an appropriate redirect
     *       when the client has stale information. Failure to handle this issue
     *       will result in reads or writes against the wrong data services,
     *       which will result in lost data from the perspective of the clients.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class BigdataFederation implements IBigdataFederation {
       
        /**
         * A temporary store used to cache various data in the client.
         */
        private IRawStore clientTempStore = new TemporaryRawStore();
        
        private final BigdataClient client;
        
        /**
         * Return a read-only view of the index partitions for the named
         * scale-out index.
         * 
         * @param name
         *            The name of the scale-out index.
         * 
         * @return The partitions for that index (keys are byte[] partition
         *         separator keys, values are serialized
         *         {@link PartitionMetadata} objects).
         * 
         * @throws NoSuchIndexException
         * 
         * @todo only statically partitioned indices are supported at this time.
         * 
         * @todo refactor to make use of this cache in the various operations of
         *       this client.
         * 
         * @todo Rather than synchronizing all requests, this should queue
         *       requests for a specific metadata index iff there is a cache
         *       miss for that index.
         */
        public MetadataIndex getMetadataIndex(String name) {

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
        private Map<String, MetadataIndex> partitions = new ConcurrentHashMap<String, MetadataIndex>();
        
        public IMetadataService getMetadataService() {
            
            return client.getMetadataService();
            
        }
        
        public BigdataFederation(BigdataClient client) {
            
            if (client == null)
                throw new IllegalArgumentException();
            
            this.client = client;
            
        }

        /**
         * Note: This does not return an {@link IIndex} since the client does
         * not provide a transaction identifier when registering an index (
         * index registration is always unisolated).
         * 
         * @see #registerIndex(String, UUID)
         */
        public UUID registerIndex(String name) {

            return registerIndex(name,null);
            
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
        public UUID registerIndex(String name, UUID dataServiceUUID) {

            try {

                UUID indexUUID = getMetadataService().registerManagedIndex(
                        name, dataServiceUUID);
                
                return indexUUID;
                
            } catch(Exception ex) {
                
                BigdataClient.log.error(ex);
                
                throw new RuntimeException(ex);
                
            }

        }
        
        public UUID registerIndex(String name, byte[][] separatorKeys, UUID[] dataServiceUUIDs) {

            try {

                UUID indexUUID = getMetadataService().registerManagedIndex(
                        name, separatorKeys, dataServiceUUIDs);
                
                return indexUUID;
                
            } catch(Exception ex) {
                
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
        
            throw new UnsupportedOperationException();
            
        }
        
        /**
         * @todo support isolated views, share cached data service information
         *       between isolated and unisolated views.
         */
        public IIndex getIndex(long tx, String name) {

            try {
            
                if( getMetadataService().getManagedIndexUUID(name) == null) {
                    
                    return null;
                    
                }
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            return new ClientIndexView(this,tx,name); 
            
        }

        /**
         * @todo setup cache. test cache and lookup on metadata service if a
         *       cache miss. the cache should be based on a lease and the data
         *       service should know whether an index partition has been moved
         *       and notify the client that it needs to re-discover the data
         *       service for an index partition. the cache is basically a
         *       partial copy of the metadata index that is local to the client.
         *       The cache needs to have "fake" entries that are the
         *       left-sibling of each real partition entry so that it can
         *       correctly determine when there is a cache miss.
         *       <p>
         *       Note that index partition definitions will evolve slowly over
         *       time through splits and joins of index segments. again, the
         *       client should presume consistency of its information but the
         *       data service should know when it no longer has information for
         *       a key range in a partition (perhaps passing the partitionId and
         *       a timestamp for the last partition update to the data service
         *       with each request).
         *       <p>
         *       Provide a historical view of the index partition definitions
         *       when transactional isolation is in use by the client. This
         *       should make it possible for a client to not be perturbed by
         *       split/joins of index partitions when executing with
         *       transactional isolation.
         *       <p>
         *       Note that service failover is at least partly orthogonal to the
         *       partition metadata in as much as the index partition
         *       definitions themselves do not evolve (the same separator keys
         *       are in place and the same resources have the consistent data
         *       for a view of the index partition), but it is possible that the
         *       data services have changed. It is an open question how to
         *       maintain isolation with failover while supporting failover
         *       without aborting the transaction. The most obvious thing is to
         *       have a transationally isolated client persue the failover
         *       services already defined in the historical transaction without
         *       causing the partition metadata to be updated on the metadata
         *       service. (Unisolated clients would begin to see updated
         *       partition metadata more or immediately.)
         * 
         * @param tx
         * @param name
         * @param key
         * @return
         */
        public PartitionMetadataWithSeparatorKeys getPartition(long tx, String name, byte[] key) {

            MetadataIndex mdi = getMetadataIndex(name);
            
            IPartitionMetadata pmd;
            
//            final byte[][] data;
            
            try {
             
                final int index = mdi.findIndexOf(key);
                
                /*
                 * The code from this point on is shared with getPartitionAtIndex() and
                 * also by some of the index partition tasks (CreatePartition for one).
                 */
                
                if(index == -1) return null;
                
                /*
                 * The serialized index partition metadata record for the partition that
                 * spans the given key.
                 */
                byte[] val = (byte[]) mdi.valueAt(index);
                
                /*
                 * The separator key that defines the left edge of that index partition
                 * (always defined).
                 */
                byte[] leftSeparatorKey = (byte[]) mdi.keyAt(index);
                
                /*
                 * The separator key that defines the right edge of that index partition
                 * or [null] iff the index partition does not have a right sibling (a
                 * null has the semantics of no upper bound).
                 */
                byte[] rightSeparatorKey;
                
                try {

                    rightSeparatorKey = (byte[]) mdi.keyAt(index+1);
                    
                } catch(IndexOutOfBoundsException ex) {
                    
                    rightSeparatorKey = null;
                    
                }
                
//                return new byte[][] { leftSeparatorKey, val, rightSeparatorKey };
//
//                data = getMetadataService().getPartition(name, key);
//                
//                if (data == null)
//                    return null;
                
                pmd = (IPartitionMetadata) SerializerUtil.deserialize(val);

                return new PartitionMetadataWithSeparatorKeys(leftSeparatorKey,
                        pmd, rightSeparatorKey);

            } catch(Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

        }

        /**
         * @todo this is subject to concurrent modification of the metadata index
         *       would can cause the index to identify a different partition. client
         *       requests that use {@link #findIndexOfPartition(String, byte[])} and
         *       {@link #getPartitionAtIndex(String, int)} really need to refer to
         *       the same historical version of the metadata index (this effects
         *       range count and range iterator requests and to some extent batch
         *       operations that span multiple index partitions).
         */
        public PartitionMetadataWithSeparatorKeys getPartitionAtIndex(
                String name, int index) {
            
            MetadataIndex mdi = getMetadataIndex(name);

            /*
             * The code from this point on is shared with getPartition()
             */

            if(index == -1) return null;
            
            /*
             * The serialized index partition metadata record for the partition that
             * spans the given key.
             */
            byte[] val = (byte[]) mdi.valueAt(index);
            
            /*
             * The separator key that defines the left edge of that index partition
             * (always defined).
             */
            byte[] leftSeparatorKey = (byte[]) mdi.keyAt(index);
            
            /*
             * The separator key that defines the right edge of that index partition
             * or [null] iff the index partition does not have a right sibling (a
             * null has the semantics of no upper bound).
             */
            byte[] rightSeparatorKey;
            
            try {

                rightSeparatorKey = (byte[]) mdi.keyAt(index+1);
                
            } catch(IndexOutOfBoundsException ex) {
                
                rightSeparatorKey = null;
                
            }
            
            return new PartitionMetadataWithSeparatorKeys(leftSeparatorKey,
                    (PartitionMetadata) SerializerUtil.deserialize(val),
                    rightSeparatorKey);
            
        }

//        public PartitionMetadataWithSeparatorKeys getPartitionAtIndex(long tx, String name, int index) {
//
//            IPartitionMetadata pmd;
//
//            byte[][] data;
//
//            try {
//                
//                data = getMetadataService().getPartitionAtIndex(name, index);
//                
//                if (data == null)
//                    return null;
//                
//                pmd = (IPartitionMetadata) SerializerUtil.deserialize(data[1]);
//                
//            } catch(Exception ex) {
//                
//                throw new RuntimeException(ex);
//                
//            }
//
//            return new PartitionMetadataWithSeparatorKeys(data[0],pmd,data[2]);
//
//        }

//        private Map<String, Map<Integer, IDataService>> indexCache = new ConcurrentHashMap<String, Map<Integer, IDataService>>(); 
//
//        synchronized(indexCache) {
//      
//          Map<Integer,IDataService> partitionCache = indexCache.get(name);
//       
//          if(partitionCache==null) {
//              
//              partitionCache = new ConcurrentHashMap<Integer, IDataService>();
//              
//              indexCache.put(name, partitionCache);
//              
//          }
//          
//          IDataService dataService = 
//          
//      }

        /**
         * Resolve the data service to which the index partition was mapped.
         * <p>
         * This uses the lookup cache provided by
         * {@link BigdataClient#getDataService(ServiceID)}
         */
        public IDataService getDataService(IPartitionMetadata pmd) {

            ServiceID serviceID = JiniUtil
                    .uuid2ServiceID(pmd.getDataServices()[0]);

            final IDataService dataService;
            
            try {

                dataService = client.getDataService(serviceID);
                
            } catch(Exception ex) {
                
                throw new RuntimeException(ex);
                
            }

            return dataService;

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

            // The name of the metadata index.
            final String metadataName = MetadataService.getMetadataName(name);
            
            // The metadata service - we will use a range query on it.
            final IMetadataService metadataService = getMetadataService();

            // The UUID for the metadata index for that scale-out index.
            final UUID metadataIndexUUID = metadataService.getIndexUUID(metadataName);
            
            if(metadataIndexUUID==null) {
                
                throw new NoSuchIndexException(name);
                
            }
            
            // The UUID for the managed scale-out index.
            final UUID managedIndexUUID = metadataService.getManagedIndexUUID(metadataName);
            
            /*
             * Allocate a cache for the defined index partitions.
             */
            MetadataIndex mdi = new MetadataIndex(clientTempStore,
                    metadataIndexUUID, managedIndexUUID, name);
            
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
            final int partitionId = IDataService.UNPARTITIONED;

            while (true) {

                try {

                    rset = metadataService.rangeQuery(IDataService.UNISOLATED,
                        metadataName, partitionId, nextKey, null, 1000,
                        IDataService.KEYS | IDataService.VALS);

                    BigdataClient.log.info("Fetched " + rset.getNumTuples()
                            + " partition records for " + name);
                    
                } catch (Exception ex) {

                    throw new RuntimeException(
                            "Could not cache index partition metadata", ex);

                }

                int npartitions = rset.getNumTuples();

                byte[][] separatorKeys = rset.getKeys();

                byte[][] values = rset.getValues();

                mdi.insert(new BatchInsert(npartitions, separatorKeys,
                        values));

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