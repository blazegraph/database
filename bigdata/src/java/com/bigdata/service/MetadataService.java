/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BytesUtil;
import com.bigdata.scaleup.AbstractPartitionTask;
import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.IResourceMetadata;
import com.bigdata.scaleup.JournalMetadata;
import com.bigdata.scaleup.MasterJournal;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.scaleup.PartitionMetadata;

/**
 * Implementation of a metadata service for a named scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Support creation and management of scale-out indices, including mapping
 *       their index partitions to data services. Build out this functionality
 *       with a series of test cases that invoke the basic operations
 *       (registerIndex (done), getPartition (done), putPartition,
 *       getPartitions, movePartition, etc.) and handle the load-balancing
 *       later.
 * 
 * @todo support transactionally isolated views onto the metadata index by
 *       passing in the tx identifier and using the appropriate historical view
 *       of the metadata index.
 * 
 * @todo Provide a means to reconstruct the metadata index from the journal and
 *       index segment data files. We tag each journal and index segment with a
 *       UUID. Each index is also tagged with a UUID, and that UUID is written
 *       into the metadata record for the index on each journal and index
 *       segment. Based on those UUIDs we are able to work backwards from the
 *       data on disk and identify the indices to which they belong. That
 *       information in combination with the timestamps in the metadata records
 *       and the first/last keys in the index partition is sufficient to
 *       regenerate the metadata indices.
 * 
 * @todo A temporal/immortable database can be realized if we never delete old
 *       journals since they contain the historical committed states of the
 *       database. The use of index segments would still provide fast read
 *       performance on recent data, while a suitable twist on the metadata
 *       index would allow access to those historical states. (E.g., you have to
 *       be able to access the historical state of the metadata index that
 *       corresponds to the commit time of interest for the database.)
 * 
 * @todo support two-tier metadata index and reconcile with
 *       {@link MetadataIndex} and {@link MasterJournal}.
 * 
 * @todo check all arguments (and on {@link IDataService}} as well. Arguments
 *       should also be tested on the client side of the interface.
 * 
 * @todo when a client accesses an index, if the #of partitions is small then
 *       just send them all back.
 * 
 * @todo the client (or the data services?) should send as async message every N
 *       seconds providing a histogram of the partitions they have touched (this
 *       could be input to the load balanced as well as info about the partition
 *       use that would inform MDS decision making).
 * 
 * @todo reconcile with the {@link AbstractPartitionTask} family. Those tasks
 *       run on the data service and manage the data on the journal and in the
 *       index segments while the corresponding tasks here update the partition
 *       metadata definitions. The general paradigm is that the data service
 *       operations need to be "safe" (idempotent) so that we can reexecute them
 *       in the worst case and the metadata service tasks need to codify changes
 *       in the data services. For example, an index split task initiated by the
 *       metadata service would be put onto a restart-safe schedule of tasks
 *       that the metadata service will run. The metadata service will run that
 *       task, at which point it will direct the data service to perform the
 *       partition split. If the metadata service fails before the data service
 *       reports success, then the task can simply be re-executed (in fact,
 *       tasks such as this do not need to be restart safe since there is no
 *       harm in the data service originating the split operation and there is
 *       no harm if the metadata service restarts and then figures out again
 *       that it wants to split that index partition -- as long as the data
 *       service can discard duplicate tasks). Eventually the data service will
 *       perform the split (even if it fails, the failover service will be
 *       directed to perform the split and the split will eventually occur).
 *       Once it performs the split, it needs to notify the metadata service
 *       which can then update the metadata for the partition (the old partition
 *       and its new right sibling). If the metadata service fails to notice the
 *       data service message indicating that it has performed the split, then
 *       it is still ok and the split can be re-requested later. The data
 *       service is responsible for making sure that its downstream failover
 *       services have the outputs from the split (new index segments) before
 *       notifying the metadata service that the split is complete.
 */
abstract public class MetadataService extends DataService implements
        IMetadataService, IServiceShutdown {

    /**
     * Return the name of the metadata index.
     * 
     * @param indexName
     *            The name of the scale-out index.
     * 
     * @return The name of the corresponding {@link MetadataIndex} that is used
     *         to manage the partitions in the named scale-out index.
     */
    public static String getMetadataName(String indexName) {
        
        return "metadata-"+indexName;
        
    }
    
    protected MetadataService(Properties properties) {

        super(properties);

    }

    /**
     * Register and statically partition a scale-out index.
     * 
     * @param name
     * @param separatorKeys
     * @param dataServices
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     * 
     * @todo Add a method to let a client cache the partitions of a scale-out
     *       index - its use will be limited to a statically partitioned index
     *       at this time and the #of expected partitions will be small enough
     *       that it makes sense for clients to pre-fetch and cache the entire
     *       set of partition definitions for a scale-out index.
     */
    public UUID registerManagedIndex(String name, byte[][] separatorKeys,
            UUID[] dataServices) throws IOException, InterruptedException,
            ExecutionException {
        
        final UUID managedIndexUUID = (UUID) journal.serialize(
                new RegisterMetadataIndexWithPartitionsTask(name,
                        separatorKeys, dataServices))
                .get();

        return managedIndexUUID; 
        
    }
    
    /**
     * @todo if if exits already? (and has consistent/inconsistent metadata)?
     * 
     * @todo index metadata options (unicode support, per-partition counters,
     *       etc.) i had been passing in the BTree instance, but that does not
     *       work as well in a distributed environment.
     */
    public UUID registerManagedIndex(String name, UUID dataServiceUUID)
            throws IOException, InterruptedException, ExecutionException {
        
        if(dataServiceUUID==null) {
            
            dataServiceUUID = getUnderUtilizedDataService();
            
        }
        
        // @todo setup the downstream replication chain.
        UUID[] dataServiceUUIDs = new UUID[] {
          
                dataServiceUUID
                
        };

        final UUID managedIndexUUID = (UUID) journal.serialize(
                new RegisterMetadataIndexTask(name, dataServiceUUIDs)).get();
        
        return managedIndexUUID; 
                
    }


    /**
     * Creates or updates a partition.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @param key
     *            The separator key for the partition -- this must be an exact
     *            match to update an existing partition.
     * 
     * @param val
     *            The metadata for the new or updated index partition.
     * 
     * @exception IllegalArgumentException
     *                if <i>key</i> is an exact match for the separator key of
     *                an existing partition and the partition identifiers do not
     *                agree.
     * 
     * @todo we could pass in a version identifier for the existing partition
     *       and use that to verify that there has not been an intervening
     *       update.
     * 
     * @todo this creates a new {@link PartitionMetadata} instance from the
     *       caller's data. The metadata index itself could be modified to allow
     *       other implementations than {@link PartitionMetadata} into the
     *       index, but I have not done so since I want to ensure datatype
     *       consistency for now.
     */
    public IPartitionMetadata createPartition(String name, byte[] separatorKey,
            UUID dataServiceUUID) throws IOException, InterruptedException,
            ExecutionException {

        if (dataServiceUUID == null) {

            dataServiceUUID = getUnderUtilizedDataService();

        }

        // @todo setup the failover data services.
        UUID[] dataServiceUUIDs = new UUID[] { dataServiceUUID };

        IPartitionMetadata pmd = (IPartitionMetadata) journal.serialize(
                new CreateIndexPartitionTask(name, separatorKey,
                        dataServiceUUIDs)).get();

        return pmd;

    }

    /**
     * @todo this could be broken down into a method to reflect a
     *       split/join/overflow operation (changing the resources) and a method
     *       to reflect a data service failover (changing the data services).
     */
    public void updatePartition(String name, byte[] key, IPartitionMetadata val)
            throws IOException, InterruptedException, ExecutionException {
        
        // the name of the metadata index itself.
        final String metadataName = getMetadataName(name);
        
        // make sure there is no metadata index for that btree.
        MetadataIndex mdi = (MetadataIndex) journal.getIndex(metadataName);
        
        if(mdi == null) {
            
            throw new IllegalArgumentException("Index not registered: " + name);
            
        }

        // FIXME run task.
//        mdi.put(key, new PartitionMetadata(val.getPartitionId(), val
//                .getDataServices(), val.getResources()));
        
        throw new UnsupportedOperationException();
        
    }
    
    public UUID getManagedIndexUUID(String name) throws IOException {
        
        // the name of the metadata index itself.
        final String metadataName = getMetadataName(name);
        
        // make sure there is no metadata index for that btree.
        MetadataIndex mdi = (MetadataIndex) journal.getIndex(metadataName);
        
        if(mdi == null) {
            
            return null;
            
        }
        
        return mdi.getManagedIndexUUID();
        
    }

    /**
     * This is equivilent to {@link MetadataIndex#findIndexOf(byte[])}.
     */
    public int findIndexOfPartition(String name,byte[] key) throws IOException {
        
        // the name of the metadata index itself.
        final String metadataName = getMetadataName(name);
        
        // make sure there is no metadata index for that btree.
        MetadataIndex mdi = (MetadataIndex) journal.getIndex(metadataName);
        
        if(mdi == null) {
            
            throw new IllegalArgumentException("Index not registered: " + name);
            
        }

        final int index = mdi.findIndexOf(key);

        return index;
        
    }

    /**
     * Note: This is equivilent to {@link MetadataIndex#find(byte[])} except
     * that it does not deserialize the {@link IPartitionMetadata} and it also
     * returns the left and right separator keys for the index partition.
     * 
     * @todo this may need to be rewritten to handle deleted index partition
     *       entries in the metadata index.
     */
    public byte[][] getPartition(String name,byte[] key) throws IOException {
        
        // the name of the metadata index itself.
        final String metadataName = getMetadataName(name);
        
        // make sure there is no metadata index for that btree.
        MetadataIndex mdi = (MetadataIndex) journal.getIndex(metadataName);
        
        if(mdi == null) {
            
            throw new IllegalArgumentException("Index not registered: " + name);
            
        }

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
        
        return new byte[][] { leftSeparatorKey, val, rightSeparatorKey };
        
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
    public byte[][] getPartitionAtIndex(String name, int index ) throws IOException {
        
        // the name of the metadata index itself.
        final String metadataName = getMetadataName(name);
        
        // make sure there is no metadata index for that btree.
        MetadataIndex mdi = (MetadataIndex) journal.getIndex(metadataName);
        
        if(mdi == null) {
            
            throw new IllegalArgumentException("Index not registered: " + name);
            
        }

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
        
        return new byte[][] { leftSeparatorKey, val, rightSeparatorKey };
        
    }
    
    /*
     * Tasks.
     */
    
    /**
     * Registers a metadata index for a named scale-out index and statically
     * partition the index using the given separator keys and data services.
     * 
     * @todo This task presumes a static system (no on-going writes on the
     *       various data services since it does not attempt to handle overflow
     *       events), runs remote operations inside the unisolated write thread,
     *       and does not provide correcting across if remote operations
     *       failure. This task was written in order to make it possible to
     *       statically partition a scale-out index before the broader problems
     *       of dynamic partitioning are solved.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class RegisterMetadataIndexWithPartitionsTask extends AbstractIndexManagementTask {
        
        final int npartitions;
        final private byte[][] separatorKeys;
        final private UUID[] dataServiceUUIDs;
        final private IDataService[] dataServices;
        
        /**
         * Create and statically partition a scale-out index.
         * 
         * @param name
         *            The name of the scale-out index.
         * @param separatorKeys
         *            The array of separator keys. Each separator key is
         *            interpreted as an <em>unsigned byte[]</em>. The first
         *            entry MUST be an empty byte[]. The entries MUST be in
         *            sorted order.
         * @param dataServiceUUIDs
         *            The array of data services onto which each partition
         *            defined by a separator key will be mapped. The #of entries
         *            in this array MUST agree with the #of entries in the
         *            <i>separatorKeys</i> array.
         */
        public RegisterMetadataIndexWithPartitionsTask(String name,
                byte[][] separatorKeys, UUID[] dataServiceUUIDs) {

            super(name);
            
            if(separatorKeys==null) 
                throw new IllegalArgumentException();

            if (separatorKeys.length == 0)
                throw new IllegalArgumentException();

            if (dataServiceUUIDs == null)
                throw new IllegalArgumentException();
            
            if (dataServiceUUIDs.length == 0)
                throw new IllegalArgumentException();
            
            if( separatorKeys.length != dataServiceUUIDs.length )
                throw new IllegalArgumentException();

            this.npartitions = separatorKeys.length;
            
            this.separatorKeys = separatorKeys;
            
            this.dataServiceUUIDs = dataServiceUUIDs;

            this.dataServices = new IDataService[dataServiceUUIDs.length];

            if( separatorKeys[0] == null )
                throw new IllegalArgumentException();
                
            if (separatorKeys[0].length != 0)
                throw new IllegalArgumentException(
                        "The first separatorKey must be an empty byte[].");
            
            for (int i = 0; i < npartitions; i++) {

                byte[] separatorKey = separatorKeys[i];
                
                if (separatorKey == null) {

                    throw new IllegalArgumentException();

                }
                
                if (i > 0) {
                    
                    if(BytesUtil.compareBytes(separatorKey, separatorKeys[i-1])<0) {
                        
                        throw new IllegalArgumentException(
                                "Separator keys out of order at index=" + i);
                        
                    }
                    
                }

                UUID uuid = dataServiceUUIDs[i];

                if (uuid == null) {

                    throw new IllegalArgumentException();

                }

                try {

                    IDataService dataService = getDataServiceByUUID(uuid);

                    if(dataService==null) {
                        
                        throw new IllegalArgumentException(
                                "Unknown data service: uuid=" + uuid);
                        
                    }
                    
                    dataServices[i] = dataService;

                } catch (IOException ex) {
                    
                    throw new RuntimeException(
                            "Could not resolve data service: UUID=" + uuid, ex);

                }

            }
            
        }

        public Object call() throws Exception {

            /*
             * @todo refactor this into the base abstract class IFF the task
             * writes on the journal. it provides safe commit iff the task
             * succeeds and otherwise invokes abort() so that partial task
             * executions are properly discarded. when possible, the original
             * exception is rethrown so that we do not encapsulate the cause
             * unless it would violate our throws clause.
             */
            try {

                Object ret = doTask();
                
                journal.commit();
                
                return ret;
                
            } catch(Exception ex) {
                
                journal.abort();
                
                throw ex;
                
            } catch(Throwable t) {
                
                journal.abort();
                
                throw new RuntimeException(t);
                
            }
            
        }
        
        /**
         * Create and statically partition the scale-out index.
         * 
         * @return The UUID assigned to the managed index.
         */
        protected UUID doTask() throws Exception {
            
            // the name of the metadata index itself.
            final String metadataName = getMetadataName(name);
            
            // make sure there is no metadata index for that btree.
            if( journal.getIndex(metadataName) != null ) {
                
                throw new IllegalStateException("Already registered: name="
                        + name);
                
            }

            /*
             * Note: there are two UUIDs here - the UUID for the metadata index
             * describing the partitions of the named scale-out index and the
             * UUID of the named scale-out index. The metadata index UUID MUST
             * be used by all B+Tree objects having data for the metadata index
             * (its mutable btrees on journals and its index segments) while the
             * managed named index UUID MUST be used by all B+Tree objects
             * having data for the named index (its mutable btrees on journals
             * and its index segments).
             */
            
            final UUID metadataIndexUUID = UUID.randomUUID();
            
            final UUID managedIndexUUID = UUID.randomUUID();
            
            /*
             * Create the metadata index.
             */
            
            MetadataIndex mdi = new MetadataIndex(journal, metadataIndexUUID,
                    managedIndexUUID, name);

            /*
             * Register the metadata index with the metadata service.
             */
            journal.registerIndex(metadataName, mdi);

            /*
             * Register the scale-out index on each data service on which a
             * partition of that index will be mapped.
             */

            Set<UUID> registered = new HashSet<UUID>();
            
            for(int i=0; i<npartitions; i++) {

                /*
                 * Register unless we have already registered the index on this
                 * data service (multiple partitions may be mapped to the same
                 * data service).
                 */

                UUID dataServiceUUID = dataServiceUUIDs[i];
                
                if(!registered.contains(dataServiceUUID)) {

                    dataServices[i].registerIndex(name, managedIndexUUID);
                 
                    registered.add(dataServiceUUID);
                    
                }

            }
            
            /*
             * Map the partitions onto the data services.
             */
            
            PartitionMetadata[] partitions = new PartitionMetadata[npartitions];
            
            for(int i=0; i<npartitions; i++) {
                
//                IDataService dataService = dataServices[i];
                
                PartitionMetadata pmd = new PartitionMetadata(//
                        mdi.nextPartitionId(),//
                        new UUID[] { //
                            dataServiceUUIDs[i]
                        },
                        new IResourceMetadata[] { //
                            dataServices[i].getJournalMetadata() }
                        );
                
                log.info("name=" + name + ", partitionId="
                        + pmd.getPartitionId());

                /*
                 * Map the initial partition onto that data service. This
                 * requires us to compute the left and right separator keys. The
                 * right separator key is just the separator key for the next
                 * partition in order and null iff this is the last partition.
                 */

                dataServices[i].mapPartition(name,
                        new PartitionMetadataWithSeparatorKeys(
                                separatorKeys[i], pmd,
                                i + 1 < npartitions ? separatorKeys[i + 1]
                                        : null));

                partitions[i] = pmd;
                
            }

            /*
             * Record each partition in the metadata index.
             */

            for(int i=0; i<npartitions; i++) {

                mdi.put(separatorKeys[i], partitions[i]);
            
            }

            // Done - caller will commit.
            return mdi.getManagedIndexUUID();
            
        }
        
    }

    /**
     * Registers a metadata index for a named scale-out index and creates the
     * initial partition for the scale-out index on a {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class RegisterMetadataIndexTask extends AbstractIndexManagementTask {
        
        final private UUID[] dataServiceUUIDs;
        final private IDataService[] dataServices;
        
        public RegisterMetadataIndexTask(String name,UUID[] dataServiceUUIDs) {

            super(name);
            
            if (dataServiceUUIDs == null)
                throw new IllegalArgumentException();
            
            if (dataServiceUUIDs.length == 0)
                throw new IllegalArgumentException();
            
            this.dataServiceUUIDs = dataServiceUUIDs;

            this.dataServices = new IDataService[dataServiceUUIDs.length];
            
            for (int i = 0; i < dataServiceUUIDs.length; i++) {

                UUID uuid = dataServiceUUIDs[i];

                if (uuid == null) {

                    throw new IllegalArgumentException();

                }

                try {

                    IDataService dataService = getDataServiceByUUID(uuid);

                    if(dataService==null) {
                        
                        throw new IllegalArgumentException(
                                "Unknown data service: uuid=" + uuid);
                        
                    }
                    
                    dataServices[i] = dataService;

                } catch (IOException ex) {
                    
                    throw new RuntimeException(
                            "Could not resolve data service: UUID=" + uuid, ex);

                }

            }
            
        }

        /**
         * @return The UUID assigned to the managed index.
         */
        public Object call() throws Exception {

            // the name of the metadata index itself.
            final String metadataName = getMetadataName(name);
            
            // make sure there is no metadata index for that btree.
            if( journal.getIndex(metadataName) != null ) {
                
                throw new IllegalStateException("Already registered: name="
                        + name);
                
            }

            /*
             * Note: there are two UUIDs here - the UUID for the metadata index
             * describing the partitions of the named scale-out index and the
             * UUID of the named scale-out index. The metadata index UUID MUST
             * be used by all B+Tree objects having data for the metadata index
             * (its mutable btrees on journals and its index segments) while the
             * managed named index UUID MUST be used by all B+Tree objects
             * having data for the named index (its mutable btrees on journals
             * and its index segments).
             */
            
            final UUID metadataIndexUUID = UUID.randomUUID();
            
            final UUID managedIndexUUID = UUID.randomUUID();
            
            /*
             * @todo it must not be possible for the journal to overflow during
             * this operation or we could wind up with stale metadata since a
             * new journal would be in effect (actually, the trigger condition
             * is that the journal is deemed to be inaccessible by any active
             * transaction and the metadata service selects the journal for
             * restart-safe deletion).
             */ 
            IResourceMetadata[] resourceMetadata = new IResourceMetadata[] {

                    dataServices[0].getJournalMetadata()
                    
                    /*
                     * Note: We the same resource exists on each failover data
                     * service since the resource is a media-level replication
                     * of the primary data service.
                     * 
                     * @todo in order to provide a global filename space we may
                     * want to locate the resources in the file system
                     * underneath a directory whose name is the UUID of the
                     * primary data service on which that resource was created.
                     * this could be handled by the data service itself, since
                     * it is responsible for reporting the names of its
                     * resources.
                     */

            };
            
            /*
             * Create the metadata index.
             */
            
            MetadataIndex mdi = new MetadataIndex(journal, metadataIndexUUID,
                    managedIndexUUID, name);
            
            PartitionMetadata pmd = new PartitionMetadata(
                    mdi.nextPartitionId(), dataServiceUUIDs, resourceMetadata);
            
            /*
             * Register the initial index partition on the target data service
             * (remote operation).
             * 
             * FIXME This must be done using a restart-safe operation such that
             * the partition is either eventually created or the operation is
             * retracted and the partition is created on a different data
             * service. Note that this is a high-latency remote operation and
             * MUST NOT be run inside of the serialized write on the metadata
             * index itself. It is a good question exactly when this operation
             * should be run.... probably on a restart-safe schedule that is
             * part of the metadata service and which can also support
             * split/join and compaction operations.
             * 
             * @todo setup the index partition on the media replication services
             * (unwritable downstream data services on which the raw store
             * writes are replicated). The replication scheme requires that the
             * same downstream services are used for all partitions that are
             * mapped onto the same journal since what is replicated is the
             * state of the raw store itself (the byte sequence of the file or
             * buffer backing the journal).
             * 
             * This suggests that the replication information does not need to
             * be stored in the metadata index, but should probably be stored in
             * the data service itself. Otherwise updating this information will
             * require up to update the partition metadata for each index
             * partition mapped onto a failed data service.
             * 
             * We also need to setup an exclusive lock that is obtained by the
             * primary data service. Any of the 2ndary data services could take
             * over if the primary fails -- once they are able to gain the lock.
             * No matter which secondary takes over, the media replication chain
             * will have to be updated, but this is easiest if the first
             * downstream data service takes over since its downstream chain
             * will remain unchanged.
             * 
             * I'm not sure if we can use Jini to manage the lock since there
             * could be a confusion over which service advertisement was
             * authorative.
             */

            dataServices[0].registerIndex(name, managedIndexUUID);
            
            // map the initial partition onto that data service.
            dataServices[0].mapPartition(name,
                    new PartitionMetadataWithSeparatorKeys(new byte[] {}, pmd,
                            null));
            
            /*
             * Register the metadata index with the metadata service.
             */
            journal.registerIndex(metadataName, mdi);
            
            /*
             * Record the partition in the metadata index.
             */
            mdi.put(new byte[] {}, pmd );

            journal.commit();

            return mdi.getManagedIndexUUID();
            
        }
        
    }

    /**
     * Create a new partition for a scale-out index - the caller specifies the
     * separatorKey, the {@link DataService}, and the {@link JournalMetadata}
     * and the task adds a new {@link IPartitionMetadata} with a unique
     * partition identifier to the {@link MetadataIndex}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class CreateIndexPartitionTask extends AbstractIndexManagementTask {
        
        /**
         * The left separator key specified by the caller.
         */
        final private byte[] separatorKey;

        final private UUID[] dataServiceUUIDs;
        
        final private IDataService[] dataServices;
        
        public CreateIndexPartitionTask(String name, byte[] separatorKey,
                UUID[] dataServiceUUIDs) {

            super(name);

            if(separatorKey==null) 
                throw new IllegalArgumentException();

            this.separatorKey = separatorKey;
            
            /*
             * @todo refactor logic to validate the dataServiceUUIDs and to
             * convert them to ServiceIDs into an abstract base class.
             */
            if (dataServiceUUIDs == null)
                throw new IllegalArgumentException();
            
            if (dataServiceUUIDs.length == 0)
                throw new IllegalArgumentException();
            
            this.dataServiceUUIDs = dataServiceUUIDs;

            this.dataServices = new IDataService[dataServiceUUIDs.length];
            
            for (int i = 0; i < dataServiceUUIDs.length; i++) {

                UUID uuid = dataServiceUUIDs[i];

                if (uuid == null) {

                    throw new IllegalArgumentException();

                }

                try {

                    IDataService dataService = getDataServiceByUUID(uuid);

                    if(dataService==null) {
                        
                        throw new IllegalArgumentException(
                                "Unknown data service: uuid=" + uuid);
                        
                    }
                    
                    dataServices[i] = dataService;

                } catch (IOException ex) {
                    
                    throw new RuntimeException(
                            "Could not resolve data service: UUID=" + uuid, ex);

                }

            }
            
        }
        
        /**
         * Figure out the separator key for the rightSibling based on the
         * pre-existing partition that spans the new separator key.
         */
        protected byte[] getRightSeparatorKey(MetadataIndex mdi,
                byte[] separatorKey) {

            /*
             * The code from this point on is shared with getPartition() and
             * getPartitionAtIndex() on MetadataService.
             */

            final int index = mdi.findIndexOf(separatorKey);

            if (index == -1)
                return null;

//            /*
//             * The serialized index partition metadata record for the partition
//             * that spans the given key.
//             */
//            byte[] val = (byte[]) mdi.valueAt(index);

//            /*
//             * The separator key that defines the left edge of that index
//             * partition (always defined).
//             */
//            byte[] leftSeparatorKey = (byte[]) mdi.keyAt(index);

            /*
             * The separator key that defines the right edge of that index
             * partition or [null] iff the index partition does not have a right
             * sibling (a null has the semantics of no upper bound).
             */
            byte[] rightSeparatorKey;

            try {

                rightSeparatorKey = (byte[]) mdi.keyAt(index + 1);

            } catch (IndexOutOfBoundsException ex) {

                rightSeparatorKey = null;

            }

            return rightSeparatorKey;
            
        }

        /**
         * @return The new {@link IPartitionMetadata} that was added to the
         *         {@link MetadataIndex}.
         */
        public Object call() throws Exception {

            // the name of the metadata index itself.
            final String metadataName = getMetadataName(name);
            
            // make sure there is no metadata index for that btree.
            
            final MetadataIndex mdi = (MetadataIndex) journal.getIndex(metadataName);
            
            if( mdi == null ) {
                
                throw new IllegalStateException("No such index: " + name);
                
            }

            /*
             * Using the definition of the index partitions _before_ we create
             * the new partition, locate the right separator key of the existing
             * partition that spans the given key. This will become the right
             * separator key of the newly created partition and we will need it
             * to notify the data service to map that partition.
             */
            final byte[] rightSeparatorKey = getRightSeparatorKey(mdi,
                    separatorKey);
            
            /*
             * @todo get the metadata for the journal resource on which the
             * index was registered.
             * 
             * @todo it must not be possible for the journal to overflow during
             * this operation or we could wind up with stale metadata since a
             * new journal would be in effect.
             */
            IResourceMetadata[] resourceMetadata = new IResourceMetadata[] {

                    dataServices[0].getJournalMetadata()
                    
            };
            
            PartitionMetadata pmd = new PartitionMetadata(
                    mdi.nextPartitionId(), dataServiceUUIDs, resourceMetadata);

            /*
             * Make sure that the target data service has a mutable index
             * registered for the named index (remote operation).
             * 
             * @todo this should be atomic. If the event originates with the
             * data services then we can accept this as a precondition.
             * 
             * @todo verify that the index exists either by trapping the
             * appropriate exception, by testing first, or by using a
             * conditional registration.
             */
            dataServices[0].registerIndex(name, mdi.getManagedIndexUUID());

            /*
             * map the partition onto that data service.
             * 
             * FIXME this needs to update the target data service mapping for
             * both the new partition and the partition whose key range was
             * split by this operation.
             * 
             * @todo Rather than define this as "creating" a partition, it
             * should be defined as "split"ing a partition. A separate operation
             * should be defined to move a partition to a different data service -
             * that "MOVE" operation may need to be queued until the journal
             * overflows or otherwise account for the data already on the data
             * service for the pre-split partition.
             * 
             * FIXME define a simple API to create a static partitioning of an
             * index onto N data services as part of the scale-out index
             * registration.  That will let me decouple the logic of working
             * through index partition split/joins and index partition moves
             * while supporting full operations on a statically partitioned
             * index.
             */
            dataServices[0].mapPartition(name,
                    new PartitionMetadataWithSeparatorKeys(separatorKey, pmd,
                            rightSeparatorKey));

            /*
             * Insert the new partition into the metadata index.
             */
            mdi.put(separatorKey, pmd);

            journal.commit();

            return pmd;
            
        }
        
    }

}
