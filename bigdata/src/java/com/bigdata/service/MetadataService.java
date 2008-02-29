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
 * Created on Mar 17, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ChunkedLocalRangeIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexExistsException;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;

/**
 * Implementation of a metadata service for a named scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class MetadataService extends DataService implements
        IMetadataService {

    /**
     * Error message when a request is made to register a scale-out index but
     * delete markers are not enabled for that index.
     */
    protected static final String ERR_DELETE_MARKERS = "Delete markers not enabled";
    
    /**
     * Return the name of the metadata index.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The name of the corresponding {@link MetadataIndex} that is used
     *         to manage the partitions in the named scale-out index.
     */
    public static String getMetadataIndexName(String name) {
        
        return "metadata-"+name;
        
    }

    /**
     * Options for the {@link MetadataService}.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends DataService.Options {
        
    }

//    /**
//     * Overriden to use a {@link Journal} rather than a {@link ResourceManager}.
//     * <p>
//     * Note: There is a cyclic dependency of the {@link ResourceManager} on an
//     * {@link IMetadataService} (for partition updates) and the
//     * {@link MetadataService} on an {@link IResourceManager} (for managing its
//     * files). This dependency grounds out because the {@link MetadataService}
//     * establishes itself as the reference returned by
//     * {@link #getMetadataService()}. However, since the {@link MetadataIndex}
//     * does not support overflow this method has been overriden to use a
//     * {@link Journal} rather than a {@link ResourceManager} as the
//     * {@link IResourceManager} as the {@link IResourceManager} implementation
//     * object.
//     */
//    protected IResourceManager newResourceManager(Properties properties) {
//
//        return new Journal(properties);
//        
//    }
    
    /**
     * @param properties
     */
    protected MetadataService(Properties properties) {

        super(properties);

    }

    public int nextPartitionId(String name) throws IOException, InterruptedException, ExecutionException {
       
        setupLoggingContext();
        
        try {

            final AbstractTask task = new NextPartitionIdTask(
                    concurrencyManager, getMetadataIndexName(name));
            
            final Integer partitionId = (Integer) concurrencyManager.submit(
                    task).get();
        
            log.info("Assigned partitionId="+partitionId+", name="+name);
            
            return partitionId.intValue();
            
        } finally {
            
            clearLoggingContext();
            
        }        
        
    }
    
    public PartitionLocator get(String name, long timestamp, final byte[] key)
            throws InterruptedException, ExecutionException, IOException {
    
        setupLoggingContext();

        try {

            final AbstractTask task = new AbstractTask(
                    concurrencyManager, timestamp, getMetadataIndexName(name)
                    ) {

                        @Override
                        protected Object doTask() throws Exception {
                            
                            MetadataIndex ndx = (MetadataIndex)getIndex(getOnlyResource());
                            
                            return ndx.get(key);
                            
                        }
                
            };
            
            return (PartitionLocator) concurrencyManager.submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        

    }

    public PartitionLocator find(String name, long timestamp, final byte[] key)
            throws InterruptedException, ExecutionException, IOException {

        setupLoggingContext();

        try {

            final AbstractTask task = new AbstractTask(concurrencyManager,
                    timestamp, getMetadataIndexName(name)) {

                @Override
                protected Object doTask() throws Exception {

                    MetadataIndex ndx = (MetadataIndex) getIndex(getOnlyResource());

                    return ndx.find(key);

                }

            };

            return (PartitionLocator) concurrencyManager.submit(task).get();

        } finally {

            clearLoggingContext();

        }

    }

    public void splitIndexPartition(String name, PartitionLocator oldLocator,
            PartitionLocator newLocators[]) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            final AbstractTask task = new SplitIndexPartitionTask(
                    concurrencyManager, getMetadataIndexName(name),
                    oldLocator, newLocators);
            
            concurrencyManager.submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        
        
    }
    
    public void joinIndexPartition(String name, PartitionLocator[] oldLocators,
            PartitionLocator newLocator) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            final AbstractTask task = new JoinIndexPartitionTask(
                    concurrencyManager, getMetadataIndexName(name),
                    oldLocators, newLocator);
            
            concurrencyManager.submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        
        
    }
    
    /**
     * @todo if if exits already? (and has consistent/inconsistent metadata)?
     */
    public UUID registerScaleOutIndex(IndexMetadata metadata,
            byte[][] separatorKeys, UUID[] dataServices) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            if (metadata.getName() == null) {

                throw new IllegalArgumentException(
                        "No name assigned to index in metadata template.");
                
            }
            
            if (!metadata.getDeleteMarkers()) {

                throw new IllegalArgumentException(ERR_DELETE_MARKERS);

            }

            final String scaleOutIndexName = metadata.getName();
            
            // Note: We need this in order to assert a lock on this resource!
            final String metadataIndexName = MetadataService
                    .getMetadataIndexName(scaleOutIndexName);
     
            final AbstractTask task = new RegisterScaleOutIndexTask(
                    concurrencyManager, resourceManager, metadataIndexName,
                    metadata, separatorKeys, dataServices);
            
            final UUID managedIndexUUID = (UUID) concurrencyManager
                    .submit(task).get();

            return managedIndexUUID;
            
        } finally {

            clearLoggingContext();

        }

    }
    
    public void dropScaleOutIndex(String name) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            final AbstractTask task = new DropScaleOutIndexTask(
                    concurrencyManager, getMetadataIndexName(name));
            
            concurrencyManager.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
   
    /*
     * Tasks.
     */
    
    /**
     * Task assigns the next partition identifier for a registered scale-out
     * index in a restart-safe manner.
     */
    protected class NextPartitionIdTask extends AbstractTask {

        /**
         * @param concurrencyManager
         * @param resource
         */
        protected NextPartitionIdTask(IConcurrencyManager concurrencyManager, String resource) {

            super(concurrencyManager, ITx.UNISOLATED, resource);
            
        }

        /**
         * @return The next partition identifier as an {@link Integer}.
         */
        @Override
        protected Object doTask() throws Exception {

            final IIndex ndx = getIndex(getOnlyResource());
            
            final int counter = (int) ndx.getCounter().incrementAndGet();
            
            return counter;
            
        }
        
    }
    
    /**
     * Atomic operation removes the pre-existing entry for specified index
     * partition and replaces it with N new entries giving the locators for the
     * N new index partitions created when that index partition was split.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class SplitIndexPartitionTask extends AbstractTask {

        protected final PartitionLocator oldLocator;
        protected final PartitionLocator newLocators[];
        
        /**
         * @param concurrencyManager
         * @param resource
         * @param oldLocator
         * @param newLocators
         */
        protected SplitIndexPartitionTask(
                IConcurrencyManager concurrencyManager, String resource,
                PartitionLocator oldLocator,
                PartitionLocator newLocators[]) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            if (oldLocator == null)
                throw new IllegalArgumentException();

            if (newLocators == null)
                throw new IllegalArgumentException();

            this.oldLocator = oldLocator;
            
            this.newLocators = newLocators;
            
        }

        @Override
        protected Object doTask() throws Exception {

            log.info("name=" + getOnlyResource() + ", oldLocator=" + oldLocator
                    + ", locators=" + Arrays.toString(newLocators));
            
            MetadataIndex mdi = (MetadataIndex)getIndex(getOnlyResource());
            
            PartitionLocator pmd = (PartitionLocator) SerializerUtil
                    .deserialize(mdi.remove(oldLocator.getLeftSeparatorKey()));
            
            if(!oldLocator.equals(pmd)) {

                /*
                 * Sanity check failed - old locator not equal to the locator
                 * found under that key in the metadata index.
                 * 
                 * @todo differences in just the data service failover chain
                 * are probably not important and might be ignored.
                 */

                throw new RuntimeException("Expected oldLocator=" + oldLocator
                        + ", but actual=" + pmd);
                
            }

            final byte[] leftSeparator = oldLocator.getLeftSeparatorKey();
            
            /*
             * Sanity check the first locator. It's leftSeparator MUST be the
             * leftSeparator of the index partition that was split.
             */
            if(!BytesUtil.bytesEqual(leftSeparator,newLocators[0].getLeftSeparatorKey())) {
                
                throw new RuntimeException("locators[0].leftSeparator does not agree.");
                
            }

            /*
             * Sanity check the last locator. It's rightSeparator MUST be the
             * rightSeparator of the index partition that was split.  For the
             * last index partition, the right separator is always null.
             */
            {
                
                final int indexOf = mdi.indexOf(leftSeparator);
                byte[] rightSeparator;
                try {

                    // The key for the next index partition.

                    rightSeparator = mdi.keyAt(indexOf + 1);

                } catch (IndexOutOfBoundsException ex) {

                    // The rightSeparator for the last index partition is null.

                    rightSeparator = null;

                }

                final PartitionLocator locator = newLocators[newLocators.length - 1];
                
                if (rightSeparator == null) {

                    if (locator.getRightSeparatorKey() != null) {

                        throw new RuntimeException("locators["
                                + newLocators.length
                                + "].rightSeparator should be null.");

                    }

                } else {

                    if (!BytesUtil.bytesEqual(rightSeparator, locator
                            .getRightSeparatorKey())) {

                        throw new RuntimeException("locators["
                                + newLocators.length
                                + "].rightSeparator does not agree.");

                    }

                }
                
            }

            for(int i=0; i<newLocators.length; i++) {
                
                PartitionLocator locator = newLocators[i];
                
//                PartitionLocator tmp = new PartitionLocator(
//                        locator.getPartitionId(),
//                        locator.getDataServices()
//                );

                mdi.insert(locator.getLeftSeparatorKey(), SerializerUtil
                        .serialize(locator));
                
            }
            
            return null;
            
        }

    }
    
    protected class JoinIndexPartitionTask extends AbstractTask {

        protected final PartitionLocator oldLocators[];
        protected final PartitionLocator newLocator;
        
        /**
         * @param concurrencyManager
         * @param resource
         * @param oldLocators
         * @param newLocator
         */
        protected JoinIndexPartitionTask(
                IConcurrencyManager concurrencyManager, String resource,
                PartitionLocator oldLocators[],
                PartitionLocator newLocator) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            if (oldLocators == null)
                throw new IllegalArgumentException();

            if (newLocator == null)
                throw new IllegalArgumentException();

            this.oldLocators = oldLocators;
            
            this.newLocator = newLocator;
            
        }

        @Override
        protected Object doTask() throws Exception {

            log.info("name=" + getOnlyResource() + ", oldLocators=" + Arrays.toString(oldLocators)
                    + ", newLocator=" + newLocator);
            
            MetadataIndex mdi = (MetadataIndex)getIndex(getOnlyResource());
            
            // remove the old locators from the metadata index.
            for(int i=0; i<oldLocators.length; i++) {
                
                PartitionLocator locator = oldLocators[i];
                
                PartitionLocator pmd = (PartitionLocator) SerializerUtil
                        .deserialize(mdi.remove(locator.getLeftSeparatorKey()));

                if (!locator.equals(pmd)) {

                    /*
                     * Sanity check failed - old locator not equal to the
                     * locator found under that key in the metadata index.
                     * 
                     * @todo differences in just the data service failover chain
                     * are probably not important and might be ignored.
                     */

                    throw new RuntimeException("Expected oldLocator=" + locator
                            + ", but actual=" + pmd);
                    
                }

                /*
                 * FIXME validate that the newLocator is a perfect fit
                 * replacement for the oldLocators in terms of the key range
                 * spanned and that there are no gaps.  Add an API constaint
                 * that the oldLocators are in key order by their leftSeparator
                 * key.
                 */
                
            }

            // add the new locator to the metadata index.
            mdi.insert(newLocator.getLeftSeparatorKey(), SerializerUtil
                    .serialize(newLocator));
            
            return null;
            
        }

    }

    /**
     * Registers a metadata index for a named scale-out index and statically
     * partition the index using the given separator keys and data services.
     * 
     * @todo this does not attempt to handle errors on data services when
     *       attempting to register the index partitions. it should failover
     *       rather than just dying. as it stands an error will result in the
     *       task aborting but any registered index partitions will already
     *       exist on the various data servers. that will make it impossible to
     *       re-register the scale-out index until those index partitions have
     *       been cleaned up, which is a more than insignificant pain!
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class RegisterScaleOutIndexTask extends AbstractTask {

        /** The name of the scale-out index. */
        final private String scaleOutIndexName;
        /** The metadata template for the scale-out index. */
        final private IndexMetadata metadata;
        /** The #of index partitions to create. */
        final private int npartitions;
        /** The separator keys for those index partitions. */
        final private byte[][] separatorKeys;
        /** The service UUIDs of the data services on which to create those index partitions. */
        final private UUID[] dataServiceUUIDs;
        /** The data services on which to create those index partitions. */
        final private IDataService[] dataServices;
        
        /**
         * Create and statically partition a scale-out index.
         * 
         * @param metadataIndexName
         *            The name of the metadata index (the resource on which the
         *            task must have a lock).
         * @param separatorKeys
         *            The array of separator keys. Each separator key is
         *            interpreted as an <em>unsigned byte[]</em>. The first
         *            entry MUST be an empty byte[]. The entries MUST be in
         *            sorted order.
         * @param dataServiceUUIDs
         *            The array of data services onto which each partition
         *            defined by a separator key will be mapped (optional). The
         *            #of entries in this array MUST agree with the #of entries
         *            in the <i>separatorKeys</i> array. When <code>null</code>,
         *            the index paritions will be auto-assigned to data
         *            services.
         */
        public RegisterScaleOutIndexTask(ConcurrencyManager concurrencyManager,
                IResourceManager resourceManager, String metadataIndexName,
                final IndexMetadata metadata, byte[][] separatorKeys,
                UUID[] dataServiceUUIDs) {

            super(concurrencyManager, ITx.UNISOLATED, metadataIndexName);

            if(metadata==null)
                throw new IllegalArgumentException();
            
            if(separatorKeys==null) 
                throw new IllegalArgumentException();

            if (separatorKeys.length == 0)
                throw new IllegalArgumentException();

            if (dataServiceUUIDs != null) {

                if (dataServiceUUIDs.length == 0)
                    throw new IllegalArgumentException();

                if (separatorKeys.length != dataServiceUUIDs.length)
                    throw new IllegalArgumentException();

            } else {
                
                /*
                 * Auto-assign the index partitions to data services.
                 */
                
                dataServiceUUIDs = new UUID[separatorKeys.length];
                
                for( int i=0; i<separatorKeys.length; i++) {
                    
                    try {

                        dataServiceUUIDs[i] = getUnderUtilizedDataService();
                    
                    } catch(IOException ex) {
                        
                        throw new RuntimeException(ex);
                        
                    }
                    
                }
                
            }

            this.scaleOutIndexName = metadata.getName();

            this.metadata = metadata;
            
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

        /**
         * Create and statically partition the scale-out index.
         * 
         * @return The UUID assigned to the managed index.
         */
        protected Object doTask() throws Exception {
            
            // the name of the metadata index itself.
            final String metadataName = getOnlyResource();
            
            // make sure there is no metadata index for that btree.
            try {
                
                getIndex(metadataName);
                
                throw new IndexExistsException(metadataName);
                
            } catch(NoSuchIndexException ex) {

                // ignore expected exception
                
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
            
            /*
             * Create the metadata index.
             */
            
            AbstractJournal journal = getJournal();
            
            MetadataIndex mdi = MetadataIndex.create(journal,
                    metadataIndexUUID, metadata);

            /*
             * Register the metadata index with the metadata service. This
             * registration will not be restart safe until the task commits.
             */
            journal.registerIndex(metadataName, mdi);

            /*
             * Map the partitions onto the data services.
             */
            
            PartitionLocator[] partitions = new PartitionLocator[npartitions];
            
            for(int i=0; i<npartitions; i++) {
                
                final byte[] leftSeparator = separatorKeys[i];

                final byte[] rightSeparator = i + 1 < npartitions ? separatorKeys[i + 1]
                        : null;

                PartitionLocator pmd = new PartitionLocator(//
                        mdi.nextPartitionId(),//
                        new UUID[] { //
                            dataServiceUUIDs[i]
                        },
                        leftSeparator,
                        rightSeparator
                        );
                
                log.info("name=" + scaleOutIndexName + ", pmd=" + pmd);

                /*
                 * Map the initial partition onto that data service. This
                 * requires us to compute the left and right separator keys. The
                 * right separator key is just the separator key for the next
                 * partition in order and null iff this is the last partition.
                 */

                IndexMetadata md = metadata.clone();
                
                // override the partition metadata.
                md.setPartitionMetadata(new LocalPartitionMetadata(
                        pmd.getPartitionId(),//
                        leftSeparator,//
                        rightSeparator,//
                        /*
                         * Note: The resourceMetadata[] is set on the data
                         * service when the index is actually registered since
                         * that is the only time when we can guarentee that we
                         * have access to the live journal. Otherwise the
                         * journal MAY have overflowed and a different journal
                         * COULD be the live journal by the time this request is
                         * processed.
                         */
                         null // resourceMetadata[]
                    ));
                
                dataServices[i].registerIndex(DataService
                        .getIndexPartitionName(scaleOutIndexName, pmd.getPartitionId()), md);

                partitions[i] = pmd;
                
            }

            /*
             * Record each partition in the metadata index.
             */

            for (int i = 0; i < npartitions; i++) {

//                mdi.put(separatorKeys[i], partitions[i]);
                
                mdi.insert(separatorKeys[i], SerializerUtil.serialize(partitions[i]));
            
            }

            // Done.
            
            return mdi.getScaleOutIndexMetadata().getIndexUUID();
            
        }
        
    }

    /**
     * Drops a scale-out index.
     * <p>
     * Since this task is unisolated, it basically has a lock on the writable
     * version of the metadata index. It then drops each index partition and
     * finally drops the metadata index itself.
     * 
     * @todo Commits by the write service could be delayed by the remote RPCs,
     *       but a change to the commit protocol to use checkpoints will fix
     *       that issue.
     * 
     * @todo This does it try to handle errors gracefully. E.g., if there is a
     *       problem with one of the data services hosting an index partition it
     *       does not fail over to the next data service for that index
     *       partition.
     */
    public class DropScaleOutIndexTask extends AbstractTask {

        /**
         * @param journal
         * @param name
         *            The name of the metadata index for some scale-out index.
         */
        protected DropScaleOutIndexTask(ConcurrencyManager concurrencyManager,
                String name) {
            
            super(concurrencyManager, ITx.UNISOLATED, name);

        }

        /**
         * Drops the index partitions and then drops the metadata index as well.
         * 
         * @return The {@link Integer} #of index partitions that were dropped.
         */
        @Override
        protected Object doTask() throws Exception {

            final MetadataIndex ndx;

            try {
                
                ndx = (MetadataIndex) getIndex(getOnlyResource());

            } catch (ClassCastException ex) {

                throw new UnsupportedOperationException(
                        "Not a scale-out index?", ex);

            }

            // name of the scale-out index.
            final String name = ndx.getScaleOutIndexMetadata().getName();
            
            log.info("Will drop index partitions for "+name);
            
            final ChunkedLocalRangeIterator itr = new ChunkedLocalRangeIterator(
                    ndx, null, null, 0/* capacity */, IRangeQuery.VALS, null/* filter */);
            
            int ndropped = 0;
            
            while(itr.hasNext()) {
                
                ITuple tuple = itr.next();

                // @TODO use getValueStream() variant once I resolve problem with stream.
                PartitionLocator pmd = (PartitionLocator) SerializerUtil
                        .deserialize(tuple.getValue());
//                .deserialize(tuple.getValueStream());

                /*
                 * Drop the index partition.
                 */
                {
                    
                    final int partitionId = pmd.getPartitionId();
                    
                    final UUID serviceUUID = pmd.getDataServices()[0];
                    
                    final IDataService dataService = getDataServiceByUUID(serviceUUID);
                    
                    log.info("Dropping index partition: partitionId="+partitionId+", dataService="+dataService);
                    
                    dataService.dropIndex(DataService.getIndexPartitionName(name, partitionId));
                    
                }
                
                ndropped++;
                
            }
            
            // flush all delete requests.
            itr.flush();
            
            log.info("Dropped "+ndropped+" index partitions for "+name);

            // drop the metadata index as well.
            getJournal().dropIndex(getOnlyResource());
            
            return ndropped;
            
        }

    }

}
