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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexExistsException;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IndexPartitionCause;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.ResourceManager;

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
     * 
     * @see DataService#getIndexPartitionName(String, int)
     */
    public static String getMetadataIndexName(String name) {
        
        return METADATA_INDEX_NAMESPACE + name;
        
    }
    
    /**
     * The namespace for the metadata indices.
     */
    public static final String METADATA_INDEX_NAMESPACE = "metadata-";

    /**
     * Options for the {@link MetadataService}.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends DataService.Options {
        
    }

    /**
     * @param properties
     */
    protected MetadataService(Properties properties) {

        super(properties);

    }

    public MetadataService start() {
        
        return (MetadataService) super.start();
        
    }
    
    /**
     * Note: You SHOULD NOT be running arbitrary tasks on a
     * {@link MetadataService}. They are specialized for the index partition
     * locator information and SHOULD NOT be overloaded for other purposes.
     * 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    @Override
    public Future<? extends Object> submit(Callable<? extends Object> task) {

        // throw new UnsupportedOperationException();
        return super.submit(task);
        
    }
    
    public int nextPartitionId(String name) throws IOException, InterruptedException, ExecutionException {
       
        setupLoggingContext();
        
        try {

            final AbstractTask task = new NextPartitionIdTask(
                    getConcurrencyManager(), getMetadataIndexName(name));
            
            final Integer partitionId = (Integer) getConcurrencyManager().submit(
                    task).get();
        
            if (INFO)
                log.info("Assigned partitionId=" + partitionId + ", name="
                        + name);
            
            return partitionId.intValue();
            
        } finally {
            
            clearLoggingContext();
            
        }        
        
    }
    
    public PartitionLocator get(String name, long timestamp, final byte[] key)
            throws InterruptedException, ExecutionException, IOException {
    
        setupLoggingContext();

        try {

            if (timestamp == ITx.UNISOLATED) {

                /*
                 * This is a read-only operation so run as read committed rather
                 * than unisolated.
                 */
                
                timestamp = ITx.READ_COMMITTED;

            }

            final AbstractTask task = new GetTask(getConcurrencyManager(),
                    timestamp, getMetadataIndexName(name), key);
            
            return (PartitionLocator) getConcurrencyManager().submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        

    }

    /**
     * Task for {@link MetadataService#get(String, long, byte[])}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private final class GetTask extends AbstractTask {

        private final byte[] key;
        
        public GetTask(IConcurrencyManager concurrencyManager, long timestamp,
                String resource, byte[] key) {

            super(concurrencyManager, timestamp, resource);

            this.key = key;
            
        }

        @Override
        protected Object doTask() throws Exception {

            MetadataIndex ndx = (MetadataIndex) getIndex(getOnlyResource());

            return ndx.get(key);

        }
        
    }

    public PartitionLocator find(String name, long timestamp, final byte[] key)
            throws InterruptedException, ExecutionException, IOException {

        setupLoggingContext();

        try {

            if (timestamp == ITx.UNISOLATED) {

                /*
                 * This is a read-only operation so run as read committed rather
                 * than unisolated.
                 */
                
                timestamp = ITx.READ_COMMITTED;

            }
            
            final AbstractTask task = new FindTask(getConcurrencyManager(),
                    timestamp, getMetadataIndexName(name), key);
            
            return (PartitionLocator) getConcurrencyManager().submit(task).get();

        } finally {

            clearLoggingContext();

        }

    }

    /**
     * Task for {@link MetadataService#find(String, long, byte[])}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static private final class FindTask extends AbstractTask {

        private final byte[] key;
        
        public FindTask(IConcurrencyManager concurrencyManager, long timestamp,
                String resource, byte[] key) {

            super(concurrencyManager, timestamp, resource);

            this.key = key;
            
        }

        @Override
        protected Object doTask() throws Exception {

            MetadataIndex ndx = (MetadataIndex) getIndex(getOnlyResource());

            return ndx.find(key);

        }
        
    }
    
    public void splitIndexPartition(String name, PartitionLocator oldLocator,
            PartitionLocator newLocators[]) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            final AbstractTask task = new SplitIndexPartitionTask(
                    getConcurrencyManager(), getMetadataIndexName(name),
                    oldLocator, newLocators);
            
            getConcurrencyManager().submit(task).get();
            
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
                    getConcurrencyManager(), getMetadataIndexName(name),
                    oldLocators, newLocator);
            
            getConcurrencyManager().submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        
        
    }
    
    public void moveIndexPartition(String name, PartitionLocator oldLocator,
            PartitionLocator newLocator) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            final AbstractTask task = new MoveIndexPartitionTask(
                    getConcurrencyManager(), getMetadataIndexName(name),
                    oldLocator, newLocator);
            
            getConcurrencyManager().submit(task).get();
            
        } finally {
            
            clearLoggingContext();
            
        }        
        
    }
    
    /**
     * @todo if if exits already? (and has consistent/inconsistent metadata)?
     */
    public UUID registerScaleOutIndex(final IndexMetadata metadata,
            final byte[][] separatorKeys, final UUID[] dataServices)
            throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            if (metadata.getName() == null) {

                throw new IllegalArgumentException(
                        "No name assigned to index in metadata template.");
                
            }
            
            /*
             * Note: This automatically turns on delete markers since they are
             * required for a scale-out index.
             */
            if(!metadata.getDeleteMarkers()) {
                
                metadata.setDeleteMarkers(true);
                
                if (INFO)
                    log.info("Enabling delete markers: "+metadata.getName());
                
            }
            
//            if (!metadata.getDeleteMarkers()) {
//
//                throw new IllegalArgumentException(ERR_DELETE_MARKERS);
//
//            }

            final String scaleOutIndexName = metadata.getName();
            
            // Note: We need this in order to assert a lock on this resource!
            final String metadataIndexName = MetadataService
                    .getMetadataIndexName(scaleOutIndexName);
     
            final AbstractTask task = new RegisterScaleOutIndexTask(getFederation(),
                    getConcurrencyManager(), getResourceManager(), metadataIndexName,
                    metadata, separatorKeys, dataServices);
            
            final UUID managedIndexUUID = (UUID) getConcurrencyManager()
                    .submit(task).get();

            return managedIndexUUID;
            
        } finally {

            clearLoggingContext();

        }

    }
    
    public void dropScaleOutIndex(final String name) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            final AbstractTask task = new DropScaleOutIndexTask(
                    getFederation(), getConcurrencyManager(),
                    getMetadataIndexName(name));
            
            getConcurrencyManager().submit(task).get();
        
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
    static protected class NextPartitionIdTask extends AbstractTask {

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

            final MetadataIndex ndx = (MetadataIndex)getIndex(getOnlyResource());

            final int partitionId = ndx.incrementAndGetNextPartitionId();
            
            assert ndx.needsCheckpoint();
            
//            final int counter = (int) ndx.getCounter().incrementAndGet();
            
            return partitionId;
            
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
    static protected class SplitIndexPartitionTask extends AbstractTask {

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

            if (INFO)
                log.info("name=" + getOnlyResource() + ", oldLocator="
                        + oldLocator + ", locators="
                        + Arrays.toString(newLocators));
            
            final MetadataIndex mdi = (MetadataIndex)getIndex(getOnlyResource());
            
            final PartitionLocator pmd = (PartitionLocator) SerializerUtil
                    .deserialize(mdi.remove(oldLocator.getLeftSeparatorKey()));
            
            if (pmd == null) {

                throw new RuntimeException("No such locator: name="
                        + getOnlyResource() + ", locator=" + oldLocator);

            }
            
            if(!oldLocator.equals(pmd)) {

                /*
                 * Sanity check failed - old locator not equal to the locator
                 * found under that key in the metadata index.
                 */

                throw new RuntimeException("Expected different locator: name="
                        + getOnlyResource() + ", oldLocator=" + oldLocator
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

            /*
             * Sanity check the partition identifers. They must be distinct from
             * one another and distinct from the old partition identifier.
             */

            for(int i=0; i<newLocators.length; i++) {
                
                PartitionLocator tmp = newLocators[i];

                if (tmp.getPartitionId() == oldLocator.getPartitionId()) {

                    throw new RuntimeException("Same partition identifier: "
                            + tmp + ", " + oldLocator);

                }

                for (int j = i + 1; j < newLocators.length; j++) {

                    if (tmp.getPartitionId() == newLocators[j].getPartitionId()) {

                        throw new RuntimeException(
                                "Same partition identifier: " + tmp + ", "
                                        + newLocators[j]);

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

    /**
     * Updates the {@link MetadataIndex} to reflect the join of 2 or more index
     * partitions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class JoinIndexPartitionTask extends AbstractTask {

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

            if (INFO)
                log.info("name=" + getOnlyResource() + ", oldLocators="
                        + Arrays.toString(oldLocators) + ", newLocator="
                        + newLocator);
            
            MetadataIndex mdi = (MetadataIndex)getIndex(getOnlyResource());

            /*
             * Sanity check the partition identifers. They must be distinct from
             * one another and distinct from the old partition identifier.
             */

            for(int i=0; i<oldLocators.length; i++) {
                
                PartitionLocator tmp = oldLocators[i];

                if (tmp.getPartitionId() == newLocator.getPartitionId()) {

                    throw new RuntimeException("Same partition identifier: "
                            + tmp + ", " + newLocator);

                }

                for (int j = i + 1; j < oldLocators.length; j++) {

                    if (tmp.getPartitionId() == oldLocators[j].getPartitionId()) {

                        throw new RuntimeException(
                                "Same partition identifier: " + tmp + ", "
                                        + oldLocators[j]);

                    }

                }
                    
            }

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
     * Updates the {@link MetadataIndex} to reflect the move of an index
     * partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class MoveIndexPartitionTask extends AbstractTask {

        protected final PartitionLocator oldLocator;
        protected final PartitionLocator newLocator;
        
        /**
         * @param concurrencyManager
         * @param resource
         * @param oldLocator
         * @param newLocator
         */
        protected MoveIndexPartitionTask(
                IConcurrencyManager concurrencyManager, String resource,
                PartitionLocator oldLocator,
                PartitionLocator newLocator) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            if (oldLocator == null)
                throw new IllegalArgumentException();

            if (newLocator == null)
                throw new IllegalArgumentException();

            this.oldLocator = oldLocator;
            
            this.newLocator = newLocator;
            
        }

        @Override
        protected Object doTask() throws Exception {

            if (INFO)
                log.info("name=" + getOnlyResource() + ", oldLocator="
                        + oldLocator + ", newLocator=" + newLocator);

            final MetadataIndex mdi = (MetadataIndex) getIndex(getOnlyResource());

            // remove the old locators from the metadata index.
            final PartitionLocator pmd = (PartitionLocator) SerializerUtil
                    .deserialize(mdi.remove(oldLocator.getLeftSeparatorKey()));


            if (pmd == null) {

                throw new RuntimeException("No such locator: name="
                        + getOnlyResource() + ", locator=" + oldLocator);

            }
            
            if (!oldLocator.equals(pmd)) {

                /*
                 * Sanity check failed - old locator not equal to the locator
                 * found under that key in the metadata index.
                 * 
                 * @todo differences in just the data service failover chain are
                 * probably not important and might be ignored.
                 */

                throw new RuntimeException("Expected oldLocator=" + oldLocator
                        + ", but actual=" + pmd);

            }

            /*
             * FIXME validate that the newLocator is a perfect fit replacement
             * for the oldLocators in terms of the key range spanned and that
             * there are no gaps. Add an API constaint that the oldLocators are
             * in key order by their leftSeparator key.
             */

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
     *       rather than just dying.
     * 
     * @todo an error during execution can result in the task aborting but any
     *       registered index partitions will already exist on the various data
     *       servers. that will make it impossible to re-register the scale-out
     *       index until those index partitions have been cleaned up, which is a
     *       more than insignificant pain (they could be cleaned up by a
     *       bottom-up index rebuild followed by dropping the rebuilt index).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class RegisterScaleOutIndexTask extends AbstractTask {

        /** The federation. */
        final private IBigdataFederation fed;
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
        public RegisterScaleOutIndexTask(
                final IBigdataFederation fed,
                final ConcurrencyManager concurrencyManager,
                final IResourceManager resourceManager,
                final String metadataIndexName,
                final IndexMetadata metadata,
                final byte[][] separatorKeys,
                UUID[] dataServiceUUIDs
                ) {

            super(concurrencyManager, ITx.UNISOLATED, metadataIndexName);

            if (fed == null)
                throw new IllegalArgumentException();

            if (metadata == null)
                throw new IllegalArgumentException();

            if (separatorKeys == null)
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

                try {

                    // discover under-utilized data service UUIDs.
                    dataServiceUUIDs = fed.getLoadBalancerService().getUnderUtilizedDataServices(
                            separatorKeys.length, // minCount
                            separatorKeys.length, // maxCount
                            null// exclude
                            );
                    
                } catch(Exception ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
            }

            this.fed = fed;
            
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

                final byte[] separatorKey = separatorKeys[i];
                
                if (separatorKey == null) {

                    throw new IllegalArgumentException();

                }
                
                if (i > 0) {
                    
                    if(BytesUtil.compareBytes(separatorKey, separatorKeys[i-1])<0) {
                        
                        throw new IllegalArgumentException(
                                "Separator keys out of order at index=" + i);
                        
                    }
                    
                }

                final UUID uuid = dataServiceUUIDs[i];

                if (uuid == null) {

                    throw new IllegalArgumentException();

                }

                final IDataService dataService = fed.getDataService(uuid);

                if (dataService == null) {

                    throw new IllegalArgumentException(
                            "Unknown data service: uuid=" + uuid);

                }

                dataServices[i] = dataService;

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
            
            final MetadataIndex mdi = MetadataIndex.create(getJournal(),
                    metadataIndexUUID, metadata);

            /*
             * Map the partitions onto the data services.
             */
            
            final PartitionLocator[] partitions = new PartitionLocator[npartitions];
            
            for (int i = 0; i < npartitions; i++) {
                
                final byte[] leftSeparator = separatorKeys[i];

                final byte[] rightSeparator = i + 1 < npartitions ? separatorKeys[i + 1]
                        : null;

                final PartitionLocator pmd = new PartitionLocator(//
                        mdi.incrementAndGetNextPartitionId(),//
                        dataServiceUUIDs[i],
                        leftSeparator,
                        rightSeparator
                        );
                
                if (INFO)
                    log.info("name=" + scaleOutIndexName + ", pmd=" + pmd);

                /*
                 * Map the initial partition onto that data service. This
                 * requires us to compute the left and right separator keys. The
                 * right separator key is just the separator key for the next
                 * partition in order and null iff this is the last partition.
                 */

                final IndexMetadata md = metadata.clone();
                
                // override the partition metadata.
                md.setPartitionMetadata(new LocalPartitionMetadata(
                        pmd.getPartitionId(),//
                        -1, // we are creating a new index, not moving an index partition.
                        leftSeparator,//
                        rightSeparator,//
                        /*
                         * Note: By setting this to null we are indicating to
                         * the RegisterIndexTask on the data service that it
                         * needs to set the resourceMetadata[] when the index is
                         * actually registered based on the live journal as of
                         * the when the task actually executes on the data
                         * service.
                         */
                         null, // [resources] Signal to the RegisterIndexTask.
                         null, // [cause] Signal to RegisterIndexTask
                         /*
                          * History.
                          */
                         "createScaleOutIndex(name="+scaleOutIndexName+") "
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

            /*
             * Register the metadata index with the metadata service. This
             * registration will not be restart safe until the task commits.
             */
            getJournal().registerIndex(metadataName, mdi);

            // Done.
            
            return mdi.getScaleOutIndexMetadata().getIndexUUID();
            
        }
        
    }

    /**
     * Drops a scale-out index.
     * <p>
     * Since this task is unisolated, it basically has a lock on the writable
     * version of the metadata index. It drops each index partition and finally
     * drops the metadata index itself.
     * <p>
     * Historical reads against the metadata index will continue to succeed both
     * during and after this operation has completed successfully. However,
     * {@link ITx#READ_COMMITTED} operations will succeed only until this
     * operation completes at which point the scale-out index will no longer be
     * visible.
     * <p>
     * The data comprising the scale-out index will remain available for
     * historical reads until it is released by whatever policy is in effect for
     * the {@link ResourceManager}s for the {@link DataService}s on which that
     * data resides.
     * 
     * @todo This does not try to handle errors gracefully. E.g., if there is a
     *       problem with one of the data services hosting an index partition it
     *       does not fail over to the next data service for that index
     *       partition.
     */
    static public class DropScaleOutIndexTask extends AbstractTask {

        private final IBigdataFederation fed;
        
        /**
         * @parma fed
         * @param journal
         * @param name
         *            The name of the metadata index for some scale-out index.
         */
        protected DropScaleOutIndexTask(IBigdataFederation fed,
                ConcurrencyManager concurrencyManager, String name) {
            
            super(concurrencyManager, ITx.UNISOLATED, name);
            
            if (fed == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;

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
            
            if (INFO)
                log.info("Will drop index partitions for " + name);
            
//            final ChunkedLocalRangeIterator itr = new ChunkedLocalRangeIterator(
//                    ndx, null, null, 0/* capacity */, IRangeQuery.VALS, null/* filter */);
            final ITupleIterator itr = ndx.rangeIterator(null, null,
                    0/* capacity */, IRangeQuery.VALS, null/* filter */);
            
            int ndropped = 0;
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();

                // FIXME There is still (5/30/08) a problem with using getValueStream() here!
                final PartitionLocator pmd = (PartitionLocator) SerializerUtil
                        .deserialize(tuple.getValue());
//                .deserialize(tuple.getValueStream());

                /*
                 * Drop the index partition.
                 */
                {
                    
                    final int partitionId = pmd.getPartitionId();
                    
                    final UUID serviceUUID = pmd.getDataServiceUUID();
                    
                    final IDataService dataService = fed
                            .getDataService(serviceUUID);

                    if (INFO)
                        log.info("Dropping index partition: partitionId="
                                + partitionId + ", dataService=" + dataService);

                    dataService.dropIndex(DataService.getIndexPartitionName(
                            name, partitionId));

                }
                
                ndropped++;
                
            }
            
//            // flush all delete requests.
//            itr.flush();
            
            if (INFO)
                log.info("Dropped " + ndropped + " index partitions for "
                        + name);

            // drop the metadata index as well.
            getJournal().dropIndex(getOnlyResource());
            
            return ndropped;
            
        }

    }

}
