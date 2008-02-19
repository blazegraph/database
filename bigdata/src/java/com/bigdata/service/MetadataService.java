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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ChunkedLocalRangeIterator;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrentJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IndexExistsException;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadata;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;

/**
 * Implementation of a metadata service for a named scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Support transparent dynamic partitioning of scale-out indices. Handle
 *       the load-balancing later.
 * 
 * @todo Support transactionally isolated views onto the metadata index by
 *       passing in the tx identifier and using the appropriate historical view
 *       of the metadata index?
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
 * @todo support two-tier metadata index.
 * 
 * @todo the client (or the data services?) should send as async message every N
 *       seconds providing a histogram of the partitions they have touched (this
 *       could be input to the load balanced as well as info about the partition
 *       use that would inform load balancing decisions).
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
    
    /**
     * @param properties
     */
    protected MetadataService(Properties properties) {

        super(properties);

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
            
            final UUID managedIndexUUID = (UUID) journal.submit(
                    new RegisterScaleOutIndexTask(journal, metadataIndexName,
                            metadata, separatorKeys, dataServices)).get();

            return managedIndexUUID;
            
        } finally {

            clearLoggingContext();

        }

    }
    
    public void dropScaleOutIndex(String name) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            journal.submit(
                    new DropScaleOutIndexTask(journal,
                            getMetadataIndexName(name))).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
   
    /*
     * Tasks.
     */
    
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
        public RegisterScaleOutIndexTask(ConcurrentJournal journal,
                String metadataIndexName, final IndexMetadata metadata,
                byte[][] separatorKeys, UUID[] dataServiceUUIDs) {

            super(journal, ITx.UNISOLATED, false/* readOnly */,
                    metadataIndexName);

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
            
            PartitionMetadata[] partitions = new PartitionMetadata[npartitions];
            
            for(int i=0; i<npartitions; i++) {
                
                PartitionMetadata pmd = new PartitionMetadata(//
                        mdi.nextPartitionId(),//
                        new UUID[] { //
                            dataServiceUUIDs[i]
                        },
                        new IResourceMetadata[] { //
                            dataServices[i].getJournalMetadata() }
                        );
                
                log.info("name=" + scaleOutIndexName + ", partitionId="
                        + pmd.getPartitionId());

                /*
                 * Map the initial partition onto that data service. This
                 * requires us to compute the left and right separator keys. The
                 * right separator key is just the separator key for the next
                 * partition in order and null iff this is the last partition.
                 */

                IndexMetadata md = metadata.clone();
                
                // override the partition metadata.
                md.setPartitionMetadata(new PartitionMetadataWithSeparatorKeys(
                        separatorKeys[i],// leftSeparator
                        pmd,//
                        i + 1 < npartitions ? separatorKeys[i + 1] : null) // rightSeparator
                    );
                
                dataServices[i].registerIndex(DataService
                        .getIndexPartitionName(scaleOutIndexName, pmd.getPartitionId()), md);

                partitions[i] = pmd;
                
            }

            /*
             * Record each partition in the metadata index.
             */

            for(int i=0; i<npartitions; i++) {

                mdi.put(separatorKeys[i], partitions[i]);
            
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
        protected DropScaleOutIndexTask(ConcurrentJournal journal,
                String name) {
            
            super(journal, ITx.UNISOLATED, false/*readOnly*/, name);

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

                IPartitionMetadata pmd = (IPartitionMetadata) SerializerUtil
//                .deserialize(tuple.getValue());
                .deserialize(tuple.getValueStream());

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
            journal.dropIndex(getOnlyResource());
            
            return ndropped;
            
        }

    }

}
