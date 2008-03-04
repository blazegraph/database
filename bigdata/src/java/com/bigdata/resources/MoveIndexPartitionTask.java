/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 29, 2008
 */

package com.bigdata.resources;

import java.io.IOException;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.RawDataServiceRangeIterator;
import com.bigdata.service.DataService.IDataServiceIndexProcedure;

/**
 * Historical read task is used to copy a view of an index partition as of the
 * lastCommitTime of old journal to another {@link IDataService}. After this
 * task has been run you must run an {@link ITx#UNISOLATED}
 * {@link UpdateMoveIndexPartitionTask} in order to atomically migrate any
 * writes on the live journal to the target {@link IDataService} and update the
 * {@link MetadataIndex}.
 * <p>
 * Note: This task is run on the target {@link IDataService} and it copies the
 * data from the source {@link IDataService}. This allows us to use standard
 * {@link IRangeQuery} operations to copy the historical view. However, the
 * {@link UpdateMoveIndexPartitionTask} is run on the source
 * {@link IDataService} since it needs to obtain an exclusive lock on the index
 * partition that is being moved in order to prevent concurrent writes during
 * the atomic cutover. For the same reason, the
 * {@link UpdateMoveIndexPartitionTask} can not use standard {@link IRangeQuery}
 * operations. Instead, it initiates a series of data transfers while holding
 * onto the exclusive lock until the target {@link IDataService} has the current
 * state of the index partition. At that point is notifies the
 * {@link IMetadataService} to perform the atomic cutover to the new index
 * partition.
 * <p>
 * Note: This task does NOT cause any resources associated with the current view
 * of the index partition to be released on the source {@link IDataService}.
 * The reason is two-fold. First, the {@link IndexSegment}(s) associated with
 * that view MAY be in used by historical views. Second, there MAY be historical
 * commit points for the index partition on the live journal before the atomic
 * cutover to the new {@link IDataService} - those historical commit points MUST
 * be preserved until the release policy for those views has been satisified.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MoveIndexPartitionTask extends AbstractTask {

    private final ResourceManager resourceManager;
    private final long lastCommitTime;
    private final UUID targetDataServiceUUID;

    /**
     * @param concurrencyManager
     * @param lastCommitTime
     *            The lastCommitTime of the old journal.
     * @param resource
     *            The name of the source index partition.
     * @param targetDataServiceUUID
     *            The UUID for the target data service.
     */
    public MoveIndexPartitionTask(ResourceManager resourceManager,
            IConcurrencyManager concurrencyManager, long lastCommitTime,
            String resource, UUID targetDataServiceUUID) {

        super(concurrencyManager, -lastCommitTime, resource);

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if (targetDataServiceUUID == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;

        this.lastCommitTime = lastCommitTime;
        
        this.targetDataServiceUUID = targetDataServiceUUID;
        
        if(resourceManager.getDataServiceUUID().equals(targetDataServiceUUID)) {
            
            throw new IllegalArgumentException("Target must be a different data service");
            
        }
                
    }

    @Override
    protected Object doTask() throws Exception {

        // view of the source index partition.
        final IIndex src = getIndex(getOnlyResource());
        
        // clone metadata.
        final IndexMetadata newMetadata = src.getIndexMetadata().clone();
        
        // name of the corresponding scale-out index.
        final String scaleOutIndexName = newMetadata.getName();
                
        // obtain new partition identifier from the metadata service (RMI)
        final int newPartitionId = resourceManager.getMetadataService()
                .nextPartitionId(scaleOutIndexName);

        // the partition metadata for the source index partition.
        final LocalPartitionMetadata oldpmd = newMetadata.getPartitionMetadata();
        
        newMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                newPartitionId,//
                oldpmd.getLeftSeparatorKey(),//
                oldpmd.getRightSeparatorKey(),//
                /*
                 * Note: This is [null] to indicate that the resource metadata
                 * needs to be filled in by the target data service when the
                 * new index partition is registered.
                 */
                null
                ));

        
        // the data service on which we will register the new index partition.
        final IDataService targetDataService = resourceManager
                .getDataService(targetDataServiceUUID);

        final String sourceIndexName = getOnlyResource();

        final UUID[] sourceDataServiceUUIDs = resourceManager.getDataServiceUUIDs();
        
        final String targetIndexName = DataService.getIndexPartitionName(
                scaleOutIndexName, newPartitionId);
       
        /*
         * Register new index partition on the target data service.
         * 
         * Note: The correct resource metadata for the new index partition will
         * be assigned when it is registered on the target data service. See above
         * and RegisterIndexTask.
         */
        targetDataService.registerIndex(targetIndexName, newMetadata);

        /*
         * Run procedure that will copy data from the old index partition to the
         * new index partition as of the [lastCommitTime] of the old journal.
         */
        targetDataService.submit(ITx.UNISOLATED, targetIndexName,
                new CopyIndexPartitionProcedure(sourceDataServiceUUIDs,
                        sourceIndexName,lastCommitTime));
        
        /*
         * At this point the historical view as of the [lastCommitTime] has been
         * copied to the target data service.
         * 
         * The MoveResult contains the information that we need to run the
         * atomic move update task which will bring that view up to the current
         * state of the index partition and then atomically switch over to the
         * new index partition.
         */
        
        return new MoveResult(sourceIndexName, src.getIndexMetadata(),
                targetDataServiceUUID, newPartitionId);
        
    }

    /**
     * The object returned by {@link MoveIndexPartitionTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MoveResult extends AbstractResult {

        final UUID targetDataServiceUUID;
        final int newPartitionId;
        
        /**
         * @param name 
         * @param indexMetadata
         * @param targetDataServiceUUID
         * @param newPartitionId
         */
        public MoveResult(String name, IndexMetadata indexMetadata,
                UUID targetDataServiceUUID, int newPartitionId) {
            
            super(name, indexMetadata);

            this.targetDataServiceUUID = targetDataServiceUUID;
            
            this.newPartitionId = newPartitionId;
            
        }

    }

    /**
     * Procedure copies data from the old index partition to the new index
     * partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CopyIndexPartitionProcedure implements IDataServiceIndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = 8024224972159862036L;

        private UUID[] sourceDataServiceUUIDs;
        private String sourceIndexName;
        private long lastCommitTime;
        
        private transient IMetadataService metadataService;
        
        /**
         * De-serialization ctor.
         */
        public CopyIndexPartitionProcedure() {
            
        }
        
        /**
         * @param sourceDataServiceUUIDs
         * @param sourceIndexName
         * @param lastCommitTime
         * 
         */
        public CopyIndexPartitionProcedure(UUID[] sourceDataServiceUUIDs,
                String sourceIndexName,long lastCommitTime) {

            if (sourceDataServiceUUIDs == null) {

                throw new IllegalArgumentException();

            }

            if (sourceDataServiceUUIDs.length == 0) {

                throw new IllegalArgumentException();

            }

            if (sourceIndexName == null) {

                throw new IllegalArgumentException();
                
            }
            
            this.lastCommitTime = lastCommitTime;
            
         }
        
        /**
         * Copy the non-delete index entries from the source index view
         * <P>
         * Note: We copy only the non-deleted keys and values since this is the
         * initial state of this view on this data service.
         * <p>
         * Note: we take care to preserve the timestamps in case the index is
         * being used to support transactions.
         * 
         * @return The #of index entries copied.
         */
        public Object apply(IIndex ndx) {

            // the live index on which we are writing.
            final BTree dst = (BTree)ndx;
            
            // @todo handle failover / read from secondaries.
            IDataService sourceDataService;
            try {
                sourceDataService = metadataService.getDataService(sourceDataServiceUUIDs[0]);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            
            // iterator reading from the source index partition.
            final ITupleIterator itr = new RawDataServiceRangeIterator(
                    sourceDataService, sourceIndexName, lastCommitTime,
                    null/* fromKey */, null/* toKey */, 0/* capacity */,
                    IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

            final boolean isIsolatable = ndx.getIndexMetadata().isIsolatable();

            long ncopied = 0L;
            
            while (itr.hasNext()) {

                final ITuple tuple = itr.next();
                
                final byte[] key = tuple.getKey();
                
                final byte[] val = tuple.getValue();
                
               if(isIsolatable) {
                   
                   final long timestamp = tuple.getVersionTimestamp();
                   
                   dst.insert(key, val, false/* delete */, timestamp, null/* tuple */);

                } else {

                    dst.insert(key, val);
                   
               }
               
               ncopied++;
                
            }

            log.info("Copied " + ncopied + " index entries from "
                    + sourceIndexName);
            
            return ncopied;
            
        }

        public void setMetadataService(IMetadataService metadataService) {

            this.metadataService = metadataService;
            
        }

    }

}
