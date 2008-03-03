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

import java.util.UUID;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ResultSet;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.MoveIndexPartitionTask.MoveResult;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.MetadataService;

/**
 * Unisolated task is executed on the source data service once the historical
 * state of an index partitions as of the lastCommitTime of the old journal has
 * been copied to the target data service. The target index partition identified
 * by the {@link MoveResult} MUST already exist and MUST have been populated
 * with the state of the source index partition view as of the lastCommitTime of
 * the old journal. The task copies any writes that have been buffered for the
 * index partition on the source data service to the target data service and
 * then does an atomic update of the {@link MetadataIndex} while it is holding
 * an exclusive lock on the source index partition.
 * <p>
 * Tasks executing after this one will discover that the source index partition
 * no longer exists as of the timestamp when this task commits. Clients that
 * submit tasks for the source index partition will be notified that it no
 * longer exists. When the client queries the {@link MetadataService} it will
 * discover that the key range has been assigned to a new index partition - the
 * one on the target data service.
 * <p>
 * Note: This task runs as {@link ITx#UNISOLATED} since it MUST have an
 * exclusive lock in order to ensure that the buffered writes are transferred to
 * the target index partition without allowing concurrent writes on the source
 * index partition. This means that the target index partition WILL NOT be able
 * to issue read-requests against the live view of source index partition since
 * this task already holds the necessary lock. Therefore this task instead sends
 * zero or more {@link ResultSet}s containing the buffered writes to the target
 * index partition.
 * <p>
 * Note: The {@link ResultSet} data structure is used since it carries "delete
 * markers" and version timestamps as well as the keys and values. The delete
 * markers MUST be transferred in case the buffered writes include deletes of
 * index entries as of the lastCommitTime on the old journal. If the index
 * supports transactions then the version timestamps MUST be preserved since
 * they are the basis for validation of transaction write sets.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UpdateMoveIndexPartitionTask extends AbstractTask {

    final private ResourceManager resourceManager;
    final private MoveResult moveResult;

    /**
     * @param concurrencyManager
     * @param resource
     *            The source index partition.
     * @param moveResult
     *            The target index partition.
     */
    public UpdateMoveIndexPartitionTask(ResourceManager resourceManager,
            IConcurrencyManager concurrencyManager, String resource,
            MoveResult moveResult) {

        super(concurrencyManager, ITx.UNISOLATED, resource);

        if (resourceManager == null)
            throw new UnsupportedOperationException();

        if (moveResult == null)
            throw new UnsupportedOperationException();

        this.resourceManager = resourceManager;
        
        this.moveResult = moveResult;

    }

    /**
     * @return The #of tuples copied.
     */
    @Override
    protected Object doTask() throws Exception {

        final IDataService targetDataService = resourceManager
                .getDataService(moveResult.targetDataServiceUUID);
        
        final int chunkSize = 10000;
        
        final IIndex src = getIndex(getOnlyResource());

        final String scaleOutIndexName = src.getIndexMetadata().getName();
        
        final String targetIndexName = DataService.getIndexPartitionName(scaleOutIndexName, moveResult.newPartitionId);

        int nchunks = 0; // #of passes.
        long ncopied = 0; // #of tuples copied.
        byte[] fromKey = null; // updated after each pass.
        final byte[] toKey = null; // upper bound is fixed.
        
        while (true) {

            // Build result set from the unisolated view.
            final ResultSet rset = new ResultSet(src, fromKey, toKey,
                    chunkSize/* capacity */, IRangeQuery.ALL, null/* filter */);

            // Copy data in result set onto the target index partition.
            targetDataService.submit(ITx.UNISOLATED, targetIndexName,
                    new CopyBufferedWritesProcedure(rset));

            // #of tuples copied.
            ncopied += rset.getNumTuples();

            // #of passes
            nchunks++;
            
            if (rset.isExhausted()) {

                log.info("Copied "+ncopied+" tuples in "+nchunks+" chunks");
                
                break;

            }

            // update to the next key to be copied.
            fromKey = rset.successor();
            
        }

        final LocalPartitionMetadata pmd = src.getIndexMetadata()
                .getPartitionMetadata();
        
        final PartitionLocator oldLocator = new PartitionLocator(
                pmd.getPartitionId(),//
                resourceManager.getDataServiceUUIDs(),//
                pmd.getLeftSeparatorKey(),//
                pmd.getRightSeparatorKey()//
                );
        
        final PartitionLocator newLocator = new PartitionLocator(
                moveResult.newPartitionId,//
                new UUID[]{moveResult.targetDataServiceUUID},//
                pmd.getLeftSeparatorKey(),//
                pmd.getRightSeparatorKey()//
                );
        
        log.info("Updating metadata index: name=" + scaleOutIndexName
                + ", oldLocator=" + oldLocator + ", newLocator=" + newLocator);

        resourceManager.getMetadataService().moveIndexPartition(
                scaleOutIndexName, oldLocator, newLocator);
        
        return ncopied;
        
    }

    /**
     * Task copy data described in a {@link ResultSet} onto the target index
     * partition. This task is invoked by the
     * {@link UpdateMoveIndexPartitionTask} while the latter holds an exclusive
     * lock on the source index partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CopyBufferedWritesProcedure implements IIndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = -7715086561289117891L;

        private ResultSet rset;
        
        /**
         * De-serialization ctor.
         */
        public CopyBufferedWritesProcedure() {
            
        }

        public CopyBufferedWritesProcedure(ResultSet rset) {
        
            if(rset==null) throw new IllegalArgumentException();
            
            this.rset = rset;
            
        }
        
        public Object apply(IIndex ndx) {
            
            assert rset != null;
         
            /*
             * Get hold of the BTree that is currently absorbing writes for the
             * target index partition.
             * 
             * Note: Since the target index partition was recently create the
             * _odds_ are that it is just a BTree on the live journal for the
             * target data service. However, if the target data service has
             * undergone a concurrent overflow then the target index partition
             * COULD be a FusedView.
             * 
             * In either case, we want the mutable BTree that is absorbing
             * writes and we copy the data from the source index partition view
             * to the target index partition view.
             */
            final BTree dst = (BTree) ((ndx instanceof AbstractBTree) ? ndx
                    : ((FusedView) ndx).getSources()[0]);
            
            final int n = rset.getNumTuples();
            
            final boolean deleteMarkers = ndx.getIndexMetadata().getDeleteMarkers();
            
            final boolean versionTimestamps = ndx.getIndexMetadata().getVersionTimestamps();
            
            for(int i=0; i<n; i++) {
                
                final byte[] key = rset.getKeys()[i];
                
                if (versionTimestamps) {

                    final long timestamp = rset.getVersionTimestamps()[i];

                    if (deleteMarkers && rset.getDeleteMarkers()[i]==0?false:true) {

                        dst.insert(key, null/* value */, true/* delete */,
                                timestamp, null/* tuple */);

                    } else {

                        dst.insert(key, rset.getValues()[i],
                                        false/* delete */, timestamp, null/* tuple */);

                    }

                } else {

                    if (deleteMarkers && rset.getDeleteMarkers()[i]==0?false:true) {

                        dst.remove(key);

                    } else {

                        dst.insert(key, rset.getValues()[i]);

                    }

                }
                
            }
            
            return null;
            
        }
        
    }
    
}
