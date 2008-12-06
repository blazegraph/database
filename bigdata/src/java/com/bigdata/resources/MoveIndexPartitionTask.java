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
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.RawDataServiceTupleIterator;

/**
 * Task moves an index partition to another {@link IDataService}.
 * <p>
 * This task runs as a historical read operation and copy the view of the index
 * partition as of the lastCommitTime of old journal to another
 * {@link IDataService}. Once that historical view has been copied, this task
 * then submits an {@link AtomicUpdateMoveIndexPartitionTask}. The atomic
 * update is an {@link ITx#UNISOLATED} operation. It is responsible copying any
 * writes buffered for the index partition on the live journal to the target
 * {@link IDataService} and then updating the {@link MetadataIndex}. Once the
 * atomic update task is finished, clients will discover that the source index
 * partition does not exist. When they query the {@link MetadataService} they
 * will discover that the key(-range) is now handled by the new index partition
 * on the target {@link IDataService}.
 * <p>
 * Note: This task is run on the target {@link IDataService} and it copies the
 * data from the source {@link IDataService}. This allows us to use standard
 * {@link IRangeQuery} operations to copy the historical view. However, the
 * {@link AtomicUpdateMoveIndexPartitionTask} is run on the source
 * {@link IDataService} since it needs to obtain an exclusive lock on the index
 * partition that is being moved in order to prevent concurrent writes during
 * the atomic cutover. For the same reason, the
 * {@link AtomicUpdateMoveIndexPartitionTask} can not use standard
 * {@link IRangeQuery} operations. Instead, it initiates a series of data
 * transfers while holding onto the exclusive lock until the target
 * {@link IDataService} has the current state of the index partition. At that
 * point it notifies the {@link IMetadataService} to perform the atomic cutover
 * to the new index partition.
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
public class MoveIndexPartitionTask extends AbstractResourceManagerTask<MoveResult> {
    
    /**
     * Last commit time on the old journal.
     */
    private final long lastCommitTime;

    /**
     * {@link UUID} of the target {@link IDataService} (the one to which the index
     * partition will be moved).
     */
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
    public MoveIndexPartitionTask(//
            final ResourceManager resourceManager,//
            final long lastCommitTime,//
            final String resource, //
            final UUID targetDataServiceUUID//
            ) {

        super(resourceManager, TimestampUtility
                .asHistoricalRead(lastCommitTime), resource);

        if (targetDataServiceUUID == null)
            throw new IllegalArgumentException();

        this.lastCommitTime = lastCommitTime;
        
        this.targetDataServiceUUID = targetDataServiceUUID;
        
        if(resourceManager.getDataServiceUUID().equals(targetDataServiceUUID)) {
            
            throw new IllegalArgumentException("Target must be a different data service");
            
        }
                
    }

    /**
     * Copies the historical writes to the target data service and then issues
     * the atomic update task to copy any buffered writes on the live journal
     * and update the {@link MetadataService}.
     */
    @Override
    protected MoveResult doTask() throws Exception {

        if (resourceManager.isOverflowAllowed())
            throw new IllegalStateException();

        // view of the source index partition.
        final ILocalBTreeView src = (ILocalBTreeView)getIndex(getOnlyResource());
        
        // clone metadata.
        final IndexMetadata newMetadata = src.getIndexMetadata().clone();
        
        // name of the corresponding scale-out index.
        final String scaleOutIndexName = newMetadata.getName();
                
        // obtain new partition identifier from the metadata service (RMI)
        final int newPartitionId = resourceManager.getFederation().getMetadataService()
                .nextPartitionId(scaleOutIndexName);

        // the partition metadata for the source index partition.
        final LocalPartitionMetadata oldpmd = newMetadata.getPartitionMetadata();

        newMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                newPartitionId,//
                oldpmd.getLeftSeparatorKey(),//
                oldpmd.getRightSeparatorKey(),//
                /*
                 * Note: This is [null] to indicate that the resource metadata
                 * needs to be filled in by the target data service when the new
                 * index partition is registered. It will be populated with the
                 * resource metadata description for the live journal on that
                 * data service.
                 */
                null,
                oldpmd.getHistory()+
                "move("+oldpmd.getPartitionId()+"->"+newPartitionId+") "
                ));

        // logging information.
        {
            
            // #of sources in the view (very fast).
            final int sourceCount = src.getSourceCount();
            
            // range count for the view (fast).
            final long rangeCount = src.rangeCount();

            // BTree's directly maintained entry count (very fast).
            final int entryCount = src.getMutableBTree().getEntryCount(); 
            
            final String details = ", entryCount=" + entryCount
                    + ", rangeCount=" + rangeCount + ", sourceCount="
                    + sourceCount;

            log.warn("name=" + getOnlyResource() + ": move("
                    + oldpmd.getPartitionId() + "->" + newPartitionId + ")"
                    + details);
            
        }
        
        // the data service on which we will register the new index partition.
        final IDataService targetDataService = resourceManager
                .getFederation().getDataService(targetDataServiceUUID);

        // the name of the index partition on this data service.
        final String sourceIndexName = getOnlyResource();

        /*
         * The name of the index partition on the target data service.
         * 
         * Note: The index partition is assigned a new partition identifier when
         * it is moved. Hence it is really a new index partition.
         */
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
        if (INFO)
            log
                    .info("Registered new index partition on target data service: targetIndexName="
                            + targetIndexName);

        /*
         * Run procedure on the target data service that will copy data from the
         * old index partition (on this data service) to the new index partition
         * (on the target data service) as of the [lastCommitTime] of the old
         * journal.
         */
        targetDataService
                .submit(ITx.UNISOLATED, targetIndexName,
                        new CopyIndexPartitionProcedure(resourceManager
                                .getDataServiceUUID(), sourceIndexName,
                                lastCommitTime));
        
        /*
         * At this point the historical view as of the [lastCommitTime] has been
         * copied to the target data service.
         * 
         * The MoveResult contains the information that we need to run the
         * atomic move update task which will bring that view up to the current
         * state of the index partition and then atomically switch over to the
         * new index partition.
         */

        final LocalPartitionMetadata pmd = src.getIndexMetadata()
                .getPartitionMetadata();

        final PartitionLocator oldLocator = new PartitionLocator(//
                pmd.getPartitionId(),//
                resourceManager.getDataServiceUUID(),//
                pmd.getLeftSeparatorKey(),//
                pmd.getRightSeparatorKey()//
        );

        final PartitionLocator newLocator = new PartitionLocator(
                newPartitionId,//
                targetDataServiceUUID,//
                pmd.getLeftSeparatorKey(),//
                pmd.getRightSeparatorKey()//
        );

        final MoveResult moveResult = new MoveResult(sourceIndexName, src
                .getIndexMetadata(), targetDataServiceUUID, newPartitionId,
                oldLocator, newLocator);

        final AbstractTask<Long> task = new AtomicUpdateMoveIndexPartitionTask(
                resourceManager, moveResult.name, moveResult);

        /*
         * Submit atomic update task and await completion
         * 
         * @todo config timeout.
         * 
         * @todo If this task (the caller) is interrupted while the atomic
         * update task is running then the atomic update task will not notice
         * the interrupt since it runs in a different thread. This is true for
         * all of the atomic update tasks when we chain them from the task that
         * prepares for the update. This is mainly an issue for responsivness to
         * the timeout since this task can not notice its interrupt (and
         * therefore the post-processing will not terminate) until its atomic
         * update task completes.
         * 
         * @todo If this task fails then we need to examine the metadata service
         * and restore the oldLocator for the partition if it had been updated
         * before the task failed. This correcting action is required for a
         * failed move to have no (long-term) side-effects.
         */

        concurrencyManager.submit(task).get();
        
        log.warn("Successfully moved index partition: source="
                + sourceIndexName + ", target=" + targetIndexName);
        
        return moveResult;
        
    }

    /**
     * Procedure copies data from the old index partition to the new index
     * partition. This runs as an {@link ITx#UNISOLATED} operations on the
     * target {@link IDataService}. It is {@link ITx#UNISOLATED} because it
     * must write on the new index partition.
     * <p>
     * Note: This procedure only copies the view as of the lastCommitTime. It
     * does NOT copy deleted tuples since the historical views for the source
     * index partition will remain on the source data service. In this sense,
     * this operation is much like a compacting merge.
     * <p>
     * Note: This is an {@link IDataServiceAwareProcedure} so it can resolve the
     * {@link UUID} of the source {@link IDataService} and issue requests to
     * that source {@link IDataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CopyIndexPartitionProcedure implements
            IDataServiceAwareProcedure, IIndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = 8024224972159862036L;

        private final UUID sourceDataServiceUUID;
        private final String sourceIndexName;
        private final long lastCommitTime;

        private transient DataService dataService;
        
        public void setDataService(final DataService dataService) {

            if (dataService == null)
                throw new IllegalArgumentException();

            if (this.dataService != null)
                throw new IllegalStateException();

            if(INFO)
                log.info("Set dataService: " + dataService);

            this.dataService = dataService;

        }

        /**
         * The {@link DataService} on which the procedure is executing.
         */
        final protected DataService getDataService() {

            if (dataService == null)
                throw new IllegalStateException();

            return dataService;
            
        }
        
        public final boolean isReadOnly() {
            
            return false;
            
        }
        
//        /**
//         * De-serialization ctor.
//         */
//        public CopyIndexPartitionProcedure() {
//            
//        }
        
        /**
         * @param sourceDataServiceUUIDs
         * @param sourceIndexName
         * @param lastCommitTime
         * 
         */
        public CopyIndexPartitionProcedure(final UUID sourceDataServiceUUID,
                final String sourceIndexName, final long lastCommitTime) {

            if (sourceDataServiceUUID == null)
                throw new IllegalArgumentException();
            
            if (sourceIndexName == null)
                throw new IllegalArgumentException();
            
            this.sourceDataServiceUUID = sourceDataServiceUUID;
            
            this.sourceIndexName = sourceIndexName; 
            
            this.lastCommitTime = lastCommitTime;
            
         }
        
        /**
         * Copy the non-deleted index entries from the source index view
         * <P>
         * Note: We copy only the non-deleted keys and values since this is the
         * initial state of this view on this data service.
         * <p>
         * Note: we take care to preserve the timestamps in case the index is
         * being used to support transactions.
         * 
         * @return The #of index entries copied.
         * 
         * FIXME Must apply overflowHandler - see AbstractBTree#rangeCopy.
         */
        public Object apply(final IIndex ndx) {

            // the live index on which we are writing.
            final BTree dst = (BTree)ndx;
            
            // @todo handle failover / read from secondaries.
            final IBigdataFederation fed = getDataService().getFederation();
            final IMetadataService metadataService = fed.getMetadataService();
            assert metadataService != null;
            final IDataService sourceDataService = fed.getDataService(sourceDataServiceUUID);
            assert sourceDataService != null;
            
            // iterator reading from the (remote) source index partition.
            final ITupleIterator itr = new RawDataServiceTupleIterator(
                    sourceDataService, //
                    sourceIndexName, //
                    TimestampUtility.asHistoricalRead(lastCommitTime),// Note: historical read.
                    true, // readConsistent,
                    null, // fromKey
                    null, // toKey
                    0,    // capacity
                    IRangeQuery.KEYS | IRangeQuery.VALS,// Note: deleted entries NOT copied!
                    null  // filter
                    );

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

            if(INFO)
                log.info("Copied " + ncopied + " index entries from "
                    + sourceIndexName);
            
            return ncopied;
            
        }

    }

    /**
     * Unisolated task is executed on the source data service once the
     * historical state of an index partitions as of the lastCommitTime of the
     * old journal has been copied to the target data service. The target index
     * partition identified by the {@link MoveResult} MUST already exist and
     * MUST have been populated with the state of the source index partition
     * view as of the lastCommitTime of the old journal. The task copies any
     * writes that have been buffered for the index partition on the source data
     * service to the target data service and then does an atomic update of the
     * {@link MetadataService} while it is holding an exclusive lock on the
     * source index partition.
     * <p>
     * Tasks executing after this one will discover that the source index
     * partition no longer exists as of the timestamp when this task commits.
     * Clients that submit tasks for the source index partition will be notified
     * that it no longer exists. When the client queries the
     * {@link MetadataService} it will discover that the key range has been
     * assigned to a new index partition - the one on the target data service.
     * <p>
     * Note: This task runs as {@link ITx#UNISOLATED} since it MUST have an
     * exclusive lock in order to ensure that the buffered writes are
     * transferred to the target index partition without allowing concurrent
     * writes on the source index partition. This means that the target index
     * partition WILL NOT be able to issue read-requests against the live view
     * of source index partition since this task already holds the necessary
     * lock. Therefore this task instead sends zero or more {@link ResultSet}s
     * containing the buffered writes to the target index partition.
     * <p>
     * Note: The {@link ResultSet} data structure is used since it carries
     * "delete markers" and version timestamps as well as the keys and values.
     * The delete markers MUST be transferred in case the buffered writes
     * include deletes of index entries as of the lastCommitTime on the old
     * journal. If the index supports transactions then the version timestamps
     * MUST be preserved since they are the basis for validation of transaction
     * write sets.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static public class AtomicUpdateMoveIndexPartitionTask extends
            AbstractResourceManagerTask<Long> {

        final private MoveResult moveResult;

        /**
         * @param concurrencyManager
         * @param resource
         *            The source index partition.
         * @param moveResult
         *            The target index partition.
         */
        public AtomicUpdateMoveIndexPartitionTask(
                final ResourceManager resourceManager, //
                final String resource,//
                final MoveResult moveResult //
                ) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if (moveResult == null)
                throw new UnsupportedOperationException();
            
            this.moveResult = moveResult;

        }

        /**
         * @return The #of tuples copied.
         */
        @Override
        protected Long doTask() throws Exception {

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            final IDataService targetDataService = resourceManager
                    .getFederation().getDataService(moveResult.targetDataServiceUUID);
            
            final IIndex src = getIndex(getOnlyResource());

            final String scaleOutIndexName = src.getIndexMetadata().getName();
            
            final String targetIndexName = DataService.getIndexPartitionName(
                    scaleOutIndexName, moveResult.newPartitionId);

            log.warn("Copying buffered writes: targeIndexName="
                    + targetIndexName + ", oldLocator=" + moveResult.oldLocator
                    + ", newLocator=" + moveResult.newLocator);

            int nchunks = 0; // #of passes.
            long ncopied = 0; // #of tuples copied.
            byte[] fromKey = null; // updated after each pass.
            final byte[] toKey = null; // upper bound is fixed.
            final int flags = IRangeQuery.ALL;
            final int capacity = 10000; // chunk size.
            
            while (true) {
                
                /*
                 * Figure out the upper bound on the #of tuples that could be
                 * materialized.
                 * 
                 * Note: the upper bound on the #of key-value pairs in the range is
                 * truncated to an [int].
                 */
                
                final int rangeCount = (int) src.rangeCount(fromKey, toKey);

                final int limit = (rangeCount > capacity ? capacity : rangeCount);

                /*
                 * Iterator reading from the unisolated view of the source index
                 * partition.
                 */
                final ITupleIterator itr = src.rangeIterator(fromKey, toKey, limit,
                        flags, null/*filter*/);
                
                // Build a result set from the iterator.
                final ResultSet rset = new ResultSet(src, capacity, flags, itr);

                /*
                 * Copy data in result set onto the target index partition.
                 * 
                 * Note: This task does not handle re-directs. The target index
                 * partition MUST be on the target data service.
                 */
                targetDataService.submit(ITx.UNISOLATED, targetIndexName,
                        new CopyBufferedWritesProcedure(rset));

                // #of tuples copied.
                ncopied += rset.getNumTuples();

                // #of passes
                nchunks++;
                
                if (rset.isExhausted()) {

                    if(INFO)
                        log.info("Copied "+ncopied+" tuples in "+nchunks+" chunks");
                    
                    break;

                }

                /*
                 * Update to the next key to be copied.
                 * 
                 * @todo there may be a fence post here if the index uses a
                 * fixed length successor semantics for the key. It might be
                 * safer to use [fromKey = rset.getLastKey()]. This would have
                 * the effect the last tuple in each result set being copied
                 * again as the first tuple in the next result set, but that is
                 * of little consequence.
                 */
                fromKey = BytesUtil.successor(rset.getLastKey());
                
            }

            /*
             * Drop the old index partition. This action will be rolled back
             * automatically if this task fails so the source index partition
             * will remain available if the move fails.
             */
            getJournal().dropIndex(getOnlyResource());
            
//            if (INFO)
//                log.info
            log.warn("Updating metadata index: name=" + scaleOutIndexName
                        + ", oldLocator=" + moveResult.oldLocator
                        + ", newLocator=" + moveResult.newLocator);

            // atomic update on the metadata server.
            resourceManager.getFederation().getMetadataService()
                    .moveIndexPartition(scaleOutIndexName,
                            moveResult.oldLocator, moveResult.newLocator);
            
            // will notify tasks that index partition has moved.
            resourceManager.setIndexPartitionGone(getOnlyResource(),
                    StaleLocatorReason.Move);
            
            // notify successful index partition move.
            resourceManager.moveCounter.incrementAndGet();

            /*
             * Note: The new index partition will not be usable until (and
             * unless) this task commits.
             * 
             * @todo Since the metadata service has already been successfully
             * updated we need to rollback the change on the metadata service if
             * the commit fails for this task.  That really needs to be done by
             * the caller
             */
            
            return ncopied;
            
        }
        
    }

    /**
     * Task copies data described in a {@link ResultSet} onto the target index
     * partition. This task is invoked by the
     * {@link AtomicUpdateMoveIndexPartitionTask} while the latter holds an
     * exclusive lock on the source index partition.
     * <p>
     * Note: This task does not handle re-directs. The target index partition
     * MUST be on the target data service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CopyBufferedWritesProcedure implements IIndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = -7715086561289117891L;

        private final ResultSet rset;
        
//        /**
//         * De-serialization ctor.
//         */
//        public CopyBufferedWritesProcedure() {
//            
//        }

        public CopyBufferedWritesProcedure(final ResultSet rset) {
        
            if (rset == null)
                throw new IllegalArgumentException();
            
            this.rset = rset;
            
        }
        
        public final boolean isReadOnly() {
            
            return false;
            
        }
        
        public Object apply(final IIndex ndx) {
            
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
      
            if(ndx.getIndexMetadata().getOverflowHandler()!=null) {

                /*
                 * FIXME Must apply overflowHandler - see AbstractBTree#rangeCopy.
                 * 
                 * Probably the easiest way to handle this is to do an
                 * UNISOLATED build on the view, or to combine an build on the
                 * old view (from the lastCommitTime) which runs before the
                 * update task with an UNISOLATED build on the BTree buffering
                 * writes. The send the resulting index segment(s) to the target
                 * data service while holding an exclusive write lock. Once the
                 * target puts those files into place as a view we do the atomic
                 * update thing here. The "send two index segments" version will
                 * have the lowest latency since we can build and send one while
                 * we continue to absorb writes and then send another with just
                 * the buffered writes.
                 */
                throw new UnsupportedOperationException("Must apply overflowHandler");

            }
            
            for (int i = 0; i < n; i++) {
                
                final byte[] key = rset.getKey(i);
                
                if (versionTimestamps) {

                    final long timestamp = rset.getVersionTimestamps()[i];

                    if (deleteMarkers && rset.getDeleteMarkers()[i]==0?false:true) {

                        dst.insert(key, null/* value */, true/* delete */,
                                timestamp, null/* tuple */);

                    } else {

                        dst.insert(key, rset.getValue(i),
                                        false/* delete */, timestamp, null/* tuple */);

                    }

                } else {

                    if (deleteMarkers && rset.getDeleteMarkers()[i]==0?false:true) {

                        dst.remove(key);

                    } else {

                        dst.insert(key, rset.getValue(i));

                    }

                }
                
            }

            return null;
            
        }
        
    }

}
