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
 * Historical read task is used to copy a view of an index partition as of the
 * lastCommitTime of old journal to another {@link IDataService}. After this
 * task has been run you must run an {@link ITx#UNISOLATED}
 * {@link AtomicUpdateMoveIndexPartitionTask} in order to atomically migrate any
 * writes on the live journal to the target {@link IDataService} and update the
 * {@link MetadataIndex}.
 * <p>
 * Note: This task is run on the target {@link IDataService} and it copies the
 * data from the source {@link IDataService}. This allows us to use standard
 * {@link IRangeQuery} operations to copy the historical view. However, the
 * {@link AtomicUpdateMoveIndexPartitionTask} is run on the source
 * {@link IDataService} since it needs to obtain an exclusive lock on the index
 * partition that is being moved in order to prevent concurrent writes during
 * the atomic cutover. For the same reason, the
 * {@link AtomicUpdateMoveIndexPartitionTask} can not use standard {@link IRangeQuery}
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
public class MoveIndexPartitionTask extends AbstractResourceManagerTask {

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
            long lastCommitTime,
            String resource, UUID targetDataServiceUUID) {

        super(resourceManager, -lastCommitTime, resource);

        if (targetDataServiceUUID == null)
            throw new IllegalArgumentException();

        this.lastCommitTime = lastCommitTime;
        
        this.targetDataServiceUUID = targetDataServiceUUID;
        
        if(resourceManager.getDataServiceUUID().equals(targetDataServiceUUID)) {
            
            throw new IllegalArgumentException("Target must be a different data service");
            
        }
                
    }

    @Override
    protected Object doTask() throws Exception {

        if (resourceManager.isOverflowAllowed())
            throw new IllegalStateException();

        // view of the source index partition.
        final IIndex src = getIndex(getOnlyResource());
        
        // clone metadata.
        final IndexMetadata newMetadata = src.getIndexMetadata().clone();
        
        // name of the corresponding scale-out index.
        final String scaleOutIndexName = newMetadata.getName();
                
        // obtain new partition identifier from the metadata service (RMI)
        final int newPartitionId = resourceManager.getFederation().getMetadataService()
                .nextPartitionId(scaleOutIndexName);

        // the partition metadata for the source index partition.
        final LocalPartitionMetadata oldpmd = newMetadata.getPartitionMetadata();

//        newMetadata.setPartitionMetadata(oldpmd.move(//
//                newPartitionId,
//                /*
//                 * Note: This is [null] to indicate that the resource metadata
//                 * needs to be filled in by the target data service when the
//                 * new index partition is registered.
//                 */
//                null
//                ));

        newMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                newPartitionId,//
                oldpmd.getLeftSeparatorKey(),//
                oldpmd.getRightSeparatorKey(),//
                /*
                 * Note: This is [null] to indicate that the resource metadata
                 * needs to be filled in by the target data service when the
                 * new index partition is registered.
                 */
                null,
                oldpmd.getHistory()+
                "move("+oldpmd.getPartitionId()+"->"+newPartitionId+") "
                ));

        
        // the data service on which we will register the new index partition.
        final IDataService targetDataService = resourceManager
                .getFederation().getDataService(targetDataServiceUUID);

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
         * Run procedure on the target data service that will copy data from the
         * old index partition (on this data service) to the new index partition
         * (on the target data service) as of the [lastCommitTime] of the old
         * journal.
         */
        targetDataService.submit(ITx.UNISOLATED, targetIndexName,
                new CopyIndexPartitionProcedure(sourceDataServiceUUIDs,
                        sourceIndexName, lastCommitTime));
        
        /*
         * At this point the historical view as of the [lastCommitTime] has been
         * copied to the target data service.
         * 
         * The MoveResult contains the information that we need to run the
         * atomic move update task which will bring that view up to the current
         * state of the index partition and then atomically switch over to the
         * new index partition.
         */
        
        final MoveResult result = new MoveResult(sourceIndexName, src
                .getIndexMetadata(), targetDataServiceUUID, newPartitionId);
        
        final AbstractTask task = new AtomicUpdateMoveIndexPartitionTask(
                resourceManager, result.name, result);

        // submit atomic update and await completion @todo config timeout.
        concurrencyManager.submit(task).get();
        
        return result;
        
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
        
        public String toString() {
            
            return "MoveResult{name=" + name + ", newPartitionId="
                    + newPartitionId + ", targetDataService="
                    + targetDataServiceUUID + "}";
            
        }


    }

    /**
     * Procedure copies data from the old index partition to the new index
     * partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class CopyIndexPartitionProcedure implements IDataServiceAwareProcedure, IIndexProcedure {

        /**
         * 
         */
        private static final long serialVersionUID = 8024224972159862036L;

        private UUID[] sourceDataServiceUUIDs;
        private String sourceIndexName;
        private long lastCommitTime;

        private transient DataService dataService;
        
        public void setDataService(DataService dataService) {

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
                String sourceIndexName, long lastCommitTime) {

            if (sourceDataServiceUUIDs == null) {

                throw new IllegalArgumentException();

            }

            if (sourceDataServiceUUIDs.length == 0) {

                throw new IllegalArgumentException();

            }

            for(int i=0; i<sourceDataServiceUUIDs.length; i++) {
                
                if(sourceDataServiceUUIDs[i]==null) {
                    
                    throw new IllegalArgumentException();
                    
                }
                
            }
            
            if (sourceIndexName == null) {

                throw new IllegalArgumentException();
                
            }
            
            this.sourceDataServiceUUIDs = sourceDataServiceUUIDs;
            
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
        public Object apply(IIndex ndx) {

            // the live index on which we are writing.
            final BTree dst = (BTree)ndx;
            
            // @todo handle failover / read from secondaries.
            final IBigdataFederation fed = getDataService().getFederation();
            final IMetadataService metadataService = fed.getMetadataService();
            assert metadataService != null;
            final IDataService sourceDataService = fed.getDataService(sourceDataServiceUUIDs[0]);
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
                    IRangeQuery.KEYS | IRangeQuery.VALS,//
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
    static public class AtomicUpdateMoveIndexPartitionTask extends AbstractResourceManagerTask {

        final private MoveResult moveResult;

        /**
         * @param concurrencyManager
         * @param resource
         *            The source index partition.
         * @param moveResult
         *            The target index partition.
         */
        public AtomicUpdateMoveIndexPartitionTask(ResourceManager resourceManager,
                String resource, MoveResult moveResult) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if (moveResult == null)
                throw new UnsupportedOperationException();
            
            this.moveResult = moveResult;

        }

        /**
         * @return The #of tuples copied.
         */
        @Override
        protected Object doTask() throws Exception {

            if (resourceManager.isOverflowAllowed())
                throw new IllegalStateException();

            final IDataService targetDataService = resourceManager
                    .getFederation().getDataService(moveResult.targetDataServiceUUID);
            
            final int capacity = 10000;
            
            final IIndex src = getIndex(getOnlyResource());

            final String scaleOutIndexName = src.getIndexMetadata().getName();
            
            final String targetIndexName = DataService.getIndexPartitionName(
                    scaleOutIndexName, moveResult.newPartitionId);

            int nchunks = 0; // #of passes.
            long ncopied = 0; // #of tuples copied.
            byte[] fromKey = null; // updated after each pass.
            final byte[] toKey = null; // upper bound is fixed.
            final int flags = IRangeQuery.ALL;
            
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

                // iterator reading from the unisolated view.
                final ITupleIterator itr = src.rangeIterator(fromKey, toKey, limit,
                        flags, null/*filter*/);
                
                // Build result set from the iterator.
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
                 */
                fromKey = BytesUtil.successor(rset.getLastKey());
                
            }

            // drop the old index partition.
            getJournal().dropIndex(getOnlyResource());
            
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
            
            if(INFO)
                log.info("Updating metadata index: name=" + scaleOutIndexName
                    + ", oldLocator=" + oldLocator + ", newLocator=" + newLocator);

            // atomic update on the metadata server.
            resourceManager.getFederation().getMetadataService().moveIndexPartition(
                    scaleOutIndexName, oldLocator, newLocator);
            
            // will notify tasks that index partition has moved.
            resourceManager.setIndexPartitionGone(getOnlyResource(),
                    StaleLocatorReason.Move);
            
            // notify successful index partition move.
            resourceManager.moveCounter.incrementAndGet();

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

        private ResultSet rset;
        
        /**
         * De-serialization ctor.
         */
        public CopyBufferedWritesProcedure() {
            
        }

        public CopyBufferedWritesProcedure(ResultSet rset) {
        
            if (rset == null)
                throw new IllegalArgumentException();
            
            this.rset = rset;
            
        }
        
        public final boolean isReadOnly() {
            
            return false;
            
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
            
            for(int i=0; i<n; i++) {
                
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
