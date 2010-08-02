package com.bigdata.resources;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ScatterSplitConfiguration;
import com.bigdata.btree.proc.BatchLookup;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.OverflowManager.ResourceScores;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.ndx.ClientIndexView;

/**
 * Task builds an {@link IndexSegment} from the fused view of an index partition
 * as of some historical timestamp and then atomically updates the view (aka a
 * compacting merge).
 * <p>
 * Note: This task may be used after {@link IResourceManager#overflow()} in
 * order to produce a compact view of the index as of the <i>lastCommitTime</i>
 * on the old journal.
 * <p>
 * Note: As its last action, this task submits a
 * {@link AtomicUpdateCompactingMergeTask} which replaces the view with one
 * defined by the current {@link BTree} on the journal and the newly built
 * {@link IndexSegment}.
 * <p>
 * Note: If the task fails, then the generated {@link IndexSegment} will be
 * deleted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CompactingMergeTask extends AbstractPrepareTask<BuildResult> {

    final protected ViewMetadata vmd;

    /**
     * 
     * @param vmd
     *            The {@link ViewMetadata} for the index partition.
     */
    public CompactingMergeTask(final ViewMetadata vmd) {

        super(vmd.resourceManager, TimestampUtility
                .asHistoricalRead(vmd.commitTime), vmd.name);
        
        this.vmd = vmd;
        
    }

    @Override
    protected void clearRefs() {
        
        vmd.clearRef();
        
    }

    /**
     * Build an {@link IndexSegment} from the compacting merge of an index
     * partition.
     * 
     * @return The {@link BuildResult}.
     */
    protected BuildResult doTask() throws Exception {

        final Event e = new Event(resourceManager.getFederation(), 
                new EventResource(vmd.indexMetadata),
                OverflowActionEnum.Merge, vmd.getParams()).start();

        BuildResult buildResult = null;
        try {

            try {

                if (resourceManager.isOverflowAllowed())
                    throw new IllegalStateException();

                /*
                 * Build the index segment.
                 * 
                 * Note: Since this is a compacting merge the view on the old
                 * journal as of the last commit time will be fully captured by
                 * the generated index segment. However, writes buffered by the
                 * live journal WILL NOT be present in that index segment and
                 * the post-condition view will include those writes.
                 */

                // build the index segment.
                buildResult = resourceManager
                        .buildIndexSegment(vmd.name, vmd.getView(),
                                true/* compactingMerge */, vmd.commitTime,
                                null/* fromKey */, null/* toKey */, e);

            } finally {

                /*
                 * Release our hold on the source view - we only needed it when
                 * we did the index segment build.
                 */

                clearRefs();

            }

            if (buildResult.builder.getCheckpoint().length >= resourceManager.nominalShardSize) {

                /*
                 * If sumSegBytes exceeds the threshold, then do a split here.
                 */

                // FIXME reconcile return type and enable post-merge split.
//                return new SplitCompactViewTask(vmd.name, buildResult);
                
            }
            
            /*
             * @todo error handling should be inside of the atomic update task
             * since it has more visibility into the state changes and when we
             * can no longer delete the new index segment.
             */
            try {

                // scale-out index UUID.
                final UUID indexUUID = vmd.indexMetadata.getIndexUUID();

                // submit task and wait for it to complete
                concurrencyManager.submit(
                        new AtomicUpdateCompactingMergeTask(resourceManager,
                                concurrencyManager, vmd.name, indexUUID,
                                buildResult, e.newSubEvent(
                                        OverflowSubtaskEnum.AtomicUpdate, vmd
                                                .getParams()))).get();

// /*
// * Verify that the view was updated. If the atomic update task
//                 * runs correctly then it will replace the IndexMetadata object
//                 * on the mutable BTree with a new view containing only the live
//                 * journal and the new index segment (for a compacting merge).
//                 * We verify that right now to make sure that the state change
//                 * to the BTree was noticed and resulted in a commit before
//                 * returning control to us here.
//                 * 
//                 * @todo comment this out or replicate for the index build task
//                 * also?
//                 */
//                concurrencyManager
//                        .submit(
//                                new VerifyAtomicUpdateTask(resourceManager,
//                                        concurrencyManager, vmd.name,
//                                        indexUUID, result)).get();

            } catch (Throwable t) {

                // make it releasable.
                resourceManager.retentionSetRemove(buildResult.segmentMetadata
                        .getUUID());

                // delete the generated index segment.
                resourceManager
                        .deleteResource(buildResult.segmentMetadata.getUUID(), false/* isJournal */);

                // re-throw the exception
                throw new Exception(t);

            }

            if (resourceManager.compactingMergeWithAfterAction) {

                /*
                 * Consider possible after-actions now that the view is compact.
                 * If any is selected, then it will be executed in the current
                 * thread.
                 */
                final AbstractTask<?> afterActionTask = chooseAfterActionTask();

                if (afterActionTask != null) {

                    afterActionTask.call();
                    
                }
                
            }
            
            return buildResult;

        } finally {

            if (buildResult != null) {

                /*
                 * At this point the index segment was either incorporated into
                 * the new view in a restart safe manner or there was an error.
                 * Either way, we now remove the index segment store's UUID from
                 * the retentionSet so it will be subject to the release policy
                 * of the StoreManager.
                 */
                resourceManager.retentionSetRemove(buildResult.segmentMetadata
                        .getUUID());

            }

            e.end();

        }
        
    }

    /**
     * Now that the index partition is compact, decide if we will take any after
     * action, such as {move, join, split, tailSplit, scatterSplit, etc). All of
     * these operations are much cheaper while the index is compact which is why
     * we do them here.
     * <p>
     * Note: asynchronous overflow processing WILL NOT complete until the
     * CompactingMergeTask is done. This means that we will still be reading
     * from the same journal. As long as we are reading from the same ordered
     * set of resources the lastCommitTime chosen here is somewhat arbitrary.
     * <p>
     * The updated view metadata as of the last commit time on the live journal.
     * 
     * FIXME Concurrent operations can replace the view definition. However,
     * what would not be good is if they changed the set of resources in the
     * view. The AtomicUpdate of the after action task MUST check for this
     * precondition (same set of resources in the view) and abort (and clean up
     * any intermediate files) if the precondition has been violated (no harm is
     * done if we abort, just some lost work).
     * 
     * @todo split + move and friends seem unnecessarily complicated. We can
     *       just move anything that is compact. [Clean up the tasks to remove
     *       this stuff.]
     * 
     * @todo We might be better off running {@link #chooseAfterActionTask()}
     *       from inside of the atomic update and then doing any work there
     *       while we have the lock on the shard. This will prevent any new data
     *       from building up and can help ensure that the preconditions for the
     *       operation remain valid. This might also help simplify the HA
     *       design.
     * 
     * @todo Once we have flow control on writes we can save the DS a lot of
     *       work by not accepting new writes for an index partition when we are
     *       going to compact it, move it, split it, etc.
     */
    private AbstractTask<?> chooseAfterActionTask() {

        final ViewMetadata vmd = new ViewMetadata(resourceManager,
                resourceManager.getLiveJournal().getLastCommitTime(),
                this.vmd.name, resourceManager.getIndexCounters(this.vmd.name));

        /*
         * Scatter split?
         * 
         * Note: Scatter splits are considered before tail splits and normal
         * splits since they can only be taken when there is a single index
         * partition for a scale-out index. The other kinds of splits are used
         * once the index has already been distributed onto the cluster by a
         * scatter split.
         */
        {

            final ScatterSplitConfiguration ssc = vmd.indexMetadata
                    .getScatterSplitConfiguration();
            
            if ( // only a single index partitions?
                (vmd.getIndexPartitionCount() == 1L)//
                // scatter splits enabled for service
                && resourceManager.scatterSplitEnabled//
                // scatter splits enabled for index
                && ssc.isEnabled()//
                // The view is compact (only one segment).
                && vmd.compactView//
                // trigger scatter split before too much data builds up in one place.
                && vmd.getPercentOfSplit() >= ssc.getPercentOfSplitThreshold()
            ) {

                // Target data services for the new index partitions.
                final UUID[] moveTargets = getScatterSplitTargets(ssc);

                if (moveTargets != null) {

                    // #of splits.
                    final int nsplits = ssc.getIndexPartitionCount() == 0//
                            ? (2 * moveTargets.length) // two per data service.
                            : ssc.getIndexPartitionCount()//
                            ;

                    if (log.isInfoEnabled())
                        log.info("will scatter: " + vmd);

                    // scatter split task.
                    return new ScatterSplitTask(vmd, nsplits, moveTargets);

                }
                
            }

        }

        /*
         * Tail split?
         * 
         * Note: We can do a tail split as long as we are "close" to a full
         * index partition. We have an expectation that the head of the split
         * will be over the minimum capacity. While the tail of the split MIGHT
         * be under the minimum capacity, if there are continued heavy writes on
         * the tail then it will should reach the minimum capacity for an index
         * partition by the time the live journal overflows again.
         */
        if (vmd.isTailSplit() && false) {

            /*
             * FIXME The current tailSplit implementation operations against the
             * BTree, NOT the FusedView and NOT the IndexSegment. It needs to be
             * refactored before it can be an after action for a compacting
             * merge.
             * 
             * It is written to identify the separator key based on an
             * examination of the mutable BTree. Once it has the separator key
             * it then does a normal build for each key-range. [@todo It
             * probably should use a compacting merge in order to avoid sharing
             * index segments across shards.]
             */

            if (log.isInfoEnabled())
                log.info("Will tailSpl" + vmd.name);

            return new SplitTailTask(vmd, null/* moveTarget */);
            
        }

        /*
         * Should split?
         * 
         * Note: Split is NOT allowed if the index is currently being moved
         * onto this data service. Split, join, and move are all disallowed
         * until the index partition move is complete since each of them
         * would cause the index partition to become invalidated.
         */
        if (vmd.getPercentOfSplit() > 1.0) {

            if (log.isInfoEnabled())
                log.info("will split  : " + vmd);

            return new SplitIndexPartitionTask(vmd, (UUID) null/* moveTarget */);

        }

        /*
         * Join undercapacity shard (either with local rightSibling or move to
         * join with remote rightSibling).
         * 
         * If the rightSibling of an undercapacity index partition is also local
         * then a {@link JoinIndexPartitionTask} is used to join those index
         * partitions.
         * 
         * If the rightSibling of an undercapacity index partition is remote,
         * then a {@link MoveTask} is created to move the undercapacity index
         * partition to the remove data service.
         * 
         * Note: joins are only considered when the rightSibling of an index
         * partition exists. The last index partition has [rightSeparatorKey ==
         * null] and there is no rightSibling for that index partition.
         * 
         * @todo What kinds of guarantees do we have that a local rightSibling
         * will be around by the time the JoinIndexPartitionTask runs?
         * 
         * @todo This has even more assumptions about [lastCommitTime] than the
         * other tasks. All these tasks need to be reviewed to make sure that
         * there are no gaps created by this refactor. Running these after
         * action tasks while we hold the write lock on the source shard could
         * probably help us to reduce the possibility of any such problems but
         * might require a revisit / refactor / simplification of the tasks.
         * 
         * FIXME Make sure that we are not running compacting merges as part of
         * the split, scatter split and other tasks. Some tasks used to do this
         * in order to have a compact view.
         */
        if (resourceManager.joinsEnabled
                && vmd.pmd.getRightSeparatorKey() != null
                && vmd.getPercentOfSplit() < resourceManager.percentOfJoinThreshold) {

            final String scaleOutIndexName = vmd.indexMetadata.getName();

            final PartitionLocator rightSiblingLocator = getRightSiblingLocator(
                    scaleOutIndexName, vmd.commitTime);

            if (rightSiblingLocator != null) {

                final UUID targetDataServiceUUID = rightSiblingLocator
                        .getDataServiceUUID();

                final String[] resources = new String[2];

                // the underutilized index partition.
                resources[0] = DataService.getIndexPartitionName(
                        scaleOutIndexName, vmd.pmd.getPartitionId());

                // its right sibling (may be local or remote).
                resources[1] = DataService
                        .getIndexPartitionName(scaleOutIndexName,
                                rightSiblingLocator.getPartitionId());

                if (resourceManager.getDataServiceUUID().equals(
                        targetDataServiceUUID)) {

                    /*
                     * JOIN underutilized index partition with its local
                     * rightSibling.
                     * 
                     * Note: This is only joining two index partitions at a
                     * time. It's possible to do more than that if it happens
                     * that N > 2 underutilized sibling index partitions are on
                     * the same data service, but that is a relatively unlikely
                     * combination of events.
                     */

                    if (log.isInfoEnabled())
                        log.info("Will JOIN: " + Arrays.toString(resources));

                    final String rightSiblingName = DataService
                            .getIndexPartitionName(scaleOutIndexName,
                                    rightSiblingLocator.getPartitionId());

                    final ViewMetadata vmd2 = new ViewMetadata(resourceManager,
                            vmd.commitTime, rightSiblingName, resourceManager
                                    .getIndexCounters(rightSiblingName));

                    return new JoinIndexPartitionTask(resourceManager,
                            vmd.commitTime, resources, new ViewMetadata[] {
                                    vmd, vmd2 });

                } else {

                    /*
                     * MOVE underutilized index partition to data service
                     * hosting the right sibling.
                     * 
                     * @todo The decision to join shards is asymmetric (an
                     * undercapacity shard is moved to its rightSibling).
                     * However, it is possible that its rightSibling was also
                     * undercapacity and was either moved to or locally joined
                     * with its rightSibling (in which case its partition
                     * identifier would have been changed). To avoid these edge
                     * cases there could be a global synchronous agreement for
                     * move/join decisions
                     */

                    if (log.isInfoEnabled()) {

                        // get the target service name.
                        String targetDataServiceName;
                        try {
                            targetDataServiceName = resourceManager
                                    .getFederation().getDataService(
                                            targetDataServiceUUID)
                                    .getServiceName();
                        } catch (Throwable t) {
                            targetDataServiceName = targetDataServiceUUID
                                    .toString();
                        }
                        
                        log.info("willMoveToJoinWithRightSibling" + "( "
                                + vmd.name + " -> " + targetDataServiceName //
                                + ", leftSibling=" + resources[0] //
                                + ", rightSibling=" + resources[1] //
                                + ")");
                    }

                    return new MoveTask(vmd, targetDataServiceUUID);

                }

            } // rightSibling != null

        } // if(join)

        /*
         * Move (to shed or redistribute load).
         * 
         * @todo We should prefer to move smaller shards (faster to move) or
         * "hotter" shards (sheds more workload). There should be a way to
         * estimate how much workload will be transferred so we know when we are
         * done.
         * 
         * FIXME We should limit the #of shards that we move in a given period
         * of time to allow both this host and the target host an opportunity to
         * adapt to their new load. [An exception would be if this host was
         * critically overloaded, but that should probably be handled by
         * different logic.]
         */
        ILoadBalancerService loadBalancerService = null;
        if (vmd.getPercentOfSplit() < resourceManager.maximumMovePercentOfSplit
                && resourceManager.maximumMovesPerTarget != 0
                && resourceManager.getLiveJournal().getName2Addr().rangeCount() > resourceManager.minimumActiveIndexPartitions
                && (loadBalancerService = getLoadBalancerService()) != null
                && shouldMove(loadBalancerService)) {

            // the UUID of this data service.
            final UUID sourceServiceUUID = resourceManager.getDataServiceUUID();

            // Obtain UUID of a relatively underutilized data service.
            final UUID targetDataServiceUUID = getMoveTarget(sourceServiceUUID,
                    loadBalancerService);

            if (targetDataServiceUUID != null) {

                if (log.isInfoEnabled()) {

                    // get the target service name.
                    String targetDataServiceName;
                    try {
                        targetDataServiceName = resourceManager
                                .getFederation().getDataService(
                                        targetDataServiceUUID)
                                .getServiceName();
                    } catch (Throwable t) {
                        targetDataServiceName = targetDataServiceUUID
                                .toString();
                    }

                    log.info("willMove" + "( " + vmd.name + " -> "
                            + targetDataServiceName + ")");

                }

                // Move the shard to the target host.
                return new MoveTask(vmd, targetDataServiceUUID);

            }

        }
        
        // No after action was chosen.
        return null;
        
    }

    /**
     * Return the {@link ILoadBalancerService} if it can be discovered.
     * 
     * @return the {@link ILoadBalancerService} if it can be discovered and
     *         otherwise <code>null</code>.
     */
    private ILoadBalancerService getLoadBalancerService() {

        // lookup the load balancer service.
        final ILoadBalancerService loadBalancerService;
        
        try {

            loadBalancerService = resourceManager.getFederation()
                    .getLoadBalancerService();

        } catch (Exception ex) {

            log.warn("Could not discover the load balancer service", ex);

            return null;
            
        }
        
        if (loadBalancerService == null) {

            log.warn("Could not discover the load balancer service");

            return null;
            
        }

        return loadBalancerService;
        
    }

    /**
     * Figure out if this data service is considered to be highly utilized, in
     * which case the DS should shed some index partitions.
     * <p>
     * Note: We consult the load balancer service on this since it is able to
     * put the load of this service into perspective by also considering the
     * load on the other services in the federation.
     * 
     * @param loadBalancerService
     *            The load balancer.
     */
    protected boolean shouldMove(final ILoadBalancerService loadBalancerService) {

        if (loadBalancerService == null)
            throw new IllegalArgumentException();

        // inquire if this service is highly utilized.
        final boolean highlyUtilizedService;
        try {

            final UUID serviceUUID = resourceManager.getDataServiceUUID();
            
            highlyUtilizedService = loadBalancerService
                    .isHighlyUtilizedDataService(serviceUUID);

        } catch (Exception ex) {

            log.warn("Could not determine if this data service is highly utilized");
            
            return false;
            
        }

        if (!highlyUtilizedService) {
            
            if(log.isInfoEnabled())
                log.info("Service is not highly utilized.");
            
            return false;
            
        }

        /*
         * At this point we know that the LBS considers this host and service to
         * be highly utilized (relative to the other hosts and services). If
         * there is evidence of resource exhaustion for critical resources (CPU,
         * RAM, or DIKS) then we will MOVE index partitions in order to shed
         * some load. Otherwise, we will SPLIT hot index partitions in order to
         * increase the potential concurrency of the workload for this service.
         * 
         * Note: CPU is the only fungable resource since things will just slow
         * down if a host has 100% CPU while it can die if it runs out of DISK
         * or RAM (including if it begins to swap heavily).
         * 
         * @todo config options for these triggers.
         */
        final ResourceScores resourceScores = resourceManager.getResourceScores();

        final boolean shouldMove = //
            // heavy CPU utilization.
            (resourceScores.percentCPUTime >= resourceManager.movePercentCpuTimeThreshold) ||
            // swapping heavily.
            (resourceScores.majorPageFaultsPerSec > 20) ||
            // running out of disk (data dir).
            (resourceScores.dataDirBytesFree < Bytes.gigabyte * 5)||
            // running out of disk (tmp dir).
            (resourceScores.dataDirBytesFree < Bytes.gigabyte * .5)
            ;

        return shouldMove;
//        if (shouldMove) {
//
//            return chooseMoves(loadBalancerService);
//            
//        }

//        return chooseHotSplits();
        
    }

    /**
     * Obtain the UUID of some relatively underutilized data service.
     * 
     * FIXME The LBS should interpret the excludedServiceUUID as the source
     * service UUID and then provide a list of those services having an LBS
     * computed service score which is significantly lower than the score for
     * this service. Changing this will break some unit tests (for the LBS
     * behavior).
     */
    private UUID getMoveTarget(final UUID sourceServiceUUID,
            final ILoadBalancerService loadBalancerService) {

        try {

            // request under utilized data service UUIDs (RMI).
            final UUID[] uuids = loadBalancerService.getUnderUtilizedDataServices(//
                    0, // minCount - no lower bound.
                    1, // maxCount - no upper bound.
                    sourceServiceUUID // exclude this data service.
                    );
            
            if (uuids != null && uuids.length > 0) {

                // Found a move target.
                return uuids[0];
                
            }

            // No move target.
            return null;

        } catch (TimeoutException t) {

            log.warn(t.getMessage());

            return null;

        } catch (InterruptedException t) {

            log.warn(t.getMessage());

            return null;

        } catch (Throwable t) {

            log.error("Could not obtain target service UUIDs: ", t);

            return null;

        }

    }

    /**
     * Locate the right sibling for this index partition.
     * <p>
     * Note: default key/val serializers are used.
     * 
     * @return The locator for the right sibling -or- <code>null</code> if no
     *         right sibling could be found (which is an error).
     * 
     * @todo This does not have to be a batch lookup any more. It could use the
     *       {@link ClientIndexView} class.
     */
    private PartitionLocator getRightSiblingLocator(
            final String scaleOutIndexName, final long lastCommitTime) {

        final BatchLookup op = BatchLookupConstructor.INSTANCE.newInstance(
                0/* fromIndex */, 1/* toIndex */, new byte[][] { vmd.pmd
                        .getRightSeparatorKey() }, null/* vals */);

        final ResultBuffer resultBuffer;
        try {
            
            resultBuffer = (ResultBuffer) resourceManager.getFederation()
                    .getMetadataService().submit(
                            TimestampUtility.asHistoricalRead(lastCommitTime),
                            MetadataService
                                    .getMetadataIndexName(scaleOutIndexName),
                            op).get();
            
        } catch (Exception e) {

            log.error("Could not locate rightSiblings: index="
                    + scaleOutIndexName, e);

            return null;

        }
        
        // the locator for the rightSibling.
        return (PartitionLocator) SerializerUtil.deserialize(resultBuffer
                .getValues().get(0));
    }
    
    /**
     * Identify the target data services for the new index partitions.
     * <p>
     * Note that when maxCount is ZERO (0) ALL joined data services will be
     * reported.
     * <p>
     * Note: This makes sure that _this_ data service is included in the array
     * so that we will leave at least one of the post-split index partitions on
     * this data service.
     * 
     * @todo For a system which has been up and running for a while we would be
     *       better off using the LBS reported move targets rather than all
     *       discovered data services. However, for a new federation we are
     *       better off with all discovered data services since there is less
     *       uncertainty about which services will be reported.
     * 
     * @todo move to OverflowManager?
     */
    private UUID[] getScatterSplitTargets(final ScatterSplitConfiguration ssc) {

        final UUID[] a = resourceManager
                .getFederation()
                .getDataServiceUUIDs(
                        ssc.getDataServiceCount()/* maxCount */);

        if (a == null || a.length == 1) {

            if (log.isInfoEnabled())
                log
                        .info("Will not scatter split - insufficient data services discovered.");
            
            // abort scatter split logic.
            return null;
            
        }
        
        final Set<UUID> tmp = new HashSet<UUID>(Arrays.asList(a));

        tmp.add(resourceManager.getDataServiceUUID());

        return tmp.toArray(new UUID[tmp.size()]);

    }
    
//    /**
//     * A paranoia test that verifies that the definition of the view was in fact
//     * updated.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static private class VerifyAtomicUpdateTask extends AbstractTask<Void> {
//
//        protected final ResourceManager resourceManager;
//        
//        final protected BuildResult buildResult;
//        
//        final private Event updateEvent;
//        
//        /**
//         * @param resourceManager
//         * @param concurrencyManager
//         * @param resource
//         * @param buildResult
//         */
//        public VerifyAtomicUpdateTask(ResourceManager resourceManager,
//                IConcurrencyManager concurrencyManager, String resource,
//                UUID indexUUID, BuildResult buildResult, Event updateEvent) {
//
//            super(concurrencyManager, ITx.UNISOLATED, resource);
//
//            if (resourceManager == null)
//                throw new IllegalArgumentException();
//
//            if (buildResult == null)
//                throw new IllegalArgumentException();
//
//            if(!buildResult.compactingMerge)
//                throw new IllegalArgumentException();
//
//            if(!resource.equals(buildResult.name))
//                throw new IllegalArgumentException();
//            
//            if (updateEvent == null)
//                throw new IllegalArgumentException();
//
//            this.resourceManager = resourceManager;
//            
//            this.buildResult = buildResult;
//
//            this.updateEvent = updateEvent;
//            
//        }
//
//        /**
//         * Verify that the update was correctly registered on the mutable
//         * {@link BTree}.
//         * 
//         * @return <code>null</code>
//         */
//        @Override
//        protected Void doTask() throws Exception {
//
//            updateEvent.start();
//            
//            try {
//            
//            if (resourceManager.isOverflowAllowed())
//                throw new IllegalStateException();
//
//            final SegmentMetadata segmentMetadata = buildResult.segmentMetadata;
//
//            // the correct view definition.
//            final IResourceMetadata[] expected = new IResourceMetadata[] {
//                    // the live journal.
//                    getJournal().getResourceMetadata(),
//                    // the newly built index segment.
//                    segmentMetadata
//                    };
//
//            /*
//             * Open the unisolated B+Tree on the live journal that is absorbing
//             * writes and verify the definition of the view.
//             */
//            final ILocalBTreeView view = (ILocalBTreeView) getIndex(getOnlyResource());
//
//            // The live B+Tree.
//            final BTree btree = view.getMutableBTree();
//
//            final LocalPartitionMetadata pmd = btree.getIndexMetadata().getPartitionMetadata();
//            
//            final IResourceMetadata[] actual = pmd.getResources();
//            
//            if (expected.length != actual.length) {
//
//                throw new RuntimeException("expected=" + expected
//                        + ", but actual=" + actual);
//
//            }
//
//            for (int i = 0; i < expected.length; i++) {
//
//                if (!expected[i].equals(actual[i])) {
//
//                    throw new RuntimeException("Differs at index=" + i
//                            + ", expected=" + expected + ", but actual="
//                            + actual);
//
//                }
//                
//            }
//            
//            return null;
//            
//            } finally {
//                
//                updateEvent.end();
//                
//            }
//
//        }
//        
//    }
        
    /**
     * <p>
     * The source view is pre-overflow (the last writes are on the old journal)
     * while the current view is post-overflow (reflects writes made since
     * overflow). What we are doing is replacing the pre-overflow history with
     * an {@link IndexSegment}.
     * </p>
     * 
     * <pre>
     * journal A
     * view={A}
     * ---- sync overflow begins ----
     * create journal B
     * view={B,A}
     * Begin build segment from view={A} (identified by the lastCommitTime)
     * ---- sync overflow ends ----
     * ... build continues ...
     * ... writes against view={B,A}
     * ... index segment S0 complete (based on view={A}).
     * ... 
     * atomic build update task runs: view={B,S0}
     * ... writes continue.
     * </pre>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class AtomicUpdateCompactingMergeTask extends
            AbstractAtomicUpdateTask<Void> {

        private final Event updateEvent;
        
        /**
         * The expected UUID of the scale-out index.
         */
        final protected UUID indexUUID;
        
        final protected BuildResult buildResult;
        
        /**
         * @param resourceManager
         * @param concurrencyManager
         * @param resource
         * @param buildResult
         */
        public AtomicUpdateCompactingMergeTask(ResourceManager resourceManager,
                IConcurrencyManager concurrencyManager, String resource,
                UUID indexUUID, BuildResult buildResult, Event updateEvent) {

            super(resourceManager, ITx.UNISOLATED, resource);

            if (indexUUID == null)
                throw new IllegalArgumentException();

            if (buildResult == null)
                throw new IllegalArgumentException();

            if(!buildResult.compactingMerge)
                throw new IllegalArgumentException();

            if(!resource.equals(buildResult.name))
                throw new IllegalArgumentException();
            
            if (updateEvent == null)
                throw new IllegalArgumentException();

            this.indexUUID = indexUUID;
            
            this.buildResult = buildResult;
            
            this.updateEvent = updateEvent;
            
        }

        /**
         * <p>
         * Atomic update.
         * </p>
         * 
         * @return <code>null</code>
         */
        @Override
        protected Void doTask() throws Exception {
            
            updateEvent.start();

            try {

                if (resourceManager.isOverflowAllowed())
                    throw new IllegalStateException();

                final SegmentMetadata segmentMetadata = buildResult.segmentMetadata;

                if (INFO)
                    log.info("Begin: name=" + getOnlyResource()
                            + ", newSegment=" + segmentMetadata);

                /*
                 * Open the unisolated B+Tree on the live journal that is
                 * absorbing writes. We are going to update its index metadata.
                 * 
                 * Note: I am using AbstractTask#getIndex(String name) so that
                 * the concurrency control logic will notice the changes to the
                 * BTree and cause it to be checkpointed if this task succeeds
                 * normally.
                 */
                final ILocalBTreeView view = (ILocalBTreeView) getIndex(getOnlyResource());

                // make sure that this is the same scale-out index.
                assertSameIndex(indexUUID, view.getMutableBTree());

                if (view instanceof BTree) {

                    /*
                     * Note: there is an expectation that this is not a simple
                     * BTree because this the build task is supposed to be
                     * invoked after an overflow event, and that event should
                     * have re-defined the view to include the BTree on the new
                     * journal plus the historical view.
                     * 
                     * One explanation for finding a simple view here is that
                     * the view was a simple BTree on the old journal and the
                     * data was copied from the old journal into the new journal
                     * and then someone decided to do a build even through a
                     * copy had already been done. However, this is not a very
                     * good explanation since we try to avoid doing a build if
                     * we have already done a copy!
                     */

                    throw new RuntimeException("View is only a B+Tree: name="
                            + buildResult.name + ", pmd="
                            + view.getIndexMetadata().getPartitionMetadata());

                }

                // The live B+Tree.
                final BTree btree = view.getMutableBTree();

                if (INFO)
                    log.info("src=" + getOnlyResource() + ",counter="
                            + view.getCounter().get() + ",checkpoint="
                            + btree.getCheckpoint());

                assert btree != null : "Expecting index: " + getOnlyResource();

                // clone the current metadata record for the live index.
                final IndexMetadata indexMetadata = btree.getIndexMetadata()
                        .clone();

                /*
                 * This is the index partition definition on the live index -
                 * the one that will be replaced with a new view as the result
                 * of this atomic update.
                 */
                final LocalPartitionMetadata currentpmd = indexMetadata
                        .getPartitionMetadata();

                // Check pre-conditions.
                final IResourceMetadata[] currentResources = currentpmd
                        .getResources();
                {

                    if (currentpmd == null) {

                        throw new IllegalStateException(
                                "Not an index partition: " + getOnlyResource());

                    }

                    if (!currentResources[0].getUUID().equals(
                            getJournal().getRootBlockView().getUUID())) {

                        throw new IllegalStateException(
                                "Expecting live journal to be the first resource: "
                                        + currentResources);

                    }

                    /*
                     * Note: I have commented out a bunch of pre-condition tests
                     * that are not valid for histories such as:
                     * 
                     * history=create() register(0) split(0)
                     * copy(entryCount=314)
                     * 
                     * This case arises when there are not enough index entries
                     * written on the journal after a split to warrant a build
                     * so the buffered writes are just copied to the new
                     * journal. The resources in the view are:
                     * 
                     * 1. journal 2. segment
                     * 
                     * And this update will replace the segment.
                     */

                    // // the old journal's resource metadata.
                    // final IResourceMetadata oldJournalMetadata =
                    // oldResources[1];
                    // assert oldJournalMetadata != null;
                    // assert oldJournalMetadata instanceof JournalMetadata :
                    // "name="
                    // + getOnlyResource() + ", old pmd=" + oldpmd
                    // + ", segmentMetadata=" + buildResult.segmentMetadata;
                    //
                    // // live journal must be newer.
                    // assert journal.getRootBlockView().getCreateTime() >
                    // oldJournalMetadata
                    // .getCreateTime();
                    // new index segment build from a view that did not include
                    // data from the live journal.
                    assert segmentMetadata.getCreateTime() < getJournal()
                            .getRootBlockView().getFirstCommitTime() : "segment createTime LT journal 1st commit time"
                            + ": segmentMetadata="
                            + segmentMetadata
                            + ", journal: " + getJournal().getRootBlockView();

                    // if (oldResources.length == 3) {
                    //
                    // // the old index segment's resource metadata.
                    // final IResourceMetadata oldSegmentMetadata =
                    // oldResources[2];
                    // assert oldSegmentMetadata != null;
                    // assert oldSegmentMetadata instanceof SegmentMetadata;
                    //
                    // assert oldSegmentMetadata.getCreateTime() <=
                    // oldJournalMetadata
                    // .getCreateTime();
                    //
                    // }

                }

                // new view definition.
                final IResourceMetadata[] newResources = new IResourceMetadata[] {
                // the live journal.
                        getJournal().getResourceMetadata(),
                        // the newly built index segment.
                        segmentMetadata };

                // describe the index partition.
                indexMetadata.setPartitionMetadata(new LocalPartitionMetadata(//
                        currentpmd.getPartitionId(),//
                        currentpmd.getSourcePartitionId(),//
                        currentpmd.getLeftSeparatorKey(),//
                        currentpmd.getRightSeparatorKey(),//
                        newResources, //
                        currentpmd.getIndexPartitionCause()
//                        currentpmd.getHistory()
//                                + OverflowActionEnum.Merge//
//                                + "(lastCommitTime="
//                                + segmentMetadata.getCreateTime()//
//                                + ",btreeEntryCount="
//                                + btree.getEntryCount()//
//                                + ",segmentEntryCount="
//                                + buildResult.builder.getCheckpoint().nentries//
//                                + ",segment="
//                                + segmentMetadata.getUUID()//
//                                + ",counter="
//                                + btree.getCounter().get()//
//                                + ",oldResources="
//                                + Arrays.toString(currentResources) + ") "
                ));

                // update the metadata associated with the btree
                btree.setIndexMetadata(indexMetadata);

                if (INFO)
                    log.info("Updated view: name=" + getOnlyResource()
                            + ", pmd=" + indexMetadata.getPartitionMetadata());

                /*
                 * Verify that the btree recognizes that it needs to be
                 * checkpointed.
                 * 
                 * Note: The atomic commit point is when this task commits.
                 */
                assert btree.needsCheckpoint();
                //            btree.writeCheckpoint();
                //            {
                //                final long id0 = btree.getCounter().get();
                //                final long pid = id0 >> 32;
                //                final long mask = 0xffffffffL;
                //                final int ctr = (int) (id0 & mask);
                //                log.warn("name="+getOnlyResource()+", counter="+id0+", pid="+pid+", ctr="+ctr);
                //            }

                // notify successful index partition build.
                resourceManager.overflowCounters.indexPartitionMergeCounter.incrementAndGet();

                return null;

            } finally {

                updateEvent.end();

            }

        } // doTask()

    } // class AtomicUpdate

}
