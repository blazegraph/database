package com.bigdata.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IJoinHandler;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.DataService;

/**
 * Examine the named indices defined on the journal identified by the
 * <i>lastCommitTime</i> and, for each named index registered on that journal,
 * determines which of the following conditions applies and then schedules any
 * necessary tasks based on that decision:
 * <ul>
 * <li>Build a new {@link IndexSegment} for an existing index partition - this
 * is essentially a compacting merge (build).</li>
 * <li>Split an index partition into N index partitions (overflow).</li>
 * <li>Join N index partitions into a single index partition (underflow).</li>
 * <li>Move an index partition to another data service (redistribution). The
 * decision here is made on the basis of (a) underutilized nodes elsewhere; and
 * (b) overutilization of this node.</li>
 * <li>Nothing. This option is selected when (a) synchronous overflow
 * processing choose to copy the index entries from the old journal onto the new
 * journal (this is cheaper when the index has not absorbed many writes); and
 * (b) the index partition is not identified as the source for a move.</li>
 * </ul>
 * <p>
 * Processing is divided into three stages:
 * <dl>
 * <dt>{@link #chooseTasks()}</dt>
 * <dd>This stage examines the named indices and decides what action (if any)
 * will be applied to each index partition.</dd>
 * <dt>{@link #runTasks()}</dt>
 * <dd>This stage reads on the historical state of the named index partitions,
 * building, splitting, joining, or moving their data as appropriate.</dd>
 * <dt>{@link #atomicUpdates()}</dt>
 * <dd>This stage runs tasks using {@link ITx#UNISOLATED} operations on the
 * live journal to make atomic updates to the index partition definitions and to
 * the {@link MetadataIndex} and/or a remote data service where necessary</dd>
 * </dl>
 * <p>
 * Note: This task is invoked after an
 * {@link ResourceManager#overflow(boolean, WriteExecutorService)}. It is run
 * on the {@link ResourceManager#service} so that its execution is asynchronous
 * with respect to the {@link IConcurrencyManager}. While it does not require
 * any locks for some of its processing stages, this task relies on the
 * {@link ResourceManager#overflowAllowed} flag to disallow additional overflow
 * operations until it has completed. The various actions taken by this task are
 * submitted as submits tasks to the {@link IConcurrencyManager} so that they
 * will obtain the appropriate locks as necessary on the named indices.
 * 
 * FIXME Moving index partitions around:
 * <p>
 * Consider failover vs moving index partitions around vs load-balancing.
 * <p>
 * In failover, we have a chain of data services that are replicating some index
 * partitions at the media level - using binary images of the journals and the
 * index segments. The failover chain can also support load-balancing of queries
 * by reading from any data service in the failover chain, not just the primary
 * data service. When a data service fails (assuming a hard failure of the
 * machine) we automatically failover the primary to the first secondary data
 * service and recruit another data service to re-populate the failover chain in
 * order to maintain the desired replication count.
 * <P>
 * When we move index partitions around we are distributing them in order to
 * make the use of CPU, RAM and DISK resources more even across the federation.
 * This is most important when a scale-out index is relatively new as we need to
 * distribute the index in order to realize performance benefits and smooth out
 * the index performance. This is also important when some index partitions are
 * so hot that they need to be wired into RAM. Finally, this is important when a
 * host is overloaded, especially when it is approaching resource exhaustion.
 * However, the fastest way to obtain more disk space on a host is to be more
 * aggressive in releasing old resources. Moving the current state of an index
 * partition does not release ANY DISK space on the host since only the current
 * state of the index partition is moved, not its historical states (which are
 * on a mixture of journals and index segments).
 * 
 * FIXME write join task. The task identifies and joins index partitions local
 * to a given resource manager and then notifies the metadata index of the index
 * partition join (atomic operation, just like split). In addition, a process
 * can scan the metadata index for 2 or more sibling index partitions that are
 * under capacity and MOVE them onto a common data service, at which point the
 * resource manager on that data service will cause them to be joined.
 * 
 * FIXME write move task. it runs on a data service and sends the resources for
 * the current view of an index partition to another data service. the index
 * segments are copied as a binary image. the live btree is copied during an
 * unisolated operation which then updates the metadata index with the new
 * locator. the outcome is an atomic index partition move. overflow processing
 * should be suspended during the move so that the resources defining the index
 * partition do not change during the move, so basically this happens at the
 * same time as splits or joins. any given index partition can only participate
 * in one of these three operations during post-procesing for a given overflow -
 * split, join, or move. the target data service for a bottom-up move (to
 * support distribution of index partitions) should be selected using a round
 * robin over the under-utilized data services (maybe divide into low, medium,
 * high, and urgent utilization categories).
 * <p>
 * Note that it makes sense to move a "hot" index partition onto under utilized
 * hosts, but that we will lock up the index partition briefly for writers
 * during the move. Moving a "cold" index partition away from a data service
 * will have basically no effect on the CPU/RAM/IO/DISK profile of that host.
 * However, moving a "warm" index partition away would release those resources
 * to the remaining partitions and so might be a better choice than moving a hot
 * partition.
 * 
 * @todo another driver for moves is "urgent" overutilization, where we are CPU
 *       or RAM bound. "urgent" disk space overutilization would be handled by
 *       deleting older resources (pruning history agressibly and aborting older
 *       transactions as necessary). overutilization of disk IO would be highly
 *       correlated with CPU utilization. overutilization of network resources
 *       might be harder to diagnose since it is less of a local phenomenon.
 * 
 * @todo write unit tests for all of these operations in which we verify that
 *       the index remains correct against a ground truth index.
 * 
 * @todo write unit tests for these operations where we guide moves and joins
 *       top-down. top-down behavior could be customized by placing a field on
 *       the {@link MetadataIndexMetadata} record.
 * 
 * @todo the client (or the data services?) should send as async message every N
 *       seconds providing a histogram of the partitions they have touched (this
 *       could be input to the load balanced as well as info about the partition
 *       use that would inform load balancing decisions).
 * 
 * FIXME edit above javadoc on resource utilization vs load balancing vs
 * failover and move to a useful location.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PostProcessOldJournalTask implements Callable<Object> {

    /**
     * 
     */
    private final ResourceManager resourceManager;

    private final long lastCommitTime;

    private IRawStore tmpStore;
    
    /**`
     * 
     * @param resourceManager
     * @param lastCommitTime
     *            The lastCommitTime on the old journal.
     */
    public PostProcessOldJournalTask(ResourceManager resourceManager, long lastCommitTime) {

        if(resourceManager==null) throw new IllegalArgumentException();
        
        this.resourceManager = resourceManager;

        assert lastCommitTime > 0L;
        
        this.lastCommitTime = lastCommitTime;

    }
    
    /**
     * Process each named index on the old journal.
     * 
     * @todo we need to choose when to join index partitions. This requires an
     *       vantage from which we can see index partitions that are
     *       undercapacity. if their sibling is also undercapacity then we join
     *       them. otherwise we move the undercapacity index partition to the
     *       node where either its left or right sibling is found so that is can
     *       be joined into one of its siblings.
     * 
     * @todo if this is a high utilization node and there are low utilization
     *       nodes then we move some index partitions to those nodes in order to
     *       improve the distribution of resources across the federation. it's
     *       probably best to shed "warm" index partitions since we will have to
     *       suspend writes on the index partition during the atomic stage of
     *       the move. we need access to some outside context in order to make
     *       this decision.
     * 
     * @see #atomicUpdates()
     */
    protected List<AbstractTask> chooseTasks() throws Exception {

        ResourceManager.log.info("begin: lastCommitTime=" + lastCommitTime);

        // the old journal.
        final AbstractJournal oldJournal = resourceManager
                .getJournal(lastCommitTime);

        // tasks to be created (capacity of the array is estimated).
        final List<AbstractTask> tasks = new ArrayList<AbstractTask>((int)oldJournal
                .getName2Addr().rangeCount(null, null));

        /*
         * Map of the under capacity index partitions for each scale-out index
         * having index partitions on the journal. Keys are the name of the
         * scale-out index. The values are B+Trees. There is an entry for each
         * scale-out index having index partitions on the journal.
         * 
         * Each B+Tree is sort of like a metadata index - its keys are the
         * leftSeparator of the index partition but its values are the
         * LocalPartitionMetadata records for the index partitions found on the
         * journal.
         * 
         * This map is populated during the scan of the named indices with index
         * partitions that are "undercapacity"
         */
        final Map<String, BTree> undercapacityIndexPartitions = new HashMap<String, BTree>();
        {

            // counters : must sum to ndone as post-condition.
            int ndone  = 0; // for each named index we process
            int nskip  = 0; // nothing.
            int nbuild = 0; // build task.
            int nsplit = 0; // split task.
            int njoin  = 0; // join task _candidate_.

            final ITupleIterator itr = oldJournal.getName2Addr().rangeIterator(
                    null, null);

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final Entry entry = EntrySerializer.INSTANCE
                        .deserialize(new DataInputBuffer(tuple.getValue()));

                // the name of an index to consider.
                final String name = entry.name;

                /*
                 * Check the index segment build threshold. If this threshold is
                 * not met then we presume that the data was already copied onto
                 * the live journal. In this case we DO NOT build an index
                 * segment from the old view. Likewise, we will NOT choose
                 * either a split or join for this index partition. However, it
                 * MAY be nominated for a move based on utilization.
                 */
                final boolean didCopy;
                {

                    // get the B+Tree on the old journal.
                    final BTree oldBTree = oldJournal.getIndex(entry.addr);

                    final int entryCount = oldBTree.getEntryCount();

                    // @todo write test directly on this fence post!
                    didCopy = entryCount < resourceManager.indexSegmentBuildThreshold;

                    if (!didCopy) {

                        ResourceManager.log
                                .warn("Will not build index segment: name="
                                        + name
                                        + ", entryCount="
                                        + entryCount
                                        + ", threshold="
                                        + resourceManager.indexSegmentBuildThreshold);

                        nskip++;

                    }

                }

                if (!didCopy) {

                    // historical view of that index at that time.
                    final IIndex view = resourceManager.getIndexOnStore(name,
                            lastCommitTime, oldJournal);

                    if (view == null) {

                        throw new AssertionError(
                                "Index not registered on old journal: " + name
                                        + ", lastCommitTime=" + lastCommitTime);

                    }

                    // index metadata for that index partition.
                    final IndexMetadata indexMetadata = view.getIndexMetadata();

                    // handler decides when and where to split an index
                    // partition.
                    final ISplitHandler splitHandler = indexMetadata
                            .getSplitHandler();

                    // handler decides when to join an index partition.
                    final IJoinHandler joinHandler = indexMetadata
                            .getJoinHandler();

                    if (splitHandler != null && splitHandler.shouldSplit(view)) {

                        /*
                         * Do an index split task.
                         */

                        final AbstractTask task = new SplitIndexPartitionTask(
                                resourceManager, resourceManager
                                        .getConcurrencyManager(),
                                lastCommitTime, name);

                        // add to set of tasks to be run.
                        tasks.add(task);

                        ResourceManager.log.info("index split: "+name);
                        
                        nsplit++;
                        
                    } else if (joinHandler != null
                            && joinHandler.shouldJoin(view)) {

                        /*
                         * Add to the set of index partitions that are
                         * candidates for join operations.
                         * 
                         * Note: If we decide to NOT join this with another
                         * local partition then we MUST do an index segment
                         * build in order to release the dependency on the old
                         * journal. This is handled below when we consider the
                         * JOIN candidates that were discovered in this loop.
                         */

                        final String scaleOutIndexName = indexMetadata.getName();

                        BTree tmp = undercapacityIndexPartitions.get(scaleOutIndexName);

                        if (tmp == null) {

                            tmp = BTree.create(tmpStore, new IndexMetadata(UUID
                                    .randomUUID()));

                            undercapacityIndexPartitions.put(scaleOutIndexName, tmp);

                        }

                        LocalPartitionMetadata pmd = indexMetadata
                                .getPartitionMetadata();

                        tmp.insert(pmd.getLeftSeparatorKey(), pmd);

                        ResourceManager.log.info("join candidate: "+name);
                        
                        njoin++;
                        
                    } else {

                        /*
                         * Just do an index build task.
                         */

                        // the file to be generated.
                        final File outFile = resourceManager
                                .getIndexSegmentFile(indexMetadata);

                        final AbstractTask task = new BuildIndexSegmentTask(
                                resourceManager, resourceManager
                                        .getConcurrencyManager(),
                                lastCommitTime, name, outFile);

                        ResourceManager.log.info("index build: "+name);
                        
                        // add to set of tasks to be run.
                        tasks.add(task);

                        nbuild++;
                        
                    }

                }

                ndone++;
                
            }

            // verify counters.
            assert ndone == nskip + nbuild + nsplit + njoin : "ndone=" + ndone
                    + ", nskip=" + nskip + ", nbuild=" + nbuild + ", nsplit="
                    + nsplit + ", njoin=" + njoin;

        }

        /*
         * Consider the JOIN candidates. If we decide not to join the index
         * partition and not to move the index partition to another data service
         * so that it can be joined there then we will add an index build task
         * so as to release the dependency on the old journal for the current
         * index state.
         */
        {

            Iterator<Map.Entry<String, BTree>> itr = undercapacityIndexPartitions
                    .entrySet().iterator();

            while(itr.hasNext()) {
                
                final Map.Entry<String,BTree> entry = itr.next();
                
                final String scaleOutIndexName = entry.getKey();
                
                ResourceManager.log.info("Considering join candidates: "+scaleOutIndexName);
                
                // keys := leftSeparator; value := LocalPartitionMetadata
                final BTree tmp = entry.getValue();
                
                final ITupleIterator titr = tmp.entryIterator();
                
                while(titr.hasNext()) {
                    
                    ITuple tuple = titr.next();
                    
                    LocalPartitionMetadata pmd = (LocalPartitionMetadata) SerializerUtil.deserialize(tuple.getValue());

                    final List<LocalPartitionMetadata> siblings = new ArrayList<LocalPartitionMetadata>();
                    
                    siblings.add(pmd);
                    
                    ResourceManager.log.info("Looking for siblings: " + pmd);
                                        
                    // while BTree also contains the rightSibling.
                    while(tmp.contains(pmd.getRightSeparatorKey())) {
                        
                        // next tuple is the rightSibling.
                        tuple = titr.next();

                        LocalPartitionMetadata rightSibling = (LocalPartitionMetadata) SerializerUtil
                                .deserialize(tuple.getValue());

                        // verify that we have the right sibling.
                        assert BytesUtil.bytesEqual(pmd.getRightSeparatorKey(),
                                rightSibling.getLeftSeparatorKey());
                    
                        // replace with reference to the rightSibling.
                        pmd = rightSibling;
                        
                        ResourceManager.log.info("Found rightSibling: "+pmd);
                        
                    }

                    if (siblings.size() > 1) {

                        /*
                         * Note: when joining it is possible that we will put so
                         * many siblings together that we trigger an overflow
                         * (split).
                         * 
                         * However, this is quite unlikely if the IJoinHandler
                         * and the ISplitHandler choose their trigger points so
                         * as to leave a comfortable gap in the middle in which
                         * the index can grow or shrink for a while without
                         * triggering either underflow or overflow handling.
                         * 
                         * @todo Nothing enforces the "gap" between overflow and
                         * underflow points. Right now these are just interfaces
                         * (ISplitHandler and IJoinHandler) and there is no
                         * constraint that looks across both interfaces. I would
                         * suggest that this is best handled at a UI layer, but
                         * perhaps these interfaces can be merged into a single
                         * interface and then the implementation object could
                         * check this constraint.
                         */
                        
                        // names of the index resources that we will read.
                        final String[] resources = new String[siblings.size()];
                        
                        for (int i = 0; i < resources.length; i++) {

                            resources[i] = DataService.getIndexPartitionName(
                                    scaleOutIndexName, siblings.get(i)
                                            .getPartitionId());
                            
                        }
                        
                        // the join task.
                        final AbstractTask task = new JoinIndexPartitionTask(
                                resourceManager, resourceManager
                                        .getConcurrencyManager(),
                                lastCommitTime, resources);

                        // add to set of tasks to be run.
                        tasks.add(task);
                        
                    } else {
                        
                        /*
                         * Do a build since we will not do a join.
                         * 
                         * @todo or do a move to another data service where it
                         * can be joined.
                         */

                        // name under which the index partition is registered.
                        final String name = DataService.getIndexPartitionName(
                                scaleOutIndexName, pmd.getPartitionId());
                        
                        // metadata for that index partition.
                        final IndexMetadata indexMetadata = oldJournal
                                .getIndex(name).getIndexMetadata();
                        
                        // the file to be generated.
                        final File outFile = resourceManager
                                .getIndexSegmentFile(indexMetadata);

                        // the build task.
                        final AbstractTask task = new BuildIndexSegmentTask(
                                resourceManager, resourceManager
                                        .getConcurrencyManager(),
                                lastCommitTime,
                                name,
                                outFile);

                        // add to set of tasks to be run.
                        tasks.add(task);
                        
                    }
                    
                }
                
            }
            
        }

        ResourceManager.log.info("end");

        return tasks;
        
    }

    protected List<AbstractTask> runTasks(List<AbstractTask> inputTasks) throws Exception {
        
        ResourceManager.log.info("Will run "+inputTasks.size()+" tasks");

        // submit all tasks, awaiting their completion.
        final List<Future<Object>> futures = resourceManager.getConcurrencyManager()
                .invokeAll(inputTasks);

        final List<AbstractTask> updateTasks = new ArrayList<AbstractTask>(inputTasks.size());
        
        // verify that all tasks completed successfully.
        for (Future<Object> f : futures) {

            /*
             * @todo Is it Ok to trap exception and continue for non-errored
             * source index partitions?
             */
            
            final AbstractResult tmp = (AbstractResult) f.get();

            if (tmp instanceof BuildResult) {

                /*
                 * We ran an index segment build and we update the index
                 * partition metadata now.
                 */

                final BuildResult result = (BuildResult) tmp;

                // task will update the index partition view definition.
                final AbstractTask task = new UpdateIndexPartition(
                        resourceManager, resourceManager.getConcurrencyManager(), result.name,
                        result.segmentMetadata);

                // add to set of tasks to be run.
                updateTasks.add(task);

            } else if (tmp instanceof SplitResult) {

                /*
                 * Now run an UNISOLATED operation that splits the live
                 * index using the same split points, generating new index
                 * partitions with new partition identifiers. The old index
                 * partition is deleted as a post-condition. The new index
                 * partitions are registered as a post-condition.
                 */
                
                final SplitResult result = (SplitResult)tmp;

                final AbstractTask task = new UpdateSplitIndexPartition(
                        resourceManager, resourceManager.getConcurrencyManager(), result.name,
                        result.splits,result.buildResults);

                // add to set of tasks to be run.
                updateTasks.add(task);

            } else if(tmp instanceof JoinResult) {
                
                final JoinResult result = (JoinResult) tmp;
                
                // The array of index names on which we will need an exclusive lock.
                final String[] names = new String[result.oldnames.length+1];
                
                names[0] = result.name;
                
                System.arraycopy(result.oldnames, 0, names, 1, result.oldnames.length);
                
                // The task to make the atomic updates on the live journal and the metadata index.
                final AbstractTask task = new UpdateJoinIndexPartition(
                        resourceManager, resourceManager
                        .getConcurrencyManager(), names, result);

                // add to set of tasks to be run.
                updateTasks.add(task);                
                
            } else {
                
                // FIXME MoveResult
                throw new UnsupportedOperationException();
                
            }

        }

        ResourceManager.log.info("end - ran " + inputTasks.size()
                + " tasks, generated " + updateTasks.size() + " update tasks");

        return updateTasks;
        
    }
    
    /**
     * Run tasks that will cause the live index partition definition to be
     * either updated (for a build task) or replaced (for an index split task).
     * These tasks are also responsible for updating the appropriate
     * {@link MetadataIndex} as required.
     * 
     * @throws Exception
     */
    protected void runUpdateTasks(List<AbstractTask> tasks) throws Exception {

        ResourceManager.log.info("begin");
        
        // submit all tasks, awaiting their completion.
        final List<Future<Object>> futures = resourceManager.getConcurrencyManager()
                .invokeAll(tasks);

        // verify that all tasks completed successfully.
        for (Future<Object> f : futures) {

            /*
             * @todo error handling?
             */
            f.get();

        }
        
        ResourceManager.log.info("end");

    }

    public Object call() throws Exception {

        if (resourceManager.overflowAllowed.get()) {

            // overflow must be disabled while we run this task.
            throw new AssertionError();

        }

        tmpStore = new TemporaryRawStore();
        
        try {

            /*
             * @todo this stages the tasks which provides parallelism within
             * each stage but not between the stages - each stage will run as
             * long as the longest running task in that stage. Consider whether
             * it would be better to use a finer grained parallelism but we need
             * to be able to tell when all stages have been completed so that we
             * can re-enable overflow on the journal.
             */
            
            List<AbstractTask> tasks = chooseTasks();
            
            List<AbstractTask> updateTasks = runTasks(tasks);
            
            runUpdateTasks(updateTasks);

            /*
             * Note: At this point we have the history as of the lastCommitTime
             * entirely contained in index segments. Also, since we constained
             * the resource manager to refuse another overflow until we have
             * handle the old journal, all new writes are on the live index.
             */
            
            return null;

        } finally {

            // enable overflow again as a post-condition.
            if (!resourceManager.overflowAllowed.compareAndSet(false, true)) {

                throw new AssertionError();

            }

            if(tmpStore!=null) {
                
                tmpStore.closeAndDelete();
                
            }
            
        }

    }

}