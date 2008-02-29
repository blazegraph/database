package com.bigdata.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.service.IMetadataService;

/**
 * Examine the named indices defined on the journal identified by the
 * <i>lastCommitTime</i> and, for each named index registered on that journal,
 * decides on and executes one of the following actions:
 * <ul>
 * <li>Nothing (either no data or the data was already copied onto the live
 * journal rather than doing an index segment build).</li>
 * <li>Build a new {@link IndexSegment} for an existing index partition
 * (build).</li>
 * <li>Split an index partition into N index partitions (overflow).</li>
 * <li>Join N index partitions into a single index partition (underflow).</li>
 * <li>Move an index partition to another data service (redistribution).</li>
 * </ul>
 * <p>
 * Processing is divided into three stages:
 * <dl>
 * <dt>decision making</dt>
 * <dd>This stage examines the named indices and decides what action (if any)
 * will be applied to each index partition.</dd>
 * <dt>{@link #chooseTasks()}</dt>
 * <dd>This stage reads on the historical state of the named index partitions,
 * building, splitting, joining, or moving their data as appropriate.</dd>
 * <dt>{@link #atomicUpdates()}</dt>
 * <dd>This stage uses {@link ITx#UNISOLATED} operations on the live journal to
 * make atomic updates to the index partition definitions and to the
 * {@link MetadataIndex} and/or a remote data service where necessary</dd>
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

    /**
     * A queue of results that are ready to be integrated into the current view
     * of index partitions.
     * 
     * @todo We could directly submit tasks to the {@link IConcurrencyManager}
     *       rather than building them up on this queue. However, it seems that
     *       it might be better to have the views cut over all-at-once from the
     *       old view definitions to the new view definitions (including on the
     *       metadata index), in which case gathering up the results first will
     *       be necessary. It also might give us somewhat better error
     *       reporting. On the other hand, performance should improve as soon as
     *       we put the new view definitions into play, so there is a good
     *       reason to do that eagerly.
     */
    private final BlockingQueue<AbstractResult> resultQueue = new LinkedBlockingQueue<AbstractResult>(/*unbounded*/);
    
    /**
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
    protected void chooseTasks() throws Exception {

        ResourceManager.log.info("begin: lastCommitTime="+lastCommitTime);
        
        int ndone = 0;
        int nskip = 0;
        
        // the old journal.
        final AbstractJournal oldJournal = resourceManager.getJournal(lastCommitTime);
        
        final int nindices = (int) oldJournal.getName2Addr().rangeCount(null,null);
        
        final List<AbstractTask> tasks = new ArrayList<AbstractTask>(
                nindices);

        final ITupleIterator itr = oldJournal.getName2Addr().rangeIterator(null,null);

        while (itr.hasNext()) {

            final ITuple tuple = itr.next();

            final Entry entry = EntrySerializer.INSTANCE
                    .deserialize(new DataInputBuffer(tuple.getValue()));

            // the name of an index to consider.
            final String name = entry.name;

            /*
             * Check the index segment build threshold. If this threshold is
             * not met then we presume that the data was already copied onto
             * the live journal and we DO NOT build an index segment from
             * the old view.
             */
            {
                
                // get the B+Tree on the old journal.
                final BTree oldBTree = oldJournal.getIndex(entry.addr);
                
                final int entryCount = oldBTree.getEntryCount();
                
                final boolean willBuildIndexSegment = entryCount >= resourceManager.indexSegmentBuildThreshold;
                
                if (!willBuildIndexSegment) {

                    ResourceManager.log.warn("Will not build index segment: name=" + name
                            + ", entryCount=" + entryCount + ", threshold="
                            + resourceManager.indexSegmentBuildThreshold);
                    
                    nskip++;
                    
                    continue;
                    
                }
                
            }
            
            // historical view of that index at that time.
            final IIndex view = resourceManager.getIndexOnStore(name, lastCommitTime, oldJournal );

            if(view == null) {
                
                throw new AssertionError(
                        "Index not registered on old journal: " + name
                                + ", lastCommitTime=" + lastCommitTime);
                
            }
            
            // index metadata for that index partition.
            final IndexMetadata indexMetadata = view.getIndexMetadata();

            // optional object that decides whether and when to split the
            // index partition.
            final ISplitHandler splitHandler = indexMetadata
                    .getSplitHandler();

            final AbstractTask task;

            if (splitHandler != null && splitHandler.shouldSplit(view)) {

                /*
                 * Do an index split task.
                 */
                task = new SplitIndexPartitionTask(resourceManager, resourceManager.getConcurrencyManager(),
                        lastCommitTime, name);

            } else {

                /*
                 * Just do an index build task.
                 */
                
                
                // the file to be generated.
                final File outFile = resourceManager.getIndexSegmentFile(indexMetadata);

                task = new BuildIndexSegmentTask(resourceManager, resourceManager.getConcurrencyManager(),
                        lastCommitTime, name, outFile);

            }

            // add to set of tasks to be run.
            tasks.add(task);

            ndone++;
            
        }

        ResourceManager.log.info("Will process "+ndone+" indices");
        
        assert ndone+nskip == nindices : "ndone="+ndone+", nskip="+nskip+", nindices="+nindices;

        // submit all tasks, awaiting their completion.
        final List<Future<Object>> futures = resourceManager.getConcurrencyManager()
                .invokeAll(tasks);

        // verify that all tasks completed successfully.
        for (Future<Object> f : futures) {

            /*
             * @todo Is it Ok to trap exception and continue for non-errored
             * source index partitions?
             */
            final AbstractResult result = (AbstractResult) f.get();
            
            /*
             * Add result to the queue of results to be processed.
             */
            
            resultQueue.put(result);
            
        }
        
        ResourceManager.log.info("end");

    }
    
    /**
     * For each task that was completed, examine the result and create
     * another task which will cause the live index partition definition to
     * be either updated (for a build task) or replaced (for an index split
     * task).
     * <p>
     * The {@link IMetadataService} is updated as a post-condition to
     * reflect the index partition split.
     * 
     * @throws Exception
     */
    protected void atomicUpdates() throws Exception {

        ResourceManager.log.info("begin");
        
        // set of tasks to be run.
        final List<AbstractTask> tasks = new LinkedList<AbstractTask>();

        while (!resultQueue.isEmpty()) {

            final AbstractResult tmp = resultQueue.take();

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
                tasks.add(task);

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
                tasks.add(task);

            }

        }

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

        if(!resultQueue.isEmpty()) {
            
            /*
             * Note: This can happen if the post-processing of the previous
             * journal terminated abnormally.
             */
            
            ResourceManager.log.warn("Result queue not empty: "+resultQueue.size());
            
        }
        
        // clear queue as pre-condition.
        resultQueue.clear();
        
        try {

            chooseTasks();

            
            
            atomicUpdates();

            /*
             * Note: At this point we have the history as of the
             * lastCommitTime entirely contained in index segments. Also,
             * since we constained the resource manager to refuse another
             * overflow until we have handle the old journal, all new writes
             * are on the live index.
             */
            
            return null;

        } finally {

            // enable overflow again as a post-condition.
            if (!resourceManager.overflowAllowed.compareAndSet(false, true)) {

                throw new AssertionError();

            }

        }

    }

}