package com.bigdata.resources;

import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.ScatterSplitConfiguration;
import com.bigdata.btree.proc.BatchLookup;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.proc.BatchLookup.BatchLookupConstructor;
import com.bigdata.btree.view.FusedView;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.resources.StoreManager.IStoreManagerCounters;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.EventType;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.DataService.IDataServiceCounters;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * This class examines the named indices defined on the journal identified by
 * the <i>lastCommitTime</i> and, for each named index registered on that
 * journal, determines which of the following conditions applies and then
 * schedules any necessary tasks based on that decision:
 * <ul>
 * <li>Build a new {@link IndexSegment} from the writes buffered on the prior
 * journal. This is done in order to clear the dependencies on the historical
 * journals. If there are deleted tuples in the buffered writes, then they are
 * propagated to the index segment.</li>
 * <li>Merge all sources in the view for an index partition into a new
 * {@link IndexSegment}. This is a compacting merge. Delete markers will not be
 * present in the generated {@link IndexSegment}.</li>
 * <li>Split an index partition into N index partitions (index partition
 * overflow). Each generated index partition will have a new partition
 * identifier. The old index partition identifier is retired except for
 * historical reads.</li>
 * <li>Join N index partitions into a single index partition (index partition
 * underflow). The join requires that the left- and right-sibling index
 * partitions reside on the same data service. If they do not, then first one
 * must be moved to the data service on which the other resides.</li>
 * <li>Move an index partition to another data service (redistribution). The
 * decision here is made on the basis of (a) underutilized nodes elsewhere; and
 * (b) over utilization of this node.</li>
 * <li>Nothing. This option is selected when (a) synchronous overflow processing
 * choose to copy the index entries from the old journal onto the new journal
 * (this is cheaper when the index has not absorbed many writes); and (b) the
 * index partition is not identified as the source for a move.</li>
 * </ul>
 * Each task has two phases
 * <ol>
 * <li>historical read from the lastCommitTime of the old journal</li>
 * <li>unisolated task performing an atomic update of the index partition view
 * and the metadata index</li>
 * </ol>
 * <p>
 * Processing is divided into two stages:
 * <dl>
 * <dt>{@link #chooseTasks()}</dt>
 * <dd>This stage examines the named indices and decides what action (if any)
 * will be applied to each index partition.</dd>
 * <dt>{@link #runTasks()}</dt>
 * <dd>This stage reads on the historical state of the named index partitions,
 * building, merging, splitting, joining, or moving their data as appropriate.
 * When each task is finished, it submits and awaits the completion of an
 * {@link AbstractAtomicUpdateTask}. The atomic update tasks use
 * {@link ITx#UNISOLATED} operations on the live journal to make atomic updates
 * to the index partition definitions and to the {@link MetadataService} and/or
 * a remote data service where necessary.</dd>
 * </dl>
 * <p>
 * Note: This task is invoked after an {@link ResourceManager#overflow()}. It is
 * run on the {@link ResourceManager}'s {@link ExecutorService} so that its
 * execution is asynchronous with respect to the {@link IConcurrencyManager}.
 * While it does not require any locks for its own processing stages, it relies
 * on the {@link ResourceManager#overflowAllowed} flag to disallow additional
 * overflow operations until it has completed. The various actions taken by this
 * task are submitted to the {@link IConcurrencyManager} so that they will
 * obtain the appropriate locks as necessary on the named indices.
 * 
 * @todo consider side-effects of post-processing tasks (build, split, join, or
 *       move) on a distributed index rebuild operation. It is possible that the
 *       new index partitions may have been defined (but not yet registered in
 *       the metadata index) and that new index resources (on journals or index
 *       segment files) may have been defined. However, none of these operations
 *       should produce incoherent results so it should be possible to restore a
 *       coherent state of the metadata index by picking and choosing carefully.
 *       The biggest danger is choosing a new index partition which does not yet
 *       have all of its state on hand, but we have the
 *       {@link LocalPartitionMetadata#getSourcePartitionId()} which captures
 *       that.
 * 
 * @todo If an index partition is moved (or split or joined) while an active
 *       transaction has a write set for that index partition on a data service
 *       then we need to move/split/join the transaction write set as well so
 *       that it stays aligned with the index partition definitions. In this way
 *       the validate and merge operations may be conducted in parallel for each
 *       index partition which participates in the transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AsynchronousOverflowTask implements Callable<Object> {

    protected static final Logger log = Logger.getLogger(AsynchronousOverflowTask.class);

    /**
     * 
     */
    private final ResourceManager resourceManager;

    private final OverflowMetadata overflowMetadata;
    
    private final long lastCommitTime;

    /**
     * Indices that have already been handled.
     * <p>
     * Note: An index that has been copied because its write set on the old
     * journal was small should not undergo an incremental build since we have
     * already captured the writes from the old journal. However, it MAY be used
     * in other operations (merge, join, split, etc). So we DO NOT add the
     * "copied" indices to the "used" set in the ctor.
     * <p>
     * Note: If a copy was performed AND we also perform another overflow action
     * then the overflow action DOES NOT read from the post-copy view. Instead,
     * it reads from the lastCommitTime view on the old journal and then
     * performs an atomic update to "catch up" with any writes on the index
     * partition on the live journal. This is still coherent.
     * <p>
     * Note: The {@link TreeMap} imposes an alpha order which is useful when
     * debugging.
     * 
     * @todo This is mostly redundant with
     *       {@link OverflowMetadata#getAction(String)}.
     *       <p>
     *       There are some additional semantics here in terms of how we treat
     *       indices that were copied over during synchronous overflow. If these
     *       are reconciled, then those additional semantics need to be captured
     *       as well.
     *       <p>
     *       The value here is pretty much action+"("+vmd+")".
     */
    private final Map<String, String> used = new TreeMap<String, String>();

    /**
     * Return <code>true</code> if the named index partition has already been
     * "used" by assigning it to participate in some build, split, join, or move
     * operation.
     * 
     * @param name
     *            The name of the index partition.
     */
    protected boolean isUsed(final String name) {
        
        if (name == null)
            throw new IllegalArgumentException();

        return used.containsKey(name);
        
    }
    
    /**
     * This method is invoked each time an index partition is "used" by
     * assigning it to participate in some build, split, join, or move
     * operation.
     * 
     * @param name
     *            The name of the index partition.
     * 
     * @throws IllegalStateException
     *             if the index partition was already used by some other
     *             operation.
     * 
     * @todo could be replaced by index on {@link BTreeMetadata} in the
     *       {@link OverflowMetadata} object and {@link BTreeMetadata#action}
     */
    protected void putUsed(final String name, final String action) {
        
        if (name == null)
            throw new IllegalArgumentException();

        if (action == null)
            throw new IllegalArgumentException();
        
        if(used.containsKey(name)) {
            
            throw new IllegalStateException("Already used: "+name);
            
        }
        
        used.put(name,action);
        
    }
    
    /**
     * Return the value of a host counter.
     * 
     * @param path
     *            The path (relative to the host root).
     * @param defaultValue
     *            The default value to use if the counter was not found.
     *            
     * @return The value if found and otherwise the defaultValue.
     */
    protected double getHostCounter(final String path, final double defaultValue) {

        final AbstractFederation fed = (AbstractFederation) resourceManager
                .getFederation();

        final ICounterSet hostRoot = fed.getHostCounterSet();

        if (hostRoot == null) {

            /*
             * Log warning but continue since may be executing before counters
             * were reported or in a test harness.
             */

            log.warn("Host counters not available?");

            return defaultValue;

        }

        final ICounter c = (ICounter) hostRoot.getPath(path);

        if (c != null) {

            return ((Number) c.getInstrument().getValue()).doubleValue();

        }

        /*
         * Log warning but continue since may be executing before counters were
         * reported or in a test harness.
         */
        log.warn("Host counter not found? " + path);

        return defaultValue;

    }

    /**
     * Return the value of a service counter.
     * 
     * @param path
     *            The path (relative to the service root).
     * @param defaultValue
     *            The default value to use if the counter was not found.
     * 
     * @return The value if found and otherwise the defaultValue.
     */
    protected double getServiceCounter(final String path,
            final double defaultValue) {

        final AbstractFederation fed = (AbstractFederation) resourceManager
                .getFederation();

        final ICounterSet serviceRoot = fed.getServiceCounterSet();

        if (serviceRoot == null) {

            /*
             * Log warning but continue since may be executing before counters
             * were reported or in a test harness.
             */

            log.warn("Service counters not available?");

            return defaultValue;

        }

        final ICounter c = (ICounter) serviceRoot.getPath(path);

        if (c != null) {

            return ((Number) c.getInstrument().getValue()).doubleValue();

        }

        /*
         * Log warning but continue since may be executing before counters were
         * reported or in a test harness.
         */

        log.warn("Service counter not found? " + path);

        return defaultValue;

    }

    /**
     * 
     * @param resourceManager
     * @param overflowMetadata
     */
    public AsynchronousOverflowTask(final ResourceManager resourceManager,
            final OverflowMetadata overflowMetadata) {

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if (overflowMetadata == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;

        this.overflowMetadata = overflowMetadata;
        
        this.lastCommitTime = overflowMetadata.lastCommitTime;
        
    }

    /**
     * Choose index partitions for scatter split operations. The scatter split
     * divides an index partition into N index partitions, one per data service,
     * and then moves N-1 of the generated index partitions to other data
     * services leaving one index partition in place on this data service.
     */
    protected List<AbstractTask> chooseScatterSplits() {
        
        // set of tasks created.
        final List<AbstractTask> tasks = new LinkedList<AbstractTask>();

        // set of index partition views to consider.
        final Iterator<ViewMetadata> itr = overflowMetadata.views();

        // lazily initialized.
        UUID[] moveTargets = null;

        while(itr.hasNext()) {

            final ViewMetadata vmd = itr.next();
            
            final String name = vmd.name;
            
            /*
             * The index partition has already been handled.
             */
            if (isUsed(name)|| overflowMetadata.isCopied(name)) {

                continue;
                
            }
            
            /*
             * Scatter split.
             * 
             * Note: Split is NOT allowed if the index is currently being moved
             * onto this data service. Split, join, and move are all disallowed
             * until the index partition move is complete since each of them
             * would cause the index partition to become invalidated.
             * 
             * FIXME This can be fooled if there are a more or less even mixture
             * of inserts and deletes on an ongoing basis. It will look like a
             * split based on getPercentOfSplit() but examination by the split
             * handler will see that there are not enough tuples to do a split.
             * 
             * FIXME The scatter split can be fooled in another way for the same
             * use case. It creates an adjusted split handler based on the fast
             * range count. With a lot of deletes mixed in there the actual
             * range count will be much less and a split will be refused since
             * there are not enough tuples to fill a split.
             * 
             * FIXME Both this case and the one mentioned above (which also
             * applies to normal splits) can be observed using the
             * StressTestConcurrent test. One way to handle this might be to
             * move the scatter split inside of the normal split and then to
             * decide how to split once we have done a more detailed inspection.
             * This would break encapsulation on the split handler and require
             * access to the potential move targets, #of index partitions for
             * the scale-out index, etc.
             */

            final ScatterSplitConfiguration ssc = vmd.indexMetadata
                    .getScatterSplitConfiguration();
            
            if ( // only a single index partitions?
                (vmd.getIndexPartitionCount() == 1L)//
                // move not in progress
                && vmd.pmd.getSourcePartitionId() == -1//
                // scatter splits enabled for service
                && resourceManager.scatterSplitEnabled//
                // scatter splits enabled for index
                && ssc.isEnabled()//
                // trigger scatter split before too much data builds up in one place.
                && vmd.getPercentOfSplit() >= ssc.getPercentOfSplitThreshold()
            ) {

                /*
                 * Do a scatter split task.
                 * 
                 * @todo For a system which has been up and running for a while
                 * we would be better off using the LBS reported move targets
                 * rather than all discovered data services. However, for a new
                 * federation we are better off with all discovered data
                 * services since there is less uncertainty about which
                 * services will be reported.
                 */
                // Target data services for the new index partitions.
                if (moveTargets == null) {
                    
                    /*
                     * Identify the target data services for the new index
                     * partitions.
                     * 
                     * Note that when maxCount is ZERO (0) ALL joined data
                     * services will be reported.
                     * 
                     * Note: This makes sure that _this_ data service is
                     * included in the array so that we will leave at least one
                     * of the post-split index partitions on this data service.
                     */

                    final UUID[] a = resourceManager
                            .getFederation()
                            .getDataServiceUUIDs(
                                    ssc.getDataServiceCount()/* maxCount */);

                    if (a == null || a.length == 1) {

                        if (log.isInfoEnabled())
                            log
                                    .info("Will not scatter split - insufficient data services discovered.");
                        
                        // abort scatter split logic.
                        return tasks;
                        
                    }
                    
                    final Set<UUID> tmp = new HashSet<UUID>(Arrays.asList(a));

                    tmp.add(resourceManager.getDataServiceUUID());

                    moveTargets = tmp.toArray(new UUID[tmp.size()]);

                }

                // #of splits.
                final int nsplits = ssc.getIndexPartitionCount() == 0//
                        ? (2 * moveTargets.length) // two per data service.
                        : ssc.getIndexPartitionCount()//
                        ;

                // scatter split task.
                final AbstractTask task = new ScatterSplitTask(vmd, nsplits,
                        moveTargets);

                // add to set of tasks to be run.
                tasks.add(task);

                overflowMetadata.setAction(vmd.name, OverflowActionEnum.Split);

                putUsed(name, "willScatter(name=" + vmd + ")");

                if (log.isInfoEnabled())
                    log.info("will scatter: " + vmd);

                continue;

            }
            
        } // itr.hasNext()
    
        return tasks;
        
    }
    
    /**
     * Scans the registered named indices and decides which ones (if any) are
     * undercapacity and should be joined.
     * <p>
     * If the rightSibling of an undercapacity index partition is also local
     * then a {@link JoinIndexPartitionTask} is created to join those index
     * partitions and both index partitions will be marked as "used".
     * <p>
     * If the rightSibling of an undercapacity index partition is remote, then a
     * {@link MoveTask} is created to move the undercapacity index partition to
     * the remove data service and the undercapacity index partition will be
     * marked as "used".
     */
    protected List<AbstractTask> chooseJoins() {

        if (!resourceManager.joinsEnabled) {

            /*
             * Joins are disabled.
             */

            if(log.isInfoEnabled())
                log.info(OverflowManager.Options.JOINS_ENABLED + "="
                        + resourceManager.joinsEnabled);

            return EMPTY_LIST;
            
        }
        
        // list of tasks that we create (if any).
        final List<AbstractTask> tasks = new LinkedList<AbstractTask>();

        if (log.isInfoEnabled())
            log.info("begin: lastCommitTime=" + lastCommitTime);

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
            int ndone = 0; // for each named index we process
            int nskip = 0; // nothing.
            int njoin = 0; // join task _candidate_.
            int nignored = 0; // #of index partitions that are NOT join candidates.
            
            assert used.isEmpty() : "There are " + used.size()
                    + " used index partitions";

            final Iterator<ViewMetadata> itr = overflowMetadata.views();
            
            while (itr.hasNext()) {

                final ViewMetadata vmd = itr.next();

                final String name = vmd.name;
                
                /*
                 * Open the historical view of that index at that time (not just
                 * the mutable BTree but the full view).
                 */
                final IIndex view = vmd.getView();

                if (view == null) {

                    throw new AssertionError(
                            "Index not found? : name=" + name
                                    + ", lastCommitTime=" + lastCommitTime);

                }

                // handler decides when and where to split an index partition.
                final ISplitHandler splitHandler = vmd.getAdjustedSplitHandler();

                // index partition metadata
                final LocalPartitionMetadata pmd = vmd.pmd;

                if (pmd.getSourcePartitionId() != -1) {

                    /*
                     * This index is currently being moved onto this data
                     * service so it is NOT a candidate for a split, join, or
                     * move.
                     */

                    if(log.isInfoEnabled())
                        log.info("Skipping index: name=" + name
                                + ", reason=moveInProgress");

                    continue;

                }
                
                if (log.isInfoEnabled())
                    log.info("Considering join: name=" + name + ", rangeCount="
                            + vmd.getRangeCount() + ", pmd=" + pmd);
                
                if (splitHandler != null
                        && pmd.getRightSeparatorKey() != null
                        && splitHandler.shouldJoin(vmd.getRangeCount())) {

                    /*
                     * Add to the set of index partitions that are candidates
                     * for join operations.
                     * 
                     * Note: joins are only considered when the rightSibling of
                     * an index partition exists. The last index partition has
                     * [rightSeparatorKey == null] and there is no rightSibling
                     * for that index partition.
                     * 
                     * Note: If we decide to NOT join this with another local
                     * partition then we MUST do an index segment build in order
                     * to release the dependency on the old journal. This is
                     * handled below when we consider the JOIN candidates that
                     * were discovered in this loop.
                     */

                    final String scaleOutIndexName = vmd.indexMetadata.getName();

                    BTree tmp = undercapacityIndexPartitions
                            .get(scaleOutIndexName);

                    if (tmp == null) {

                        tmp = BTree.createTransient(new IndexMetadata(UUID
                                .randomUUID()));

                        undercapacityIndexPartitions
                                .put(scaleOutIndexName, tmp);

                    }

                    tmp.insert(pmd.getLeftSeparatorKey(), SerializerUtil
                            .serialize(pmd));

                    if (log.isInfoEnabled())
                        log.info("join candidate: " + name);

                    njoin++;

                } else {
                    
                    nignored++;
                    
                }

                ndone++;

            } // itr.hasNext()

            // verify counters.
            assert ndone == nskip + njoin + nignored : "ndone=" + ndone
                    + ", nskip=" + nskip + ", njoin=" + njoin + ", nignored="
                    + nignored;

        }

        /*
         * Consider the JOIN candidates. These are underutilized index
         * partitions on this data service. They are grouped by the scale-out
         * index to which they belong.
         * 
         * In each case we will lookup the rightSibling of the underutilized
         * index partition using its rightSeparatorKey and the metadata index
         * for that scale-out index.
         * 
         * If the rightSibling is local, then we will join those siblings.
         * 
         * If the rightSibling is remote, then we will move the index partition
         * to the remote data service.
         * 
         * Note: If we decide neither to join the index partition NOR to move
         * the index partition to another data service then we MUST add an index
         * build task for that index partition in order to release the history
         * dependency on the old journal.
         */
        {

            /*
             * This iterator visits one entry per scale-out index on this data
             * service having an underutilized index partition on this data
             * service.
             */
            final Iterator<Map.Entry<String, BTree>> itr = undercapacityIndexPartitions
                    .entrySet().iterator();

            int ndone = 0;
            int njoin = 0; // do index partition join on local service.
            int nmove = 0; // move to another service for index partition join.

            assert used.isEmpty() : "There are " + used.size()
                    + " used index partitions";

            // this data service.
            final UUID sourceDataService = resourceManager.getDataServiceUUID();

            while (itr.hasNext()) {

                final Map.Entry<String, BTree> entry = itr.next();

                // the name of the scale-out index.
                final String scaleOutIndexName = entry.getKey();

                if (log.isInfoEnabled())
                    log.info("Considering join candidates: "
                            + scaleOutIndexName);

                // keys := leftSeparator; value := LocalPartitionMetadata
                final BTree tmp = entry.getValue();

                // #of underutilized index partitions for that scale-out index.
                final int ncandidates = tmp.getEntryCount();
                
                assert ncandidates > 0 : "Expecting at least one candidate";
                
                final ITupleIterator titr = tmp.rangeIterator();

                /*
                 * Setup a BatchLookup query designed to locate the rightSibling
                 * of each underutilized index partition for the current
                 * scale-out index.
                 * 
                 * Note: This approach makes it impossible to join the last
                 * index partition when it is underutilized since it does not,
                 * by definition, have a rightSibling. However, the last index
                 * partition always has an open key range and is far more likely
                 * than any other index partition to receive new writes.
                 */

                if (log.isInfoEnabled())
                    log.info("Formulating rightSiblings query="
                            + scaleOutIndexName + ", #underutilized="
                            + ncandidates);
                
                final byte[][] keys = new byte[ncandidates][];
                
                final LocalPartitionMetadata[] underUtilizedPartitions = new LocalPartitionMetadata[ncandidates];
                
                int i = 0;
                
                while (titr.hasNext()) {

                    final ITuple tuple = titr.next();

                    final LocalPartitionMetadata pmd = (LocalPartitionMetadata) SerializerUtil
                            .deserialize(tuple.getValue());

                    underUtilizedPartitions[i] = pmd;

                    /*
                     * Note: the right separator key is also the key under which
                     * we will find the rightSibling.
                     */
                    
                    if (pmd.getRightSeparatorKey() == null) {

                        throw new AssertionError(
                                "The last index partition may not be a join candidate: name="
                                        + scaleOutIndexName + ", " + pmd);

                    }
                    
                    keys[i] = pmd.getRightSeparatorKey();
                    
                    i++;
                    
                } // next underutilized index partition.

                if (log.isInfoEnabled())
                    log.info("Looking for rightSiblings: name="
                            + scaleOutIndexName + ", #underutilized="
                            + ncandidates);

                /*
                 * Submit a single batch request to identify rightSiblings for
                 * all of the undercapacity index partitions for this scale-out
                 * index.
                 * 
                 * Note: default key/val serializers are used.
                 */
                final BatchLookup op = BatchLookupConstructor.INSTANCE
                        .newInstance(0/* fromIndex */, ncandidates/* toIndex */,
                                keys, null/*vals*/);
                final ResultBuffer resultBuffer;
                try {
                    resultBuffer = (ResultBuffer) resourceManager
                            .getFederation().getMetadataService()
                            .submit(
                                    TimestampUtility.asHistoricalRead(lastCommitTime),
                                    MetadataService
                                            .getMetadataIndexName(scaleOutIndexName),
                                    op).get();
                } catch (Exception e) {
                    
                    log.error("Could not locate rightSiblings: index="
                            + scaleOutIndexName, e);

                    continue;
                    
                }

                /*
                 * Now that we know where the rightSiblings are, examine the
                 * join candidates for the current scale-out index once and
                 * decide whether the rightSibling is local (we can do a join)
                 * or remote (we will move the underutilized index partition to
                 * the same data service as the rightSibling).
                 */
                for (i = 0; i < ncandidates; i++) {

                    // an underutilized index partition on this data service.
                    final LocalPartitionMetadata pmd = underUtilizedPartitions[i];

                    final ViewMetadata vmd = overflowMetadata
                            .getViewMetadata(DataService.getIndexPartitionName(
                                    scaleOutIndexName, pmd.getPartitionId()));

                    // the locator for the rightSibling.
                    final PartitionLocator rightSiblingLocator = (PartitionLocator) SerializerUtil
                            .deserialize(resultBuffer.getResult(i));

                    final UUID targetDataServiceUUID = rightSiblingLocator
                            .getDataServiceUUID();

                    final String[] resources = new String[2];

                    // the underutilized index partition.
                    resources[0] = DataService.getIndexPartitionName(
                           scaleOutIndexName, pmd.getPartitionId());
                    
                    // its right sibling (may be local or remote).
                    resources[1] = DataService.getIndexPartitionName(
                            scaleOutIndexName, rightSiblingLocator.getPartitionId());
                    
                    if (sourceDataService.equals(targetDataServiceUUID)) {

                        /*
                         * JOIN underutilized index partition with its local
                         * rightSibling.
                         * 
                         * Note: This is only joining two index partitions at a
                         * time. It's possible to do more than that if it
                         * happens that N > 2 underutilized sibling index
                         * partitions are on the same data service, but that is
                         * a relatively unlikely combination of events.
                         */

                        // e.g., already joined as the rightSibling with some
                        // other index partition on this data service.
                        if(isUsed(resources[0])) continue;

                        // this case should not occur, but better safe than
                        // sorry.
                        if(isUsed(resources[1])) continue;

                        if(log.isInfoEnabled())
                            log.info("Will JOIN: " + Arrays.toString(resources));
                        
                        final ViewMetadata vmd2 = overflowMetadata
                                .getViewMetadata(DataService
                                        .getIndexPartitionName(
                                                scaleOutIndexName,
                                                rightSiblingLocator
                                                        .getPartitionId()));

                        final AbstractTask task = new JoinIndexPartitionTask(
                                resourceManager, lastCommitTime, resources,
                                new ViewMetadata[] { vmd, vmd2 });

                        // add to set of tasks to be run.
                        tasks.add(task);
                        
                        putUsed(resources[0], "willJoin(leftSibling="
                                + resources[0] + ",rightSibling="
                                + resources[1] + ")");

                        putUsed(resources[1], "willJoin(leftSibling="
                                + resources[0] + ",rightSibling="
                                + resources[1] + ")");
                        
                        njoin++;

                    } else {
                        
                        /*
                         * MOVE underutilized index partition to data service
                         * hosting the right sibling.
                         */
                        
                        // e.g., already joined as the rightSibling with some
                        // other index partition on this data service.
                        if(isUsed(resources[0])) continue;
                        
                        final String sourceIndexName = DataService
                                .getIndexPartitionName(scaleOutIndexName, pmd
                                        .getPartitionId());

                        final AbstractTask task = new MoveTask(vmd,
                                targetDataServiceUUID);

                        // get the target service name.
                        String targetDataServiceName;
                        try {
                            targetDataServiceName = resourceManager.getFederation()
                                    .getDataService(targetDataServiceUUID)
                                    .getServiceName();
                        } catch (Throwable t) {
                            targetDataServiceName = targetDataServiceUUID.toString();
                        }

                        tasks.add(task);

                        /*
                         * FIXME If both this data service and the target data
                         * service decide to JOIN these index partitions at the
                         * same time then we will have a problem. They need to
                         * establish a mutex for this decision. To avoid the
                         * possibility of deadlock, perhaps a global lock on the
                         * scale-out index name for the purpose of move?
                         */
                        putUsed(resources[0], "willMoveToJoinWithRightSibling"
                                + "( " + sourceIndexName + " -> "
                                + targetDataServiceName //
                                + ", leftSibling=" + resources[0] //
                                + ", rightSibling=" + resources[1] //
                                + ")");

                        nmove++;
                        
                    }

                    ndone++;
                    
                }
                
            } // next scale-out index with underutilized index partition(s).

            assert ndone == njoin + nmove;
            
        } // consider JOIN candidates.

        return tasks;
        
    }

    /**
     * Helper class reports performance counters of interest for this service.
     * <p>
     * Note: Default values are used when the performance counter is not
     * available. Reasonable defaults are choosen, but they could still trigger
     * inappropriate behavior depending on the thresholds set for move/split and
     * if the host is selected as "highly utilized" by the LBS.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class ResourceScores {
        
        final double percentCPUTime;
//        final double bytesFree;
//        final double bytesAvailable;
        final double majorPageFaultsPerSec;
        final double dataDirBytesFree;
        final double tmpDirBytesFree;
        
        ResourceScores() {

            percentCPUTime = getHostCounter(
                    IRequiredHostCounters.CPU_PercentProcessorTime, .5d/* defaultValue */);

            majorPageFaultsPerSec = getHostCounter(
                    IRequiredHostCounters.Memory_majorFaultsPerSecond, .0d/* defaultValue */);

//            // @todo not collected for Windows
//            bytesFree = getHostCounter(IHostCounters.Memory_Bytes_Free,
//                    Bytes.megabyte * 500/* defaultValue */);
//
//            // @todo not collected for Windows or Linux.
//            bytesAvailable = getHostCounter(
//                    IHostCounters.Memory_Bytes_Available, Bytes.gigabyte * 4/* defaultValue */);
            
            dataDirBytesFree = getServiceCounter(
                    IDataServiceCounters.resourceManager
                            + ICounterSet.pathSeparator
                            + IResourceManagerCounters.StoreManager
                            + ICounterSet.pathSeparator
                            + IStoreManagerCounters.DataDirBytesAvailable,
                    Bytes.gigabyte * 20/* defaultValue */);

            tmpDirBytesFree = getServiceCounter(
                    IDataServiceCounters.resourceManager
                            + ICounterSet.pathSeparator
                            + IResourceManagerCounters.StoreManager
                            + ICounterSet.pathSeparator
                            + IStoreManagerCounters.TmpDirBytesAvailable,
                    Bytes.gigabyte * 10/* defaultValue */);
            
        }

    }
    
    /**
     * Identify index partitions which are global hot spots and either schedule
     * them for a move (if this host is at peak CPU, low disk, or swapping
     * heavily) or a split (if the CPU is underutilized).
     * <p>
     * The LBS ranks services based on their average queuing time. When a
     * service is "highly utilized" according to the LBS, we consider its
     * resource utilization. If the service is utilizing most of its CPU, if it
     * is swapping heavily, or if it is low on disk, then we need to shed an
     * index partition in order to reduce the workload. On the other hand, if
     * the service is NOT utilizing a significant percentage of its CPU then its
     * concurrency generally can be improved by splitting a "hot" index
     * partition since this allows the host to allocate two cores to handle the
     * same key range which was previously being serviced by a single core.
     * 
     * @return
     */
    protected List<AbstractTask> chooseMoveOrSplit() {

        /*
         * Figure out if this data service is considered to be highly utilized.
         * 
         * Note: We consult the load balancer service on this since it is able
         * to put the load of this service into perspective by also considering
         * the load on the other services in the federation.
         * 
         * Note: This is robust to failure of the load balancer service. When it
         * is not available we simply do not consider index partition moves.
         */

        // lookup the load balancer service.
        final ILoadBalancerService loadBalancerService;
        
        try {

            loadBalancerService = resourceManager.getFederation()
                    .getLoadBalancerService();

        } catch (Exception ex) {

            log.warn("Could not discover the load balancer service", ex);

            return EMPTY_LIST;
            
        }
        
        if (loadBalancerService == null) {

            log.warn("Could not discover the load balancer service");

            return EMPTY_LIST;
            
        }

        // inquire if this service is highly utilized.
        final boolean highlyUtilizedService;
        try {

            final UUID serviceUUID = resourceManager.getDataServiceUUID();
            
            highlyUtilizedService = loadBalancerService
                    .isHighlyUtilizedDataService(serviceUUID);

        } catch (Exception ex) {

            log.warn("Could not determine if this data service is highly utilized");
            
            return EMPTY_LIST;
            
        }

        if (!highlyUtilizedService) {
            
            if(log.isInfoEnabled())
                log.info("Service is not highly utilized.");
            
            return EMPTY_LIST;
            
        }
        
        /*
         * At this point we know that the LBS considers this host and service to
         * be highly utilized (relative to the other hosts and services). If
         * there is evidence of resource exhaustion for critical resources (CPU,
         * RAM, or DIKS) then we will MOVE index partitions in order to shed
         * some load. Otherwise, we will SPLIT hot index partitions in order to
         * increase the potential concurrency of the workload for this service.
         * 
         * FIXME Be careful that the unit tests for MOVE will still run when I
         * make these changes.
         * 
         * @todo config options for these triggers.
         * 
         * FIXME Review the move policy with an eye towards how it selects which
         * index partition(s) to move. Note that chooseMoves() is now invoked
         * ONLY when the host is heavily utilized (on both the global and the
         * local scale). CPU is the only fungable resource since things will
         * just slow down if a host has 100% CPU while it can die if it runs out
         * of DISK or RAM (including if it begins to swap heavily).
         */
        final ResourceScores resourceScores = new ResourceScores();

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
        
        if (shouldMove) {

            return chooseMoves(loadBalancerService);
            
        }

        return chooseHotSplits();
        
    }
        
    /**
     * Return tasks which will MOVE selected index partitions onto one or more
     * other host(s).
     * <p>
     * Note: This method should not be invoked: (a) unless the LBS has
     * identified this host and service has being highly utilized; and (b) there
     * is evidence of resource exhaustion for one or more of the critical
     * resources (CPU, RAM, DISK).
     * 
     * @param loadBalancerService
     *            The proxy for the LBS.
     * 
     * @return The tasks.
     */
    private List<AbstractTask> chooseMoves(
            final ILoadBalancerService loadBalancerService) {
        
        if (resourceManager.maximumMovesPerTarget == 0) {

            // Moves are not allowed.
            
            return EMPTY_LIST;
            
        }

        /*
         * The minimum #of active index partitions on a data service. We will
         * consider moving index partitions iff this threshold is exceeeded.
         */
        final int minActiveIndexPartitions = resourceManager.minimumActiveIndexPartitions;

        /*
         * The #of active index partitions on this data service.
         */
        final int nactive = overflowMetadata.getActiveCount();

        if (nactive <= minActiveIndexPartitions) {

            if (log.isInfoEnabled())
                log.info("Preconditions for move not satisified: nactive="
                        + nactive + ", minActive=" + minActiveIndexPartitions);
            
            return EMPTY_LIST;
            
        }

        /*
         * Note: We make sure that we do not move all the index partitions to
         * the same target by moving at most M index partitions per
         * under-utilized data service recommended to us.
         */
        final int maxMovesPerTarget = resourceManager.maximumMovesPerTarget;

        // the UUID of this data service.
        final UUID sourceServiceUUID = resourceManager.getDataServiceUUID();
        /*
         * Obtain some data service UUIDs onto which we will try and offload
         * some index partitions iff this data service is deemed to be highly
         * utilized.
         * 
         * FIXME The LBS should interpret the excludedServiceUUID as the source
         * service UUID and then provide a list of those services having an LBS
         * computed service score which is significantly lower than the score
         * for this service. [@todo changing this will break some unit tests
         * and javadoc will need to be updated as well.]
         */
        final UUID[] underUtilizedDataServiceUUIDs;
        try {

            // request under utilized data service UUIDs (RMI).
            underUtilizedDataServiceUUIDs = loadBalancerService
                    .getUnderUtilizedDataServices(//
                            0, // minCount - no lower bound.
                            0, // maxCount - no upper bound.
                            sourceServiceUUID // exclude this data service.
                            );

        } catch (TimeoutException t) {
            
            log.warn(t.getMessage());
            
            return EMPTY_LIST;

        } catch (InterruptedException t) {

            log.warn(t.getMessage());

            return EMPTY_LIST;
            
        } catch (Throwable t) {

            log.error("Could not obtain target service UUIDs: ", t);

            return EMPTY_LIST;

        }

        if (underUtilizedDataServiceUUIDs == null
                || underUtilizedDataServiceUUIDs.length == 0) {

            if (log.isInfoEnabled())
                log.info("Load balancer does not report any underutilized services.");

            return EMPTY_LIST;
            
        }
        
        /*
         * The maximum #of index partition moves that we will attempt. This will
         * be zero if there are no under-utilized services onto which we can
         * move an index partition. Also, it will be zero unless there is a
         * surplus of active index partitions on this data service.
         */
        final int maxMoves;
        {
            
            final int nactiveSurplus = nactive - minActiveIndexPartitions;
            
            assert nactiveSurplus > 0;
            
            assert underUtilizedDataServiceUUIDs != null;

            maxMoves = Math.min(resourceManager.maximumMoves, //
                    Math.min(nactiveSurplus, //
                            maxMovesPerTarget
                                    * underUtilizedDataServiceUUIDs.length));
            
        }

        /*
         * Move candidates.
         * 
         * Note: We make sure that we don't move all the hot/warm index
         * partitions by choosing the index partitions to be moved from the
         * middle of the range. However, note that "cold" index partitions are
         * NOT assigned scores, so there has been either an unisolated or
         * read-committed operation on any index partition that was assigned a
         * score.
         * 
         * Note: This could lead to a failure to recommend a move if all the
         * "warm" index partitions happen to require a split or join. However,
         * this is unlikely since the "warm" index partitions change size, if
         * not slowly, then at least not as rapidly as the hot index partitions.
         * Eventually a lot of splits will lead to some index partition not
         * being split during an overflow and then we can move it.
         */

        if(log.isInfoEnabled())
            log.info("Considering index partition moves: #targetServices="
                + underUtilizedDataServiceUUIDs.length + ", maxMovesPerTarget="
                + maxMovesPerTarget + ", nactive=" + nactive + ", maxMoves="
                + maxMoves + ", sourceService="+sourceServiceUUID+", targetServices="
                + Arrays.toString(underUtilizedDataServiceUUIDs));

        // The maximum range count for any active index.
        long maxRangeCount = 0L;
        
        // just those indices that survive the cuts we impose here.
        final List<Score> scores = new LinkedList<Score>();
        
        for (Score score : overflowMetadata.getScores()) {

            final String name = score.name;
            
            if(isUsed(name)) continue;

            if (overflowMetadata.isCopied(name)) {

                /*
                 * The write set from the old journal was already copied to the
                 * new journal so we do not need to do a build.
                 */

                putUsed(name, "wasCopied(name=" + name + ")");

                continue;
                
            }

            // test for indices that have been split, joined, or moved.
            final StaleLocatorReason reason = resourceManager
                    .getIndexPartitionGone(score.name);

            if (reason != null) {
                
                /*
                 * Note: The counters are accumulated over the life of the
                 * journal. This tells us that the named index was moved, split,
                 * or joined sometimes during the live of that old journal.
                 * Since it is gone we skip over it here.
                 */
                
                if (log.isInfoEnabled())
                    log.info("Skipping index: name=" + score.name + ", reason="
                            + reason);
                
                continue;
                
            }
            
            // get the view metadata.
            final ViewMetadata vmd = overflowMetadata.getViewMetadata(name);
            
            if (vmd == null) {

                /*
                 * Note: The counters are accumulated over the live of the
                 * journal. This tells us that the named index was dropped
                 * sometimes during the life cycle of that old journal. Since it
                 * is gone we skip over it here.
                 */

                if (log.isInfoEnabled())
                    log.info("Skipping index: name=" + name
                            + ", reason=dropped");

                continue;

            }

            if (vmd.pmd.getSourcePartitionId() != -1) {

                /*
                 * This index is currently being moved onto this data service so
                 * it is NOT a candidate for a split, join, or move.
                 */

                if (log.isInfoEnabled())
                    log.info("Skipping index: name=" + name
                            + ", reason=moveInProgress");

                continue;

            }

            // handler decides when and where to split an index partition.
            final ISplitHandler splitHandler = vmd.getAdjustedSplitHandler();

            final long rangeCount = vmd.getRangeCount();
            
            if (splitHandler.shouldSplit(rangeCount)) {

                /*
                 * This avoids moving index partitions that are large and really
                 * should be split before they are moved.
                 */

                if (log.isInfoEnabled())
                    log.info("Skipping index: name=" + name
                            + ", reason=shouldSplit");

                continue;

            }

            // this is an index that we will consider again below.
            scores.add(score);
            
            // track the maximum range count over all active indices.
            maxRangeCount = Math.max(maxRangeCount, vmd.getRangeCount());
            
        }
            
        /*
         * Queue places the move candidates into a total order. We then choose
         * from the candidates based on that order.
         * 
         * Note: The natural order of [Priority] is DESCENDING [largest
         * numerical value to smallest numerical value]! We assign larger scores
         * to the index partitions that we want to move.
         */
        final PriorityQueue<Priority<ViewMetadata>> moveQueue = new PriorityQueue<Priority<ViewMetadata>>();

        for(Score score : scores) {

            // get the view metadata.
            final ViewMetadata vmd = overflowMetadata.getViewMetadata(score.name);

            /*
             * Note: Moving an index partition is the most expensive overflow
             * operation. Its cost has two dimensions which affect the move
             * time.
             * 
             * The first cost dimension is simply the #of bytes to be moved, and
             * we use the rangeCount as an estimate of that cost.
             * 
             * The second cost dimension is the #of new bytes that arrive on the
             * live journal for the index partition to be moved while we are
             * moving its historical view onto the target data service - we use
             * [score.drank] as a proxy for that cost. Since moving the buffered
             * writes from the live journal requires an exclusive lock on the
             * live index, an additional delay is imposed on the application
             * during that phase of the move. The more writes buffered before
             * the move, the longer the application will block during the move.
             * 
             * Whenever possible, we want to move the index partition with the
             * smallest range count since it takes the least effort to move and
             * move is the most expensive of the overflow operations.
             * 
             * Whenever feasible, we want to move the more active index
             * partitions since that will shed more load BUT NOT if that makes
             * us move more data. This decision is the most problematic when an
             * index is hot for writes as newly buffered writes will increase
             * the actual move time and cause the application to block during
             * the atomic update phase of the move.
             * 
             * ---------------
             * 
             * Given two active indices having small range counts, we prefer to
             * move the more active since that will let us shed more effort.
             * 
             * Note: This condition is formulated to reject a hot index
             * (score.drank GTE .6) unless it has a low range rank (LT .3). It
             * will also accept any active index if it has a lower range rank.
             * We then choose the actual index partition moves based on the
             * index scores (in the next step below).
             * 
             * Note: The code here is only considering active index partitions
             * since we are trying to balance LOAD rather than the allocation of
             * data on the disk.
             * 
             * @todo we also need to balance the on disk allocations - there are
             * notes on that elsewhere. this is a tricky topic since historical
             * data are not moved (the locators in the MDS for historical data
             * are immutable - at least they are without some fancy footwork),
             * so moving an index partition that is hot for writes is an
             * excellent way to balance the on disk storage IF there is a
             * history limit such that older data will be released thereby
             * freeing space on the disk.
             * 
             * @todo it deleted tuples are a large part of the operations on the
             * index partition during its life on the old journal then do a
             * compacting merge on the index before doing any other operation so
             * that we can better tell how many tuples remain in the index
             * partition.
             * 
             * FIXME We are currently using the performance counters for all
             * AbstractTask operations, not just those which are UNISOLATED or
             * READ_COMMITTED. Think about what this means when choosing an
             * index partition to move. E.g., a read-hot index would appear to
             * be hot, not just a write-hot index. This could be changed easily
             * enough by changing the indices for which AbstractTask will report
             * the performance counters or by tracking the read-only vs
             * unisolated and read-committed performance counters separately.
             * 
             * @todo it would be nice to get the moveCandidate and moveRatio
             * onto the ViewMetadata but we would need to have the score.drank
             * available in that context.
             */
            
            /*
             * FIXME As an alternative design a split could just occur when the
             * sum of the segment bytes after a compacting merge was GTE the
             * target split size. The split would occur in the atomic update
             * task of the compacting merge (or a build which had compacting
             * merge semantics).
             * 
             * If we select moves solely based on activity and begin a move with
             * a compacting merge then we could do a split if required (index
             * segment is too large to move). Since a split involves a
             * compacting merge of each key-range, we have the history in a
             * compact index segment either way. At that point we can just blast
             * the index segment (and the selected index segment if we did a
             * split first) across a socket and then catch up with any writes
             * buffered by the live journal to complete the move.
             */
            
            /*
             * Either the index partition is a tail split, which is an ideal
             * move candidate and we will move the righSibling, or it is not
             * "too large" and we will move the index partition, or it is
             * overdue for a split and we will split the index partition and
             * then move the smallest of the post-split index partitions.
             * 
             * Very cold index partitions are ignored.
             * 
             * Note: It is important that we choose a move if this data service
             * is highly utilized. Failure to choose a move will result in the
             * host on which the data service resides becoming a bottleneck in
             * the federation. For that reason we have move variants that handle
             * large index partitions (tail split, normal split) and also the
             * standard move operation when the index partition is not too
             * large.
             */
            final double moveMinScore = .1;
            final boolean moveCandidate = //
                /*
                 * Note: barely active indices are not moved since moving them
                 * does not change the load on the data service.
                 */
                score.drank >= moveMinScore
//                (tailSplit || (vmd.getPercentOfSplit() < MOVE_MAX_PERCENT_OF_SPLIT))
                ;

            /*
             * Ratio used to choose prioritize the index partitions for moves.
             * 
             * Note: Since the rightSibling of a tail split is always very small
             * we substitute a small "percentOfSplit" (.1) if a tail split would
             * be chosen. This let's tail split + move candidates rank up there
             * with small index partitions which are equally hot.
             * 
             * Note: This also helps to prevent very small indices that are not
             * getting much activity from bounding around.
             */
            final double movePriority = vmd.isTailSplit() //
                ? score.drank / .1// 
                : score.drank / vmd.getPercentOfSplit()//
                ;

            if (log.isInfoEnabled())
                log
                        .info(vmd.name + " : tailSplit=" + vmd.isTailSplit()
                                + ", moveCandidate=" + moveCandidate
                                + ", movePriority=" + movePriority + ", drank="
                                + score.drank + ", percentOfSplit="
                                + vmd.getPercentOfSplit() + " : " + vmd + " : "
                                + score);

            if (!moveCandidate) {

                /*
                 * Don't attempt moves for indices with larger range counts.
                 */

                continue;

            }

            /*
             * Place into the queue for consideration. Among the indices that
             * have lower range counts we prefer those which have the highest
             * scores.
             */
            moveQueue.add(new Priority<ViewMetadata>(movePriority, vmd));

        } // next Score (active index).

        /*
         * Now consider the move candidates in their assigned priority order.
         */
        int nmove = 0;

        final List<AbstractTask> tasks = new ArrayList<AbstractTask>(maxMoves);
        
        while (nmove < maxMoves && !moveQueue.isEmpty()) {

            // the highest priority candidate for a move.
            final ViewMetadata vmd = moveQueue.poll().v;
            
            if (log.isInfoEnabled())
                log.info("Considering move candidate: " + vmd);
            
            /*
             * Choose target using round robin among candidates. This means that
             * we will choose the least utilized of the services first.
             */
            final UUID targetDataServiceUUID = underUtilizedDataServiceUUIDs[nmove
                    % underUtilizedDataServiceUUIDs.length];

            if (sourceServiceUUID.equals(targetDataServiceUUID)) {

                log
                        .error("LBS included the source data service in the set of possible targets: source="
                                + sourceServiceUUID
                                + ", targets="
                                + Arrays
                                        .toString(underUtilizedDataServiceUUIDs));

                /*
                 * Note: by continuing here we will not do a move for this index
                 * partition (it would throw an exception) but we will at least
                 * consider the next index partition for a move.
                 */

                continue;

            }

            // get the target service name.
            String targetDataServiceName;
            try {
                targetDataServiceName = resourceManager.getFederation()
                        .getDataService(targetDataServiceUUID).getServiceName();
            } catch (Throwable t) {
                targetDataServiceName = targetDataServiceUUID.toString();
            }

            if (vmd.isTailSplit()) {

                /*
                 * tailSplit
                 * 
                 * Note: One ideal candidate for a move is an index partition
                 * where most writes are on the tail of the index. In this case
                 * we can do tailSplit, leaving the majority of the data in
                 * place and only moving the empty or nearly empty tail of the
                 * index partition to the target data service. If the case is of
                 * pure tail append (we always write a key that is a successor
                 * of every prior key), then we can move an "empty" tail - this
                 * could even be done during synchronous overflow. However, if
                 * the tail writes are somewhat more distributed, then we need
                 * to move the key range of the tail that is receiving the
                 * writes.
                 * 
                 * Tail splits allow us to move the least possible data. This is
                 * the most possible reward for the least possible effort. Even
                 * when the index is very hot, the move of the post-split
                 * rightSibling will typically move less data than the move of
                 * another index partition
                 * 
                 * @todo support headSplits? what kind of application pattern
                 * can produce the necessary preconditions?. Maybe a FIFO queue?
                 */
                
                if (log.isInfoEnabled())
                    log.info("Will tailSplit " + vmd.name
                            + " and move the rightSibling to dataService="
                            + targetDataServiceName);

                final AbstractTask task = new SplitTailTask(vmd,
                        targetDataServiceUUID);

                tasks.add(task);

                putUsed(vmd.name, "willTailSplit + moveRightSibling("
                        + vmd.name + " -> " + targetDataServiceName + ") : "
                        + vmd + " : " + overflowMetadata.getScore(vmd.name));

                nmove++;

              } else if (!vmd.getAdjustedSplitHandler().shouldJoin(
                    vmd.getRangeCount())) {

                /*
                 * Split the index partition and then move the smallest of the
                 * post-split index partitions.
                 * 
                 * Note: This uses a more eager criteria for selecting a split
                 * than is applied in a non-move context. This is done so that
                 * we can move smaller post-split index partitions rather than
                 * moving the entire index partition which would otherwise not
                 * be split until it was "overcapacity".
                 */

                if (log.isInfoEnabled())
                    log
                            .info("Will split "
                                    + vmd.name
                                    + " and move the smallest post-split index partition to dataService="
                                    + targetDataServiceName);

                final AbstractTask task = new SplitIndexPartitionTask(vmd,
                        targetDataServiceUUID);

                tasks.add(task);

                putUsed(vmd.name, "willSplit+Move(" + vmd.name + " -> "
                        + targetDataServiceName + ") : " + vmd + " : "
                        + overflowMetadata.getScore(vmd.name));

                nmove++;

            } else {

                /*
                 * Normal move (does not split first).
                 */

                if (log.isInfoEnabled())
                    log.info("Will move " + vmd.name + " to dataService="
                            + targetDataServiceName);

                final AbstractTask task = new MoveTask(vmd,
                        targetDataServiceUUID);

                tasks.add(task);

                putUsed(vmd.name, "willMove(" + vmd.name + " -> "
                        + targetDataServiceName + ") : " + vmd + " : "
                        + overflowMetadata.getScore(vmd.name));

                nmove++;

            }

        }
        
        if (log.isInfoEnabled())
            log.info("Will move " + nmove
                    + " index partitions based on utilization.");

        return tasks;

    }

    /**
     * Return tasks which will split "hot" index partitions.
     * <p>
     * Note: This method should not be invoked: (a) unless the LBS deems that
     * this service is heavily utilized; and (b) if the local host and service
     * performance counters indicate exhaustion of any critical resources (CPU,
     * RAM or DISK).
     * <p>
     * Note: This chooses "hot splits" based on the assumption that an index
     * partition with a high {@link Score} (which means a lot of write time)
     * will continue to grow. If there are heavy writes on an index partition
     * but it does not continue to grow, then it is possible that the index
     * partition will later be JOINed with itself.
     */
    protected List<AbstractTask> chooseHotSplits() {

        // just those indices that survive the cuts we impose here.
        final List<Score> scores = new LinkedList<Score>();

        // filter the scores for just the most active indices.
        for (Score score : overflowMetadata.getScores()) {

            final String name = score.name;
            
            if(isUsed(name)) continue;

            if (overflowMetadata.isCopied(name)) {

                /*
                 * The write set from the old journal was already copied to the
                 * new journal so we do not need to do a build.
                 */

                putUsed(name, "wasCopied(name=" + name + ")");

                continue;
                
            }

            // test for indices that have been split, joined, or moved.
            final StaleLocatorReason reason = resourceManager
                    .getIndexPartitionGone(score.name);

            if (reason != null) {
                
                /*
                 * Note: The counters are accumulated over the life of the
                 * journal. This tells us that the named index was moved, split,
                 * or joined sometimes during the live of that old journal.
                 * Since it is gone we skip over it here.
                 */
                
                if (log.isInfoEnabled())
                    log.info("Skipping index: name=" + score.name + ", reason="
                            + reason);
                
                continue;
                
            }
            
            // get the view metadata.
            final ViewMetadata vmd = overflowMetadata.getViewMetadata(name);
            
            if (vmd == null) {

                /*
                 * Note: The counters are accumulated over the live of the
                 * journal. This tells us that the named index was dropped
                 * sometimes during the life cycle of that old journal. Since it
                 * is gone we skip over it here.
                 */

                if (log.isInfoEnabled())
                    log.info("Skipping index: name=" + name
                            + ", reason=dropped");

                continue;

            }

            if (vmd.pmd.getSourcePartitionId() != -1) {

                /*
                 * This index is currently being moved onto this data service so
                 * it is NOT a candidate for a split, join, or move.
                 */

                if (log.isInfoEnabled())
                    log.info("Skipping index: name=" + name
                            + ", reason=moveInProgress");

                continue;

            }

            /*
             * Don't hot split an index partition if it is the only partition
             * for that index and scatter splits are enabled.
             */
            if (vmd.getIndexPartitionCount() == 1
                    && resourceManager.scatterSplitEnabled
                    && vmd.indexMetadata.getScatterSplitConfiguration()
                            .isEnabled()) {

                if (log.isInfoEnabled())
                    log.info("Skipping index: name=" + name
                            + ", reason=preferScatterSplit");

                continue;

            }

            /*
             * If the Score is high in the ordinal ranking or above a threshold
             * in the double precision rank then we will consider a hot split
             * for this index partition.
             */
            if (score.rank >= scores.size() - 2 || score.drank > .8) {

                /*
                 * There must be enough data in the index partition to make it
                 * worth while to split.
                 * 
                 * Note: Avoid hot splits which do not lead to increased
                 * concurrency.
                 * 
                 * For example, an index which is hot for tailSplits is NOT a
                 * good candidate for a hot split since only one of the
                 * resulting index partitions (the rightSibling) will have a
                 * significant workload.
                 * 
                 * FIXME However, there are other conditions under which hot
                 * splits do not help. For instance, if the indices are all more
                 * or less equally active but the workload is not high enough to
                 * increase the CPU utilization. [This condition occurs when we
                 * use a counter with some fast high bits to randomly distribute
                 * writes.]
                 * 
                 * FIXME Even worse, there is nothing to prevent a split from
                 * being hot split again. That is, this acceleration term does
                 * not take history into account.
                 */
                if (vmd.getPercentOfSplit() > resourceManager.hotSplitThreshold
                        && vmd.percentTailSplits < .25) {

                    // this is an index that we will consider again below.
                    scores.add(score);

                }

            }
            
        }

        // convert to an array.
        final Score[] a = scores.toArray(new Score[0]);
        
        // put into descending order (highest score first).
        Arrays.sort(a, new Score.DESC());
        
        /*
         * Now consider the move candidates in their assigned priority order.
         */
        int nsplit = 0;
        final int maxHotSplits = a.length; // @todo config maxHotSplits

        final List<AbstractTask> tasks = new ArrayList<AbstractTask>(maxHotSplits);
        
        // the surviving scores in order from highest to lowest.
        final Iterator<Score> itr = Arrays.asList(a).iterator();
        
        while (nsplit < maxHotSplits && itr.hasNext()) {

            final Score score = itr.next();
            
            final String name = score.name;
            
            // the highest priority candidate for a split.
            final ViewMetadata vmd = overflowMetadata.getViewMetadata(name);
            
            if (log.isInfoEnabled())
                log.info("Considering hot split candidate: " + vmd);

            if (vmd.percentTailSplits >= resourceManager.tailSplitThreshold) {

                /*
                 * Do an index (tail) split task.
                 */

                final AbstractTask task = new SplitTailTask(vmd, null/* moveTarget */);

                // add to set of tasks to be run.
                tasks.add(task);

                overflowMetadata.setAction(vmd.name,
                        OverflowActionEnum.TailSplit);

                putUsed(name, "tailSplit(name=" + vmd + ")");

                if (log.isInfoEnabled())
                    log.info("will tailSpl: " + vmd);

                continue;

            }
            
            /*
             * Do a normal split.
             */

            final AbstractTask task = new SplitIndexPartitionTask(vmd,
                    (UUID) null/* moveTarget */);

            // add to set of tasks to be run.
            tasks.add(task);

            overflowMetadata.setAction(vmd.name, OverflowActionEnum.Split);

            putUsed(name, "willSplit(name=" + vmd + ")");

        } // itr.hasNext()

        return tasks;
        
    }

    /**
     * Examine each named index on the old journal and decide what, if anything,
     * to do with that index. These indices are key range partitions of named
     * scale-out indices. The {@link LocalPartitionMetadata} describes the key
     * range partition and identifies the historical resources required to
     * present a coherent view of that index partition.
     * <p>
     * Note: Overflow actions which define a new index partition (Split, Join,
     * and Move) all require a phase (which is part of their atomic update
     * tasks) in which they will block the application. This is necessary in
     * order for them to "catch up" with buffered writes on the new journal -
     * those writes need to be incorporated into the new index partition.
     * 
     * <h2> Compacting Merge </h2>
     * 
     * A compacting merge is performed when there are buffered writes, when the
     * buffered writes were not simply copied onto the new journal during the
     * atomic overflow operation, when the index partition is neither
     * overcapacity (split) nor undercapacity (joined), and when the #of source
     * index components for the index partition exceeds some threshold (~4).
     * Also, we do not do a build if the index partitions will be moved.
     * 
     * <h2> Incremental Build </h2>
     * 
     * A incremental build is performed when there are buffered writes, when the
     * buffered writes were not simply copied onto the new journal during the
     * atomic overflow operation, when the index partition is neither
     * overcapacity (split) nor undercapacity (joined), and when there are fewer
     * than some threshold (~4) #of components in the index partition view.
     * Also, we do not do a build if the index partitions will be moved.
     * <p>
     * An incremental build is generally faster than a compacting merge because
     * it only copies those writes that were buffered on the mutable
     * {@link BTree}. However, an incremental build must copy ALL tuples,
     * including deleted tuples, so it can do more work and does not cause the
     * rangeCount() to be reduced since the deleted tuples are preserved.
     * 
     * <h2> Split </h2>
     * 
     * A split is considered when an index partition appears to be overcapacity.
     * The split operation will inspect the index partition in more detail when
     * it runs. If the index partition does not, in fact, have sufficient
     * capacity to warrant a split then a build will be performed instead (the
     * build is treated more or less as a one-to-one split, but we do not assign
     * a new partition identifier). An index partition which is WAY overcapacity
     * can be split into more than 2 new index partitions.
     * 
     * <h2> Join </h2>
     * 
     * A join is considered when an index partition is undercapacity. Joins
     * require both an undercapacity index partition and its rightSibling. Since
     * the rightSeparatorKey for an index partition is also the key under which
     * the rightSibling would be found, we use the rightSeparatorKey to lookup
     * the rightSibling of an index partition in the {@link MetadataIndex}. If
     * that rightSibling is local (same {@link ResourceManager}) then we will
     * JOIN the index partitions. Otherwise we will MOVE the undercapacity index
     * partition to the {@link IDataService} on which its rightSibling was
     * found.
     * 
     * <h2> Move </h2>
     * 
     * We move index partitions around in order to make the use of CPU, RAM and
     * DISK resources more even across the federation and prevent hosts or data
     * services from being either under- or over-utilized. Index partition moves
     * are necessary when a scale-out index is relatively new in to distribute
     * the index over more than a single data service. Likewise, index partition
     * moves are important when a host is overloaded, especially when it is
     * approaching resource exhaustion. However, index partition moves DO NOT
     * release DISK space on a host since only the current state of the index
     * partition is moved, not its historical states (which are on a mixture of
     * journals and index segments).
     * <p>
     * We can choose which index partitions to move fairly liberally. Cold index
     * partitions are not consuming any CPU/RAM/IO resources and moving them to
     * another host will not effect the utilization of either the source or the
     * target host. Moving an index partition which is "hot for write" can
     * impose a noticeable latency because the "hot for write" partition will
     * have absorbed more writes on the journal while we are moving the data
     * from the old view and we will need to move those writes as well. When we
     * move those writes the index will be unavailable for write until it
     * appears on the target data service. Therefore we generally choose to move
     * "warm" index partitions since it will introduce less latency when we
     * temporarily suspend writes on the index partition.
     * <p>
     * Indices typically have many commit points, and any one of them could
     * become "hot for read". However, moving an index partition is not going to
     * reduce the load on the old node for historical reads since we only move
     * the current state of the index, not its history. Nodes that are hot for
     * historical reads spots should be handled by increasing its replication
     * count and reading from the secondary data services. Note that
     * {@link ITx#READ_COMMITTED} and {@link ITx#UNISOLATED} both count against
     * the "live" index - read-committed reads always track the most recent
     * state of the index partition and would be moved if the index partition
     * was moved.
     * <p>
     * Bottom line: if a node is hot for historical read then increase the
     * replication count and read from failover services. If a node is hot for
     * read-committed and unisolated operations then move one or more of the
     * warm read-committed/unisolated index partitions to a node with less
     * utilization.
     * <p>
     * Index partitions that get a lot of action are NOT candidates for moves
     * unless the node itself is either overutilized, about to exhaust its DISK,
     * or other nodes are at very low utilization. We always prefer to move the
     * "warm" index partitions instead.
     * 
     * <h2> DISK exhaustion </h2>
     * 
     * Running out of DISK space causes an urgent condition and can lead to
     * failure or all services on the same host. Therefore, when a host is near
     * to exhausting its DISK space it (a) MUST notify the
     * {@link ILoadBalancerService}; (b) temporary files SHOULD be purged; it
     * MAY choose to shed indices that are "hot for write" since that will slow
     * down the rate at which the disk space is consumed; and (d) the resource
     * manager MAY aggressively release old resources, even at the expense of
     * forcing transactions to abort.
     * 
     * @todo document tailSplits.
     * 
     * FIXME implement suggestions for handling cases when we are nearing DISK
     * exhaustion.
     * 
     * FIXME read locks for read-committed operations. For example, queries to
     * the mds should use a read-historical tx so that overflow in the mds will
     * not cause the views to be discarded during data service overflow. In
     * fact, we can just create a read-historical transaction when we start data
     * service overflow and then pass it into the rest of the process, aborting
     * that tx when overflow is complete.
     * 
     * FIXME make the atomic update tasks truly atomic using full transactions
     * and/or distributed locks and correcting actions.
     */
    protected List<AbstractTask> chooseTasks() throws Exception {

        /*
         * Note whether or not a compacting merge was requested and clear the
         * flag.
         */
        final boolean compactingMerge = resourceManager.compactingMerge
                .getAndSet(false); 
        
        // the old journal.
        final AbstractJournal oldJournal = resourceManager
                .getJournal(lastCommitTime);

        final long oldJournalSize = oldJournal.size();
        
        if (log.isInfoEnabled())
            log.info("begin: lastCommitTime=" + lastCommitTime
                    + ", compactingMerge=" + compactingMerge
                    + ", oldJournalSize=" + oldJournalSize);

        // tasks to be created (capacity of the array is estimated).
        final List<AbstractTask> tasks = new ArrayList<AbstractTask>(
                (int) oldJournal.getName2Addr().rangeCount());

        if (!compactingMerge) {

            /*
             * Note: When a compacting merge is requested we do not consider
             * either join or move tasks.
             */
            
            /*
             * Choose index partitions for scatter split operations. The scatter
             * split divides an index partition into N index partitions, one per
             * data service, and then moves N-1 of the generated index
             * partitions to other data services leaving one index partition in
             * place on this data service.
             */
            tasks.addAll(chooseScatterSplits());

            /*
             * Identify any index partitions that have underflowed and will
             * either be joined or moved. When an index partition is joined its
             * rightSibling is also processed. This is why we identify the index
             * partition joins first - so that we can avoid attempts to process
             * the rightSibling now that we know it is being used by the join
             * operation.
             */
            tasks.addAll(chooseJoins());

            /*
             * Identify index partitions that will be split or moved when this
             * service is highly utilized.
             */
            tasks.addAll(chooseMoveOrSplit());

        }

        /*
         * Review all index partitions on the old journal as of the last commit
         * time and verify for each one that we have either already assigned a
         * post-processing task to handle it, that we assign one now (either a
         * split or a build task), or that no post-processing is required (this
         * last case occurs when the view state was copied onto the new
         * journal).
         */

        tasks.addAll(chooseSplitBuildOrMerge(compactingMerge));

        /*
         * Log the selected post-processing decisions at a high level.
         */
        {
            final StringBuilder sb = new StringBuilder();
            final Iterator<Map.Entry<String, String>> itrx = used.entrySet()
                    .iterator();
            while (itrx.hasNext()) {
                final Map.Entry<String, String> entry = itrx.next();
                sb.append("\n" + entry.getKey() + "\t = " + entry.getValue());
            }
            log.warn("\nlastCommitTime=" + lastCommitTime
                    + ", compactingMerge=" + compactingMerge
                    + ", oldJournalSize=" + oldJournalSize + sb);
        }

        return tasks;

    }

    /**
     * For each index (partition) that has not been handled, decide whether we
     * will:
     * <ul>
     * 
     * <li>Split the index partition.</li>
     * 
     * <li>Compacting merge - build an {@link IndexSegment} the
     * {@link FusedView} of the the index partition.</li>
     * 
     * <li>Incremental build - build an {@link IndexSegment} from the writes
     * absorbed by the mutable {@link BTree} on the old journal (this removes
     * the dependency on the old journal as of its lastCommitTime); or</li>
     * 
     * </ul>
     * 
     * Note: Compacting merges are decided in two passes. First manditory
     * compacting merges and splits are identified and a "merge" priority is
     * computed for the remaining index partitions. In the second pass, we
     * consume the remaining index partitions in "merge priority" order,
     * assigning compacting merge tasks until we reach the maximum #of
     * compacting merges to be performed in a given asynchronous overflow
     * operation.
     * 
     * @param compactingMerge
     *            When <code>true</code> a compacting merge will be performed
     *            for all index partitions.
     * 
     * @return The list of tasks.
     */
    protected List<AbstractTask> chooseSplitBuildOrMerge(
            final boolean compactingMerge) {

        // counters : must sum to ndone as post-condition.
        int ndone = 0; // for each named index we process
        int nskip = 0; // nothing.
        int nbuild = 0; // incremental build task.
        int nmerge = 0; // compacting merge task.
        int nsplit = 0; // split task.

        // set of tasks created.
        final List<AbstractTask> tasks = new LinkedList<AbstractTask>();

        // set of index partition views to consider.
        final Iterator<ViewMetadata> itr = overflowMetadata.views();

        /*
         * A priority queue used to decide between optional compacting merges
         * and index segment builds. There is a common metric for this decision.
         * 
         * Note: The natural order of [Priority] is DESCENDING [largest
         * numerical value to smallest numerical value]! We assign larger scores
         * to the index partitions that we want to merge.
         */
        final PriorityQueue<Priority<ViewMetadata>> mergeQueue = new PriorityQueue<Priority<ViewMetadata>>(
                overflowMetadata.getIndexCount());

        while (itr.hasNext()) {

            final ViewMetadata vmd = itr.next();
            
            final String name = vmd.name;
            
            /*
             * The index partition has already been handled.
             */
            if (isUsed(name)) {

                if (log.isInfoEnabled())
                    log.info("was  handled: " + name);

                nskip++;

                ndone++;

                continue;

            }
            
            if (overflowMetadata.isCopied(name)) {

                /*
                 * The write set from the old journal was already copied to the
                 * new journal so we do not need to do a build.
                 */

                putUsed(name, "wasCopied(name=" + name + ")");

                if (log.isInfoEnabled())
                    log.info("was  copied : " + vmd);

                nskip++;

                ndone++;

                continue;
                
            }

            // manditory merge.
            if (compactingMerge || vmd.manditoryMerge) {

                /*
                 * Mandatory compacting merge.
                 */
                
                final AbstractTask task = new CompactingMergeTask(vmd);

                // add to set of tasks to be run.
                tasks.add(task);

                overflowMetadata.setAction(vmd.name, OverflowActionEnum.Merge);

                putUsed(name, "willManditoryMerge(" + vmd + ")");

                if (log.isInfoEnabled())
                    log.info("will merge    : " + vmd);

                nmerge++;
                
                ndone++;

                continue;
            
            }
            
            // the adjusted split handler.
            final ISplitHandler splitHandler = vmd.getAdjustedSplitHandler();
            
            /*
             * Tail split?
             * 
             * Note: We can do a tail split as long as we are "close" to a full
             * index partition. We have an expectation that the head of the
             * split will be over the minimum capacity. While the tail of the
             * split MIGHT be under the minimum capacity, if there are continued
             * heavy writes on the tail then it will should reach the minimum
             * capacity for an index partition by the time the live journal
             * overflows again.
             */
            if (!compactingMerge //
                    // move not in progress
                    && vmd.pmd.getSourcePartitionId() == -1//
                    // satisfies tail split criteria
                    && vmd.isTailSplit()//
                    ) {

                /*
                 * Do an index (tail) split task.
                 */

                final AbstractTask task = new SplitTailTask(vmd, null/* moveTarget */);

                // add to set of tasks to be run.
                tasks.add(task);

                overflowMetadata.setAction(vmd.name,
                        OverflowActionEnum.TailSplit);
                
                putUsed(name, "tailSplit(name=" + vmd + ")");

                if (log.isInfoEnabled())
                    log.info("will tailSpl: " + vmd);

                nsplit++;
                
                ndone++;
                
                continue;
                
            }
            
            /*
             * Should split?
             * 
             * Note: Split is NOT allowed if the index is currently being moved
             * onto this data service. Split, join, and move are all disallowed
             * until the index partition move is complete since each of them
             * would cause the index partition to become invalidated.
             * 
             * Note: [Split performs a compacting merge so it is allowed when
             * compactingMerge is true].
             */
            if (    // move not in progress
                    vmd.pmd.getSourcePartitionId() == -1//
                    // looks like a split candidate.
                    && splitHandler.shouldSplit(vmd.getRangeCount())//
            ) {

                /*
                 * Do an index split task.
                 */

                final AbstractTask task = new SplitIndexPartitionTask(vmd,
                        (UUID) null/* moveTarget */);

                // add to set of tasks to be run.
                tasks.add(task);

                overflowMetadata.setAction(vmd.name, OverflowActionEnum.Split);

                putUsed(name, "willSplit(name=" + vmd + ")");

                if (log.isInfoEnabled())
                    log.info("will split  : " + vmd);

                nsplit++;
                
                ndone++;
                
                continue;

            }
            
            /*
             * Compute a score that will be used to prioritize compacting merges
             * vs builds for index partitions where either option is allowable.
             * The higher the score, the more we want to make sure that we do a
             * compacting merge for that index.
             * 
             * Note: The main purpose of an index partition build is to convert
             * from a write-order to a read-order and permit the release of the
             * old journal. However, applications which require frequent access
             * to historical commit points on the old journals will continue to
             * rely on the write-order journals.
             * 
             * @todo if the application requires access to modest amounts of
             * history then consider a policy where the buffers are retained for
             * old journals up to the minReleaseAge. Of course, this can run
             * into memory constraints so that needs to be traded off against
             * IOWAIT.
             */
            final double mergePriority = (vmd.sourceJournalCount - 1) * 3
                    + vmd.sourceSegmentCount;

            // put into priority queue to be processed below.
            mergeQueue.add(new Priority<ViewMetadata>(mergePriority, vmd));
            
        } // itr.hasNext()

        /*
         * Assign merge or build actions to the remaining index partitions based
         * on the assigned priority.
         */
        while (!mergeQueue.isEmpty()) {
            
            final Priority<ViewMetadata> e = mergeQueue.poll();

            final ViewMetadata vmd = e.v;
                        
            // optional merge.
            if (nmerge < resourceManager.maximumOptionalMergesPerOverflow) {

                /*
                 * Select an optional compacting merge.
                 */
                
                final AbstractTask task = new CompactingMergeTask(vmd);

                // add to set of tasks to be run.
                tasks.add(task);

                overflowMetadata.setAction(vmd.name, OverflowActionEnum.Merge);

                putUsed(vmd.name, "willOptionalMerge(" + vmd + ")");

                if (log.isInfoEnabled())
                    log.info("will merge : " + vmd);

                nmerge++;
                
                ndone++;

            } else {

                /*
                 * Incremental build.
                 */
                
                final AbstractTask task = new IncrementalBuildTask(vmd);

                // add to set of tasks to be run.
                tasks.add(task);

                overflowMetadata.setAction(vmd.name, OverflowActionEnum.Build);
                
                putUsed(vmd.name, "willBuild(" + vmd + ")");

                if (log.isInfoEnabled())
                    log.info("will build: " + vmd);

                nbuild++;

                ndone++;

            }

        } // while : next index partition
        
        // verify counters.
        if (ndone != nskip + nbuild + nmerge + nsplit) {

            log.warn("ndone=" + ndone + ", but : nskip=" + nskip + ", nbuild="
                    + nbuild + ", ncompact=" + nmerge + ", nsplit=" + nsplit);

        }

        // verify all indices were handled in one way or another.
        if (ndone != used.size()) {

            log.warn("ndone=" + ndone + ", but #used=" + used.size());

        }

        return tasks;
        
    }
    
    /**
     * Note: This task is interrupted by {@link OverflowManager#shutdownNow()}.
     * Therefore is tests {@link Thread#isInterrupted()} and returns immediately
     * if it has been interrupted.
     * 
     * @return The return value is always null.
     * 
     * @throws Exception
     *             This implementation does not throw anything since there is no
     *             one to catch the exception. Instead it logs exceptions at a
     *             high log level.
     */
    public Object call() throws Exception {

        /*
         * Mark the purpose of the thread using the same variable name as the
         * AbstractTask.
         */
        MDC.put("taskname", "overflowService");
        
        if (resourceManager.overflowAllowed.get()) {

            // overflow must be disabled while we run this task.
            throw new AssertionError();

        }

        final long begin = System.currentTimeMillis();
        
        resourceManager.asynchronousOverflowStartMillis.set(begin);
        
        final Event e = new Event(resourceManager.getFederation(),
                new EventResource(), EventType.AsynchronousOverflow).addDetail(
                "asynchronousOverflowCounter",
                resourceManager.asynchronousOverflowCounter.get()).start();
        
        try {

            if(log.isInfoEnabled()) {
                
                // The pre-condition views.
                log.info("\npre-condition views: overflowCounter="
                        + resourceManager.asynchronousOverflowCounter.get()
                        + "\n"
                        + resourceManager.listIndexPartitions(TimestampUtility
                                .asHistoricalRead(lastCommitTime)));
                
            }
            
            // choose the tasks to be run.
            final List<AbstractTask> tasks = chooseTasks();
            
            runTasks(tasks);
            
            /*
             * Note: At this point we have the history as of the lastCommitTime
             * entirely contained in index segments. Also, since we constrained
             * the resource manager to refuse another overflow until we have
             * handle the old journal, all new writes are on the live index.
             */

            final long overflowCounter = resourceManager.asynchronousOverflowCounter
                    .incrementAndGet();

            log.warn("done: overflowCounter=" + overflowCounter
                    + ", lastCommitTime="
                    + resourceManager.getLiveJournal().getLastCommitTime()
                    + ", elapsed=" + (System.currentTimeMillis() - begin)
                    + "ms");

            // The post-condition views.
            if (log.isInfoEnabled())
                log.info("\npost-condition views: overflowCounter="
                        + resourceManager.asynchronousOverflowCounter.get()
                        + "\n"
                        + resourceManager.listIndexPartitions(ITx.UNISOLATED));

            /*
             * Note: I have moved the purge of old resources back into the
             * synchronous overflow logic. When the data service is heavily
             * loaded by write activity, it takes a while to obtain the
             * exclusive lock on the write service which we need to be able to
             * purge old resources and this can actually impact throughput.
             * However, we already have that lock when we are doing a
             * synchronous overflow so it makes to purge resources while we are
             * holding the lock.
             */
            
//            // purge resources that are no longer required.
//            resourceManager.getFederation().getExecutorService().submit(
//                    new PurgeResourcesAfterActionTask(resourceManager));
            
            return null;
            
        } catch(Throwable t) {
            
            /*
             * Note: This task is run normally from a Thread by the
             * ResourceManager so no one checks the Future for the task.
             * Therefore it is very important to log any errors here since
             * otherwise they will not be noticed.
             * 
             * At the same time, the resource manager can be shutdown at any
             * time. Asynchronous shutdown will provoke an exception here, but
             * those exceptions do not indicate a problem.
             */
            
            resourceManager.asyncOverflowFailedCounter.incrementAndGet();
            
            if(isNormalShutdown(t)) { 
                
                log.warn("Normal shutdown? : "+t);
                
            } else {
                
                log.error(t/*msg*/, t/*stack trace*/);
                
            }

            throw new RuntimeException( t );
            
        } finally {

            e.end();
            
            // enable overflow again as a post-condition.
            if (!resourceManager.overflowAllowed.compareAndSet(
                    false/* expect */, true/* set */)) {

                throw new AssertionError();

            }

            resourceManager.asynchronousOverflowMillis
                    .addAndGet(e.getElapsed());

            // clear references to the views so that they may GC'd more readily.
            overflowMetadata.clearViews();
            
        }

    }

//    /**
//     * Helper task used to purge resources <strong>after</strong> asynchronous
//     * overflow is complete.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static protected class PurgeResourcesAfterActionTask implements Callable<Void> {
//
//        private final OverflowManager overflowManager;
//        
//        public PurgeResourcesAfterActionTask(final OverflowManager overflowManager) {
//            
//            this.overflowManager = overflowManager;
//            
//        }
//        
//        /**
//         * Sleeps for a few seconds to give asynchronous overflow processing a
//         * chance to quit and release its hard reference on the old journal and
//         * then invokes {@link OverflowManager#purgeOldResources(long, boolean)}.
//         */
//        public Void call() throws Exception {
//            
//            // wait for the asynchronous overflow task to finish.
//            Thread.sleep(2000);
//
//            try {
//
//                final long timeout = overflowManager.getPurgeResourcesTimeout();
//
//                /*
//                 * Try to get the exclusive write service lock and then purge
//                 * resources.
//                 */
//                if (!overflowManager
//                        .purgeOldResources(timeout, false/* truncateJournal */)) {
//
//                    /*
//                     * This can become a serious problem if it persists since
//                     * the disk will fill up with old journals and index
//                     * segments.
//                     * 
//                     * @todo (progressively?) double the timeout if we are
//                     * nearing disk exhaustion
//                     */
//
//                    log.error("Purge resources did not run: service="
//                            + overflowManager.getFederation().getServiceName()
//                            + ", timeout=" + timeout);
//
//                }
//                
//            } catch (InterruptedException ex) {
//
//                // Ignore.
//
//            } catch (Throwable t) {
//
//                // log and ignore.
//                log.error("Problem purging old resources?", t);
//
//            }
//
//            return null;
//
//        }
//
//    }

    /**
     * Submit all tasks, awaiting their completion and check their futures for
     * errors.
     * 
     * @throws InterruptedException
     */
    protected void runTasks(final List<AbstractTask> tasks)
            throws InterruptedException {

        if (log.isInfoEnabled())
            log.info("begin : will run " + tasks.size() + " update tasks");

        if (resourceManager.overflowTasksConcurrent == 1) {
        
            runTasksInSingleThread(tasks);

        } else {

            runTasksConcurrent(tasks);
            
        }

        if (log.isInfoEnabled())
            log.info("end");
        
    }

    /**
     * Runs the overflow tasks one at a time, stopping when the journal needs to
     * overflow again, when we run out of time, or when there are no more tasks
     * to be executed.
     */
    protected void runTasksInSingleThread(final List<AbstractTask> tasks)
        throws InterruptedException {
        
        final ExecutorService executorService = Executors
                .newSingleThreadExecutor(DaemonThreadFactory
                        .defaultThreadFactory());

        try {

            final long begin = System.nanoTime();
            
            // remaining nanoseconds in which to execute overflow tasks.
            long nanos = TimeUnit.MILLISECONDS
                    .toNanos(resourceManager.overflowTimeout);
            
            final Iterator<AbstractTask> titr = tasks.iterator();

            int ndone = 0;
            
            while (titr.hasNext() && nanos > 0) {
                
                final boolean shouldOverflow = resourceManager
                        .isOverflowEnabled()
                        && resourceManager.shouldOverflow();
                
                if (shouldOverflow) {

                    if (resourceManager.overflowCancelledWhenJournalFull) {
                        
                        // end async overflow.
                        break;
                    
                    } else {

                        // issue warning since journal is already full again.
                        final long elapsed = (System.nanoTime() - begin);

                        log.warn("Overflow still running: elapsed="
                                + TimeUnit.NANOSECONDS.toMillis(elapsed));

                    }
                    
                }

                final AbstractTask task = titr.next();
                
                final Future<? extends Object> f = resourceManager
                        .getConcurrencyManager().submit(task);

                getFutureForTask(f, task, nanos, TimeUnit.NANOSECONDS);
                
                nanos -= (System.nanoTime() - begin);
                
                ndone++;
                
            }
            
            log.warn("Completed " + ndone + " out of " + tasks.size()
                    + " tasks");
            
        } finally {
            
            executorService.shutdownNow();
            
//            executorService.awaitTermination(arg0, arg1)
            
        }
        
    }

    /**
     * Runs the overflow tasks in parallel, cancelling any tasks which have not
     * completed if we run out of time. A dedicated thread pool is allocated for
     * this purpose. Depending on the configuration, it will be either a cached
     * thread pool (full parallelism) or a fixed thread pool (limited
     * parallelism).
     * 
     * @param tasks
     * 
     * @throws InterruptedException
     * 
     * @see {@link OverflowManager#overflowTasksConcurrent}
     */
    protected void runTasksConcurrent(final List<AbstractTask> tasks)
        throws InterruptedException {

        assert resourceManager.overflowTasksConcurrent >= 0;
        
        final ExecutorService executorService;
        final boolean shutdownAfter;
        if (resourceManager.overflowTasksConcurrent == 0) {

            // run all tasks in parallel on a shared service.
            executorService = resourceManager.getFederation()
                    .getExecutorService();
            
            shutdownAfter = false;

        } else {

            // run with limited parallelism on our own service.
            executorService = Executors.newFixedThreadPool(
                    resourceManager.overflowTasksConcurrent,
                    new DaemonThreadFactory(getClass().getName()));

            shutdownAfter = true;

        }

        try {
            
            /*
             * Note: On return tasks that are not completed are cancelled.
             */
            final List<Future> futures = resourceManager
                    .getConcurrencyManager().invokeAll(tasks,
                            resourceManager.overflowTimeout,
                            TimeUnit.MILLISECONDS);

            // Note: list is 1:1 correlated with [futures].
            final Iterator<AbstractTask> titr = tasks.iterator();

            // verify that all tasks completed successfully.
            for (Future<? extends Object> f : futures) {

                // the task for that future.
                final AbstractTask task = titr.next();

                /*
                 * Non-blocking: all tasks have already either completed or been
                 * canceled.
                 */

                getFutureForTask(f, task, 0L, TimeUnit.NANOSECONDS);

            }

        } finally {
            
            if(shutdownAfter) {

                /*
                 * Note: this test prevents us from shutting down the
                 * federation's thread pool!
                 */

                executorService.shutdownNow();
                
            }

            //        executorService.awaitTermination(arg0, arg1)

        }

    }
    
    /**
     * Note: An error here MAY be ignored. The index partition will remain
     * coherent and valid but its view will continue to have a dependency on the
     * old journal until a post-processing task for that index partition
     * succeeds.
     */
    private void getFutureForTask(final Future<? extends Object> f,
            final AbstractTask task, final long timeout, final TimeUnit unit) {

        try {

            f.get(timeout, unit);

            // elapsed execution time for the task.
            final long elapsed = TimeUnit.NANOSECONDS
                    .toMillis(task.nanoTime_finishedWork
                            - task.nanoTime_beginWork);

            if(log.isInfoEnabled())
                log.info("Task complete: elapsed=" + elapsed + ", task=" + task);

        } catch (Throwable t) {

            /*
             * Elapsed execution time for the task.
             * 
             * Note: finishedWork may be zero if a task was cancelled and we
             * look at its future before the task notices the interrupt. in such
             * cases the elapsed time reported here will be negative.
             */ 
            final long elapsed = TimeUnit.NANOSECONDS
                    .toMillis(task.nanoTime_finishedWork
                            - task.nanoTime_beginWork);

            if (t instanceof CancellationException) {

                log.warn("Task cancelled: elapsed=" + elapsed + ", task="
                        + task + " : " + t);

                resourceManager.asyncOverflowTaskCancelledCounter
                        .incrementAndGet();

            } else if (isNormalShutdown(t)) {

                log.warn("Normal shutdown? : elapsed=" + elapsed + ", task="
                        + task + " : " + t);

            } else {

                resourceManager.asyncOverflowTaskFailedCounter
                        .incrementAndGet();

                log.error("Child task failed: elapsed=" + elapsed + ", task="
                        + task + " : " + t, t);

            }

            // fall through!

        }

    }
    
    /**
     * These are all good indicators that the data service was shutdown.
     */
    protected boolean isNormalShutdown(final Throwable t) {

        return isNormalShutdown(resourceManager, t);
        
    }
    
    /**
     * These are all good indicators that the data service was shutdown.
     */
    static protected boolean isNormalShutdown(
            final ResourceManager resourceManager, final Throwable t) {

        if(Thread.currentThread().isInterrupted()) return true;
        
        if (!resourceManager.isRunning()
                || !resourceManager.getConcurrencyManager()
                        .isOpen()
                || InnerCause.isInnerCause(t,
                        InterruptedException.class)
// Note: cancelled indicates that overflow was timed out.
//                || InnerCause.isInnerCause(t,
//                        CancellationException.class)
                || InnerCause.isInnerCause(t,
                        ClosedByInterruptException.class)
                || InnerCause.isInnerCause(t,
                        ClosedChannelException.class)
                || InnerCause.isInnerCause(t,
                        AsynchronousCloseException.class)) {
            
            return true;
            
        }
        
        return false;
            
    }
    
    @SuppressWarnings("unchecked")
    static private final List<AbstractTask> EMPTY_LIST = Collections.EMPTY_LIST; 
    
}
