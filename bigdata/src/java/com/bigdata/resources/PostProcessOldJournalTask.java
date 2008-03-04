package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.Counters;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.MoveIndexPartitionTask.MoveResult;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.MetadataService;

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
 * {@link ResourceManager#overflow(boolean, boolean)}. It is run on the
 * {@link ResourceManager#service} so that its execution is asynchronous with
 * respect to the {@link IConcurrencyManager}. While it does not require any
 * locks for some of its processing stages, this task relies on the
 * {@link ResourceManager#overflowAllowed} flag to disallow additional overflow
 * operations until it has completed. The various actions taken by this task are
 * submitted as submits tasks to the {@link IConcurrencyManager} so that they
 * will obtain the appropriate locks as necessary on the named indices.
 * 
 * @todo write alternative to the "build" task (which is basically a full
 *       compacting merge) that builds an index segment from the buffered writes
 *       on the journal and updates the view. This would let the #of index
 *       segments grow before performing a full compacting merge on the index
 *       partition. The advantage is that we only need to consider the buffered
 *       writes for the index partition vs a full key range scan of the fused
 *       view for the index partition. This lighter weight operation will copy
 *       any deleted index entries in the btree write buffer as of the
 *       lastCommitState of the old journal, but it will typically copy less
 *       data than was buffered since overwrites will reduce to a single index
 *       entry.
 *       <P>
 *       Think of a pithy name, such as "compact" vs "build" or "build" vs
 *       "buffer". The distinction could also be "full compacting merge" vs
 *       "compacting merge" but "merge" sounds too close to "join" for me and
 *       those labels are too long.
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
 * @todo if an index partition is moved (or split or joined) while an active
 *       transaction has a write set for that index partition on a data service
 *       then we may need to move (or split and move) the transaction write set
 *       before it can be validated.
 * 
 * @todo its possible to play "catch up" on a "hot for write" index by copying
 *       behind the commit point on the live journal to the target journal in
 *       order to minimize the duration of the unisolated operation that handles
 *       the atomic cut over of the index to its new host. it might also be
 *       possible to duplicate the writes on the new host while they continue to
 *       be absorbed on the old host, but it would seem to be difficult to get
 *       the conditions just right for that.
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

    /** Aggregated counters for the named indices. */ 
    private final Counters totalCounters;
    
    /** Raw score computed for those aggregated counters. */
    private final double totalRawStore;
    
    /** Individual counters for the named indices. */
    private final Map<String/*name*/,Counters> indexCounters;

    /**
     * Scores computed for each named index in order by ascending score
     * (increased activity).
     */
    private final Score[] scores;
    /**
     * Random access to the index {@link Score}s.
     */
    private final Map<String,Score> scoreMap;
    
    /**
     * Helper class assigns a raw and a normalized score to each index based on
     * its per-index {@link Counters} and on the global (non-restart safe)
     * {@link Counters} for the data service during the life cycle of the last
     * journal.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class Score implements Comparable<Score>{

        /** The name of the index partition. */
        public final String name;
        /** The counters collected for that index partition. */
        public final Counters counters;
        /** The raw score computed for that index partition. */
        public final double rawScore;
        /** The normalized score computed for that index partition. */
        public final double score;
        /** The rank in [0:#scored].  This is an index into the Scores[]. */
        public int rank = -1;
        /** The normalized double precision rank in [0.0:1.0]. */
        public double drank = -1d;
        
        public String toString() {
            
            return "Score{name=" + name + ", rawScore=" + rawScore + ", score="
                    + score + ", rank=" + rank + ", drank=" + drank + "}";
            
        }
        
        public Score(String name,Counters counters, double totalRawScore) {
            
            assert name != null;
            
            assert counters != null;
            
            this.name = name;
            
            this.counters = counters;
            
            rawScore = counters.computeRawScore();
            
            score = Counters.normalize( rawScore , totalRawScore );
            
        }

        /**
         * Places elements into order by ascending {@link #rawScore}. The
         * {@link #name} is used to break any ties.
         */
        public int compareTo(Score arg0) {
            
            if(rawScore < arg0.rawScore) {
                
                return -1;
                
            } else if(rawScore> arg0.rawScore ) {
                
                return 1;
                
            }
            
            return name.compareTo(arg0.name);
            
        }
        
    }
    
    /**
     * Return <code>true</code> if the named index partition is "warm" for
     * {@link ITx#UNISOLATED} and/or {@link ITx#READ_COMMITTED} operations.
     * <p>
     * Note: This method informs the selection of index partitions that will be
     * moved to another {@link IDataService}. The preference is always to move
     * an index partition that is "warm" rather than "hot" or "cold". Moving a
     * "hot" index partition causes more latency since more writes will have
     * been buffered and unisolated access to the index partition will be
     * suspended during the atomic part of the move operation. Likewise, "cold"
     * index partitions are not consuming any resources other than disk space
     * for their history, and the history of an index is not moved when the
     * index partition is moved.
     * <p>
     * Since the history of an index partition is not moved when the index
     * partition is moved, the determination of cold, warm or hot is made in
     * terms of the resources consumed by {@link ITx#READ_COMMITTED} and
     * {@link ITx#UNISOLATED} access to named index partitions. If an index
     * partition is hot for historical read, then your only choices are to shed
     * other index partitions from the data service, to read from a failover
     * data service having the same index partition, or possibly to increase the
     * replication count for the index partition.
     * 
     * @param name
     *            The name of an index partition.
     * 
     * @return The index {@link Score} -or- <code>null</code> iff the index
     *         was not touched for read-committed or unisolated operations.
     */
    public Score getScore(String name) {
        
        Score score = scoreMap.get(name);
        
        if(score == null) {
            
            /*
             * Index was not touched for read-committed or unisolated
             * operations.
             */

            ResourceManager.log.info("Index is cold: "+name);
            
            return null;
            
        }
        
        ResourceManager.log.info("Index score: "+score);
        
        return score;
        
    }
    
    /**
     * 
     * 
     * @param resourceManager
     * @param lastCommitTime
     *            The lastCommitTime on the old journal.
     * @param totalCounters
     *            The total {@link Counters} reported for all unisolated and
     *            read-committed index views on the old journal.
     * @param indexCounters
     *            The per-index {@link Counters} for the unisolated and
     *            read-committed index views on the old journal.
     */
    public PostProcessOldJournalTask(ResourceManager resourceManager,
            long lastCommitTime, Counters totalCounters,
            Map<String/* name */, Counters> indexCounters) {

        if (resourceManager == null)
            throw new IllegalArgumentException();

        if( lastCommitTime <= 0)
            throw new IllegalArgumentException();

        if (totalCounters == null)
            throw new IllegalArgumentException();

        if (indexCounters == null)
            throw new IllegalArgumentException();

        this.resourceManager = resourceManager;

        this.lastCommitTime = lastCommitTime;

        this.totalCounters = totalCounters;
        
        this.indexCounters = indexCounters;

        final int nscores = indexCounters.size();
        
        this.scores = new Score[nscores];

        this.scoreMap = new HashMap<String/*name*/,Score>(nscores);

        this.totalRawStore = totalCounters.computeRawScore();

        if (nscores > 0) {

            final Iterator<Map.Entry<String, Counters>> itr = indexCounters
                    .entrySet().iterator();

            int i = 0;

            while (itr.hasNext()) {

                final Map.Entry<String, Counters> entry = itr.next();

                final String name = entry.getKey();

                final Counters counters = entry.getValue();

                scores[i] = new Score(name, counters, totalRawStore);

                i++;

            }

            // sort into ascending order (inceasing activity).
            Arrays.sort(scores);
            
            ResourceManager.log.info("The most active index was: "
                    + scores[scores.length - 1]);

            ResourceManager.log
                    .info("The least active index was: " + scores[0]);

            for(i=0;i<scores.length; i++) {
                
                scores[i].rank = i;
                
                scores[i].drank = ((double)i)/scores.length;
                
                scoreMap.put(scores[i].name, scores[i]);
                
            }
            
        }

    }

    /**
     * Examine each named index on the old journal and decide what, if anything,
     * to do with that index. These indices are key range partitions of named
     * scale-out indices. The {@link LocalPartitionMetadata} describes the key
     * rage partition and identifies the historical resources required to
     * present a coherent view of that index partition.
     * <p>
     * 
     * <h2> Build </h2>
     * 
     * A build is performed when there are buffered writes, when they buffered
     * writes were not simply copied onto the new journal during the atomic
     * overflow operation, and when the index partition is neither overcapacity
     * nor undercapacity.
     * 
     * <h2> Split </h2>
     * 
     * A split is considered when an index partition appears to be overcapacity.
     * The split operation will inspect the index partition in more detail when
     * it runs. If the index partition does not, in fact, have sufficient
     * capacity to warrant a split then a build will be performed instead (the
     * build is treated more or less as a one-to-one split). An index partition
     * which is WAY overcapacity can be split into more than 2 new index
     * partitions.
     * 
     * <h2> Join </h2>
     * 
     * A join is considered when an index partition is undercapacity. Since the
     * rightSeparatorKey for an index partition is also the key under which the
     * rightSibling would be found, we use the rightSeparatorKey to lookup the
     * rightSibling of an index partition in the {@link MetadataIndex}. If that
     * rightSibling is local (same {@link ResourceManager}) then we will JOIN
     * the index partitions. Otherwise we will MOVE the undercapacity index
     * partition to the {@link IDataService} on which its rightSibling was
     * found.
     * 
     * <h2> Move </h2>
     * 
     * We move index partitions around in order to make the use of CPU, RAM and
     * DISK resources more even across the federation and prevent hosts or data
     * services from being either under- or over-utilized. Index partition moves
     * are necessary when a scale-out index is relatively new in to distribute
     * the index over more than a single data service. Index partition moves are
     * is important when a host is overloaded, especially when it is approaching
     * resource exhaustion. However, index partition moves DO NOT release ANY
     * DISK space on a host since only the current state of the index partition
     * is moved, not its historical states (which are on a mixture of journals
     * and index segments). When a host is close to exhausting its DISK space
     * temporary files should be purge and the resource manager may aggressively
     * release old resources, even at the expense of forcing transactions to
     * abort.
     * 
     * <p>
     * 
     * We generally choose to move "warm" or "hot" index partitions. Cold index
     * partitions are not consuming any CPU/RAM/IO resources and moving them to
     * another host will not effect the utilization of either the source or the
     * target host. Moving "warm" index partitions is preferrable since it will
     * introduce less latency when we suspect writes on the index partition for
     * the atomic cutover to the new data service.
     * 
     * <p>
     * 
     * @todo If this is a high utilization node and there are low utilization
     *       nodes then we move some index partitions to those nodes in order to
     *       improve the distribution of resources across the federation. it's
     *       probably best to shed "warm" index partitions since we will have to
     *       suspend writes on the index partition during the atomic stage of
     *       the move. we need access to some outside context in order to make
     *       this decision.
     *       <p>
     *       Note utilization should be defined in terms of transient system
     *       resources : CPU, IO (DISK and NET), RAM. Running out of DISK space
     *       causes an urgent condition and can lead to node failure and it
     *       makes sense to shed all indices that are "hot for write" from a
     *       node that is low on disk space, but DISK space does not otherwise
     *       determine node utilization. These system resources should probably
     *       be measured with counters (Windows) or systat (linux) as it is
     *       otherwise difficult to measure these things from Java. This data
     *       needs to get reported to a centralized service which can then be
     *       queried to identify underutilized data services - that method is on
     *       the {@link IMetadataService} right now but could be its own service
     *       since reporting needs to be done to that service as well.
     *       <p>
     *       When a federation is newly deployed on a cluster, or when new
     *       hardware is made available, node utilization discrepancies should
     *       become immediately apparent and index partitions should be moved on
     *       the next overflow. This will help to distribute the load over the
     *       available hardware.
     *       <p>
     *       We can choose which index partitions to move fairly liberally, but
     *       moving an index partition which is "hot for write" will impose a
     *       noticable latency while moving ones that are "hot for read" will
     *       not - this is because the "hot for write" partition will have
     *       absorbed more writes on the journal while we are moving the data
     *       from the old view and we will need to move those writes as well.
     *       When we move those writes the index will be unavailable for write
     *       until it appears on the target data service.
     *       <p>
     *       Indices typically have many commit points, and any one of them
     *       could become hot for read. However, moving an index partition is
     *       not going to reduce the load on the old node for those historical
     *       reads since we only move the current state of the index, not its
     *       history. Nodes that are hot for historical reads spots should be
     *       handled by increasing its replication count and reading from the
     *       secondary data services. Note that {@link ITx#READ_COMMITTED} and
     *       {@link ITx#UNISOLATED} should both count against the "live" index -
     *       read-committed reads always track the most recent state of the
     *       index and would be moved if the index was moved.
     *       <p>
     *       Bottom line: if a node is hot for historical read then increase the
     *       replication count and read from failover services. if a node is hot
     *       for read-committed and unisolated operations then move one or more
     *       of the hot read-committed/unisolated index partitions to a node
     *       with less utilization.
     *       <p>
     *       Index partitions that get a lot of action are NOT candidates for
     *       moves unless the node itself is either overutilized or other nodes
     *       are at very low utilization.
     * 
     * @todo A host with enough CPU/RAM/IO/DISK can support more than one data
     *       service thereby using more than 2G of RAM (a limit on 32 bit
     *       hosts). In this case it is important that we can move index
     *       partitions between those data services or the additional resources
     *       will not be fully exploited. We do this by looking at not just host
     *       utilization but also at process utilization, where the process is a
     *       data service.
     */
    protected List<AbstractTask> chooseTasks() throws Exception {

        ResourceManager.log.info("begin: lastCommitTime=" + lastCommitTime);

        // the old journal.
        final AbstractJournal oldJournal = resourceManager
                .getJournal(lastCommitTime);

        // tasks to be created (capacity of the array is estimated).
        final List<AbstractTask> tasks = new ArrayList<AbstractTask>(
                (int) oldJournal.getName2Addr().rangeCount(null, null));

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

            /*
             * Set true if this node/data service is highly utilized. We will
             * consider moves IFF this is a highly utilized service.
             * 
             * FIXME set highlyUtilizedService based on recent load for this
             * host and service.
             */
            final boolean highlyUtilizedService = true;

            /*
             * Obtain some data service UUIDs onto which we will try and offload
             * some index partitions iff this data service is deemed to be
             * highly utilized.
             */
            
            UUID[] underUtilizedDataServiceUUIDs = null;

            if (highlyUtilizedService) {
                
                try {

                    underUtilizedDataServiceUUIDs = resourceManager
                            .getMetadataService().getUnderUtilizedDataServices(
                                    0/* limit */,
                                    resourceManager.getDataServiceUUID());

                } catch (IOException ex) {

                    ResourceManager.log.warn(
                            "Could not obtain target service UUIDs: ", ex);

                }
                
            }
            
            // counters : must sum to ndone as post-condition.
            int ndone = 0; // for each named index we process
            int nskip = 0; // nothing.
            int nbuild = 0; // build task.
            int nsplit = 0; // split task.
            int njoin = 0; // join task _candidate_.
            int nmove = 0; // move task (to underutilized node).

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
                    didCopy = resourceManager.copyIndexThreshold != 0
                            && entryCount < resourceManager.copyIndexThreshold;

                    if (didCopy) {

                        ResourceManager.log
                                .warn("Will not build index segment: name="
                                        + name
                                        + ", entryCount="
                                        + entryCount
                                        + ", threshold="
                                        + resourceManager.copyIndexThreshold);

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

                    // index partition metadata
                    final LocalPartitionMetadata pmd = indexMetadata.getPartitionMetadata();
                    
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

                        ResourceManager.log.info("index split: " + name);

                        nsplit++;

                    } else if (splitHandler != null
                            && pmd.getRightSeparatorKey() != null
                            && splitHandler.shouldJoin(view)) {

                        /*
                         * Add to the set of index partitions that are
                         * candidates for join operations.
                         * 
                         * Note: joins are only considered when the rightSibling
                         * of an index partition exists. The last index
                         * partition has [rightSeparatorKey == null] and there
                         * is no rightSibling for that index partition.
                         * 
                         * Note: If we decide to NOT join this with another
                         * local partition then we MUST do an index segment
                         * build in order to release the dependency on the old
                         * journal. This is handled below when we consider the
                         * JOIN candidates that were discovered in this loop.
                         */

                        final String scaleOutIndexName = indexMetadata
                                .getName();

                        BTree tmp = undercapacityIndexPartitions
                                .get(scaleOutIndexName);

                        if (tmp == null) {

                            tmp = BTree.create(tmpStore, new IndexMetadata(UUID
                                    .randomUUID()));

                            undercapacityIndexPartitions.put(scaleOutIndexName,
                                    tmp);

                        }

                        tmp.insert(pmd.getLeftSeparatorKey(), pmd);

                        ResourceManager.log.info("join candidate: " + name);

                        njoin++;

                    } else {
                        
                        boolean moved = false;
                        
                        /*
                         * Note: This makes sure that we don't do a move if
                         * there are only a few index partitions on this data
                         * service.
                         */
                        
                        final int MIN_INDEX_PARTITIONS_BEFORE_MOVE = 3;
                        
                        /*
                         * Note: We make sure that we do not move all the
                         * index partitions to the same target by moving at
                         * most M index partitions per under-utilized data
                         * service recommended to us.
                         */
                        final int MAX_MOVES_PER_TARGET = 3;
                        
                        final int maxMoves = MAX_MOVES_PER_TARGET * underUtilizedDataServiceUUIDs.length;
                        
                        if (highlyUtilizedService
                                && underUtilizedDataServiceUUIDs != null
                                && scores.length > MIN_INDEX_PARTITIONS_BEFORE_MOVE
                                && nmove < maxMoves) {

                            /*
                             * Note: We make sure that we don't move all the
                             * hot/warm index partitions by choosing the index
                             * partitions to be moved from the middle of the
                             * range.
                             * 
                             * Note: This could lead to a failure to recommend a
                             * move if all the "warm" index partitions happen to
                             * require a split or join. However, this is
                             * unlikely since the "warm" index partitions change
                             * size, if not slowly, then at least not as rapidly
                             * as the hot index partitions. Eventually a lot of
                             * splits will lead to some index partition not
                             * being split during an overflow and then we can
                             * move it.
                             * 
                             * @todo could adjust the bounds here based on how
                             * important it is to begin moving index partitions
                             * off of this data service.
                             */

                            final Score score = getScore(name);

                            if (score != null && score.drank > .3
                                    && score.drank < .8) {

                                /*
                                 * Move this index partition to an
                                 * under-utilized data service.
                                 */

                                // choose the target round robin among those offered to us.
                                final UUID targetDataServiceUUID = underUtilizedDataServiceUUIDs[nmove
                                        % underUtilizedDataServiceUUIDs.length];
                                
                                final AbstractTask task = new MoveIndexPartitionTask(
                                        resourceManager, resourceManager
                                                .getConcurrencyManager(),
                                        lastCommitTime, name,
                                        targetDataServiceUUID);

                                tasks.add(task);

                                nmove++;

                                moved = true;

                            }

                        }

                        if (!moved) {

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

                            ResourceManager.log.info("index build: " + name);

                            // add to set of tasks to be run.
                            tasks.add(task);

                            nbuild++;
                            
                        }
                        
                    }

                } // if(!didCopy)

                ndone++;

            } // itr.hasNext()

            // verify counters.
            assert ndone == nskip + nbuild + nsplit + njoin + nmove : "ndone="
                    + ndone + ", nskip=" + nskip + ", nbuild=" + nbuild
                    + ", nsplit=" + nsplit + ", njoin=" + njoin + ", nmove";

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
//            int nbuild = 0; // 
            
            while (itr.hasNext()) {

                final Map.Entry<String, BTree> entry = itr.next();

                // the name of the scale-out index.
                final String scaleOutIndexName = entry.getKey();

                ResourceManager.log.info("Considering join candidates: "
                        + scaleOutIndexName);

                // keys := leftSeparator; value := LocalPartitionMetadata
                final BTree tmp = entry.getValue();

                // #of underutilized index partitions for that scale-out index.
                final int ncandidates = tmp.getEntryCount();
                
                assert ncandidates > 0 : "Expecting at least one candidate";
                
                final ITupleIterator titr = tmp.entryIterator();

                /*
                 * Setup a BatchLookup query designed to locate the rightSibling
                 * of each underutilized index partition for the current
                 * scale-out index.
                 * 
                 * Note: This approach makes it impossible to join the last
                 * index partition when it is underutilized since it does not,
                 * by definition, have a rightSibling. However, the last index
                 * partition always has an open key range and is far more likely
                 * than any other index partition to recieve new writes.
                 */

                ResourceManager.log
                        .info("Formulating rightSiblings query="
                                + scaleOutIndexName + ", #underutilized="
                                + ncandidates);
                
                final byte[][] keys = new byte[ncandidates][];
                
                final LocalPartitionMetadata[] underUtilizedPartitions = new LocalPartitionMetadata[ncandidates];
                
                int i = 0;
                
                while (titr.hasNext()) {

                    ITuple tuple = titr.next();

                    LocalPartitionMetadata pmd = (LocalPartitionMetadata) SerializerUtil
                            .deserialize(tuple.getValue());

                    underUtilizedPartitions[i] = pmd;

                    /*
                     * Note: the right separator key is also the key under which
                     * we will find the rightSibling.
                     * 
                     */
                    
                    if (pmd.getRightSeparatorKey() == null) {

                        throw new AssertionError(
                                "The last index partition may not be a join candidate: name="
                                        + scaleOutIndexName + ", " + pmd);

                    }
                    
                    keys[i] = pmd.getRightSeparatorKey();
                    
                    i++;
                    
                } // next underutilized index partition.

                ResourceManager.log
                        .info("Looking for rightSiblings: name=" + scaleOutIndexName
                                + ", #underutilized=" + ncandidates);

                final BatchLookup op = new BatchLookup(ncandidates, 0/*offset*/, keys);

                final ResultBuffer resultBuffer = (ResultBuffer) resourceManager
                        .getMetadataService()
                        .submit(
                                -lastCommitTime,
                                MetadataService
                                        .getMetadataIndexName(scaleOutIndexName),
                                op);

                final UUID sourceDataService = resourceManager
                        .getDataServiceUUID();

                for (i = 0; i < ncandidates; i++) {

                    final LocalPartitionMetadata pmd = underUtilizedPartitions[i];
                    
                    final PartitionLocator rightSiblingLocator = (PartitionLocator) SerializerUtil
                            .deserialize(resultBuffer.getResult()[i]);

                    final UUID targetDataServiceUUID = rightSiblingLocator.getDataServices()[0];

                    if (sourceDataService.equals(targetDataServiceUUID)) {

                        /*
                         * JOIN underutilized index partition with its right
                         * sibling on same data service.
                         * 
                         * Note: This is only joining two index partitions at a
                         * time. It's possible to do more than that if it
                         * happens that N > 2 underutilized sibling index
                         * partitions are on the same data service, but that is
                         * a relatively unlikley combination of events.
                         */
                        
                        final String[] resources = new String[2];
                        
                        resources[0] = DataService
                                .getIndexPartitionName(scaleOutIndexName,
                                        underUtilizedPartitions[i].getPartitionId());
                        
                        resources[1] = DataService.getIndexPartitionName(
                                scaleOutIndexName, rightSiblingLocator.getPartitionId());

                        ResourceManager.log.info("Will JOIN: "+Arrays.toString(resources));
                        
                        final AbstractTask task = new JoinIndexPartitionTask(
                                    resourceManager, resourceManager
                                            .getConcurrencyManager(),
                                    lastCommitTime, resources);

                        // add to set of tasks to be run.
                        tasks.add(task);
                        
                        njoin++;

                    } else {
                        
                        /*
                         * MOVE underutilized index partition to data service
                         * hosting the right sibling.
                         */
                        
                        final String sourceIndexName = DataService
                                .getIndexPartitionName(scaleOutIndexName, pmd
                                        .getPartitionId());

                        final AbstractTask task = new MoveIndexPartitionTask(
                                resourceManager, resourceManager
                                        .getConcurrencyManager(),
                                lastCommitTime, sourceIndexName,
                                targetDataServiceUUID);

                        tasks.add(task);
                        
                        nmove++;
                        
                    }

                    ndone++;
                    
                }
                
            } // next scale-out index with underutilized index partition(s).

            assert ndone == njoin + nmove; // + nbuild;
            
        } // consider JOIN candidates.

        ResourceManager.log.info("end");

        return tasks;

    }

    /*
     * Do a build since we will not do a join.
     *

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
                lastCommitTime, name, outFile);

        // add to set of tasks to be run.
        tasks.add(task);

     */
    
    protected List<AbstractTask> runTasks(List<AbstractTask> inputTasks)
            throws Exception {

        ResourceManager.log.info("Will run " + inputTasks.size() + " tasks");

        // submit all tasks, awaiting their completion.
        final List<Future<Object>> futures = resourceManager
                .getConcurrencyManager().invokeAll(inputTasks);

        final List<AbstractTask> updateTasks = new ArrayList<AbstractTask>(
                inputTasks.size());

        // verify that all tasks completed successfully.
        for (Future<Object> f : futures) {

            /*
             * @todo Is it Ok to trap exception and continue for non-errored
             * source index partitions?
             */

            final AbstractResult tmp = (AbstractResult) f.get();

            assert tmp != null;
            
            if (tmp instanceof BuildResult) {

                /*
                 * We ran an index segment build and we update the index
                 * partition metadata now.
                 */

                final BuildResult result = (BuildResult) tmp;

                // task will update the index partition view definition.
                final AbstractTask task = new UpdateIndexPartition(
                        resourceManager, resourceManager
                                .getConcurrencyManager(), result.name,
                        result.segmentMetadata);

                // add to set of tasks to be run.
                updateTasks.add(task);

            } else if (tmp instanceof SplitResult) {

                /*
                 * Now run an UNISOLATED operation that splits the live index
                 * using the same split points, generating new index partitions
                 * with new partition identifiers. The old index partition is
                 * deleted as a post-condition. The new index partitions are
                 * registered as a post-condition.
                 */

                final SplitResult result = (SplitResult) tmp;

                final AbstractTask task = new UpdateSplitIndexPartition(
                        resourceManager, resourceManager
                                .getConcurrencyManager(), result.name,
                        result);

                // add to set of tasks to be run.
                updateTasks.add(task);

            } else if (tmp instanceof JoinResult) {

                final JoinResult result = (JoinResult) tmp;

                // The array of index names on which we will need an exclusive
                // lock.
                final String[] names = new String[result.oldnames.length + 1];

                names[0] = result.name;

                System.arraycopy(result.oldnames, 0, names, 1,
                        result.oldnames.length);

                // The task to make the atomic updates on the live journal and
                // the metadata index.
                final AbstractTask task = new UpdateJoinIndexPartition(
                        resourceManager, resourceManager
                                .getConcurrencyManager(), names, result);

                // add to set of tasks to be run.
                updateTasks.add(task);

            } else if(tmp instanceof MoveResult) {

                final MoveResult result = (MoveResult) tmp;

                AbstractTask task = new UpdateMoveIndexPartitionTask(
                        resourceManager, resourceManager
                                .getConcurrencyManager(), result.name, result);

                // add to set of tasks to be run.
                updateTasks.add(task);
                
            } else {

                throw new AssertionError("Unexpected result type: " + tmp.toString());

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
        final List<Future<Object>> futures = resourceManager
                .getConcurrencyManager().invokeAll(tasks);

        // verify that all tasks completed successfully.
        for (Future<Object> f : futures) {

            /*
             * @todo error handling?
             */
            f.get();

        }

        ResourceManager.log.info("end");

    }

    /**
     * @return The return value is always null.
     * 
     * @throws Exception
     *             This implementation does not throw anything since there is no
     *             one to catch the exception. Instead it logs exceptions at a
     *             high log level.
     */
    public Object call() throws Exception {

        try {

            if (resourceManager.overflowAllowed.get()) {

            // overflow must be disabled while we run this task.
                throw new AssertionError();

            }

            tmpStore = new TemporaryRawStore();

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
            
            final long overflowCounter = resourceManager.overflowCounter
                    .incrementAndGet();

            ResourceManager.log
                    .info("Done: overflowCounter=" + overflowCounter);
            
            return null;
            
        } catch(Throwable t) {
            
            /*
             * Note: This task is run normally from a Thread by the
             * ResourceManager so no one checks the Future for the task.
             * Therefore it is very important to log any errors here since
             * otherwise they will not be noticed.
             */
            
            ResourceManager.log.error(t/*msg*/, t/*stack trace*/);

            throw new RuntimeException( t );
            
        } finally {

            // enable overflow again as a post-condition.
            if (!resourceManager.overflowAllowed.compareAndSet(false/*expect*/, true/*set*/)) {

                throw new AssertionError();

            }

            if (tmpStore != null) {

                tmpStore.closeAndDelete();

            }

        }

    }

}