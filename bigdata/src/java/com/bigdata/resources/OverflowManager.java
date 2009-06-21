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
 * Created on Mar 25, 2008
 */

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.BufferedDiskStrategy;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.EventType;
import com.bigdata.service.IDataService;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.DataService.IDataServiceCounters;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Class encapsulates logic for handling journal overflow events. Overflow is
 * triggered automatically when the user data extent on the journal nears a
 * configured threshold. Once the preconditions for overflow are satisfied,
 * the {@link WriteExecutorService}s for the journal are paused and all running
 * tasks on those services are allowed to complete and commit. Once no writers
 * are running, the {@link WriteExecutorService} triggers synchronous overflow.
 * Synchronous overflow is a low-latency process which creates a new journal to
 * absorb future writes, re-defines the views for all index partitions found on
 * the old journal to include the new journal as their first source, and
 * initiates a background thread performing asynchronous overflow
 * post-processing.
 * <p>
 * Asynchronous overflow post-processing is responsible for identifying index
 * partitions overflow (resulting in a split into two or more index partitions),
 * index partition underflow (resulting in the join of the under-capacity index
 * partition with its rightSibling), index partition moves (the index partition
 * is moved to a different {@link DataService}), and index partition builds (an
 * {@link IndexSegment} is created from the current view in what is effectively
 * a compacting merge). Overflow processing is suspended during asynchronous
 * post-processing, but is automatically re-enabled once post-processing
 * completes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class OverflowManager extends IndexManager {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(OverflowManager.class);

    /**
     * @see Options#COPY_INDEX_THRESHOLD
     */
    final protected int copyIndexThreshold;
    
    /**
     * @see Options#ACCELERATE_SPLIT_THRESHOLD
     */
    final protected int accelerateSplitThreshold; 
    
    /**
     * @see Options#PERCENT_OF_SPLIT_THRESHOLD
     */
    final protected double percentOfSplitThreshold;

    /**
     * @see Options#TAIL_SPLIT_THRESHOLD
     */
    final protected double tailSplitThreshold;

    /**
     * @see Options#HOT_SPLIT_THRESHOLD
     */
    final protected double hotSplitThreshold;

    /**
     * @see Options#SCATTER_SPLIT_ENABLED
     */
    final protected boolean scatterSplitEnabled;

    /**
     * @see Options#JOINS_ENABLED
     */
    final protected boolean joinsEnabled;
    
    /**
     * @see Options#MINIMUM_ACTIVE_INDEX_PARTITIONS 
     */
    protected final int minimumActiveIndexPartitions;
    
    /**
     * @see Options#MAXIMUM_MOVES
     */
    protected final int maximumMoves;

    /**
     * @see Options#MAXIMUM_MOVES_PER_TARGET
     */
    protected final int maximumMovesPerTarget;

    /**
     * @see Options#MOVE_PERCENT_CPU_TIME_THRESHOLD
     */
    protected final double movePercentCpuTimeThreshold;
    
    /**
     * The maximum #of optional compacting merge operations that will be
     * performed during a single overflow event.
     * 
     * @see Options#MAXIMUM_OPTIONAL_MERGES_PER_OVERFLOW
     */
    protected final int maximumOptionalMergesPerOverflow;
    
    /**
     * @see Options#MAXIMUM_JOURNALS_PER_VIEW
     */
    protected final int maximumJournalsPerView;
    
    /**
     * @see Options#MAXIMUM_SEGMENTS_PER_VIEW
     */
    protected final int maximumSegmentsPerView;
    
    /**
     * @see Options#MAXIMUM_BUILD_SEGMENT_BYTES
     */
    final protected long maximumBuildSegmentBytes;
    
    /**
     * The timeout for {@link #shutdown()} -or- ZERO (0L) to wait for ever.
     * 
     * @see IServiceShutdown#SHUTDOWN_TIMEOUT
     */
    final private long shutdownTimeout;

    /**
     * The service that runs the asynchronous overflow
     * {@link AsynchronousOverflowTask}.
     */
    private final ExecutorService overflowService;

    /**
     * @see Options#OVERFLOW_ENABLED
     */
    private final boolean overflowEnabled;
    
    /**
     * @see Options#OVERFLOW_MAX_COUNT
     */
    private final int overflowMaxCount;
    
    /**
     * @see Options#OVERFLOW_THRESHOLD
     */
    protected final double overflowThreshold;
    
    /**
     * A flag used to disable overflow of the live journal until asynchronous
     * post-processing of the old journal has been completed.
     * 
     * @see AsynchronousOverflowTask
     */
    protected final AtomicBoolean overflowAllowed = new AtomicBoolean(true);
    
    /**
     * A flag used to disable the asynchronous overflow processing for some unit
     * tests.
     */
    protected final AtomicBoolean asyncOverflowEnabled = new AtomicBoolean(true);
    
    /**
     * #of synchronous overflows that have taken place. This counter is
     * incremented each time the synchronous overflow operation is complete.
     */
    protected final AtomicLong synchronousOverflowCounter = new AtomicLong(0L);

    /**
     * The elapsed milliseconds for synchronous overflow processing to date.  
     */
    protected final AtomicLong synchronousOverflowMillis = new AtomicLong(0L);

    /**
     * The time in milliseconds since the epoch at which the most recent asynchronous overflow started. This is
     * used to compute the elapsed asynchronous overflow time when the service
     * is currently performing asynchronous overflow processing. The value is
     * set each time asynchronous overflow processing starts, but never cleared.
     */
    protected final AtomicLong asynchronousOverflowStartMillis = new AtomicLong(0L);
    
    /**
     * #of asynchronous overflows that have taken place. This counter is
     * incremented each time the entire overflow operation is complete,
     * including any asynchronous post-processing of the old journal.
     */
    protected final AtomicLong asynchronousOverflowCounter = new AtomicLong(0L);

    /**
     * The elapsed milliseconds for asynchronous overflow processing to date.  
     */
    protected final AtomicLong asynchronousOverflowMillis = new AtomicLong(0L);
    
    /**
     * A flag that may be set to force the next overflow to perform a compacting
     * merge for all indices that are not simply copied over to the new journal.
     * This may be used to minimize the footprint of the {@link StoreManager} on
     * the disk for the current view. However, note that you must also arrange
     * to purge any unused resources after the compacting merge overflow in
     * order to regain storage associated with views older than the current
     * releaseTime.
     */
    public final AtomicBoolean compactingMerge = new AtomicBoolean(false);
    
    /**
     * The #of asynchronous overflow operations which fail.
     * 
     * @see AsynchronousOverflowTask
     */
    protected final AtomicLong asyncOverflowFailedCounter = new AtomicLong(0L);

    /**
     * The #of asynchronous overflow tasks (index partition splits, joins, or moves)
     * that failed.
     */
    protected final AtomicLong asyncOverflowTaskFailedCounter = new AtomicLong(0L);

    /**
     * The #of asynchronous overflow tasks (index partition splits, joins, or
     * moves) that were canceled due to timeout.
     * 
     * @see Options#OVERFLOW_TIMEOUT
     */
    protected final AtomicLong asyncOverflowTaskCancelledCounter = new AtomicLong(0L);

    /**
     * #of successful index partition incremental build operations.
     */
    protected final AtomicLong indexPartitionBuildCounter = new AtomicLong(0L);

    /**
     * #of successful index partition compacting merge operations.
     */
    protected final AtomicLong indexPartitionMergeCounter = new AtomicLong(0L);

    /**
     * #of successful index partition split operations.
     */
    protected final AtomicLong indexPartitionSplitCounter = new AtomicLong(0L);
    
    /**
     * #of successful index partition tail split operations.
     */
    protected final AtomicLong indexPartitionTailSplitCounter = new AtomicLong(0L);
    
    /**
     * #of successful index partition join operations.
     */
    protected final AtomicLong indexPartitionJoinCounter = new AtomicLong(0L);
    
    /**
     * #of successful index partition move operations.
     */
    protected final AtomicLong indexPartitionMoveCounter = new AtomicLong(0L);
    
    /**
     * #of successful index partition move operations where this service was
     * the target of the move (it received the index partition).
     */
    protected final AtomicLong indexPartitionReceiveCounter = new AtomicLong(0L);
    
    /**
     * The timeout for asynchronous overflow processing.
     * 
     * @see Options#OVERFLOW_TIMEOUT
     */
    protected final long overflowTimeout;
    
    /**
     * @see Options#OVERFLOW_TASKS_CONCURRENT
     */
    protected final int overflowTasksConcurrent;
    
    /**
     * @see Options#OVERFLOW_CANCELLED_WHEN_JOURNAL_FULL
     */
    protected final boolean overflowCancelledWhenJournalFull;

//    /**
//     * @see Options#PURGE_RESOURCES_TIMEOUT
//     */
//    private final long purgeResourcesTimeout;

//    /**
//     * The timeout in milliseconds that we will await an exclusive write lock on
//     * the {@link WriteExecutorService} in order to purge unused resources.
//     * 
//     * @see Options#PURGE_RESOURCES_TIMEOUT
//     */
//    public long getPurgeResourcesTimeout() {
//        
//        return purgeResourcesTimeout;
//        
//    }
    
    /**
     * #of synchronous overflows that have taken place. This counter is
     * incremented each time the synchronous overflow operation.
     */
    public long getSynchronousOverflowCount() {
    
        return synchronousOverflowCounter.get();
        
    }
    
    /**
     * #of asynchronous overflows that have taken place. This counter is
     * incremented each time the entire overflow operation is complete,
     * including any post-processing of the old journal.
     */
    public long getAsynchronousOverflowCount() {
    
        return asynchronousOverflowCounter.get();
        
    }
    
    /**
     * <code>true</code> if overflow processing is enabled and
     * <code>false</code> if overflow processing was disabled as a
     * configuration option or if a maximum overflow count was configured and
     * has been satisfied, in which case the live journal will NOT overflow.
     * 
     * @see Options#OVERFLOW_ENABLED
     * @see Options#OVERFLOW_MAX_COUNT
     */
    public boolean isOverflowEnabled() {
        
        return overflowEnabled
                && (overflowMaxCount == 0 || synchronousOverflowCounter.get() < overflowMaxCount);
        
    }

    /**
     * <code>true</code> unless an overflow event is currently being
     * processed.
     */
    public boolean isOverflowAllowed() {
        
        return overflowAllowed.get();
        
    }

    /**
     * Options understood by the {@link OverflowManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends IndexManager.Options, IServiceShutdown.Options {
       
        /**
         * Boolean property determines whether or not
         * {@link IResourceManager#overflow()} processing is enabled (default
         * {@value #DEFAULT_OVERFLOW_ENABLED}). When disabled the journal will
         * grow without bounds, {@link IndexSegment}s will never be generated
         * and index partitions will not be split, joined nor moved away from
         * this {@link ResourceManager}.
         */
        String OVERFLOW_ENABLED = OverflowManager.class.getName()+".overflowEnabled";

        String DEFAULT_OVERFLOW_ENABLED = "true";

        /**
         * Option may be used to permit a fixed number of synchronous overflow
         * operations after which overflow is disabled (default
         * {@value #DEFAULT_OVERFLOW_MAX_COUNT}). When ZERO (0) there is no
         * limit on the #of synchonous overflow operations. This option is
         * mainly used for testing, but it can be enabled if you want higher
         * throughput (for a while) and you know that the data will be well
         * distributed on the federation after N overflows. Once synchronous
         * overflow is disabled, all future writes will be buffered by the live
         * journal and index partition builds, merges, splits, joins, and moves
         * will no longer be executed. Eventually the live journal extent will
         * grow large enough that throughput will drop (due to IOWAIT on random
         * seeks against the journal) and it is possible that the maximum
         * possible journal extent can be exceeded unless you also configure
         * {@link com.bigdata.journal.Options#OFFSET_BITS} for scale-up.
         */
        String OVERFLOW_MAX_COUNT = OverflowManager.class.getName()
                + ".overflowMaxCount";

        String DEFAULT_OVERFLOW_MAX_COUNT = "0";
        
        /**
         * Floating point property specifying the percentage of the maximum
         * extent at which synchronous overflow processing will be triggered
         * (default {@link #DEFAULT_OVERFLOW_THRESHOLD}). The value is
         * multiplied into the configured
         * {@link com.bigdata.journal.Options#MAXIMUM_EXTENT}. If the result is
         * GTE the current extend of the live journal, then synchronous overflow
         * processing will be triggered. However, note that synchronous overflow
         * processing can not be triggered until asynchronous overflow
         * processing for the last journal is complete. Therefore if
         * asynchronous overflow processing takes a long time, the overflow
         * threashold might not be checked until after it has already been
         * exceeded.
         * <p>
         * The main purpose of this property is to trigger overflow processing
         * before the maximum extent is exceeded. The trigger needs to lead the
         * maximum extent somewhat since overflow processing can not proceed
         * until there is an exclusive lock on the write service, and tasks
         * already running will continue to write on the live journal.
         * Overflowing the maximum extent is not a problem as long as the
         * {@link BufferMode} supports transparent extension of the journal.
         * However, some {@link BufferMode}s do not and therefore they can not
         * be used reliably with the overflow manager.
         */
        String OVERFLOW_THRESHOLD = OverflowManager.class.getName()
                + ".overflowThreshold";

        String DEFAULT_OVERFLOW_THRESHOLD = ".9";
        
        /**
         * Index partitions having no more than this many entries as reported by
         * a range count will be copied to the new journal during synchronous
         * overflow processing rather than building a new index segment from the
         * buffered writes (default {@value #DEFAULT_COPY_INDEX_THRESHOLD}).
         * When ZERO (0), index partitions will not be copied during overflow
         * processing (unless they are empty). While it is important to keep
         * down the latency of synchronous overflow processing, small indices
         * can be copied so quickly that it is worth it to avoid the heavier
         * index segment build operation.
         * 
         * @see #DEFAULT_COPY_INDEX_THRESHOLD
         */
        String COPY_INDEX_THRESHOLD = OverflowManager.class.getName()
                + ".copyIndexThreshold";

        String DEFAULT_COPY_INDEX_THRESHOLD = "1000";

        /**
         * The #of index partitions below which we will accelerate the decision
         * to split an index partition (default
         * {@value #DEFAULT_ACCELERATE_SPLIT_THRESHOLD}). When a new scale-out
         * index is created there is by default only a single index partition on
         * a single {@link IDataService}. Since each index (partition) is
         * single threaded for writes, we can increase the potential concurrency
         * if we split the initial index partition. We accelerate decisions to
         * split index partitions by reducing the minimum and target #of tuples
         * per index partition for an index with fewer than the #of index
         * partitions specified by this parameter. When ZERO (0) this feature is
         * disabled and we do not count the #of index partitions.
         */
        String ACCELERATE_SPLIT_THRESHOLD = OverflowManager.class.getName()
                + ".accelerateSplitThreshold";

        String DEFAULT_ACCELERATE_SPLIT_THRESHOLD = "20";

        /**
         * The minimum percentage (where <code>1.0</code> corresponds to 100
         * percent) that an index partition must constitute of a nominal index
         * partition before a head or tail split will be considered (default
         * {@value #DEFAULT_PERCENT_OF_SPLIT_THRESHOLD}). Values near to and
         * greater than <code>1.0</code> are permissible and imply that the
         * post-split leftSibling index partition will be approximately a
         * nominal index partition. However the maximum percentage may not be
         * greater than <code>2.0</code> (200 percent).
         */
        String PERCENT_OF_SPLIT_THRESHOLD = OverflowManager.class.getName()
                + ".percentOfSplitThreshold";

        String DEFAULT_PERCENT_OF_SPLIT_THRESHOLD = ".9";
        
        /**
         * The minimum percentage (in [0:1]) of leaf splits which must be in the
         * tail of the index partition before a tail split of an index partition
         * will be considered (default {@value #DEFAULT_TAIL_SPLIT_THRESHOLD}).
         */
        String TAIL_SPLIT_THRESHOLD = OverflowManager.class.getName()
                + ".tailSplitThreshold";
        
        String DEFAULT_TAIL_SPLIT_THRESHOLD = ".4";

        /**
         * The minimum percentage (in [0:2]) of a nominal split before an index
         * partition will be "hot split" (default
         * {@value #DEFAULT_HOT_SPLIT_THRESHOLD}). Hot splits are taken by
         * hosts which are more heavily utilized than their peers but not
         * heavily utilized in terms of their own resources. This is basically
         * an acceleration factor for index partition splits when a host has a
         * relatively higher workload than its peers. The purpose of a "hot
         * split" is to increase the potential concurrency by breaking an active
         * index partition into two index partitions. If the writes on the index
         * partition are evenly distributed, then this can double the
         * concurrency if the host has spare cycles. Reasonable values are on
         * the order of [.25:.75]. Hot splits may be effectively disabled by
         * raising the percent of split to GTE
         * {@value #PERCENT_OF_SPLIT_THRESHOLD}.
         */
        String HOT_SPLIT_THRESHOLD = OverflowManager.class.getName()
                + ".hotSplitThreshold";
        
        String DEFAULT_HOT_SPLIT_THRESHOLD = ".4";
        
        /**
         * Boolean option indicates whether or not scatter splits are allowed
         * (default {@value #SCATTER_SPLIT_ENABLED}) on this service.
         * 
         * @see IndexMetadata.Options#SCATTER_SPLIT_ENABLED
         */
        String SCATTER_SPLIT_ENABLED = OverflowManager.class.getName()
                + ".scatterSplitEnabled";

        String DEFAULT_SCATTER_SPLIT_ENABLED = "true";
        
        /**
         * Option may be used to disable index partition joins.
         * 
         * FIXME Joins are being triggered by the
         * {@link #ACCELERATE_SPLIT_THRESHOLD} behavior since the target for the
         * split size increases as a function of the #of index partitions. In
         * order to fix this we have to somehow discount joins, either by
         * requiring deletes on the index partition or by waiting some #of
         * overflows since the split, etc. For the moment they are disabled by
         * default.
         */
        String JOINS_ENABLED = OverflowManager.class.getName()
                + ".joinsEnabled";

        String DEFAULT_JOINS_ENABLED = "false";
        
        /**
         * The minimum #of active index partitions on a data service before the
         * resource manager will consider moving an index partition to another
         * service (default {@value #DEFAULT_MINIMUM_ACTIVE_INDEX_PARTITIONS}).
         * <p>
         * Note: This makes sure that we don't do a move if there are only a few
         * active index partitions on this service. This value is also used to
         * place an upper bound on the #of index partitions that can be moved
         * away from this service - if we move too many (or too many at once)
         * then this service stands a good chance of becoming under-utilized and
         * index partitions will just bounce around which is very inefficient.
         * <p>
         * Note: Even when only a single index partition for a new scale-out
         * index is initially allocated on this service, if it is active and
         * growing it will eventually split into enough index partitions that we
         * will begin to re-distribute those index partitions across the
         * federation.
         * <p>
         * Note: Index partitions are considered to be "active" iff
         * {@link ITx#UNISOLATED} or {@link ITx#READ_COMMITTED} operations are
         * run against the index partition during the life cycle of the live
         * journal. There may be many other index partitions on the same service
         * that either are never read or are subject only to historical reads.
         * However, since only the current state of the index partition is
         * moved, not its history, moving index partitions which are only the
         * target for historical reads will not reduce the load on the service.
         * Instead, read burdens are reduced using replication.
         * 
         * @see #DEFAULT_MINIMUM_ACTIVE_INDEX_PARTITIONS
         */
        String MINIMUM_ACTIVE_INDEX_PARTITIONS = OverflowManager.class
                .getName()
                + ".minimumActiveIndexPartitions";

        String DEFAULT_MINIMUM_ACTIVE_INDEX_PARTITIONS = "1";

        /**
         * This is the maximum #of index partitions that the resource manager is
         * willing to move in a given overflow operations across all of the
         * identified under-utilized services (default
         * {@value #DEFAULT_MAXIMUM_MOVES}).
         * <p>
         * Note: Index partition moves MAY be disabled by setting this property
         * to ZERO (0).
         * 
         * @see #DEFAULT_MAXIMUM_MOVES
         */
        String MAXIMUM_MOVES = OverflowManager.class.getName()
                + ".maximumMoves";

        String DEFAULT_MAXIMUM_MOVES = "3";

        /**
         * This is the maximum #of index partitions that the resource manager is
         * willing to move in a given overflow operation onto each identified
         * under-utilized service (default
         * {@value #DEFAULT_MAXIMUM_MOVES_PER_TARGET}).
         * <p>
         * Note: Index partitions are moved to the identified under-utilized
         * services using a round-robin approach which aids in distributing the
         * load across the federation.
         * <p>
         * Note: Index partition moves MAY be disabled by setting this property
         * to ZERO (0).
         * 
         * @see #DEFAULT_MAXIMUM_MOVES_PER_TARGET
         */
        String MAXIMUM_MOVES_PER_TARGET = OverflowManager.class.getName()
                + ".maximumMovesPerTarget";

        String DEFAULT_MAXIMUM_MOVES_PER_TARGET = "2";

        /**
         * The threshold for a service to consider itself sufficiently loaded
         * that it will consider moving an index partition (default
         * {@value #DEFAULT_MOVE_PERCENT_CPU_TIME_THRESHOLD}). This threshold
         * IS NOT considered for scatter splits, since the goal there is to
         * distribute the data evenly across the federation.
         */
        String MOVE_PERCENT_CPU_TIME_THRESHOLD = OverflowManager.class
                .getName()
                + ".movePercentCpuTimeThreshold";

        String DEFAULT_MOVE_PERCENT_CPU_TIME_THRESHOLD = ".7";
        
        /**
         * The maximum #of optional compacting merge operations that will be
         * performed during a single overflow event (default
         * {@value #DEFAULT_OPTIONAL_COMPACTING_MERGES_PER_OVERFLOW}).
         * <p>
         * Once this #of optional compacting merge tasks have been identified
         * for a given overflow event, the remainder of the index partitions
         * that are neither split, joined, moved, nor copied will use
         * incremental builds. An incremental build is generally cheaper since
         * it only copies the data on the mutable {@link BTree} for the
         * lastCommitTime rather than the fused view. Acompacting merge permits
         * the older index segments to be released and results in a simpler view
         * with view {@link IndexSegment}s. Either a compacting merge or an
         * incremental build will permit old journals to be released once the
         * commit points on those journals are no longer required.
         * <p>
         * Note: Mandatory compacting merges are identified based on
         * {@link #MAXIMUM_JOURNALS_PER_VIEW} and
         * {@link #MAXIMUM_SEGMENTS_PER_VIEW}. There is NO limit the #of
         * manditory compacting merges that will be performed during an
         * asynchronous overflow event. However, each manditory compacting merge
         * does count towards the maximum #of optional merges. Therefore if the
         * #of manditory compacting merges is greater than this parameter then
         * NO optional compacting merges will be selected in a given overflow
         * cycle.
         */
        String MAXIMUM_OPTIONAL_MERGES_PER_OVERFLOW = OverflowManager.class
                .getName()
                + ".maximumOptionalMergesPerOverflow";

        String DEFAULT_OPTIONAL_COMPACTING_MERGES_PER_OVERFLOW = "2";

//        /**
//         * The maximum #of sources for an index partition view before a
//         * compacting merge of the index partition will be triggered in
//         * preference to an incremental build (default
//         * {@value #DEFAULT_MAXIMUM_SOURCES_PER_VIEW}). The minimum value is
//         * ONE (1) since the source view must always include the mutable
//         * {@link BTree}. When ONE (1), a compacting merge is always indicated.
//         * <p>
//         * Note: An index partition view is comprised of a mutable {@link BTree}
//         * on the live journal, zero or more mutable {@link BTree}s from
//         * historical journals, and zero or more {@link IndexSegment}s. An
//         * incremental build replaces the {@link BTree} from the old journal (as
//         * of the lastCommitTime for that journal) with an {@link IndexSegment}
//         * having the same data. A compacting merge replaces the <em>view</em>
//         * as of the lastCommitTime of the old journal and results in a mutable
//         * {@link BTree} on the live journal and a single {@link IndexSegment}.
//         * Split and move operations have the same effect as a compacting merge
//         * since their output will contain at most one {@link IndexSegment}.
//         * 
//         * @deprecated should be redundent with
//         *             {@link #MAXIMUM_JOURNALS_PER_VIEW} and
//         *             {@link #MAXIMUM_SEGMENTS_PER_VIEW}.
//         */
//        String MAXIMUM_SOURCES_PER_VIEW = OverflowManager.class.getName()
//                + ".maximumSourcesPerView";
//
//        String DEFAULT_MAXIMUM_SOURCES_PER_VIEW = "5";

        /**
         * A compacting merge will be triggered when the #of journals in an
         * index partition view is GTE to this value (default
         * {@value #DEFAULT_MAXIMUM_JOURNALS_PER_VIEW}). The minimum value is
         * TWO (2) since there will be two journals in a view when an index
         * partition overflows and {@link OverflowActionEnum#Copy} is not
         * selected. As long as index partition splits, builds or merges are
         * performed the #of journals in the view WILL NOT exceed 2 and will
         * always be ONE (1) after an asynchronous overflow in which a split,
         * build or merge was performed.
         * <p>
         * It is extremely important to perform compacting merges in order to
         * release dependencies on old resources (both journals and index
         * segments) and keep down the #of sources in a view. This is especially
         * true when those sources are journals. Journals are organized by write
         * access, not read access. Once the backing buffer for a journal is
         * released there will be large spikes in IOWAIT when reading on an old
         * journal as reads are more or less random.
         * <p>
         * Note: The {@link #MAXIMUM_OPTIONAL_MERGES_PER_OVERFLOW} will be
         * ignored if a compacting merge is recommended for an index partition
         * based on this parameter.
         * <p>
         * Note: Synchronous overflow will refuse to copy tuples for an index
         * partition whose mutable {@link BTree} otherwise satisifies the
         * {@link #COPY_INDEX_THRESHOLD} if the #of sources in the view exceeds
         * thresholds which demand a compacting merge.
         */
        String MAXIMUM_JOURNALS_PER_VIEW = OverflowManager.class.getName()
                + ".maximumJournalsPerView";

        String DEFAULT_MAXIMUM_JOURNALS_PER_VIEW = "3";

        /**
         * A compacting merge will be triggered when the #of index segments in
         * an index partition view is GTE to this value (default
         * {@value #DEFAULT_MAXIMUM_SEGMENTS_PER_VIEW}).
         * <p>
         * It is extremely important to perform compacting merges in order to
         * release dependencies on old resources (both journals and index
         * segments) and keep down the #of sources in a view. However, this is
         * less important when those resources are {@link IndexSegment}s since
         * they are very efficient for read operations. In this case the main
         * driver is to reduce the complexity of the view, to require fewer open
         * index segments (and associated resources) in order to materialize the
         * view, and to make it possible to release index segments and thus have
         * less of a footprint on the disk.
         * <p>
         * Note: The {@link #MAXIMUM_OPTIONAL_MERGES_PER_OVERFLOW} will be
         * ignored if a compacting merge is recommended for an index partition
         * based on this parameter.
         * <p>
         * Note: Synchronous overflow will refuse to copy tuples for an index
         * partition whose mutable {@link BTree} otherwise satisifies the
         * {@link #COPY_INDEX_THRESHOLD} if the #of sources in the view exceeds
         * thresholds which demand a compacting merge.
         */
        String MAXIMUM_SEGMENTS_PER_VIEW = OverflowManager.class.getName()
                + ".maximumSegmentsPerView";

        String DEFAULT_MAXIMUM_SEGMENTS_PER_VIEW = "6";
        
        /**
         * Option limits the #of {@link IndexSegmentStore} bytes that an
         * {@link OverflowActionEnum#Build} operation will process (default
         * {@value #DEFAULT_MAXIMUM_BUILD_SEGMENTS_BYTES}). Given that the
         * nominal size of an index partition is 200M, a reasonable value for
         * this might be 1/10th to 1/5th of that, so 20-40M. The key is to keep
         * the builds fast so they should not do too much work while reducing
         * the frequency with which we must do a compacting merge. This option
         * only effects the #of {@link IndexSegment}s that will be incorporated
         * into an {@link OverflowActionEnum#Build} operation. When ZERO (0L),
         * {@link OverflowActionEnum#Build} operations will only include the
         * data from the historical journal.
         */
        String MAXIMUM_BUILD_SEGMENT_BYTES = OverflowManager.class.getName()
                + ".maximumBuildSegmentsBytes";

        String DEFAULT_MAXIMUM_BUILD_SEGMENTS_BYTES = ""
                + (Bytes.megabyte * 20);
        
        /**
         * The timeout in milliseconds for asynchronous overflow processing to
         * complete (default {@link #DEFAULT_OVERFLOW_TIMEOUT}). Any overflow
         * task that does not complete within this timeout will be canceled.
         * <p>
         * Asynchronous overflow processing is responsible for splitting,
         * moving, and joining index partitions. The asynchronous overflow tasks
         * are written to fail "safe". Also, each task may succeed or fail on
         * its own. Iff the task succeeds, then its effect is made restart safe.
         * Otherwise clients continue to use the old view of the index
         * partition.
         * <p>
         * If asynchronous overflow processing DOES NOT complete each time then
         * we run several very serious and non-sustainable risks, including: (a)
         * the #of sources in a view can increase without limit; (b) the #of
         * journal that must be retained can increase without limit; and (c)
         * when using the {@link BufferedDiskStrategy}, the direct buffers for
         * those journals can increase without limit.
         */
        String OVERFLOW_TIMEOUT = OverflowManager.class.getName() + ".timeout";

        /**
         * The default timeout in milliseconds for asynchronous overflow
         * processing (equivalent to 10 minutes).
         */
        String DEFAULT_OVERFLOW_TIMEOUT = "" + (10 * 1000 * 60L); // 10 minutes.

        /**
         * The #of threads used to execute the asynchronous overflow tasks in
         * parallel, ZERO (0) to execute ALL asynchronous overflow tasks in
         * parallel, or ONE (1) to execute the asynchronous overflow tasks
         * sequentially (default {@value #DEFAULT_OVERFLOW_TASKS_CONCURRENT}).
         */
        String OVERFLOW_TASKS_CONCURRENT = OverflowManager.class.getName()
                + ".overflowTasksConcurrent";

        String DEFAULT_OVERFLOW_TASKS_CONCURRENT = "0";

        /**
         * Cancel an existing asynchronous overflow process (interrupting any
         * running tasks) if the live journal is again approaching its maximum
         * extent (default
         * {@value #DEFAULT_OVERFLOW_CANCELLED_WHEN_JOURNAL_FULL}).
         * 
         * @todo this option is ignored if {@link #OVERFLOW_TASKS_CONCURRENT}
         *       has a value other than ONE (1) <code>true</code>.
         */
        String OVERFLOW_CANCELLED_WHEN_JOURNAL_FULL = OverflowManager.class
                .getName()
                + ".overflowCancelledWhenJournalFull";

        String DEFAULT_OVERFLOW_CANCELLED_WHEN_JOURNAL_FULL = "true";

        /**
         * The timeout in milliseconds that we will await an exclusive lock on
         * the {@link WriteExecutorService} in order to release unused resources
         * (journals and segment files).
         */
        String PURGE_RESOURCES_TIMEOUT = "purgeResourcesTimeout";

        String DEFAULT_PURGE_RESOURCES_TIMEOUT = "" + (1000 * 60L);
        
    }

    /**
     * Performance counters for the {@link OverflowManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IOverflowManagerCounters {

        /**
         * <code>true</code> iff overflow processing is enabled as a
         * configuration option.
         */
        String OverflowEnabled = "Overflow Enabled";

        /**
         * <code>true</code> iff overflow processing is currently permitted.
         */
        String OverflowAllowed = "Overflow Allowed";

        /**
         * <code>true</code> iff synchronous overflow should be initiated
         * based on an examination of the state of the live journal and whether
         * or not overflow processing is enabled and currently allowed.
         */
        String ShouldOverflow = "Should Overflow";

        /**
         * The #of synchronous overflow events that have taken place. This
         * counter is incremented each time the synchronous overflow operation
         * is complete.
         */
        String SynchronousOverflowCount = "Synchronous Overflow Count";

        /**
         * The elapsed time for synchronous overflow processing to date.
         */
        String SynchronousOverflowMillis = "Synchronous Overflow Millis";

        /**
         * The elapsed time for asynchronous overflow processing to date.
         */
        String AsynchronousOverflowMillis = "Asynchronous Overflow Millis";

        /**
         * The #of asynchronous overflow events that have taken place. This
         * counter is incremented each time the entire overflow operation is
         * complete, including any post-processing of the old journal.
         */
        String AsynchronousOverflowCount = "Asynchronous Overflow Count";

        /**
         * The #of asynchronous overflow operations which have failed.
         */
        String AsynchronousOverflowFailedCount = "Asynchronous Overflow Failed Count";

        /**
         * The #of asynchronous overflow tasks (split, join, merge, etc) which
         * have failed.
         */
        String AsynchronousOverflowTaskFailedCount = "Asynchronous Overflow Task Failed Count";

        /**
         * The #of asynchronous overflow tasks (split, join, merge, etc) that
         * were canceled due to timeout.a
         */
        String AsynchronousOverflowTaskCancelledCount = "Asynchronous Overflow Task Cancelled Count";

    }
    
    /**
     * Performance counters for the index partition tasks.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IIndexPartitionTaskCounters {
        
        /**
         * The #of index partition build operations which have completed
         * successfully.
         */
        String BuildCount = "Build Count";

        /**
         * The #of index partition merge (compacting merge) operations which
         * have completed successfully.
         */
        String MergeCount = "Merge Count";

        /**
         * The #of index partition split operations which have completed
         * successfully.
         */
        String SplitCount = "Split Count";

        /**
         * The #of index partition tail split operations which have completed
         * successfully.
         */
        String TailSplitCount = "Tail Split Count";

        /**
         * The #of index partition join operations which have completed
         * successfully.
         */
        String JoinCount = "Join Count";

        /**
         * The #of index partition move operations which have completed
         * successfully.
         */
        String MoveCount = "Move Count";

        /**
         * The #of index partitions received by this data service in response to
         * an index partition move from another data service.
         */
        String ReceiveCount = "Receive Count";

        /**
         * The running index partition builds for this service. The vast
         * majority of any of the index partition tasks (split, move, join,
         * etc.) lies in the index segment build operations. Therefore you can
         * use the tasks reported here to see the majority of the effort for
         * asynchronous overflow operations.
         */
        String RunningBuilds = "Active Builds";
        
    }
    
    /**
     * @param properties
     */
    public OverflowManager(final Properties properties) {

        super(properties);

        // overflowEnabled
        {

            overflowEnabled = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.OVERFLOW_ENABLED,
                            Options.DEFAULT_OVERFLOW_ENABLED));

            if (log.isInfoEnabled())
                log.info(Options.OVERFLOW_ENABLED + "=" + overflowEnabled);

        }

        // overflowMaxCount
        {

            overflowMaxCount = Integer.parseInt(properties.getProperty(
                    Options.OVERFLOW_MAX_COUNT,
                    Options.DEFAULT_OVERFLOW_MAX_COUNT));

            if (log.isInfoEnabled())
                log.info(Options.OVERFLOW_MAX_COUNT + "=" + overflowMaxCount);

        }

        // overflowThreshold
        {

            overflowThreshold = Double
                    .parseDouble(properties.getProperty(
                            Options.OVERFLOW_THRESHOLD,
                            Options.DEFAULT_OVERFLOW_THRESHOLD));

            if (log.isInfoEnabled())
                log.info(Options.OVERFLOW_THRESHOLD + "=" + overflowThreshold);

        }

        // overflowTimeout
        {
            
            overflowTimeout = Long
                    .parseLong(properties.getProperty(
                            Options.OVERFLOW_TIMEOUT,
                            Options.DEFAULT_OVERFLOW_TIMEOUT));

            if(log.isInfoEnabled())
                log.info(Options.OVERFLOW_TIMEOUT + "=" + overflowTimeout);
            
        }

        // overflowTasksConcurrent
        {

            overflowTasksConcurrent = Integer.parseInt(properties
                    .getProperty(Options.OVERFLOW_TASKS_CONCURRENT,
                            Options.DEFAULT_OVERFLOW_TASKS_CONCURRENT));

            if (log.isInfoEnabled())
                log.info(Options.OVERFLOW_TASKS_CONCURRENT + "="
                        + overflowTasksConcurrent);

            if (overflowTasksConcurrent < 0) {
                
                throw new IllegalArgumentException(
                        Options.OVERFLOW_TASKS_CONCURRENT
                                + " : must be non-negative.");
           
            }
            
        }
        
        // overflowCancelledWhenJournalFull
        {

            overflowCancelledWhenJournalFull = Boolean
                    .parseBoolean(properties
                            .getProperty(
                                    Options.OVERFLOW_CANCELLED_WHEN_JOURNAL_FULL,
                                    Options.DEFAULT_OVERFLOW_CANCELLED_WHEN_JOURNAL_FULL));

            if (log.isInfoEnabled())
                log.info(Options.OVERFLOW_CANCELLED_WHEN_JOURNAL_FULL + "="
                        + overflowCancelledWhenJournalFull);

        }

//        // purgeResourcesTimeout
//        {
//            
//            purgeResourcesTimeout = Long
//                    .parseLong(properties.getProperty(
//                            Options.PURGE_RESOURCES_TIMEOUT,
//                            Options.DEFAULT_PURGE_RESOURCES_TIMEOUT));
//
//            if(log.isInfoEnabled())
//                log.info(Options.PURGE_RESOURCES_TIMEOUT + "=" + purgeResourcesTimeout);
//            
//        }

        // copyIndexThreshold
        {

            copyIndexThreshold = Integer.parseInt(properties
                    .getProperty(Options.COPY_INDEX_THRESHOLD,
                            Options.DEFAULT_COPY_INDEX_THRESHOLD));

            if(log.isInfoEnabled())
                log.info(Options.COPY_INDEX_THRESHOLD + "="
                    + copyIndexThreshold);

            if (copyIndexThreshold < 0) {

                throw new RuntimeException(
                        Options.COPY_INDEX_THRESHOLD
                                + " must be non-negative");

            }
            
        }
       
        // accelerateSplitThreshold
        {

            accelerateSplitThreshold = Integer.parseInt(properties.getProperty(
                    Options.ACCELERATE_SPLIT_THRESHOLD,
                    Options.DEFAULT_ACCELERATE_SPLIT_THRESHOLD));

            if (log.isInfoEnabled())
                log.info(Options.ACCELERATE_SPLIT_THRESHOLD + "="
                        + accelerateSplitThreshold);

            if (accelerateSplitThreshold < 0) {

                throw new RuntimeException(Options.ACCELERATE_SPLIT_THRESHOLD
                        + " must be non-negative");

            }
            
        }
        
        // percentOfSplitThreshold
        {

            percentOfSplitThreshold = Double.parseDouble(properties.getProperty(
                    Options.PERCENT_OF_SPLIT_THRESHOLD,
                    Options.DEFAULT_PERCENT_OF_SPLIT_THRESHOLD));

            if (log.isInfoEnabled())
                log.info(Options.PERCENT_OF_SPLIT_THRESHOLD + "="
                        + percentOfSplitThreshold);

            if (percentOfSplitThreshold < 0 || percentOfSplitThreshold > 2) {

                throw new RuntimeException(Options.PERCENT_OF_SPLIT_THRESHOLD
                        + " must be in [0:2]");

            }

        }

        // tailSplitThreshold
        {

            tailSplitThreshold = Double.parseDouble(properties.getProperty(
                    Options.TAIL_SPLIT_THRESHOLD,
                    Options.DEFAULT_TAIL_SPLIT_THRESHOLD));

            if (log.isInfoEnabled())
                log.info(Options.TAIL_SPLIT_THRESHOLD + "="
                        + tailSplitThreshold);

            if (tailSplitThreshold < 0 || tailSplitThreshold > 1) {

                throw new RuntimeException(Options.TAIL_SPLIT_THRESHOLD
                        + " must be in [0:1]");

            }

        }

        // hotSplitThreshold
        {

            hotSplitThreshold = Double.parseDouble(properties.getProperty(
                    Options.HOT_SPLIT_THRESHOLD,
                    Options.DEFAULT_HOT_SPLIT_THRESHOLD));

            if (log.isInfoEnabled())
                log.info(Options.HOT_SPLIT_THRESHOLD + "="
                        + hotSplitThreshold);

            if (hotSplitThreshold < 0 || hotSplitThreshold > 2) {

                throw new RuntimeException(Options.HOT_SPLIT_THRESHOLD
                        + " must be in [0:2]");

            }

        }

        // scatterSplitEnabled
        {

            scatterSplitEnabled = Boolean.parseBoolean(properties.getProperty(
                    Options.SCATTER_SPLIT_ENABLED,
                    Options.DEFAULT_SCATTER_SPLIT_ENABLED));

            if (log.isInfoEnabled())
                log.info(Options.SCATTER_SPLIT_ENABLED + "="
                        + scatterSplitEnabled);

        }
        
//        // scatterSplitPercentOfSplitThreshold
//        {
//
//            scatterSplitPercentOfSplitThreshold = Double
//                    .parseDouble(properties
//                            .getProperty(
//                                    Options.SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD,
//                                    Options.DEFAULT_SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD));
//
//            if (log.isInfoEnabled())
//                log.info(Options.SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD + "="
//                        + scatterSplitPercentOfSplitThreshold);
//
//            if (scatterSplitPercentOfSplitThreshold < 0.1
//                    || scatterSplitPercentOfSplitThreshold > 1.0) {
//
//                throw new RuntimeException(
//                        Options.SCATTER_SPLIT_PERCENT_OF_SPLIT_THRESHOLD
//                                + " must be in [0.1:1.0]");
//
//            }
//
//        }
//
//        // scatterSplitDataServicesCount
//        {
//
//            scatterSplitDataServicesCount = Integer.parseInt(properties
//                    .getProperty(Options.SCATTER_SPLIT_DATA_SERVICES_COUNT,
//                            Options.DEFAULT_SCATTER_SPLIT_DATA_SERVICES_COUNT));
//
//            if (log.isInfoEnabled())
//                log.info(Options.SCATTER_SPLIT_DATA_SERVICES_COUNT + "="
//                        + scatterSplitDataServicesCount);
//
//            if (scatterSplitDataServicesCount < 0) {
//
//                throw new RuntimeException(
//                        Options.SCATTER_SPLIT_DATA_SERVICES_COUNT
//                                + " must be non-negative");
//
//            }
//
//        }
//
//        // scatterSplitIndexPartitionsCount
//        {
//
//            scatterSplitIndexPartitionsCount = Integer
//                    .parseInt(properties
//                            .getProperty(
//                                    Options.SCATTER_SPLIT_INDEX_PARTITIONS_COUNT,
//                                    Options.DEFAULT_SCATTER_SPLIT_INDEX_PARTITIONS_COUNT));
//
//            if (log.isInfoEnabled())
//                log.info(Options.SCATTER_SPLIT_INDEX_PARTITIONS_COUNT + "="
//                        + scatterSplitIndexPartitionsCount);
//
//            if (scatterSplitIndexPartitionsCount < 0) {
//
//                throw new RuntimeException(
//                        Options.SCATTER_SPLIT_INDEX_PARTITIONS_COUNT
//                                + " must be non-negative");
//
//            }
//            
//        }  
        
        // joinsEnabled
        {
            
            joinsEnabled = Boolean.parseBoolean(properties.getProperty(
                    Options.JOINS_ENABLED, Options.DEFAULT_JOINS_ENABLED));

            if (log.isInfoEnabled())
                log.info(Options.JOINS_ENABLED + "=" + joinsEnabled);
            
        }

        // minimumActiveIndexPartitions
        {

            minimumActiveIndexPartitions = Integer.parseInt(properties
                    .getProperty(Options.MINIMUM_ACTIVE_INDEX_PARTITIONS,
                            Options.DEFAULT_MINIMUM_ACTIVE_INDEX_PARTITIONS));

            if(log.isInfoEnabled())
                log.info(Options.MINIMUM_ACTIVE_INDEX_PARTITIONS + "="
                    + minimumActiveIndexPartitions);

            if (minimumActiveIndexPartitions <= 0) {

                throw new RuntimeException(
                        Options.MINIMUM_ACTIVE_INDEX_PARTITIONS
                                + " must be positive");
                
            }
            
        }
        
        // maximum moves
        {

            maximumMoves = Integer.parseInt(properties.getProperty(
                    Options.MAXIMUM_MOVES, Options.DEFAULT_MAXIMUM_MOVES));

            if (log.isInfoEnabled())
                log.info(Options.MAXIMUM_MOVES + "=" + maximumMoves);

            if (maximumMoves < 0) {

                throw new RuntimeException(Options.MAXIMUM_MOVES
                        + " must be non-negative");

            }
            
        }

        // maximum moves per target
        {
            
            maximumMovesPerTarget = Integer.parseInt(properties.getProperty(
                    Options.MAXIMUM_MOVES_PER_TARGET,
                    Options.DEFAULT_MAXIMUM_MOVES_PER_TARGET));

            if(log.isInfoEnabled())
                log.info(Options.MAXIMUM_MOVES_PER_TARGET + "="
                    + maximumMovesPerTarget);

            if (maximumMovesPerTarget < 0) {

                throw new RuntimeException(Options.MAXIMUM_MOVES_PER_TARGET
                        + " must be non-negative");
                
            }

            if (maximumMovesPerTarget > maximumMoves) {

                throw new RuntimeException(Options.MAXIMUM_MOVES_PER_TARGET
                        + " must be less than " + Options.MAXIMUM_MOVES);
                
            }
            
        }

        // movePercentCpuTimeThreshold
        {

            movePercentCpuTimeThreshold = Double.parseDouble(properties
                    .getProperty(Options.MOVE_PERCENT_CPU_TIME_THRESHOLD,
                            Options.DEFAULT_MOVE_PERCENT_CPU_TIME_THRESHOLD));

            if (log.isInfoEnabled())
                log.info(Options.MOVE_PERCENT_CPU_TIME_THRESHOLD + "="
                        + movePercentCpuTimeThreshold);
            
            if (movePercentCpuTimeThreshold < .0
                    || movePercentCpuTimeThreshold > 1.) {

                throw new RuntimeException(
                        Options.MOVE_PERCENT_CPU_TIME_THRESHOLD
                                + " must be in [0.0:1.0] ");
                
            }

        }
        
//        {
//            maximumSourcesPerView = Integer.parseInt(properties.getProperty(
//                    Options.MAXIMUM_SOURCES_PER_VIEW,
//                    Options.DEFAULT_MAXIMUM_SOURCES_PER_VIEW));
//
//            if(log.isInfoEnabled())
//                log.info(Options.MAXIMUM_SOURCES_PER_VIEW+ "="
//                    + maximumSourcesPerView);
//
//            if (maximumSourcesPerView < 1) {
//
//                throw new RuntimeException(
//                        Options.MAXIMUM_SOURCES_PER_VIEW
//                                + " must be GT ONE (1)");
//                
//            }
//            
//        }

        {
            
            maximumOptionalMergesPerOverflow = Integer.parseInt(properties.getProperty(
                    Options.MAXIMUM_OPTIONAL_MERGES_PER_OVERFLOW,
                    Options.DEFAULT_OPTIONAL_COMPACTING_MERGES_PER_OVERFLOW));

            if (log.isInfoEnabled())
                log.info(Options.MAXIMUM_OPTIONAL_MERGES_PER_OVERFLOW + "="
                        + maximumOptionalMergesPerOverflow);

            if (maximumOptionalMergesPerOverflow < 0) {

                throw new RuntimeException(
                        Options.MAXIMUM_OPTIONAL_MERGES_PER_OVERFLOW
                                + " must be non-negative");
                
            }
            
        }

        {
            
            maximumJournalsPerView = Integer.parseInt(properties.getProperty(
                    Options.MAXIMUM_JOURNALS_PER_VIEW,
                    Options.DEFAULT_MAXIMUM_JOURNALS_PER_VIEW));

            if (log.isInfoEnabled())
                log.info(Options.MAXIMUM_JOURNALS_PER_VIEW + "="
                        + maximumJournalsPerView);

            if (maximumJournalsPerView < 2) {

                throw new RuntimeException(Options.MAXIMUM_JOURNALS_PER_VIEW
                        + " must be GTE 2");
                
            }
            
        }

        {
            
            maximumSegmentsPerView = Integer.parseInt(properties.getProperty(
                    Options.MAXIMUM_SEGMENTS_PER_VIEW,
                    Options.DEFAULT_MAXIMUM_SEGMENTS_PER_VIEW));

            if (log.isInfoEnabled())
                log.info(Options.MAXIMUM_SEGMENTS_PER_VIEW + "="
                        + maximumSegmentsPerView);

            if (maximumSegmentsPerView < 1) {

                throw new RuntimeException(Options.MAXIMUM_SEGMENTS_PER_VIEW
                        + " must be GTE 1");

            }
            
        }
        
        // maximumBuildSegmentBytes
        {

            maximumBuildSegmentBytes = Long.parseLong(properties.getProperty(
                    Options.MAXIMUM_BUILD_SEGMENT_BYTES,
                    Options.DEFAULT_MAXIMUM_BUILD_SEGMENTS_BYTES));

            if (maximumBuildSegmentBytes < 0) {

                throw new RuntimeException("The '" + Options.SHUTDOWN_TIMEOUT
                        + "' must be non-negative.");

            }

            if (log.isInfoEnabled())
                log.info(Options.MAXIMUM_BUILD_SEGMENT_BYTES + "="
                        + maximumBuildSegmentBytes);

        }

        // shutdownTimeout
        {

            shutdownTimeout = Long
                    .parseLong(properties.getProperty(Options.SHUTDOWN_TIMEOUT,
                            Options.DEFAULT_SHUTDOWN_TIMEOUT));

            if (shutdownTimeout < 0) {

                throw new RuntimeException("The '" + Options.SHUTDOWN_TIMEOUT
                        + "' must be non-negative.");

            }

            if(log.isInfoEnabled())
                log.info(Options.SHUTDOWN_TIMEOUT + "=" + shutdownTimeout);

        }

        if(overflowEnabled) {

            /*
             * Obtain the service name so that we can include it in the
             * overflowService thread name (if possible).
             */
            String serviceName = null;
            try {
                serviceName = getDataService().getServiceName();
            } catch(UnsupportedOperationException ex) {
                // ignore.
            } catch(Throwable t) {
                log.warn(t.getMessage(),t);
            }
         
            overflowService = Executors.newFixedThreadPool(1,
                    new DaemonThreadFactory((serviceName == null ? ""
                            : serviceName + "-")
                            + "overflowService"));
         
            /*
             * Note: The core thread is pre-started so that the MDC logging
             * information does not get inherited from whatever thread was
             * running the AbstractTask that wound up doing the groupCommit
             * during which overflow processing was initiated - this just cleans
             * up the log which is otherwise (even more) confusing.
             */
            
            ((ThreadPoolExecutor) overflowService).prestartCoreThread();
            
        } else {
            
            overflowService = null;
            
        }

    }

    synchronized public void shutdown() {

        if(!isOpen()) return;
        
        final long begin = System.currentTimeMillis();

        if(log.isInfoEnabled())
            log.info("Begin");
        
        /*
         * overflowService shutdown
         * 
         * Note: This uses immediate termination even during shutdown since
         * asynchronous overflow processing does not need to complete and will
         * remain coherent regardless of when it is interrupted.
         */
        if (overflowService != null)
            overflowService.shutdownNow();
//            {
//
//            /*
//             * Note: when the timeout is zero we approximate "forever" using
//             * Long.MAX_VALUE.
//             */
//
//            final long shutdownTimeout = this.shutdownTimeout == 0L ? Long.MAX_VALUE
//                    : this.shutdownTimeout;
//
//            final TimeUnit unit = TimeUnit.MILLISECONDS;
//
//            overflowService.shutdown();
//
//            try {
//
//                log.info("Awaiting service termination");
//
//                long elapsed = System.currentTimeMillis() - begin;
//
//                if (!overflowService.awaitTermination(shutdownTimeout - elapsed, unit)) {
//
//                    log.warn("Service termination: timeout");
//
//                }
//
//            } catch (InterruptedException ex) {
//
//                log.warn("Interrupted awaiting service termination.", ex);
//
//            }
//            
//        }

        super.shutdown();
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        if(log.isInfoEnabled())
            log.info("Done: elapsed="+elapsed+"ms");
        
    }

    synchronized public void shutdownNow() {

        if(!isOpen()) return;
        
        final long begin = System.currentTimeMillis();
        
        if (log.isInfoEnabled())
            log.info("Begin");

        if(overflowService!=null)
            overflowService.shutdownNow();

        super.shutdownNow();
        
        if(log.isInfoEnabled()) {

            final long elapsed = System.currentTimeMillis() - begin;

            log.info("Done: elapsed=" + elapsed + "ms");
            
        }
        
    }

    /**
     * An overflow condition is recognized when the journal is within some
     * declared percentage of {@link Options#MAXIMUM_EXTENT}. However, this
     * method will return <code>false</code> if overflow has been disabled
     * or if there is an asynchronous overflow operation in progress.
     */
    public boolean shouldOverflow() {
     
        if (isTransient()) {

            /*
             * Note: This is disabled in part because we can not close out and
             * then re-open a transient journal.
             */

            if (log.isDebugEnabled())
                log.debug("Overflow processing not allowed for transient journals");

            return false;

        }

        if (!isOverflowEnabled()) {
            
            if (log.isDebugEnabled())
                log.debug("Overflow processing is disabled");
            
            return false;
        }

        if(!overflowAllowed.get()) {
            
            /*
             * Note: overflow is disabled until we are done processing the old
             * journal.
             * 
             * @todo show elapsed time since disabled in log message.
             */
            
            if (log.isInfoEnabled())
                log.info("Asynchronous overflow still active");
            
            return false;
            
        }
        
        /*
         * Look for overflow condition on the "live" journal.
         */
        final AbstractJournal journal = getLiveJournal();
        // true iff the journal meets the pre-conditions for overflow.
        final boolean shouldOverflow;
        // #of bytes written on the journal.
        final long nextOffset;
        {

            nextOffset = journal.getRootBlockView().getNextOffset();
            
            if (nextOffset > overflowThreshold * journal.getMaximumExtent()) {

                shouldOverflow = true;

            } else {
                
                shouldOverflow = false;
                
            }

            if (!shouldOverflow && log.isDebugEnabled()) {

                log.debug("should not overflow" + ": nextOffset=" + nextOffset
                        + ", maximumExtent=" + journal.getMaximumExtent());

            } else if (shouldOverflow && log.isInfoEnabled()) {

                log.debug("shouldOverflow" + ": nextOffset=" + nextOffset
                        + ", maximumExtent=" + journal.getMaximumExtent());

            }
               
        }

        return shouldOverflow;
        
    }

    /**
     * Core method for overflow with post-processing.
     * <p>
     * Note: This method does not test preconditions based on the extent of the
     * journal.
     * <p>
     * Note: The caller is responsible for ensuring that this method is invoked
     * with an exclusive lock on the write service.
     * <p>
     * Preconditions:
     * <ol>
     * <li>Exclusive lock on the {@link WriteExecutorService}</li>
     * <li>{@link #isOverflowAllowed()}</li>
     * </ol>
     * <p>
     * Post-conditions:
     * <ol>
     * <li>Overflowed onto new journal</li>
     * <li>{@link PostProcessOldJournal} task was submitted.</li>
     * <li>{@link #isOverflowAllowed()} was set <code>false</code> and will
     * remain <code>false</code> until {@link PostProcessOldJournal}</li>
     * </ol>
     * 
     * @todo write unit test for an overflow edge case in which we attempt to
     *       perform an read-committed task on a pre-existing index immediately
     *       after an {@link #overflow()} and verify that a commit record exists
     *       on the new journal and that the read-committed task can read from
     *       the fused view of the new (empty) index on the new journal and the
     *       old index on the old journal.
     */
    public Future<Object> overflow() {

        assert overflowAllowed.get();

        final Event e = new Event(getFederation(), new EventResource(),
                EventType.SynchronousOverflow).addDetail(
                "synchronousOverflowCounter", synchronousOverflowCounter.get())
                .start();

        try {

            /*
             * We have an exclusive lock and the overflow conditions are
             * satisfied.
             */
            // Do overflow processing.
            final OverflowMetadata overflowMetadata = doSynchronousOverflow();

            // Note: commented out to protect access to the new journal until
            // the write service is resumed.
            // report event.
            // notifyJournalOverflowEvent(getLiveJournal());

            if (asyncOverflowEnabled.get()) {

                if (overflowMetadata.postProcess) {

                    /*
                     * Post-processing SHOULD be performed.
                     */

                    if (log.isInfoEnabled())
                        log
                                .info("Will start asynchronous overflow processing.");

                    /*
                     * Start the asynchronous processing of the named indices on
                     * the old journal.
                     */
                    if (!overflowAllowed
                            .compareAndSet(true/* expect */, false/* set */)) {

                        throw new AssertionError();

                    }

                    /*
                     * Submit task on private service that will run
                     * asynchronously and clear [overflowAllowed] when done.
                     * 
                     * Note: No one ever checks the Future returned by this
                     * method. Instead the PostProcessOldJournalTask logs
                     * anything that it throws in its call() method.
                     */

                    return overflowService
                            .submit(new AsynchronousOverflowTask(
                                    (ResourceManager) this, overflowMetadata));

                }

                if (log.isInfoEnabled())
                    log.info("Asynchronous overflow not required");

                /*
                 * Note: increment the counter now since we will not do
                 * asynchronous overflow processing.
                 */

                asynchronousOverflowCounter.incrementAndGet();

                return null;

            } else {

                log.warn("Asynchronous overflow processing is disabled!");

                /*
                 * Note: increment the counter now since we will not do
                 * asynchronous overflow processing.
                 */

                asynchronousOverflowCounter.incrementAndGet();

                return null;

            }

        } finally {

            e.end();
            
            synchronousOverflowMillis.addAndGet(e.getElapsed());

        }

    }
    
    /**
     * Synchronous overflow processing.
     * <p>
     * This is invoked once all preconditions have been satisfied.
     * <p>
     * Index partitions that have fewer than some threshold #of index entries
     * will be copied onto the new journal. Otherwise the view of the index will
     * be re-defined to place writes on the new journal and read historical data
     * from the old journal.
     * <p>
     * This uses {@link #purgeOldResources()} to delete old resources from the
     * local file system that are no longer required as determined by
     * {@link #setReleaseTime(long)} and {@link #getEffectiveReleaseTime()}.
     * <p>
     * Note: This method does NOT start a {@link AsynchronousOverflowTask}.
     * <P>
     * Note: You MUST have an exclusive lock on the {@link WriteExecutorService}
     * before you invoke this method!
     * 
     * @return Metadata about the overflow operation including whether or not
     *         asynchronous should be performed.
     */
    protected OverflowMetadata doSynchronousOverflow() {

        if (log.isInfoEnabled())
            log.info("begin");
        
        final OverflowMetadata overflowMetadata = new OverflowMetadata(
                (ResourceManager) this);

        final AbstractJournal oldJournal = getLiveJournal();
        final ManagedJournal newJournal;

        /*
         * Note: We assign the same timestamp to the createTime of the new
         * journal and the closeTime of the old journal.
         */
        final long createTime = nextTimestamp();
        final long closeTime = createTime;

        /*
         * Close out the old journal.
         * 
         * Note: closeForWrites() does NOT "close" the old journal in order to
         * avoid disturbing concurrent readers (we only have an exclusive lock
         * on the writeService, NOT the readService or the txWriteService).
         * 
         * Note: The old journal MUST be closed out before we open the new
         * journal since the journal will use the SAME direct ByteBuffer
         * instance for their write cache.
         * 
         * FIXME Change this to use a direct buffer pool so we can prepare the
         * new journal entirely before we close out the old one and thereby
         * avoid burning the bridge (closing the old journal) before we cross it
         * (onto the new journal). Do this when changing the buffer pool
         * allocation strategy to support a direct buffers for the Direct and
         * BufferedDisk modes. Update the comments inline above when making this
         * change.
         * 
         * FIXME The current approach opens a hole during synchronous overflow
         * when there are NO indices defined on the (new) live journal. This has
         * not shown up as a problem primarily because I have not been testing
         * with writes with concurrent read during synchronous overflow, but
         * closure might provoke this error.
         * 
         * @todo The logic to report the #of index partitions on the data
         * services in ResourceManager has been hacked to work around this hole.
         * Simplify that logic once this hole is fixed.
         */
        {

            // writes no longer accepted.
            oldJournal.closeForWrites(closeTime);

            // // remove from list of open journals.
            // storeCache.remove(oldJournal.getRootBlockView().getUUID());

            if (log.isInfoEnabled())
                log.info("Closed out the old journal.");

            if (maximumJournalSizeAtOverflow < oldJournal.size()) {

                maximumJournalSizeAtOverflow = oldJournal.getBufferStrategy()
                        .getExtent();
                
            }
            
        }

        /*
         * Create the new journal.
         * 
         * @todo this is not using the temp filename mechanism in a manner that
         * truly guarantees an atomic file create. The CREATE_TEMP_FILE option
         * should probably be extended with a CREATE_DIR option that allows you
         * to override the directory in which the journal is created. That will
         * allow the atomic creation of the journal in the desired directory
         * without changing the existing semantics for CREATE_TEMP_FILE.
         * 
         * See StoreFileManager#start() which has very similar logic with the
         * same problem.
         */
        {

            final File file;
            try {
                file = File.createTempFile("journal", // prefix
                        Options.JNL,// suffix
                        journalsDir // directory
                        ).getCanonicalFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            file.delete();

            final Properties p = getProperties();

            p.setProperty(Options.FILE, file.toString());

            /*
             * Set the create time on the new journal.
             */
            p.setProperty(Options.CREATE_TIME, Long.toString(createTime));

            /*
             * Note: the new journal will be handed the write cache from the old
             * journal so the old journal MUST have been closed for writes
             * before this point in order to ensure that it can no longer write
             * on that write cache.
             */
            assert oldJournal.getRootBlockView().getCloseTime() != 0L : "Old journal has not been closed for writes";

            newJournal = new ManagedJournal(p);

            assert createTime == newJournal.getRootBlockView().getCreateTime();

        }

        /*
         * Cut over to the new journal.
         * 
         * @todo Once the direct buffer allocation issue is resolved, refactor
         * this into a private method, perhaps combined with the close out of
         * the old journal and the propagation of the performance counters. That
         * will simply this code and make easier to understand.
         */
        {

            // add to the journalIndex and the Map<UUID,File>.
            addResource(newJournal.getResourceMetadata(), newJournal.getFile());
            
            // add to the cache.
            storeCache.put(newJournal.getRootBlockView().getUUID(), newJournal);
//                    false/* dirty */);

            // atomic cutover.
            this.liveJournalRef.set( newJournal );

            /*
             * Update the bytes under management to reflect the final extent of
             * the old journal and to discount the extent of the new live
             * journal.
             */
            bytesUnderManagement.addAndGet(oldJournal.getBufferStrategy().getExtent());
            bytesUnderManagement.addAndGet(-newJournal.getBufferStrategy().getExtent());

            journalBytesUnderManagement.addAndGet(oldJournal.getBufferStrategy().getExtent());
            journalBytesUnderManagement.addAndGet(-newJournal.getBufferStrategy().getExtent());

            // note the lastCommitTime on the old journal.
            lastOverflowTime = oldJournal.getRootBlockView().getLastCommitTime();
           
            if (log.isInfoEnabled())
                log.info("New live journal: " + newJournal.getFile());

        }

        /*
         * Propagate the index declarations to the new journal. If an error
         * arises during overflow then the new journal is deleted and the old
         * journal remains in place so that we continue to run against a known
         * good state.
         * 
         * FIXME In order to handle an error in this manner we need to make the
         * change in how the direct buffers are allocated for the new journal so
         * we can keep the old journal open for writes until the atomic cutover.
         * At it stands, we have already closed out the old journal for writes
         * so error handling here WILL NOT permit the application to continue
         * writing on the old journal. 
         */
//        try {
            propagateIndexDecls(oldJournal, newJournal, overflowMetadata);
//        } catch (Throwable t) {
//            log.error("Overflow error (continuing with the old journal): " + t,
//                    t);
//            newJournal.destroy();
//            // @todo rethrow the exception or just return?
//            throw t;
//        }
        
        // make the index declarations restart safe on the new journal.
        final long firstCommitTime = newJournal.commit();
        // @todo atomic cutover really belongs here.

        /*
         * Change over the counter set to the new live journal.
         * 
         * Note: The spelling of the counter set names MUST be consistent with
         * their declarations!
         * 
         * Note: getCounters() on this class gets attached to the serviceRoot by
         * the DataService so that is where we need to go to detach and then
         * re-attach the counters.
         */
        try {

            // The service's counter set hierarchy.
            final CounterSet serviceRoot = getFederation()
                    .getServiceCounterSet();

            if (serviceRoot != null) {

                /*
                 * The counters for the resource manager within the service's
                 * counter hierarchy.
                 * 
                 * Note: The [serviceRoot] is not defined by the MockFederation
                 * used by some unit tests, so we test against != null above.
                 */

                final CounterSet tmp = (CounterSet) serviceRoot
                        .getPath(IDataServiceCounters.resourceManager);

                if (tmp != null) {
                    
                    /*
                     * Again, the resourceManager counters are not defined for
                     * some unit tests.
                     */

                    synchronized (tmp) {

                        // // the live journal is a child of the resource
                        // manager.
                        // tmp.detach(IResourceManagerCounters.LiveJournal);

                        // tmp.makePath(IResourceManagerCounters.LiveJournal).attach(
                        // getLiveJournal().getCounters());

                        ((CounterSet) tmp
                                .getPath(IResourceManagerCounters.LiveJournal))
                                .attach(getLiveJournal()//.getBufferStrategy()
                                        .getCounters(), true/* replace */);

                        log.warn("Re-attached live journal counters: path="
                                + tmp.getPath());

                    }

                }

            }
            
        } catch(Throwable t) {
            
            log.warn("Problem updating counters: "+t, t);
            
        }
        
        synchronousOverflowCounter.incrementAndGet();

        /*
         * Show the new views once we have cut over to the new journal. if we do
         * this before we cut over then the data will still be read from the old
         * journal.
         */
        if (log.isInfoEnabled())
            log.info("\ndoOverflow(): firstCommitTime="
                    + firstCommitTime
                    + "\nfile="
                    + newJournal.getFile()
                    + "\npost-condition views: synchronousOverflowCounter="
                    + getSynchronousOverflowCount()
                    + "\n"
                    + listIndexPartitions(TimestampUtility
                            .asHistoricalRead(firstCommitTime)));

      try {

            /*
             * When there is sustained heavy write activity on the data service
             * it can take a while to obtain the exclusive write lock, which can
             * negatively impact throughput as write tasks are blocked until
             * that lock is obtained.
             * 
             * While asynchronous overflow handling updates the index partition
             * views and is the means by which older views become release free,
             * we release the old resource here rather than as an after action
             * for asynchronous overflow in order to avoid the throughput hit.
             * As a consequence, the data service tends to retain slightly more
             * persistent state than it would otherwise need.
             */

          purgeOldResources();

        } catch (Throwable t) {

            /*
             * Note: An error releasing old resources can become a serious
             * problem if it persists since the disk will fill up with old
             * journals and index segments.
             */
            
            log.error("Problem purging old resources? service="
                    + getFederation().getServiceName(), t);

        }

        return overflowMetadata;

    }

    /**
     * Propagate the index declarations from the old journal to the new journal.
     */
    private void propagateIndexDecls(final AbstractJournal oldJournal,
            final AbstractJournal newJournal,
            final OverflowMetadata overflowMetadata) {

        /*
         * Overflow each index by re-defining its view on the new journal.
         */
        // #of declared indices.
        final int numIndices = overflowMetadata.getIndexCount();
        // #of indices processed (copied over or view redefined).
        int numIndicesProcessed = 0;
        // #of indices whose view was redefined on the new journal.
        int numIndicesViewRedefined = 0;
        // #of indices with at least one index entry that were copied.
        int numIndicesNonZeroCopy = 0;
        // #of indices that were copied over.
        int ncopy = 0;
        /*
         * Maximum #of non-zero indices that we will copy over.
         * 
         * @todo config. maxNonZeroCopy might not be a good idea in some cases.
         * if there is a large #of small indices on the journal then some should
         * really be moved somewhere else and this limit can promote that.
         * however, if the entire federation is filled with such small indices
         * then we hardly needs to be doing index builds for all of them.
         */
        final int maxNonZeroCopy = 100;
        final long lastCommitTime = oldJournal.getRootBlockView().getLastCommitTime();
        {

            if (log.isInfoEnabled())
                log.info("doOverflow(): lastCommitTime="
                        + lastCommitTime
                        + "\nfile="
                        + oldJournal.getFile()
                        + "\npre-condition views: synchronousOverflowCounter="
                        + getSynchronousOverflowCount()
                        + "\n"
                        + listIndexPartitions(TimestampUtility
                                .asHistoricalRead(lastCommitTime)));

            final Iterator<ViewMetadata> itr = overflowMetadata.views();

            while (itr.hasNext()) {

                final ViewMetadata bm = itr.next();

                final BTree oldBTree = bm.getBTree();

                // clone index metadata.
                final IndexMetadata indexMetadata = oldBTree.getIndexMetadata()
                        .clone();

                // old partition metadata (from cloned IndexMetadata record).
                final LocalPartitionMetadata oldpmd = indexMetadata
                        .getPartitionMetadata();

                if (oldpmd == null) {

                    /*
                     * A named index that is not an index partition.
                     * 
                     * Note: In the scale-out system all named indices are
                     * registered as partitioned indices so this condition
                     * SHOULD NOT arise.
                     * 
                     * @todo probably overflow the entire index, but it is a
                     * problem to have an unpartitioned index if you are
                     * expecting to do overflows since the index can never be
                     * broken down and can't be moved around.
                     * 
                     * @todo move runtime check to before we close the old
                     * journal.
                     */

                    throw new RuntimeException("Not a partitioned index: "
                            + bm.name);

                }

                // true iff an overflow handler is defined.
                final boolean hasOverflowHandler = indexMetadata
                        .getOverflowHandler() != null;

                /*
                 * When an index partition is empty we always just copy it onto
                 * the new journal (since there is no data, all that we are
                 * doing is registering the index on the new journal).
                 * 
                 * When the copyIndexThreshold is ZERO (0) index partitions will
                 * not be copied unless they are empty.
                 * 
                 * When an index partition is non-empty, the copyIndexThreshold
                 * is non-zero, and the entry count of the buffered write set is
                 * LTE the threshold then the buffered writes will be copied to
                 * the new journal UNLESS an overflow handler is defined
                 * (overflow handlers are used to copy raw records from the
                 * journal onto the index segment - such records can be quite
                 * large, for example the distributed file system allows records
                 * up to 64M each, so we do not want to copy over even a small
                 * index with an overflow handler since there may be large
                 * records on the journal that would have to be copied as well).
                 * 
                 * Note: The other reason for NOT copying the tuples over is
                 * that the view already includes more than one journal. We DO
                 * NOT copy the tuples over in this case since we want to purge
                 * that journal from the view using a compacting merge.
                 * 
                 * Otherwise we will let the asynchronous post-processing figure
                 * out what it wants to do with this index partition.
                 */
                final int entryCount = bm.entryCount;
                final boolean copyIndex = (entryCount == 0)
                        || ((copyIndexThreshold > 0 && entryCount <= copyIndexThreshold) //
                                && numIndicesNonZeroCopy < maxNonZeroCopy //
                                && !hasOverflowHandler // must be applied
                        && !bm.manditoryMerge // 
                        );

                if (copyIndex) {

                    /*
                     * We will copy the index data from the B+Tree old journal
                     * (but not from the full index view) onto the new journal.
                     * In this case the index will use a view that DOES NOT
                     * include the old index on the old journal.
                     */

                    final IResourceMetadata[] oldResources = oldpmd
                            .getResources();

                    final IResourceMetadata[] newResources = new IResourceMetadata[oldResources.length];

                    System.arraycopy(oldResources, 0, newResources, 0,
                            oldResources.length);

                    // new resource is listed first (reverse chronological
                    // order)
                    newResources[0] = newJournal.getResourceMetadata();

                    // describe the index partition.
                    indexMetadata
                            .setPartitionMetadata(new LocalPartitionMetadata(
                                    oldpmd.getPartitionId(),//
                                    oldpmd.getSourcePartitionId(),//
                                    oldpmd.getLeftSeparatorKey(),//
                                    oldpmd.getRightSeparatorKey(),//
                                    newResources, //
                                    oldpmd.getIndexPartitionCause(), oldpmd
                                            .getHistory()
                                            + OverflowActionEnum.Copy
                                            + "(lastCommitTime="
                                            + lastCommitTime
                                            + ",entryCount="
                                            + entryCount
                                            + ",counter="
                                            + oldBTree.getCounter().get()
                                            + ") "));

                } else {

                    /*
                     * We will only create a empty index on the new journal.
                     * 
                     * We will update the partition metadata so that the new
                     * index reflects its location on the new journal. The index
                     * view will continue to read from the old journal as well
                     * until asynchronous post-processing decides what to do
                     * with the index partition.
                     * 
                     * Note that the old journal will continue to be required
                     * for historical reads on the new journal between its
                     * firstCommitTime and the commit point at which the index
                     * partition view is updated to no longer include the old
                     * journal.
                     */

                    final IResourceMetadata[] oldResources = oldpmd
                            .getResources();

                    final IResourceMetadata[] newResources = new IResourceMetadata[oldResources.length + 1];

                    System.arraycopy(oldResources, 0, newResources, 1,
                            oldResources.length);

                    // new resource is listed first (reverse chronological
                    // order).
                    newResources[0] = newJournal.getResourceMetadata();

                    // describe the index partition.
                    indexMetadata
                            .setPartitionMetadata(new LocalPartitionMetadata(
                                    oldpmd.getPartitionId(),//
                                    oldpmd.getSourcePartitionId(),//
                                    oldpmd.getLeftSeparatorKey(),//
                                    oldpmd.getRightSeparatorKey(),//
                                    newResources, //
                                    oldpmd.getIndexPartitionCause(), oldpmd
                                            .getHistory()
                                            + "overflow(lastCommitTime="
                                            + lastCommitTime
                                            + ",entryCount="
                                            + entryCount
                                            + ",counter="
                                            + oldBTree.getCounter().get()
                                            + ") "));

                }

                /*
                 * Create and register the index with the new view on the new
                 * journal.
                 * 
                 * Note: This is essentially a variant of BTree#create() where
                 * we need to propagate the counter from the old BTree to the
                 * new BTree.
                 */
                {

                    /*
                     * Write metadata record on store. The address of that
                     * record is set as a side-effect on the metadata object.
                     */
                    indexMetadata.write(newJournal);

                    // note the current counter value.
                    final long oldCounter = oldBTree.getCounter().get();

                    if (log.isInfoEnabled())
                        log.info("Re-defining view on new journal"//
                                + ": name=" + bm.name //
                                + ", copyIndex=" + copyIndex//
                                // + ", copyIndexThreashold=" +
                                // copyIndexThreshold //
                                + ", entryCount=" + entryCount//
                                + ", counter=" + oldCounter//
                                + ", partitionId=" + oldpmd.getPartitionId()//
                                + ", checkpoint=" + oldBTree.getCheckpoint()//
                        );

                    // Create checkpoint for the new B+Tree.
                    final Checkpoint overflowCheckpoint = indexMetadata
                            .overflowCheckpoint(oldBTree.getCheckpoint());

                    /*
                     * Write the checkpoint record on the store. The address of
                     * the checkpoint record is set on the object as a side
                     * effect.
                     */
                    overflowCheckpoint.write(newJournal);

                    /*
                     * Load the B+Tree from the store using that checkpoint
                     * record.
                     */
                    final BTree newBTree = BTree.load(newJournal,
                            overflowCheckpoint.getCheckpointAddr());

                    // Note the counter value on the new BTree.
                    final long newCounter = newBTree.getCounter().get();

                    // Verify the counter was propagated to the new BTree.
                    assert newCounter == oldCounter : "expected oldCounter="
                            + oldCounter + ", but found newCounter="
                            + newCounter;

                    if (copyIndex) {

                        /*
                         * Copy the data from the B+Tree on the old journal into
                         * the B+Tree on the new journal.
                         * 
                         * Note: [overflow := true] since we are copying from
                         * the old journal onto the new journal, but the
                         * overflow handler will never be applied since we do
                         * NOT copy an index with a non-null overflow handler
                         * (see above).
                         */

                        if (log.isDebugEnabled())
                            log.debug("Copying data to new journal: name="
                                    + bm.name + ", entryCount=" + entryCount
                                    + ", threshold=" + copyIndexThreshold);

                        newBTree
                                .rangeCopy(oldBTree, null, null, true/* overflow */);

                        // Note that index partition was copied for the caller.
                        // overflowMetadata.copied.add(bm.name);
                        overflowMetadata.setAction(bm.name,
                                OverflowActionEnum.Copy);
                        ncopy++;

                        if (entryCount > 0) {

                            // count copied indices with at least one index
                            // entry.
                            numIndicesNonZeroCopy++;

                        }

                    } else {

                        /*
                         * The index was not copied so its view was re-defined
                         * on the new journal.
                         */

                        numIndicesViewRedefined++;

                    }

                    /*
                     * Register the new B+Tree on the new journal.
                     */
                    newJournal.registerIndex(bm.name, newBTree);

                }

                numIndicesProcessed++;

                // log.info("Did overflow: " + noverflow + " of " + nindices
                // + " : " + entry.name);

            }

            if (log.isInfoEnabled())
                log.info("Processed indices: #indices=" + numIndices
                        + ", ncopy=" + ncopy + ", ncopyNonZero="
                        + numIndicesNonZeroCopy + ", #viewRedefined="
                        + numIndicesViewRedefined);

            assert numIndices == numIndicesProcessed;
            assert numIndices == (ncopy + numIndicesViewRedefined);
            assert ncopy == overflowMetadata
                    .getActionCount(OverflowActionEnum.Copy);

            /*
             * post processing should be performed if any indices were redefined
             * onto the new journal rather than being copied over.
             */
            overflowMetadata.postProcess = numIndicesViewRedefined > 0;

        }

    }

}
