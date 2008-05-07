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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.Counters;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.counters.CounterSet;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IServiceShutdown;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Class encapsulates logic for handling journal overflow events. Overflow is
 * triggered automatically when the user data extent on the journal nears a
 * configured threshold. Once the pre-conditions for overflow are satisified,
 * the {@link WriteExecutorService}s for the journal are paused and all running
 * tasks on those services are allowed to complete and commit. Once no writers
 * are running, the {@link WriteExecutorService} triggers synchronous overflow.
 * Synchronous overflow is a low-latency process which creates a new journal to
 * absorb future writes, re-defines the views for all index partitions found on
 * the old journal to include the new journal as their first source, and
 * initiates a background thread performing asynchronous overflow
 * post-processing.
 * <p>
 * Asynchronous overflow post-processing is reponsible for identifying index
 * partitions overflow (resulting in a split into two or more index partitions),
 * index partition underflow (resulting in the join of the undercapacity index
 * partition with its rightSibling), index partition moves (the index partition
 * is moved to a different {@link DataService}), and index partition builds (an
 * {@link IndexSegment} is created from the current view in what is effectively
 * a compacting merge). Overflow processing is suspended during asynchronous
 * post-processing, but is automatically re-enabled once post-processing
 * completes.
 * 
 * @todo recommend setting the
 *       {@link com.bigdata.journal.Options#INITIAL_EXTENT} to the
 *       {@link com.bigdata.journal.Options#MAXIMUM_EXTENT} in minimize the
 *       likelyhood of having to extend the journal and in order to keep the
 *       allocation size on the file system large to minimize fragmentation?
 *       <p>
 *       Also, consider moving the target journal size up to 500M.
 *       <p>
 *       Note: The choice of the #of offset bits governs both the maximum #of
 *       records (indirectly through the maximum byte offset) and the maximum
 *       record length (directly).
 *       <p>
 *       A scale-up deployment based on a {@link ResourceManager} can address a
 *       very large index either by using more offset bits and never overflowing
 *       the journal or by using fewer offset bits and a series of journals and
 *       migrating data off of the journals onto index segments. Since there is
 *       no {@link MetadataIndex} the cost of a full compacting merge for an
 *       index grows as a function of the total index size. This makes full
 *       compacting merges increasingly impractical for large indices.
 *       <p>
 *       However, a scale-out deployment based on an {@link IBigdataFederation}
 *       supports key-range partitioned indices regardless of whether it is an
 *       {@link EmbeddedFederation} or a distributed federation. This
 *       means that the cost of a full compacting merge for an index partition
 *       is capped by the size limits we place on index partitions regardless of
 *       the total size of the scale-out index.
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
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * Set based on {@link Options#COPY_INDEX_THRESHOLD}.
     * 
     * @todo make this a per-index option on {@link IndexMetadata}?
     */
    final int copyIndexThreshold;
    
    /**
     * Set based on {@link Options#MINIMUM_ACTIVE_INDEX_PARTITIONS}. 
     */
    protected final int minimumActiveIndexPartitions;
    
    /**
     * Set based on {@link Options#MAXIMUM_MOVES_PER_TARGET}.
     */
    protected final int maximumMovesPerTarget;
    
    /**
     * The timeout for {@link #shutdown()} -or- ZERO (0L) to wait for ever.
     * 
     * @see IServiceShutdown#SHUTDOWN_TIMEOUT
     */
    final private long shutdownTimeout;

    /**
     * The service that runs the asynchronous overflow
     * {@link PostProcessOldJournalTask}.
     */
    private final ExecutorService overflowService;

    /**
     * Flag set based on {@link Options#OVERFLOW_ENABLED}
     */
    private final boolean overflowEnabled;
    
    /**
     * A flag used to disable overflow of the live journal until asynchronous
     * post-processing of the old journal has been completed.
     * 
     * @see PostProcessOldJournalTask
     */
    protected final AtomicBoolean overflowAllowed = new AtomicBoolean(true);

    /**
     * A flag used to disable the asynchronous overflow processing for some unit
     * tests.
     */
    protected final AtomicBoolean asyncOverflowEnabled = new AtomicBoolean(true);
    
    /**
     * #of overflows that have taken place. This counter is incremented each
     * time the entire overflow operation is complete, including any
     * asynchronous post-processing of the old journal.
     */
    protected final AtomicLong overflowCounter = new AtomicLong(0L);

    /**
     * The #of asynchronous overflow operations which fail.
     * 
     * @see PostProcessOldJournalTask
     */
    protected final AtomicLong overflowFailedCounter = new AtomicLong(0L);

    /**
     * #of successful index partition build operations.
     */
    protected final AtomicLong buildCounter = new AtomicLong(0L);

    /**
     * #of successful index partition split operations.
     */
    protected final AtomicLong splitCounter = new AtomicLong(0L);
    
    /**
     * #of successful index partition join operations.
     */
    protected final AtomicLong joinCounter = new AtomicLong(0L);
    
    /**
     * #of successful index partition move operations.
     */
    protected final AtomicLong moveCounter = new AtomicLong(0L);
    
    /**
     * #of overflows that have taken place. This counter is incremented each
     * time the entire overflow operation is complete, including any
     * post-processing of the old journal.
     */
    public long getOverflowCount() {
    
        return overflowCounter.get();
        
    }
    
    /**
     * <code>true</code> if overflow processing is enabled and
     * <code>false</code> if overflow processing was disabled as a
     * configuration option, in which case the live journal will NOT overflow.
     * 
     * @see Options#OVERFLOW_ENABLED
     */
    public boolean isOverflowEnabled() {
        
        return overflowEnabled;
        
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
    public interface Options extends IndexManager.Options, IServiceShutdown.Options {
       
        /**
         * Boolean property determines whether or not
         * {@link IResourceManager#overflow()} processing is enabled (default
         * <code>true</code>). When disabled the journal will grow without
         * bounds, {@link IndexSegment}s will never be generated and index
         * partitions will not be split, joined nor moved away from this
         * {@link ResourceManager}.
         */
        String OVERFLOW_ENABLED = "overflowEnabled";

        String DEFAULT_OVERFLOW_ENABLED = "true";

        /**
         * Index partitions having no more than this many entries as reported by
         * a range count will be copied to the new journal during synchronous
         * overflow processing rather than building a new index segment from the
         * buffered writes (default is <code>1000</code>). When ZERO (0),
         * index partitions will not be copied during overflow processing
         * (unless they are empty). While it is important to keep down the
         * latency of synchronous overflow processing, small indices can be
         * copied so quickly that it is worth it to avoid the heavier index
         * segment build operation.
         * 
         * @see #DEFAULT_COPY_INDEX_THRESHOLD
         */
        String COPY_INDEX_THRESHOLD = "copyIndexThreshold";

        String DEFAULT_COPY_INDEX_THRESHOLD = "1000";

        /**
         * The minimum #of active index partitions on a data service before the
         * resource manager will consider moving an index partition to another
         * service (default <code>3</code>).
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
         * 
         * @see #DEFAULT_MINIMUM_ACTIVE_INDEX_PARTITIONS
         */
        String MINIMUM_ACTIVE_INDEX_PARTITIONS = "minimumActiveIndexPartitions";

        String DEFAULT_MINIMUM_ACTIVE_INDEX_PARTITIONS = "3";

        /**
         * This is the maximum #of index partitions that the resource manager is
         * willing to move in a given overflow onto identified under-utilized
         * service (default <code>3</code>).
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
        String MAXIMUM_MOVES_PER_TARGET = "maximumMovesPerTarget";

        String DEFAULT_MAXIMUM_MOVES_PER_TARGET = "3";

    }
    
    /**
     * @param properties
     */
    public OverflowManager(Properties properties) {

        super(properties);
        
        // overflowEnabled
        {
            
            overflowEnabled = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.OVERFLOW_ENABLED,
                            Options.DEFAULT_OVERFLOW_ENABLED));

            log.info(Options.OVERFLOW_ENABLED+"="+overflowEnabled);
            
        }
        
        // index segment build threshold
        {

            copyIndexThreshold = Integer.parseInt(properties
                    .getProperty(Options.COPY_INDEX_THRESHOLD,
                            Options.DEFAULT_COPY_INDEX_THRESHOLD));

            log.info(Options.COPY_INDEX_THRESHOLD + "="
                    + copyIndexThreshold);

            if (copyIndexThreshold < 0) {

                throw new RuntimeException(
                        Options.COPY_INDEX_THRESHOLD
                                + " must be non-negative");

            }
            
        }
        
        // minimumActiveIndexPartitions
        {

            minimumActiveIndexPartitions = Integer.parseInt(properties
                    .getProperty(Options.MINIMUM_ACTIVE_INDEX_PARTITIONS,
                            Options.DEFAULT_MINIMUM_ACTIVE_INDEX_PARTITIONS));

            log.info(Options.MINIMUM_ACTIVE_INDEX_PARTITIONS + "="
                    + minimumActiveIndexPartitions);

            if (minimumActiveIndexPartitions <= 0) {

                throw new RuntimeException(
                        Options.MINIMUM_ACTIVE_INDEX_PARTITIONS
                                + " must be positive");
                
            }
            
        }
        
        // maximum moves per target
        {
            
            maximumMovesPerTarget = Integer.parseInt(properties.getProperty(
                    Options.MAXIMUM_MOVES_PER_TARGET,
                    Options.DEFAULT_MAXIMUM_MOVES_PER_TARGET));

            log.info(Options.MAXIMUM_MOVES_PER_TARGET + "="
                    + maximumMovesPerTarget);

            if (maximumMovesPerTarget < 0) {

                throw new RuntimeException(Options.MAXIMUM_MOVES_PER_TARGET
                        + " must be non-negative");
                
            }
            
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

            log.info(Options.SHUTDOWN_TIMEOUT + "=" + shutdownTimeout);

        }

        if(overflowEnabled) {

            overflowService = Executors.newFixedThreadPool(1,
                    DaemonThreadFactory.defaultThreadFactory());
         
            /*
             * Note: The core thread is pre-started so that the MDC logging
             * information does not get inherited from whatever thread was
             * running the AbstractTask that wound up doing the groupCommit
             * during which overflow processing was initiated - this just cleans
             * up the log which is otherwise (even more) confusing.
             */
            
            ((ThreadPoolExecutor)overflowService).prestartCoreThread();
            
        } else {
            
            overflowService = null;
            
        }

    }

    synchronized public void shutdown() {

        if(!isOpen()) return;
        
        final long begin = System.currentTimeMillis();

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
        
        log.info("Done: elapsed="+elapsed+"ms");
        
    }

    synchronized public void shutdownNow() {

        if(!isOpen()) return;
        
        final long begin = System.currentTimeMillis();
        
        log.info("Begin");

        if(overflowService!=null)
            overflowService.shutdownNow();

        super.shutdownNow();
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        log.info("Done: elapsed="+elapsed+"ms");
        
    }

    /**
     * An overflow condition is recognized when the journal is within some
     * declared percentage of {@link Options#MAXIMUM_EXTENT}.
     */
    public boolean shouldOverflow() {
     
        if (isTransient()) {

            /*
             * Note: This is disabled in part because we can not close out and
             * then re-open a transient journal.
             */

            if (DEBUG)
                log.debug("Overflow processing not allowed for transient journals");

            return false;

        }

        if (!overflowEnabled) {
            
            if (DEBUG)
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
            
            if (INFO)
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

            /*
             * Choose maximum of the target maximum extent and the current user
             * data extent so that we do not re-trigger overflow immediately if
             * the buffer has been extended beyond the target maximum extent.
             * Among other things this lets you run the buffer up to a
             * relatively large extent (if you use a disk-only mode since you
             * will run out of memory if you use a fully buffered mode).
             */
            
            nextOffset = journal.getRootBlockView().getNextOffset();
            
            if (nextOffset > .9 * journal.getMaximumExtent()) {

                shouldOverflow = true;

            } else {
                
                shouldOverflow = false;
                
            }

            if (!shouldOverflow && DEBUG) {

                log.debug("should not overflow" + ": nextOffset=" + nextOffset
                        + ", maximumExtent=" + journal.getMaximumExtent());

            } else if (shouldOverflow && INFO) {

                log.debug("shouldOverflow" + ": nextOffset=" + nextOffset
                        + ", maximumExtent=" + journal.getMaximumExtent());

            }
               
        }

        return shouldOverflow;
        
    }

    /**
     * Core method for overflow with post-processing.
     * <p>
     * Note: This method does not test pre-conditions based on the extent of the
     * journal.
     * <p>
     * Note: The caller is responsible for ensuring that this method is invoked
     * with an exclusive lock on the write service.
     * <p>
     * Pre-conditions:
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
        
        final ConcurrencyManager concurrencyManager = (ConcurrencyManager) getConcurrencyManager();

        /*
         * We have an exclusive lock and the overflow conditions are satisifed.
         */
        final long lastCommitTime;
        final Set<String> copied = new HashSet<String>();

        // Do overflow processing.
        lastCommitTime = doSynchronousOverflow(copied);
                    
        // Note: commented out to protect access to the new journal until the write service is resumed.
        // report event.
//        notifyJournalOverflowEvent(getLiveJournal());

        if (asyncOverflowEnabled.get()) {

            log.info("Will start asynchronous overflow processing.");

            /*
             * Start the asynchronous processing of the named indices on the old
             * journal.
             */
            if(!overflowAllowed.compareAndSet(true/*expect*/, false/*set*/)) {

                throw new AssertionError();
                
            }
            
            /*
             * Aggregate the statistics for the named indices and reset the
             * various counters for those indices. These statistics are used to
             * decide which indices are "hot" and which are not.
             */
            final Counters totalCounters = concurrencyManager.getTotalIndexCounters();

            final Map<String/* name */, Counters> indexCounters = concurrencyManager
                    .resetIndexCounters();

            /*
             * Submit task on private service that will run asynchronously and clear
             * [overflowAllowed] when done.
             * 
             * Note: No one ever checks the Future returned by this method. Instead
             * the PostProcessOldJournalTask logs anything that it throws in its
             * call() method.
             */

            return overflowService.submit(new PostProcessOldJournalTask(
                    (ResourceManager) this, lastCommitTime, copied,
                    totalCounters, indexCounters));

        } else {
            
            log.warn("Asynchronous overflow processing is disabled!");

            /*
             * Note: increment the counter now since we will not do asynchronous
             * overflow processing.
             */
            
            overflowCounter.incrementAndGet();
            
            return null;
            
        }

    }
    
    /**
     * Synchronous overflow processing.
     * <p>
     * This is invoked once all pre-conditions have been satisified.
     * <p>
     * Index partitions that have fewer than some threshold #of index entries
     * will be copied onto the new journal.  Otherwise the view of the index
     * will be re-defined to place writes on the new journal and read historical
     * data from the old journal.
     * <p>
     * This uses {@link #purgeOldResources()} to delete old resources from the
     * local file system that are no longer required as determined by
     * {@link #setReleaseTime(long)} and {@link #getEffectiveReleaseTime()}.
     * <p>
     * Note: This method does NOT start a {@link PostProcessOldJournalTask}.
     * <P>
     * Note: You MUST have an exclusive lock on the {@link WriteExecutorService}
     * before you invoke this method!
     * 
     * @param copied
     *            Any index partitions that are copied are added to this set.
     * 
     * @return The lastCommitTime of the old journal.
     */
    protected long doSynchronousOverflow(Set<String> copied) {
        
        log.info("begin");
        
        /*
         * Note: We assign the same timestamp to the createTime of the new
         * journal and the closeTime of the old journal.
         */
        final long createTime = nextTimestampRobust();
        final long closeTime = createTime;
        
        final AbstractJournal oldJournal = getLiveJournal();
        final ManagedJournal newJournal;

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
         */
        {

            // writes no longer accepted.
            oldJournal.closeForWrites(closeTime);

            // // remove from list of open journals.
            // storeCache.remove(oldJournal.getRootBlockView().getUUID());

            log.info("Closed out the old journal.");

        }

        /*
         * Create the new journal.
         * 
         * @todo this is not using the temp filename mechanism in a manner that
         * truely guarentees an atomic file create. The CREATE_TEMP_FILE option
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
            p.setProperty(Options.CREATE_TIME, "" + createTime);

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
         */
        {

            // add to the journalIndex and the Map<UUID,File>.
            addResource(newJournal.getResourceMetadata(), newJournal.getFile());

            // add to the cache.
            storeCache.put(newJournal.getRootBlockView().getUUID(), newJournal,
                    false/* dirty */);

            // atomic cutover.
            this.liveJournalRef.set( newJournal );

            log.info("Changed over to a new live journal");

        }
        
        /*
         * Overflow each index by re-defining its view on the new journal.
         */
        int noverflow = 0;
        // #of indices with at least one index entry that were copied.
        int numNonZeroCopy = 0;
        // Maximum #of non-zero indices that we will copy over.
        final int maxNonZeroCopy = 100;
        final long lastCommitTime = oldJournal.getRootBlockView().getLastCommitTime();
        final long firstCommitTime;
        {

            if(INFO)
            log.info("doOverflow(): lastCommitTime=" + lastCommitTime + "\nfile="
                    + oldJournal.getFile()
                    + "\npre-condition views: overflowCounter="
                    + getOverflowCount() + "\n"
                    + listIndexPartitions(-lastCommitTime));

            // using read-committed view of Name2Addr
            final int nindices = (int) oldJournal.getName2Addr().rangeCount(null,null);

            // using read-committed view of Name2Addr
            final ITupleIterator itr = oldJournal.getName2Addr().rangeIterator(null,null);

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final Entry entry = EntrySerializer.INSTANCE
                        .deserialize(new DataInputBuffer(tuple.getValue()));

                // old index (just the mutable btree on the old journal, not the full view of that index).
                final BTree oldBTree = (BTree) oldJournal.getIndex(entry.checkpointAddr);

                // #of index entries on the old index.
                final int entryCount = oldBTree.getEntryCount();
                
                // clone index metadata.
                final IndexMetadata indexMetadata = oldBTree.getIndexMetadata()
                        .clone();

                // old partition metadata.
                final LocalPartitionMetadata oldpmd = indexMetadata
                        .getPartitionMetadata();
                
                // true iff an overflow handler is defined.
                final boolean hasOverflowHandler = indexMetadata.getOverflowHandler() != null;

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
                     */

                    throw new RuntimeException("Not a partitioned index: "
                            + entry.name);
                     
                    
                }
                
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
                 * Otherwise we will let the asynchronous post-processing figure
                 * out what it wants to do with this index partition.
                 */
                final boolean copyIndex = (entryCount == 0)
                        || ((copyIndexThreshold > 0 && entryCount <= copyIndexThreshold) //
                                && numNonZeroCopy < maxNonZeroCopy//
                                && !hasOverflowHandler);

                if(copyIndex) {
                    
                    /*
                     * We will copy the index data from the B+Tree old journal
                     * (but not from the full index view) onto the new journal.
                     * In this case the index will use a view that DOES NOT
                     * include the old index on the old journal.
                     */

                    final IResourceMetadata[] oldResources = oldpmd.getResources();

                    final IResourceMetadata[] newResources = new IResourceMetadata[oldResources.length];

                    System.arraycopy(oldResources, 0, newResources, 0, oldResources.length);

                    // new resource is listed first (reverse chronological order)
                    newResources[0] = newJournal.getResourceMetadata();

                    // describe the index partition.
                    indexMetadata
                            .setPartitionMetadata(new LocalPartitionMetadata(
                                    oldpmd.getPartitionId(),//
                                    oldpmd.getLeftSeparatorKey(),//
                                    oldpmd.getRightSeparatorKey(),//
                                    newResources, //
                                    oldpmd.getHistory()+
                                    "copy(lastCommitTime="+lastCommitTime+",entryCount="+entryCount+",counter="+oldBTree.getCounter().get()+") "
                            ));

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

                    // new resource is listed first (reverse chronological order).
                    newResources[0] = newJournal.getResourceMetadata();

                    // describe the index partition.
                    indexMetadata
                            .setPartitionMetadata(new LocalPartitionMetadata(
                                    oldpmd.getPartitionId(),//
                                    oldpmd.getLeftSeparatorKey(),//
                                    oldpmd.getRightSeparatorKey(),//
                                    newResources, //
                                    oldpmd.getHistory()+
                                    "overflow(lastCommitTime="+lastCommitTime+",entryCount="+entryCount+",counter="+oldBTree.getCounter().get()+") "
                            ));

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

                    if(INFO)
                    log.info("Re-defining view on new journal"//
                            + ": name=" + entry.name //
                            + ", copyIndex=" + copyIndex//
//                            + ", copyIndexThreashold=" + copyIndexThreshold //
                            + ", entryCount=" + entryCount//
                            + ", counter=" + oldCounter//
                            + ", partitionId="+oldpmd.getPartitionId()//
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
                    
                    if(copyIndex) {
                        
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
                        
                        if(DEBUG)
                        log.debug("Copying data to new journal: name=" + entry.name
                                + ", entryCount=" + entryCount + ", threshold="
                                + copyIndexThreshold);
                        
                        newBTree.rangeCopy(oldBTree, null, null, true/*overflow*/);

                        // Note that index partition was copied for the caller.
                        copied.add(entry.name);
                        
                        if (entryCount > 0) {
                            
                            // count copied indices with at least one index entry.
                            numNonZeroCopy++;
                            
                        }

                    }
                    
                    /*
                     * Register the new B+Tree on the new journal.
                     */
                    newJournal.registerIndex(entry.name, newBTree);

                }

                noverflow++;

//                log.info("Did overflow: " + noverflow + " of " + nindices
//                        + " : " + entry.name);

            }

            if(INFO)
            log.info("Did overflow of " + noverflow + " indices");

            assert nindices == noverflow;

            // make the index declarations restart safe on the new journal.
            firstCommitTime = newJournal.commit();

        }

        /*
         * Show the new views once we have cut over to the new journal. if we do
         * this before we cut over then the data will still be read from the old
         * journal.
         */
        if(INFO)
        log.info("\ndoOverflow(): firstCommitTime=" + firstCommitTime
                + "\nfile=" + newJournal.getFile()
                + "\npost-condition views: overflowCounter="
                + getOverflowCount() + "\n"
                + listIndexPartitions(-firstCommitTime));
        
        /*
         * Change over the counter set to the new live journal.
         * 
         * Note: The spelling of the counter set names MUST be consistent with
         * their declarations!
         */
        try {

            final CounterSet tmp = (CounterSet)getCounters();
            
            tmp.detach("Live Journal");
            
            tmp.makePath("Live Journal").attach(getLiveJournal().getCounters());

        } catch(Throwable t) {
            
            log.warn("Problem updating counters: "+t, t);
            
        }

        /*
         * Cause old resources to be deleted on the file system.
         * 
         * Note: This is run while we have a lock on the write executor service
         * so that we can guarentee that no tasks are running with access to
         * historical views which might be deleted when we purge old resources.
         */
        purgeOldResources();
        
        log.info("end");
        
        return lastCommitTime;

    }
    
}
