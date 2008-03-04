/**

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 13, 2007
 */

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.Counters;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IsolationEnum;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedBigdataFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.MetadataService;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * The {@link ResourceManager} is responsible for locating, opening, closing
 * managed resources (journals and index segements), for providing views of an
 * index partition, and integrates concurrency controls such that you can run
 * procedures against either individual indices or index views. In addition,
 * this class provides event reporting for resources and facilitates resource
 * transfer for the distributed database architecture.
 * <p>
 * This class is responsible for integrating events that report on resource
 * consumption and latency and taking actions that may seek to minimize latency
 * or resource consumption.
 * <p>
 * Resource consumption events include
 * <ol>
 * <li>mutable unisolated indices open on the journal</li>
 * <li>mutable isolated indices open in writable transactions</li>
 * <li>historical read-only indices open on old journals</li>
 * <li>historical read-only index segments</li>
 * </ol>
 * 
 * The latter two classes of event sources exist iff {@link Journal#overflow()}
 * is handled by creating a new {@link Journal} and evicting data from the old
 * {@link Journal} asynchronously onto read-optimized {@link IndexSegment}s.
 * 
 * Other resource consumption events deal directly with transactions
 * <ol>
 * <li>open a transaction</li>
 * <li>close a transaction</li>
 * <li>a heartbeat for each write operation on a transaction is used to update
 * the resource consumption of the store</li>
 * </ol>
 * 
 * <p>
 * Latency events include
 * <ol>
 * <li>request latency, that is, the time that a request waits on a queue
 * before being serviced</li>
 * <li>transactions per second</li>
 * </ol>
 * 
 * @todo update javadoc.
 * 
 * @todo recommend setting the
 *       {@link com.bigdata.journal.Options#INITIAL_EXTENT} to the
 *       {@link com.bigdata.journal.Options#MAXIMUM_EXTENT} in minimize the
 *       likelyhood of having to extend the journal and in order to keep the
 *       allocation size on the file system large to minimize fragmentation?
 *       <p>
 *       Also, consider moving the target journal size up to 500M.
 * 
 * @todo track the disk space used by the {@link #getDataDir()} and the free
 *       space remaining on the mount point that hosts the data directory. if we
 *       approach the limit on the space in use then we need to shed index
 *       partitions to other data services or potentially become more aggressive
 *       in releasing old resources.
 * 
 * @todo The choice of the #of offset bits governs both the maximum #of records
 *       (indirectly through the maximum byte offset) and the maximum record
 *       length (directly).
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
 *       {@link EmbeddedBigdataFederation} or a distributed federation. This
 *       means that the cost of a full compacting merge for an index partition
 *       is capped by the size limits we place on index partitions regardless of
 *       the total size of the scale-out index.
 * 
 * @todo Transparent promotion of unpartitioned indices to indicate that support
 *       delete markers and can therefore undergo {@link #overflow()}. This is
 *       done by defining one partition that encompases the entire legal key
 *       range and setting the resource metadata for the view.
 *       <p>
 *       Transparent promotion of indices to support delete markers on
 *       {@link #overflow()}? We don't need to maintain delete markers until
 *       the first overflow event....
 *       <P>
 *       Do NOT break the ability to use concurrency control on unpartitioned
 *       indices -- note that overflow handling will only work on that support
 *       deletion markers.
 * 
 * @todo Scale-out import and index recovery
 *       <p>
 *       Note that key range partitioned indices are simply registered under a
 *       name that reflects both the scale-out index name and the index
 *       partition#. The metadata index provides the integrating structure for
 *       those individual index partitions. A {@link ClientIndexView} uses the
 *       {@link MetadataIndex} to provide transparent access to the scale-out
 *       index.
 *       <p>
 *       It should be possible to explicitly convert an index that supports
 *       delete markers into a scale-out index. The right model might be to
 *       "import" the index into an existing federation since there needs to be
 *       an explicit {@link MetadataService} on hand. There will only be a
 *       single partition of the index initially, but that can be broken down
 *       either because it is too large or because it becomes too large. The
 *       import can be realized by moving all of the data off of the journal
 *       onto one or more {@link IndexSegment}s, moving those index segment
 *       files into a selected data service, and then doing an "index recovery"
 *       operation that hooks up the index segment(s) into an index and
 *       registers the metadata index for that index. (Note that we can't make
 *       an index into a key-range partitioned index unless there is a metadata
 *       index lying around somewhere.)
 *       <p>
 *       Work through a federated index recovery where we re-generate the
 *       metadata index from the on hand data services.
 * 
 * FIXME review use of synchronization and make sure that there is no way in
 * which we can double-open a store or index.
 * 
 * @todo refactor logging calls into uses of a {@link ResourceManager} instance?
 * 
 * @todo use {@link MDC} to put metadata into the logging context {thread, host,
 *       dataService, global index name, local index name (includes the index
 *       partition), etc}.
 * 
 * @todo Use a hard reference queue to track recently used AbstractBTrees (and
 *       stores?). Add a public referenceCount field on AbstractBTree and close
 *       the AbstractBTree on eviction from the hard reference queue iff the
 *       referenceCount is zero (no references to that AbstractBTree remain on
 *       the hard reference queue).
 * 
 * @todo re-examine the caching for B+Trees from the perspective of the
 *       {@link ResourceManager}. Ideally a checkpoint operation will not
 *       discard the per-btree node / leaf cache (the write retention and/or
 *       read retention queues). Equally, it would be nice if read-committed and
 *       historical reads for "hot" points (such as the lastCommitTime of the
 *       old journal or an intensive tx) were able to benefit from a read-cache
 *       at the node/leaf or record level. Also, note that {@link IndexSegment}s
 *       may be reused in a number of views, e.g., both the unisolated and
 *       read-committed view of an index, but that all of those views share the
 *       same {@link IndexSegment} instance and hence the same read cache. This
 *       makes it worth while to fully buffer the nodes of the index segment,
 *       but since the branching factor is larger the write/read retention queue
 *       should be smaller.
 * 
 * @todo Consider the use of an inner class to impose read-only semantics on a
 *       BTree in {@link #getIndex(String, long)} or
 *       {@link #getIndexOnStore(String, long, IRawStore)} so that the result
 *       will still be a BTree but it will not be mutable. This is just extract
 *       protection against attempts to write on historical views. Since the
 *       IndexSegment is always read-only we do not have to anything for that
 *       case.
 * 
 * @todo consider handling close out of index partitions "whole at once" to
 *       include all index segments in the current view of that partition. this
 *       probably does not matter but might be a nicer level of aggregation than
 *       the individual index segment. It's easy enough to identify the index
 *       segments from the btree using the partition metadata record. However,
 *       it is harder to go the other way (and in fact impossible since the same
 *       index segment can be used in multiple index partition views as the
 *       partition definition evolves - in contrast 1st btree in the index
 *       partition view always has the current partition metadata description
 *       and therefore could be used to decrement the usage counters on the
 *       other components of that view (but not to directly close them out)).
 * 
 * @todo this still does not suggest a mechanism for close by timeout. one
 *       solutions is to just close down all open indices if the server quieses.
 *       if the server is not quiesent then unused indices will get shutdown in
 *       any case (this is basically how we are closing btrees and index
 *       segments now, so they remain available but release their resources).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ResourceManager implements IResourceManager {

    /**
     * Logger.
     * 
     * @todo change the logger configuration to write on a JMS queue or JINI
     *       discovered service in order to aggregate results from multiple
     *       hosts in a scale-out solution.
     */
    protected static final Logger log = Logger.getLogger(ResourceManager.class);

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

    static NumberFormat cf;

    static NumberFormat fpf;
    
    /**
     * Leading zeros without commas used to format the partition identifiers
     * into index segment file names.
     */
    static NumberFormat leadingZeros;

    static {

        cf = NumberFormat.getNumberInstance();

        cf.setGroupingUsed(true);

        fpf = NumberFormat.getNumberInstance();

        fpf.setGroupingUsed(false);

        fpf.setMaximumFractionDigits(2);

        leadingZeros = NumberFormat.getIntegerInstance();
        
        leadingZeros.setMinimumIntegerDigits(5);
        
        leadingZeros.setGroupingUsed(false);
        
    }

    // /*
    // * Unisolated index reporting.
    // */
    // private Map<String/*name*/,Counters> unisolated = new
    // ConcurrentHashMap<String, Counters>();

    /**
     * Report opening of a mutable unisolated named index on an {@link IJournal}.
     * 
     * @param name
     *            The index name.
     */
    static public void openUnisolatedBTree(String name) {

        if (INFO)
            log.info("name=" + name);

    }

    /**
     * Report closing of a mutable unisolated named index on an {@link IJournal}.
     * 
     * @param name
     *            The index name.
     * 
     * @todo never invoked since we do not explicitly close out indices and are
     *       not really able to differentiate the nature of the index when it is
     *       finalized (unisolated vs isolated vs index segment can be
     *       identified based on their interfaces).
     * 
     * @todo add reporting for {@link AbstractBTree#reopen()}.
     */
    static public void closeUnisolatedBTree(String name) {

        if (INFO)
            log.info("name=" + name);

    }

    /**
     * Report drop of a named unisolated index.
     * 
     * @param name
     *            The index name.
     */
    static public void dropUnisolatedBTree(String name) {

        if (INFO)
            log.info("name=" + name);

    }

    /*
     * Index segment reporting.
     */

    /**
     * Report that an {@link IndexSegment} has been opened.
     * 
     * @param name
     *            The index name or null if this is not a named index.
     * @param filename
     *            The name of the file containing the {@link IndexSegment}.
     * @param nbytes
     *            The size of that file in bytes.
     * 
     * @todo memory burden depends on the buffered data (nodes or nodes +
     *       leaves)
     * 
     * @todo the index name is not being reported since it is not part of the
     *       extension metadata record at this time. this means that we can not
     *       aggregate events for index segments for a given named index at this
     *       time (actually, we can aggregate them by the indexUUID).
     */
    static public void openIndexSegment(String name, String filename,
            long nbytes) {

        if (INFO)
            log.info("name=" + name + ", filename=" + filename + ", #bytes="
                    + nbytes);

    }

    /**
     * Report that an {@link IndexSegment} has been closed.
     * 
     * @param filename
     * 
     * @todo we do not close out index segments based on non-use (e.g., timeout
     *       or LRU).
     */
    static public void closeIndexSegment(String filename) {

        if (INFO)
            log.info("filename=" + filename);

    }

    /**
     * Report on a bulk merge/build of an {@link IndexSegment}.
     * 
     * @param builder
     *            The object responsible for building the {@link IndexSegment}.
     */
    public void notifyIndexSegmentBuildEvent(IndexSegmentBuilder builder) {

        if (INFO) {

            String name = builder.metadata.getName();
            String filename = builder.outFile.toString();
            int nentries = builder.plan.nentries;
            long elapsed = builder.elapsed;
            long commitTime = builder.checkpoint.commitTime;
            long nbytes = builder.checkpoint.length;
            
            // data rate in MB/sec.
            float mbPerSec = builder.mbPerSec;

            log.info("name=" + name + ", filename=" + filename + ", nentries="
                    + nentries + ", commitTime=" + commitTime + ", elapsed="
                    + elapsed + ", "
                    + fpf.format(((double) nbytes / Bytes.megabyte32)) + "MB"
                    + ", rate=" + fpf.format(mbPerSec) + "MB/sec");
        }

    }

    /*
     * Transaction reporting.
     * 
     * @todo the clock time for a distributed transaction can be quite different
     * from the time that a given transaction was actually open on a given data
     * service. the former is simply [commitTime - startTime] while the latter
     * depends on the clock times at which the transaction was opened and closed
     * on the data service.
     */

    /**
     * Report the start of a new transaction.
     * 
     * @param startTime
     *            Both the transaction identifier and its global start time.
     * @param level
     *            The isolation level of the transaction.
     */
    static public void openTx(long startTime, IsolationEnum level) {

        if (INFO)
            log.info("tx=" + startTime + ", level=" + level);

    }

    /**
     * Report completion of a transaction.
     * 
     * @param startTime
     *            The transaction identifier.
     * @param commitTime
     *            The commit timestamp (non-zero iff this was a writable
     *            transaction that committed successfully and zero otherwise).
     * @param aborted
     *            True iff the transaction aborted vs completing successfully.
     */
    static public void closeTx(long startTime, long commitTime, boolean aborted) {

        if (INFO)
            log.info("tx=" + startTime + ", commitTime=" + commitTime
                    + ", aborted=" + aborted + ", elapsed="
                    + (commitTime - startTime));

    }

    /**
     * Report the extension of the {@link TemporaryRawStore} associated with a
     * transaction and whether or not it has spilled onto the disk.
     * 
     * @param startTime
     *            The transaction identifier.
     * @param nbytes
     *            The #of bytes written on the {@link TemporaryRawStore} for
     *            that transaction.
     * @param onDisk
     *            True iff the {@link TemporaryRawStore} has spilled over to
     *            disk for the transaction.
     * 
     * @todo event is not reported.
     */
    static public void extendTx(long startTime, long nbytes, boolean onDisk) {

    }

    /**
     * Report the isolation of a named index by a transaction.
     * 
     * @param startTime
     *            The transaction identifier.
     * @param name
     *            The index name.
     */
    static public void isolateIndex(long startTime, String name) {

        if (INFO)
            log.info("tx=" + startTime + ", name=" + name);

        /*
         * Note: there is no separate close for isolated indices - they are
         * closed when the transaction commits or aborts. read-write indices can
         * not be closed before the transactions completes, but read-only
         * indices can be closed early and reopened as required. read-committed
         * indices are always changing over to the most current committed state
         * for an index. both read-only and read-committed indices MAY be shared
         * by more than one transaction (@todo verify that the protocol for
         * sharing is in place on the journal).
         */

    }

    /*
     * Journal file reporting.
     */

    /**
     * Report the opening of an {@link IJournal} resource.
     * 
     * @param filename
     *            The filename or null iff the journal was not backed by a file.
     * @param nbytes
     *            The total #of bytes available on the journal.
     * @param bufferMode
     *            The buffer mode in use by the journal.
     */
    static public void openJournal(String filename, long nbytes,
            BufferMode bufferMode) {

        if (INFO)
            log.info("filename=" + filename + ", #bytes=" + nbytes + ", mode="
                    + bufferMode);

    }

    /**
     * Report the extension of an {@link IJournal}.
     * 
     * @param filename
     *            The filename or null iff the journal was not backed by a file.
     * @param nbytes
     *            The total #of bytes available (vs written) on the journal.
     * 
     * @todo this does not differentiate between extension of a buffer backing a
     *       journal and extension of a {@link TemporaryRawStore}. This means
     *       that the resources allocated to a transaction vs the unisolated
     *       indices on a journal can not be differentiated.
     */
    static public void extendJournal(String filename, long nbytes) {

        if (INFO)
            log.info("filename=" + filename + ", #bytes=" + nbytes);

    }

    /**
     * Report an overflow event.
     * 
     * @param filename
     *            The filename or null iff the journal was not backed by a file.
     * @param nbytes
     *            The total #of bytes written on the journal.
     */
    public void notifyJournalOverflowEvent(AbstractJournal journal) {

        if (INFO) {

            log.info("filename=" + journal.getFile() + ", nextOffset="
                    + journal.getBufferStrategy().getNextOffset() + ", extent="
                    + journal.getBufferStrategy().getExtent() + ", maxExtent="
                    + journal.getMaximumExtent());

        }
        
    }

    /**
     * Report close of an {@link IJournal} resource.
     * 
     * @param filename
     *            The filename or null iff the journal was not backed by a file.
     */
    static public void closeJournal(String filename) {

        if (INFO)
            log.info("filename=" + filename);

    }

    /**
     * Report deletion of an {@link IJournal} resource.
     * 
     * @param filename
     *            The filename or null iff the journal was not backed by a file.
     * 
     * @todo also report deletion of resources for journals that were already
     *       closed but not yet deleted pending client leases or updates of the
     *       metadata index (in the {@link MasterJournal}).
     */
    static public void deleteJournal(String filename) {

        if (INFO)
            log.info("filename=" + filename);

    }

    /**
     * The properties given to the ctor.
     */
    private final Properties properties;
    
    /**
     * The directory in which the data files reside.
     * <p>
     * Note: It is a hard requirement that each resource is located by the
     * {@link IResourceMetadata#getFile() path} relative to the {@link #dataDir}.
     * 
     * @see Options#DATA_DIR
     * @see IResourceMetadata#getFile()
     */
    protected final File dataDir;

    /** Directory containing the journal resources. */
    protected final File journalsDir;

    /** Directory containing the index segment resources. */
    protected final File segmentsDir;

    /**
     * The directory in which the temporary files will reside.
     * 
     * @see Options#TMP_DIR
     */
    protected final File tmpDir;

    /**
     * A hard reference to the live journal.
     */
    private AbstractJournal liveJournal;

    /**
     * A temporary store used for the {@link #journalIndex} and the
     * {@link #indexSegmentIndex}. The store is not used much so it is
     * configured to keep its in-memory footprint small.
     * 
     * @todo since this is a WORM store it will never shrink in size. Eventually
     *       it could grow modestly large if the data service were to run long
     *       enough. Therefore it makes sense to periodically "overflow" this by
     *       copying the current state of the {@link #journalIndex} and the
     *       {@link #indexSegmentIndex} onto a new tmpStore (or just rebuilding
     *       them from the {@link #dataDir}).
     */
    private final IRawStore tmpStore = new TemporaryRawStore(
            WormAddressManager.DEFAULT_OFFSET_BITS,//
            10 * Bytes.kilobyte, // initial in memory extent
            100* Bytes.megabyte, // maximum in memory extent
            false // useDirectBuffers
            );

    /**
     * A map over the journal histories. The map is transient and is
     * re-populated from a scan of the file system during startup.
     * <P>
     * The keys are the timestamp at which the journal was put into service. The
     * values are the journal resource descriptions. Given the timestamp of some
     * historical state of an index, this map is used to locate the journal on
     * which that historical state of the index would be found.
     */
    final private JournalIndex journalIndex;

    /**
     * A map over the index segments by ascending createTime and UUID. The map
     * is transient and is re-populated from a scan of the file system during
     * startup.
     * <p>
     * The keys are the createTime of the index segment followed by the index
     * segment UUID (to break ties). The values are the
     * {@link IResourceMetadata} object describing that index segment. This map
     * is used to provide some basic reporting but is primarily used to delete
     * index segment resources once they are no longer required.
     */
    final private IndexSegmentIndex indexSegmentIndex;
    
    /**
     * Set <code>true</code> by {@link #start()} and remains <code>true</code>
     * until the {@link ResourceManager} is shutdown.
     */
    private boolean open;

    /**
     * <code>true</code> iff {@link BufferMode#Transient} was indicated.
     */
    private final boolean isTransient;
    
    /**
     * Set based on {@link Options#COPY_INDEX_THRESHOLD}.
     * 
     * @todo make this a per-index option on {@link IndexMetadata}?
     */
    final int copyIndexThreshold;
    
    /** Release time is zero (0L) until notified otherwise - 0L is ignored. */
    private long releaseTime = 0L;

    /**
     * Resources MUST be at least this many milliseconds before they may be
     * deleted.
     * <p>
     * The minReleaseAge is just how long you want to hold onto an immortal
     * database view. E.g., 3 days of full history. There should be a setting,
     * e.g., zero (0L), for a true immortal database.
     * 
     * @see Options#MIN_RELEASE_AGE
     */
    final long minReleaseAge;
    
    /**
     * @throws IllegalStateException
     *             unless open.
     */
    protected void assertOpen() {
        
        if (!open)
            throw new IllegalStateException();
        
    }
    
    /**
     * A map from the resource UUID to the absolute {@link File} for that
     * resource.
     * <p>
     * Note: The {@link IResourceMetadata} reported by an
     * {@link AbstractJournal} or {@link IndexSegmentFileStore} generally
     * reflects the name of the file as specified to the ctor for that class, so
     * it may be relative to some arbitrary directory or absolute within the
     * file system.
     * 
     * @todo We do not need to insist on the file names - we could just use the
     *       file under whatever name we find it (Therefore we only insist that
     *       the name of the file (as understood by the {@link ResourceManager})
     *       agree with the {@link IResourceMetadata} and the
     *       {@link ResourceManager} maintains its own map from the
     *       {@link IResourceMetadata} to an <em>absolute</em> path for that
     *       resource.)
     */
    private Map<UUID, File> resourceFiles = new HashMap<UUID, File>();

    /**
     * A map from the resource description to the open resource.
     */
    private Map<UUID, IRawStore> openStores = new HashMap<UUID, IRawStore>();

    /**
     * The timeout for {@link #shutdown()} -or- ZERO (0L) to wait for ever.
     * 
     * @todo config param.
     */
    final private long shutdownTimeout = 5*1000;

    /**
     * The service that runs asynchronous resource management tasks, primarily
     * handling index segment builds and enrolling the index segments into use.
     * <p>
     * Note: index partition joins need to be directed top-down since the index
     * partitions need to be co-located on a data service before we can join
     * them.
     */
    ExecutorService service;

    /**
     * A flag used to disable overflow of the live journal until asynchronous
     * post-processing of the old journal has been completed.
     * 
     * @see PostProcessOldJournalTask
     */
    protected final AtomicBoolean overflowAllowed = new AtomicBoolean(true);

    /**
     * #of overflows that have taken place. This counter is incremented each
     * time the entire overflow operation is complete, including any
     * post-processing of the old journal.
     */
    public final AtomicLong overflowCounter = new AtomicLong(0L);
    
    /**
     * <code>true</code> unless an overflow event is currently being
     * processed.
     */
    public boolean isOverflowAllowed() {
        
        return overflowAllowed.get();
        
    }
    
    /**
     * {@link ResourceManager} options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options {

        /**
         * <code>data.dir</code> - The property whose value is the name of the
         * directory in which the store files will be created (no default). This
         * property is required unless the instance is transient. If you specify
         * {@link com.bigdata.journal.Options#BUFFER_MODE} as
         * {@link BufferMode#Transient} then journals will be NOT stored in the
         * file system and {@link ResourceManager#overflow()} will be disabled.
         * <p>
         * The files are created within subdirectories as follows: The
         * "journals" subdirectory contains the journal files. The "segments"
         * directory contains subdirectories corresponding to the index UUID for
         * each scale-out index. Within those index-specific directories, the
         * index segment files are assigned to files using the temporary file
         * mechanisms using the munged index name as the file prefix and
         * {@link Options#SEG} as the file suffix.  If the index is partitioned
         * then the partition identifier appears as part of the file prefix.
         * <p>
         * Note: While files are stored per the scheme described above, the
         * entire {@link #DATA_DIR} will be scanned recursively to identify all
         * journal files and index segments during startup. Files will be used
         * whereever they are found but the {@link IResourceMetadata#getFile()}
         * read from a given resource MUST correspond to its relative location
         * within the {@link #DATA_DIR}.
         * <p>
         * Note: Each {@link DataService} or {@link MetadataService} MUST have
         * its own {@link #DATA_DIR}.
         * 
         * @todo When copying a resource from a remote {@link DataService} make
         *       sure that it gets placed according to the scheme defined here.
         * 
         * @todo Perhaps there is no need for the "file" in the
         *       {@link IResourceMetadata} since it appears that we need to
         *       maintain a map from UUID to local file system files in anycase.
         */
        public static final String DATA_DIR = "data.dir";

        /**
         * Index partitions having no more than this many entries as reported by a 
         * range count will be copied to the new journal during overflow processing
         * rather than building a new index segment from the buffered writes (default
         * is <code>1000</code>).  When ZERO (0), index partitions will never be copied
         * during overflow processing.
         * 
         * @see #DEFAULT_COPY_INDEX_THRESHOLD
         */
        public static final String COPY_INDEX_THRESHOLD = "copyIndexThreshold";

        public static final String DEFAULT_COPY_INDEX_THRESHOLD = "1000";
        
        /**
         * How long you want to hold onto the database history (in milliseconds)
         * or <code>0L</code> for an immortal database. Some convenience
         * values have been declared.
         * 
         * @see DEFAULT_MIN_RELEASE_AGE
         * @see MIN_RELEASE_AGE_1H
         * @see MIN_RELEASE_AGE_1D
         * @see MIN_RELEASE_AGE_1W
         */
        public static final String MIN_RELEASE_AGE = "minReleaseAge";

        /** Minimum release age is one hour. */
        public static final String MIN_RELEASE_AGE_1H = ""+1/*hr*/*60/*mn*/*60/*sec*/*1000/*ms*/;
        /** Minimum release age is one day. */
        public static final String MIN_RELEASE_AGE_1D = ""+24/*hr*/*60/*mn*/*60/*sec*/*1000/*ms*/;
        /** Minimum release age is one week. */
        public static final String MIN_RELEASE_AGE_1W = ""+7/*d*/*24/*hr*/*60/*mn*/*60/*sec*/*1000/*ms*/;

        /** Default minimum release age is one day. */
        public static final String DEFAULT_MIN_RELEASE_AGE = MIN_RELEASE_AGE_1D;

    }
    
    private IConcurrencyManager concurrencyManager;
    
    /**
     * The object used to control access to the index resources.
     * 
     * @throws IllegalStateException
     *             if the object has not been set yet using
     *             {@link #setConcurrencyManager(IConcurrencyManager)}.
     */
    public IConcurrencyManager getConcurrencyManager() {
        
        if(concurrencyManager==null) {
            
            // Not assigned!
            
            throw new IllegalStateException();
            
        }
        
        return concurrencyManager;
        
    }

    public void setConcurrencyManager(IConcurrencyManager concurrencyManager) {

        if (concurrencyManager == null)
            throw new IllegalArgumentException();

        if (this.concurrencyManager != null)
            throw new IllegalStateException();

        this.concurrencyManager = concurrencyManager;
        
    }

    protected long nextTimestamp() {
        
        return getConcurrencyManager().getTransactionManager().nextTimestamp();
        
    }

    /**
     * (Re-)open the {@link ResourceManager}.
     * <p>
     * Note: You MUST use {@link #setConcurrencyManager(IConcurrencyManager)}
     * after calling this constructor (the parameter can not be passed in since
     * there is a circular dependency between the {@link IConcurrencyManager}
     * and {@link #commit(long)} on this class, which requires access to the
     * {@link IConcurrencyManager} to submit a task).
     * 
     * @param properties
     *            See {@link Options}.
     * 
     * @see #start()
     */
    public ResourceManager(Properties properties) {

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties;

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

        // minimum release age
        {

            minReleaseAge = Long.parseLong(properties.getProperty(
                    Options.MIN_RELEASE_AGE, Options.DEFAULT_MIN_RELEASE_AGE));

            log.info(Options.MIN_RELEASE_AGE + "=" + minReleaseAge);

            if (minReleaseAge < 0L) {

                throw new RuntimeException(Options.MIN_RELEASE_AGE
                        + " must be non-negative");

            }

        }
        
        /*
         * Create the _transient_ index in which we will store the mapping from
         * the commit times of the journals to their resource descriptions.
         */
        journalIndex = JournalIndex.create(tmpStore);
        indexSegmentIndex = IndexSegmentIndex.create(tmpStore);

        log.info("Current working directory: "
                + new File(".").getAbsolutePath());

        // true iff transient journals is requested.
        isTransient = BufferMode.valueOf(properties.getProperty(
                Options.BUFFER_MODE, Options.DEFAULT_BUFFER_MODE.toString())) == BufferMode.Transient;
        
        /*
         * data directory.
         */
        if (isTransient) {

            /*
             * Transient.
             */
            
            dataDir = null;
            
            journalsDir = null;
            
            segmentsDir = null;
            
        } else {

            /*
             * Persistent.
             */
            
            // Note: dataDir is _canonical_
            final File dataDir;
            try {

                final String val = properties.getProperty(Options.DATA_DIR);
                
                if (val == null) {
                    
                    throw new RuntimeException("Required property: "+Options.DATA_DIR);
                    
                }
                
                // Note: stored in canonical form.
                dataDir = new File(val).getCanonicalFile();

                log.info(Options.DATA_DIR + "=" + dataDir);

                journalsDir = new File(dataDir, "journals").getCanonicalFile();

                segmentsDir = new File(dataDir, "segments").getCanonicalFile();

            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

            if (!dataDir.exists()) {

                log.info("Creating: " + dataDir);

                if (!dataDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + dataDir.getAbsolutePath());

                }

            }

            if (!journalsDir.exists()) {

                log.info("Creating: "+journalsDir);

                if (!journalsDir.mkdir()) {

                    throw new RuntimeException("Could not create directory: "
                            + journalsDir.getAbsolutePath());

                }

            }

            if (!segmentsDir.exists()) {

                log.info("Creating: "+segmentsDir);
                
                if (!segmentsDir.mkdir()) {

                    throw new RuntimeException("Could not create directory: "
                            + segmentsDir.getAbsolutePath());

                }

            }
            
            // verify all are directories vs regular files.
            
            if (!dataDir.isDirectory()) {

                throw new RuntimeException("Not a directory: "
                        + dataDir.getAbsolutePath());

            }

            if (!journalsDir.isDirectory()) {

                throw new RuntimeException("Not a directory: "
                        + journalsDir.getAbsolutePath());

            }

            if (!segmentsDir.isDirectory()) {

                throw new RuntimeException("Not a directory: "
                        + segmentsDir.getAbsolutePath());

            }

            this.dataDir = dataDir;

        }

        // temp directory.
        {

            // Note: tmpDir is _canonical_
            final File tmpDir;
            try {
                
                tmpDir = new File(properties.getProperty(
                    Options.TMP_DIR, System.getProperty("java.io.tmpdir")))
                    .getCanonicalFile();
                
            } catch(IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            log.info(Options.TMP_DIR + "=" + tmpDir);

            if (!tmpDir.exists()) {

                log.info("Creating temp directory: " + tmpDir);

                if (!tmpDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + tmpDir.getAbsolutePath());

                }

            }

            if (!tmpDir.isDirectory()) {

                throw new RuntimeException("Not a directory: "
                        + tmpDir.getAbsolutePath());

            }

            this.tmpDir = tmpDir;

        }
        
    }

    /**
     * Starts up the {@link ResourceManager}.
     * 
     * @throws IllegalStateException
     *             if the {@link IConcurrencyManager} has not been set.
     * 
     * @throws IllegalStateException
     *             if the the {@link ResourceManager} is already running.
     */
    synchronized public void start() {
    
        getConcurrencyManager();
        
        if (open) {
            
            throw new IllegalStateException();
            
        }
        
        /*
         * Look for pre-existing data files.
         */
        if(!isTransient) {

            log.info("Starting scan of data directory: " + dataDir);

            Stats stats = new Stats();

            scanDataDirectory(dataDir, stats);

            log.info("Data directory contains " + stats.njournals
                    + " journals and " + stats.nsegments
                    + " index segments for a total of " + stats.nfiles
                    + " resources and " + stats.nbytes + " bytes");

            assert journalIndex.getEntryCount() == stats.njournals;

            assert indexSegmentIndex.getEntryCount() == stats.nsegments;

            assert resourceFiles.size() == stats.nfiles;

        }

        /*
         * Open the "live" journal.
         */
        {

            final Properties p = getProperties();
            final File file;
            final boolean newJournal;
            
            if (journalIndex.getEntryCount() == 0) {

                /*
                 * There are no existing journal files. Create new journal using
                 * a unique filename in the appropriate subdirectory of the data
                 * directory.
                 * 
                 * @todo this is not using the temp filename mechanism in a
                 * manner that truely guarentees an atomic file create. The
                 * CREATE_TEMP_FILE option should probably be extended with a
                 * CREATE_DIR option that allows you to override the directory
                 * in which the journal is created. That will allow the atomic
                 * creation of the journal in the desired directory without
                 * changing the existing semantics for CREATE_TEMP_FILE.
                 */

                log.info("Creating initial journal");

                // unique file name for new journal.
                if (isTransient) {

                    file = null;
                    
                } else {
                    
                    try {

                        file = File.createTempFile("journal", // prefix
                                Options.JNL,// suffix
                                journalsDir // directory
                                ).getCanonicalFile();

                    } catch (IOException e) {

                        throw new RuntimeException(e);

                    }

                    // delete temp file.
                    file.delete();
                    
                }

                /*
                 * Set the createTime on the new journal resource.
                 */
                p.setProperty(Options.CREATE_TIME, "" + nextTimestamp());
                
                newJournal = true;

            } else {

                /*
                 * There is at least one pre-existing journal file, so we open
                 * the one with the largest timestamp - this will be the most
                 * current journal and the one that will receive writes until it
                 * overflows.
                 */

                // resource metadata for journal with the largest timestamp.
                final IResourceMetadata resource = journalIndex
                        .find(Long.MAX_VALUE);

                log.info("Will open " + resource);

                assert resource != null : "No resource? : timestamp="
                        + Long.MAX_VALUE;

                // lookup absolute file for that resource.
                file = resourceFiles.get(resource.getUUID());

                assert file != null : "No file? : resource=" + resource;

                log.info("Opening most recent journal: " + file + ", resource="
                        + resource);

                newJournal = false;

            }

            if(!isTransient) {

                assert file.isAbsolute() : "Path must be absolute: " + file;

                p.setProperty(Options.FILE, file.toString());
                
            }

            // Create/open journal.
            liveJournal = new ManagedJournal(p);

            if (newJournal) {

                // add to the set of managed resources.
                addResource(liveJournal.getResourceMetadata(), liveJournal
                        .getFile());
                
            }

            // add to set of open stores.
            if (openStores.put(liveJournal.getRootBlockView().getUUID(),
                    liveJournal) != null) {

                throw new AssertionError();

            }

        }

        service = Executors.newSingleThreadExecutor(DaemonThreadFactory.defaultThreadFactory());
        
        open = true;

    }

    /**
     * Helper class gathers statistics about files during a scan.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class Stats {

        public int nfiles;

        public int njournals;

        public int nsegments;

        public long nbytes;

    };

    /**
     * Recursively scan a directory structure identifying all journal and index
     * segment resources and populating the internal {@link #resourceFiles} map.
     * In addition, all journal files are listed in the {@link #journals} map so
     * that we can find the relevant journal quickly for a given timestamp.
     * <p>
     * Note: This requires that we open each resource in order to extract its
     * {@link IResourceMetadata} description. We only open the {@link IRawStore}
     * for the resource, not its indices. The stores are closed again
     * immediately.
     * 
     * @param dir
     *            A directory to scan.
     */
    private void scanDataDirectory(File dir, Stats stats) {

        if (dir == null)
            throw new IllegalArgumentException();

        if (!dir.isDirectory())
            throw new IllegalArgumentException();

        final File[] files = dir.listFiles(newFileFilter());

        for (final File file : files) {

            if (file.isDirectory()) {

                scanDataDirectory(file, stats);

            } else {

                scanFile(file, stats);

            }

        }

    }

    private void scanFile(File file, Stats stats) {

        final IResourceMetadata resource;

        final String name = file.getName();

        if (name.endsWith(Options.JNL)) {

            Properties properties = getProperties();

            properties.setProperty(Options.FILE, file.getAbsolutePath());

            properties.setProperty(Options.READ_ONLY, "true");

            AbstractJournal tmp = new ManagedJournal(properties);

            resource = tmp.getResourceMetadata();

            stats.njournals++;
            stats.nfiles++;
            stats.nbytes += tmp.getBufferStrategy().getExtent();

            tmp.close();

        } else if (name.endsWith(Options.SEG)) {

            IndexSegmentFileStore segStore = new IndexSegmentFileStore(file
                    .getAbsoluteFile());

            resource = segStore.getResourceMetadata();

            stats.nsegments++;
            stats.nfiles++;
            stats.nbytes += segStore.size();

            segStore.close();

        } else {

            /*
             * This file is not relevant to the resource manager.
             */

            log.warn("Ignoring file: " + file);

            return;

        }

        log.info("Found " + resource + " in " + file);

        if (!file.getName().equals(new File(resource.getFile()).getName())) {

            /*
             * The base name and extension of the file does not agree with that
             * metadata reported by the store (unlikely since the store reports
             * its metadata based on the file that it opened).
             */

            log.error("Resource out of place: actual=" + file + ", expected="
                    + file);

        }

        addResource(resource,file.getAbsoluteFile());
        
    }

    /**
     * An object wrapping the {@link Properties} given to the ctor.
     */
    public Properties getProperties() {

        return new Properties(this.properties);

    }
    
    public File getTmpDir() {
        
        return tmpDir;
        
    }
    
    /**
     * Note: The returned {@link File} is in canonical form.
     */
    public File getDataDir() {
        
        return dataDir;
        
    }

    public void shutdown() {

        assertOpen();
        
        /*
         * Note: when the timeout is zero we approximate "forever" using
         * Long.MAX_VALUE.
         */

        final long shutdownTimeout = this.shutdownTimeout == 0L ? Long.MAX_VALUE
                : this.shutdownTimeout;
        
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        
        final long begin = System.currentTimeMillis();
        
        service.shutdown();

        try {

            log.info("Awaiting service termination");
            
            long elapsed = System.currentTimeMillis() - begin;
            
            if(!service.awaitTermination(shutdownTimeout-elapsed, unit)) {
                
                log.warn("Service termination: timeout");
                
            }

        } catch(InterruptedException ex) {
            
            log.warn("Interrupted awaiting service termination.", ex);
            
        }

        closeStores();
        
        tmpStore.closeAndDelete();
        
    }

    public void shutdownNow() {

        assertOpen();

        service.shutdownNow();

        closeStores();

        tmpStore.closeAndDelete();

    }

    private void closeStores() {
        
        open = false;

        Iterator<IRawStore> itr = openStores.values().iterator();

        while (itr.hasNext()) {

            IRawStore store = itr.next();

            store.close();

            itr.remove();

        }

    }
    
    /**
     * The #of journals on hand.
     */
    public int getJournalCount() {

        return journalIndex.getEntryCount();

    }

    public int getIndexSegmentCount() {
        
        return indexSegmentIndex.getEntryCount();
        
    }
    
    /**
     * The journal on which writes are made.
     */
    public AbstractJournal getLiveJournal() {

        return liveJournal;

    }

    /*
     * @todo write tests for unisolated and read-committed. make sure that there
     * is no fencepost for read committed immediately after an overflow (there
     * should not be since we do a commit when we register the indices on the
     * new store).
     */
    public AbstractJournal getJournal(long timestamp) {

        if (timestamp == ITx.UNISOLATED || timestamp == ITx.READ_COMMITTED) {

            /*
             * This is a request for the live journal.
             * 
             * Note: The live journal remains open except during overflow, when
             * it is changed to a new journal and the old live journal is
             * closed. Therefore we NEVER cause the live journal to be opened
             * from the disk in this method.
             */

            assert liveJournal != null;
            assert liveJournal.isOpen();
            assert !liveJournal.isReadOnly();

            return getLiveJournal();
            
        }

        final IResourceMetadata resource;

        synchronized (journalIndex) {

            resource = journalIndex.find(Math.abs(timestamp));

        }

        if (resource == null) {

            log.info("No such journal: timestamp="+timestamp);
            
            return null;
            
        }
        
        return (AbstractJournal) openStore(resource.getUUID());

    }

    /**
     * Opens an {@link IRawStore}.
     * 
     * @param uuid
     *            The UUID identifying that store file.
     * 
     * @return The open {@link IRawStore}.
     * 
     * @throws RuntimeException
     *             if something goes wrong.
     */
    synchronized public IRawStore openStore(UUID uuid) {

        if (uuid == null) {

            throw new IllegalArgumentException();

        }

        /*
         * Check to see if the given resource is already open.
         * 
         * Note: The live journal remains open except during overflow, when it
         * is changed to a new journal and the old live journal is closed.
         * Therefore we NEVER cause the live journal to be opened from the disk
         * in this method.
         */
        
        assert liveJournal != null;
        assert liveJournal.isOpen();
        assert !liveJournal.isReadOnly();

        IRawStore store = openStores.get(uuid);

        if (store != null) {

            if (!store.isOpen()) {

                if (store instanceof IndexSegmentFileStore) {

                    /*
                     * We can simply re-open an index segment's store file.
                     */

                    // Note: relative to the data directory!
                    final File file = resourceFiles.get(uuid);

                    if (file == null) {

                        throw new RuntimeException("Unknown resource: uuid="
                                + uuid);

                    }

                    if (!file.exists()) {

                        throw new RuntimeException(
                                "Resource file missing? uuid=" + uuid
                                        + ", file=" + file);

                    }

                    // re-open the store file.
                    ((IndexSegmentFileStore) store).reopen();

                    // done.
                    return store;

                } else {

                    /*
                     * Note: Journals should not be closed without also removing
                     * them from the list of open resources. The live journal
                     * SHOULD NOT be closed except during shutdown or overflow
                     * (when it is replaced by a new live journal).
                     */

                    throw new AssertionError();

                }

            }

            return store;

        }

        if (store == null) {

            /*
             * Attempt to open the resource.
             */

            // Lookup filename by resource UUID.
            final File file = resourceFiles.get(uuid);

            if (file == null) {

                throw new RuntimeException("Unknown resource: uuid=" + uuid);

            }

            if (!file.exists()) {

                throw new RuntimeException("Resource file missing? uuid="
                        + uuid + ", file=" + file);

            }

            final UUID actualUUID;

            if (file.getName().endsWith(Options.JNL)) {

                /*
                 * Open a historical journal.
                 * 
                 * Note: The live journal is never opened by this code path. It
                 * is opened when the resource manager is instantiated and it
                 * will remain open except during shutdown and overflow (when it
                 * is replaced by a new live journal).
                 */
                
                final Properties properties = getProperties();

                properties.setProperty(Options.FILE, file.toString());

                // All historical journals are read-only!
                properties.setProperty(Options.READ_ONLY, "true");

                final AbstractJournal journal = new ManagedJournal(properties);

                final long closeTime = journal.getRootBlockView().getCloseTime();
                
                // verify journal was closed for writes.
                assert closeTime != 0 : "Journal not closed for writes? "
                        + " : file=" + file + ", uuid=" + uuid + ", closeTime="
                        + closeTime;
                
                assert journal.isReadOnly();
                
                actualUUID = journal.getRootBlockView().getUUID();

                store = journal;

            } else {

                IndexSegmentFileStore segStore = new IndexSegmentFileStore(file);

                actualUUID = segStore.getCheckpoint().segmentUUID;

                store = segStore;

            }

            /*
             * Verify the resource UUID.
             */
            if (!actualUUID.equals(uuid)) {

                // close the resource.
                store.close();

                throw new RuntimeException("Wrong UUID: file=" + file
                        + ", expecting=" + uuid + ", actual=" + actualUUID);

            }

            assert store != null;

            assert store.isOpen();

            assert store.isStable();

        }

        // cache the reference.
        if (openStores.put(uuid, store) != null) {

            throw new AssertionError();

        }

        // return the reference to the open store.
        return store;

    }

    /**
     * Return a reference to the named index as of the specified timestamp on
     * the identified resource.
     * <p>
     * Note: The returned index is NOT isolated.
     * <p>
     * Note: When the index is a {@link BTree} associated with a historical
     * commit point (vs the live unisolated index) it will be marked as
     * {@link AbstractBTree#isReadOnly()} and
     * {@link AbstractBTree#getLastCommitTime()} will reflect the commitTime of
     * the {@link ICommitRecord} from which that {@link BTree} was loaded. Note
     * further that the commitTime MAY NOT be the same as the specified
     * <i>timestamp</i> for a number of reasons. First, <i>timestamp</i> MAY
     * be negative to indicate a historical read vs a transactional read.
     * Second, the {@link ICommitRecord} will be the record having the greatest
     * commitTime LTE the specified <i>timestamp</i>.
     * <p>
     * Note: An {@link IndexSegment} is always read-only and always reports the
     * commitTime associated with the view from which it was constructed.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            The startTime of an active transaction, <code>0L</code> for
     *            the current unisolated index view, or <code>-timestamp</code>
     *            for a historical view no later than the specified timestamp.
     * @param store
     *            The store from which the index will be loaded.
     * 
     * @return A reference to the index -or- <code>null</code> if the index
     *         was not registered on the resource as of the timestamp or if the
     *         store has no data for that timestamp.
     * 
     * @todo add hard reference queue for {@link AbstractBTree} to the journal
     *       and track the #of instances of each {@link AbstractBTree} on the
     *       queue using #referenceCount and "touch()", perhaps in Name2Addr;
     *       write tests. consider one queue for mutable btrees and another for
     *       index segments, partitioned indices, metadata indices, etc.
     *       consider the meaning of "queue length" here and how to force close
     *       based on timeout. improve reporting of index segments by name and
     *       partition.<br>
     *       Mutable indices are low-cost to close/open. Closing them once they
     *       are no longer receiving writes can release some large buffers and
     *       reduce the latency of commits since dirty nodes will already have
     *       been flushed to disk. The largest cost on re-open is de-serializing
     *       nodes and leaves for subsequent operations. Those nodes will be
     *       read from a fully buffered store, so the latency will be small even
     *       though deserialization is CPU intensive. <br>
     *       Close operations on unisolated indices need to be queued in the
     *       {@link #writeService} so that they are executed in the same thread
     *       as other operations on the unisolated index.<br>
     *       Make sure that we close out old {@link Journal}s that are no
     *       longer required by any open index. This will require a distinct
     *       referenceCount on the {@link Journal}.
     */
    public AbstractBTree getIndexOnStore(String name, long timestamp, IRawStore store) {

        if (name == null)
            throw new IllegalArgumentException();

        if (store == null)
            throw new IllegalArgumentException();
        
        final AbstractBTree btree;

        if (store instanceof IJournal) {

            // the given journal.
            final AbstractJournal journal = (AbstractJournal) store;

            if (timestamp == ITx.UNISOLATED) {

                /*
                 * Unisolated index.
                 */

                // MAY be null.
                btree = (BTree) journal.getIndex(name);

            } else if( timestamp == ITx.READ_COMMITTED ) {

                /*
                 * Read committed operation against the most recent commit point.
                 * 
                 * Note: This commit record is always defined, but that does not
                 * mean that any indices have been registered.
                 */

                final ICommitRecord commitRecord = journal.getCommitRecord();

                final long ts = commitRecord.getTimestamp();

                if (ts == 0L) {

                    log.warn("Nothing committed: read-committed operation.");

                    return null;

                }

                // MAY be null.
                btree = journal.getIndex(name, commitRecord);
                
                if (btree != null) {

                    /*
                     * Mark the B+Tree as read-only and set the lastCommitTime
                     * timestamp from the commitRecord.
                     */
                    
                    ((BTree)btree).setReadOnly(true);
                    
                    ((BTree)btree).setLastCommitTime(ts);
                    
                }

            } else {

                /*
                 * A specified historical index commit point.
                 */

                // use absolute value in case timestamp is negative.
                final long ts = Math.abs(timestamp);

                // the corresponding commit record on the journal.
                final ICommitRecord commitRecord = journal.getCommitRecord(ts);

                if (commitRecord == null) {

                    log.warn("Resource has no data for timestamp: name=" + name
                            + ", timestamp=" + timestamp + ", resource="
                            + store.getResourceMetadata());

                    return null;
                    
                }

                // open index on that journal (MAY be null).
                btree = (BTree) journal.getIndex(name, commitRecord);

                if (btree != null) {

                    /*
                     * Mark the B+Tree as read-only and set the lastCommitTime
                     * timestamp from the commitRecord.
                     */
                    
                    ((BTree)btree).setReadOnly(true);
                    
                    ((BTree)btree).setLastCommitTime(ts);
                    
                }

            }

        } else {

            final IndexSegmentFileStore segStore = ((IndexSegmentFileStore) store);

            if (timestamp != ITx.READ_COMMITTED && timestamp != ITx.UNISOLATED) {
            
                // use absolute value in case timestamp is negative.
                final long ts = Math.abs(timestamp);

                if (segStore.getCheckpoint().commitTime > ts) {

                    log.warn("Resource has no data for timestamp: name=" + name
                            + ", timestamp=" + timestamp + ", store=" + store);

                    return null;

                }

            }
            
            // Open an index segment.
            btree = segStore.load();

        }

        log.info("name=" + name + ", timestamp=" + timestamp + ", store="
                + store + " : " + btree);

        return btree;

    }

    /**
     * Return the ordered {@link AbstractBTree} sources for an index or a view
     * of an index partition. The {@link AbstractBTree}s are ordered from the
     * most recent to the oldest and together comprise a coherent view of an
     * index partition.
     * 
     * @param name
     *            The name of the index.
     * @param timestamp
     *            The startTime of an active transaction, <code>0L</code> for
     *            the current unisolated index view, or <code>-timestamp</code>
     *            for a historical view no later than the specified timestamp.
     * 
     * @return The sources for the index view -or- <code>null</code> if the
     *         index was not defined as of the timestamp.
     * 
     * @see FusedView
     */
    public AbstractBTree[] getIndexSources(String name, long timestamp) {

        log.info("name=" + name + ", timestamp=" + timestamp);

        /*
         * Open the index on the journal for that timestamp.
         */
        final AbstractBTree btree;
        {

            // the corresponding journal (can be the live journal).
            final AbstractJournal journal = getJournal(timestamp);

            if(journal == null) {
                
                log.warn("No journal with data for timestamp: name="+name+", timestamp="+timestamp);
                
                return null;
                
            }
            
            btree = getIndexOnStore(name, timestamp, journal);

            if (btree == null) {

                log.warn("No such index: name=" + name + ", timestamp="
                        + timestamp);

                return null;

            }

            log.info("View based on " + journal.getResourceMetadata());

        }

        if (btree == null) {

            // No such index.

            return null;

        }

        /*
         * Get the index partition metadata (if any). If defined, then we know
         * that this is an index partition and that the view is defined by the
         * resources named in that index partition. Otherwise the index is
         * unpartitioned.
         */
        final LocalPartitionMetadata pmd = btree.getIndexMetadata()
                .getPartitionMetadata();

        if (pmd == null) {

            // An unpartitioned index (one source).

            log.info("Unpartitioned index: name=" + name + ", ts=" + timestamp);

            return new AbstractBTree[] { btree };

        }

        /*
         * An index partition.
         */
        final AbstractBTree[] sources;
        {

            // live resources for that index partition.
            final IResourceMetadata[] a = pmd.getResources();

            assert a != null : "No resources: name="+name+", pmd="+pmd;
            
            sources = new AbstractBTree[a.length];

            // the most recent is this btree.
            sources[0/* j */] = btree;

            for (int i = 1; i < a.length; i++) {

                final IResourceMetadata resource = a[i];

                final IRawStore store = openStore(resource.getUUID());

                final AbstractBTree ndx = getIndexOnStore(name, timestamp, store);

                if (ndx == null) {

                    throw new RuntimeException(
                            "Could not load component index: name=" + name
                                    + ", timestamp=" + timestamp
                                    + ", resource=" + resource);

                }

                log.info("Added to view: " + resource);

                sources[i] = ndx;

            }

        }

        log.info("Opened index partition:  name=" + name + ", timestamp="
                + timestamp);

        return sources;

    }
    
    public String getStatistics(String name, long timestamp) {
        
        AbstractBTree[] sources = getIndexSources(name, timestamp);
        
        if(sources==null) {
            
            return null;
            
        }
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("name="+name+", timestamp="+timestamp);
        
        for(int i=0; i<sources.length; i++) {
         
            sb.append("\n"+sources[i].getStatistics());
            
        }
        
        return sb.toString();
        
    }
    
    /**
     * Note: logic duplicated by {@link Journal#getIndex(String, long)}
     */
    public IIndex getIndex(String name, long timestamp) {
        
        if (name == null) {

            throw new IllegalArgumentException();

        }

        final boolean isTransaction = timestamp > ITx.UNISOLATED;
        
        final ITx tx = (isTransaction ? getConcurrencyManager()
                .getTransactionManager().getTx(timestamp) : null); 
        
        if(isTransaction) {

            if(tx == null) {
                
                log.warn("Unknown transaction: name="+name+", tx="+timestamp);
                
                return null;
                    
            }
            
            if(!tx.isActive()) {
                
                // typically this means that the transaction has already prepared.
                log.warn("Transaction not active: name=" + name + ", tx="
                        + timestamp + ", prepared=" + tx.isPrepared()
                        + ", complete=" + tx.isComplete() + ", aborted="
                        + tx.isAborted());

                return null;
                
            }
                                
        }
        
        if( isTransaction && tx == null ) {
        
            /*
             * Note: This will happen both if you attempt to use a transaction
             * identified that has not been registered or if you attempt to use
             * a transaction manager after the transaction has been either
             * committed or aborted.
             */
            
            log.warn("No such transaction: name=" + name + ", tx=" + tx);

            return null;
            
        }
        
        final boolean readOnly = (timestamp < ITx.UNISOLATED)
                || (isTransaction && tx.isReadOnly());

        final IIndex tmp;

        if (isTransaction) {

            /*
             * Isolated operation.
             * 
             * Note: The backing index is always a historical state of the named
             * index.
             */

            final IIndex isolatedIndex = tx.getIndex(name);

            if (isolatedIndex == null) {

                log.warn("No such index: name="+name+", tx="+timestamp);
                
                return null;

            }

            tmp = isolatedIndex;

        } else {
            
            /*
             * historical read -or- unisolated read operation.
             */

            if (readOnly) {

                final AbstractBTree[] sources = getIndexSources(name, timestamp);

                if (sources == null) {

                    log.warn("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;

                }

                assert sources.length > 0;

                assert sources[0].isReadOnly();

                if (sources.length == 1) {

                    tmp = sources[0];

                } else {

                    tmp = new FusedView(sources);
                    
                }
                
            } else {
                
                /*
                 * Writable unisolated index.
                 * 
                 * Note: This is the "live" mutable index. This index is NOT
                 * thread-safe. A lock manager is used to ensure that at most
                 * one task has access to this index at a time.
                 */

                assert timestamp == ITx.UNISOLATED;
                
                final AbstractBTree[] sources = getIndexSources(name, ITx.UNISOLATED);
                
                if (sources == null) {

                    log.warn("No such index: name="+name+", timestamp="+timestamp);
                    
                    return null;
                    
                }

                assert ! sources[0].isReadOnly();
                
                if (sources.length == 1) {

                    tmp = sources[0];
                    
                } else {
                    
                    tmp = new FusedView( sources );
                    
                }

            }

        }
        
        return tmp;

    }

    /**
     * An overflow condition is recognized when the journal is within some
     * declared percentage of {@link com.bigdata.journal.Options#MAXIMUM_EXTENT}.
     * <p>
     * Once an overflow condition is recognized the {@link ResourceManager} will
     * {@link WriteExecutorService#pause()} the {@link WriteExecutorService}
     * unless it already has an <i>exclusiveLock</i>. Eventually the
     * {@link WriteExecutorService} will quiese, at which point there will be
     * another group commit and this method will be invoked again, this time
     * with an <i>exclusiveLock</i> on the {@link WriteExecutorService}.
     * <p>
     * Once this method is invoked with an <i>exclusiveLock</i> and when an
     * overflow condition is recognized it will create a new journal and
     * re-define the views for all named indices to include the pre-overflow
     * view with reads being absorbed by a new btree on the new journal.
     * 
     * @todo write unit test for an overflow edge case in which we attempt to
     *       perform an read-committed task on a pre-existing index immediately
     *       after an {@link #overflow()} and verify that a commit record exists
     *       on the new journal and that the read-committed task can read from
     *       the fused view of the new (empty) index on the new journal and the
     *       old index on the old journal.
     * 
     * @todo we can reach the {@link WriteExecutorService} from
     *       {@link #getConcurrencyManager()} so drop it from the method
     *       signature.
     */
    public boolean overflow(boolean forceOverflow, boolean exclusiveLock) {

        if (isTransient) {

            /*
             * Note: This is disabled in part because we can not close out and
             * then re-open a transient journal.
             */

            log.warn("Overflow processing not allowed for transient journals");

            return false;

        }

        if(!overflowAllowed.get()) {
            
            /*
             * Note: overflow is disabled until we are done processing the old
             * journal.
             */
            
            log.warn("Overflow processing still disabled");
            
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
            
//            final long limit = Math.max(journal.maximumExtent, journal
//                    .getBufferStrategy().getUserExtent());
            
            nextOffset = journal.getRootBlockView().getNextOffset();
            assert nextOffset == journal.getBufferStrategy().getNextOffset();

            if (nextOffset > .9 * journal.getMaximumExtent()) {

                shouldOverflow = true;

            } else {
                
                shouldOverflow = false;
                
            }

            System.err.println("testing overflow: forceOverflow="
                    + forceOverflow + ", exclusiveLock=" + exclusiveLock
                    + ", nextOffset=" + nextOffset + ", maximumExtent="
                    + journal.getMaximumExtent() + ", shouldOverflow="
                    + shouldOverflow + ", dataServiceUUID="
                    + getDataServiceUUID());
               
        }

        if(!forceOverflow && !shouldOverflow) return false;
        
        if(!exclusiveLock) {

            final WriteExecutorService writeService = concurrencyManager
                    .getWriteService();
            
            if(!writeService.isPaused()) {

                log.info("Pausing write service");
                
                writeService.pause();
                
            }
            
            return false;

        }
        
        log.info("Exclusive lock on write service");

        overflowNow();
        
        // did overflow.
        return true;

    }

    /**
     * Core method for overflow with post-processing.
     * <p>
     * Note: This method does not test pre-conditions based on the extent of the
     * journal.
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
     */
    protected Future<Object> overflowNow() {
       
        assert overflowAllowed.get();
        
        /*
         * We have an exclusive lock and the overflow conditions are satisifed.
         */
        final long lastCommitTime;
        try {

            // Do overflow processing.
            lastCommitTime = doOverflow();
            
        } finally {
            
            // Allow the write service to resume executing tasks on the new journal.
            concurrencyManager.getWriteService().resume();

            log.info("Resumed write service.");

        }
        
        // report event.
        notifyJournalOverflowEvent(getLiveJournal());

        /*
         * Start the asynchronous processing of the named indices on the old
         * journal.
         */
        if(!overflowAllowed.compareAndSet(true/*expect*/, false/*set*/)) {

            throw new AssertionError();
            
        }
        
        /*
         * Submit task on private service that will run asynchronously and clear
         * [overflowAllowed] when done.
         * 
         * Note: No one ever checks the Future returned by this method. Instead
         * the PostProcessOldJournalTask logs anything that it throws in its
         * call() method.
         */

        Counters totalCounters = ((ConcurrencyManager)concurrencyManager).getTotalCounters();

        Map<String/*name*/,Counters> indexCounters = ((ConcurrencyManager)concurrencyManager).resetCounters();
        
        return service.submit(new PostProcessOldJournalTask(this, lastCommitTime, totalCounters, indexCounters));

    }
    
    /**
     * Performs the actual overflow handling once all pre-conditions have been
     * satisified and uses {@link #purgeOldResources()} to delete old resources
     * from the local file system that are no longer required as determined by
     * {@link #setReleaseTime(long)} and {@link #getEffectiveReleaseTime()}.
     * <p>
     * Note: This method does NOT start a {@link PostProcessOldJournalTask}.
     * <P>
     * Note: You MUST have an exclusive lock on the {@link WriteExecutorService}
     * before you invoke this method!
     * 
     * @return The lastCommitTime of the old journal.
     * 
     * @todo closing out and re-opening the old journal as read-only is going to
     *       discard some buffering that we might prefer to keep on hand during
     *       the {@link PostProcessOldJournalTask}.
     * 
     * @todo If a very large #of index partitions are hosted on the same journal
     *       then they should be re-distributed regardless of their size.
     * 
     * @todo Work out high-level alerting for resource exhaustion and failure to
     *       maintain QOS on individual machines, indices, and across the
     *       federation. Consider resource limits on indices.
     */
    protected long doOverflow() {
        
        log.info("begin");
        
        /*
         * Note: We assign the same timestamp to the createTime of the new
         * journal and the closeTime of the old journal.
         */
        final long createTime = nextTimestamp();
        final long closeTime = createTime;
        
        /*
         * Create the new journal.
         * 
         * @todo this is not using the temp filename mechanism in a manner that
         * truely guarentees an atomic file create. The CREATE_TEMP_FILE option
         * should probably be extended with a CREATE_DIR option that allows you
         * to override the directory in which the journal is created. That will
         * allow the atomic creation of the journal in the desired directory
         * without changing the existing semantics for CREATE_TEMP_FILE.
         */
        final AbstractJournal newJournal;
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
            p.setProperty(Options.CREATE_TIME, ""+createTime);
            
            newJournal = new ManagedJournal(p);
            
            assert createTime == newJournal.getRootBlockView().getCreateTime();

        }

        /*
         * Overflow each index by re-defining its view on the new journal.
         */
        int noverflow = 0;
        final AbstractJournal oldJournal = getLiveJournal();
        {

            int nindices = (int) oldJournal.getName2Addr().rangeCount(null,null);

            ITupleIterator itr = oldJournal.getName2Addr().rangeIterator(null,null);

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final Entry entry = EntrySerializer.INSTANCE
                        .deserialize(new DataInputBuffer(tuple.getValue()));

                // old index (just the mutable btree on the old journal, not the full view of that index).
                final BTree oldBTree = (BTree) oldJournal.getIndex(entry.addr);

                // #of index entries on the old index.
                final int entryCount = oldBTree.getEntryCount();
                
                // clone index metadata.
                final IndexMetadata indexMetadata = oldBTree.getIndexMetadata()
                        .clone();

                // old partition metadata.
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
                     */

                    throw new RuntimeException("Not a partitioned index: "
                            + entry.name);
                     
                    
                }
                
                /*
                 * When true, a new index segment will be build from the view
                 * defined by the old index on the old journal during
                 * post-overflow processing. Otherwise we will copy the data
                 * from the old index into the new index
                 */
                final boolean willBuildIndexSegment = copyIndexThreshold == 0
                        || entryCount >= copyIndexThreshold;

                /*
                 * We will only create a new empty index, taking care to
                 * propagate the index local counter.
                 * 
                 * Update the partition metadata so that the new index reflects
                 * its location on the new journal.
                 * 
                 * Note: The old index on the old journal is retained as part of
                 * the view. We need it until the new index segment is ready. It
                 * is also required for historical reads on the new journal
                 * between its firstCommitTime and the commit point the index
                 * partition view is updated to include the newly built index
                 * segment.
                 */
                if(willBuildIndexSegment) {
                    
                    final IResourceMetadata[] oldResources = oldpmd
                            .getResources();

                    final IResourceMetadata[] newResources = new IResourceMetadata[oldResources.length + 1];

                    System.arraycopy(oldResources, 0, newResources, 1,
                            oldResources.length);

                    // new resource is listed first (reverse chronological
                    // order)
                    newResources[0] = newJournal.getResourceMetadata();

                    // describe the index partition.
                    indexMetadata
                            .setPartitionMetadata(new LocalPartitionMetadata(
                                    oldpmd.getPartitionId(),//
                                    oldpmd.getLeftSeparatorKey(),//
                                    oldpmd.getRightSeparatorKey(),//
                                    newResources //
                            ));
                    
                } else {
                    
                    /*
                     * When false we will copy the index data from the B+Tree
                     * old journal (but not from the full index view) onto the
                     * new journal. In this case the index will use a view that
                     * DOES NOT include the old index on the old journal.
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
                                    newResources //
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

                    // Propagate the counter to the new B+Tree.
                    final long oldCounter = oldBTree.getCounter().get();

                    log.info("Propagating counter=" + oldCounter + " for "
                            + entry.name);

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

                    assert newBTree.getCounter().get() == oldCounter;
                    
                    if(!willBuildIndexSegment) {
                        
                        /*
                         * Copy the data from the B+Tree on the old journal into
                         * the B+Tree on the new journal.
                         */
                        
                        log.info("Copying data to new journal: name=" + entry.name
                                + ", entryCount=" + entryCount + ", threshold="
                                + copyIndexThreshold);
                        
                        newBTree.rangeCopy(oldBTree, null, null);
                     
                    }
                    
                    /*
                     * Register the new B+Tree on the new journal.
                     */
                    newJournal.registerIndex(entry.name, newBTree);

                }

                log.info("Did overflow: " + noverflow + " of " + nindices
                        + " : " + entry.name);

                noverflow++;

            }

            log.info("Did overflow of " + noverflow + " indices");

            assert nindices == noverflow;

            // make the index declarations restart safe on the new journal.
            newJournal.commit();

        }

        /*
         * Close out the old journal.
         */
        final long lastCommitTime = oldJournal.getRootBlockView().getLastCommitTime();
        {
            
            // writes no longer accepted.
            oldJournal.close(closeTime);
            
            // remove from list of open journals.
            if (this.openStores.remove(oldJournal.getRootBlockView().getUUID()) != oldJournal) {
                
                throw new AssertionError();
                
            }

            log.info("Closed out the old journal.");
            
        }
        
        /*
         * Cut over to the new journal.
         */
        {

            this.liveJournal = newJournal;

            addResource(newJournal.getResourceMetadata(), newJournal.getFile());
            
            if (this.openStores.put(newJournal.getRootBlockView().getUUID(),
                    newJournal) != null) {

                throw new AssertionError();

            }

            log.info("Changed over to a new live journal");

        }
        
        /*
         * Cause old resources to be deleted on the file system.
         */
        purgeOldResources();
        
        log.info("end");
        
        return lastCommitTime;

    }
    
    public void destroyAllResources() {
        
        if (open)
            throw new IllegalStateException();

        // NOP if transient.
        if(isTransient) return; 
        
        log.warn("Deleting all resources: " + dataDir);
        
        recursiveDelete(dataDir);
        
    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(File f) {
        
        if(f.isDirectory()) {
            
            File[] children = f.listFiles(newFileFilter());
            
            for(int i=0; i<children.length; i++) {
                
                recursiveDelete( children[i] );
                
            }
            
            log.info("Removing: "+f);
            
            if(!f.delete()) {
                
                log.warn("Could not remove: "+f);
                
            }
            
        }
            
        log.info("Removing: " + f);

        if (!f.delete()) {

            log.warn("Could not remove: " + f);

        }
        
    }

    /**
     * Returns a filter that is used to recognize files that are managed by this
     * class. The {@link ResourceManager} will log warnings if it sees an
     * unexpected file and will NOT {@link #destroyAllResources()} files that it does not
     * recognize.
     * 
     * @see ResourceFileFilter
     * 
     * @todo perhaps define setFileFilter and getFileFilter instead since
     *       subclassing this method is a bit difficult. The
     *       {@link ResourceFileFilter} would have to be a static class and we
     *       would have to pass in the {@link IResourceManager} so that it could
     *       get the {@link #dataDir}.
     */
    protected ResourceFileFilter newFileFilter() {

        return new ResourceFileFilter(this);
        
    }
    
    /**
     * Updates the {@link #releaseTime}.
     * 
     * @see #purgeOldResources(), which is responsible for actually deleting the
     * old resources.
     */
    public void setReleaseTime(long releaseTime) {

        log.info("Updating the releaseTime: old="+this.releaseTime+", new="+this.releaseTime);

        this.releaseTime = releaseTime;
        
    }

    /**
     * Delete resources having no data for this release time.
     * <p>
     * Note: The ability to read from a historical commit point requires the
     * existence of the journals back until the one covering that historical
     * commit point. This is because the distinct historical commit points for
     * the indices are ONLY defined on the journals. The index segments carry
     * forward the commit state of a specific index as of the commitTime of the
     * index from which the segment was built. This means that you can
     * substitute the index segment for the historical index state on older
     * journals, but the index segment carries forward only a single commit
     * state for the index so it can not be used to read from arbitrary
     * historical commit points.
     * <p>
     * Note: The caller MUST arrange for synchronization. Typically this is
     * invoked during {@link #doOverflow()}.
     * 
     * FIXME write tests (a) when immortal store; (b) when minReleaseAge is
     * non-zero. For (b) test when {@link #getEffectiveReleaseTime()} yeilds a
     * value which causes 1, 2, or 3 old journals to be retained.
     */
    protected void purgeOldResources() {

        final long t = getEffectiveReleaseTime();
        
        if(t == 0L) {
            
            log.warn("Immortal database - resources will NOT be released");
            
        }
        
        log.info("Effective release time: " + t + ", currentTime="
                + nextTimestamp());

        /*
         * Delete old resources.
         * 
         * The basic steps are:
         * 
         * a) close iff open (#openStores)
         * 
         * b) remove from lists of known resources (resourceFiles).
         * 
         * c) delete in the file system
         * 
         * @todo what should be logged as a warning and what should throw an
         * exception? Can the operations be re-ordered such that we only update
         * the transient data structures once we have successfully delete the
         * local resource so that we will re-try the next time if we fail this
         * time?
         */
        
        /*
         * Delete old journals.
         * 
         * Journals are indexed by createTime, so we can just scan that index to
         * identify the journals to be released.
         */
        {
        
            /*
             * Note: used to bulk delete the entries for deleted journals from
             * the journalIndex since the iterator does not support removal.
             */
            final Set<byte[]> keys = new TreeSet<byte[]>(UnsignedByteArrayComparator.INSTANCE);
            
            final ITupleIterator itr = journalIndex.entryIterator();
        
            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final IResourceMetadata resourceMetadata = (IResourceMetadata) SerializerUtil
                        .deserialize(tuple.getValue());

                if (resourceMetadata.getCreateTime() > t) {
                    
                    log.info("No more journals of sufficient age to warrant deletion");
                    
                    break;
                    
                }

                log.info("Will delete old journal: createTime=" + t
                        + ", resource=" + resourceMetadata);

                final UUID uuid = resourceMetadata.getUUID();
                
                // close out store iff open.
                {
                    
                    final IRawStore store = openStores.remove(uuid);

                    assert store instanceof AbstractJournal;

                    // can't close out the live journal!
                    assert uuid != getLiveJournal().getRootBlockView().getUUID();

                    if (store != null) {

                        store.close();

                    }
                    
                }

                // delete the backing file.
                {

                    final File file = resourceFiles.remove(uuid);

                    if (file == null) {
                     
                        throw new RuntimeException("No file for resource? uuid=" + uuid);
                        
                    } else {
                        
                        if (file.exists()) {

                            throw new RuntimeException("Not found: " + file);

                        } else if (!file.delete()) {

                            throw new RuntimeException("Could not delete: " + file);

                        }
                        
                    }

                }
                
            }

            // remove entries from the journalIndex.
            for( byte[] key : keys ) {
                
                if(journalIndex.remove(key)==null) {
                    
                    throw new AssertionError();
                    
                }
                
            }
            
        }
        
        /*
         * Delete old index segments.
         * 
         * Scan the index over {createTime, segmentUUID} to identify the index
         * segments to be released.
         */
        {
        
            /*
             * Note: used to bulk delete the entries for deleted journals from
             * the journalIndex since the iterator does not support removal.
             */
            final Set<byte[]> keys = new TreeSet<byte[]>(UnsignedByteArrayComparator.INSTANCE);
            
            final ITupleIterator itr = indexSegmentIndex.entryIterator();
        
            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final IResourceMetadata resourceMetadata = (IResourceMetadata) SerializerUtil
                        .deserialize(tuple.getValue());

                if (resourceMetadata.getCreateTime() > t) {
                    
                    log.info("No more index segments of sufficient age to warrant deletion");
                    
                    break;
                    
                }

                log.info("Will delete old index segment: createTime=" + t
                        + ", resource=" + resourceMetadata);

                final UUID uuid = resourceMetadata.getUUID();

                // can't close out the live journal!
                assert uuid != getLiveJournal().getRootBlockView().getUUID();
                
                // close out store iff open.
                {
                    
                    final IRawStore store = openStores.remove(uuid);

                    assert store instanceof IndexSegmentFileStore;
                    
                    if (store != null) {

                        store.close();

                    }
                    
                }

                // delete the backing file.
                {

                    final File file = resourceFiles.remove(uuid);

                    if (file == null) {
                     
                        throw new RuntimeException("No file for resource? uuid=" + uuid);
                        
                    } else {
                        
                        if (file.exists()) {

                            throw new RuntimeException("Not found: " + file);

                        } else if (!file.delete()) {

                            throw new RuntimeException("Could not delete: " + file);

                        }
                        
                    }

                }
                
            }

            // remove entries from the journalIndex.
            for( byte[] key : keys ) {
                
                if(indexSegmentIndex.remove(key)==null) {
                    
                    throw new AssertionError();
                    
                }
                
            }
            
        }
        
    }

    /**
     * The effective release time is the minimum of (a) the current time minus
     * the {@link #minReleaseAge} and (b) the
     * {@link #getEarliestDependencyTimestamp(long)}. Resources whose
     * createTime is LTE this timestamp MAY be deleted.
     * 
     * @return The effective release time -or- <code>0L</code> iff the
     *         {@link #minReleaseAge} is zero (0L), which indicates an immortal
     *         database.
     */
    protected long getEffectiveReleaseTime() {

        if(minReleaseAge==0L) {
            
            log.info("Immortal database - resoureces will NOT be released");

            return 0L;
            
        }
        
        final long t1 = nextTimestamp() - minReleaseAge;

        final long t;
        if (releaseTime == 0L) {

            log.warn("Release time has not been set.");
            
            t = t1;
            
        } else {
            
            final long t2 = getEarliestDependencyTimestamp(releaseTime);

            t = Math.min(t1, t2);
            
        }

        return t;
        
    }
    
    /**
     * Finds the journal covering the specified timestamp, lookups up the commit
     * record for that timestamp, and then scans the named indices for that
     * commit record returning minimum of the createTime for resources that are
     * part of the definition of each named index as of that commit record. This
     * is the timestamp of the earliest resource on which that timestamp has a
     * dependency.
     * 
     * @param releaseTime
     *            A release time as set by {@link #setReleaseTime(long)}.
     * 
     * @return Releasing resources as of the returned timestamp is always safe
     *         since all named index view as of the given <i>timestamp</i> will
     *         be preserved.
     *         <p>
     *         There MAY be additional resources which COULD be released if you
     *         used more information to make the decision, but a decision made
     *         on the basis of the returned value will always be safe.
     */
    protected long getEarliestDependencyTimestamp(long releaseTime) {
        
        return 0L;
        
    }
    
    /**
     * @todo munge the index name so that we can support unicode index names in
     *       the filesystem.
     * 
     * @todo should the filename be relative or absolute?
     */
    public File getIndexSegmentFile(IndexMetadata indexMetadata) {

        // munge index name to fit the file system.
        final String mungedName = indexMetadata.getName();

        // subdirectory using the scale-out indices unique index UUID.
        final File indexDir = new File(segmentsDir, indexMetadata
                .getIndexUUID().toString());

        // make sure that directory exists.
        indexDir.mkdirs();
        
        final IPartitionMetadata pmd = indexMetadata.getPartitionMetadata();
        
        final String partitionStr = (pmd == null ? "" : "_part"
                + leadingZeros.format(pmd.getPartitionId()));

        final String prefix = mungedName + "" + partitionStr + "_";

        final File file;
        try {

            file = File.createTempFile(prefix, Options.SEG, indexDir);

        } catch (IOException e) {

            throw new RuntimeException(e);

        }

        log.info("Created file: " + file);

        return file;

    }

    /**
     * Notify the resource manager of a new resource. The resource is added to
     * {@link #resourceFiles} and to either {@link #journalIndex} or
     * {@link #indexSegmentIndex} as appropriate.
     * 
     * @param resourceMetadata
     *            The metadata describing that resource.
     * @param file
     *            The file in the local file system which is the resource.
     * 
     * @throws RuntimeException
     *             if the file does not exist.
     * @throws RuntimeException
     *             if there is already a resource registered with the same UUID
     *             as reported by {@link IResourceMetadata#getUUID()}
     * @throws RuntimeException
     *             if the {@link #journalIndex} or {@link #indexSegmentIndex}
     *             already know about that resource.
     * @throws RuntimeException
     *             if {@link #openStore(UUID)} already knows about that
     *             resource.
     * @throws IllegalArgumentException
     *             if the <i>resourceMetadata</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>file</i> is <code>null</code> and {@link #isTransient} is
     *             <code>false</code>.
     */
    synchronized protected void addResource(IResourceMetadata resourceMetadata, File file) {

        if (resourceMetadata == null)
            throw new IllegalArgumentException();

        if (file == null && !isTransient)
            throw new IllegalArgumentException();
        
        final UUID uuid = resourceMetadata.getUUID();

        if(openStores.containsKey(uuid)) {
            
            throw new RuntimeException("Resource already open?: "+resourceMetadata);
            
        }
        
        if(!isTransient) {

            if( ! file.exists()) {
                
                throw new RuntimeException("File not found: "+file);
                
            }
            
            // check for existing entry under that UUID.

            final File tmp = resourceFiles.get(uuid);

            if (tmp != null) {

                throw new RuntimeException("Resource already registered: uuid="
                        + uuid + " as file=" + tmp + " (given file=" + file
                        + ")");

            }
            
            // add new entry.
            resourceFiles.put(uuid, file);

        }
        
        if(resourceMetadata.isJournal()) {
            
            journalIndex.add(resourceMetadata);
            
        } else {
            
            indexSegmentIndex.add(resourceMetadata);
            
        }
        
    }
    
    /**
     * Implementation designed to use a shared {@link ConcurrencyManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ManagedJournal extends AbstractJournal {

        public ManagedJournal(Properties properties) {
            
            super(properties);
            
        }

        public long nextTimestamp() {
            
            return ResourceManager.this.nextTimestamp();
            
        }
        
        @Override
        public long commit() {

            return commitNow(nextTimestamp());
            
        }
               
    }
    
    /**
     * Build an index segment from an index partition.
     * 
     * @param name
     *            The name of the index partition (not the name of the scale-out
     *            index).
     * @param src
     *            A view of the index partition as of the <i>createTime</i>.
     * @parma outFile The file on which the {@link IndexSegment} will be
     *        written.
     * @param createTime
     *            The timestamp of the view. This is typically the
     *            lastCommitTime of the old journal after an
     *            {@link #overflow(boolean, boolean)} operation.
     * @param fromKey
     *            The lowest key that will be counted (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will not be counted (exclusive). When
     *            <code>null</code> there is no upper bound.
     * 
     * @return A {@link BuildResult} identifying the new {@link IndexSegment}
     *         and the source index.
     * 
     * @throws IOException
     */
    public BuildResult buildIndexSegment(String name, IIndex src, File outFile,
            long createTime, byte[] fromKey, byte[] toKey) throws IOException {

        if (name == null)
            throw new IllegalArgumentException();
        
        if (src == null)
            throw new IllegalArgumentException();
        
        if (outFile == null)
            throw new IllegalArgumentException();

        if (createTime <= 0L)
            throw new IllegalArgumentException();
        
        // metadata for that index / index partition.
        final IndexMetadata indexMetadata = src.getIndexMetadata();

        // the branching factor for the generated index segment.
        final int branchingFactor = indexMetadata
                .getIndexSegmentBranchingFactor();

//         // Note: truncates nentries to int.
//        final int nentries = (int) Math.min(src.rangeCount(fromKey, toKey),
//                Integer.MAX_VALUE);

        /*
         * Use the range iterator to get an exact entry count for the view.
         * 
         * Note: We need the exact entry count for the IndexSegmentBuilder. It
         * requires the exact #of index entries when it creates its plan for
         * populating the index segment.
         */
        final int nentries;
        {

            final ITupleIterator itr = src
                    .rangeIterator(fromKey, toKey, 0/* capacity */,
                            0/*no flags*/, null/* filter */);

            int i = 0;

            while(itr.hasNext()) {
                
                itr.next();
                
                i++;
                
            }
            
            nentries = i;
            
            log.info("There are "+nentries+" non-deleted index entries: "+name);
            
        }
        
         /*
          * Note: Delete markers are ignored so they will NOT be transferred to
          * the new index segment (compacting merge).
          */
         final ITupleIterator itr = src
                 .rangeIterator(fromKey, toKey, 0/* capacity */,
                         IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);
         
         // Build index segment.
         final IndexSegmentBuilder builder = new IndexSegmentBuilder(//
                 outFile, //
                 getTmpDir(), //
                 nentries,//
                 itr, //
                 branchingFactor,//
                 indexMetadata,//
                 createTime//
         );

         // report event
         notifyIndexSegmentBuildEvent(builder);

         /*
          * Describe the index segment.
          */
         final SegmentMetadata segmentMetadata = new SegmentMetadata(//
                 "" + outFile, //
                 outFile.length(),//
                 builder.segmentUUID,//
                 createTime//
                 );

         /*
          * notify the resource manager so that it can find this file.
          * 
          * @todo once the index segment has been built the resource manager
          * should notice it in a restart manner and put it into play if it
          * has not already been used to update the view.
          */

         addResource(segmentMetadata, outFile);

         return new BuildResult(name, indexMetadata, segmentMetadata);
         
     }
     
    public String getStatistics() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("ResourceManager: dataService="+getDataServiceUUID()+", dataDir="+dataDir);
        
        return sb.toString();
        
    }
    
    public IDataService getDataService(UUID serviceUUID) {
        
        try {

            return getMetadataService().getDataService(serviceUUID);
            
        } catch (IOException e) {
            
            throw new RuntimeException(e);
            
        }
        
    }
    
}
