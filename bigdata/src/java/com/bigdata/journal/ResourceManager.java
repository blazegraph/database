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

package com.bigdata.journal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.AbstractPartitionTask;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocatorMetadataWithSeparatorKeys;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedBigdataFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.MetadataService;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;
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
 * FIXME A separate process must examine the #of index partitions per data
 * service and move those partitions to under utilized data services in the same
 * zone in order to load balance the zone. Without this we can't move index
 * partitions off of their original data service.
 * 
 * @todo The resource manager needs to track the invalidated (aka deleted when
 *       an index partition was split or merged) and moved index partitions on
 *       an LRU and refuse attempts to access that index with a special
 *       exception which clients can interpret as requiring a refresh of the key
 *       range of the old index partition from the metadata index. This will
 *       also support moving an index to another data service. Eventually
 *       entries will drop off of the LRU and clients will get an index not
 *       found error rather than an index partition invalidated error or an
 *       index partition moved error.
 * 
 * @todo track the disk space used by the {@link #getDataDir()} and the free
 *       space remaining on the mount point that hosts the data directory. if we
 *       approach the limit on the space in use then we need to shed index
 *       partitions to other data services or potentially become more aggressive
 *       in releasing old resources.
 * 
 * @todo maintain "read locks" for resources and a latency queue. use this to
 *       close down resources and to delete old resources according to a
 *       resource release policy (configurable). coordinate read locks with the
 *       transaction manager and the concurrency manager.
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
 * @todo review use of synchronization and make sure that there is no way in
 *       which we can double-open a store or index.
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
                    + journal.maximumExtent);

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
     * Set <code>true</code> by {@link #start()} and remains <code>true</code>
     * until the {@link ResourceManager} is shutdown.
     */
    private boolean open;

    /**
     * <code>true</code> iff {@link BufferMode#Transient} was indicated.
     */
    private final boolean isTransient;
    
    /**
     * Set based on {@link Options#INDEX_SEGMENT_BUILD_THRESHOLD}.
     */
    private final int indexSegmentBuildThreshold;
    
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
    private ExecutorService service;

    /**
     * A flag used to disable overflow of the live journal until asynchronous
     * post-processing of the old journal has been completed.
     * 
     * @see PostProcessOldJournalTask
     */
    private final AtomicBoolean overflowAllowed = new AtomicBoolean(true);

    /**
     * <code>true</code> unless an overflow event is currently being
     * processed.
     */
    public boolean isOverflowAllowed() {
        
        return overflowAllowed.get();
        
    }
    
//    /**
//     * A non-restart safe cache of recently invalidated index partitions. In
//     * some cases the index partition has simply been moved to another data
//     * service. In other cases the index partition has been split and requests
//     * for the old index partition must be replaced by requests for one or all
//     * of the new index partitions arising from that split.
//     */
//    private LRUCache<byte[]/* key */, PartitionLocatorMetadata/* val */> invalidatedPartitionCache = new LRUCache<byte[], PartitionLocatorMetadata>(
//            100);
//
//    protected void invalidateIndexPartition(String name, int partitionId, Object response) {
//        
//    }
    
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
         * The minimum #of index entries before an {@link IndexSegment} will be
         * built from an index during overflow processing (default is
         * <code>5000</code>).
         * 
         * @see #DEFAULT_INDEX_SEGMENT_BUILD_THRESHOLD
         */
        public static final String INDEX_SEGMENT_BUILD_THRESHOLD = "indexSegment.buildThreshold";

        public static final String DEFAULT_INDEX_SEGMENT_BUILD_THRESHOLD = "5000";
        
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

        {

            indexSegmentBuildThreshold = Integer.parseInt(properties
                    .getProperty(Options.INDEX_SEGMENT_BUILD_THRESHOLD,
                            Options.DEFAULT_INDEX_SEGMENT_BUILD_THRESHOLD));

            log.info(Options.INDEX_SEGMENT_BUILD_THRESHOLD + "="
                    + indexSegmentBuildThreshold);

            if (indexSegmentBuildThreshold < 1) {

                throw new RuntimeException(
                        Options.INDEX_SEGMENT_BUILD_THRESHOLD
                                + " must be positive");

            }
            
        }
        
        /*
         * Create the _transient_ index in which we will store the mapping from
         * the commit times of the journals to their resource descriptions.
         */
        journalIndex = JournalIndex.create(new SimpleMemoryRawStore());

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

                log.warn("Creating: " + dataDir);

                if (!dataDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + dataDir.getAbsolutePath());

                }

            }

            if (!journalsDir.exists()) {

                log.warn("Creating: "+journalsDir);

                if (!journalsDir.mkdir()) {

                    throw new RuntimeException("Could not create directory: "
                            + journalsDir.getAbsolutePath());

                }

            }

            if (!segmentsDir.exists()) {

                log.warn("Creating: "+segmentsDir);
                
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

                log.warn("Creating temp directory: " + tmpDir);

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

                log.warn("Creating initial journal");

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

                log.warn("Opening most recent journal: " + file + ", resource="
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

                // add to index since not found on file scan.
                journalIndex.add(liveJournal.getResourceMetadata());

                // add to set of local resources that we know about.
                if (this.resourceFiles.put(liveJournal.getRootBlockView()
                        .getUUID(), liveJournal.getFile()) != null) {

                    throw new AssertionError();

                }   
                
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

            // add to ordered set of journals by timestamp.
            journalIndex.add(resource);

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

        // locate resource files by resource description.
        resourceFiles.put(resource.getUUID(), file.getAbsoluteFile());

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
        
    }

    public void shutdownNow() {

        assertOpen();

        service.shutdownNow();

        closeStores();
        
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

                    log.warn("Resource has no data for timestamp: timestamp="
                            + ts + ", resource=" + store.getResourceMetadata());

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

            // use absolute value in case timestamp is negative.
            final long ts = Math.abs(timestamp);

            final IndexSegmentFileStore segStore = ((IndexSegmentFileStore) store);

            if (segStore.getCheckpoint().commitTime < ts) {

                log.warn("Resource has no data for timestamp: timestamp=" + ts
                        + ", store=" + store);
                
                return null;

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
    public boolean overflow(boolean exclusiveLock,
            WriteExecutorService writeService) {

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

            if (nextOffset > .9 * journal.maximumExtent) {

                shouldOverflow = true;

            } else {
                
                shouldOverflow = false;
                
            }

            System.err.println("testing overflow: exclusiveLock="
                    + exclusiveLock + ", nextOffset=" + nextOffset
                    + ", maximumExtent=" + journal.maximumExtent
                    + ", willOverflow=" + shouldOverflow + ", dataServiceUUID="
                    + getDataServiceUUID());
               
        }

        if(!shouldOverflow) return false;
        
        if(!exclusiveLock) {

            if(!writeService.isPaused()) {

                log.info("Pausing write service");
                
                writeService.pause();
                
            }
            
            return false;

        }
        
        log.info("Exclusive lock on write service");

        /*
         * We have an exclusive lock and the overflow conditions are satisifed.
         */
        final long lastCommitTime;
        try {

            // Do overflow processing.
            lastCommitTime = doOverflow();
            
        } finally {
            
            // Allow the write service to resume executing tasks on the new journal.
            writeService.resume();

            log.info("Resumed write service.");

        }
        
        // report event.
        notifyJournalOverflowEvent(journal);

        /*
         * Start the asynchronous processing of the named indices on the old
         * journal.
         */
        if(!overflowAllowed.compareAndSet(true, false)) {

            throw new AssertionError();
            
        }
        
        /*
         * Submit task on private service that will run asynchronously and clear
         * [overflowAllowed] when done.
         */
        service.submit(new PostProcessOldJournalTask(lastCommitTime));

        // did overflow.
        return true;

    }

    /**
     * Performs the actual overflow handling once all pre-conditions have been
     * satisified.
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
     *       federation.  Consider resource limits on indices.
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

            int nindices = oldJournal.name2Addr.getEntryCount();

            IEntryIterator itr = oldJournal.name2Addr.entryIterator();

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
                 * post-overflow processing.  Otherwise we will copy the data
                 * from the old index into the new index
                 */
                final boolean willBuildIndexSegment = entryCount >= indexSegmentBuildThreshold;

                /*
                 * We will only create a new empty index, taking care to
                 * propagate the index local counter.
                 * 
                 * Update the partition metadata so that the new index reflects
                 * its location on the new journal.
                 * 
                 * Note: The old index on the old journal is retained as part of
                 * the view.
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
                        
                        log.warn("Copying data to new journal: name=" + entry.name
                                + ", entryCount=" + entryCount + ", threshold="
                                + indexSegmentBuildThreshold);
                        
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

            this.journalIndex.add(newJournal.getResourceMetadata());

            if (this.resourceFiles.put(newJournal.getRootBlockView().getUUID(),
                    newJournal.getFile()) != null) {

                throw new AssertionError();
                
            }   
            
            if (this.openStores.put(newJournal.getRootBlockView().getUUID(),
                    newJournal) != null) {

                throw new AssertionError();

            }

            log.info("Changed over to a new live journal");

        }
        
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
            
            log.warn("Removing: "+f);
            
            if(!f.delete()) {
                
                log.error("Could not remove: "+f);
                
            }
            
        }
            
        log.warn("Removing: " + f);

        if (!f.delete()) {

            log.error("Could not remove: " + f);

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

        return new ResourceFileFilter();
        
    }
    
    /**
     * The default implementation accepts directories under the configured
     * {@link IResourceManager#getDataDir()} and files with either
     * {@link com.bigdata.journal.Options#JNL} or
     * {@link com.bigdata.journal.Options#SEG} file extensions.
     * <p> *
     * <P>
     * If you define additional files that are stored within the
     * {@link ResourceManager#getDataDir()} then you SHOULD subclass
     * {@link ResourceFileFilter} to recognize those files and override
     * {@link ResourceManager#newFileFilter()} method to return your
     * {@link ResourceFileFilter} subclass.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ResourceFileFilter implements FileFilter {

        /**
         * Override this method to extend the filter.
         * 
         * @param f
         *            A file passed to {@link #accept(File)}
         *            
         * @return <code>true</code> iff the file should be accepted by the
         *         filter.
         */
        protected boolean accept2(File f) {
            
            return false;
            
        }
        
        final public boolean accept(File f) {

            if (f.isDirectory()) {

//                // Either f iff it is a directory or the directory containing f.
//                final File dir = f.isDirectory() ? f : f.getParentFile();
                
                final File dir = f;

                // get the canonical form of the directory.
                final String fc;
                try {

                    fc = dir.getCanonicalPath();

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

                if (!fc.startsWith(getDataDir().getPath())) {

                    throw new RuntimeException("File not in data directory: file="
                            + f + ", dataDir=" + dataDir);

                }

                // directory inside of the data directory.
                return true;

            }
            
            if (f.getName().endsWith(Options.JNL)) {

                // journal file.
                return true;
                
            }

            if (f.getName().endsWith(Options.SEG)) {

                // index segment file.
                return true;
                
            }

            if (accept2(f)) {
                
                // accepted by subclass.
                return true;
                
            }
            
            log.warn("Unknown file: " + f);

            return false;

        }

    }
    
    /**
     * @todo release point is min(earliestTx, minReleaseAge, createTime of the
     *       oldest journal still required by an index view).
     *       <p>
     *       The earlestTx is effectivetly how we do "read locks" for
     *       transactions. The transaction manager periodically notifies the
     *       resource manager of the earliest running tx. That timestamp will
     *       naturally grow as transactions complete, thereby releasing their
     *       "read locks" on resources.
     *       <p>
     *       The minReleaseAge is just how long you want to hold onto an
     *       immortal database view. E.g., 3 days of full history. There should
     *       be a setting, e.g., zero (0L), for a true immortal database.
     *       <p>
     *       The last value is the createTime on the journal whose indices are
     *       still being merged onto index segments asynchronously. That time
     *       gets updated every time we finish processing the indices for a
     *       given historical journal.
     */
    public void releaseOldResources(long timestamp) {
        // TODO Auto-generated method stub

        throw new UnsupportedOperationException();
        
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

        log.warn("Created file: " + file);

        return file;

    }

    // @todo make sure not pre-existing and file in dataDir.
    public void addResource(UUID uuid, File file) {

        if (resourceFiles.put(uuid, file) != null) {

            // should not already there.
            
            throw new AssertionError();
            
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
     * After an {@link IResourceManager#overflow(boolean, WriteExecutorService)}
     * we post-process the named indices defined on the old journal as of the
     * lastCommitTime of the old journal. For each named index we will either
     * build an index segment containing the compact view of that named index or
     * split the index into N index partitions. Either as each named index is
     * processed or once all named indices have been processed, we atomically
     * update the view definitions for those named indices to reflect the new
     * index segments and/or index partitions. At this point we can allow the
     * live journal to overflow again since we are caught up on the old journal.
     * <p>
     * Note: this task runs on the {@link ResourceManager#service}. In turn, it
     * submits tasks to the {@link IConcurrencyManager}.
     * <p>
     * Note: After this task completes clients MAY have to re-submit requests
     * which targetted the old index partition (they must test for the root
     * cause exception and re-submit the request to the appropriate new index
     * partitions rather than failing the total operation).
     * 
     * @todo The old journal may still be required (and actively in use) by
     *       historical reads after this task has been completed so we can only
     *       reduce a reference counter, not close it out alltogether.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class PostProcessOldJournalTask implements Callable<Object> {

        private final long lastCommitTime;
        
        /**
         * A queue of results that are ready to be integrated into the current
         * view of index partitions.
         * 
         * @todo We could directly submit tasks to the
         *       {@link IConcurrencyManager} rather than building them up on
         *       this queue. However, it seems that it might be better to have
         *       the views cut over all-at-once from the old view definitions to
         *       the new view definitions (including on the metadata index), in
         *       which case gathering up the results first will be necessary. It
         *       also might give us somewhat better error reporting. On the
         *       other hand, performance should improve as soon as we put the
         *       new view definitions into play, so there is a good reason to do
         *       that eagerly.
         */
        private final BlockingQueue<AbstractResult> resultQueue = new LinkedBlockingQueue<AbstractResult>(/*unbounded*/);
        
        /**
         * 
         * @param lastCommitTime The lastCommitTime on the old journal.
         */
        public PostProcessOldJournalTask(long lastCommitTime) {
            
            assert lastCommitTime > 0L;
            
            this.lastCommitTime = lastCommitTime;

        }
        
        /**
         * Process each named index on the old journal. In each case we will
         * either build a new {@link IndexSegment} for the index partition or
         * split the index partition into N index partitions.
         */
        protected void processNamedIndices() throws Exception {

            log.info("begin: lastCommitTime="+lastCommitTime);
            
            int ndone = 0;
            int nskip = 0;
            
            // the old journal.
            final AbstractJournal oldJournal = getJournal(lastCommitTime);
            
            final int nindices = oldJournal.name2Addr.getEntryCount();
            
            final List<AbstractTask> tasks = new ArrayList<AbstractTask>(
                    nindices);

            final IEntryIterator itr = oldJournal.name2Addr.entryIterator();

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
                    
                    final boolean willBuildIndexSegment = entryCount >= indexSegmentBuildThreshold;
                    
                    if (!willBuildIndexSegment) {

                        log.warn("Will not build index segment: name=" + name
                                + ", entryCount=" + entryCount + ", threshold="
                                + indexSegmentBuildThreshold);
                        
                        nskip++;
                        
                        continue;
                        
                    }
                    
                }
                
                // historical view of that index at that time.
                final IIndex view = getIndexOnStore(name, lastCommitTime, oldJournal );

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
                    task = new SplitIndexPartitionTask(getConcurrencyManager(),
                            lastCommitTime, name);

                } else {

                    /*
                     * Just do an index build task.
                     */
                    
                    
                    // the file to be generated.
                    final File outFile = getIndexSegmentFile(indexMetadata);

                    task = new BuildIndexSegmentTask(getConcurrencyManager(),
                            lastCommitTime, name, outFile);

                }

                // add to set of tasks to be run.
                tasks.add(task);

                ndone++;
                
            }

            log.info("Will process "+ndone+" indices");
            
            assert ndone+nskip == nindices : "ndone="+ndone+", nskip="+nskip+", nindices="+nindices;

            // submit all tasks, awaiting their completion.
            final List<Future<Object>> futures = getConcurrencyManager()
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
            
            log.info("end");

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
        protected void processResults() throws Exception {

            log.info("begin");
            
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
                            getConcurrencyManager(), result.name,
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
                            getConcurrencyManager(), result.name,
                            result.splits,result.buildResults);

                    // add to set of tasks to be run.
                    tasks.add(task);

                }

            }

            // submit all tasks, awaiting their completion.
            final List<Future<Object>> futures = getConcurrencyManager()
                    .invokeAll(tasks);

            // verify that all tasks completed successfully.
            for (Future<Object> f : futures) {

                /* @todo error handling?
                 * 
                 * @todo redirect clients attempting to address old index partitions.
                 * 
                 * @todo update the metadata index?
                 * 
                 * @todo handle index partition moves as well (could be bottom-up).
                 */
                f.get();

            }
            
            log.info("end");

        }

        public Object call() throws Exception {

            if (overflowAllowed.get()) {

                // overflow must be disabled while we run this task.
                throw new AssertionError();

            }

            if(!resultQueue.isEmpty()) {
                
                /*
                 * Note: This can happen if the post-processing of the previous
                 * journal terminated abnormally.
                 */
                
                log.warn("Result queue not empty: "+resultQueue.size());
                
            }
            
            // clear queue as pre-condition.
            resultQueue.clear();
            
            try {

                // generate index segments.
                processNamedIndices();

                // put index segments into use by updating index partition definitions.
                processResults();

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
                if (!overflowAllowed.compareAndSet(false, true)) {

                    throw new AssertionError();

                }

            }

        }

    }

    /**
     * Task builds an {@link IndexSegment} from the fused view of an index
     * partition as of some historical timestamp. This task is typically applied
     * after an {@link IResourceManager#overflow(boolean, WriteExecutorService)}
     * in order to produce a compact view of the index as of the lastCommitTime
     * on the old journal. Note that the task by itself does not update the
     * definition of the live index, merely builds an {@link IndexSegment}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class BuildIndexSegmentTask extends AbstractPartitionTask {

        final protected File outFile;

        final protected byte[] fromKey;

        final protected byte[] toKey;

        /**
         * 
         * @param concurrencyManager
         * @param timestamp
         *            The lastCommitTime of the journal whose view of the index
         *            you wish to capture in the generated {@link IndexSegment}.
         * @param name
         *            The name of the index.
         * @param outFile
         *            The file on which the {@link IndexSegment} will be
         *            written.
         */
        public BuildIndexSegmentTask(IConcurrencyManager concurrencyManager,
                long timestamp, String name, File outFile) {

            this(concurrencyManager, timestamp, name, outFile,
                    null/* fromKey */, null/* toKey */);

        }

        /**
         * 
         * @param concurrencyManager
         * @param timestamp
         *            The lastCommitTime of the journal whose view of the index
         *            you wish to capture in the generated {@link IndexSegment}.
         * @param name
         *            The name of the index.
         * @param outFile
         *            The file on which the {@link IndexSegment} will be
         *            written.
         * @param fromKey
         *            The lowest key that will be counted (inclusive). When
         *            <code>null</code> there is no lower bound.
         * @param toKey
         *            The first key that will not be counted (exclusive). When
         *            <code>null</code> there is no upper bound.
         */
        public BuildIndexSegmentTask(IConcurrencyManager concurrencyManager,
                long timestamp, String name, File outFile, byte[] fromKey, byte[] toKey) {

            super(concurrencyManager, timestamp, name);

            if(outFile==null) throw new IllegalArgumentException();
            
            this.outFile = outFile;
            
            this.fromKey = fromKey;
             
            this.toKey = toKey;

        }

        /**
         * Build an {@link IndexSegment} from an index partition.
         * 
         * @return The {@link BuildResult}.
         */
        public Object doTask() throws Exception {

            // the name under which the index partition is registered.
            final String name = getOnlyResource();
            
            // The source view.
            final IIndex src = getIndex( name );
            
            /*
             * This MUST be the timestamp of the commit record from for the
             * source view. The startTime specified for the task has exactly the
             * correct semantics since you MUST choose the source view by
             * choosing the startTime!
             */
            final long commitTime = Math.abs( startTime );
            
            /*
             * Build the index segment.
             */
            
            return buildIndexSegment(name, src, outFile, commitTime, fromKey, toKey);

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
     *            {@link #overflow(boolean, WriteExecutorService)} operation.
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

        assert createTime > 0L;

        // metadata for that index / index partition.
        final IndexMetadata indexMetadata = src.getIndexMetadata();

        // the branching factor for the generated index segment.
        final int branchingFactor = indexMetadata
                .getIndexSegmentBranchingFactor();

         // Note: truncates nentries to int.
        final int nentries = (int) Math.min(src.rangeCount(fromKey, toKey),
                Integer.MAX_VALUE);
         
         /*
          * Note: Delete markers are ignored so they will NOT be transferred to
          * the new index segment (compacting merge).
          */
         final IEntryIterator itr = src
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
          * notify the resource manager so that it can find this file.
          * 
          * @todo once the index segment has been built the resource manager
          * should notice it in a restart manner and put it into play if it
          * has not already been used to update the view.
          */
         addResource(builder.segmentUUID, outFile);

         /*
          * Describe the index segment.
          */
         SegmentMetadata segmentMetadata = new SegmentMetadata(//
                 "" + outFile, //
                 outFile.length(),//
                 builder.segmentUUID,//
                 createTime//
                 );

         return new BuildResult(name, indexMetadata, segmentMetadata);
         
     }
     
    /**
     * Abstract base class for results when post-processing a named index
     * partition on the old journal after an overflow operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static abstract class AbstractResult {

        /**
         * The name under which the processed index partition was registered
         * (this is typically different from the name of the scale-out index).
         */
        public final String name;

        /**
         * The index metadata object for the processed index as of the timestamp
         * of the view from which the {@link IndexSegment} was generated.
         */
        public final IndexMetadata indexMetadata;

        /**
         * 
         * @param name
         *            The name under which the processed index partition was
         *            registered (this is typically different from the name of
         *            the scale-out index).
         * @param indexMetadata
         *            The index metadata object for the processed index as of
         *            the timestamp of the view from which the
         *            {@link IndexSegment} was generated.
         */
        public AbstractResult(String name, IndexMetadata indexMetadata) {

            this.name = name;
            
            this.indexMetadata = indexMetadata;

        }

    }

    /**
     * The result of an {@link BuildIndexSegmentTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class BuildResult extends AbstractResult {

        /**
         * The metadata describing the generated {@link IndexSegment}.
         */
        public final SegmentMetadata segmentMetadata;

        /**
         * 
         * @param name
         *            The name under which the processed index partition was
         *            registered (this is typically different from the name of
         *            the scale-out index).
         * @param indexMetadata
         *            The index metadata object for the processed index as of
         *            the timestamp of the view from which the
         *            {@link IndexSegment} was generated.
         * @param segmentMetadata
         *            The metadata describing the generated {@link IndexSegment}.
         */
        public BuildResult(String name, IndexMetadata indexMetadata,
                SegmentMetadata segmentMetadata) {

            super(name, indexMetadata);

            this.segmentMetadata = segmentMetadata;

        }

    }

    /**
     * The result of a {@link SplitIndexPartitionTask} including enough metadata
     * to identify the index partitions to be created and the index partition to
     * be deleted.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class SplitResult extends AbstractResult {

        /**
         * The array of {@link Split}s that describes the new key range for
         * each new index partition created by splitting the old index
         * partition.
         */
        public final Split[] splits;
        
        /**
         * An array of the {@link BuildResult}s for each output split.
         */
        public final BuildResult[] buildResults;

        /**
         * @param name
         *            The name under which the processed index partition was
         *            registered (this is typically different from the name of
         *            the scale-out index).
         * @param indexMetadata
         *            The index metadata object for the processed index as of
         *            the timestamp of the view from which the
         *            {@link IndexSegment} was generated.
         * @param splits
         *            Note: At this point we have the history as of the
         *            lastCommitTime in N index segments. Also, since we
         *            constain the resource manager to refuse another overflow
         *            until we have handle the old journal, all new writes are
         *            on the live index.
         * @param buildResults
         *            A {@link BuildResult} for each output split.
         */
        public SplitResult(String name, IndexMetadata indexMetadata,
                Split[] splits, BuildResult[] buildResults) {

            super( name, indexMetadata);

            assert splits != null;
            
            assert buildResults != null;
            
            assert splits.length == buildResults.length;
            
            for(int i=0; i<splits.length; i++) {
                
                assert splits[i] != null;

                assert splits[i].pmd != null;

                assert splits[i].pmd instanceof LocalPartitionMetadata;

                assert buildResults[i] != null;
                
            }
            
            this.splits = splits;
            
            this.buildResults = buildResults;

        }

    }

    /**
     * Task updates the definition of an index partition such that the specified
     * index segment is used in place of any older index segments and any
     * journal last commitTime is less than or equal to the createTime of the
     * new index segment.
     * <p>
     * The use case for this task is that you have just done an overflow on a
     * journal, placing empty indices on the new journal and defining their
     * views to read from the new index and the old journal. Then you built an
     * index segment from the last committed state of the index on the old
     * journal. Finally you use this task to update the view on the new journal
     * such that the index now reads from the new index segment rather than the
     * old journal.
     * <p>
     * Note: this implementation only works with a full compacting merge
     * scenario. It does NOT handle the case when multiple index segments are
     * required to complete the index partition view. the presumption is that
     * the new index segment was built from the fused view as of the last
     * committed state on the old journal, not just from the {@link BTree} on
     * the old journal.
     * <h2>Pre-conditions</h2>
     * 
     * <ol>
     * 
     * <li> The view is comprised of:
     * <ol>
     * 
     * <li>the live journal</li>
     * <li>the previous journal</li>
     * <li>an index segment having data for some times earlier than the old
     * journal (optional) </li>
     * </ol>
     * </li>
     * 
     * <li> The createTime on the live journal MUST be GT the createTime on the
     * previous journal (it MUST be newer).</li>
     * 
     * <li> The createTime of the new index segment MUST be LTE the
     * firstCommitTime on the live journal. (The new index segment should have
     * been built from a view that did not read on the live journal. In fact,
     * the createTime on the new index segment should be exactly the
     * lastCommitTime on the oldJournal.)</li>
     * 
     * <li> The optional index segment in the view MUST have a createTime LTE to
     * the createTime of the previous journal. (That is, it must have been from
     * a prior overflow operation and does not include any data from the prior
     * journal.)
     * </ol>
     * 
     * <h2>Post-conditions</h2>
     * 
     * The view is comprised of:
     * <ol>
     * <li>the live journal</li>
     * <li>the new index segment</li>
     * </ol>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
     public class UpdateIndexPartition extends AbstractTask {

         final protected SegmentMetadata segmentMetadata;
         
         /**
          * @param concurrencyManager
          * @param resource
          */
         protected UpdateIndexPartition(IConcurrencyManager concurrencyManager,
                 String resource, SegmentMetadata segmentMetadata) {

             super(concurrencyManager, ITx.UNISOLATED, resource);

             this.segmentMetadata = segmentMetadata;
             
         }

         @Override
         protected Object doTask() throws Exception {

             // the live journal.
             final AbstractJournal journal = getJournal();
             
             // live index
             final BTree btree = journal.getIndex(getOnlyResource());
             
             // clone the current metadata record for the live index.
             final IndexMetadata indexMetadata = btree.getIndexMetadata().clone();
             
             /*
              * This is the old index partition definition.
              */
             final LocalPartitionMetadata oldpmd = indexMetadata
                     .getPartitionMetadata();

             // Check pre-conditions.
             {

                 assert oldpmd != null : "Not an index partition: "+getOnlyResource();

                 final IResourceMetadata[] oldResources = oldpmd.getResources();

                 assert oldResources.length == 2 || oldResources.length == 3 : "Expecting either 2 or 3 resources: "
                         + oldResources;

                 assert oldResources[0].getUUID().equals(journal
                         .getRootBlockView().getUUID()) : "Expecting live journal to the first resource: "
                         + oldResources;

                 // the old journal's resource metadata.
                 final IResourceMetadata oldJournalMetadata = oldResources[1];
                 assert oldJournalMetadata != null;
                 assert oldJournalMetadata instanceof JournalMetadata;
                 
                 // live journal must be newer.
                 assert journal.getRootBlockView().getCreateTime() > oldJournalMetadata.getCreateTime();

                 // new index segment build from a view that did not include data from the live journal.
                 assert segmentMetadata.getCreateTime() < journal
                         .getRootBlockView().getFirstCommitTime();
                 
                 if (oldResources.length == 3) {
                     
                     // the old index segment's resource metadata.
                     final IResourceMetadata oldSegmentMetadata = oldResources[2];
                     assert oldSegmentMetadata != null;
                     assert oldSegmentMetadata instanceof SegmentMetadata;

                     assert oldSegmentMetadata.getCreateTime() <= oldJournalMetadata.getCreateTime();
                     
                 }
                 
             }

             // new view definition.
             final IResourceMetadata[] newResources = new IResourceMetadata[] {
                     journal.getResourceMetadata(),
                     segmentMetadata
             };

             // describe the index partition.
             indexMetadata
                     .setPartitionMetadata(new LocalPartitionMetadata(
                             oldpmd.getPartitionId(),//
                             oldpmd.getLeftSeparatorKey(),//
                             oldpmd.getRightSeparatorKey(),//
                             newResources //
                     ));
             
             // update the metadata associated with the btree.
             btree.setIndexMetadata(indexMetadata);
             
             // verify that the btree recognizes that it needs to be checkpointed.
             assert btree.needsCheckpoint();
             
             return null;
             
         }
         
     }

    /**
     * Task splits an index partition and should be invoked when there is strong
     * evidence that an index partition has grown large enough to be split into
     * 2 or more index partitions, each of which should be 50-75% full.
     * <p>
     * The task reads from the lastCommitTime of the old journal after an
     * overflow. It uses a key range scan to sample the index partition,
     * building an ordered set of {key,offset} tuples. Based on the actual #of
     * index entries and the target #of index entries per index partition, it
     * chooses the #of output index partitions, N, and selects N-1 {key,offset}
     * tuples to split the index partition. If the index defines a constraint on
     * the split rule, then that constraint will be applied to refine the actual
     * split points, e.g., so as to avoid splitting a logical row of a
     * {@link SparseRowStore}.
     * <p>
     * Once the N-1 split points have been selected, N index segments are built -
     * one from each of the N key ranges which those N-1 split points define.
     * After each index segment is built, it is added to a queue of index
     * segments that will be used to update the live index partition
     * definitions.
     * <p>
     * Unlike a {@link BuildIndexSegmentTask}, the
     * {@link SplitIndexPartitionTask} will eventually result in the original
     * index partition becoming un-defined and new index partitions being
     * defined in its place which span the same total key range.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class SplitIndexPartitionTask extends AbstractTask {

        /**
         * @param concurrencyManager
         * @param lastCommitTime
         * @param resource
         */
        protected SplitIndexPartitionTask(
                IConcurrencyManager concurrencyManager, long lastCommitTime,
                String resource) {

            super(concurrencyManager, -lastCommitTime, resource);

        }

        /**
         * Validate splits, including: that the separator keys are strictly
         * ascending, that the separator keys perfectly cover the source key
         * range without overlap, that the rightSeparator for each split is the
         * leftSeparator for the prior split, that the fromIndex offsets are
         * strictly ascending, etc.
         * 
         * @param src
         *            The source index.
         * @param splits
         *            The recommended split points.
         */
        protected void validateSplits(IIndex src, Split[] splits) {

            final IndexMetadata indexMetadata = src.getIndexMetadata();

            final int nsplits = splits.length;

            assert nsplits > 1 : "Expecting at least two splits, but found "
                    + nsplits;

            // verify splits obey index order constraints.
            int lastToIndex = -1;

            // Note: the first leftSeparator must be this value.
            byte[] fromKey = indexMetadata.getPartitionMetadata()
                    .getLeftSeparatorKey();

            for (int i = 0; i < nsplits; i++) {

                final Split split = splits[i];

                assert split != null;

                assert split.pmd != null;

                assert split.pmd instanceof LocalPartitionMetadata;

                LocalPartitionMetadata pmd = (LocalPartitionMetadata) split.pmd;

                // check the leftSeparator key.
                assert pmd.getLeftSeparatorKey() != null;
                assert BytesUtil.bytesEqual(fromKey, pmd.getLeftSeparatorKey());

                // verify rightSeparator is ordered after the left
                // separator.
                assert pmd.getRightSeparatorKey() == null
                        || BytesUtil.compareBytes(fromKey, pmd
                                .getRightSeparatorKey()) < 0;

                // next expected leftSeparatorKey.
                fromKey = pmd.getRightSeparatorKey();

                if (i == 0) {

                    assert split.fromIndex == 0;

                    assert split.toIndex > split.fromIndex;

                } else {

                    assert split.fromIndex == lastToIndex;

                }

                if (i + 1 == nsplits && split.toIndex == 0) {

                    /*
                     * Note: This is allowed in case the index partition has
                     * more than int32 entries in which case the toIndex of the
                     * last split can not be defined and will be zero.
                     */

                    assert split.ntuples == 0;

                    log.warn("Last split has no definate tuple count");

                } else {

                    assert split.toIndex - split.fromIndex == split.ntuples;

                }

                lastToIndex = split.toIndex;

            }

            /*
             * verify left separator key for 1st partition is equal to the left
             * separator key of the source (this condition is also checked
             * above).
             */
            assert ((LocalPartitionMetadata) splits[0].pmd)
                    .getLeftSeparatorKey().equals(
                            indexMetadata.getPartitionMetadata()
                                    .getLeftSeparatorKey());

            /*
             * verify right separator key for last partition is equal to the
             * right separator key of the source.
             */
            {
                
                // right separator for the last split.
                final byte[] rightSeparator = ((LocalPartitionMetadata) splits[splits.length - 1].pmd)
                        .getRightSeparatorKey();
                
                if(rightSeparator == null ) {
                    
                    // if null then the source right separator must have been null.
                    assert indexMetadata.getPartitionMetadata()
                            .getRightSeparatorKey() == null;
                    
                } else {
                    
                    // otherwise must compare as equals byte-by-byte.
                    assert rightSeparator.equals(
                            indexMetadata.getPartitionMetadata()
                                    .getRightSeparatorKey());
                    
                }
            }

        }
        
        /**
         * 
         * @return A {@link SplitResult } if the index partition was split into
         *         2 or more index partitions -or- a {@link BuildResult} iff the
         *         index partition was not split.
         */
        @Override
        protected Object doTask() throws Exception {

            final String name = getOnlyResource();
            
            final IIndex src = getIndex(name);
            
            final long createTime = Math.abs(startTime);
            
            final IndexMetadata indexMetadata = src.getIndexMetadata();
            
            final ISplitHandler splitHandler = indexMetadata.getSplitHandler();
            
            if (splitHandler == null) {
                
                // This was checked as a pre-condition and should not be null.
                
                throw new AssertionError();
                
            }

            /*
             * Get the split points for the index. Each split point describes a
             * new index partition. Together the split points MUST exactly span
             * the source index partitions key range. There MUST NOT be any
             * overlap in the key ranges for the splits.
             */
            final Split[] splits = splitHandler.getSplits(ResourceManager.this,src);
            
            if (splits == null) {
                
                /*
                 * No splits were choosen so the index will not be split at this
                 * time.  Instead we do a normal index segment build task.
                 */
                                
                // the file to be generated.
                final File outFile = getIndexSegmentFile(indexMetadata);

                return buildIndexSegment(name, src, outFile, createTime,
                        null/* fromKey */, null/*toKey*/);
                
            }
            
            final int nsplits = splits.length;
            
            log.info("Will build index segments for " + nsplits
                    + " splits for " + name);
            
            // validate the splits before processing them.
            validateSplits(src, splits);

            /*
             * Build N index segments based on those split points.
             * 
             * Note: This is done in parallel to minimize latency.
             */
            
            final List<AbstractTask> tasks = new ArrayList<AbstractTask>(nsplits);
            
            for (int i=0; i<splits.length; i++) {
                
                final Split split = splits[i];

                final LocalPartitionMetadata pmd = (LocalPartitionMetadata)split.pmd;
                
                final File outFile = getIndexSegmentFile(indexMetadata);
                
                AbstractTask task = new BuildIndexSegmentTask(
                        getConcurrencyManager(), -createTime, name, //
                        outFile,//
                        pmd.getLeftSeparatorKey(), //
                        pmd.getRightSeparatorKey());

                tasks.add(task);
                
            }
            
            // submit and await completion.
            final List<Future<Object>> futures = getConcurrencyManager().invokeAll(tasks);
            
            final BuildResult[] buildResults = new BuildResult[nsplits];

            int i = 0;
            for( Future<Object> f : futures ) {

                // @todo error handling?
                buildResults[i++] = (BuildResult) f.get();
                    
            }

            log.info("Generated "+splits.length+" index segments: name="+name);
            
            return new SplitResult(name,indexMetadata,splits,buildResults);
            
        }

    }

    /**
     * An {@link ITx#UNISOLATED} operation that splits the live index using the
     * same {@link Split} points, generating new index partitions with new
     * partition identifiers. The old index partition is deleted as a
     * post-condition. The new index partitions are registered as a
     * post-condition. Any data that was accumulated in the live index on the
     * live journal is copied into the appropriate new {@link BTree} for the new
     * index partition on the live journal.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class UpdateSplitIndexPartition extends AbstractTask {
        
        protected final Split[] splits;
        
        protected final BuildResult[] buildResults;
        
        public UpdateSplitIndexPartition(
                IConcurrencyManager concurrencyManager, String resource,
                Split[] splits, BuildResult[] buildResults) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            assert splits != null;
            
            assert buildResults != null;
            
            assert splits.length == buildResults.length;
            
            for(int i=0; i<splits.length; i++) {
                
                assert splits[i] != null;

                assert splits[i].pmd != null;

                assert splits[i].pmd instanceof LocalPartitionMetadata;
                
                assert buildResults[i] != null;
                
            }
            
            this.splits = splits;

            this.buildResults = buildResults;
            
        }

        @Override
        protected Object doTask() throws Exception {
            
            // the name of the source index.
            final String name = getOnlyResource();
            
            // this is the live journal since this task is unisolated.
            final AbstractJournal journal = getJournal();
            
            /*
             * Note: the source index is the BTree on the live journal that has
             * been absorbing writes since the last overflow.  This is NOT a fused
             * view.  All we are doing is re-distributing the writes onto the new
             * splits of the index partition.
             */
            final BTree src = (BTree) getIndexOnStore(name, ITx.UNISOLATED, journal); 
            
            /*
             * Locators for the new index partitions.
             */

            final LocalPartitionMetadata oldpmd = (LocalPartitionMetadata) src.getIndexMetadata().getPartitionMetadata();

            final PartitionLocatorMetadataWithSeparatorKeys[] locators = new PartitionLocatorMetadataWithSeparatorKeys[splits.length];
            
            for(int i=0; i<splits.length; i++) {

                // new metadata record (cloned).
                final IndexMetadata md = src.getIndexMetadata().clone();
                
                final LocalPartitionMetadata pmd = (LocalPartitionMetadata)splits[i].pmd;
                
                assert pmd.getResources() == null : "Not expecting resources for index segment: "+pmd;

                /*
                 * form locator for the new index partition for this split..
                 */
                final PartitionLocatorMetadataWithSeparatorKeys locator = new PartitionLocatorMetadataWithSeparatorKeys(
                        oldpmd.getPartitionId(),//
                        /*
                         * This is the set of failover services for this index
                         * partition. The first element of the array is always
                         * the data service on which this resource manager is
                         * running. The remainder of elements in the array are
                         * the failover services.
                         * 
                         * @todo The index partition data will be replicated at
                         * the byte image level for the live journal.
                         * 
                         * @todo New index segment resources will be replicated
                         * as well.
                         * 
                         * @todo Once the index partition data is fully
                         * replicated we update the metadata index.
                         */
                        getDataServiceUUIDs(),//
                        pmd.getLeftSeparatorKey(),//
                        pmd.getRightSeparatorKey()//
                        );
                
                locators[i] = locator;
                
                /*
                 * Update the view definition.
                 */
                md.setPartitionMetadata(pmd);
                
                // create new btree.
                final BTree btree = BTree.create(journal, md);

                // the new partition identifier.
                final int partitionId = pmd.getPartitionId();
                
                // name of the new index partition.
                final String name2 = DataService.getIndexPartitionName(name, partitionId);
                
                // register it on the live journal.
                journal.registerIndex(name2, btree);
                
                // lower bound (inclusive) for copy.
                final byte[] fromKey = pmd.getLeftSeparatorKey();
                
                // upper bound (exclusive) for copy.
                final byte[] toKey = pmd.getRightSeparatorKey();
                
                // copy all data in this split from the source index.
                final long ncopied = btree.rangeCopy(src, fromKey, toKey);
                
                log.info("Copied " + ncopied
                        + " index entries from the live index " + name
                        + " onto " + name2);
                
            }

            // drop the source index (the old index partition).
            journal.dropIndex(name);

            /*
             * Notify the metadata service that the index partition has been split.
             */
            getMetadataService().splitIndexPartition(
                    src.getIndexMetadata().getName(),//
                    oldpmd.getPartitionId(),//
                    oldpmd.getLeftSeparatorKey(),//
                    locators);
            
            return null;
            
        }
        
    }

    public String getStatistics() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("ResourceManager: dataService="+getDataServiceUUID()+", dataDir="+dataDir);
        
        return sb.toString();
        
    }
    
}
