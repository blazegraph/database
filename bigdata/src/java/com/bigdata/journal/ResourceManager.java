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
import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.btree.ReadOnlyFusedView;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.isolation.IsolatedFusedView;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.EmbeddedBigdataFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.MetadataService;

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
 * FIXME Move the executor services out of the {@link ConcurrentJournal}. This
 * is fine if there is only a single journal, but with multiple journals the
 * executor services need to be decoupled from the journal.
 * <p>
 * Make sure that {@link AbstractTask#getIndex(String)} returns a {@link BTree},
 * {@link FusedView}, {@link IsolatedFusedView}, {@link ReadOnlyFusedView},
 * or even a {@link KeyRangePartitionedView} as necessary for index partitions
 * (the KeyRangePartitionedView is a trial balloon for transparently promoting
 * an index to scale-out, but maybe that operation should be explicit - not
 * transparent - since it radically changes how the index is accessed from a
 * services perspective).
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
 * @todo handle re-opening of an embedded federation using this class.
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
public class ResourceManager {

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

    static {

        cf = NumberFormat.getNumberInstance();

        cf.setGroupingUsed(true);

        fpf = NumberFormat.getNumberInstance();

        fpf.setGroupingUsed(false);

        fpf.setMaximumFractionDigits(2);

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
     * @param name
     *            The index name or null if this is not a named index.
     * @param filename
     *            The name of the file on which the index segment was written.
     * @param nentries
     *            The #of entries in the {@link IndexSegment}.
     * @param elapsed
     *            The elapsed time of the operation that built the index segment
     *            (merge + build).
     * @param nbytes
     *            The #of bytes in the {@link IndexSegment}.
     * 
     * @todo the event is reported from {@link IndexSegmentBuilder} and does not
     *       report the index name and does not account for resources
     *       (time/space) required by the merge aspect of a bulk build.
     */
    static public void buildIndexSegment(String name, String filename,
            int nentries, long elapsed, long nbytes) {

        if (INFO) {

            // data rate in MB/sec.
            float mbPerSec = (elapsed == 0 ? 0 : nbytes / Bytes.megabyte32
                    / (elapsed / 1000f));

            log.info("name=" + name + ", filename=" + filename + ", nentries="
                    + nentries + ", elapsed=" + elapsed + ", "
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
    static public void overflowJournal(String filename, long nbytes) {

        if (INFO)
            log.info("filename=" + filename + ", #bytes=" + nbytes);

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
     * {@link ResourceManager} options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options {

        /**
         * <code>data.dir</code> - The property whose value is the name of the
         * directory in which the store files will be created (default is the
         * current working directory).
         * <p>
         * The files are created within subdirectories as follows: The
         * "journals" subdirectory contains the journal files. The "segments"
         * subdirectory contains the index segment files. The "segments"
         * directory is contains subdirectories corresponding to the index name
         * (munged as necessary to conform to the file system), then the index
         * UUID (always unique), and finally the index partition identifier.
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
         * @todo Make sure that the tasks that handle {@link IndexSegment}
         *       generation place the {@link IndexSegment} files according to
         *       the scheme described here.
         * 
         * @todo When copying a resource from a remote {@link DataService} make
         *       sure that it gets placed according to the scheme defined here.
         * 
         * @todo Perhaps there is no need for the "file" in the
         *       {@link IResourceMetadata} since it appears that we need to
         *       maintain a map from UUID to local file system files in anycase.
         *       <p>
         *       Likewise, if the {@link MetadataIndex} maintains historical
         *       entries using historical reads then we probably do not need to
         *       store anything except the "live" entries in the index partition
         *       metadata.
         */
        public static final String DATA_DIR = "data.dir";

        /**
         * The default is the current working directory.
         */
        public static final String DEFAULT_DATA_DIR = ".";

    }

    /**
     * (Re-)open the {@link ResourceManager}.
     * 
     * @param properties
     *            See {@link Options}.
     * 
     * @throws IOException
     */
    public ResourceManager(Properties properties) throws IOException {

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties;

        /*
         * Create the _transient_ index in which we will store the mapping from
         * the commit times of the journals to their resource descriptions.
         */
        journalIndex = JournalIndex.create(new SimpleMemoryRawStore());

        log.info("Current working directory: "
                + new File(".").getAbsolutePath());

        /*
         * data directory.
         */
        {

            // Note: dataDir is _canonical_
            final File dataDir = new File(properties.getProperty(
                    Options.DATA_DIR, Options.DEFAULT_DATA_DIR))
                    .getCanonicalFile();

            log.info(Options.DATA_DIR + "=" + dataDir);

            journalsDir = new File(dataDir, "journals").getCanonicalFile();

            segmentsDir = new File(dataDir, "segments").getCanonicalFile();

            if (!dataDir.exists()) {

                log.warn("Creating data directory: " + dataDir);

                if (!dataDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + dataDir.getAbsolutePath());

                }

                if (!journalsDir.mkdir()) {

                    throw new RuntimeException("Could not create directory: "
                            + journalsDir.getAbsolutePath());

                }

                if (!segmentsDir.mkdir()) {

                    throw new RuntimeException("Could not create directory: "
                            + segmentsDir.getAbsolutePath());

                }

            }

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
            final File tmpDir = new File(properties.getProperty(
                    Options.TMP_DIR, System.getProperty("java.io.tmpdir")))
                    .getCanonicalFile();

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

        /*
         * Look for pre-existing data files.
         */
        {

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
                file = File.createTempFile("journal", // prefix
                        Options.JNL,// suffix
                        journalsDir // directory
                        ).getCanonicalFile();

                // delete temp file.
                file.delete();

                /*
                 * Set the createTime on the new journal resource.
                 * 
                 * FIXME Use the same timesource as the tx manager!
                 */
                p.setProperty(Options.CREATE_TIME, ""
                        + System.currentTimeMillis());
                
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

            assert file.isAbsolute() : "Path must be absolute: " + file;

            p.setProperty(Options.FILE, file.toString());

            // Create/open journal.
            liveJournal = new Journal(p);

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

        final File[] files = dir.listFiles();

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

            Journal tmp = new Journal(properties);

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

            log.warn("Ignoring unknown file: " + file);

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

    /**
     * @todo update the shutdown methods if we add a thread pool for anything.
     * 
     * @todo note that the resource manager is closed when we start the shutdown
     *       and check for !open on various methods.
     * 
     * FIXME There needs to be a shutdown protocol that coordinates with the
     * shutdown of the concurrency control (services for reading or writing on
     * index resourcesa).
     */
    public void shutdown() {

        shutdownNow();

    }

    public void shutdownNow() {

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

    /**
     * Return the reference to the journal which has the most current data for
     * the given timestamp. If necessary, the journal will be opened.
     * 
     * @param timestamp
     *            The startTime of an active transaction, <code>0L</code> for
     *            the current unisolated index view, or <code>-timestamp</code>
     *            for a historical view no later than the specified timestamp.
     * 
     * @return The corresponding journal for that timestamp.
     */
    public AbstractJournal getJournal(long timestamp) {

        if (timestamp == 0L) {

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

            resource = journalIndex.find(timestamp);

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

                final AbstractJournal journal = new Journal(properties);

                // verify journal was closed for writes.
                assert journal.getRootBlockView().getCloseTime() != 0;
                
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
     * Note: The returned index is NOT isolated and is NOT protected against
     * writes (unless it is on an {@link IndexSegmentFileStore}).
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
     *         was not registered on the resource as of the timestamp.
     * 
     * @throws RuntimeException
     *             if the resource could not be located.
     * @throws RuntimeException
     *             if the resource does not have data for the specified
     *             timestamp.
     * 
     * @todo work through {@link Tx#getIndex(String)} and verify that it will
     *       return the isolated index (or index partition view).
     */
    public AbstractBTree openIndex(String name, long timestamp, IRawStore store) {

        // use absolute value in case timestamp is negative.
        final long ts = Math.abs(timestamp);

        final AbstractBTree btree;

        if (store instanceof IJournal) {

            AbstractJournal journal = (AbstractJournal) store;

            if (timestamp == 0L) {

                // unisolated view.

                btree = (BTree) journal.getIndex(name);

            } else {

                /*
                 * A historical view.
                 */

                // the corresponding commit record on that journal.
                final ICommitRecord commitRecord = journal.getCommitRecord(ts);

                if (commitRecord == null) {

                    throw new RuntimeException(
                            "Resource has no data for timestamp: timestamp="
                                    + ts + ", store=" + store);

                }

                // open index on that journal.
                btree = (BTree) journal.getIndex(name, commitRecord);

            }

        } else {

            final IndexSegmentFileStore segStore = ((IndexSegmentFileStore) store);

            if (segStore.getCheckpoint().commitTime < ts) {

                throw new RuntimeException(
                        "Resource has no data for timestamp: timestamp=" + ts
                                + ", store=" + store);

            }

            // Open an index segment.
            btree = segStore.load();

        }

        log.info("name=" + name + ", timestamp=" + ts + ", store=" + store
                + " : " + btree);

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
     * @return The sources for the index view or <code>null</code> if the
     *         index was not defined as of the timestamp.
     * 
     * @see FusedView
     */
    public AbstractBTree[] openIndexPartition(String name, long timestamp) {

        log.info("name=" + name + ", timestamp=" + timestamp);

        /*
         * Open the index on the journal for that timestamp.
         */
        final AbstractBTree btree;
        {

            // the corresponding journal (can be the live journal).
            final AbstractJournal journal = getJournal(timestamp);

            btree = openIndex(name, timestamp, journal);

            log.info("View base on " + journal.getResourceMetadata());

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
        final PartitionMetadataWithSeparatorKeys pmd = btree.getIndexMetadata()
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
            final IResourceMetadata[] a = pmd.getLiveResources();

            sources = new AbstractBTree[a.length];

            // the most recent is this btree.
            sources[0/* j */] = btree;

            for (int i = 1; i < a.length; i++) {

                final IResourceMetadata resource = a[i];

                final IRawStore store = openStore(resource.getUUID());

                final AbstractBTree ndx = openIndex(name, timestamp, store);

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

    /**
     * Creates a new journal and re-defines the views for all named indices to
     * include the pre-overflow view with reads being absorbed by a new btree on
     * the new journal.
     * <p>
     * Note: This method MUST NOT be invoked unless you have exclusive access to
     * the journal. Typically, the only time you can guarentee this is during
     * commit processing. Therefore overflow is generally performed by a
     * post-commit event handler.
     * <p>
     * Note: Journal references MUST NOT be presumed to survive this method. In
     * particular, the old journal will be closed out by this method and marked
     * as read-only henceforth.
     * <p>
     * Note: The decision to simply re-define views rather than export the data
     * on the named indices on the old journal
     */
    public void overflow() {

        log.info("begin");

        /*
         * FIXME Use the same timesource as the tx manager.
         */
        final long createTime = System.currentTimeMillis();
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
            
            newJournal = new Journal(p);
            
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

                // old index
                final BTree oldBTree = (BTree) oldJournal.getIndex(entry.addr);

                // clone index metadata.
                final IndexMetadata indexMetadata = oldBTree.getIndexMetadata()
                        .clone();

                // old partition metadata.
                final PartitionMetadataWithSeparatorKeys oldpmd = indexMetadata
                        .getPartitionMetadata();

                if (oldpmd == null) {

                    /*
                     * @todo this is a bit brittle when using an embedded
                     * journal since there are no checks to ensure that all
                     * indices are scale-out. However, when using a federation
                     * all indices are scale-out indices (...except the metadata
                     * indices themselves so you can't overflow on the metadata
                     * service right now...).
                     */

                    throw new RuntimeException(
                            "Can not overflow unpartitioned index: "
                                    + entry.name);

                }

                IResourceMetadata[] oldResources = oldpmd.getResources();

                IResourceMetadata[] newResources = new IResourceMetadata[oldResources.length + 1];

                System.arraycopy(oldResources, 0, newResources, 1,
                        oldResources.length);

                // new resource is listed first (reverse chronological order)
                newResources[0] = newJournal.getResourceMetadata();

                // describe the index partition.
                indexMetadata
                        .setPartitionMetadata(new PartitionMetadataWithSeparatorKeys(
                                oldpmd.getPartitionId(),//
                                oldpmd.getDataServices(),//
                                newResources,//
                                oldpmd.getLeftSeparatorKey(),//
                                oldpmd.getRightSeparatorKey()//
                        ));

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
                            .overflowCheckpoint(oldCounter);

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
        {
            
            // writes no longer accepted.
            oldJournal.close(closeTime);
            
            // remove from list of open journals.
            if(this.openStores.remove(oldJournal.getRootBlockView().getUUID())==null) {
                
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

    }

}
