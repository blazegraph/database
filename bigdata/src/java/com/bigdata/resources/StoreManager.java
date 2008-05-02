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
 * Created on Mar 24, 2008
 */

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.service.DataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.MetadataService;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Class encapsulates logic for managing the store files (journals and index
 * segments), including the logic to compute the effective release time for the
 * managed resources and to release those resources by deleting them from the
 * file system.
 * <p>
 * Note: Since the journal already manages its own index resources we simply
 * keep the journals open until they are purged. When a journal is purged we
 * closeAndDelete() it and any readers will simply abort.
 * 
 * @todo This is Ok if the journal uses [bufferMode := DiskOnly] since a journal
 *       without a write cache and which is not fully buffered will not have a
 *       large memory footprint outside of its most recently used indices. In
 *       order to do better than this and proactively close historical journals
 *       we will require a mechanisms similar to the one for
 *       {@link IndexSegmentStore}s in the {@link IndexManager}.
 * 
 * @todo There is neither a "CREATE_TEMP_DIR" and "DELETE_ON_CLOSE" does not
 *       remove all directories created during setup. One of the consequences is
 *       that you have to explicitly clean up after a unit test using a
 *       {@link ResourceManager} or it will leave its files around.
 * 
 * @todo track the disk space used by the {@link #getDataDir()} and the free
 *       space remaining on the mount point that hosts the data directory and
 *       report via counters to the {@link ILoadBalancerService}. if we
 *       approach the limit on the space in use then we need to shed index
 *       partitions to other data services or potentially become more aggressive
 *       in releasing old resources.
 *       <p>
 *       See
 *       http://commons.apache.org/io/api-release/index.html?org/apache/commons/io/FileSystemUtils.html
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class StoreManager extends ResourceEvents implements
        IResourceManager {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(StoreManager.class);

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
     * Options for the {@link StoreManager}.
     * <p>
     * Note: See {@link com.bigdata.journal.Options} for options that may be
     * applied when opening an {@link AbstractJournal}.
     * <p>
     * Note: See {@link IndexSegmentStore.Options} for options that may be
     * applied when opening an {@link IndexSegment}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options,
            IndexSegmentStore.Options {

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
         * {@link Options#SEG} as the file suffix. If the index is partitioned
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
         */
        String DATA_DIR = "data.dir";

        /**
         * How long you want to hold onto the database history (in milliseconds)
         * or {@link Long#MAX_VALUE} for an (effectively) immortal database.
         * Some convenience values have been declared.
         * 
         * @see #DEFAULT_MIN_RELEASE_AGE
         * @see #MIN_RELEASE_AGE_1H
         * @see #MIN_RELEASE_AGE_1D
         * @see #MIN_RELEASE_AGE_1W
         * @see #MIN_RELEASE_AGE_NEVER
         */
        String MIN_RELEASE_AGE = "minReleaseAge";
        
        /** Minimum release age is one minutes. */
        String MIN_RELEASE_AGE_1M = "" + 1/* mn */* 60/* sec */* 1000/* ms */;

        /** Minimum release age is five minutes. */
        String MIN_RELEASE_AGE_5M = "" + 5/* mn */* 60/* sec */* 1000/* ms */;

        /** Minimum release age is one hour. */
        String MIN_RELEASE_AGE_1H = "" + 1/* hr */* 60/* mn */* 60/* sec */
                * 1000/* ms */;

        /** Minimum release age is one day. */
        String MIN_RELEASE_AGE_1D = "" + 24/* hr */* 60/* mn */* 60/* sec */
                * 1000/* ms */;

        /** Minimum release age is one week. */
        String MIN_RELEASE_AGE_1W = "" + 7/* d */* 24/* hr */* 60/* mn */
                * 60/* sec */
                * 1000/* ms */;

        /** Immortal database (the release time is set to {@link Long#MAX_VALUE}). */
        String MIN_RELEASE_AGE_NEVER = "" + Long.MAX_VALUE;

        /** Default minimum release age is one day. */
        String DEFAULT_MIN_RELEASE_AGE = MIN_RELEASE_AGE_1D;

        /**
         * The capacity of the LRU cache of open {@link IRawStore}s. The
         * capacity of this cache indirectly controls how many stores will be
         * held open. The main reason for keeping an store open is to reuse its
         * buffers if another request arrives "soon" which would read on that
         * store. Note that "stores" includes both {@link ManagedJournal}s and
         * {@link IndexSegmentStore}s.
         * <p>
         * The effect of this parameter is indirect owning to the semantics of
         * weak references and the control of the JVM over when they are
         * cleared. Once an index becomes weakly reachable, the JVM will
         * eventually GC the index object, thereby releasing its object graph.
         * Since stores which are strongly reachable never have their weak
         * reference cleared this provides our guarentee that stores are never
         * closed if they are in use.
         * <p>
         * Stores have non-transient resources and MUST explicitly be closed.
         * Since we are not notified before the weak reference is closed, our
         * only remaining option is {@link AbstractJournal#finalize()} and
         * {@link IndexSegmentStore#finalize()}, both of which close the store
         * if it is still open.
         * 
         * @see #DEFAULT_INDEX_SEGMENT_CACHE_CAPACITY
         * 
         * @todo define maximum age on the LRU and the delay between sweeps of
         *       the LRU
         */
        String STORE_CACHE_CAPACITY = "storeCacheCapacity";

        /**
         * The default for the {@link #STORE_CACHE_CAPACITY} option.
         */
        String DEFAULT_STORE_CACHE_CAPACITY = "20";

        /**
         * A boolean property whose value determines whether or not startup will
         * complete successfully if bad files are identified during the startup
         * scan (default {@value #DEFAULT_IGNORE_BAD_FILES}). When
         * <code>false</code> the {@link StoreManager} will refuse to start if
         * if find bad files. When <code>true</code> the {@link StoreManager}
         * will startup anyway but some index views may not be available.
         * Regardless, bad files will be logged as they are identified and all
         * files will be scanned before the {@link StoreManager} aborts.
         */
        String IGNORE_BAD_FILES = "ignoreBadFiles";

        String DEFAULT_IGNORE_BAD_FILES = "false";

    }

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
     * A temporary store used for the {@link #journalIndex} and the
     * {@link #segmentIndex}. The store is not used much so it is
     * configured to keep its in-memory footprint small.
     * 
     * @todo since this is a WORM store it will never shrink in size. Eventually
     *       it could grow modestly large if the data service were to run long
     *       enough. Therefore it makes sense to periodically "overflow" this by
     *       copying the current state of the {@link #journalIndex} and the
     *       {@link #segmentIndex} onto a new tmpStore (or just rebuilding
     *       them from the {@link #dataDir}).
     */
    private final IRawStore tmpStore = new TemporaryRawStore(
            WormAddressManager.SCALE_UP_OFFSET_BITS,//
            10 * Bytes.kilobyte, // initial in memory extent
            100 * Bytes.megabyte, // maximum in memory extent
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
    final private IndexSegmentIndex segmentIndex;

    /**
     * A cache that is used by the to automatically close out unused
     * {@link IndexSegmentStore}s. An {@link IndexSegment} that is no longer
     * used will have its reference cleared when it is swept by the garbage
     * collector and will automatically release all of its buffers (node and
     * leaf cache, etc). However, at that point the {@link IndexSegmentStore} is
     * still open, and it can buffer a significant amount of data in addition to
     * the file handle.
     * <p>
     * When the weak reference is cleared we know that there are no longer any
     * hard references to the {@link IndexSegment} and hence the corresponding
     * {@link IndexSegmentStore} should be closed. In fact, we can immediately
     * remove the {@link IndexSegmentStore} from the cache of open stores and
     * then close the store. At this point if the store is re-opened it will be
     * a new object. This is easy enough to do since the {@link UUID} of the
     * {@link IndexSegmentStore} is the key in our map!
     * 
     * @see Options#INDEX_SEGMENT_CACHE_CAPACITY
     * 
     * @todo purges entries that have not been touched in the last N seconds,
     *       where N might be 60.
     */
    final protected WeakValueCache<UUID, IRawStore> storeCache;

    /**
     * The #of entries in the hard reference cache for {@link IRawStore}s,
     * including both {@link ManagedJournal}s and IndexSegment}s. There MAY be
     * more {@link IRawStore}s open than are reported by this method if there
     * are hard references held by the application to those {@link IRawStore}s.
     * {@link IRawStore}s that are not fixed by a hard reference will be
     * quickly finalized by the JVM.
     */
    public int getStoreCacheSize() {

        return storeCache.size();

    }

    /**
     * <code>true</code> iff {@link BufferMode#Transient} was indicated.
     */
    private final boolean isTransient;

    /**
     * A direct {@link ByteBuffer} that will be used as the write cache for the
     * live journal and which will be handed off from live journal to live
     * journal during overflow processing which is allocated iff
     * {@link BufferMode#Disk} is choosen.
     * <p>
     * Note: This design is motivated by by JVM bug <a
     * href="http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=8fab76d1d4479fffffffffa5abfb09c719a30?bug_id=6210541">
     * 6210541</a> which describes a failure by
     * <code>releaseTemporaryDirectBuffer()</code> to release temporary direct
     * {@link ByteBuffer}s that are allocated for channel IO.
     * 
     * @see com.bigdata.journal.Options#WRITE_CACHE_CAPACITY
     * @see DiskOnlyStrategy
     */
    protected final ByteBuffer writeCache;

    /**
     * A hard reference to the live journal.
     */
    protected ManagedJournal liveJournal;

    /**
     * <code>true</code> initially and remains <code>true</code> until the
     * {@link ResourceManager} is shutdown.
     * 
     * @see #isOpen()
     */
    private final AtomicBoolean open = new AtomicBoolean(true);

    /**
     * <code>true</code> initially and until {@link #start()} completes
     * successfully, this is used to disambiguate the startup transient state
     * from the shutdown state.
     * 
     * @see #isStarting()
     */
    private final AtomicBoolean starting = new AtomicBoolean(true);

    /**
     * @see Options#IGNORE_BAD_FILES
     */
    private final boolean ignoreBadFiles;

    /**
     * Used to run the {@link Startup}.
     */
    private final ExecutorService startupService = Executors
            .newSingleThreadExecutor(DaemonThreadFactory.defaultThreadFactory());

    /**
     * Succeeds if the {@link StoreManager} {@link #isOpen()} and is NOT
     * {@link #isStarting()}.
     * 
     * @throws IllegalStateException
     *             unless open and not starting.
     */
    protected void assertRunning() {

        if (!isOpen())
            throw new IllegalStateException("Not open");

        if (isStarting())
            throw new IllegalStateException("Starting up");

    }

    /**
     * Return <code>true</code> iff the {@link StoreManager} is open and
     * startup processing has been completed.
     */
    public boolean isRunning() {

        return isOpen() && !isStarting();

    }

    /**
     * @throws IllegalStateException
     *             unless open.
     */
    protected void assertOpen() {

        if (!isOpen())
            throw new IllegalStateException();

    }

    /**
     * @throws IllegalStateException
     *             if open.
     */
    protected void assertNotOpen() {

        if (isOpen())
            throw new IllegalStateException();

    }

    /**
     * Return <code>true</code> iff the {@link StoreManager} is running. If
     * the {@link StoreManager} is currently starting up, then this will await
     * the completion of the {@link Startup} task.
     * 
     * @return <code>true</code> if the {@link StoreManager} is running and
     *         <code>false</code> if it is shutdown.
     */
    public boolean awaitRunning() {

        while (isOpen() && isStarting()) {

            try {

                log.info("Waiting on startup : " + dataDir + " ...");

                Thread.sleep(1000/* ms */);

            } catch (InterruptedException ex) {

                throw new RuntimeException("Interrupted awaiting startup: "
                        + ex);

            }

        }

        return isRunning();

    }

    /**
     * A map from the resource UUID to the absolute {@link File} for that
     * resource.
     * <p>
     * Note: The {@link IResourceMetadata} reported by an
     * {@link AbstractJournal} or {@link IndexSegmentStore} generally reflects
     * the name of the file as specified to the ctor for that class, so it may
     * be relative to some arbitrary directory or absolute within the file
     * system.
     * 
     * @todo We do not need to insist on the file names - we could just use the
     *       file under whatever name we find it (Therefore we only insist that
     *       the name of the file (as understood by the {@link ResourceManager})
     *       agree with the {@link IResourceMetadata} and the
     *       {@link ResourceManager} maintains its own map from the
     *       {@link IResourceMetadata} to an <em>absolute</em> path for that
     *       resource.)
     */
    private final Map<UUID, File> resourceFiles = new HashMap<UUID, File>();

    /**
     * The properties given to the ctor.
     */
    private final Properties properties;

    /**
     * Release time is zero (0L) until notified otherwise - 0L is ignored.
     * 
     * @see #setReleaseTime(long)
     */
    protected long releaseTime = 0L;

    /**
     * The last value computed by {@link #getEffectiveReleaseTime()} and ZERO(0)
     * until a value has been calculated.
     */
    protected long lastCommitTimePreserved = 0L;

    /**
     * Resources MUST be at least this many milliseconds before they may be
     * deleted.
     * <p>
     * The minReleaseAge is just how long you want to hold onto an immortal
     * database view. E.g., 3 days of full history. Specify
     * {@link Long#MAX_VALUE} for an immortal database (resources will never be
     * deleted).
     * 
     * @see Options#MIN_RELEASE_AGE
     */
    final long minReleaseAge;

    /**
     * The #of journals that have been opened to date.
     */
    final protected AtomicLong journalOpenCount = new AtomicLong();

    /**
     * The #of index segments that have been opened to date.
     */
    final protected AtomicLong segmentOpenCount = new AtomicLong();
    
    /**
     * An object wrapping the {@link Properties} given to the ctor.
     */
    public Properties getProperties() {

        return new Properties(this.properties);

    }

    /**
     * Return <code>true</code> iff data can not be made restart-safe.
     */
    public boolean isTransient() {

        return isTransient;

    }

    /**
     * Note: This constructor starts an asynchronous thread that scans the data
     * directory for journals and index segments and creates the initial journal
     * if no store files are found.
     * <p>
     * Note: The store files are NOT accessible until the asynchronous startup
     * is finished. Caller's MUST verify that the {@link StoreManager#isOpen()}
     * AND NOT submit tasks until {@link StoreManager#isStarting()} returns
     * <code>false</code>.
     * 
     * @param properties
     *            See {@link Options}.
     * 
     * @see Startup
     */
    protected StoreManager(Properties properties) {

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties;

        // ignoreBadFiles
        {

            ignoreBadFiles = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.IGNORE_BAD_FILES,
                            Options.DEFAULT_IGNORE_BAD_FILES));

            log.info(Options.IGNORE_BAD_FILES + "=" + ignoreBadFiles);

        }

        /*
         * storeCacheCapacity
         */
        {

            final int storeCacheCapacity = Integer.parseInt(properties
                    .getProperty(Options.STORE_CACHE_CAPACITY,
                            Options.DEFAULT_STORE_CACHE_CAPACITY));

            log.info(Options.STORE_CACHE_CAPACITY + "=" + storeCacheCapacity);

            if (storeCacheCapacity <= 0)
                throw new RuntimeException(Options.STORE_CACHE_CAPACITY
                        + " must be non-negative");

            storeCache = new WeakValueCache<UUID, IRawStore>(
            // WeakValueCache.INITIAL_CAPACITY,//
                    // WeakValueCache.LOAD_FACTOR, //
                    new LRUCache<UUID, IRawStore>(storeCacheCapacity)
            // new WeakCacheEntryFactory<UUID,IndexSegment>(),
            // new ClearReferenceListener()
            );

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
         * Allocate an optional write cache that will be passed from live
         * journal to live journal during overflow.
         */
        {

            writeCache = AbstractJournal.getWriteCache(properties);

        }

        /*
         * Create the _transient_ index in which we will store the mapping from
         * the commit times of the journals to their resource descriptions.
         */
        journalIndex = JournalIndex.create(tmpStore);
        segmentIndex = IndexSegmentIndex.create(tmpStore);

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

                    throw new RuntimeException("Required property: "
                            + Options.DATA_DIR);

                }

                // Note: stored in canonical form.
                dataDir = new File(val).getCanonicalFile();

                log.info(Options.DATA_DIR + "=" + dataDir);

                journalsDir = new File(dataDir, "journals").getCanonicalFile();

                segmentsDir = new File(dataDir, "segments").getCanonicalFile();

            } catch (IOException ex) {

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

                log.info("Creating: " + journalsDir);

                if (!journalsDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + journalsDir.getAbsolutePath());

                }

            }

            if (!segmentsDir.exists()) {

                log.info("Creating: " + segmentsDir);

                if (!segmentsDir.mkdirs()) {

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

                tmpDir = new File(properties.getProperty(Options.TMP_DIR,
                        System.getProperty("java.io.tmpdir")))
                        .getCanonicalFile();

            } catch (IOException ex) {

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

        /*
         * Asynchronous startup processing.
         */
        startupService.submit(new Startup());

    }

    /**
     * Runs a startup scan of the data directory and creates the initial journal
     * if none was found. If the {@link Startup} task fails or is interrupted
     * then the {@link StoreManager} will be {@link StoreManager#shutdownNow()}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class Startup implements Runnable {

        public void run() {

            try {

                try {

                    start();

                } catch (Throwable ex) {

                    // avoid possibility that isRunning() could become true.
                    open.set(false);

                    log.error("Problem during startup? : " + ex, ex);

                    shutdownNow();

                    // terminate Startup task.
                    throw new RuntimeException(ex);

                }

            } finally {

                /*
                 * Whether or not startup was successful, we now turn this flag
                 * off.
                 */

                starting.set(false);

                log.info("Startup "
                        + (isOpen() ? "successful" : "failed")
                        + " : "
                        + (isTransient ? "transient" : Options.DATA_DIR + "="
                                + dataDir));

            }

        }

        /**
         * Starts up the {@link StoreManager}.
         * <p>
         * Note: Implementations of this method MUST be
         * <code>synchronized</code>.
         * 
         * @throws InterruptedException
         * 
         * @throws IllegalStateException
         *             if the {@link IConcurrencyManager} has not been set.
         * 
         * @throws IllegalStateException
         *             if the the {@link ResourceManager} is already running.
         * 
         * @throws InterruptedException
         *             if the startup scan is interrupted.
         * 
         * @throws RuntimeException
         *             if bad files are encountered, etc.
         */
        final private void start() throws InterruptedException {

            if (!isStarting()) {

                throw new IllegalStateException();

            }

            /*
             * Verify that the concurrency manager has been set and wait a while
             * it if is not available yet.
             */
            log.info("Waiting for concurrency manager");
            for (int i = 0; i < 3; i++) {
                try {
                    getConcurrencyManager();
                } catch (IllegalStateException ex) {
                    Thread.sleep(100/* ms */);
                }
            }
            getConcurrencyManager();
            if (Thread.interrupted())
                throw new InterruptedException();

            /*
             * Look for pre-existing data files.
             */
            if (!isTransient) {

                log.info("Starting scan of data directory: " + dataDir);

                final Stats stats = new Stats();

                scanDataDirectory(dataDir, stats);

                final int nbad = stats.badFiles.size();

                log.info("Scan results: " + stats);

                if (!stats.badFiles.isEmpty()) {

                    if (ignoreBadFiles) {

                        log
                                .warn("The following "
                                        + nbad
                                        + " file(s) had problems and are being ignored: "
                                        + stats.badFiles);

                    } else {

                        final String msg = "There are " + nbad
                                + " bad files - will not start: badFiles="
                                + stats.badFiles;

                        log.fatal(msg);

                        throw new RuntimeException(msg);

                    }

                }

                assert journalIndex.getEntryCount() == stats.njournals;

                assert segmentIndex.getEntryCount() == stats.nsegments;

                assert resourceFiles.size() + nbad == stats.nfiles : "#resourceFiles="
                        + resourceFiles.size()
                        + ", #nbad="
                        + nbad
                        + ", nfiles=" + stats.nfiles;

            }

            /*
             * Open the "live" journal.
             */
            {

                log.info("Creating/opening the live journal: dataDir="
                        + dataDir);

                if (Thread.interrupted())
                    throw new InterruptedException();

                final Properties p = getProperties();
                final File file;
                final boolean newJournal;

                if (journalIndex.getEntryCount() == 0) {

                    /*
                     * There are no existing journal files. Create new journal
                     * using a unique filename in the appropriate subdirectory
                     * of the data directory.
                     * 
                     * @todo this is not using the temp filename mechanism in a
                     * manner that truely guarentees an atomic file create. The
                     * CREATE_TEMP_FILE option should probably be extended with
                     * a CREATE_DIR option that allows you to override the
                     * directory in which the journal is created. That will
                     * allow the atomic creation of the journal in the desired
                     * directory without changing the existing semantics for
                     * CREATE_TEMP_FILE.
                     * 
                     * See OverflowManager#doOverflow() which has very similar
                     * logic with the same problem.
                     */

                    log.info("Creating initial journal: dataDir=" + dataDir);

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
                    p.setProperty(Options.CREATE_TIME, ""
                            + nextTimestampRobust());

                    newJournal = true;

                } else {

                    /*
                     * There is at least one pre-existing journal file, so we
                     * open the one with the largest timestamp - this will be
                     * the most current journal and the one that will receive
                     * writes until it overflows.
                     */

                    // resource metadata for journal with the largest
                    // timestamp.
                    final IResourceMetadata resource = journalIndex
                            .find(Long.MAX_VALUE);

                    log.info("Will open as live journal: " + resource);

                    assert resource != null : "No resource? : timestamp="
                            + Long.MAX_VALUE;

                    // lookup absolute file for that resource.
                    file = resourceFiles.get(resource.getUUID());

                    assert file != null : "No file? : resource=" + resource;

                    log.info("Opening most recent journal: " + file
                            + ", resource=" + resource);

                    newJournal = false;

                }

                if (!isTransient) {

                    assert file.isAbsolute() : "Path must be absolute: " + file;

                    p.setProperty(Options.FILE, file.toString());

                }

                log.info("Open/create of live journal: newJournal="
                        + newJournal + ", file=" + file);

                // Create/open journal.
                if (Thread.interrupted())
                    throw new InterruptedException();
                liveJournal = new ManagedJournal(p);

                if (newJournal) {

                    // add to the set of managed resources.
                    addResource(liveJournal.getResourceMetadata(), liveJournal
                            .getFile());

                }

                // add to set of open stores.
                storeCache.put(liveJournal.getRootBlockView().getUUID(),
                        liveJournal, false/* dirty */);

                if (Thread.interrupted())
                    throw new InterruptedException();

            }

        }

    }

    /**
     * <code>true</code> initally and until {@link #start()} completes
     * successfully.
     */
    public boolean isStarting() {

        return starting.get();

    }

    /**
     * <code>false</code> initially and remains <code>false</code> until
     * {@link #start()} completes successfully. once <code>true</code> this
     * remains <code>true</code> until either {@link #shutdown()} or
     * {@link #shutdownNow()} is invoked.
     */
    public boolean isOpen() {

        return open.get();

    }

    synchronized public void shutdown() {

        log.info("");

        final boolean wasOpen = this.open.get();

        /*
         * Note: clear before we clear [starting] or the
         * StoreManager#isRunning() could report true.
         */
        this.open.set(false);

        // Note: if startup is running, then cancel immediately.
        startupService.shutdownNow();

        // failsafe clear : note that [open] is already false.
        starting.set(false);

        if (!wasOpen)
            return;

        try {
            closeStores();
        } catch (Exception ex) {
            log.warn(ex.getMessage(), ex);
        }

        try {
            tmpStore.closeAndDelete();
        } catch (Exception ex) {
            log.warn(ex.getMessage(), ex);
        }

    }

    synchronized public void shutdownNow() {

        log.info("");

        final boolean wasOpen = this.open.get();

        /*
         * Note: clear before we clear [starting] or the
         * StoreManager#isRunning() could report true.
         */
        this.open.set(false);

        startupService.shutdownNow();

        // failsafe clear : note that [open] is already false.
        starting.set(false);

        if (!wasOpen)
            return;

        try {
            closeStores();
        } catch (Exception ex) {
            log.warn(ex.getMessage(), ex);
        }

        try {
            tmpStore.closeAndDelete();
        } catch (Exception ex) {
            log.warn(ex.getMessage(), ex);
        }

    }

    /**
     * Helper class gathers statistics about files during a scan.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class Stats {

        /**
         * #of files scanned.
         */
        public int nfiles;

        /**
         * #of journal files scanned.
         */
        public int njournals;

        /**
         * #of index segment files found.
         */
        public int nsegments;

        /**
         * A list of all bad files found during the scan.
         */
        public Collection<String> badFiles = Collections
                .synchronizedCollection(new TreeSet<String>());

        /**
         * total #of bytes of user data found in those files.
         */
        public long nbytes;

        public String toString() {

            return "Stats{nfiles=" + nfiles + ", njournals=" + njournals
                    + ", nsegments=" + nsegments + ", nbad=" + badFiles.size()
                    + ", nbytes=" + nbytes + ", badFiles=" + badFiles + "}";

        }

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
     * 
     * @throws InterruptedException
     */
    private void scanDataDirectory(File dir, Stats stats)
            throws InterruptedException {

        if (dir == null)
            throw new IllegalArgumentException();

        if (!dir.isDirectory())
            throw new IllegalArgumentException();

        if (Thread.interrupted())
            throw new InterruptedException();

        final File[] files = dir.listFiles(newFileFilter());

        for (final File file : files) {

            if (file.isDirectory()) {

                scanDataDirectory(file, stats);

            } else {

                scanFile(file, stats);

            }

        }

    }

    private void scanFile(File file, Stats stats) throws InterruptedException {

        if (Thread.interrupted())
            throw new InterruptedException();

        log.info("Scanning file: " + file + ", stats=" + stats);

        stats.nfiles++;

        final IResourceMetadata resource;

        final String name = file.getName();

        if (name.endsWith(Options.JNL)) {

            final Properties properties = getProperties();

            properties.setProperty(Options.FILE, file.getAbsolutePath());

            // Note: no writes allowed during startup.
            // Note: disables the write cache among other things.
            properties.setProperty(Options.READ_ONLY, "true");

            final AbstractJournal tmp;
            try {

                tmp = new ManagedJournal(properties);

            } catch (Exception ex) {

                log.error("Problem opening journal: file="
                        + file.getAbsolutePath(), ex);

                stats.badFiles.add(file.getAbsolutePath());

                return;

            }

            try {

                resource = tmp.getResourceMetadata();

                stats.njournals++;

                stats.nbytes += file.length(); // tmp.size();
                                                // //getBufferStrategy().getExtent();

            } finally {

                tmp.close();

            }

        } else if (name.endsWith(Options.SEG)) {

            final Properties p = new Properties();

            p.setProperty(IndexSegmentStore.Options.SEGMENT_FILE, file
                    .getAbsolutePath());

            // Note: disables buffering nodes during the scan.
            p.setProperty(
                    IndexSegmentStore.Options.MAX_BYTES_TO_FULLY_BUFFER_NODES,
                    "1");

            /*
             * Attempt to open the index segment.
             */
            final IndexSegmentStore segStore;
            try {

                segStore = new IndexSegmentStore(p);

            } catch (Exception ex) {

                log.error("Problem opening segment: file="
                        + file.getAbsolutePath(), ex);

                stats.badFiles.add(file.getAbsolutePath());

                return;

            }

            try {

                resource = segStore.getResourceMetadata();

                stats.nsegments++;

                stats.nbytes += file.length(); // segStore.size();

            } finally {

                segStore.close();

            }

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

        addResource(resource, file.getAbsoluteFile());

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

    /**
     * Closes ALL open store files.
     */
    private void closeStores() {

        final Iterator<IRawStore> itr = storeCache.iterator();

        while (itr.hasNext()) {

            final IRawStore store = itr.next();

            try {
                store.close();
            } catch (Exception ex) {
                log.warn(ex.getMessage(), ex);
            }

            itr.remove();

        }

    }

    /**
     * The #of journals on hand.
     */
    public int getJournalCount() {

        assertOpen();

        return journalIndex.getEntryCount();

    }

    /**
     * The #of index segments on hand.
     */
    public int getIndexSegmentCount() {

        assertOpen();

        return segmentIndex.getEntryCount();

    }

    /**
     * Notify the resource manager of a new resource. The resource is added to
     * {@link #resourceFiles} and to either {@link #journalIndex} or
     * {@link #segmentIndex} as appropriate. As a post-condition, you can
     * use {@link #openStore(UUID)} to open the resource using the {@link UUID}
     * specified by {@link IResourceMetadata#getUUID()}.
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
     *             if the {@link #journalIndex} or {@link #segmentIndex}
     *             already know about that resource.
     * @throws RuntimeException
     *             if {@link #openStore(UUID)} already knows about that
     *             resource.
     * @throws IllegalArgumentException
     *             if the <i>resourceMetadata</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>file</i> is <code>null</code> and
     *             {@link #isTransient} is <code>false</code>.
     */
    synchronized protected void addResource(IResourceMetadata resourceMetadata,
            File file) {

        assertOpen();

        if (resourceMetadata == null)
            throw new IllegalArgumentException();

        if (file == null && !isTransient)
            throw new IllegalArgumentException();

        final UUID uuid = resourceMetadata.getUUID();

        if (storeCache.get(uuid) != null) {

            throw new RuntimeException("Resource already open?: "
                    + resourceMetadata);

        }

        if (!isTransient) {

            if (!file.exists()) {

                throw new RuntimeException("File not found: " + file);

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

        if (resourceMetadata.isJournal()) {

            journalIndex.add(resourceMetadata);

        } else {

            segmentIndex.add(resourceMetadata);

        }

    }

    /**
     * Returns a filter that is used to recognize files that are managed by this
     * class. The {@link ResourceManager} will log warnings if it sees an
     * unexpected file and will NOT {@link #deleteResources()} files that it
     * does not recognize.
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
     * The object used to control access to the index resources.
     * 
     * @throws IllegalStateException
     *             if the object has not been set yet using
     *             {@link #setConcurrencyManager(IConcurrencyManager)}.
     */
    public abstract IConcurrencyManager getConcurrencyManager();

    /**
     * Implementation designed to use a shared {@link ConcurrencyManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class ManagedJournal extends AbstractJournal {

        /**
         * Note: Each instance of the {@link ManagedJournal} reuses the SAME
         * {@link StoreManager#writeCache}. Therefore you MUST close out writes
         * on the old journal BEFORE you may allocate a new journal.
         * 
         * @param properties
         */
        protected ManagedJournal(Properties properties) {

            super(properties, writeCache);

        }

        public long nextTimestamp() {

            return StoreManager.this.nextTimestampRobust();

        }

        public ILocalTransactionManager getLocalTransactionManager() {

            return getConcurrencyManager().getTransactionManager();

        }

        // @Override
        // public long commit() {
        //
        // return commitNow(nextTimestamp());
        //            
        // }

    }

    /**
     * The journal on which writes are made.
     * 
     * @throws IllegalStateException
     *             if the {@link StoreManager} is not open.
     * @throws IllegalStateException
     *             if the {@link StoreManager} is still starting up.
     */
    public AbstractJournal getLiveJournal() {

        assertRunning();

        return liveJournal;

    }

    /**
     * @throws IllegalStateException
     *             if the {@link StoreManager} is not open.
     * @throws IllegalStateException
     *             if the {@link StoreManager} is still starting up.
     * 
     * @todo write tests for unisolated and read-committed. make sure that there
     *       is no fencepost for read committed immediately after an overflow
     *       (there should not be since we do a commit when we register the
     *       indices on the new store).
     */
    public AbstractJournal getJournal(long timestamp) {

        assertRunning();

        if (timestamp == ITx.UNISOLATED || timestamp == ITx.READ_COMMITTED) {

            /*
             * This is a request for the live journal.
             * 
             * Note: The live journal remains open except during overflow, when
             * it is changed to a new journal and the old live journal is
             * closed. Therefore we NEVER cause the live journal to be opened
             * from the disk in this method.
             */

            assert liveJournal != null : "open=" + isOpen() + ", starting="
                    + isStarting() + ", dataDir=" + dataDir;
            assert liveJournal.isOpen();
            assert !liveJournal.isReadOnly();

            return getLiveJournal();

        }

        final IResourceMetadata resource;

        synchronized (journalIndex) {

            resource = journalIndex.find(Math.abs(timestamp));

        }

        if (resource == null) {

            log.info("No such journal: timestamp=" + timestamp);

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
     * @throws IllegalStateException
     *             if the {@link StoreManager} is not open.
     * @throws IllegalStateException
     *             if the {@link StoreManager} is still starting up.
     * @throws IllegalArgumentException
     *             if <i>uuid</i> is <code>null</code>.
     * @throws RuntimeException
     *             if something goes wrong.
     * 
     * FIXME per-store lock to reduce latency.
     * <p>
     * Since these operations can have modest latency, especially if we open an
     * fully buffered index segment, it would be nice to use a per-store (or
     * store UUID) lock to avoid imposing latency on threads requiring access to
     * different stores.
     */
    synchronized public IRawStore openStore(UUID uuid) {

        assertRunning();

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

        IRawStore store = storeCache.get(uuid);

        if (store != null) {

            if (!store.isOpen()) {

                if (store instanceof IndexSegmentStore) {

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
                    ((IndexSegmentStore) store).reopen();

                    // re-opening the store.
                    segmentOpenCount.incrementAndGet();

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
                // Note: disables the write cache among other things.
                properties.setProperty(Options.READ_ONLY, "true");

                final AbstractJournal journal = new ManagedJournal(properties);

                final long closeTime = journal.getRootBlockView()
                        .getCloseTime();

                // verify journal was closed for writes.
                assert closeTime != 0 : "Journal not closed for writes? "
                        + " : file=" + file + ", uuid=" + uuid + ", closeTime="
                        + closeTime;

                assert journal.isReadOnly();

                actualUUID = journal.getRootBlockView().getUUID();

                store = journal;

                // opened another journal.
                journalOpenCount.incrementAndGet();

            } else {

                IndexSegmentStore segStore = new IndexSegmentStore(file);

                actualUUID = segStore.getCheckpoint().segmentUUID;

                store = segStore;

                // opened another index segment store.
                segmentOpenCount.incrementAndGet();

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
        storeCache.put(uuid, store, false/* dirty */);

        // return the reference to the open store.
        return store;

    }

    /**
     * Report the next timestamp assigned by the
     * {@link ILocalTransactionManager}.
     * <p>
     * Note: the {@link ILocalTransactionManager} handles the "robust" semantics
     * for discoverying the timestamp service and obtaining the next timestamp
     * from that service.
     */
    protected long nextTimestampRobust() {

        final ILocalTransactionManager transactionManager = getConcurrencyManager()
                .getTransactionManager();

        return transactionManager.nextTimestampRobust();

    }

    public void deleteResources() {

        assertNotOpen();

        // NOP if transient.
        if (isTransient())
            return;

        log.warn("Deleting all resources: " + dataDir);

        recursiveDelete(dataDir);

    }

    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * <p>
     * Note: Files that are not recognized will be logged by the
     * {@link ResourceFileFilter}.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles(newFileFilter());

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }

    /**
     * Updates the {@link #releaseTime}.
     * 
     * @see #purgeOldResources(), which is responsible for actually deleting the
     *      old resources.
     * 
     * @todo When the transaction manager will control [releaseTime] does it
     *       need to set the [releaseTime] on all resource managers in the
     *       federation before it can assign its first transaction identifier?
     */
    public void setReleaseTime(final long releaseTime) {

        assertOpen();

        log.info("Updating the releaseTime: old=" + this.releaseTime + ", new="
                + this.releaseTime);

        if (releaseTime < this.releaseTime) {

            throw new IllegalArgumentException(
                    "The release time can only move forward: releaseTime="
                            + this.releaseTime + ", but given " + releaseTime);

        }

        // Note: RMI call.
        final long currentTime = nextTimestampRobust();
        
        if(releaseTime>currentTime) {
            
            throw new IllegalArgumentException(
                    "The release time is in the future: releaseTime="
                            + releaseTime + ", but currentTime=" + currentTime);
            
        }
        
        this.releaseTime = releaseTime;

    }

    /**
     * Return the last value set with {@link #setReleaseTime(long)}.
     * 
     * @return
     */
    public long getReleaseTime() {

        return releaseTime;

    }

    /**
     * Return the minimum age of a resource before it may become a candidate for
     * release. This is a configuration time constant. The purpose of this value
     * is to guarentee that resources will remain available for at least this
     * many milliseconds. This places an upper bound on the release time.
     * 
     * @see Options#MIN_RELEASE_AGE
     */
    public long getMinReleaseAge() {

        return minReleaseAge;

    }

    /**
     * Delete resources having no data for the
     * {@link #getEffectiveReleaseTime()}.
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
     * @throws IllegalStateException
     *             if the {@link StoreManager} is not running.
     */
    protected void purgeOldResources() {

        assertRunning();

        if (minReleaseAge == Long.MAX_VALUE) {

            /*
             * Constant for an immortal database so we do not need to check the
             * timestamp service. Return zero(0) indicating that only resources
             * whose timestamp is LTE zero may be deleted (e.g., nothing may be
             * deleted).
             */

            log.info("Immortal database");
            
            return;

        }

        // the current time (RMI).
        final long currentTime = nextTimestampRobust();

        // the upper bound on the release time.
        final long maxReleaseTime = currentTime - minReleaseAge;

        if (maxReleaseTime < 0L) {

            /*
             * Note: Someone specified a very large value for
             * [minReleaseAge] and the clock has not yet reached that value.
             * (The test above for Long.MAX_VALUE just avoids the RMI to the
             * timestamp service when we KNOW that the database is
             * immortal).
             */

            log.info("Nothing is old enough to release.");

            return;

        }

        final long releaseTime;
        if (this.releaseTime == 0L) {

            /*
             * Note: The [releaseTime] is normally advanced by the
             * transaction manager when it decides that a historical time
             * will no longer be reachable by new transactions and no
             * running transactions is reading from that historical time.
             * Absent a transaction manager, the [releaseTime] is assumed to
             * be [minReleaseAge] milliseconds in the past.
             * 
             * Note: If there are active tasks reading from that historical
             * time then they will fail once the resources have been
             * released.
             */

            releaseTime = maxReleaseTime;

            log.info("releaseTime is not set: using maxReleaseTime="
                    + maxReleaseTime);

        } else {

            /*
             * Choose whichever timestamp would preserve more history.
             */

            releaseTime = Math.min(maxReleaseTime, this.releaseTime);

            log.info("releaseTime=" + releaseTime);

        }
        
        final long commitTimeToPreserve;
        if (releaseTime > getLiveJournal().getRootBlockView()
                .getLastCommitTime()) {

            /*
             * When [minReleaseAge] is small (e.g., zero), the [currentTime] can
             * be greater than the [lastCommitTime] on the live journal and so
             * we choose the [lastCommitTime] instead.
             */

            log.info("Choosing the last commit time for the live journal");
            
            commitTimeToPreserve = getLiveJournal().getRootBlockView()
                    .getLastCommitTime();

        } else {

            /*
             * Find the timestamp for the commit record that is strictly greater
             * than the release time.
             */
            commitTimeToPreserve = getCommitTimeStrictlyGreaterThan(releaseTime);
        
        }
        
        /*
         * Make a note for reporting purposes.
         */
        this.lastCommitTimePreserved = commitTimeToPreserve;
        
        /*
         * Find resources that are still in use as of that commitTime.
         */
        final Set<UUID> resourcesInUse = getResourcesForTimestamp(commitTimeToPreserve);
        
        // and delete anything that is ((NOT in use) AND createTime<commitTimeToPreserve)
        deleteUnusedResources(commitTimeToPreserve,resourcesInUse);
        
    }
    
    /**
     * Delete unused resources given a set of resources that are still in use.
     * The basic steps are:
     * <ol>
     * 
     * <li>close iff open</li>
     * 
     * <li>remove from lists of known resources.</li>
     * 
     * <li>delete in the file system</li>
     * 
     * </ol>
     * 
     * @param commitTimeToPreserve
     *            Resources created as of or later than this timestamp WILL NOT
     *            be deleted.
     * @param resourcesInUse
     *            The set of resources required by views as of the
     *            <i>commitTimeToPreserve</i>. These resources have create
     *            times LTE to <i>commitTimeToPreserve</i> but are in use but
     *            at least one view as of that commit time and therefore MUST
     *            NOT be deleted.
     */
    private void deleteUnusedResources(final long commitTimeToPreserve, final Set<UUID> resourcesInUse) {
        
        /*
         * Delete old journals.
         */
        // #of journals deleted.
        int njournals = 0;
        {

            /*
             * Note: used to bulk delete the entries for deleted journals from
             * the journalIndex since the iterator does not support removal.
             */
            final Set<byte[]> keys = new TreeSet<byte[]>(
                    UnsignedByteArrayComparator.INSTANCE);

            final ITupleIterator itr = journalIndex.entryIterator();

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final IResourceMetadata resourceMetadata = (IResourceMetadata) SerializerUtil
                        .deserialize(tuple.getValue());

                // the create timestamp for that resource.
                final long createTime = resourceMetadata.getCreateTime();

                if (createTime > commitTimeToPreserve) {

                    /*
                     * Do NOT delete any resources whose createTime is GTE the
                     * given commit time.
                     */
                    
                    break;
                    
                }
                
                final UUID uuid = resourceMetadata.getUUID();

                if (resourcesInUse.contains(uuid)) {
                    
                    // still required as of that timestamp.
                    
                    continue;
                    
                }

                log.info("Will delete: " + resourceMetadata);

                deleteResource(uuid, true/*isJournal*/);

                // add to set for batch remove.
                keys.add(journalIndex.getKey(resourceMetadata.getCreateTime()));

                njournals++;
                
            }

            // remove entries from the journalIndex.
            for (byte[] key : keys) {

                if (journalIndex.remove(key) == null) {

                    throw new AssertionError();

                }

            }

        }

        /*
         * Delete old index segments.
         */
        // #of segments deleted.
        int nsegments = 0;
        {

            /*
             * Note: used to bulk delete the entries for deleted segments from
             * the segmentIndex since the iterator does not support removal.
             */
            final Set<byte[]> keys = new TreeSet<byte[]>(
                    UnsignedByteArrayComparator.INSTANCE);

            final ITupleIterator itr = segmentIndex.entryIterator();

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final IResourceMetadata resourceMetadata = (IResourceMetadata) SerializerUtil
                        .deserialize(tuple.getValue());

                // the create timestamp for that resource.
                final long createTime = resourceMetadata.getCreateTime();

                if (createTime > commitTimeToPreserve) {

                    /*
                     * Do NOT delete any resources whose createTime is GTE the
                     * given commit time.
                     */
                    
                    break;
                    
                }
                
                final UUID uuid = resourceMetadata.getUUID();

                if (resourcesInUse.contains(uuid)) {
                    
                    // still required as of that timestamp.
                    
                    continue;
                    
                }

                log.info("Will delete: " + resourceMetadata);

                // delete the backing file.
                deleteResource(uuid, false/*isJournal*/);
                
                // add to set for batch remove.
                keys.add(segmentIndex.getKey(resourceMetadata.getCreateTime(), uuid));

                nsegments++;
                
            }

            // remove entries from the journalIndex.
            for (byte[] key : keys) {

                if (segmentIndex.remove(key) == null) {

                    throw new AssertionError();

                }

            }

        }

        log.info("Given " + resourcesInUse.size()
                + " resources that are in use as of timestamp="
                + commitTimeToPreserve + ", deleted " + njournals
                + " journals and " + nsegments + " segments");
        
    }
    
    /**
     * Delete the resource in the file system and remove it from the
     * {@link #storeCache} and {@link #resourceFiles}.
     * <p>
     * Note: The caller is responsible for removing it from the
     * {@link #journalIndex} or the {@link #segmentIndex} as appropriate.
     * 
     * @param uuid
     *            The {@link UUID} which identifies the resource.
     * @param isJournal
     *            <code>true</code> if the resource is a journal.
     */
    private void deleteResource(UUID uuid, boolean isJournal) {

        // can't close out the live journal!
        if (uuid == getLiveJournal().getRootBlockView().getUUID()) {

            throw new IllegalArgumentException();
            
        }

        /*
         * Close out store iff open.
         */
        {

            final IRawStore store = storeCache.remove(uuid);

            if (store != null) {
                
                if(isJournal) {
                    
                    assert store instanceof AbstractJournal;
                    
                } else {
                    
                    assert store instanceof IndexSegmentStore;
                    
                }
                
                try {

                    store.close();
                    
                } catch(Throwable t) {
                    
                    log.error(t.getMessage(),t);
                    
                }

            }

        }

        /*
         * delete the backing file.
         */
        try {

            final File file = resourceFiles.remove(uuid);

            if (file == null) {

                throw new RuntimeException("No file for resource? uuid=" + uuid);

            } else {

                if (!file.exists()) {

                    throw new RuntimeException("Not found: " + file);

                } else if (!file.delete()) {

                    throw new RuntimeException("Could not delete: " + file);

                }

            }

        } catch(Throwable t) {
            
            log.error(t.getMessage(), t);
            
        }

    }

//    /**
//     * The effective release time is the minimum timestamp for which resources
//     * may be safely released (aka deleted). The upper bound on the release time
//     * is the current time reported by {@link #nextTimestampRobust()} minus the
//     * {@link #getMinReleaseAge()}. When the {@link #getReleaseTime()} is
//     * non-zero, the lower bound on the release time is computed using
//     * {@link #getEarliestDependencyTimestamp(long)}. The effective release
//     * time is the minimum of these two timestamps. Resources whose createTime
//     * is LTE that effective release time MAY be safely deleted.
//     * 
//     * @return The effective release time.
//     * 
//     * @see Options#MIN_RELEASE_AGE
//     * @see IResourceManager#setReleaseTime(long)
//     */
//    protected long getEffectiveReleaseTime() {
//
//        assertOpen();
//
//
//        final long effectiveReleaseTime = Math.min(maxReleaseTime, earliestDependencyTime);
//
//        log.info("effectiveReleaseTime(" + effectiveReleaseTime
//                + ") := Min(maxReleaseTime=" + maxReleaseTime
//                + ", earliestDependencyTime=" + earliestDependencyTime + ")");
//
//        assert effectiveReleaseTime <= maxReleaseTime : "effectiveReleaseTime="
//                + effectiveReleaseTime + ", maxReleaseTime=" + maxReleaseTime;
//
//        lastEffectiveReleaseTime = effectiveReleaseTime;
//
//        return effectiveReleaseTime;
//
//    }

    /**
     * Finds the journal spanning the first {@link ICommitRecord} that is
     * strictly greater than the specified timestamp and returns the timestamp
     * of that {@link ICommitRecord}.
     * 
     * @param releaseTime
     *            A release time as set by {@link #setReleaseTime(long)}. Any
     *            resource as of this timestamp is available for release.
     * 
     * @return The timestamp of the first commit record whose timestamp is
     *         strictly greater than <i>releaseTime</i>.
     * 
     * @throws IllegalArgumentException
     *             if there is no commit point that is strictly greater than the
     *             releaseTime is not spanned by any journal (this implies that
     *             the release time is either in the future or, if the
     *             releaseTime is equal to the last commitTime, that you are
     *             trying to release everything in the database).
     */
    protected long getCommitTimeStrictlyGreaterThan(final long releaseTime) {

        final ManagedJournal journal = (ManagedJournal) getJournal(releaseTime);

        if (journal == null) {

            throw new IllegalArgumentException("No data for releaseTime="
                    + releaseTime);

        }

        final IRootBlockView rootBlockView = journal.getRootBlockView();

        final ICommitRecord commitRecord = journal
                .getCommitRecordStrictlyGreaterThan(releaseTime);

        if (commitRecord == null) {

            final long closeTime = rootBlockView.getCloseTime();

            if (closeTime == 0L) {

                /*
                 * Since this journal is not closed then we know that the next
                 * commit would be on this journal, but there is no commit for
                 * that release time.
                 */

                throw new IllegalArgumentException("No data for releaseTime="
                        + releaseTime);
                
            }

            /*
             * Otherwise this journal was closed as of this timestamp.
             * getJournal(timestamp) returns the journal having data for the
             * timestamp. However, since we are interested in the _next_ commit
             * point, we need to recursively invoke ourselves when the close
             * time of this journal.
             */
            
            return getCommitTimeStrictlyGreaterThan(closeTime);
            
        }

        /*
         * This is the timestamp associated with the commit point that is the
         * first commit point strictly greater than the given release time.
         */
        
        final long commitTime = commitRecord.getTimestamp();
        
        log.warn("commitPoint="+commitTime+" for releaseTime="+releaseTime);
        
        assert commitTime > releaseTime;
    
        return commitTime;
        
    }
    
    /**
     * Finds all resources used by any registered index as of the given
     * commitTime.
     * 
     * @param commitTime
     *            A commit time.
     * 
     * @return The set of resource {@link UUID}s required by at least one
     *         registered index as of that commit time.
     * 
     * @throws IllegalArgumentException
     *             if there is no commit point that is strictly greater than the
     *             releaseTime is not spanned by any journal (this implies that
     *             the release time is either in the future or, if the
     *             releaseTime is equal to the last commitTime, that you are
     *             trying to release everything in the database).
     */
    protected Set<UUID> getResourcesForTimestamp(final long commitTime) {

        final ManagedJournal journal = (ManagedJournal) getJournal(commitTime);

        if (journal == null) {

            throw new IllegalArgumentException("No data for commitTime="
                    + commitTime);

        }

        // the Name2Addr instance for that timestamp.
        final IIndex name2Addr = journal.getName2Addr(commitTime);

        assert name2Addr != null;

        log.info("commitTime=" + commitTime + "\njournal="
                + journal.getResourceMetadata());

        final Set<UUID> uuids = new HashSet<UUID>();

        final ITupleIterator itr = name2Addr.rangeIterator(null, null);

        while (itr.hasNext()) {

            final ITuple tuple = itr.next();

            final Entry entry = EntrySerializer.INSTANCE
                    .deserialize(new DataInputBuffer(tuple.getValue()));

            /*
             * Open the historical BTree used to absorb writes on the historical
             * journal.
             */
            final BTree btree = (BTree) journal.getIndex(entry.checkpointAddr);

            assert btree != null : entry.toString();

            // index metadata for that index partition.
            final IndexMetadata indexMetadata = btree.getIndexMetadata();

            // index partition metadata
            final LocalPartitionMetadata pmd = indexMetadata
                    .getPartitionMetadata();

            for (IResourceMetadata tmp : pmd.getResources()) {

                // add to set of in-use resources.
                uuids.add(tmp.getUUID());

            }

        }

        log.info("There are " + uuids.size() + " resources in use: commitTime="
                + commitTime);

        return uuids;

    }

    /**
     * Munge the name of a scale out index so that it is suitable for use in a
     * filesystem. In particular, any non-word characters are converted to an
     * underscore character ("_"). This gets rid of all punctuation characters
     * and whitespace in the index name itself, but will not translate unicode
     * characters.
     * <p>
     * Note: The index name appears in the file path beneath the {@link UUID} of
     * the scale-out index. Therefore it is not possible to have collisions
     * arise in the file system when given indices whose scale-out names differ
     * only in characters that are munged onto the same character since the
     * files will always be stored in a directory specific to the scale-out
     * index.
     * 
     * @param s
     *            The name of the scale-out index.
     * 
     * @return A string suitable for inclusion in a filename.
     */
    private String munge(String s) {

        return s.replaceAll("[\\W]", "_");

    }

    /**
     * 
     * @todo should the filename be relative or absolute?
     */
    public File getIndexSegmentFile(IndexMetadata indexMetadata) {

        assertOpen();

        // munge index name to fit the file system.
        final String mungedName = munge(indexMetadata.getName());

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

}
