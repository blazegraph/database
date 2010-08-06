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
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.LRUNexus;
import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.cache.IGlobalLRU.ILRUCache;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractLocalTransactionManager;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.WORMStrategy.StoreCounters;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.IndexPartitionCause;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.SegmentMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.service.DataService;
import com.bigdata.service.Event;
import com.bigdata.service.EventResource;
import com.bigdata.service.EventType;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.MetadataService;
import com.bigdata.service.ResourceService;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Class encapsulates logic for managing the store files (journals and index
 * segments), including the logic to compute the effective release time for the
 * managed resources and to release those resources by deleting them from the
 * file system.
 * 
 * @todo There is neither a "CREATE_TEMP_DIR" and "DELETE_ON_CLOSE" does not
 *       remove all directories created during setup. One of the consequences is
 *       that you have to explicitly clean up after a unit test using a
 *       {@link ResourceManager} or it will leave its files around.
 * 
 * @todo {@link BufferMode#Temporary} is not supported (verify whether the
 *       Transient mode is supported).
 * 
 * @todo If we approach the limit on free space for the {@link #dataDir} then we
 *       need to shed index partitions to other data services or potentially
 *       become more aggressive in releasing old resources. See
 *       {@link #getDataDirFreeSpace(File)}
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
     * Options for the {@link StoreManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options {

        /**
         * The property whose value is the name of the directory in which the
         * store files will be created (no default). This property is required
         * unless the instance is transient. If you specify
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
         * where ever they are found but the {@link IResourceMetadata#getFile()}
         * read from a given resource MUST correspond to its relative location
         * within the {@link #DATA_DIR}.
         * <p>
         * Note: Each {@link DataService} or {@link MetadataService} MUST have
         * its own {@link #DATA_DIR}.
         */
        String DATA_DIR = StoreManager.class.getName()+".dataDir";

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
         * reference cleared this provides our guarantee that stores are never
         * closed if they are in use.
         * <p>
         * Stores have non-transient resources and MUST explicitly be closed.
         * Since we are not notified before the weak reference is closed, our
         * only remaining option is {@link AbstractJournal#finalize()} and
         * {@link IndexSegmentStore#finalize()}, both of which close the store
         * if it is still open.
         * 
         * @see #DEFAULT_STORE_CACHE_CAPACITY
         */
        String STORE_CACHE_CAPACITY = StoreManager.class.getName()
                + ".storeCacheCapacity";

        /**
         * The default for the {@link #STORE_CACHE_CAPACITY} option.
         */
        String DEFAULT_STORE_CACHE_CAPACITY = "20";

        /**
         * The time in milliseconds before an entry in the store cache will be
         * cleared from the backing {@link HardReferenceQueue} (default
         * {@value #DEFAULT_STORE_CACHE_TIMEOUT}). This property controls how
         * long the store cache will retain an {@link IRawStore} which has not
         * been recently used. This is in contrast to the cache capacity.
         */
        String STORE_CACHE_TIMEOUT = StoreManager.class.getName()
                + ".storeCacheTimeout";

        String DEFAULT_STORE_CACHE_TIMEOUT = "" + (60 * 1000); // One minute.

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
        String IGNORE_BAD_FILES = StoreManager.class.getName()+".ignoreBadFiles";

        String DEFAULT_IGNORE_BAD_FILES = "false";
        
        /**
         * Option may be used to disable the purge of old resources during
         * startup.
         */
        String PURGE_OLD_RESOURCES_DURING_STARTUP = StoreManager.class.getName()
                + ".purgeOldResourcesDuringStartup";

        String DEFAULT_PURGE_OLD_RESOURCES_DURING_STARTUP = "true";
        
        /**
         * Option specifies the #of bytes under management below which we will
         * accelerate the overflow of the live journal by reducing its maximum
         * extent below the nominal configured maximum extent. The purpose of
         * this option is to promote rapid overflow of a new data service (where
         * new is measured by the #of bytes under management). This helps to
         * increase the rate at which index partitions are split (and moved if
         * the there is more than one new data service starting). When ZERO (0)
         * the feature is disabled.
         */
        String ACCELERATE_OVERFLOW_THRESHOLD = StoreManager.class.getName()
                + ".accelerateOverflowThreshold";

        String DEFAULT_ACCELERATE_OVERFLOW_THRESHOLD = ""+(Bytes.gigabyte);
        
    }

    /**
     * Performance counters for the {@link StoreManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IStoreManagerCounters {
       
        /**
         * The configured data directory.
         */
        String DataDir = "DataDir";

        /**
         * The configured tmp directory.
         */
        String TmpDir = "TmpDir";

        /**
         * <code>true</code> iff {@link StoreManager#isOpen()}
         */
        String IsOpen = "isOpen";
        
        /**
         * <code>true</code> iff {@link StoreManager#isStarting()}
         */
        String IsStarting = "isStarting";
        
        /**
         * <code>true</code> iff {@link StoreManager#isRunning()}
         */
        String IsRunning = "isRunning";

        String StoreCacheCapacity = "Store Cache Capacity";

        String StoreCacheSize = "Store Cache Size";

        /**
         * #of journals currently under management.
         */
        String ManagedJournalCount = "Managed Journal Count";

        /**
         * #of index segments currently under management."
         */
        String ManagedSegmentStoreCount = "Managed Segment Store Count";

        String JournalReopenCount = "Journal (Re-)open Count";

        String SegmentStoreReopenCount = "Segment Store (Re-)open Count";

        /**
         * #of journals which have been deleted.
         */
        String JournalDeleteCount = "Journal Delete Count";

        /**
         * #of index segments which have been deleted.
         */
        String SegmentStoreDeleteCount = "Segment Store Delete Count";

        /**
         * The #of bytes currently under management by the {@link StoreManager}.
         */
        String BytesUnderManagement = "Bytes Under Management";

        /**
         * The #of bytes in journals currently under management by the
         * {@link StoreManager}.
         */
        String JournalBytesUnderManagement = "Journal Bytes Under Management";

        /**
         * The #of bytes in index segments currently under management by the
         * {@link StoreManager}.
         */
        String SegmentBytesUnderManagement = "Segment Bytes Under Management";

        /**
         * The #of bytes in resources that have been deleted by the
         * {@link StoreManager} after they became release free.
         */
        String BytesDeleted = "Bytes Deleted";

        /**
         * The #of bytes available on the disk volume on which the data
         * directory is located.
         */
        String DataDirBytesAvailable = "Data Volume Bytes Available";

        /**
         * The #of bytes available on the disk volume on which the temporary
         * directory is located.
         */
        String TmpDirBytesAvailable = "Temp Volume Bytes Available";

        /**
         * The maximum extent of any journal managed by this service as of the
         * time when it was closed out by synchronous overflow processing.
         */
        String MaximumJournalSizeAtOverflow = "Maximum Journal Size At Overflow";

        /**
         * The elapsed milliseconds to date required to purge old resources from
         * the file system.
         * 
         * @see StoreManager#purgeOldResources()
         */
        String PurgeResourcesMillis = "Purge Resources Millis";

        /**
         * The current release time for the {@link StoreManager}.
         * 
         * @see StoreManager#getReleaseTime()
         */
        String ReleaseTime = "Release Time";

        /**
         * The timestamp associated with the last synchronous overflow event.
         */
        String LastOverflowTime = "Last Overflow Time";

        /**
         * The most recent commit time preserved when resources were last purged
         * from the {@link StoreManger}.
         * 
         * @see StoreManager#purgeResources
         */
        String LastCommitTimePreserved = "Last Commit Time Preserved";

        /**
         * The most recent commit time.
         */
        String LastCommitTime = "Last Commit Time";
       
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
     * The performance counters for the {@link IBufferStrategy} backing the live
     * journal and any historical journals which are concurrently open with the
     * live journal. A single instance of this object is used, and a hard
     * reference to that instance is held here, so that we can track the
     * cumulative performance counters across the live cycles of all journal
     * instances used by the data service over time. The performance counters
     * are not themselves persistent and do not survive a restart of the
     * {@link StoreManager}.
     */
    private final StoreCounters storeCounters = new StoreCounters();

    /**
     * The performance counters for the {@link IBufferStrategy} backing the live
     * journal and any historical journals which are concurrently open with the
     * live journal. A single instance of this object is used, and a hard
     * reference to that instance is held here, so that we can track the
     * cumulative performance counters across the live cycles of all journal
     * instances used by the data service over time. The performance counters
     * are not themselves persistent and do not survive a restart of the
     * {@link StoreManager}.
     */
    public final StoreCounters getStoreCounters() {
        
        return storeCounters;
        
    }
    
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
     * 
     * @todo Is this strictly necessary? Do we have all the necessary
     *       information in the journals?  Review the logic and decide.
     */
    final private IndexSegmentIndex segmentIndex;

    /**
     * A non-thread-safe collection of {@link UUID}s for {@link IndexSegment}s
     * which have been newly built but not yet incorporated in a re-start safe
     * manner into an index partition view. {@link UUID}s in this collection
     * are excluded from release by {@link #purgeOldResources()}.
     * 
     * @see #purgeOldResources()
     * @see IndexManager#buildIndexSegment(String,
     *      com.bigdata.btree.ILocalBTreeView, boolean, long, byte[], byte[],
     *      Event)
     */
    final private Set<UUID> retentionSet = new HashSet<UUID>();
    
    /**
     * Add an {@link IndexSegment} to the set of {@link IndexSegment}s which
     * have been generated but not yet incorporated into an index partition view
     * and hence we must take special cautions to prevent their release.
     * 
     * @param The {@link UUID} of the {@link IndexSegmentStore}.
     * 
     * @see #retentionSetRemove(UUID)
     * @see #retentionSet
     */
    protected void retentionSetAdd(final UUID uuid) {

        if (uuid == null)
            throw new IllegalArgumentException();

        synchronized (retentionSet) {

            if (!retentionSet.add(uuid)) {

                // that UUID is already in this collection.
                throw new IllegalStateException("Already in set: " + uuid);

            }
            
        }

    }
 
    /**
     * Remove an {@link IndexSegment} from the {@link #retentionSet}. DO NOT
     * invoke this until the {@link IndexSegment} has been incorporated in a
     * restart safe manner into an index partition view (that is, post-commit
     * rather than during the task that incorporates it into the view) or is
     * known to be no longer required (post MOVE, task failed, etc).
     * 
     * @param uuid
     *            The {@link UUID} of the {@link IndexSegmentStore}.
     * 
     * @see #retentionSetAdd(UUID)
     * @see #retentionSet
     */
    protected void retentionSetRemove(final UUID uuid) {

        if (uuid == null)
            throw new IllegalArgumentException();

        synchronized (retentionSet) {

            if (!retentionSet.remove(uuid)) {

                /*
                 * Note: Only a warning since invoked during error handling when
                 * the resource might have not made it into the retentionSet in
                 * the first place.
                 */

                log.warn("Not in retentionSet: " + uuid);

            }

        }
        
    }
    
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
     * @see Options#STORE_CACHE_CAPACITY
     * @see Options#STORE_CACHE_TIMEOUT
     */
//    final protected WeakValueCache<UUID, IRawStore> storeCache;
    final protected ConcurrentWeakValueCacheWithTimeout<UUID, IRawStore> storeCache;

    /**
     * Provides locks on a per-{resourceUUID} basis for higher concurrency.
     */
    private final transient NamedLock<UUID> namedLock = new NamedLock<UUID>();
    
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

//    /**
//     * A direct {@link ByteBuffer} that will be used as the write cache for the
//     * live journal and which will be handed off from live journal to live
//     * journal during overflow processing which is allocated iff
//     * {@link BufferMode#Disk} is chosen.
//     * <p>
//     * Note: This design is motivated by by JVM bug <a
//     * href="http://bugs.sun.com/bugdatabase/view_bug.do;jsessionid=8fab76d1d4479fffffffffa5abfb09c719a30?bug_id=6210541">
//     * 6210541</a> which describes a failure by
//     * <code>releaseTemporaryDirectBuffer()</code> to release temporary direct
//     * {@link ByteBuffer}s that are allocated for channel IO.
//     * 
//     * @see com.bigdata.journal.Options#WRITE_CACHE_CAPACITY
//     * @see DiskOnlyStrategy
//     */
//    private ByteBuffer writeCache;

    /**
     * A atomic hard reference to the live journal.
     */
    final protected AtomicReference<ManagedJournal> liveJournalRef = new AtomicReference<ManagedJournal>(null);

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

    protected ResourceService resourceService;
    
    /**
     * The port at which you can connect to the {@link ResourceService}. This
     * service provides remote access to resources hosted by the owning
     * {@link DataService}. This is used for moving resources to other data
     * services in the federation, including supporting service failover.
     * 
     * @return The port used to connect to that service.
     * 
     * @todo this could also be used for remote backup. however, note that you
     *       can not read the live journal using this object.
     */
    public int getResourceServicePort() {
        
        assertRunning();
        
        return resourceService.port;
        
    }
    
    /**
     * @see Options#IGNORE_BAD_FILES
     */
    private final boolean ignoreBadFiles;
    
    /**
     * @see Options#PURGE_OLD_RESOURCES_DURING_STARTUP
     */
    private final boolean purgeOldResourcesDuringStartup;

    /**
     * @see Options#ACCELERATE_OVERFLOW_THRESHOLD
     */
    protected final long accelerateOverflowThreshold;
    
    /**
     * Used to run the {@link Startup}.  @todo defer to init() outside of ctor.  Also, defer {@link Startup} until init() outside of ctor.
     */
    private final ExecutorService startupService = Executors
            .newSingleThreadExecutor(new DaemonThreadFactory
                    (getClass().getName()+".startupService"));

    /**
     * Succeeds if the {@link StoreManager} {@link #isOpen()} and is NOT
     * {@link #isStarting()} (the test itself is NOT atomic).
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

                if (log.isInfoEnabled())
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
    private long releaseTime = 0L;

    /**
     * The elapsed #of milliseconds in {@link #purgeOldResources()}
     */
    protected long purgeResourcesMillis = 0L;
    
    /**
     * The last value computed by {@link #getEffectiveReleaseTime()} and ZERO(0)
     * until a value has been calculated.
     */
    protected long lastCommitTimePreserved = 0L;

    /**
     * The last commit time corresponding to the last synchronous overflow event
     * and ZERO (0L) until there has been a synchronous overflow event.
     */
    protected long lastOverflowTime = 0L;

    /**
     * The observed maximum size of a journal (its length in bytes) as measured
     * at each synchronous overflow event.
     */
    protected long maximumJournalSizeAtOverflow = 0L;
    
    /**
     * The #of {@link ManagedJournal}s that have been (re-)opened to date.
     */
    final protected AtomicLong journalReopenCount = new AtomicLong();

    /**
     * The #of {@link IndexSegmentStore}s that have been (re-)opened to date.
     */
    final protected AtomicLong segmentStoreReopenCount = new AtomicLong();
    
    /**
     * The #of {@link ManagedJournal}s that have been deleted to date.
     */
    final protected AtomicLong journalDeleteCount = new AtomicLong();
    
    /**
     * The #of {@link IndexSegmentStore}s that have been deleted to date.
     */
    final protected AtomicLong segmentStoreDeleteCount = new AtomicLong();
    
    /**
     * The #of bytes currently under management EXCEPT those on the live
     * journal. This is incremented each time a new resource is added using
     * {@link #addResource(IResourceMetadata, File)} and decremented each
     * time a resource is deleted.
     */
    final protected AtomicLong bytesUnderManagement = new AtomicLong();
    final protected AtomicLong journalBytesUnderManagement = new AtomicLong();
    final protected AtomicLong segmentBytesUnderManagement = new AtomicLong();
    
    /**
     * The #of bytes that have been deleted since startup.
     */
    final protected AtomicLong bytesDeleted = new AtomicLong();
    
    /**
     * The #of bytes currently under management, including those written on the
     * live journal.
     * 
     * @throws IllegalStateException
     *             during startup or if the {@link StoreManager} is closed.
     */
    public long getBytesUnderManagement() {
        
        assertRunning();
        
        return bytesUnderManagement.get()
                + getLiveJournal().getBufferStrategy().getExtent();
        
    }
    
    /**
     * The #of bytes in {@link ManagedJournal}s, including those written on the
     * live journal.
     * 
     * @throws IllegalStateException
     *             during startup or if the {@link StoreManager} is closed.
     */
    public long getJournalBytesUnderManagement() {

        assertRunning();
        
        return journalBytesUnderManagement.get()
                + getLiveJournal().getBufferStrategy().getExtent();
        
    }
    
    /**
     * The #of bytes in managed {@link IndexSegmentStore}s.
     * 
     * @throws IllegalStateException
     *             during startup or if the {@link StoreManager} is closed.
     */
    public long getSegmentBytesUnderManagement() {
        
        assertRunning();
        
        return segmentBytesUnderManagement.get();
        
    }
    
    /**
     * The #of bytes of free space remaining on the volume hosting the
     * {@link #dataDir}.
     * 
     * @return The #of bytes of free space remaining -or- <code>-1L</code> if
     *         the free space could not be determined.
     */
    public long getDataDirFreeSpace() {

        return getFreeSpace(dataDir);
        
    }
    
    /**
     * The #of bytes of free space remaining on the volume hosting the
     * {@link #tmpDir}.
     * 
     * @return The #of bytes of free space remaining -or- <code>-1L</code> if
     *         the free space could not be determined.
     */
    public long getTempDirFreeSpace() {
        
        return getFreeSpace(tmpDir);
        
    }

    /**
     * Return the free space in bytes on the volume hosting some directory.
     * 
     * @param dir
     *            A directory hosted on some volume.
     * 
     * @return The #of bytes of free space remaining for the volume hosting the
     *         directory -or- <code>-1L</code> if the free space could not be
     *         determined.
     */
    /*
     * Note: This was written using Apache FileSystemUtil originally. That would
     * shell out "df" under un*x. Unfortunately, shelling out a child process
     * requires a commitment from the OS to support a process with as much
     * process space as the parent. For the data service, that is a lot of RAM.
     * In general, the O/S allows "over committment" of the available swap
     * space, but you can run out of swap and then you have a problem. If the
     * host was configured with scanty swap, then this problem could be
     * triggered very easily and would show up as "Could not allocate memory".
     * 
     * See http://forums.sun.com/thread.jspa?messageID=9834041#9834041
     */
    private long getFreeSpace(final File dir) {
        
        try {

            if(!dir.exists()) {
                
                return -1;
                
            }

            /*
             * Note: This return 0L if there is no free space or if the File
             * does not "name" a partition in the file system semantics. That
             * is why we check dir.exists() above.
             */

            return dir.getUsableSpace();
            
        } catch(Throwable t) {
            
            log.error("Could not get free space: dir=" + dir + " : "
                            + t, t);
            
            // the error is logger and ignored.
            return -1L;
            
        }

    }

//    /**
//     * Return the free space in bytes on the volume hosting some directory.
//     * <p>
//     * Note: This uses the apache IO commons {@link FileSystemUtils} to report
//     * the free space on the volume hosting the directory and then converts kb
//     * to bytes.
//     * 
//     * @param dir
//     *            A directory hosted on some volume.
//     * 
//     * @return The #of bytes of free space remaining for the volume hosting the
//     *         directory -or- <code>-1L</code> if the free space could not be
//     *         determined.
//     * 
//     * @see http://commons.apache.org/io/api-release/org/apache/commons/io/FileSystemUtils.html
//     */
//    private long getFreeSpace(final File dir) {
//        
//        try {
//
//            return FileSystemUtils.freeSpaceKb(dir.toString())
//                    * Bytes.kilobyte;
//            
//        } catch(Throwable t) {
//            
//            log.error("Could not get free space: dir=" + dir + " : "
//                            + t, t);
//            
//            // the error is logger and ignored.
//            return -1L;
//            
//        }
//
//    }
    
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
    protected StoreManager(final Properties properties) {

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties;

        // ignoreBadFiles
        {

            ignoreBadFiles = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.IGNORE_BAD_FILES,
                            Options.DEFAULT_IGNORE_BAD_FILES));

            if (log.isInfoEnabled())
                log.info(Options.IGNORE_BAD_FILES + "=" + ignoreBadFiles);

        }

        // purgeOldResourcesDuringStartup
        {

            purgeOldResourcesDuringStartup = Boolean
                    .parseBoolean(properties.getProperty(
                            Options.PURGE_OLD_RESOURCES_DURING_STARTUP,
                            Options.DEFAULT_PURGE_OLD_RESOURCES_DURING_STARTUP));

            if (log.isInfoEnabled())
                log.info(Options.PURGE_OLD_RESOURCES_DURING_STARTUP + "="
                        + purgeOldResourcesDuringStartup);

        }


        // accelerateOverflowThreshold
        {

            accelerateOverflowThreshold = Long.parseLong(properties
                    .getProperty(Options.ACCELERATE_OVERFLOW_THRESHOLD,
                            Options.DEFAULT_ACCELERATE_OVERFLOW_THRESHOLD));

            if (log.isInfoEnabled())
                log.info(Options.ACCELERATE_OVERFLOW_THRESHOLD + "="
                        + accelerateOverflowThreshold);

            if (accelerateOverflowThreshold < 0) {

                throw new RuntimeException(
                        Options.ACCELERATE_OVERFLOW_THRESHOLD
                                + " must be non-negative");

            }
            
        }
        
        /*
         * storeCacheCapacity
         */
        {

            final int storeCacheCapacity = Integer.parseInt(properties
                    .getProperty(Options.STORE_CACHE_CAPACITY,
                            Options.DEFAULT_STORE_CACHE_CAPACITY));

            if (log.isInfoEnabled())
                log.info(Options.STORE_CACHE_CAPACITY + "="
                        + storeCacheCapacity);

            if (storeCacheCapacity <= 0)
                throw new RuntimeException(Options.STORE_CACHE_CAPACITY
                        + " must be positive");

            final long storeCacheTimeout = Long.parseLong(properties
                    .getProperty(Options.STORE_CACHE_TIMEOUT,
                            Options.DEFAULT_STORE_CACHE_TIMEOUT));

            if (log.isInfoEnabled())
                log.info(Options.STORE_CACHE_TIMEOUT + "=" + storeCacheTimeout); 

            if (storeCacheTimeout < 0)
                throw new RuntimeException(Options.STORE_CACHE_TIMEOUT
                        + " must be non-negative");
            
            storeCache = new ConcurrentWeakValueCacheWithTimeout<UUID, IRawStore>(
                    storeCacheCapacity, TimeUnit.MILLISECONDS
                            .toNanos(storeCacheTimeout));
            
//            storeCache = new WeakValueCache<UUID, IRawStore>(
//                    new LRUCache<UUID, IRawStore>(storeCacheCapacity));

        }
        
//        /*
//         * Allocate an optional write cache that will be passed from live
//         * journal to live journal during overflow.
//         */
//        {
//
//            writeCache = AbstractJournal.getWriteCache(properties);
//
//        }

        /*
         * Create the _transient_ index in which we will store the mapping from
         * the commit times of the journals to their resource descriptions.
         */
        journalIndex = JournalIndex.createTransient();//tmpStore);
        segmentIndex = IndexSegmentIndex.createTransient();//(tmpStore);

        if (log.isInfoEnabled())
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

                if (log.isInfoEnabled())
                    log.info(Options.DATA_DIR + "=" + dataDir);

                journalsDir = new File(dataDir, "journals").getCanonicalFile();

                segmentsDir = new File(dataDir, "segments").getCanonicalFile();

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            if (!dataDir.exists()) {

                if (log.isInfoEnabled())
                    log.info("Creating: " + dataDir);

                if (!dataDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + dataDir.getAbsolutePath());

                }

            }

            if (!journalsDir.exists()) {

                if(log.isInfoEnabled())
                    log.info("Creating: " + journalsDir);

                if (!journalsDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + journalsDir.getAbsolutePath());

                }

            }

            if (!segmentsDir.exists()) {

                if(log.isInfoEnabled())
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

            if(log.isInfoEnabled())
                log.info(Options.TMP_DIR + "=" + tmpDir);

            if (!tmpDir.exists()) {

                if(log.isInfoEnabled())
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

                    // successful startup
                    starting.set(false);
                    
                    // Purge any resources that we no longer require.
                    if(purgeOldResourcesDuringStartup)
                        purgeOldResources();
                    
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
                 * Whether or not startup was successful, we make sure that this
                 * flag is turned off.
                 */

                starting.set(false);

                if (log.isInfoEnabled())
                    log.info("Startup "
                            + (isOpen() ? "successful" : "failed")
                            + " : "
                            + (isTransient ? "transient" : Options.DATA_DIR
                                    + "=" + dataDir));

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
         *             if the {@link IConcurrencyManager} has not been set
         *             (after a timeout).
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
			{
				int nwaits = 0;
				while (true) {
					try {
						getConcurrencyManager();
						break;
					} catch (IllegalStateException ex) {
						Thread.sleep(100/* ms */);
						if (++nwaits % 50 == 0)
							log.warn("Waiting for concurrency manager");
					}
				}
            }

			try {
				final IBigdataFederation<?> fed = getFederation();
				if (fed == null) {
					/*
					 * Some of the unit tests do not start the txs until after
					 * the DataService. For those unit tests getFederation()
					 * will return null during startup() of the DataService. To
					 * have a common code path, we throw the exception here
					 * which is caught below.
					 */
					throw new UnsupportedOperationException();
				}
				while (true) {
					if (fed.getTransactionService() != null) {
						break;
					}
					log.warn("Waiting for transaction service discovery");
				}
			} catch (UnsupportedOperationException ex) {
				log.warn("Federation not available - running in test case?");
			}

			/*
			 * Look for pre-existing data files.
			 */
            if (!isTransient) {

                if (log.isInfoEnabled())
                    log.info("Starting scan of data directory: " + dataDir);

                final Stats stats = new Stats();

                scanDataDirectory(dataDir, stats);

                final int nbad = stats.badFiles.size();

                if(log.isInfoEnabled())
                    log.info("Scan results: " + stats);

                if (!stats.badFiles.isEmpty()) {

                    if (ignoreBadFiles) {

                        log.warn("The following "
                                        + nbad
                                        + " file(s) had problems and are being ignored: "
                                        + stats.badFiles);

                    } else {

                        /*
                         * Note: This exception will be thrown if we could not
                         * get a lock on a journal file (see FileMetadata - the
                         * lock error is not reported until we try to read the
                         * magic field) or if there is a problem with the data
                         * in the file. You have to examine the stack trace to
                         * see what the root cause is.
                         */
                        
                        final String msg = "Could not open " + nbad
                                + " files - will not start : problem files="
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
             * Open the live journal.
             */
            openLiveJournal();

//            /*
//             * Purge any index partition moves which did not complete before
//             * shutdown.
//             */
//            purgeIncompleteMoves();
            
            /*
             * Notify the transaction service of the last commit time for the
             * live journal for this data service. This will be zero (0L) iff
             * this is a new journal on a new data service.
             * 
             * Note: This notification is not required unless the commit time
             * log for the transaction service is lost. In that case it provides
             * a backup allowing new transactions to read from the last global
             * commit point (once all data services have joined).
             */

            final long lastCommitTime = liveJournalRef.get()
                    .getLastCommitTime();

            if (lastCommitTime != 0L) {

                getConcurrencyManager().getTransactionManager().notifyCommit(
                        lastCommitTime);

            }
            

            try {

                resourceService = new ResourceService() {

                    @Override
                    protected File getResource(UUID uuid) throws Exception {

                        if (!isRunning()) {

                            throw new Exception("Not running.");

                        }

                        return resourceFiles.get(uuid);

                    }

                };

            } catch (IOException ex) {

                throw new RuntimeException("Could not start: "
                        + resourceService, ex);

            }

        }

        /**
         * Open the "live" journal.
         */
        private void openLiveJournal() throws InterruptedException {

            if (log.isInfoEnabled())
                log.info("Creating/opening the live journal: dataDir="
                        + dataDir);

            if (Thread.interrupted())
                throw new InterruptedException();

            final Properties p = getProperties();
            final File file;
            final boolean newJournal;

            if (journalIndex.getEntryCount() == 0) {

                /*
                 * There are no existing journal files. Create new journal using
                 * a unique filename in the appropriate subdirectory of the data
                 * directory.  Since the file is empty, it will be initialized 
                 * as a new Journal.
                 */

                if (log.isInfoEnabled())
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

                }

                /*
                 * Set the createTime on the new journal resource.
                 */
                p.setProperty(Options.CREATE_TIME, Long
                        .toString(nextTimestamp()));

                overrideJournalExtent(p);
                
                newJournal = true;

            } else {

                /*
                 * There is at least one pre-existing journal file, so we open
                 * the one with the largest timestamp - this will be the most
                 * current journal and the one that will receive writes until it
                 * overflows.
                 */

                // resource metadata for journal with the largest
                // timestamp.
                final IResourceMetadata resource = journalIndex
                        .find(Long.MAX_VALUE);

                if (log.isInfoEnabled())
                    log.info("Will open as live journal: " + resource);

                assert resource != null : "No resource? : timestamp="
                        + Long.MAX_VALUE;

                // lookup absolute file for that resource.
                file = resourceFiles.get(resource.getUUID());

                if (file == null) {

                    throw new NoSuchStoreException(resource.getUUID());

                }

                if (log.isInfoEnabled())
                    log.info("Opening most recent journal: " + file
                            + ", resource=" + resource);

                newJournal = false;

            }

            if (!isTransient) {

                assert file.isAbsolute() : "Path must be absolute: " + file;

                p.setProperty(Options.FILE, file.toString());

            }

            if (log.isInfoEnabled())
                log.info("Open/create of live journal: newJournal="
                        + newJournal + ", file=" + file);

            // Create/open journal.
            {

                if (Thread.interrupted())
                    throw new InterruptedException();

                final ManagedJournal tmp = new ManagedJournal(p);

                if (newJournal) {

                    // add to the set of managed resources.
                    addResource(tmp.getResourceMetadata(), tmp.getFile());

                }

                /*
                 * Add to set of open stores.
                 * 
                 * Note: single-threaded during startup.
                 */
                storeCache.put(tmp.getRootBlockView().getUUID(), tmp);
                // storeCache.put(tmp.getRootBlockView().getUUID(), tmp, false/*
                // dirty */);

                if (Thread.interrupted())
                    throw new InterruptedException();

                liveJournalRef.set(tmp);

                /*
                 * Subtract out the #of bytes in the live journal.
                 */

                final long extent = -tmp.getBufferStrategy().getExtent();
                
                bytesUnderManagement.addAndGet(extent);
                
                journalBytesUnderManagement.addAndGet(extent);

            }

        }
        
//        /**
//         * Purge any index partition moves which did not complete successfully
//         * on restart. These index partitions are identified by scanning the
//         * indices registered on the live journal. If an index has
//         * <code>sourcePartitionId != -1</code> in its
//         * {@link LocalPartitionMetadata} then the index was being moved onto
//         * this {@link IDataService} when the service was shutdown. The index
//         * (together with any {@link IndexSegment} resources that are identified
//         * in its {@link LocalPartitionMetadata}) is deleted.
//         * 
//         * @todo write a unit test for this feature.
//         * 
//         * @todo test MDS to verify that the index partition flagged as an
//         *       incomplete move is not registered as part of scale-out index?
//         * 
//         * @deprecated This is no longer necessary. The new MOVE does not use
//         *             {@link LocalPartitionMetadata#getSourcePartitionId()}
//         *             field. Index segments are cleaned up during a failed
//         *             receive. If the index segment for some reason is NOT
//         *             cleaned up, then it will be released eventually (unless
//         *             an immortal database is being used) since it will not be
//         *             incorporated into any index partition view.
//         */
//        private void purgeIncompleteMoves() {
//
//            final boolean reallyDelete = true;
//
//            final ManagedJournal liveJournal = liveJournalRef.get();
//            
//            // using read-committed view of Name2Addr
//            final ITupleIterator itr = liveJournal.getName2Addr()
//                    .rangeIterator();
//
//            // the list of indices that will be dropped.
//            final List<String> toDrop = new LinkedList<String>();
//            
//            while (itr.hasNext()) {
//
//                final ITuple tuple = itr.next();
//
//                final Entry entry = EntrySerializer.INSTANCE
//                        .deserialize(new DataInputBuffer(tuple.getValue()));
//
//                /*
//                 * Open the mutable btree on the journal (not the full view of
//                 * that index).
//                 */
//                final BTree btree = (BTree) liveJournal.getIndex(entry.checkpointAddr);
//                
//                final String name = btree.getIndexMetadata().getName();
//
//                final LocalPartitionMetadata pmd = btree.getIndexMetadata().getPartitionMetadata();
//
//                if (pmd != null) {
//
////                    System.err.println("\nname=" + name + "\npmd=" + pmd);
//
//                    if (pmd.getSourcePartitionId() != -1) {
//
//                        log.warn("Incomplete index partition move: name="
//                                + name + ", pmd=" + pmd);
//
//                        for (IResourceMetadata resource : pmd.getResources()) {
//
//                            if (resource.isIndexSegment()) {
//
//                                final File file = resourceFiles.get(resource.getUUID());
//                                
////                                final File file = new File(segmentsDir,
////                                        resource.getFile());
//
//                                log.warn("Deleting index segment: " + file);
//
//                                if (file.exists()) {
//
//                                    if (reallyDelete) {
//
//                                        deleteResource(resource.getUUID(),
//                                                false/* isJournal */);
//
//                                    }
//
//                                } else {
//
//                                    log.warn("Could not locate file: " + file);
//                                    
//                                }
//
//                            }
//
//                        }
//
//                    }
//
//                }
//
//                if (!toDrop.isEmpty() && reallyDelete) {
//
//                    for (String s : toDrop) {
//
//                        liveJournal.dropIndex(s);
//
//                    }
//
//                    liveJournal.commit();
//
//                }
//
//            }
//
//        } // purgeIncompleteMoves()
        
    } // class Startup

    /**
     * <code>true</code> initially and until {@link #start()} completes
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
    
//    /**
//     * Clears any stale entries in the LRU backing the {@link #storeCache}
//     */
//    public void clearStaleCacheEntries() {
//
//        storeCache.clearStaleRefs();
//        
//    }
    
    synchronized public void shutdown() {

        if (log.isInfoEnabled())
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

        if (resourceService != null) {
            resourceService.shutdown();
            resourceService = null;
        }

//        try {
//            tmpStore.destroy();
//        } catch (Exception ex) {
//            log.warn(ex.getMessage(), ex);
//        }

//        // release the write cache.
//        writeCache = null;
        
    }

    synchronized public void shutdownNow() {

        if(log.isInfoEnabled())
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

        if (resourceService != null) {
            resourceService.shutdownNow();
            resourceService = null;
        }

//        try {
//            tmpStore.destroy();
//        } catch (Exception ex) {
//            log.warn(ex.getMessage(), ex);
//        }

//        // release the write cache.
//        writeCache = null;

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

        if (log.isInfoEnabled())
            log.info("Scanning file: " + file + ", stats=" + stats);

        final IResourceMetadata resource;

        // name of the file.
        final String name = file.getName();

        // #of bytes in the file as reported by the OS.
        final long len = file.length();

        if (len > 0 && name.endsWith(Options.JNL)) {

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

                stats.nfiles++;
                
                stats.badFiles.add(file.getAbsolutePath());

                return;

            }

            try {

                resource = tmp.getResourceMetadata();

                stats.nfiles++;
                
                stats.njournals++;

                stats.nbytes += len;

            } finally {

                tmp.close();

            }

        } else if (len > 0 && name.endsWith(Options.SEG)) {

            /*
             * Attempt to open the index segment.
             */
            final IndexSegmentStore segStore;
            try {

                segStore = new IndexSegmentStore(file);

            } catch (Exception ex) {

                log.error("Problem opening segment: file="
                        + file.getAbsolutePath(), ex);

                stats.nfiles++;

                stats.badFiles.add(file.getAbsolutePath());

                return;

            }

            try {

                resource = segStore.getResourceMetadata();

                stats.nfiles++;

                stats.nsegments++;

                stats.nbytes += len;

            } finally {

                if(segStore.isOpen()) {

                    /*
                     * Note: opening the segment with [load == false] does not
                     * really open anything so you do not need to close the
                     * segment afterwards. I've put the conditional logic here
                     * just in case that changes.
                     */
                    segStore.close();
                    
                }

            }

        } else {

            if (len == 0L
                    && (name.endsWith(Options.JNL) || name
                            .endsWith(Options.SEG))) {

                log.warn("Ignoring empty file: " + file);

            } else {

                /*
                 * This file is not relevant to the resource manager.
                 */

                log.warn("Ignoring file: " + file);

            }

            return;

        }

        if (log.isInfoEnabled())
            log.info("Found " + resource + " in " + file);

//        if (!file.getName().equals(new File(resource.getFile()).getName())) {
//
//            /*
//             * The base name and extension of the file does not agree with that
//             * metadata reported by the store (unlikely since the store reports
//             * its metadata based on the file that it opened).
//             */
//
//            log.error("Wrong filename: actual=" + file + ", expected="
//                    + file);
//
//        }

//        addResource(resource, file.getAbsoluteFile());
        addResource(resource, file);

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
     * <p>
     * Note: This is invoked by {@link #shutdown()} and {@link #shutdownNow()}.
     */
    private void closeStores() {

//        final Iterator<IRawStore> itr = storeCache.iterator();

        final Iterator<WeakReference<IRawStore>> itr = storeCache.iterator();
        
        while (itr.hasNext()) {

//            final IRawStore store = itr.next();
            
            final IRawStore store = itr.next().get();

            if (store == null) {
                // weak reference has been cleared.
                continue;
            }
            
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
    synchronized public int getManagedJournalCount() {

        assertOpen();

        return journalIndex.getEntryCount();

    }

    /**
     * The #of index segments on hand.
     */
    synchronized public int getManagedSegmentCount() {

        assertOpen();

        return segmentIndex.getEntryCount();

    }

    /**
     * Notify the resource manager of a new resource. The resource is added to
     * {@link #resourceFiles} and to either {@link #journalIndex} or
     * {@link #segmentIndex} as appropriate. As a post-condition, you can use
     * {@link #openStore(UUID)} to open the resource using the {@link UUID}
     * specified by {@link IResourceMetadata#getUUID()}.
     * <p>
     * Note: This also adds the size of the store in bytes as reported by the OS
     * to {@link #bytesUnderManagement}.
     * <p>
     * Note: Adding a resource to the store manager has no persistent effect
     * other than the presumed presence of the specified file in the file
     * system. However, error handling routines SHOULD invoke
     * {@link #deleteResource(UUID, boolean)} in order to remove a resource that
     * was not built correctly or not incorporated into the view. Otherwise the
     * mapping from the {@link UUID} to the {@link File} will be maintained in
     * memory and the {@link StoreManager} will overreport the #of bytes under
     * management.
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
     *             if the {@link #journalIndex} or {@link #segmentIndex} already
     *             know about that resource.
     * @throws RuntimeException
     *             if {@link #openStore(UUID)} already knows about that
     *             resource.
     * @throws IllegalArgumentException
     *             if the <i>resourceMetadata</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if the <i>file</i> is <code>null</code> and
     *             {@link #isTransient} is <code>false</code>.
     * 
     * @see #deleteResource(UUID, boolean)
     * @see #retentionSetAdd(UUID)
     * @see #retentionSetRemove(UUID)
     */
    synchronized protected void addResource(
            final IResourceMetadata resourceMetadata,
            File file
            ) {

        if (resourceMetadata == null)
            throw new IllegalArgumentException();

        if (file == null && !isTransient)
            throw new IllegalArgumentException();

        assertOpen();

        final UUID uuid = resourceMetadata.getUUID();

        if (log.isInfoEnabled())
            log.info("file=" + file + ", uuid=" + uuid);

        if (file != null) {

            file = file.getAbsoluteFile();

        }
        
//        synchronized (storeCache) {
        
            if (storeCache.get(uuid) != null) {

                throw new RuntimeException("Resource already open?: "
                        + resourceMetadata);

            }
        
//        }

        final long extent;
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
            
            // size of the file.
            extent = file.length();

        } else {
            
            // transient resource - no extent.
            extent = 0L;
            
        }

        if (resourceMetadata.isJournal()) {

            journalIndex.add((JournalMetadata)resourceMetadata);
            
            journalBytesUnderManagement.addAndGet(extent);

        } else {

            segmentIndex.add((SegmentMetadata)resourceMetadata);

            segmentBytesUnderManagement.addAndGet(extent);
            
        }
        
        /*
         * Track the #of bytes under management.
         */
        bytesUnderManagement.addAndGet(extent);

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

    public abstract void setConcurrencyManager(IConcurrencyManager concurrencyManager);

    /**
     * The {@link ManagedJournal} provides the backing store used to absorb
     * writes and retain history for the scale-out architecture.
     * <p>
     * Note: This implementation is designed to use a shared
     * {@link ConcurrencyManager} across all open journal instances for a
     * {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    public class ManagedJournal extends AbstractJournal {

//        /**
//         * Note: Each instance of the {@link ManagedJournal} reuses the SAME
//         * {@link StoreManager#writeCache}. Therefore you MUST close out writes
//         * on the old journal BEFORE you may allocate a new journal.
//         * 
//         * @param properties
//         * 
//         * @see AbstractJournal#closeForWrites(long)
//         */
        protected ManagedJournal(final Properties properties) {

            super(properties);//, writeCache);

            /*
             * Set the performance counters on the new store so that we have a
             * cumulative track of all activity on both the "live" journals and
             * the "historical" journals managed by this data service.
             * 
             * FIXME Must also roll the counters forward for the other journal
             * buffer strategies! (The implementation class is different for the
             * WORMStrategy, which is causing complications right now.)
             */
            if (getBufferStrategy() instanceof DiskOnlyStrategy) {

                ((DiskOnlyStrategy) getBufferStrategy())
                        .setStoreCounters(getStoreCounters());

            } else if (getBufferStrategy() instanceof WORMStrategy) {

                ((WORMStrategy) getBufferStrategy())
                        .setStoreCounters(getStoreCounters());

            }
 
        }

        public String toString() {
            
            /*
             * Note: Should not depend on any state that might be unreachable,
             * e.g., because the store is not open, etc.
             */
            
            final IRootBlockView rootBlock = getRootBlockView();
            
            return getClass().getName()
                    + "{file="
                    + getFile()
                    + ", open="
                    + ManagedJournal.this.isOpen()
                    + (rootBlock != null ? ", uuid="
                            + getRootBlockView().getUUID() : "") + "}";
            
        }
        
        /**
         * Note: Exposed for the {@link DataService} which needs this for its
         * 2-phase commit protocol.
         */
        public long commitNow(final long commitTime) {
            
            return super.commitNow(commitTime);
            
        }
        
        /**
         * Exposed for {@link StoreManger#getResourcesForTimestamp(long)} which
         * requires access to the {@link CommitRecordIndex} for the
         * lastCommitTime on the historical journals.
         * <p>
         * Note: This always returns a distinct index object. The code relies on
         * this fact to avoid contention with the live {@link CommitRecordIndex}
         * for the live journal.
         */
        public CommitRecordIndex getCommitRecordIndex(final long addr) {
            
            return super.getCommitRecordIndex(addr);
            
        }

        public AbstractLocalTransactionManager getLocalTransactionManager() {

            return (AbstractLocalTransactionManager) getConcurrencyManager()
                    .getTransactionManager();

        }

//        public DataServiceTransactionManager getLocalTransactionManager() {
//
//            return (DataServiceTransactionManager) getConcurrencyManager()
//                    .getTransactionManager();
//
//        }

        public SparseRowStore getGlobalRowStore() {
            
            return getFederation().getGlobalRowStore();
            
        }
        
        public BigdataFileSystem getGlobalFileSystem() {
            
            return getFederation().getGlobalFileSystem();
            
        }
        
        public DefaultResourceLocator getResourceLocator() {
            
            return (DefaultResourceLocator) getFederation()
                    .getResourceLocator();
            
        }
        
        public ExecutorService getExecutorService() {
            
            return getFederation().getExecutorService();
            
        }
        
        public IResourceLockService getResourceLockService() {

            return getFederation().getResourceLockService();
            
        }

        public TemporaryStore getTempStore() {
            
            return getFederation().getTempStore();
            
        }

        /**
         * Extended to set the {@link IResourceMetadata} to this journal if it
         * is <code>null</code> since a remote caller can not have the correct
         * metadata on hand when they formulate the request.
         */
        protected void validateIndexMetadata(final String name,
                final IndexMetadata metadata) {

            super.validateIndexMetadata(name, metadata);
            
            final LocalPartitionMetadata pmd = metadata.getPartitionMetadata();

            if(pmd == null) {
                
                /*
                 * Note: This case permits unpartitioned indices for the MDS.
                 */
                return;

            }
            
            if (pmd.getResources() == null) {

                /*
                 * A [null] for the resources field is a specific indication
                 * that we need to specify the resource metadata for the live
                 * journal at the time that the index partition is registered.
                 * This indicator is used when the metadata service registers an
                 * index partition remotely on a data service since it does not
                 * (and can not) have access to the resource metadata for the
                 * live journal as of the time that the index partition actually
                 * gets registered on the data service.
                 * 
                 * The index partition split and join tasks do not have this
                 * problem since they are run locally. However, an index
                 * partition move operation also needs to do this.
                 */
                final ResourceManager resourceManager = ((ResourceManager) (StoreManager.this));

                metadata.setPartitionMetadata(//
                        new LocalPartitionMetadata(//
                                pmd.getPartitionId(),//
                                pmd.getSourcePartitionId(),//
                                pmd.getLeftSeparatorKey(),//
                                pmd.getRightSeparatorKey(),//
                                new IResourceMetadata[] {//
                                // The live journal.
                                getResourceMetadata() //
                                },
                                // cause
                                IndexPartitionCause.register(resourceManager)
//                                /*
//                                 * Note: Retains whatever history given by the
//                                 * caller.
//                                 */
//                                , pmd.getHistory() + "register(name=" + name
//                                        + ",partitionId="
//                                        + pmd.getPartitionId() + ") "
                        ));

            } else {

                if (pmd.getResources().length == 0) {

                    throw new RuntimeException(
                            "Missing resource description: name=" + name
                                    + ", pmd=" + pmd);

                }

                if (!pmd.getResources()[0].isJournal()) {

                    throw new RuntimeException(
                            "Expecting resources[0] to be journal: name="
                                    + name + ", pmd=" + pmd);

                }

                if (!pmd.getResources()[0].getUUID().equals(
                        getRootBlockView().getUUID())) {

                    throw new RuntimeException(
                            "Expecting resources[0] to be this journal but has wrong UUID: name="
                                    + name + ", pmd=" + pmd);

                }

            }

        }
        
    } // class ManagedJournal

    /**
     * The journal on which writes are made.
     * 
     * @throws IllegalStateException
     *             if the {@link StoreManager} is not open.
     * @throws IllegalStateException
     *             if the {@link StoreManager} is still starting up.
     */
    public ManagedJournal getLiveJournal() {
        
        assertRunning();

        final ManagedJournal tmp = liveJournalRef.get();
        
        assert tmp != null : "open=" + isOpen() + ", starting="
                + isStarting() + ", dataDir=" + dataDir;
        assert tmp.isOpen();

        /*
         * Note: There is a brief period when we close out writes on the live
         * journal before we cut over to the new live journal. Therefore this
         * assertion can not be made since it is violated during that brief
         * period.
         * 
         * Note: Concurrent readers are always allowed, even during that brief
         * period.
         */
//        assert !liveJournal.isReadOnly();

        return tmp;

    }

//    /**
//     * This lock is used to prevent asynchronous processes such as
//     * {@link ConcurrencyManager#getIndexCounters()} from acquiring the live
//     * journal during the period between when we close out the old journal
//     * against future writes and when the new live journal is in place.
//     * <p>
//     * Note: {@link AbstractJournal#closeForWrites(long)} does not disturb
//     * concurrent readers.
//     */
//    protected final ReentrantLock liveJournalLock = new ReentrantLock();
    
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
    public AbstractJournal getJournal(final long timestamp) {

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

            return getLiveJournal();

        }

        final IResourceMetadata resource;

        synchronized (journalIndex) {

            /*
             * @todo add a weak reference cache in front of this by timestamp?
             * (The MDI had a hotspot for a similar pattern of use, but I have
             * not verified yet whether there is such a hotspot here).
             */

            resource = journalIndex.find(Math.abs(timestamp));

        }

        if (resource == null) {

            if (log.isInfoEnabled())
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
     * @throws NoSuchStoreException
     *             if the {@link UUID} is not recognized.
     * @throws NoSuchStoreException
     *             if the resource for that {@link UUID} could not be found.
     * @throws RuntimeException
     *             if something else goes wrong.
     * 
     * @todo it seems that we always have the {@link IResourceMetadata} on hand
     *       when we need to (re-)open a store so it might be nice to pass that
     *       in as it would make for more informative error messages when
     *       something goes wrong (except that I was planning to drop the file
     *       name from that interface).
     */
    public IRawStore openStore(final UUID uuid) {

        assertRunning();

        if (uuid == null) {

            throw new IllegalArgumentException();

        }

        /*
         * Note: These operations can have modest latency, especially if we open
         * a fully buffered index segment. Therefore we use a per-store
         * (actually, per-resource UUID, which is the same thing) lock to avoid
         * imposing latency on threads requiring access to different stores.
         */
        final Lock lock = namedLock.acquireLock(uuid);

        try {

            /*
             * Check to see if the given resource is already open.
             */

            IRawStore store;
//            synchronized(storeCache) {
                
                store = storeCache.get(uuid);
                
//            }

            if (store != null) {

                if (!store.isOpen()) {

                    if (store instanceof IndexSegmentStore) {

                        /*
                         * We can simply re-open an index segment's store file.
                         */

//                        // Note: relative to the data directory!
//                        final File file = resourceFiles.get(uuid);
//
//                        if (file == null) {
//
//                            throw new NoSuchStoreException(uuid);
//
//                        }
//
//                        if (!file.exists()) {
//
//                            throw new RuntimeException(
//                                    "Resource file missing? uuid=" + uuid
//                                            + ", file=" + file);
//
//                        }

                        // re-open the store file. it will complain if the file is gone.
                        ((IndexSegmentStore) store).reopen();

                        // re-opening the store.
                        segmentStoreReopenCount.incrementAndGet();

                        // done.
                        return store;

                    } else {

                        /*
                         * Note: Journals should not be closed without also
                         * removing them from the list of open resources. The
                         * live journal SHOULD NOT be closed except during
                         * shutdown or overflow (when it is replaced by a new
                         * live journal).
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
                    
                    /*
                     * Note: Non-transactional read-historical operations DO NOT
                     * declare read locks and therefore are unable to prevent
                     * resources from being released, which can lead to this
                     * exception.
                     */
                    
                    throw new NoSuchStoreException(uuid);

                }

                if (!file.exists()) {

                    throw new NoSuchStoreException("Resource file missing? uuid="
                            + uuid + ", file=" + file);

                }

                final UUID actualUUID;

                if (file.getName().endsWith(Options.JNL)) {

                    /*
                     * Open a historical journal.
                     * 
                     * Note: The live journal is never opened by this code path.
                     * It is opened when the resource manager is instantiated
                     * and it will remain open except during shutdown and
                     * overflow (when it is replaced by a new live journal).
                     */

                    final Properties properties = getProperties();

                    properties.setProperty(Options.FILE, file.toString());

                    // All historical journals are read-only!
                    // Note: disables the write cache among other things.
                    properties.setProperty(Options.READ_ONLY, "true");

                    final AbstractJournal journal = new ManagedJournal(
                            properties);

                    final long closeTime = journal.getRootBlockView()
                            .getCloseTime();

                    // verify journal was closed for writes.
                    assert closeTime != 0 : "Journal not closed for writes? "
                            + " : file=" + file + ", uuid=" + uuid
                            + ", closeTime=" + closeTime;

                    assert journal.isReadOnly();

                    actualUUID = journal.getRootBlockView().getUUID();

                    store = journal;

                    // opened another journal.
                    journalReopenCount.incrementAndGet();
                    
                } else {

                    /*
                     * FIXME Make sure that the segStore either makes it into
                     * the cache or is closed even for spurious exceptions.
                     * E.g.,
                     * 
                     * try {segStore=...; store=segStore;} catch()
                     * {if(store!=null)store.close();}
                     * 
                     * But not it if was already open and not after it makes
                     * it into the cache.
                     */
                    final IndexSegmentStore segStore = new IndexSegmentStore(file);

                    actualUUID = segStore.getCheckpoint().segmentUUID;

                    store = segStore;

                    // opened another index segment store.
                    segmentStoreReopenCount.incrementAndGet();

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
//            synchronized(storeCache) {

                storeCache.put(uuid, store);//, false/* dirty */);
//                storeCache.put(uuid, store, false/* dirty */);
                
//            }

            // return the reference to the open store.
            return store;

        } finally {

            lock.unlock();

        }
        
    }

    /**
     * Report the next timestamp assigned by the {@link ITransactionService}.
     */
    protected long nextTimestamp() {

        final ILocalTransactionManager transactionManager = getConcurrencyManager()
                .getTransactionManager();

        return transactionManager.nextTimestamp();

    }
    
    public void deleteResources() {

        assertNotOpen();

        // NOP if transient.
        if (isTransient())
            return;

        if (log.isInfoEnabled())
            log.info("Deleting all resources: " + dataDir);

        recursiveDelete(dataDir);

        // approx. #of bytes deleted.
        bytesDeleted.addAndGet(bytesUnderManagement.get());
        
        // nothing left under management.
        bytesUnderManagement.set(0L);
        journalBytesUnderManagement.set(0L);
        segmentBytesUnderManagement.set(0L);
        
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
    private void recursiveDelete(final File f) {

        if (f.isDirectory()) {

            final File[] children = f.listFiles(newFileFilter());

            if (children == null) {

                // No such file or directory exists.
                return;
                
            }
            
            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);

            }

        }

        if (log.isInfoEnabled())
            log.info("Removing: " + f);

        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }

    }

    /**
     * Updates the {@link #releaseTime}.
     * <p>
     * Data services MAY release data for views whose timestamp is less than or
     * equal to the specified release time IFF that action would be in keeping
     * with their local history retention policy (minReleaseAge) AND if the data
     * is not required for the most current committed state (data for the most
     * current committed state is not releasable regardless of the release time
     * or the minReleaseAge).
     * 
     * @see #purgeOldResources(), which is responsible for actually deleting the
     *      old resources.
     */
    public void setReleaseTime(final long releaseTime) {

        assertOpen();

        if (releaseTime < 0L) {

            throw new IllegalArgumentException();
            
        }

        this.releaseTime = releaseTime;
        
    }

    /**
     * Return the last value set with {@link #setReleaseTime(long)}.
     */
    public long getReleaseTime() {

        return releaseTime;

    }

    /**
     * @see IndexManager#getIndexRetentionTime()
     */
    abstract protected long getIndexRetentionTime();
    
    /**
     * In order to have atomic semantics and prevent a read-historical operation
     * from starting concurrently that would have access to a view that is being
     * purged, {@link IndexManager#getIndex(String, long)} and
     * {@link StoreManager#purgeOldResources()} MUST contend for a shared lock.
     * This is a {@link ReentrantReadWriteLock} since concurrent getIndex()
     * requests can proceed as long as {@link StoreManager#purgeOldResources()}
     * is not running. Also note that contention is not required for
     * {@link ITx#UNISOLATED} index views.
     */
    protected final ReentrantReadWriteLock indexCacheLock = new ReentrantReadWriteLock();

    /**
     * Identify and delete resources no longer required by the index views from
     * the current releaseTime up to the lastCommitTime.
     * <p>
     * Note: The ability to read from a historical commit point requires the
     * existence of the journals back until the one covering that historical
     * commit point. This is because the distinct historical commit points for
     * the indices are ONLY defined on the journals. The index segments carry
     * forward the committed state of a specific index as of the commitTime of
     * the index from which the segment was built. This means that you can
     * substitute the index segment for the historical index state on older
     * journals, but the index segment carries forward only a single commit
     * point for the index so it can not be used to read from arbitrary
     * historical commit points.
     * <p>
     * The caller MUST hold the exclusive lock on the
     * {@link WriteExecutorService}.
     * 
     * @return A summary of the work done -or- <code>null</code> if the
     *         preconditions for the purge operation were not satisfied.
     * 
     * @see src/architecture/purgeResourceDecisionsMatrix.xls
     * 
     * @see #purgeOldResources(long, boolean)
     */
    final protected PurgeResult purgeOldResources() {

        final long beginPurgeTime = System.currentTimeMillis();
        
        /*
         * The last commit time on record in the live journal.
         * 
         * Note: This used to be invoked during synchronous overflow so the
         * [lastCommitTime] was in fact the last commit time on the OLD journal.
         * However, this is now invoked at arbitrary times (as long as there is
         * a lock on the write service) so we really need to use the
         * [lastOverflowTime] here to have the same semantics.
         */
        final long lastCommitTime = getLiveJournal().getRootBlockView().getLastCommitTime(); 

        if (lastCommitTime == 0L) {
            
            if (log.isInfoEnabled())
                log.info("Nothing committed yet.");
            
            return null;
            
        }

        /*
         * Make sure that we have the current release time. It is periodically
         * pushed by the transaction manager, but we pull it here since we are
         * about to make a decision based on the releaseTime concerning which
         * resources to release.
         */
        {

            final IBigdataFederation fed;
            try {

                fed = getFederation();

            } catch (UnsupportedOperationException ex) {

                log.warn("Federation not available: Running in test harness?");

                return null;

            }

            try {

                final ITransactionService txService = fed
                        .getTransactionService();

                if (txService != null) {

                    this.releaseTime = txService.getReleaseTime();

                } else {

                    log
                            .warn("Could not discover txService - Proceeding with current release time.");

                }

            } catch (IOException ex) {

                /*
                 * Since the releaseTime is monotonically increasing, if there
                 * is an RMI problem then we use the last release time that was
                 * pushed to us by the txService.
                 */

                log.warn("Proceeding with current release time: " + ex);

            }

        }

        if (this.releaseTime == 0L) {

            /*
             * Note: The [releaseTime] is advanced by the transaction service
             * when it decides that a commit point will no longer be reachable
             * by new transactions and no running transactions is reading from
             * that commit point.
             * 
             * Note: We do not release anything until the releaseTime has been
             * set by the transaction service. This centralizes decisions
             * concerning how long to preserve history while distributing the
             * actions taken based on those decisions.
             */

            log.warn("releaseTime not set.");

            return null;

        }

        // // debugging - writes out stores and indices in their respective
        // caches.
//        if(false) {// @todo remove code.
//            int nstores = 0, nindices = 0;
//            {
//                Iterator<WeakReference<IRawStore>> itr = storeCache.iterator();
//                while (itr.hasNext()) {
//                    IRawStore store = itr.next().get();
//                    if (store != null) {
//                        log.warn("Store: " + store);
//                        nstores++;
//                    }
//                }
//            }
//            {
//                Iterator<WeakReference<ILocalBTreeView>> itr2 = ((IndexManager) this).indexCache
//                        .iterator();
//                while (itr2.hasNext()) {
//                    IIndex ndx = itr2.next().get();
//                    if (ndx != null) {
//                        log.warn("Index: " + ndx);
//                        nindices++;
//                    }
//                }
//            }
//            log.warn("nstores=" + nstores + ", nindices=" + nindices);
//        }

        final Event e = new Event(getFederation(), new EventResource(),
                EventType.PurgeResources).start();
        
        /*
         * Prevent concurrent access to the index cache.
         */
        indexCacheLock.writeLock().lock();

        try {

            /*
             * The earliest timestamp that MUST be retained for the
             * read-historical indices in the cache.
             * 
             * FIXME There is a cycle here which makes it impossible to release
             * an index view sooner than the timeout on the index cache when the
             * index cache capacity is larger than the current minimum
             * requirements (review store cache and index segment as well).
             * 
             * The problem is that the backing hard reference queue for the
             * index cache does not distinguish between actively used indices
             * and those that are just being held open in case they might be
             * used against "soon" so we are not able to figure out which
             * indices can be closed and are therefore required to accept a
             * release time which is MUCH earlier than the release time given by
             * the transaction service.
             * 
             * There are a few ways to approach this. One is to use local
             * read-historical transactions for flyweight read-only operations.
             * That will give us a real measure of the #of operations reading on
             * any given timestamp [a fair amount of work and requires
             * duplicating many of the facilities of the distributed transaction
             * manager so that we can track the earliest local tx]. Another is
             * to reduce the index cache capacity and timeout and then use a
             * fully buffered journal so it does not matter as much if we close
             * out an index [a partial fix].
             */
            final long indexRetentionTime = getIndexRetentionTime();

            /*
             * Choose whichever timestamp would preserve more history (that is,
             * choose the earlier timestamp). Note that the index retention time
             * is -1 if there are no indices in the cache.
             */
            final long choosenReleaseTime = indexRetentionTime == -1L ? this.releaseTime
                    : Math.min(indexRetentionTime, this.releaseTime);

            // final long releaseTime = Math.min(indexRetentionTime, Math.min(
            // maxReleaseTime, this.releaseTime));

            /*
             * This is the age of the selected release time as computed from the
             * last commit time on the live journal.
             */
            final long releaseAge = (lastCommitTime - choosenReleaseTime); 
            
            if (log.isInfoEnabled())
                log.info("Choosen releaseTime=" + choosenReleaseTime
                        + ": given releaseTime=" + this.releaseTime
                        + ", indexRetentionTime=" + indexRetentionTime
                        + " (this is "
                        + TimeUnit.MILLISECONDS.toSeconds(releaseAge)
                        + " seconds before/after the lastCommitTime="+lastCommitTime+")");

            /*
             * The earliest commit time on record in any journal available to
             * the StoreManager.
             */
            final long firstCommitTime;
            {

                // the earliest journal available to the store manager.
                final IResourceMetadata resource = journalIndex.findNext(0L);

                // open that journal.
                final AbstractJournal j0 = (AbstractJournal) openStore(resource
                        .getUUID());

                // the first commit time on the earliest journal available.
                firstCommitTime = j0.getRootBlockView().getFirstCommitTime();

            }

            /*
             * Find the commitTime that we are going to preserve.
             */
            final long commitTimeToPreserve;
            if (choosenReleaseTime < firstCommitTime) {

                /*
                 * If the computed [releaseTime] is before the first commit
                 * record on the earliest available journal then there was
                 * nothing that could be deleted and we just return immediately.
                 */

                if (log.isInfoEnabled())
                    log.info("Release time is earlier than any commit time.");

                // Nothing to do.
                return null;

            } else if (choosenReleaseTime >= lastCommitTime) {

                /*
                 * If the computed [releaseTime] GTE the last commit point then
                 * we choose the [lastCommitTime] instead.
                 * 
                 * Note: If there have been no writes on this data service but
                 * there have been writes on other data services then the
                 * txService will eventually advance the releaseTime beyond the
                 * lastCommitTime on this data service. Since we never release
                 * the last commit point we set the commitTimeToPreserve to the
                 * lastCommitTime on the local data service.
                 */

                commitTimeToPreserve = lastCommitTime;

                if (log.isInfoEnabled())
                    log.info("commitTimeToPreserve := " + commitTimeToPreserve
                            + " (this is the lastCommitTime)");

            } else {

                /*
                 * Find the timestamp for the commit record that is strictly
                 * greater than the release time.
                 */

                commitTimeToPreserve = getCommitTimeStrictlyGreaterThan(choosenReleaseTime);

                if (log.isInfoEnabled())
                    log
                            .info("commitTimeToPreserve := "
                                    + commitTimeToPreserve
                                    + " (this is the first commitTime GT the releaseTime="
                                    + choosenReleaseTime + ")");

            }

            /*
             * Make a note for reporting purposes.
             */
            this.lastCommitTimePreserved = commitTimeToPreserve;

            /*
             * Find resources that were in use as of that commitTime.
             */
            final Set<UUID> resourcesInUse;
            final long elapsedScanCommitIndicesTime;
            {
                final long begin = System.currentTimeMillis();

                resourcesInUse = getResourcesForTimestamp(commitTimeToPreserve);

                synchronized(retentionSet) {

                    resourcesInUse.addAll(retentionSet);
                    
                }
                
                elapsedScanCommitIndicesTime = System.currentTimeMillis()
                        - begin;
            }
            if (log.isInfoEnabled()) {
                /* Log the in use resources (resources that MUST NOT be
                 * deleted).
                 */
                for (UUID uuid : resourcesInUse) {
                    log.info("In use: file=" + resourceFiles.get(uuid)
                            + ", uuid=" + uuid);
                }
            }

            final int journalBeforeCount = getManagedJournalCount();
            final int segmentBeforeCount = getManagedSegmentCount();
            final long bytesBeforeCount = getBytesUnderManagement();

            /*
             * Delete anything that is: ( NOT in use )
             * 
             * AND ( createTime < commitTimeToPreserve )
             */
            final long elapsedDeleteResourcesTime;
            {
                final long begin = System.currentTimeMillis();

                deleteUnusedResources(commitTimeToPreserve, resourcesInUse);
                
                elapsedDeleteResourcesTime = System.currentTimeMillis() - begin;
            }

            final int journalAfterCount = getManagedJournalCount();
            final int segmentAfterCount = getManagedSegmentCount();
            final long bytesAfterCount = getBytesUnderManagement();
            
            final long elapsedPurgeResourcesTime = System.currentTimeMillis() - beginPurgeTime;
            
            purgeResourcesMillis += elapsedPurgeResourcesTime;
            
            final PurgeResult result = new PurgeResult(firstCommitTime, lastCommitTime,
                    this.releaseTime, indexRetentionTime, choosenReleaseTime,
                    commitTimeToPreserve, resourcesInUse.size(),
                    journalBeforeCount, journalAfterCount, segmentBeforeCount,
                    segmentAfterCount, bytesBeforeCount, bytesAfterCount,
                    elapsedScanCommitIndicesTime, elapsedDeleteResourcesTime,
                    elapsedPurgeResourcesTime);

            e.addDetails(result.getParams());
            
            return result;
            
        } finally {

            indexCacheLock.writeLock().unlock();

            e.end();
            
        }

    }
    
    /**
     * Delete unused resources given a set of resources that are still in use.
     * The unused resources are identified by scanning the {@link #journalIndex}
     * and the {@link #segmentIndex}. For each resource found in either of
     * those indices which is NOT found in <i>resourcesInUse</i> and whose
     * createTime is GTE the specified timestamp, we take the following steps:
     * <ol>
     * <li>close iff open</li>
     * <li>remove from lists of known resources</li>
     * <li>clear the associated {@link ILRUCache}</li>
     * <li>delete in the file system</li>
     * </ol>
     * Note: {@link IndexSegment}s pose a special case. Their create time is
     * the timestamp associated with their source view. During asynchronous
     * overflow processing we generate {@link IndexSegment}s from the
     * lastCommitTime of the old journal. Therefore their createTime timestamp
     * is often LT the <i>commitTimeToPreserve</i>. In order to prevent these
     * {@link IndexSegment}s from being released before they are put to use (by
     * incorporating them into an index partition view) we DO NOT add them to
     * the {@link #segmentIndex} until they are part of an index partition view.
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
     * 
     * @see IndexManager#buildIndexSegment(String,
     *      com.bigdata.btree.ILocalBTreeView, boolean, long, byte[], byte[],
     *      Event)
     */
    private void deleteUnusedResources(final long commitTimeToPreserve,
            final Set<UUID> resourcesInUse) {
        
        /*
         * Delete old journals.
         */
        
        // #of journals deleted.
        int njournals = 0;
        {

            /*
             * Note: This iterator supports traversal with concurrent
             * modification (by a single thread). If we decide to delete a
             * journal resource, then deleteResource() will be tasked to delete
             * it from the [journalIndex] as well.
             */
            final ITupleIterator itr = journalIndex.rangeIterator(
                    null/* fromKey */, null/* toKey */, 0/* capacity */,
                    IRangeQuery.DEFAULT | IRangeQuery.CURSOR, null/*filter*/);

            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final IResourceMetadata resourceMetadata = (IResourceMetadata) SerializerUtil
                        .deserialize(tuple.getValue());

                // the create timestamp for that resource.
                final long createTime = resourceMetadata.getCreateTime();

                if (createTime >= commitTimeToPreserve) {

                    /*
                     * Do NOT delete any resources whose createTime is GTE the
                     * given commit time.
                     */

                    if (log.isInfoEnabled())
                        log
                                .info("Stopping at resource GTE commitTime to preserve: createTime="
                                        + createTime
                                        + ", file="
                                        + resourceMetadata.getFile());
                    
                    break;

                }

                final UUID uuid = resourceMetadata.getUUID();

                if (resourcesInUse.contains(uuid)) {

                    // still required as of that timestamp.

                    continue;

                }

                try {

                    deleteUnusedResource(resourceMetadata);

                } catch (Throwable t) {

                    // log error and keep going.
                    log.error("Could not delete journal: "
                            + resourceMetadata.getFile(), t);
                    
                }

                // remove from the [journalIndex].
                itr.remove();
                
                njournals++;
                
            }

        }

        /*
         * Delete old index segments.
         */
        
        // #of segments deleted.
        int nsegments = 0;
        {

            /*
             * Note: This iterator supports traversal with concurrent
             * modification (by a single thread). If we decide to delete a
             * indexSegment resource, then deleteResource() will be tasked to
             * delete it from the [segmentIndex] as well.
             */
            final ITupleIterator itr = segmentIndex.rangeIterator(
                    null/* fromKey */, null/* toKey */, 0/* capacity */,
                    IRangeQuery.DEFAULT | IRangeQuery.CURSOR, null/* filter */);
            
            while (itr.hasNext()) {

                final ITuple tuple = itr.next();

                final IResourceMetadata resourceMetadata = (IResourceMetadata) SerializerUtil
                        .deserialize(tuple.getValue());

                // the create timestamp for that resource.
                final long createTime = resourceMetadata.getCreateTime();

                if (createTime >= commitTimeToPreserve) {

                    /*
                     * Do NOT delete any resources whose createTime is GTE the
                     * given commit time.
                     */
                    
                    if (log.isInfoEnabled())
                        log
                                .info("Stopping at resource GTE commitTime to preserve: createTime="
                                        + createTime
                                        + ", file="
                                        + resourceMetadata.getFile());

                    break;
                    
                }
                
                final UUID uuid = resourceMetadata.getUUID();

                if (resourcesInUse.contains(uuid)) {
                    
                    // still required as of that timestamp.
                    
                    continue;
                    
                }

                try {

                    // delete the backing file.
                    deleteUnusedResource(resourceMetadata);

                } catch (Throwable t) {

                    // log error and keep going.
                    log.error("Could not delete segment - continuing: "
                            + resourceMetadata.getFile(), t);

                }

                // remove from the [segmentIndex]
                itr.remove();

                nsegments++;
                
            }

        }

        if (log.isInfoEnabled())
            log.info("Given " + resourcesInUse.size()
                    + " resources that are in use as of timestamp="
                    + commitTimeToPreserve + ", deleted " + njournals
                    + " journals and " + nsegments + " segments");
        
    }
    
    /**
     * Delete the resource in the file system and remove it from the
     * {@link #storeCache} and {@link #resourceFiles} and either
     * {@link #journalIndex} or {@link #segmentIndex} as appropriate.
     * <p>
     * 
     * <strong>DO NOT delete resources that are in use!</strong>
     * 
     * A resource that has not yet been incoporated into a view may be deleted
     * without futher concern. However, once a resource has been incorporated
     * into a view then you MUST arange for appropriate synchronization before
     * the resource may be deleted. For example, {@link #purgeOldResources()}
     * imposes that constraint on the caller that they are responsible for
     * synchronization and is generally invoked during synchronous overflow
     * since we know that there are no active writers at that time.
     * <p>
     * Pre-conditions:
     * <ul>
     * <li>The resource identified by that {@link UUID} exists and is not the
     * live journal.</li>
     * <li>The resource is not in use (not checked).</li>
     * <li>The resource is found in {@link #resourceFiles}.</li>
     * </ul>
     * Post-conditions:
     * <ul>
     * <li>The resource is closed if it was open and is no longer found in the
     * {@link #storeCache}.</li>
     * <li>The resource is no longer found in {@link #resourceFiles}. </li>
     * <li>The backing file for the resource has been deleted (the backing file
     * is obtain from {@link #resourceFiles}).</li>
     * <li>Various counters maintained by the {@link StoreManager} have been
     * updated (bytes delete, bytes under management, etc).</li>
     * <li>The file has been removed from either the {@link #journalIndex} or
     * the {@link #segmentIndex} as appropriate.</li>
     * </ul>
     * 
     * @param uuid
     *            The {@link UUID} which identifies the resource.
     * @param isJournal
     *            <code>true</code> if the resource is a journal.
     */
    protected void deleteResource(final UUID uuid, final boolean isJournal)
            throws NoSuchStoreException {

        if (log.isInfoEnabled())
            log.info("deleteResource: uuid=" + uuid + ", isJournal="
                    + isJournal);
        
        if (uuid == null)
            throw new IllegalArgumentException();
        
        if (uuid == liveJournalRef.get().getRootBlockView().getUUID()) {

            /*
             * Can't close out the live journal!
             * 
             * Note: using the reference directly since invoked during startup
             * to delete index segments left lying around if there is an
             * incomplete move.
             */

            throw new IllegalArgumentException();
            
        }

        synchronized (retentionSet) {

            if (retentionSet.contains(uuid)) {

                throw new IllegalStateException("Resource in retentionSet: "
                        + uuid);

            }

        }
        
        /*
         * Close out store iff open.
         */
        {

            final IRawStore store = storeCache.remove(uuid);

            if (store != null) {
                
                final File file = store.getFile();

                if(isJournal) {
                    
                    assert store instanceof AbstractJournal;
                    
                } else {
                    
                    assert store instanceof IndexSegmentStore;
                    
                }
                
                try {

                    if (store.isOpen()) {

                        // make sure the store is closed.
                        store.close();
                        
                    }

                } catch (IllegalStateException t) {

                    /*
                     * There should not be closed journals in the cache since
                     * they are only closed by the finalizer.
                     * 
                     * However, an IndexSegmentStore will be closed if the
                     * IndexSegment is closed and it can still be in the cache
                     * until its reference is cleared when it gets finalized.
                     * 
                     * Note: if there is a concurrent close then that might be
                     * interesting and should at least be explored further.
                     */
                    if (isJournal)
                        // probably a problem.
                        log.error(file, t);
                    else
                        // probably NOT a problem.
                        log.warn(file, t);

                }

            }

        }
        
        /*
         * delete the backing file.
         */
        {

            final File file = resourceFiles.remove(uuid);

            if (log.isInfoEnabled())
                log.info("DELETE: file=" + file + ", uuid=" + uuid + ", isJournal="
                    + isJournal);
            
            if (file == null) {

                /*
                 * Note: This can happen if you confuse the indexUUID and the
                 * indexSegment's UUID in the code. The former is on the
                 * IndexMetadata while the latter (the one that you want) is on
                 * the SegmentMetadata.
                 */

                throw new NoSuchStoreException(uuid);

            }

            if (!file.exists()) {

                throw new RuntimeException("Not found: " + file);

            }

            final long length = file.length();

            if (!file.delete()) {

                throw new RuntimeException("Could not delete: " + file);

            }

            // track #of bytes deleted since startup.
            bytesDeleted.addAndGet(length);

            // track #of bytes still under management.
            bytesUnderManagement.addAndGet(-length);
            
            if(isJournal) {
                journalBytesUnderManagement.addAndGet(-length);
                journalDeleteCount.incrementAndGet();
            } else {
                segmentBytesUnderManagement.addAndGet(-length);
                segmentStoreDeleteCount.incrementAndGet();
            }

        }

        /*
         * Remove the resource from either journalIndex or segmentIndex as
         * appropriate.
         */
        {

            boolean found = false;
            
            if (isJournal) {

                synchronized (journalIndex) {

                    @SuppressWarnings("unchecked")
                    final ITupleIterator<JournalMetadata> itr = journalIndex
                            .rangeIterator(null/* fromKey */,
                                    null/* toKey */, 0/* capacity */,
                                    IRangeQuery.DEFAULT | IRangeQuery.CURSOR,
                                    null/* filter */);
                    
                    while(itr.hasNext()) {

                        final IResourceMetadata md = itr.next().getObject();
                        
                        if(md.getUUID().equals(uuid)) {
                            
                            itr.remove();
                            
                            found = true;
                            
                            break;
                            
                        }
                        
                    }

                }
            
            } else {

                synchronized (segmentIndex) {

                    @SuppressWarnings("unchecked")
                    final ITupleIterator<SegmentMetadata> itr = segmentIndex
                            .rangeIterator(null/* fromKey */,
                                    null/* toKey */, 0/* capacity */,
                                    IRangeQuery.DEFAULT | IRangeQuery.CURSOR,
                                    null/* filter */);

                    while (itr.hasNext()) {

                        final IResourceMetadata md = itr.next().getObject();

                        if (md.getUUID().equals(uuid)) {

                            itr.remove();

                            found = true;

                            break;

                        }

                    }

                }
         
            }

            if (!found)
                throw new NoSuchStoreException(uuid);
            
        }
        
    }

    /**
     * Variant used by {@link #deleteUnusedResources(long, Set)}, which is in
     * turned invoked by {@link #purgeOldResources()}. This implementation is
     * different in that we have the {@link IResourceManager} on hand when we
     * need to delete the resource. I judge it worth the redundency in the code
     * to have a variant specific to this use case so that the DELETE log
     * messages report the {@link IResourceMetadata#getCreateTime() create time}
     * which can be used as a cross-check on {@link #purgeOldResources()}.
     * Pre-conditions:
     * <ul>
     * <li>The resource described by the {@link IResourceMetadata} exists and
     * is not the live journal.</li>
     * <li>The resource is not in use (not checked).</li>
     * <li>The resource is found in {@link #resourceFiles}.</li>
     * </ul>
     * Post-conditions:
     * <ul>
     * <li>The resource is closed if it was open and is no longer found in the
     * {@link #storeCache}.</li>
     * <li>The resource is no longer found in {@link #resourceFiles}. </li>
     * <li>The {@link ILRUCache} for that resource has been cleared. </li>
     * <li>The backing file for the resource has been deleted (the backing file
     * is obtain from {@link #resourceFiles}).</li>
     * <li>Various counters maintained by the {@link StoreManager} have been
     * updated (bytes delete, bytes under management, etc).</li>
     * </ul>
     * <p>
     * Note: The caller MUST remove the entry for the resource from either
     * {@link #journalIndex} or the {@link #segmentIndex} as appropriate. For
     * this use case, the caller can handle that efficiently since they are
     * already traversing an iterator on the appropriate {@link BTree} and can
     * use {@link Iterator#remove()} to delete the corresponding entry from the
     * {@link BTree}.
     * 
     * @param resourceMetadata
     *            The metadata describing the resource to be deleted.
     */
    private void deleteUnusedResource(final IResourceMetadata resourceMetadata) {
        
        if (log.isInfoEnabled())
            log.info("deleteResource: " + resourceMetadata);
        
        if (resourceMetadata == null)
            throw new IllegalArgumentException();

        final UUID uuid = resourceMetadata.getUUID();
        
        if (uuid == liveJournalRef.get().getRootBlockView().getUUID()) {

            /*
             * Can't close out the live journal!
             * 
             * Note: using the reference directly since invoked during startup
             * to delete index segments left lying around if there is an
             * incomplete move.
             */

            throw new IllegalArgumentException();
            
        }

        synchronized (retentionSet) {

            if (retentionSet.contains(uuid)) {

                throw new IllegalStateException("Resource in retentionSet: "
                        + uuid);

            }

        }

        /*
         * Close out store iff open.
         */
        {

            final IRawStore store = storeCache.remove(uuid);

            if (store != null) {
                
                final File file = store.getFile();

                if(resourceMetadata.isJournal()) {
                    
                    assert store instanceof AbstractJournal;
                    
                } else {
                    
                    assert store instanceof IndexSegmentStore;
                    
                }
                
                try {

                    if (store.isOpen()) {

                        // make sure the store is closed.
                        store.close();
                        
                    }

                } catch (IllegalStateException t) {

                    /*
                     * There should not be closed journals in the cache since
                     * they are only closed by the finalizer.
                     * 
                     * However, an IndexSegmentStore will be closed if the
                     * IndexSegment is closed and it can still be in the cache
                     * until its reference is cleared when it gets finalized.
                     * 
                     * Note: if there is a concurrent close then that might be
                     * interesting and should at least be explored further.
                     */
                    if (resourceMetadata.isJournal())
                        // probably a problem.
                        log.error(file, t);
                    else
                        // probably NOT a problem.
                        log.warn(file, t);

                }

            }

        }

        /*
         * Clear record for that store from the LRUNexus and remove the entry
         * for the store itself from the LRUNexus.
         */
        if (LRUNexus.INSTANCE != null) {

            LRUNexus.INSTANCE.deleteCache(uuid);
            
        }
        
        /*
         * delete the backing file.
         */
        {

            final File file = resourceFiles.remove(uuid);

            /*
             * Note: This logs the file as reported by [resourceFiles] as well
             * as the file in IResourceMetadata in case any discrepency arises.
             */
            if (log.isInfoEnabled())
                log.info("DELETE: " + resourceMetadata + " : " + file);
//                log.warn("DELETE: " + resourceMetadata + " : " + file);
            
            if (file == null) {

                /*
                 * Note: This can happen if you confuse the indexUUID and the
                 * indexSegment's UUID in the code. The former is on the
                 * IndexMetadata while the latter (the one that you want) is on
                 * the SegmentMetadata.
                 */

                throw new NoSuchStoreException(uuid);

            }

            if (!file.exists()) {

                throw new RuntimeException("Not found: " + file);

            }

            final long length = file.length();

            if (!file.delete()) {

                throw new RuntimeException("Could not delete: " + file);

            }

            // track #of bytes deleted since startup.
            bytesDeleted.addAndGet(length);

            // track #of bytes still under management.
            bytesUnderManagement.addAndGet(-length);
            
            if(resourceMetadata.isJournal()) {
                journalBytesUnderManagement.addAndGet(-length);
                journalDeleteCount.incrementAndGet();
            } else {
                segmentBytesUnderManagement.addAndGet(-length);
                segmentStoreDeleteCount.incrementAndGet();
            }

        }

    }
    
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
     *             If there is no commit point that is strictly greater than the
     *             releaseTime. This implies that the release time is either in
     *             the future or, if the releaseTime is equal to the last
     *             commitTime, that you are trying to release everything in the
     *             database.
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

            log.warn("Examining prior journal (fence post): closeTime="
                    + closeTime + ", releaseTime=" + releaseTime);

            return getCommitTimeStrictlyGreaterThan(closeTime);
            
        }

        /*
         * This is the timestamp associated with the commit point that is the
         * first commit point strictly greater than the given release time.
         */
        
        final long commitTime = commitRecord.getTimestamp();
        
        log.warn("Chose commitTime=" + commitTime + " given releaseTime="
                + releaseTime);
        
        assert commitTime > releaseTime;
    
        return commitTime;
        
    }
    
    /**
     * Finds all resources used by any registered index as of the
     * <i>commitTimeToPreserve</i> up to and including the lastCommitTime for
     * the live journal.
     * <p>
     * Note: We include all dependencies for all commit points subsequent to the
     * probe in order to ensure that we do not accidently release dependencies
     * required for more current views of the index.
     * <p>
     * Note: This method solely considers the index views as defined at each
     * commit point starting with the given commit point. It DOES NOT pay
     * attention to the release time or to any other aspect of the state of the
     * system.
     * 
     * @param commitTimeToPreserve
     *            The commit time corresponding to the first commit point which
     *            must be preserved.
     * 
     * @return The set of resource {@link UUID}s required by at least one index
     *         for any commit time GTE the specified commit time.
     */
    protected Set<UUID> getResourcesForTimestamp(final long commitTimeToPreserve) {

        if (log.isDebugEnabled())
            log.debug("commitTimeToPreserve=" + commitTimeToPreserve
                    + ", lastCommitTime="
                    + getLiveJournal().getRootBlockView().getLastCommitTime());
        
        // must be a commitTime.
        if (commitTimeToPreserve <= 0)
            throw new IllegalArgumentException();
        
        final Set<UUID> uuids = new LinkedHashSet<UUID>(512);

        /*
         * The live journal is always a dependency, even if there are no indices
         * declared.
         */
        uuids.add(getLiveJournal().getRootBlockView().getUUID());
        
        /*
         * Scan all journals having data for commit points GTE the given
         * [commitTime].
         * 
         * Note: We have to scan ALL journals since they are organized by their
         * createTime in the [journalIndex] not their [lastCommitTime].
         */
        synchronized(journalIndex) {

            @SuppressWarnings("unchecked")
            final ITupleIterator<JournalMetadata> itr = journalIndex.rangeIterator();
            
            while(itr.hasNext()) {
                
                final ITuple<JournalMetadata> tuple = itr.next();

                final JournalMetadata journalMetadata = tuple.getObject();
                
                final UUID uuid = journalMetadata.getUUID();
                
                final ManagedJournal journal = (ManagedJournal) openStore(uuid);
                
                // the last commit point on that journal.
                final long lastCommitTime = journal.getRootBlockView()
                        .getLastCommitTime();

                if (lastCommitTime < commitTimeToPreserve) {
                    
                    /*
                     * Ignore this journal since last commit point is strictly
                     * LT our starting [commitTime].
                     * 
                     * Note: Since the index partition views are re-defined on
                     * the new journal by each synchronous overflow operation,
                     * we do not need to consider older journals in order to
                     * discover the resources used by all index partition views
                     * defined as of the start of any given journal.
                     */
                    
                    continue;
                    
                }
                
                /*
                 * Scan commit points on that journal.
                 */
                {
                    
                    if (log.isDebugEnabled())
                        log.debug("Examining journal: file="
                            + journal.getFile() + ", lastCommitTime="
                            + lastCommitTime + ", uuid="
                            + journal.getRootBlockView().getUUID());
                    
                    /*
                     * The index of commit points for the journal, loaded from
                     * the last commit point on the journal. This is Ok since we
                     * always want to read up to the lastCommitPoint on each
                     * journal, including on the live journal.
                     * 
                     * Note: This is NOT the live CommitRecordIndex. The live
                     * CommitRecordIndex is NOT protected for use by concurrent
                     * threads.
                     */
                    final CommitRecordIndex commitRecordIndex = journal
                            .getCommitRecordIndex(journal.getRootBlockView()
                                    .getCommitRecordIndexAddr());

                    /*
                     * A per-journal hash set of the [checkpointAddr] for the
                     * BTree's that we have examined so that we can skip over
                     * any BTree whose state has not been changed since the last
                     * commit point (if it has the same checkpointAddr in two
                     * different commit point then its state has not changed
                     * between those commit points).
                     */
                    final Set<Long/* checkpointAddr */> addrs = new HashSet<Long>(
                            512);
                    
                    /*
                     * In order to scan timestamps from [commitTime] through to
                     * the end. For each tuple, fetch the corresponding
                     * [commitRecord]. For each commitRecord, fetch the
                     * Name2Addr index and visit its Entries.
                     */
                    @SuppressWarnings("unchecked")
                    final ITupleIterator<ICommitRecord> itr2 = commitRecordIndex
                            .rangeIterator(commitTimeToPreserve/* fromKey */, null/* toKey */);
                    
                    while(itr2.hasNext()) {
                        
                        final ITuple tuple2 = itr2.next();
                        
                        final CommitRecordIndex.Entry entry2 = (CommitRecordIndex.Entry) tuple2
                                .getObject();

                        /*
                         * For each distinct checkpoint, load the BTree and
                         * fetch its local partition metadata which specifies
                         * its resource dependencies. For each resource, add it
                         * to the set of resources that we are collecting. All
                         * of those resources MUST be retained.
                         */
                        final ICommitRecord commitRecord = commitRecordIndex
                                .fetchCommitRecord(entry2);
                        
                        final Name2Addr name2addr = (Name2Addr) Name2Addr
                                .load(
                                        journal,
                                        commitRecord
                                                .getRootAddr(AbstractJournal.ROOT_NAME2ADDR),
                                        true/* readOnly */);
                        
                        @SuppressWarnings("unchecked")
                        final ITupleIterator<Name2Addr.Entry> itr3 = name2addr.rangeIterator();
                        
                        while(itr3.hasNext()) {
                            
                            final ITuple<Name2Addr.Entry> tuple3 = itr3.next();
                            
                            final Name2Addr.Entry entry3 = tuple3.getObject(); 
                            
                            final long checkpointAddr = entry3.checkpointAddr;
                            
                            if(addrs.add(checkpointAddr)) {
                               
                                /*
                                 * New checkpoint address.
                                 */

                                if (log.isDebugEnabled())
                                    log.debug("index: name=" + entry3.name);
                                
                                // load checkpoint record from the store.
                                final Checkpoint checkpoint = Checkpoint.load(journal, entry3.checkpointAddr);
                                
                                // read the index metadata object for that checkpoint.
                                final IndexMetadata indexMetadata = IndexMetadata.read(journal, checkpoint.getMetadataAddr());
                                
                                // this is where the definition of the view is stored.
                                final LocalPartitionMetadata pmd = indexMetadata.getPartitionMetadata();
                                
                                if (pmd == null) {

                                    /*
                                     * For scale-out, all indices should be
                                     * index partitions and should define the
                                     * resources required by their view.
                                     * 
                                     * Note: However, the metadata service is
                                     * not currently partitioned so you will see
                                     * unpartitioned indices there.
                                     */

                                    continue;
                                    
                                }
                                
                                for(IResourceMetadata t : pmd.getResources()) {
                                    
                                    if (uuids.add(t.getUUID())) {

                                        if (log.isInfoEnabled())
                                            log.info("Dependency: file="
                                                    + t.getFile() + ", uuid="
                                                    + t.getUUID() + ", view="
                                                    + pmd);
                                        
                                    }
                                    
                                } // next resource in view
                                
                            } // end if 
                            
                        } // next Name2Addr.Entry
                        
                    } // next CommitRecordIndex.Entry
                    
                } // block
                
            } // while(journalIndex.rangeIterator.hasNext())
            
        } // synchronized( journalIndex )

        if (log.isInfoEnabled())
            log.info("commitTime=" + commitTimeToPreserve + ", #used=" + uuids.size());

        return uuids;

    }

    /**
     * Munge a name index so that it is suitable for use in a filesystem. In
     * particular, any non-word characters are converted to an underscore
     * character ("_"). This gets rid of all punctuation characters and
     * whitespace in the index name itself, but will not translate unicode
     * characters.
     * 
     * @param s
     *            The name of the scale-out index.
     * 
     * @return A string suitable for inclusion in a filename.
     */
    static public String munge(final String s) {

        return s.replaceAll("[\\W]", "_");

    }

    public File getIndexSegmentFile(final IndexMetadata indexMetadata) {

        if (indexMetadata == null)
            throw new IllegalArgumentException();

        final IPartitionMetadata pmd = indexMetadata.getPartitionMetadata();

        return getIndexSegmentFile(indexMetadata.getName(), indexMetadata
                .getIndexUUID(), pmd == null ? -1 : pmd.getPartitionId());
        
    }

    /**
     * Return the file on which a new {@link IndexSegment} should be written.
     * The file will exist but will have zero length. The file is created using
     * the {@link File#createTempFile(String, String, File)} mechanism within
     * the configured {@link #dataDir} in the subdirectory for the specified
     * scale-out index.
     * <p>
     * Note: The index name appears in the file path above the {@link UUID} of
     * the scale-out index. Therefore it is not possible to have collisions
     * arise in the file system when given indices whose scale-out names differ
     * only in characters that are munged onto the same character since the
     * files will always be stored in a directory specific to the scale-out
     * index.
     * 
     * @param scaleOutIndexName
     *            The name of the scale-out index.
     * @param indexUUID
     *            The UUID of the scale-out index.
     * @param partitionId
     *            The index partition identifier -or- <code>-1</code> if the
     *            index is not partitioned (handles the MDS which does not use
     *            partitioned indices at this time).
     * 
     * @return The {@link File} on which a {@link IndexSegmentStore} for that
     *         index partition may be written. The file will be unique and
     *         empty.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if the partitionId is negative and not <code>-1</code>
     * 
     * @todo should the filename be relative or absolute?
     */
    public File getIndexSegmentFile(final String scaleOutIndexName,
            final UUID indexUUID, final int partitionId) {

        assertOpen();

        if (scaleOutIndexName == null)
            throw new IllegalArgumentException();

        if (indexUUID == null)
            throw new IllegalArgumentException();

        if (partitionId < -1)
            throw new IllegalArgumentException();
        
        // munge index name to fit the file system.
        final String mungedName = munge(scaleOutIndexName);

        // subdirectory into which the individual index segs will be placed.
        final File indexDir = new File(segmentsDir, mungedName + File.separator
                + indexUUID.toString());

        // make sure that directory exists.
        indexDir.mkdirs();

        final String partitionStr = (partitionId == -1 ? "" : "_shardId"
                + leadingZeros.format(partitionId));

        final String prefix = mungedName + "" + partitionStr + "_";

        final File file;
        try {

            file = File.createTempFile(prefix, Options.SEG, indexDir);

        } catch (IOException e) {

            throw new RuntimeException(e);

        }

        if (log.isInfoEnabled())
            log.info("Created file: " + file);

        return file;

    }

    /**
     * This attempts to obtain the exclusive lock for the
     * {@link WriteExecutorService}. If successful, it purges any resources that
     * are no longer required based on
     * {@link StoreManager.Options#MIN_RELEASE_AGE} and optionally truncates the
     * live journal such that no free space remains in the journal.
     * <p>
     * Note: If there is heavy write activity on the service then the timeout
     * may well expire before the exclusive write lock becomes available.
     * Further, the acquisition of the exclusive write lock will throttle
     * concurrent write activity and negatively impact write performance if the
     * system is heavily loaded by write tasks.
     * 
     * @param timeout
     *            The timeout (in milliseconds) that the method will await the
     *            pause of the write service.
     * @param truncateJournal
     *            When <code>true</code>, the live journal will be truncated to
     *            its minimum extent (all writes will be preserved but there
     *            will be no free space left in the journal). This may be used
     *            to force the {@link DataService} to its minimum possible
     *            footprint for the configured history retention policy.
     * 
     * @return <code>true</code> if successful and <code>false</code> if the
     *         write service could not be paused after the specified timeout.
     * 
     * @param truncateJournal
     *            When <code>true</code> the live journal will be truncated such
     *            that no free space remains in the journal. If writes are
     *            directed to the live journal after it has been truncated then
     *            it will transparently re-extended.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws IllegalStateException
     *             if the {@link StoreManager} is not running.
     */
    public boolean purgeOldResources(final long timeout,
            final boolean truncateJournal) throws InterruptedException {

        final WriteExecutorService writeService = getConcurrencyManager()
                .getWriteService();

        if (writeService.tryLock(timeout, TimeUnit.MILLISECONDS)) {

            assertRunning();

            try {

                final Event event = new Event(getFederation(),
                        new EventResource(), EventType.PurgeResources).start();

                try {

                    final PurgeResult purgeResult = purgeOldResources();

                    if (purgeResult != null) {

                        log.warn(purgeResult.toString());

                        event.addDetails(purgeResult.getParams());
                        
                    }
                    
                    if (truncateJournal) {

                        assertRunning();

                        getLiveJournal().truncate();

                    }

                } finally {
                
                    event.end();
                    
                }
            
                return true;
        
            } finally {

                // release the lock.
                writeService.unlock();

            }

        } else {

            log.warn("Purge resources did not run: timeout=" + timeout);

            return false;

        }

    }

    /**
     * When the {@link StoreManager} is relatively new (as measured by the #of
     * bytes under management) we discount the journal extent in order to
     * trigger overflow earlier. Together with the discount applied to the split
     * handler by the {@link AsynchronousOverflowTask}, this helps to break
     * down new index partitions allocated on the new data service and
     * re-distribute those index partitions (if there are other data services
     * which have even less utilization).
     * 
     * @param p
     *            The properties (modified as side-effect).
     */
    protected void overrideJournalExtent(final Properties p) {

        final long bytesUnderManagement = this.bytesUnderManagement.get();
        
        if (accelerateOverflowThreshold == 0
                || bytesUnderManagement >= accelerateOverflowThreshold) {

            /*
             * Crossed the threshold where we no longer accelerate overflow.
             */

            return;

        }

        final double d = (double) bytesUnderManagement
                / accelerateOverflowThreshold;

        final long initialExtent = Long.parseLong(p.getProperty(
                Options.INITIAL_EXTENT, Options.DEFAULT_INITIAL_EXTENT));

        final long maximumExtent = Long.parseLong(p.getProperty(
                Options.INITIAL_EXTENT, Options.DEFAULT_MAXIMUM_EXTENT));

        /*
         * Don't allow a journal w/ less than 10M or the minimum specified by
         * Options.
         */
        final long minimumExtent = Math.max(Options.minimumInitialExtent,
                Bytes.megabyte * 10);

        /*
         * Use the same value for initial and maximum extents since we plan to
         * overflow rapidly. We choose the value as a discount on the maximum
         * extent. This prevents numerous extensions until we get near to the
         * maximum extent.
         */
        final long adjustedExtent = Math.max(minimumExtent,
                (long) (maximumExtent * d));

        p.setProperty(Options.INITIAL_EXTENT, Long.toString(adjustedExtent));

        p.setProperty(Options.MAXIMUM_EXTENT, Long.toString(adjustedExtent));

        if (log.isInfoEnabled())
            log.info("discount=" + d //
                    + ", bytesUnderManagement=" + bytesUnderManagement //
                    + ", threshold=" + accelerateOverflowThreshold//
                    + ", minimimInitialExtent=" + minimumExtent//
                    + ", initialExtent=" + initialExtent //
                    + ", maximumExtent=" + maximumExtent //
                    + ", adjustedExtent=" + adjustedExtent);

        return;

    }

}
