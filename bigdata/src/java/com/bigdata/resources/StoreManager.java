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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakCacheEntryFactory;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.cache.WeakValueCache.IClearReferenceListener;
import com.bigdata.concurrent.LockManager;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ILocalTransactionManager;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Name2Addr;
import com.bigdata.journal.Options;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.service.DataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.IServiceShutdown;
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
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class StoreManager extends ResourceEvents implements IResourceManager {

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
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo add options to control whether an {@link IndexSegmentStore} is
     *       opened with the nodes fully buffered.
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
         * or <code>0L</code> for an immortal database. Some convenience
         * values have been declared.
         * 
         * @see DEFAULT_MIN_RELEASE_AGE
         * @see MIN_RELEASE_AGE_1H
         * @see MIN_RELEASE_AGE_1D
         * @see MIN_RELEASE_AGE_1W
         */
        String MIN_RELEASE_AGE = "minReleaseAge";

        /** Minimum release age is one hour. */
        String MIN_RELEASE_AGE_1H = "" + 1/* hr */* 60/* mn */* 60/* sec */* 1000/* ms */;

        /** Minimum release age is one day. */
        String MIN_RELEASE_AGE_1D = "" + 24/* hr */* 60/* mn */* 60/* sec */* 1000/* ms */;

        /** Minimum release age is one week. */
        String MIN_RELEASE_AGE_1W = "" + 7/* d */* 24/* hr */* 60/* mn */* 60/* sec */
                * 1000/* ms */;

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
     * FIXME make sure this cache purges entries that have not been touched in
     * the last N seconds, where N might be 60.
     * 
     * FIXME make sure that finalize() is doing what we need on
     * {@link AbstractJournal} and {@link IndexSegmentStore}. Do we have to use
     * any JVM options to make sure it gets called during normal operation? It
     * probably does not matter whether or not it is getting called before
     * system shutdown, which is I think the only place where there are JVM
     * issues concerning whether or not it is called.
     * 
     * FIXME make sure that we do not fully buffer an {@link IndexSegmentStore}
     * in {@link #scanFile(File, com.bigdata.resources.StoreManager.Stats)} -
     * what a waste that would be!
     */
    final protected WeakValueCache<UUID, IRawStore> storeCache;

//    /**
//     * Listens for cleared references on the {@link #storeCache}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    private class ClearReferenceListener implements
//            IClearReferenceListener<UUID> {
//
//        public void cleared(UUID uuid) {
//
////            lock.lock();
//
//            try {
//
//                /*
//                 * FIXME concurrency of the notifications by the
//                 * {@link WeakValueCache} and requests for indices is going to
//                 * be problem. The risk is that we get a notice that the weak
//                 * reference paired with an IndexSegment was cleared while there
//                 * is a concurrent request for the store on which that index
//                 * segment resides. This can result in a hard reference for the
//                 * store in the hands of the application at the same time as we
//                 * are planning to close the store here.
//                 * 
//                 * Perhaps the problem is that I should be maintaining a weak
//                 * reference cache on the store rather than the index segment.
//                 * The index segment has a reference to the store, so the store
//                 * will be strongly held while there is a hard reference to any
//                 * index segment on that store. The StoreManager currently holds
//                 * a hard reference to all open stores, but maybe that could be
//                 * changed and this logic moved back into that class and we
//                 * would have a solution for closing out ANY store (journal or
//                 * index segment - it would not matter). The only catch is that
//                 * the journal also maintains weak value cache backed by a hard
//                 * reference LRU. The hard references to the indices in the LRU
//                 * might prevent us from ever closing a journal (actually, they
//                 * probably would not if there is no hard reference to any of
//                 * the indices other than from the journal since that object
//                 * graph would not be reachable). However, by ensuring that LRU
//                 * entries are purged after a certain age we can certainly help
//                 * things along!
//                 * 
//                 * Modify the LRU to close out entries older than some threshold
//                 * (a configuration parameter). This seems a worthy general
//                 * purpose refinement since the whole point of the LRU is
//                 * _recent_ use. If things just hang around on the LRU forever
//                 * then the information is basically worthless. Clearing an LRU
//                 * entry is always safe since the purpose of the LRU is to cause
//                 * references to remain strongly reachable and hence not be
//                 * cleared from the weak value cache. The LRU should be cleared
//                 * from the MRU position forward to the LRU position by a
//                 * scheduled thread since otherwise there is no guarentee that
//                 * old entries will be cleared. Old entries can also be cleared
//                 * each time there is a touch on the LRU. The asynchronous
//                 * thread can run every so often (a configuration parameter).
//                 * The delay between runs will place an upper bound on the
//                 * length of time that a reference to an unused object will
//                 * remain on the queue.
//                 * 
//                 * For store files, we should default the maximum unused age to
//                 * 60 seconds and the asynchronous thread to 60 seconds as well.
//                 * That way we will close out old journals and index segments no
//                 * later than 60 seconds after the last touch.
//                 */
//                
//                log.warn("Cleared index segment reference: " + uuid);
//
//                final IRawStore store = openStores.remove(uuid);
//
//                assert store instanceof IndexSegmentStore;
//
//                assert store != null;
//
//                log.error("SHOULD close index segment store: " + store.getFile());
//
////                store.close();
//
////            } finally {
////
////                lock.unlock();
////                
////            }
//            
//        }
//        
//    }

    /**
     * <code>true</code> iff {@link BufferMode#Transient} was indicated.
     */
    private final boolean isTransient;

    /**
     * A hard reference to the live journal.
     */
    protected AbstractJournal liveJournal;

    /**
     * Set <code>true</code> by {@link #start()} and remains <code>true</code>
     * until the {@link ResourceManager} is shutdown.
     */
    private boolean open;

    /**
     * @throws IllegalStateException
     *             unless open.
     */
    protected void assertOpen() {
        
        if (!open)
            throw new IllegalStateException();
        
    }

    /**
     * @throws IllegalStateException
     *             if open.
     */
    protected void assertNotOpen() {
        
        if (open)
            throw new IllegalStateException();

    }
    
    /**
     * A map from the resource UUID to the absolute {@link File} for that
     * resource.
     * <p>
     * Note: The {@link IResourceMetadata} reported by an
     * {@link AbstractJournal} or {@link IndexSegmentStore} generally
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
    private final Map<UUID, File> resourceFiles = new HashMap<UUID, File>();

//    /**
//     * A map from the resource {@link UUID} to the open {@link IRawStore}
//     * reference.
//     * <p>
//     * Note: This is exposed to the {@link OverflowManager} since it needs to
//     * insert the newly created journal during overflow.
//     */
//    protected final Map<UUID, IRawStore> openStores = new HashMap<UUID, IRawStore>();
        
//    /**
//     * The timeout for {@link #shutdown()} -or- ZERO (0L) to wait for ever.
//     * 
//     * @see IServiceShutdown.Options#SHUTDOWN_TIMEOUT
//     */
//    final private long shutdownTimeout;

//    /**
//     * A service that periodically runs the {@link CloseUnusedStoresTask}.
//     */
//    private ScheduledExecutorService closeStoreService;

//    /**
//     * Task closes any stores (journals or index segments) that have not been
//     * recently accessed.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    protected class CloseUnusedStoresTask implements Runnable {
//
//        /**
//         * Note: Don't throw anything here since we don't want to have the task
//         * suppressed!
//         */
//        public void run() {
//
//            try {
//            
//                closeStores();
//                
//            } catch(Exception ex) {
//                
//                log.warn(ex.getMessage(),ex);
//                
//            }
//            
//        }
//        
//        /**
//         * Closes out store files that have not been recently accessed.
//         * 
//         * @throws Exception
//         * 
//         * @todo never close the live journal.
//         */
//        protected void closeStores() throws Exception {
//           
//            log.warn("Close out of unused stores is not implemented yet!");
//            
//        }
//        
//    }
    
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
     * The last value computed by {@link #getEffectiveReleaseTime()} and ZERO
     * (0L) until a value has been calculated.
     */
    protected long lastEffectiveReleaseTime = 0L;
    
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
    
    protected StoreManager(Properties properties) {
                
        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties;

        /*
         * storeCacheCapacity
         */
        {
            
            final int storeCacheCapacity = Integer.parseInt(properties.getProperty(
                    Options.STORE_CACHE_CAPACITY,
                    Options.DEFAULT_STORE_CACHE_CAPACITY));

            log.info(Options.STORE_CACHE_CAPACITY+"="+storeCacheCapacity);

            if (storeCacheCapacity <= 0)
                throw new RuntimeException(Options.STORE_CACHE_CAPACITY
                        + " must be non-negative");

            storeCache = new WeakValueCache<UUID, IRawStore>(
//                    WeakValueCache.INITIAL_CAPACITY,//
//                    WeakValueCache.LOAD_FACTOR, //
                    new LRUCache<UUID, IRawStore>(storeCacheCapacity)
//                    new WeakCacheEntryFactory<UUID,IndexSegment>(),
//                    new ClearReferenceListener()
                    );

        }


//        // shutdownTimeout
//        {
//
//            shutdownTimeout = Long
//                    .parseLong(properties.getProperty(Options.SHUTDOWN_TIMEOUT,
//                            Options.DEFAULT_SHUTDOWN_TIMEOUT));
//
//            if (shutdownTimeout < 0) {
//
//                throw new RuntimeException("The '" + Options.SHUTDOWN_TIMEOUT
//                        + "' must be non-negative.");
//
//            }
//
//            log.info(Options.SHUTDOWN_TIMEOUT + "=" + shutdownTimeout);
//
//        }

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

                if (!journalsDir.mkdirs()) {

                    throw new RuntimeException("Could not create directory: "
                            + journalsDir.getAbsolutePath());

                }

            }

            if (!segmentsDir.exists()) {

                log.info("Creating: "+segmentsDir);
                
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
     * Starts up the {@link StoreManager}.
     * <p>
     * Note: Implementations of this method MUST be <code>synchronized</code>.
     * 
     * @throws IllegalStateException
     *             if the {@link IConcurrencyManager} has not been set.
     * 
     * @throws IllegalStateException
     *             if the the {@link ResourceManager} is already running.
     */
    synchronized public void start() {

        assertNotOpen();
        
        // verify concurrency manager has been set.
        getConcurrencyManager();
        
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
                 * 
                 * See OverflowManager#doOverflow() which has very similar logic
                 * with the same problem.
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
            storeCache.put(liveJournal.getRootBlockView().getUUID(),
                    liveJournal,false/*dirty*/);
            
        }

//        /*
//         * Start service which will close out unused stores.
//         */
//        {
//
//            // @todo config initialDelay and delay.
//            final long initialDelay = 10000; // initial delay in ms.
//            final long delay = 5000; // delay in ms.
//            final TimeUnit unit = TimeUnit.MILLISECONDS;
//
//            closeStoreService = Executors
//                    .newSingleThreadScheduledExecutor(DaemonThreadFactory
//                            .defaultThreadFactory());
//
//            closeStoreService.scheduleWithFixedDelay(new CloseUnusedStoresTask(),
//                    initialDelay, delay, unit);
//
//        }

        open = true;
        
    }
    
    public void shutdown() {

        final long begin = System.currentTimeMillis();
        
        assertOpen();

//        // closeStoreService shutdown
//        {
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
//            closeStoreService.shutdown();
//
//            try {
//
//                log.info("Awaiting service termination");
//
//                long elapsed = System.currentTimeMillis() - begin;
//
//                if (!closeStoreService.awaitTermination(shutdownTimeout - elapsed, unit)) {
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
        
        closeStores();

        tmpStore.closeAndDelete();

        open = false;
        
    }
    
    public void shutdownNow() {

        assertOpen();

//        closeStoreService.shutdownNow();
        
        closeStores();

        tmpStore.closeAndDelete();

        open = false;

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
         * total #of bytes of user data found in those files.
         */
        public long nbytes;

        public String toString() {
            
            return "Stats{nfiles=" + nfiles + ", njournals=" + njournals
                    + ", nsegments=" + nsegments + ", nbytes=" + nbytes + "}";
            
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

        log.info("Scanning file: "+file+", stats="+stats);
        
        final IResourceMetadata resource;

        final String name = file.getName();

        if (name.endsWith(Options.JNL)) {

            final Properties properties = getProperties();

            properties.setProperty(Options.FILE, file.getAbsolutePath());

            properties.setProperty(Options.READ_ONLY, "true");

            final AbstractJournal tmp = new ManagedJournal(properties);

            try {
            
                resource = tmp.getResourceMetadata();
    
                stats.njournals++;
                stats.nfiles++;
                stats.nbytes += file.length(); //tmp.size(); //getBufferStrategy().getExtent();
                
            } finally {
            
                tmp.close();
                
            }

        } else if (name.endsWith(Options.SEG)) {

            final IndexSegmentStore segStore = new IndexSegmentStore(file
                    .getAbsoluteFile());

            try {

                resource = segStore.getResourceMetadata();

                stats.nsegments++;
                stats.nfiles++;
                stats.nbytes += file.length(); //segStore.size();

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

        addResource(resource,file.getAbsoluteFile());
        
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
        
        open = false;

        Iterator<IRawStore> itr = storeCache.iterator();

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
     * The #of index segments on hand.
     */
    public int getIndexSegmentCount() {
        
        return indexSegmentIndex.getEntryCount();
        
    }
    
    /**
     * Notify the resource manager of a new resource. The resource is added to
     * {@link #resourceFiles} and to either {@link #journalIndex} or
     * {@link #indexSegmentIndex} as appropriate.  As a post-condition, you can
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

        if (storeCache.get(uuid) != null) {
            
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
     * Returns a filter that is used to recognize files that are managed by this
     * class. The {@link ResourceManager} will log warnings if it sees an
     * unexpected file and will NOT {@link #deleteResources()} files that it does not
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

        public ManagedJournal(Properties properties) {
            
            super(properties);
            
        }

        public long nextTimestamp() {
            
            return StoreManager.this.nextTimestamp();
            
        }
        
        @Override
        public long commit() {

            return commitNow(nextTimestamp());
            
        }

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
     * Closes the {@link IRawStore}.
     * 
     * @param uuid
     *            The {@link UUID} identifying that store file.
     * 
     * FIXME Either implement or remove. This is basically a close() but not a
     * removeResource() with some additional protection for synchronization
     * issues.
     */
    synchronized public void closeStore(UUID uuid) {
     
        throw new UnsupportedOperationException();
        
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
     * 
     * @todo Since these operations can have modest latency, especially if we
     *       open an fully buffered index segment, it would be nice to use a
     *       per-store (or store UUID) lock to avoid imposing latency on threads
     *       requiring access to different stores.
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

                IndexSegmentStore segStore = new IndexSegmentStore(file);

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
        storeCache.put(uuid, store, false/* dirty */);
        
        // return the reference to the open store.
        return store;
        
    }

    /**
     * Report the next timestamp assigned by the {@link ILocalTransactionManager}.
     */
    protected long nextTimestamp() {
        
        return getConcurrencyManager().getTransactionManager().nextTimestamp();
        
    }

    public void deleteResources() {

        assertNotOpen();

        // NOP if transient.
        if(isTransient()) return; 
        
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
        
        if(f.isDirectory()) {
            
            final File[] children = f.listFiles(newFileFilter());
            
            for(int i=0; i<children.length; i++) {
                
                recursiveDelete( children[i] );
                
            }
            
        }
        
        log.info("Removing: "+f);
        
        if (f.exists() && !f.delete()) {

            log.warn("Could not remove: " + f);

        }
        
    }
    
    /**
     * Updates the {@link #releaseTime}.
     * 
     * @see #purgeOldResources(), which is responsible for actually deleting the
     *      old resources.
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
                    
                    final IRawStore store = storeCache.remove(uuid);

                    assert store instanceof AbstractJournal;

                    // can't close out the live journal!
                    assert uuid != getLiveJournal().getRootBlockView().getUUID();

                    if (store != null) {

                        store.close(); // @todo try/catch?

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
                    
                    final IRawStore store = storeCache.remove(uuid);

                    assert store instanceof IndexSegmentStore;
                    
                    if (store != null) {

                        store.close(); // @todo try/catch

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

        final long effectiveReleaseTime;
        
        if (minReleaseAge == 0L) {

            log.info("Immortal database - resoureces will NOT be released");

            effectiveReleaseTime = 0L;
            
        } else {
        
            final long t1 = nextTimestamp() - minReleaseAge;

            final long t;
            if (releaseTime == 0L) {

                log.warn("Release time has not been set.");

                t = t1;

            } else {

                final long t2 = getEarliestDependencyTimestamp(releaseTime);

                t = Math.min(t1, t2);

            }
        
            effectiveReleaseTime = t;
        
        }
        
        lastEffectiveReleaseTime = effectiveReleaseTime;
        
        return effectiveReleaseTime;
        
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

}
