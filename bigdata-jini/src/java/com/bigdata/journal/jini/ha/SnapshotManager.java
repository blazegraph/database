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
package com.bigdata.journal.jini.ha;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.concurrent.FutureTaskInvariantMon;
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.msg.HASnapshotResponse;
import com.bigdata.ha.msg.IHASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ITx;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.jini.ha.SnapshotIndex.ISnapshotRecord;
import com.bigdata.journal.jini.ha.SnapshotIndex.SnapshotRecord;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumException;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.IServiceInit;
import com.bigdata.striterator.Resolver;
import com.bigdata.striterator.Striterator;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;
import com.bigdata.util.concurrent.LatchedExecutor;

/**
 * Class to manage the snapshot files.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SnapshotManager implements IServiceInit<Void> {

    private static final Logger log = Logger.getLogger(SnapshotManager.class);

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /**
     * The file extension for journal snapshots.
     */
    public final static String SNAPSHOT_EXT = ".jnl.gz";

    /**
     * The prefix for the temporary files used to generate snapshots.
     */
    public final static String SNAPSHOT_TMP_PREFIX = "snapshot";

    /**
     * The suffix for the temporary files used to generate snapshots.
     */
    public final static String SNAPSHOT_TMP_SUFFIX = ".tmp";

    /**
     * A {@link FileFilter} that visits all files ending with the
     * {@link #SNAPSHOT_EXT} and the names of all direct child directories. This
     * {@link FileFilter} may be used to establish recursive scans of the
     * {@link #getSnapshotDir()}.
     */
    static public final FileFilter SNAPSHOT_FILTER = new FileFilter() {

        @Override
        public boolean accept(File f) {

            if (f.isDirectory()) {

                return true;

            }

            return f.getName().endsWith(SnapshotManager.SNAPSHOT_EXT);

        }

    };
    
    /**
     * A {@link FileFilter} that visits all temporary files used to generate
     * snapshots and the names of all direct child directories.  This is used
     * to clean out any temporary files that might be left lying around if the
     * process was terminated while taking a snapshot.
     * 
     * @see #SNAPSHOT_TMP_PREFIX
     * @see #SNAPSHOT_TMP_SUFFIX
     */
    static private final FileFilter TEMP_FILE_FILTER = new FileFilter() {

        @Override
        public boolean accept(final File file) {
            if (file.isDirectory()) {
                // Visit directory, but do not delete.
                return true;
            }
            final String name = file.getName();
            if (name.startsWith(SNAPSHOT_TMP_PREFIX)
                    && name.endsWith(SNAPSHOT_TMP_SUFFIX)) {

                // One of our temporary files.
                return true;

            }
            return false;
        }
    };
    
    /**
     * The journal.
     */
    private final HAJournal journal;
    
    /**
     * @see HAJournalServer.ConfigurationOptions#SNAPSHOT_DIR
     */
    private final File snapshotDir;

    /**
     * @see HAJournalServer.ConfigurationOptions#SNAPSHOT_POLICY
     */
    private final ISnapshotPolicy snapshotPolicy;
    
//    /**
//     * @see HAJournalServer.ConfigurationOptions#SNAPSHOT_ON_FIRST_MEET
//     */
//    private final boolean snapshotOnFirstMeet;
    
    /**
     * @see HAJournalServer.ConfigurationOptions#RESTORE_POLICY
     */
    private final IRestorePolicy restorePolicy;
    
    /**
     * @see HAJournalServer.ConfigurationOptions#STARTUP_THREADS
     */
    private final int startupThreads;
    
    /**
     * An in memory index over the last commit time of each snapshot. This is
     * populated when the {@link HAJournal} starts from the file system and
     * maintained as snapshots are taken or destroyed.
     * <p>
     * Note: This index is not strictly necessary. We can also visit the files
     * in the file system. However, the index makes it much faster to locate a
     * specific snapshot based on a commit time and provides low latency access
     * to the {@link IRootBlockView} for that snapshot (faster than opening the
     * snapshot file on the disk).
     */
    private final SnapshotIndex snapshotIndex;

    /**
     * Lock used to guard the decision to take a snapshot.
     */
    private final Lock lock = new ReentrantLock();

    /**
     * The {@link Future} of the current snapshot (if any).
     * <p>
     * This field is guarded by the {@link #lock}.
     */
    private Future<IHASnapshotResponse> snapshotFuture = null;
    
    /**
     * Return the {@link ISnapshotPolicy}.
     *
     * @see HAJournalServer.ConfigurationOptions#SNAPSHOT_POLICY
     */
    public ISnapshotPolicy getSnapshotPolicy() {

        return snapshotPolicy;
        
    }

//    /**
//     * Return <code>true</code> iff the service will take a snapshot of the
//     * empty journal when the service first joins with a met quorum.
//     * 
//     * @see HAJournalServer.ConfigurationOptions#SNAPSHOT_ON_FIRST_MEET
//     */
//    boolean isSnapshotOnFirstMeet() {
//        
//        return snapshotOnFirstMeet;
//        
//    }
    
    /**
     * Return the {@link IRestorePolicy}.
     *
     * @see HAJournalServer.ConfigurationOptions#RESTORE_POLICY
     */
    public IRestorePolicy getRestorePolicy() {

        return restorePolicy;
        
    }

    public final File getSnapshotDir() {
        
        return snapshotDir;
        
    }
    
//    /**
//     * An in memory index over the commitTime for the commit point of each
//     * snapshot. This is populated when the {@link HAJournal} starts from the
//     * file system and maintained as snapshots are taken or destroyed.
//     */
//    private SnapshotIndex getSnapshotIndex() {
//
//        return snapshotIndex;
//
//    }

    public SnapshotManager(final HAJournalServer server,
            final HAJournal journal, final Configuration config)
            throws IOException, ConfigurationException {

        this.journal = journal;
        
        // Note: This is the effective service directory.
        final File serviceDir = server.getServiceDir(); 

        // Note: Default is relative to the serviceDir.
        snapshotDir = (File) config
                .getEntry(
                        HAJournalServer.ConfigurationOptions.COMPONENT,
                        HAJournalServer.ConfigurationOptions.SNAPSHOT_DIR,
                        File.class,//
                        new File(
                                serviceDir,
                                HAJournalServer.ConfigurationOptions.DEFAULT_SNAPSHOT_DIR)//
                );

        snapshotPolicy = (ISnapshotPolicy) config.getEntry(
                HAJournalServer.ConfigurationOptions.COMPONENT,
                HAJournalServer.ConfigurationOptions.SNAPSHOT_POLICY,
                ISnapshotPolicy.class,//
                HAJournalServer.ConfigurationOptions.DEFAULT_SNAPSHOT_POLICY);

//        snapshotOnFirstMeet = !(snapshotPolicy instanceof NoSnapshotPolicy);
//        snapshotOnFirstMeet = (Boolean) config.getEntry(
//                        HAJournalServer.ConfigurationOptions.COMPONENT,
//                        HAJournalServer.ConfigurationOptions.SNAPSHOT_ON_FIRST_MEET,
//                        Boolean.TYPE,
//                        HAJournalServer.ConfigurationOptions.DEFAULT_SNAPSHOT_ON_FIRST_MEET);

        restorePolicy = (IRestorePolicy) config.getEntry(
                HAJournalServer.ConfigurationOptions.COMPONENT,
                HAJournalServer.ConfigurationOptions.RESTORE_POLICY,
                IRestorePolicy.class, //
                HAJournalServer.ConfigurationOptions.DEFAULT_RESTORE_POLICY);

        {

            startupThreads = (Integer) config
                    .getEntry(
                            HAJournalServer.ConfigurationOptions.COMPONENT,
                            HAJournalServer.ConfigurationOptions.STARTUP_THREADS,
                            Integer.TYPE,
                            HAJournalServer.ConfigurationOptions.DEFAULT_STARTUP_THREADS);

            if (startupThreads <= 0) {
                throw new ConfigurationException(
                        HAJournalServer.ConfigurationOptions.STARTUP_THREADS
                                + "=" + startupThreads + " : must be GT ZERO");
            }

        }
        
        snapshotIndex = SnapshotIndex.createTransient();

        // Note: Caller MUST invoke init() Callable.

    }

    @Override
    public Callable<Void> init() {
        
        return new InitTask();

    }

    /**
     * Task that is used to initialize the {@link SnapshotManager}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     * @see <a href="http://trac.bigdata.com/ticket/775" > HAJournal start()
     *      (optimization) </a>
     */
    private class InitTask implements Callable<Void> {

        @Override
        public Void call() throws Exception {

            lock.lock();
            
            try {
            
                doRunWithLock();
                
                // Done.
                return (Void) null;
                
            } finally {
                
                lock.unlock();
                
            }
            
        }

        private void doRunWithLock() throws IOException, InterruptedException,
                ExecutionException {

            /*
             * Delete any temporary files that were left lying around in the
             * snapshot directory.
             * 
             * TODO This may be relatively lengthy. It would be better to
             * combine this with the scan in which we read the root blocks and
             * index the snapshots. However, this will require another refactor
             * of the parallel scan logic. For now, I am merely reporting out
             * the times for these different scans so I can get a better sense
             * of the latencies involved.
             */
            if (log.isInfoEnabled())
                log.info("Starting cleanup.");

            CommitCounterUtility.recursiveDelete(false/* errorIfDeleteFails */,
                    getSnapshotDir(), TEMP_FILE_FILTER);

            // Make sure the snapshot directory exists.
            ensureSnapshotDirExists();

            if (log.isInfoEnabled())
                log.info("Starting scan.");

            final LatchedExecutor executor = new LatchedExecutor(
                    journal.getExecutorService(), startupThreads);

            // Populate the snapshotIndex from the snapshotDir.
            populateIndexRecursive(//
                    executor,//
                    getSnapshotDir(), //
                    SNAPSHOT_FILTER, //
                    0 // depth@root
            );

            if (log.isInfoEnabled())
                log.info("Starting policy.");

            // Initialize the snapshot policy. It can self-schedule.
            snapshotPolicy.init(journal);

            if (log.isInfoEnabled())
                log.info("Done.");

        }

        /**
         * Scans the {@link SnapshotManager#getSnapshotDir()} and populates the
         * {@link SnapshotIndex} from the root blocks in snapshot files found in
         * that directory.
         * 
         * @throws IOException
         * @throws ExecutionException
         * @throws InterruptedException
         * 
         *             TODO Follow the code pattern for the HALogNexus and
         *             provide robust error handling for snapshot files. Note
         *             that snapshots are taken locally based on various
         *             criteria (including the size of the delta, the #of
         *             HALogs, etc.). As long as we have all HALogs the services
         *             should be able to make a purely local decisions about
         *             what to do if we have a bad snapshot file. One option is
         *             to force a snapshot when the service starts. That option
         *             is only available of course if the service can join with
         *             the quorum.
         *             <p>
         *             Note: If the service CAN NOT do a point in time recovery
         *             because it lacks a combination of valid HALog files and
         *             snapshots, then a failover to that service will degrade
         *             the availability of the cluster.
         */
        private void populateIndexRecursive(final LatchedExecutor executor,
                final File f, final FileFilter fileFilter, final int depth)
                throws IOException, InterruptedException, ExecutionException {

            if (depth == CommitCounterUtility.getLeafDirectoryDepth()) {

                /*
                 * Leaf directory.
                 */
                
                final File[] children = f.listFiles(fileFilter);

                /*
                 * Setup tasks for parallel threads to read the commit record from
                 * each file.
                 */
                final List<FutureTask<SnapshotRecord>> futures = new ArrayList<FutureTask<SnapshotRecord>>(
                        children.length);

                for (int i = 0; i < children.length; i++) {

                    final File child = children[i];

                    final FutureTask<SnapshotRecord> ft = new FutureTask<SnapshotRecord>(

                    new Callable<SnapshotRecord>() {

                        @Override
                        public SnapshotRecord call() throws Exception {

                            return getSnapshotRecord(child);

                        }

                    });

                    futures.add(ft);

                }

                try {

                    /*
                     * Schedule all futures.
                     */
                    for (FutureTask<SnapshotRecord> ft : futures) {

                        executor.execute(ft);

                    }
                    
                    /*
                     * Await futures, obtaining snapshot records for the current
                     * leaf directory.
                     * 
                     * TODO If the root blocks are bad, then this will throw an
                     * IOException and that will prevent the startup of the
                     * HAJournalServer. However, if we start up the server with
                     * a known bad snapshot *and* the snapshot is the earliest
                     * snapshot, then we can not restore commit points which
                     * depend on that earliest snapshot (we can still restore
                     * commit points that are GTE the first useable snapshot).
                     * 
                     * TODO A similar problem exists if any of the HALog files
                     * GTE the earliest snapshot are missing, have bad root
                     * blocks, etc. We will not be able to restore the commit
                     * point associated with that HALog file unless it also
                     * happens to correspond to a snapshot.
                     */
                    final List<SnapshotRecord> records = new ArrayList<SnapshotRecord>(
                            children.length);

                    for (int i = 0; i < children.length; i++) {

                        final Future<SnapshotRecord> ft = futures.get(i);

                        final SnapshotRecord r = ft.get();

                        records.add(r);

                    }

                    // Add all records in the caller's thread.
                    for (SnapshotRecord r : records) {

                        snapshotIndex.add(r);

                        final long nentries = snapshotIndex.getEntryCount();

                        if (nentries % 1000 == 0) {

                            /*
                             * Provide an indication that the server is doing
                             * work during startup (it would be unusual to have
                             * a lot of snapshot files, but this provides
                             * symmetry with the HALog startup procedure).
                             */

                            haLog.warn("Indexed " + nentries
                                    + " snapshot files");

                        }

                    }

                } finally {

                    /*
                     * Ensure tasks are terminated.
                     */
                    
                    for (Future<SnapshotRecord> ft : futures) {

                        ft.cancel(true/* mayInterruptIfRunning */);

                    }
                    
                }
                
            } else if (f.isDirectory()) {

                /*
                 * Sequential recursion into a child directory.
                 */
                
                final File[] children = f.listFiles(fileFilter);

                for (int i = 0; i < children.length; i++) {

                    final File child = children[i];

                    populateIndexRecursive(executor, child, fileFilter, depth + 1);

                }

            } else {

                log.warn("Ignoring file in non-leaf directory: " + f);

            }

        }

    } // class InitTask
    
    private void ensureSnapshotDirExists() throws IOException {

        if (!snapshotDir.exists()) {

            // Create the directory.
            if (!snapshotDir.mkdirs())
                throw new IOException("Could not create directory: "
                        + snapshotDir);

        }

    }

    /**
     * Read the current root block out of the snapshot.
     * 
     * @param file
     *            the file.
     * @return The current root block from that file.
     * 
     * @throws IllegalArgumentException
     *             if argument is <code>null</code>.
     * @throws IOException
     *             if the file can not be read.
     * @throws ChecksumError
     *             if there is a checksum problem with the root blocks.
     */
    static public IRootBlockView getRootBlockForSnapshot(final File file)
            throws IOException {

        if(file == null)
            throw new IllegalArgumentException();
        
        final byte[] b0 = new byte[RootBlockView.SIZEOF_ROOT_BLOCK];
        final byte[] b1 = new byte[RootBlockView.SIZEOF_ROOT_BLOCK];
        
        final DataInputStream is = new DataInputStream(new GZIPInputStream(
                new FileInputStream(file), FileMetadata.headerSize0));

        try {

            final int magic = is.readInt();

            if (magic != FileMetadata.MAGIC)
                throw new IOException("Bad journal magic: expected="
                        + FileMetadata.MAGIC + ", actual=" + magic);

            final int version = is.readInt();

            if (version != FileMetadata.CURRENT_VERSION)
                throw new IOException("Bad journal version: expected="
                        + FileMetadata.CURRENT_VERSION + ", actual=" + version);

            // read root blocks.
            is.readFully(b0);
            is.readFully(b1);

        } finally {

            is.close();

        }

        final IRootBlockView rb0 = new RootBlockView(true, ByteBuffer.wrap(b0),
                ChecksumUtility.getCHK());

        final IRootBlockView rb1 = new RootBlockView(true, ByteBuffer.wrap(b1),
                ChecksumUtility.getCHK());

        final IRootBlockView currentRootBlock = RootBlockUtility
                .chooseRootBlock(rb0, rb1);

        return currentRootBlock;

    }

    /**
     * Add a snapshot to the {@link #snapshotIndex}.
     * 
     * @param file
     *            The snapshot file.
     * 
     * @throws IllegalArgumentException
     *             if argument is <code>null</code>.
     * @throws IOException
     *             if the file can not be read.
     * @throws ChecksumError
     *             if there is a checksum problem with the root blocks.
     */
    private void addSnapshot(final File file) throws IOException {

        snapshotIndex.add(getSnapshotRecord(file));

    }

    /**
     * Create a {@link SnapshotRecord} from a file.
     * 
     * @param file
     *            The snapshot file.
     * 
     * @throws IllegalArgumentException
     *             if argument is <code>null</code>.
     * @throws IOException
     *             if the file can not be read.
     * @throws ChecksumError
     *             if there is a checksum problem with the root blocks.
     */
    private SnapshotRecord getSnapshotRecord(final File file) throws IOException {
        
        if (file == null)
            throw new IllegalArgumentException();
        
        // Validate the snapshot.
        final IRootBlockView currentRootBlock = getRootBlockForSnapshot(file);

        final long sizeOnDisk = file.length();

        return new SnapshotRecord(currentRootBlock, sizeOnDisk);
        
    }
    
    /**
     * Remove an snapshot from the file system and the {@link #snapshotIndex}.
     * 
     * @param file
     *            The snapshot file.
     *            
     * @return <code>true</code> iff it was removed.
     * 
     * @throws IllegalArgumentException
     *             if argument is <code>null</code>.
     */
    private boolean removeSnapshot(final File file) {

        if (file == null)
            throw new IllegalArgumentException();

        final IRootBlockView currentRootBlock;
        try {

            currentRootBlock = getRootBlockForSnapshot(file);
            
        } catch (IOException ex) {
            
            haLog.error("Could not read root block: " + file);
            
            return false;
            
        }

        final long commitTime = currentRootBlock.getLastCommitTime();
        
        final Lock lock = snapshotIndex.writeLock();
        
        lock.lock();
        
        try {

            final ISnapshotRecord tmp = (ISnapshotRecord) snapshotIndex
                    .lookup(commitTime);

            if (tmp == null) {

                log.error("Snapshot not in index? commitTime=" + commitTime);

                return false;

            }

            if (!currentRootBlock.equals(tmp.getRootBlock())) {

                log.error("Root blocks differ for index and snapshot: commitTime="
                        + commitTime
                        + ", snapshot="
                        + currentRootBlock
                        + ", indexRootBlock=" + tmp);

                return false;

            }

            // Remove the index entry for that commit time.
            snapshotIndex.remove(commitTime);

        } finally {
            
            lock.unlock();
            
        }

        // Remove the snapshot file on the disk.
        if (!file.delete()) {

            return false;

        }

        return true;

    }

    /**
     * Find the {@link ISnapshotRecord} for the most oldest snapshot (if any).
     * 
     * @return That {@link ISnapshotRecord} -or- <code>null</code> if there are no
     *         snapshots.
     */
    public ISnapshotRecord getOldestSnapshot() {

        return snapshotIndex.getOldestEntry();

    }

    /**
     * Find the {@link ISnapshotRecord} for the most recent snapshot (if any).
     * 
     * @return That {@link ISnapshotRecord} -or- <code>null</code> if there are no
     *         snapshots.
     */
    public ISnapshotRecord getNewestSnapshot() {
        
        return snapshotIndex.getNewestEntry();
        
    }
    
    /**
     * Return the {@link ISnapshotRecord} identifying the snapshot having the
     * largest lastCommitTime that is less than or equal to the given value.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The {@link ISnapshotRecord} of the identified snapshot -or-
     *         <code>null</code> iff there are no snapshots in the index that
     *         satisify the probe.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than ZERO (0L).
     */
    public ISnapshotRecord find(final long timestamp) {
        
        return snapshotIndex.find(timestamp);
        
    }

    /**
     * Return the {@link ISnapshotRecord} identifying the first snapshot whose
     * <em>commitTime</em> is strictly greater than the timestamp.
     * 
     * @param timestamp
     *            The timestamp. A value of ZERO (0) may be used to find the
     *            first snapshot.
     * 
     * @return The {@link ISnapshotRecord} for that snapshot -or-
     *         <code>null</code> if there is no snapshot whose timestamp is
     *         strictly greater than <i>timestamp</i>.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than ZERO (0L).
     */
    public ISnapshotRecord findNext(final long timestamp) {
        
        return snapshotIndex.findNext(timestamp);
        
    }

    /**
     * Find the oldest snapshot that is at least <i>minRestorePoints</i> old and
     * returns its commit counter.
     * 
     * @return The {@link ISnapshotRecord} for that snapshot -or-
     *         <code>null</code> if there is no such snapshot.
     */
    public ISnapshotRecord findByCommitCounter(final long commitCounter) {

        return snapshotIndex.findByCommitCounter(commitCounter);

    }
    
    /**
     * Return the snapshot that is associated with the specified ordinal index
     * (origin ZERO) counting backwards from the most recent snapshot (0)
     * towards the earliest snapshot (nsnapshots-1).
     * 
     * @param index
     *            The index.
     * 
     * @return The {@link ISnapshotRecord} for that snapshot -or-
     *         <code>null</code> if there is no such snapshot.
     */
    public ISnapshotRecord getSnapshotByReverseIndex(final int index) {
        
        return snapshotIndex.getEntryByReverseIndex(index);
        
    }
    
    /**
     * Return an iterator that will visit all known snapshots. The list will be
     * in order of increasing <code>commitTime</code>. This should also
     * correspond to increasing <code>commitCounter</code>.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterator<ISnapshotRecord> getSnapshots() {

        final ITupleIterator<ISnapshotRecord> itr = snapshotIndex
                .rangeIterator();

        return new Striterator(itr)
                .addFilter(new Resolver<ITupleIterator<ISnapshotRecord>, ITuple<ISnapshotRecord>, ISnapshotRecord>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    protected ISnapshotRecord resolve(ITuple<ISnapshotRecord> e) {
                        return e.getObject();
                    }
                });
    }
    
    /**
     * Delete all snapshots and any empty directories, but ensure that the
     * {@link #getSnapshotDir()} exists as a post-condition.
     * 
     * @throws IOException
     *             if any file can not be deleted.
     */
    public void deleteAllSnapshots() throws IOException {

        lock.lock();

        try {

            CommitCounterUtility.recursiveDelete(true/* errorIfDeleteFails */,
                    snapshotDir, SNAPSHOT_FILTER);

            snapshotIndex.removeAll();

            ensureSnapshotDirExists();

        } finally {

            lock.unlock();

        }

    }

    /**
     * Delete snapshots that are no longer required.
     * <p>
     * Note: If ZERO (0) is passed into this method, then no snapshots will be
     * deleted. This is because the first possible commit counter is ONE (1).
     * <p>
     * Note: As a special case, if there are no snapshots then we DO NOT pin the
     * HALogs and return {@link Long#MAX_VALUE} so all HALogs except the live
     * log will be purged at each commit.
     * 
     * @param earliestRestorableCommitPoint
     *            The earliest commit counter that we need to be able to restore
     *            from local backups.
     * 
     * @return The commitCounter of the earliest retained snapshot.
     */
    public long deleteSnapshots(final long token,
            final long earliestRestorableCommitPoint) {

        // #of snapshots.
        final long nbefore = snapshotIndex.getEntryCount();
        
        if (haLog.isInfoEnabled())
            log.info("token="
                    + token//
                    + ", earliestRestoreableCommitPoint="
                    + earliestRestorableCommitPoint//
                    + ", nsnapshots=" + nbefore);

        if (nbefore == 0L) {

            /*
             * As a special case, if there are no snapshots then we DO NOT pin
             * the HALogs.
             */
            
            return Long.MAX_VALUE;
            
        }
        
        /*
         * Iterator scanning all snapshots. We break out of the scan once we
         * encounter the first snapshot GTE the [earliestRestorableCommitPoint].
         */
        
        @SuppressWarnings("unchecked")
        final ITupleIterator<ISnapshotRecord> titr = snapshotIndex
                .rangeIterator();

        // #of snapshots that we wind up deleting.
        long ndeleted = 0L;
        
        // #of bytes on the disk for those deleted snapshots.
        long totalBytesReclaimed = 0L;
        
        while(titr.hasNext()) {

            final ISnapshotRecord r = titr.next().getObject();
            
            final IRootBlockView rb = r.getRootBlock();

            final long commitCounter = rb.getCommitCounter();
            
            // true iff we will delete this snapshot.
            final boolean deleteFile = commitCounter < earliestRestorableCommitPoint;

            final long len = r.sizeOnDisk();
            
            final File file = getSnapshotFile(commitCounter);

            if (haLog.isInfoEnabled())
                log.info("snapshotFile="
                        + file//
                        + ", sizeOnDisk="
                        + len//
                        + ", deleteFile="
                        + deleteFile//
                        + ", commitCounter="
                        + commitCounter//
                        + ", earliestRestoreableCommitPoint="
                        + earliestRestorableCommitPoint);

            if(!deleteFile) {
            
                // Break out of the scan.
                break;

            }
            
            if (!journal.getQuorum().isQuorumFullyMet(token)) {
                /*
                 * Halt operation.
                 * 
                 * Note: This is not an error, but we can not remove snapshots
                 * or HALogs if this invariant is violated.
                 */
                break;
            }

            if (!removeSnapshot(file)) {

                haLog.warn("COULD NOT DELETE FILE: " + file);

                continue;

            }

            ndeleted++;

            totalBytesReclaimed += len;

        }

        /*
         * If people specify NoSnapshotPolicy then backup is in their hands.
         * HALogs will not be retained beyond a fully met commit unless there is
         * a snapshot against which they can be applied..
         */

        // The earliest remaining snapshot.
        final ISnapshotRecord oldestSnapshot = snapshotIndex
                .getOldestEntry();

        /*
         * The commit counter for the earliest remaining snapshot and 0L if
         * there are no retained snapshots.
         */
        final long earliestRetainedSnapshotCommitCounter = oldestSnapshot == null ? 0L
                : oldestSnapshot.getRootBlock().getCommitCounter();

        if (haLog.isInfoEnabled())
            haLog.info("PURGED SNAPSHOTS: nbefore=" + nbefore + ", ndeleted="
                    + ndeleted + ", totalBytesReclaimed=" + totalBytesReclaimed
                    + ", earliestRestorableCommitPoint="
                    + earliestRestorableCommitPoint
                    + ", earliestRetainedSnapshotCommitCounter="
                    + earliestRetainedSnapshotCommitCounter);

        return earliestRetainedSnapshotCommitCounter;

    }

    /**
     * Return the {@link Future} of the current snapshot operation (if any).
     * 
     * @return The {@link Future} of the current snapshot operation -or-
     *         <code>null</code> if there is no snapshot operation running.
     */
    public Future<IHASnapshotResponse> getSnapshotFuture() {

        lock.lock();

        try {

            if (snapshotFuture != null) {

                if (!snapshotFuture.isDone()) {

                    // Still running.
                    return snapshotFuture;

                }

                snapshotFuture = null;

            }

            return snapshotFuture;

        } finally {

            lock.unlock();

        }

    }

    /**
     * Conditionally take a snapshot of the journal iff there is no existing
     * snapshot. The journal may or may not be empty, but we do not have any
     * existing snapshots and we need to have one to serve as a restore point.
     * The service MUST be joined with a met quorum in order to take a snapshot.
     * <p>
     * Note: A snapshot WILL NOT be taken by this method if the
     * {@link NoSnapshotPolicy} is used. Snapshots MUST be requested using
     * {@link #takeSnapshot(IHASnapshotRequest)} when that policy is used.
     * 
     * @return <code>null</code> unless a snapshot was scheduled in by this
     *         method.
     */
    public Future<IHASnapshotResponse> takeInitialSnapshot() {

        lock.lock();

        try {

            if (getNewestSnapshot() != null) {

                // There are existing snapshot(s).
                return null;

            }

            if (snapshotPolicy instanceof NoSnapshotPolicy) {

                // No automatic snapshots.
                return null;

            }

            if (snapshotFuture != null) {

                if (!snapshotFuture.isDone()) {

                    // Still running. Return null since we did not schedule it.
                    return null;

                }

                snapshotFuture = null;

            }

            // Request the snapshot.
            return snapshotFuture = takeSnapshotNow();

        } finally {

            lock.unlock();

        }

    }
    
    /**
     * Take a new snapshot. This is a NOP if a snapshot is already being made.
     * <p>
     * Note: The service must be joined with a met quorum to take a snapshot.
     * This is checked here and also in HAJournal when we take the snapshot.
     * This is necessary in order to ensure that the snapshots are copies of a
     * journal state that the quorum agrees on, otherwise we could later attempt
     * to restore from an invalid state.
     * 
     * @return The {@link Future} if a snapshot is already being made -or- if a
     *         snapshot was started by the request and <code>null</code> if no
     *         snapshot will be taken in response to this request.
     */
    public Future<IHASnapshotResponse> takeSnapshot(final IHASnapshotRequest req) {

//        if (req == null)
//            throw new IllegalArgumentException();
        
        lock.lock();
        
        try {

            if (snapshotFuture != null) {

                if (!snapshotFuture.isDone()) {

                    // Still running.
                    return snapshotFuture;

                }

                snapshotFuture = null;

            }

            if (req == null) {

                /* Not running. No snapshot was scheduled (null request). */
                return null;
                
            }
            
            final long token = journal.getQuorum().token();

            if (!journal.getQuorum().getClient().isJoinedMember(token)) {

                haLog.warn("Service not joined with met quorum.");
                
                // This service is not joined with a met quorum.
                return null;
                
            }

            if (!isReadyToSnapshot(req)) {

                // Pre-conditions are not met.
                return null;

            }

            // Take the snapshot, return Future but save a reference.
            return snapshotFuture = takeSnapshotNow();

        } finally {

            lock.unlock();

        }

    }

    /**
     * Return the snapshot {@link File} associated with the commitCounter.
     * 
     * @param commitCounter
     *            The commit counter for the current root block on the journal.
     *            
     * @return The name of the corresponding snapshot file.
     */
    public File getSnapshotFile(final long commitCounter) {
        
        return getSnapshotFile(snapshotDir, commitCounter);
        
    }

    /**
     * Return the snapshot {@link File} associated with the commitCounter.
     * 
     * @param snapshotDir
     *            The directory in which the snapshot files are stored.
     * @param commitCounter
     *            The commit counter for the current root block on the journal.
     * 
     * @return The name of the corresponding snapshot file.
     */
    public static File getSnapshotFile(final File snapshotDir,
            final long commitCounter) {

        return CommitCounterUtility.getCommitCounterFile(snapshotDir,
                commitCounter, SNAPSHOT_EXT);

    }

    /**
     * Parse out the commitCounter from the file name.
     */
    public static long parseCommitCounterFile(final String name)
            throws NumberFormatException {

        return CommitCounterUtility.parseCommitCounterFile(name, SNAPSHOT_EXT);

    }
    
    /**
     * Find the commit counter for the most recent snapshot (if any). Count up
     * the bytes on the disk for the HALog files GTE the commitCounter of that
     * snapshot. If the size(halogs) as a percentage of the size(journal) is LTE
     * the given [percentLogSize], then we return [false] to indicate that no
     * snapshot should be taken.
     */
    public boolean isReadyToSnapshot(final IHASnapshotRequest req) {

        if (req == null)
            throw new IllegalArgumentException();

        final ISnapshotRecord newestSnapshot = snapshotIndex
                .getNewestEntry();

        final IRootBlockView snapshotRootBlock = newestSnapshot == null ? null
                : newestSnapshot.getRootBlock();

        if (snapshotRootBlock != null
                && journal.getRootBlockView().getCommitCounter() == snapshotRootBlock
                        .getCommitCounter()) {

            /*
             * We already have a snapshot for the most recent commit point on
             * the journal.
             */

            return false;

        }

        final long sinceCommitCounter = snapshotRootBlock == null ? 0L
                : snapshotRootBlock.getCommitCounter();

        // Get HALog bytes on disk since that commit counter (strictly GT).
        final long haLogBytesOnDisk = journal.getHALogNexus()
                .getHALogFileBytesSinceCommitCounter(sinceCommitCounter);

        /*
         * Figure out the size of the HALog files written since the last
         * snapshot as a percentage of the size of the journal.
         */
        
        // Note: This is the file size on the disk (or in memory). No locks
        // should be required.
        final long journalSize = journal.getBufferStrategy().getExtent();

        // size(HALogs)/size(journal) as percentage.
        final int actualPercentLogSize = (int) (100 * (((double) haLogBytesOnDisk) / ((double) journalSize)));

        final int thresholdPercentLogSize = req.getPercentLogSize();

        final boolean takeSnapshot = (actualPercentLogSize >= thresholdPercentLogSize);

        if (haLog.isInfoEnabled()) {

            haLog.info("sinceCommitCounter="
                    + sinceCommitCounter//
                    + ", haLogBytesOnDisk="
                    + haLogBytesOnDisk//
                    + ", journalSize="
                    + journalSize//
                    + ", thresholdPercentLogSize="
                    + thresholdPercentLogSize//
                    + ", percentLogSize="
                    + actualPercentLogSize//
                    + "%, takeSnapshot=" + takeSnapshot //
                    );

        }
        return takeSnapshot;

    }

    /**
     * Take a snapshot.
     * 
     * @return The {@link Future} of the task that is taking the snapshot. The
     *         {@link Future} will evaluate to {@link IHASnapshotResponse}
     *         containing the closing {@link IRootBlockView} on the snapshot.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private Future<IHASnapshotResponse> takeSnapshotNow() {

        final FutureTaskMon<IHASnapshotResponse> ft = new FutureTaskInvariantMon<IHASnapshotResponse>(
                new SnapshotTask(this), journal.getQuorum()) {

            @Override
            protected void establishInvariants() {
                /*
                 * The snapshot is only taken if we have a consensus that this
                 * service has state that is consistent with a met quorum. This
                 * prevents the service from creating backups that are
                 * potentially invalid.
                 */
                assertQuorumMet();
                assertJoined(journal.getQuorum().getClient().getServiceId());
            }

        };

        // Run task.
        journal.getExecutorService().submit(ft);

        return ft;

    }

    /**
     * Take a snapshot.
     */
    static private class SnapshotTask implements Callable<IHASnapshotResponse> {

        private final SnapshotManager snapshotManager;
        private final HAJournal journal;

        public SnapshotTask(final SnapshotManager snapshotManager) {

            if (snapshotManager == null)
                throw new IllegalArgumentException();

            this.snapshotManager = snapshotManager;

            this.journal = snapshotManager.journal;

        }

        @Override
		public IHASnapshotResponse call() throws Exception {

			// The quorum token (must remain valid through this operation).
			final long token = journal.getQuorumToken();

			if (!journal.getQuorum().getClient().isJoinedMember(token)) {

				/*
				 * Note: The service must be joined with a met quorum to take a
				 * snapshot. This is necessary in order to ensure that the
				 * snapshots are copies of a journal state that the quorum
				 * agrees on, otherwise we could later attempt to restore from
				 * an invalid state.
				 */

				throw new QuorumException("Service not joined with met quorum");

			}

			final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
					.getQuorum();

			// Grab a read lock.
			final long txId = journal.newTx(ITx.READ_COMMITTED);
			try {

				// Get all snapshot core data, including rootblocks and any
				// allocation data, setting
				// the current committed rootblock view
				final AtomicReference<IRootBlockView> rbv = new AtomicReference<IRootBlockView>();
				final Set<java.util.Map.Entry<Long, byte[]>> coreData = journal
						.snapshotAllocationData(rbv);

				final File file = snapshotManager.getSnapshotFile(rbv.get()
						.getCommitCounter());

				if (file.exists() && file.length() != 0L) {

					/*
					 * Snapshot exists and is not (logically) empty.
					 * 
					 * Note: The SnapshotManager will not recommend taking a
					 * snapshot if a snapshot already exists for the current
					 * commit point since there is no committed delta that can
					 * be captured by the snapshot.
					 * 
					 * This code makes sure that we do not attempt to overwrite
					 * a snapshot if we already have one for the same commit
					 * point. If you want to re-generate a snapshot for the same
					 * commit point (e.g., because the existing one is corrupt)
					 * then you MUST remove the pre-existing snapshot first.
					 */

					throw new IOException("File exists: " + file);

				}

				final File parentDir = file.getParentFile();

				// Make sure the parent directory(ies) exist.
				if (!parentDir.exists())
					if (!parentDir.mkdirs())
						throw new IOException("Could not create directory: "
								+ parentDir);

				/*
				 * Create a temporary file. We will write the snapshot here. The
				 * file will be renamed onto the target file name iff the
				 * snapshot is successfully written.
				 */
				final File tmp = File.createTempFile(
						SnapshotManager.SNAPSHOT_TMP_PREFIX,
						SnapshotManager.SNAPSHOT_TMP_SUFFIX, parentDir);

				DataOutputStream os = null;
				boolean success = false;
				try {

					os = new DataOutputStream(new GZIPOutputStream(
							new FileOutputStream(tmp)));

					// write out the file data.
					((IHABufferStrategy) journal.getBufferStrategy())
					// .writeOnStream(os, journal.getQuorum(), token);
							.writeOnStream(os, coreData, journal.getQuorum(),
									token);

					// flush the output stream.
					os.flush();

					// done.
					success = true;
				} catch (Throwable t) {
					/*
					 * Log @ ERROR and launder throwable.
					 */
					log.error(t, t);
					if (t instanceof Exception)
						throw (Exception) t;
					else
						throw new RuntimeException(t);
				} finally {

					if (os != null) {
						try {
							os.close();
						} finally {
							// ignore.
						}
					}

					/*
					 * Either rename the temporary file onto the target filename
					 * or delete the tempoary file. The snapshot is not
					 * considered to be valid until it is found under the
					 * appropriate name.
					 */
					if (success) {

						if (!journal.getQuorum().getClient()
								.isJoinedMember(token)) {
							// Verify before putting down the root blocks.
							throw new QuorumException(
									"Snapshot aborted: service not joined with met quorum.");
						}

						if (!tmp.renameTo(file)) {

							log.error("Could not rename " + tmp + " as " + file);

						} else {

							// Add to the set of known snapshots.
							snapshotManager.addSnapshot(file);

							if (haLog.isInfoEnabled())
								haLog.info("Captured snapshot: " + file
										+ ", commitCounter="
										+ rbv.get().getCommitCounter()
										+ ", length=" + file.length());

							/*
							 * Attempt to purge older snapshots and HALogs.
							 */

							if (quorum != null) {

								// This quorum member.
								final QuorumService<HAGlue> localService = quorum
										.getClient();

								if (localService != null) {

									localService.purgeHALogs(token);

								}

							}

						}

					} else {

						if (!tmp.delete()) {

							log.warn("Could not delete temporary file: " + tmp);

						}

					}

				}

				// Done.
				return new HASnapshotResponse(rbv.get());
			} finally {
				// Release the read lock.
				journal.abort(txId);
			}
		}

		public IHASnapshotResponse call2() throws Exception {

			// The quorum token (must remain valid through this operation).
			final long token = journal.getQuorumToken();

			if (!journal.getQuorum().getClient().isJoinedMember(token)) {

				/*
				 * Note: The service must be joined with a met quorum to take a
				 * snapshot. This is necessary in order to ensure that the
				 * snapshots are copies of a journal state that the quorum
				 * agrees on, otherwise we could later attempt to restore from
				 * an invalid state.
				 */

				throw new QuorumException("Service not joined with met quorum");

			}

			final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
					.getQuorum();

			// Grab a read lock.
			final long txId = journal.newTx(ITx.READ_COMMITTED);

			/*
			 * Get both root blocks (atomically).
			 * 
			 * Note: This is done AFTER we take the read-lock and BEFORE we copy
			 * the data from the backing store. These root blocks MUST be
			 * consistent for the leader's backing store because we are not
			 * recycling allocations (since the read lock has pinned them). The
			 * journal MIGHT go through a concurrent commit before we obtain
			 * these root blocks, but they are still valid for the data on the
			 * disk because of the read-lock.
			 */
			final IRootBlockView[] rootBlocks = journal.getRootBlocks();

			final IRootBlockView currentRootBlock = RootBlockUtility
					.chooseRootBlock(rootBlocks[0], rootBlocks[1]);

			final File file = snapshotManager.getSnapshotFile(currentRootBlock
					.getCommitCounter());

			if (file.exists() && file.length() != 0L) {

				/*
				 * Snapshot exists and is not (logically) empty.
				 * 
				 * Note: The SnapshotManager will not recommend taking a
				 * snapshot if a snapshot already exists for the current commit
				 * point since there is no committed delta that can be captured
				 * by the snapshot.
				 * 
				 * This code makes sure that we do not attempt to overwrite a
				 * snapshot if we already have one for the same commit point. If
				 * you want to re-generate a snapshot for the same commit point
				 * (e.g., because the existing one is corrupt) then you MUST
				 * remove the pre-existing snapshot first.
				 */

				throw new IOException("File exists: " + file);

			}

			final File parentDir = file.getParentFile();

			// Make sure the parent directory(ies) exist.
			if (!parentDir.exists())
				if (!parentDir.mkdirs())
					throw new IOException("Could not create directory: "
							+ parentDir);

			/*
			 * Create a temporary file. We will write the snapshot here. The
			 * file will be renamed onto the target file name iff the snapshot
			 * is successfully written.
			 */
			final File tmp = File.createTempFile(
					SnapshotManager.SNAPSHOT_TMP_PREFIX,
					SnapshotManager.SNAPSHOT_TMP_SUFFIX, parentDir);

			DataOutputStream os = null;
			boolean success = false;
			try {

				os = new DataOutputStream(new GZIPOutputStream(
						new FileOutputStream(tmp)));
				
				

				// Write out the file header.
				os.writeInt(FileMetadata.MAGIC);
				os.writeInt(FileMetadata.CURRENT_VERSION);

				// write out the root blocks.
				os.write(BytesUtil.toArray(rootBlocks[0].asReadOnlyBuffer()));
				os.write(BytesUtil.toArray(rootBlocks[1].asReadOnlyBuffer()));

				// write out the file data.
				((IHABufferStrategy) journal.getBufferStrategy())
						// .writeOnStream(os, journal.getQuorum(), token);
						.writeOnStream(os, null, journal.getQuorum(), token);

				// flush the output stream.
				os.flush();

				// done.
				success = true;
			} catch (Throwable t) {
				/*
				 * Log @ ERROR and launder throwable.
				 */
				log.error(t, t);
				if (t instanceof Exception)
					throw (Exception) t;
				else
					throw new RuntimeException(t);
			} finally {

				// Release the read lock.
				journal.abort(txId);

				if (os != null) {
					try {
						os.close();
					} finally {
						// ignore.
					}
				}

				/*
				 * Either rename the temporary file onto the target filename or
				 * delete the tempoary file. The snapshot is not considered to
				 * be valid until it is found under the appropriate name.
				 */
				if (success) {

					if (!journal.getQuorum().getClient().isJoinedMember(token)) {
						// Verify before putting down the root blocks.
						throw new QuorumException(
								"Snapshot aborted: service not joined with met quorum.");
					}

					if (!tmp.renameTo(file)) {

						log.error("Could not rename " + tmp + " as " + file);

					} else {

						// Add to the set of known snapshots.
						snapshotManager.addSnapshot(file);

						if (haLog.isInfoEnabled())
							haLog.info("Captured snapshot: " + file
									+ ", commitCounter="
									+ currentRootBlock.getCommitCounter()
									+ ", length=" + file.length());

						/*
						 * Attempt to purge older snapshots and HALogs.
						 */

						if (quorum != null) {

							// This quorum member.
							final QuorumService<HAGlue> localService = quorum
									.getClient();

							if (localService != null) {

								localService.purgeHALogs(token);

							}

						}

					}

				} else {

					if (!tmp.delete()) {

						log.warn("Could not delete temporary file: " + tmp);

					}

				}

			}

			// Done.
			return new HASnapshotResponse(currentRootBlock);

		}
    } // class SnapshotTask
    
    /**
     * Compute the digest of a snapshot file.
     * <p>
     * Note: The digest is only computed for the data beyond the file header.
     * This is for consistency with
     * {@link IHABufferStrategy#computeDigest(Object, MessageDigest)}
     * 
     * @param commitCounter
     *            The commit counter that identifies the snapshot.
     * @param digest
     *            The digest.
     *            
     * @throws IOException
     * @throws FileNotFoundException
     * @throws DigestException
     * 
     *             TODO We should pin the snapshot if we are reading it to
     *             compute its digest.
     */
    public void getDigest(final long commitCounter, final MessageDigest digest)
            throws FileNotFoundException, IOException, DigestException {

        final File file = getSnapshotFile(commitCounter);

        getSnapshotDigest(file, digest);
        
    }

    /**
     * Compute the digest of a snapshot file.
     * <p>
     * Note: The digest is only computed for the data beyond the file header.
     * This is for consistency with
     * {@link IHABufferStrategy#computeDigest(Object, MessageDigest)}
     * 
     * @param commitCounter
     *            The commit counter that identifies the snapshot.
     * @param digest
     *            The digest.
     * 
     * @throws IOException
     * @throws FileNotFoundException
     * @throws DigestException
     * 
     *             TODO We should pin the snapshot if we are reading it to
     *             compute its digest. Right now we could use either the
     *             {@link #lock} and/or the {@link SnapshotIndex#readLock()}.
     *             However if we are going to pin a specific file then we
     *             probably want a {@link NamedLock} for that file. 
     */
    static public void getSnapshotDigest(final File file,
            final MessageDigest digest) throws FileNotFoundException,
            IOException, DigestException {

        // Note: Throws FileNotFoundException.
        final GZIPInputStream is = new GZIPInputStream(
                new FileInputStream(file));

        try {

            if (log.isInfoEnabled())
                log.info("Computing digest: " + file);

            computeDigest(is, digest);

        } finally {

            is.close();

        }

    }

    private static void computeDigest(final InputStream is,
            final MessageDigest digest) throws DigestException, IOException {

        // The capacity of that buffer.
        final int bufferCapacity = Bytes.kilobyte32 * 4;

        // A byte[] with the same capacity as that ByteBuffer.
        final byte[] a = new byte[bufferCapacity];

        while (true) {

            // Read as much as we can.
            final int nread = is.read(a, 0/* off */, a.length);

            if (nread == -1) {

                // End of stream.
                return;

            }

            // update digest
            digest.update(a, 0/* off */, nread/* len */);

        }

    }

    /**
     * Copy the input stream to the output stream.
     * 
     * @param content
     *            The input stream.
     * @param outstr
     *            The output stream.
     * 
     * @throws IOException
     */
    static private void copyStream(final InputStream content,
            final OutputStream outstr) throws IOException {

        final byte[] buf = new byte[1024];

        while (true) {

            final int rdlen = content.read(buf);

            if (rdlen <= 0) {

                break;

            }

            outstr.write(buf, 0, rdlen);

        }

    }

    /**
     * Decompress a snapshot onto the specified file. The original file is not
     * modified.
     * 
     * @param src
     *            The snapshot.
     * @param dst
     *            The file onto which the decompressed snapshot will be written.
     * 
     * @throws IOException
     *             if the source file does not exist.
     * @throws IOException
     *             if the destination file exists and is not empty.
     * @throws IOException
     *             if there is a problem decompressing the source file onto the
     *             destination file.
     */
    public static void decompress(final File src, final File dst)
            throws IOException {

        if (!src.exists())
            throw new FileNotFoundException(src.getAbsolutePath());

        if (dst.exists() && dst.length() != 0)
            throw new IOException("Output file exists and is not empty: "
                    + dst.getAbsolutePath());

        if (log.isInfoEnabled())
            log.info("src=" + src + ", dst=" + dst);

        InputStream is = null;
        OutputStream os = null;
        try {
            is = new GZIPInputStream(new FileInputStream(src));
            os = new FileOutputStream(dst);
            copyStream(is, os);
            os.flush();
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException ex) {
                }
            if (os != null)
                try {
                    os.close();
                } catch (IOException ex) {
                }
        }

    }

}
