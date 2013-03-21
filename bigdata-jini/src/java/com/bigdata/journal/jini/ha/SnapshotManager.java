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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
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
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.HASnapshotResponse;
import com.bigdata.ha.msg.IHASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ITx;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.RootBlockView;
import com.bigdata.quorum.QuorumException;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.ChecksumUtility;

/**
 * Class to manage the snapshot files.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SnapshotManager {

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

    private final HAJournal journal;
    
    /**
     * @see HAJournalServer.ConfigurationOptions#SNAPSHOT_DIR
     */
    private final File snapshotDir;

    /**
     * @see HAJournalServer.ConfigurationOptions#SNAPSHOT_POLICY
     */
    private final ISnapshotPolicy snapshotPolicy;
    
    /**
     * @see HAJournalServer.ConfigurationOptions#RESTORE_POLICY
     */
    private final IRestorePolicy restorePolicy;
    
    /**
     * An in memory index over the last commit time of each snapshot. This is
     * populated when the {@link HAJournal} starts from the file system and
     * maintained as snapshots are taken or destroyed. All operations on this
     * index MUST be synchronized on its object monitor.
     * <p>
     * Note: This index is not strictly necessary. We can also visit the files
     * in the file system. However, the index makes it much faster to locate a
     * specific snapshot based on a commit time and provides low latency access
     * to the {@link IRootBlockView} for that snapshot (faster than opening the
     * snapshot file on the disk).
     */
    private final CommitTimeIndex snapshotIndex;

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
//     * An in memory index over the last commit time of each snapshot. This is
//     * populated when the {@link HAJournal} starts from the file system and
//     * maintained as snapshots are taken or destroyed. All operations on this
//     * index MUST be synchronized on its object monitor.
//     */
//    private CommitTimeIndex getSnapshotIndex() {
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

        if (!snapshotDir.exists()) {

            // Create the directory.
            snapshotDir.mkdirs();

        }

        snapshotPolicy = (ISnapshotPolicy) config.getEntry(
                HAJournalServer.ConfigurationOptions.COMPONENT,
                HAJournalServer.ConfigurationOptions.SNAPSHOT_POLICY,
                ISnapshotPolicy.class,//
                HAJournalServer.ConfigurationOptions.DEFAULT_SNAPSHOT_POLICY);

        restorePolicy = (IRestorePolicy) config.getEntry(
                HAJournalServer.ConfigurationOptions.COMPONENT,
                HAJournalServer.ConfigurationOptions.RESTORE_POLICY,
                IRestorePolicy.class, //
                HAJournalServer.ConfigurationOptions.DEFAULT_RESTORE_POLICY);

        snapshotIndex = CommitTimeIndex.createTransient();

        populateSnapshotIndex();
        
    }
    
    /**
     * Scans the {@link #snapshotDir} and populates the {@link #snapshotIndex}
     * from the root blocks in snapshot files found in that directory.
     * 
     * @throws IOException 
     */
    private void populateSnapshotIndex() throws IOException {

        /*
         * Delete any temporary files that were left lying around in the
         * snapshot directory.
         */
        {
            final File[] files;

            final File snapshotDir = getSnapshotDir();
            
            files = snapshotDir.listFiles(new FilenameFilter() {

                /**
                 * Return <code>true</code> iff the file is an HALog file that
                 * should be deleted.
                 * 
                 * @param name
                 *            The name of that HALog file (encodes the
                 *            commitCounter).
                 */
                @Override
                public boolean accept(final File dir, final String name) {

                    if (name.startsWith(SNAPSHOT_TMP_PREFIX)
                            && name.endsWith(SNAPSHOT_TMP_SUFFIX)) {

                        // One of our temporary files.
                        return true;
                        
                    }

                    return false;

                }
            });

            for(File file : files) {
                
                if(!file.delete()) {

                    log.warn("Could not delete temporary file: "+file);
                    
                }
                
            }
            
        }

        /*
         * List the snapshot files for this service.
         */
        final File[] files;
        {

            final File snapshotDir = getSnapshotDir();

            files = snapshotDir.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(final File dir, final String name) {

                    if (!name.endsWith(SNAPSHOT_EXT)) {
                        // Not an snapshot file.
                        return false;
                    }

                    return true;

                }
            });

        }
        
        /*
         * Populate the snapshot index from the file system.
         */
        for (File file : files) {

            addSnapshot(file);

        }

    }

    /**
     * Read the current root block out of the snapshot.
     * 
     * @param file
     *            the file.
     * @return The current root block from that file.
     * 
     * @throws IOException
     */
    static public IRootBlockView getRootBlockForSnapshot(final File file)
            throws IOException {

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

    void addSnapshot(final File file) throws IOException {

        /*
         * Validate the snapshot.
         * 
         * TODO If the root blocks are bad, then this will throw an
         * IOException and that will prevent the startup of the
         * HAJournalServer. However, if we start up the server with a known
         * bad snapshot *and* the snapshot is the earliest snapshot, then we
         * can not restore commit points which depend on that earliest
         * snapshot.
         * 
         * TODO A similar problem exists if any of the HALog files GTE the
         * earliest snapshot are missing, have bad root blocks, etc. We will
         * not be able to restore the commit point associated with that
         * HALog file unless it also happens to correspond to a snapshot.
         */
        final IRootBlockView currentRootBlock = getRootBlockForSnapshot(file);

        synchronized (snapshotIndex) {

            snapshotIndex.add(currentRootBlock);

        }

    }

    boolean removeSnapshot(final File file) {

        final IRootBlockView currentRootBlock;
        try {

            currentRootBlock = getRootBlockForSnapshot(file);
            
        } catch (IOException ex) {
            
            haLog.error("Could not read root block: " + file);
            
            return false;
            
        }

        final long commitTime = currentRootBlock.getLastCommitTime();
        
        synchronized (snapshotIndex) {

            final IRootBlockView tmp = (IRootBlockView) snapshotIndex
                    .lookup(commitTime);

            if (tmp == null) {

                log.error("Snapshot not in index? commitTime=" + commitTime);

                return false;

            }

            if (!currentRootBlock.equals(tmp)) {

                log.error("Root blocks differ for index and snapshot: commitTime="
                        + commitTime
                        + ", snapshot="
                        + currentRootBlock
                        + ", indexRootBlock=" + tmp);

                return false;

            }

            // Remove the index entry for that commit time.
            snapshotIndex.remove(commitTime);

        }

        // Remove the snapshot file on the disk.
        if (!file.delete()) {

            return false;

        }

        return true;

    }

    /**
     * Find the commit counter for the most recent snapshot (if any).
     * 
     * @return That commit counter -or- ZERO (0L) if there are no snapshots.
     */
    public long getMostRecentSnapshotCommitCounter() {
        
        return snapshotIndex.getMostRecentSnapshotCommitCounter();
        
    }
    
    /**
     * Return the {@link IRootBlock} identifying the snapshot having the largest
     * commitTime that is less than or equal to the given value.
     * 
     * @param timestamp
     *            The given timestamp.
     * 
     * @return The {@link IRootBlockView} of the identified snapshot -or-
     *         <code>null</code> iff there are no snapshots in the index that
     *         satisify the probe.
     * 
     * @throws IllegalArgumentException
     *             if <i>timestamp</i> is less than or equals to ZERO (0L).
     */
    public IRootBlockView find(final long timestamp) {
        
        return snapshotIndex.find(timestamp);
        
    }
    
    /**
     * Return a list of all known snapshots. The list consists of the
     * {@link IRootBlockView} for each snapshot. The list will be in order of
     * increasing <code>commitTime</code>. This should also correspond to
     * increasing <code>commitCounter</code>.
     * 
     * @return A list of the {@link IRootBlockView} for the known snapshots.
     */
    public List<IRootBlockView> getSnapshots() {

        final List<IRootBlockView> l = new LinkedList<IRootBlockView>();
        
        synchronized (snapshotIndex) {

            @SuppressWarnings("unchecked")
            final ITupleIterator<IRootBlockView> itr = snapshotIndex.rangeIterator();

            while(itr.hasNext()) {

                final ITuple<IRootBlockView> t = itr.next();

                final IRootBlockView rootBlock = t.getObject();

                l.add(rootBlock);

            }

        }

        return l;
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
     * Take a new snapshot. This is a NOP if a snapshot is already being made.
     * 
     * @return The {@link Future} if a snapshot is already being made -or- if a
     *         snapshot was started by the request and <code>null</code> if no
     *         snapshot will be taken in response to this request.
     * 
     * @throws Exception
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Future<IHASnapshotResponse> takeSnapshot(final IHASnapshotRequest req) {

        if (req == null)
            throw new IllegalArgumentException();
        
        lock.lock();
        
        try {

            if (snapshotFuture != null) {

                if (!snapshotFuture.isDone()) {

                    // Still running.
                    return snapshotFuture;

                }

                snapshotFuture = null;

            }

            /*
             * FIXME Handle remove of historical snapshots when they are no
             * longer required.
             */

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

        /*
         * Format the name of the file.
         * 
         * Note: The commit counter in the file name should be zero filled to 20
         * digits so we have the files in lexical order in the file system (for
         * convenience).
         */
        final String file;
        {

            final StringBuilder sb = new StringBuilder();

            final Formatter f = new Formatter(sb);

            f.format("%020d" + SNAPSHOT_EXT, commitCounter);
            f.flush();
            f.close();

            file = sb.toString();

        }

        return new File(snapshotDir, file);

    }

    /**
     * Find the commit counter for the most recent snapshot (if any). Count up
     * the bytes on the disk for the HALog files GTE the commitCounter of that
     * snapshot. If the size(halogs) as a percentage of the size(journal) is LTE
     * the given [percentLogSize], then we return [false] to indicate that no
     * snapshot should be taken.
     * <p>
     * Note: The service must be joined with a met quorum to take a snapshot.
     * This is checked here and also in HAJournal when we take the snapshot.
     * This is necessary in order to ensure that the snapshots are copies of a
     * journal state that the quorum agrees on, otherwise we could later attempt
     * to restore from an invalid state.
     */
    private boolean isReadyToSnapshot(final IHASnapshotRequest req) {

        if(req == null)
            throw new IllegalArgumentException();
        
        final long token = journal.getQuorum().token();
        
        if (!journal.getQuorum().getClient().isJoinedMember(token)) {

            haLog.warn("Service not joined with met quorum.");
            
            // This service is not joined with a met quorum.
            return false;
            
        }
        
        final long snapshotCommitCounter = snapshotIndex.getMostRecentSnapshotCommitCounter();

        if (journal.getRootBlockView().getCommitCounter() == snapshotCommitCounter) {

            /*
             * We already have a snapshot for the most recent commit point on
             * the journal.
             */
            return false;
            
        }
        
        /*
         * List the HALog files for this service.
         */
        final File[] files;
        {

            final File currentLogFile = journal.getHALogWriter().getFile();

            final String currentLogFileName = currentLogFile == null ? null
                    : currentLogFile.getName();

            final File logDir = journal.getHALogDir();

            files = logDir.listFiles(new FilenameFilter() {

                /**
                 * Return <code>true</code> iff the file is an HALog file
                 * associated with a commit counter GTE the commit counter of
                 * the most recent snaphot.
                 * 
                 * @param name
                 *            The name of that HALog file (encodes the
                 *            commitCounter).
                 */
                @Override
                public boolean accept(final File dir, final String name) {

                    if (!name.endsWith(IHALogReader.HA_LOG_EXT)) {
                        // Not an HALog file.
                        return false;
                    }

                    // filter out the current log file
                    if (currentLogFile != null
                            && name.equals(currentLogFileName)) {
                        /*
                         * The caller requested that we NOT purge the
                         * current HALog, and this is it.
                         */
                        return false;
                    }

                    // Strip off the filename extension.
                    final String logFileBaseName = name.substring(0,
                            IHALogReader.HA_LOG_EXT.length());

                    // Closing commitCounter for HALog file.
                    final long logCommitCounter = Long
                            .parseLong(logFileBaseName);

                    if (logCommitCounter >= snapshotCommitCounter) {
                        /*
                         * HALog is more recent than the current snapshot
                         */
                        return true;
                    }

                    return false;

                }
            });
            
        }

        /*
         * Count up the bytes in those HALog files.
         */

        long totalBytes = 0L;

        for (File file : files) {

            // #of bytes in that file.
            final long len = file.length();

            totalBytes += len;

        }

        /*
         * Figure out the size of the HALog files written since the last
         * snapshot as a percentage of the size of the journal.
         */
        
        final long journalSize = journal.size();

        // size(HALogs)/size(journal) as percentage.
        final int actualPercentLogSize = (int) (100 * (((double) totalBytes) / ((double) journalSize)));

        final int thresholdPercentLogSize = req.getPercentLogSize();

        final boolean takeSnapshot = (actualPercentLogSize >= thresholdPercentLogSize);

        if (haLog.isInfoEnabled()) {

            haLog.info("There are " + files.length
                    + " HALog files since the last snapshot occupying "
                    + totalBytes + " bytes.  The journal is currently "
                    + journalSize + " bytes.  The HALogs are " + actualPercentLogSize
                    + " of the journal on the disk.  A new snapshot should"
                    + (takeSnapshot ? "" : " not") + " be taken");

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
    Future<IHASnapshotResponse> takeSnapshotNow() {

        final FutureTask<IHASnapshotResponse> ft = new FutureTaskMon<IHASnapshotResponse>(
                new SnapshotTask(this));

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

            /*
             * Create a temporary file. We will write the snapshot here. The
             * file will be renamed onto the target file name iff the snapshot
             * is successfully written.
             */
            final File tmp = File.createTempFile(
                    SnapshotManager.SNAPSHOT_TMP_PREFIX,
                    SnapshotManager.SNAPSHOT_TMP_SUFFIX, file.getParentFile());

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
                        .writeOnStream(os, journal.getQuorum(), token);

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
                         * FIXME SNAPSHOTS: This is where we need to see whether
                         * or not we can release an earlier snapshot and the
                         * intervening HALog files.
                         */

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

    } // class SendStoreTask

    
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
     *             compute its digest.
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

        if (!dst.exists() && dst.length() == 0)
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
