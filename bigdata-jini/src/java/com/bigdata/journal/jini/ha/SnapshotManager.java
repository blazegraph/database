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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.NOPReopener;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;

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
    public final static String SNAPSHOT_EXT = ".snap";
    
    private HAJournal journal;
    
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
    private Future<IRootBlockView> snapshotFuture = null;
    
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
    
    /**
     * An in memory index over the last commit time of each snapshot. This is
     * populated when the {@link HAJournal} starts from the file system and
     * maintained as snapshots are taken or destroyed. All operations on this
     * index MUST be synchronized on its object monitor.
     */
    CommitTimeIndex getSnapshotIndex() {

        return snapshotIndex;
        
    }

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
         * List the snapshot files for this service.
         */
        final File[] files;
        {

            final File snapshotDir = getSnapshotDir();

            files = snapshotDir.listFiles(new FilenameFilter() {

                /**
                 * Return <code>true</code> iff the file is an HALog file
                 * that should be deleted.
                 * 
                 * @param name
                 *            The name of that HALog file (encodes the
                 *            commitCounter).
                 */
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
     * @return
     * @throws IOException
     * 
     *             FIXME DETECT EMPTY SNAPSHOTS!!!! (root blocks are as if for
     *             an empty journal) EMPTY SNAPSHOTS SHOULD BE REMOVED ON
     *             STARTUP AND ARE NOT VALID AND SHOULD NOT BE USED WHEN
     *             CHECKING FOR THE PREVIOUS SNAPSHOT, etc.  VERIFY THE
     *             ROOT BLOCK BEFORE RELYING ON A SNAPSHOT.
     */
    static IRootBlockView getRootBlockForSnapshot(final File file)
            throws IOException {

        final RandomAccessFile raf = new RandomAccessFile(file, "r");

        try {

            final IReopenChannel<FileChannel> opener = new NOPReopener(
                    raf.getChannel());

            final RootBlockUtility util = new RootBlockUtility(opener, file,
                    true/* validateChecksum */, false/* alternateRootBlock */,
                    false/* ignoreBadRootBlock */);

            final IRootBlockView currentRootBlock = util.chooseRootBlock();

            return currentRootBlock;

        } finally {

            raf.close();

        }

    }

    void addSnapshot(final File file) throws IOException {

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
                        + ",indexRootBlock=" + tmp);

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
     * Return the {@link Future} of the current snapshot operation (if any).
     * 
     * @return The {@link Future} of the current snapshot operation -or-
     *         <code>null</code> if there is no snapshot operation running.
     */
    public Future<IRootBlockView> getSnapshotFuture() {

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
    public Future<IRootBlockView> takeSnapshot(final int percentLogSize) {

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
             * FIXME lock should also cause purge of snapshots to be deleted if
             * we are in the middle of making a decision about whether or not to
             * make a new snapshot.
             */

            if (!isReadyToSnapshot(percentLogSize)) {

                // Pre-conditions are not met.
                return null;

            }

            // Take the snapshot, return Future but save a reference.
            return snapshotFuture = journal.takeSnapshotNow();

        } finally {

            lock.unlock();

        }

    }

    /**
     * Return the snapshot {@link File} associated with the commitCounter.
     * 
     * @param commitCounter
     * @return
     */
    public File getSnapshotFile(final long commitCounter) {

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
     * Find the commit counter for the most recent snapshot (if any).
     * 
     * @return That commit counter -or- ZERO (0L) if there are no snapshots.
     */
    private long getMostRecentSnapshotCommitCounter() {
        
        final long snapshotCommitCounter;
        synchronized (snapshotIndex) {

            final ITupleIterator<IRootBlockView> itr = snapshotIndex
                    .rangeIterator(null/* fromKey */, null/* toKey */,
                            1/* capacity */, IRangeQuery.DEFAULT
                                    | IRangeQuery.REVERSE/* flags */, null/* filter */);

            if (itr.hasNext()) {

                final IRootBlockView rootBlock = itr.next().getObject();

                snapshotCommitCounter = rootBlock.getCommitCounter();

            } else {

                snapshotCommitCounter = 0L;

            }

        }

        return snapshotCommitCounter;
        
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
    private boolean isReadyToSnapshot(final int percentLogSize) {

        final long token = journal.getQuorum().token();
        
        if (!journal.getQuorum().getClient().isJoinedMember(token)) {

            haLog.warn("Service not joined with met quorum.");
            
            // This service is not joined with a met quorum.
            return false;
            
        }
        
        final long snapshotCommitCounter = getMostRecentSnapshotCommitCounter();

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

        final double percent = ((double) totalBytes) / ((double) journalSize);
       
        final boolean takeSnapshot = (percent > percentLogSize);

        if (haLog.isInfoEnabled()) {

            haLog.info("There are " + files.length
                    + " HALog files since the last snapshot occupying "
                    + totalBytes + " bytes.  The journal is currently "
                    + journalSize + " bytes.  The HALogs are " + percent
                    + " of the journal on the disk.  A new snapshot should "
                    + (takeSnapshot ? "" : " not") + " be taken");

        }
        
        return takeSnapshot;

    }

}
