/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.ha.QuorumServiceBase;
import com.bigdata.ha.halog.HALogReader;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.halog.IHALogWriter;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockUtility;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.jini.ha.HALogIndex.HALogRecord;
import com.bigdata.journal.jini.ha.HALogIndex.IHALogRecord;
import com.bigdata.striterator.Resolver;
import com.bigdata.striterator.Striterator;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.ChecksumUtility;
import com.bigdata.util.InnerCause;

/**
 * A utility class to bring together the {@link HALogWriter}, an index over the
 * HALog files, and various other operations on the HALog directory.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HALogNexus implements IHALogWriter {

    private static final Logger log = Logger.getLogger(SnapshotManager.class);

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /**
     * The owning journal.
     */
    private final HAJournal journal;
    
    /**
     * The directory for the HALog files.
     */
    private final File haLogDir;
    
    /**
     * Write ahead log for replicated writes used to resynchronize services that
     * are not in the met quorum.
     * 
     * @see HAJournalServer.ConfigurationOptions#HA_LOG_DIR
     * @see HALogWriter
     */
    private final HALogWriter haLogWriter;

    /**
     * Lock to guard operations taken by the {@link HALogWriter}.
     */
    private final Lock logLock = new ReentrantLock(); 

    /**
     * The most recently observed *live* {@link IHAWriteMessage}.
     * <p>
     * Note: The {@link HALogWriter} will log live messages IFF they are
     * consistent with the state of the {@link HAJournalServer} when they are
     * received. In contrast, this field notices each *live* message that is
     * replicated along the HA pipline.
     * <p>
     * Note: package private - exposed to {@link HAJournalServer}.
     * 
     * @see QuorumServiceBase#handleReplicatedWrite(IHASyncRequest,
     *      IHAWriteMessage, ByteBuffer)
     */
    volatile IHAWriteMessage lastLiveHAWriteMessage = null;
    
    /*
     * Set to protect log files against deletion while a digest is
     * computed.  This is checked by deleteHALogs.
     */
    private  final AtomicInteger logAccessors = new AtomicInteger();

    /**
     * Filter visits all HALog files <strong>except</strong> the current HALog
     * file.
     * <p>
     * Note: The caller should own the {@link #logLock} in order to prevent
     * concurrent create or destroy of the current HALog file.
     */
    private final FileFilter HALOG_FILTER_EXCLUDES_CURRENT = new FileFilter() {

        @Override
        public boolean accept(final File f) {

            if (f.isDirectory())
                return true;

            final File currentLogFile = getHALogWriter().getFile();

            // filter out the current log file
            if (currentLogFile != null && f.equals(currentLogFile)) {
                /*
                 * This is the current HALog. We never purge it.
                 */
                return false;
            }

            return f.getName().endsWith(IHALogReader.HA_LOG_EXT);

        }

    };
    
    /**
     * The {@link HALogWriter} for this {@link HAJournal} and never
     * <code>null</code>.
     */
    public HALogWriter getHALogWriter() {

        return haLogWriter;
        
    }
    
    /**
     * The directory spanning the HALog files.
     */
    public File getHALogDir() {
        
        return haLogDir;
        
    }

    /**
     * Return the {@link Lock} to guard operations taken by the
     * {@link HALogWriter}.
     */
    Lock getLogLock() {
        
        return logLock;
        
    }
    
    /**
     * An in memory index over the last commit time the of each HALog. This is
     * populated when the {@link HAJournal} starts from the file system and
     * maintained as HALog files are created and destroyed.
     * <p>
     * Note: This index is not strictly necessary. We can also visit the files
     * in the file system. However, the index makes it MUCH faster to locate a
     * specific HALog based on a commit time and provides low latency access to
     * the {@link IRootBlockView} for that HALog (faster than opening the HALog
     * file on the disk). However, significant latency is associated with
     * scanning the file system during each 2-phase commit once there are 1000s
     * of HALogs. The use of this index eliminates that lantacy.
     * <p>
     * Note: We MUST NOT enter the live log into this index. Since the opening
     * and closing root blocks are the same when a new HALog file is created,
     * the live HAlog will have the same commit time as the previous HALog. This
     * will cause a collision in the index since they will be mapped to the same
     * key.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/670">
     *      Accumulating HALog files cause latency for HA commit</a>
     */
    private final HALogIndex haLogIndex;

    public HALogNexus(final HAJournalServer server,
            final HAJournal journal, final Configuration config)
            throws IOException, ConfigurationException {

        this.journal = journal;
        
        // Note: This is the effective service directory.
        final File serviceDir = server.getServiceDir(); 

        // Note: Default is relative to the serviceDir.
        haLogDir = (File) config
                .getEntry(
                        HAJournalServer.ConfigurationOptions.COMPONENT,
                        HAJournalServer.ConfigurationOptions.HA_LOG_DIR,
                        File.class,//
                        new File(
                                serviceDir,
                                HAJournalServer.ConfigurationOptions.DEFAULT_HA_LOG_DIR)//
                );

        if (!haLogDir.exists()) {

            // Create the directory.
            if (!haLogDir.mkdirs())
                throw new IOException("Could not create directory: "
                        + haLogDir);

        }

        // Set up the HA log writer.
        haLogWriter = new HALogWriter(haLogDir, journal.isDoubleSync());

        haLogIndex = HALogIndex.createTransient();

        // Make sure the snapshot directory exists.
        ensureHALogDirExists();
        
        /**
         * Populate the in-memory index from the directory.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/679" >
         *      HAJournalServer can not restart due to logically empty log files
         *      </a>
         */
        {

            /*
             * Used to detect a logically empty HALog (iff it is the last one in
             * commit order).
             */
            final HALogScanState tmp = new HALogScanState();
            
            // Scan the HALog directory, populating the in-memory index.
            populateIndexRecursive(haLogDir, IHALogReader.HALOG_FILTER, tmp);

            if (tmp.emptyHALogFile != null) {

                /*
                 * The last HALog file is logically empty. It WAS NOT added to
                 * the in-memory index. We try to remove it now.
                 * 
                 * Note: It is not critical that we succeed in removing this
                 * HALog file so long as it does not interfere with the correct
                 * startup of the HAJournalServer.
                 */
                final File f = tmp.emptyHALogFile;

                if (!f.delete()) {

                    log.warn("Could not remove empty HALog: " + f);

                }

            }
            
        }
        
    }

    private void ensureHALogDirExists() throws IOException {

        if (!haLogDir.exists()) {

            // Create the directory.
            if (!haLogDir.mkdirs())
                throw new IOException("Could not create directory: " + haLogDir);

        }

    }

    /**
     * State used to trace the scan of the HALog files on startup.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/679" >
     *      HAJournalServer can not restart due to logically empty log files
     *      </a>
     */
    private static class HALogScanState {
        /**
         * Flag is set the first time an empty HALog file is identified.
         * <p>
         * Note: We scan the HALog files in commit counter order. If the last
         * file is (logically) empty, then we will silently remove it. However,
         * if any other HALog file is logically empty, then this is an error.
         */
        File emptyHALogFile = null;
    }
    
    /**
     * Scans the {@link #haLogDir} and populates the {@link #haLogIndex} from
     * the root blocks in HALog files found in that directory.
     * <p>
     * Note: If the last HALog file (in commit counter sequence) is discovered
     * without a closing root block (the opening and closing root blocks are the
     * same) then it can not be used. The log will be identified as a
     * side-effect using the {@link HALogScanState} and will NOT be added to the
     * index. The caller SHOULD then remove the logically empty HALog file
     * 
     * TODO If an HALog is discovered to have bad checksums or otherwise corrupt
     * root blocks and there is a met quorum, then we should re-replicate that
     * HALog from the quourm leader.
     * 
     * TODO For HALog files other than the last HALog file (in commit counter
     * sequence) if there are any missing HALog files in the sequence, if any if
     * the files in the sequence other than the last HALog file is logically
     * empty, or if any of those HALog files has a bad root bloxks) then we
     * should either recover those HALog file(s) from another service joined
     * with the met quorum or fail this service (since it will not be able to
     * provide those log files to another service if it becomes the leader). If
     * we allow the service to start, then it will have limited rollback
     * capability. All of this could be checked in an index scan once we have
     * identified all of the HALog files in the file system.
     */
    private void populateIndexRecursive(final File f,
            final FileFilter fileFilter, final HALogScanState state)
            throws IOException {

        if (f.isDirectory()) {

            final File[] files = f.listFiles(fileFilter);

            /*
             * Sort into lexical order to force visitation in lexical order.
             * 
             * Note: This should work under any OS. Files will be either
             * directory names (3 digits) or filenames (21 digits plus the file
             * extension). Thus the comparison centers numerically on the digits
             * that encode either part of a commit counter (subdirectory) or an
             * entire commit counter (HALog file).
             */
            Arrays.sort(files);

            for (int i = 0; i < files.length; i++) {

                populateIndexRecursive(files[i], fileFilter, state);

            }

        } else {

            if (state.emptyHALogFile != null) {

                /*
                 * We already have an empty HALog file. If there are any more
                 * HALog files to visit then this is an error. There can be at
                 * most one empty HALog file and it must be the last HALog file
                 * in commit counter order (we are scanning in commit counter
                 * order).
                 */

                throw new LogicallyEmptyHALogException(state.emptyHALogFile);
                
            }
            
            try {

                // Attempt to add to the index.
                addHALog(f);

            } catch (LogicallyEmptyHALogException ex) {
                
                // Should be null since we checked this above.
                assert state.emptyHALogFile == null;

                /*
                 * The first empty HALog file. There is at most one allowed and
                 * it must be the last HALog file in commit counter order.
                 */
                state.emptyHALogFile = f;
                
            }

        }

    }

    /**
     * Read the current root block out of the HALog. If the root blocks are the
     * same then this will be the opening root block. Otherwise it is the
     * closing root block.
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
    private RootBlockUtility getRootBlocksForHALog(final File file)
            throws IOException {

        if (file == null)
            throw new IllegalArgumentException();
        
        final byte[] b0 = new byte[RootBlockView.SIZEOF_ROOT_BLOCK];
        final byte[] b1 = new byte[RootBlockView.SIZEOF_ROOT_BLOCK];
        
        final DataInputStream is = new DataInputStream(
                new FileInputStream(file));

        try {

            final int magic = is.readInt();

            if (magic != HALogWriter.MAGIC)
                throw new IOException("Bad journal magic: expected="
                        + HALogWriter.MAGIC + ", actual=" + magic);

            final int version = is.readInt();

            if (version != HALogWriter.VERSION1)
                throw new IOException("Bad version: expected="
                        + HALogWriter.VERSION1 + ", actual=" + version);

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

        return new RootBlockUtility(rb0, rb1);

    }

    /**
     * Add an HALog to the {@link #haLogIndex}.
     * 
     * @param file
     *            The HALog file.
     * 
     * @throws IllegalArgumentException
     *             if argument is <code>null</code>.
     * @throws LogicallyEmptyHALogException
     *             if the HALog file has opening and closing root blocks that
     *             are identical.
     * @throws IOException
     *             if the file can not be read.
     * @throws ChecksumError
     *             if there is a checksum problem with the root blocks.
     * 
     *             TODO If the root blocks are the same then this is an empty
     *             HALog. Right now that is an error. [We might want to simply
     *             remove any such HALog file.]
     *             <p>
     *             Likewise, it is an error if any HALog has bad root blocks
     *             (checksum or other errors).
     * 
     *             TODO A similar problem exists if any of the HALog files GTE
     *             the earliest snapshot are missing, have bad root blocks, etc.
     *             We will not be able to restore the commit point associated
     *             with that HALog file unless it also happens to correspond to
     *             a snapshot.
     */
    private void addHALog(final File file) throws IOException,
            LogicallyEmptyHALogException {

        if (file == null)
            throw new IllegalArgumentException();
        
        // Validate the HALog.
        final RootBlockUtility u = getRootBlocksForHALog(file);
        
        if (u.rootBlock0.getCommitCounter() == u.rootBlock1.getCommitCounter()) {
         
            /*
             * If this occurs during populateIndex() then we should simple
             * remove the HALog file.
             * 
             * If this occurs at other times, then it is a logic error. We MUST
             * NOT add an HALog file to the index if the opening and closing
             * root blocks are identical. The closing root block commit time is
             * used as the key for the index. If the opening and closing root
             * blocks are the same, then the closing commit time will be the
             * same as the closing commit time of the _previous_ HALog file and
             * they would collide in the index. DO NOT ADD THE LIVE HALOG FILE
             * TO THE INDEX.
             */

            throw new LogicallyEmptyHALogException(file);
            
        }

        final IRootBlockView closingRootBlock = u.chooseRootBlock();
        
        final long sizeOnDisk = file.length();

        haLogIndex.add(new HALogRecord(closingRootBlock, sizeOnDisk));

        final long nentries = haLogIndex.getEntryCount();

        if (nentries % 1000 == 0) {

            /*
             * Provide an indication that the server is doing work during
             * startup. If there are a lot of HALog files, then we can spend
             * quite a bit of time in this procedure.
             */

            haLog.warn("Indexed " + nentries + " HALog files");

        }
        
    }

    /**
     * Exception raise when an HALog file is logically empty (the opening and
     * closing root blocks are identicial).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class LogicallyEmptyHALogException extends IOException {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        public LogicallyEmptyHALogException(final File file) {

            super(file.getAbsolutePath());
            
        }
        
    }

    /**
     * Remove an snapshot from the file system and the {@link #haLogIndex}.
     * 
     * @param file
     *            The HALog file.
     * 
     * @return <code>true</code> iff it was removed.
     * 
     * @throws IllegalArgumentException
     *             if argument is <code>null</code>.
     */
    private boolean removeHALog(final File file) {

        if (file == null)
            throw new IllegalArgumentException();

        final IRootBlockView currentRootBlock;
        try {

            currentRootBlock = getRootBlocksForHALog(file).chooseRootBlock();
            
        } catch (IOException ex) {
            
            haLog.error("Could not read root block: " + file);
            
            return false;
            
        }

        final long commitTime = currentRootBlock.getLastCommitTime();
        
        final Lock lock = haLogIndex.writeLock();
        
        lock.lock();
        
        try {

            final IHALogRecord tmp = (IHALogRecord) haLogIndex
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
            haLogIndex.remove(commitTime);

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
     * Return an iterator that will visit all known HALogs. The list will be in
     * order of increasing <code>commitTime</code>. This should also correspond
     * to increasing <code>commitCounter</code>.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterator<IHALogRecord> getHALogs() {

        final ITupleIterator<IHALogRecord> itr = haLogIndex
                .rangeIterator();

        return new Striterator(itr)
                .addFilter(new Resolver<ITupleIterator<IHALogRecord>, ITuple<IHALogRecord>, IHALogRecord>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    protected IHALogRecord resolve(ITuple<IHALogRecord> e) {
                        return e.getObject();
                    }
                });
    }
    
    /**
     * Return the #of bytes in the HALog files since a given commit point.
     * <p>
     * Note: The current (live) HALog file is NOT in the reported total. The
     * total only reports the bytes on disk for the committed transactions.
     * <p>
     * Note: We do not include the HALog file if it was for the commit point of
     * the snapshot. We are only counting HALog file bytes that have NOT yet
     * been incorporated into a snapshot.
     * 
     * @param sinceCommitCounter
     *            The exclusive lower bound and <code>-1L</code> if the total
     *            bytes on disk for ALL HALog files should be reported.
     * 
     * @return The #of bytes in those HALog files.
     */
    @SuppressWarnings("unchecked")
    public long getHALogFileBytesSinceCommitCounter(
            final long sinceCommitCounter) {

        final ITupleIterator<IHALogRecord> titr;
        /*
         * The oldest HALog LTE the specified commit counter. We will start the
         * scan here.
         * 
         * Note: The keys in the index are commit times, but the caller is
         * providing a commitCounter. This lookup allows us to translate from
         * the commitCounter into a commitTime.
         */
        if (sinceCommitCounter == -1L) {

            // Scan all files.
            titr = haLogIndex.rangeIterator();

        } else {

            // Scan starting at the specified commit counter.
            final IHALogRecord start = haLogIndex
                    .findByCommitCounter(sinceCommitCounter);

            if (start == null) {

                // Scan all files.
                titr = haLogIndex.rangeIterator();

            } else {

                // Scan only the necessary range.
                titr = haLogIndex
                        .rangeIterator(
                                haLogIndex.getKey(start.getCommitTime()), null/* toKey */);

            }

        }

        long nfiles = 0L, nbytes = 0L;
        
        while(titr.hasNext()) {

            final IHALogRecord r = titr.next().getObject();

            if (r.getCommitCounter() < sinceCommitCounter) {

                /*
                 * This can occur due to a fence post. If [start] is LT rather
                 * than EQ to the specified commitCounter, then we will have to
                 * ignore one or more HALog files before we reach the ones of
                 * interest.
                 */
                
                continue;
                
            }
            
            nfiles++;
            
            nbytes += r.sizeOnDisk();

        }
        
//        /*
//         * List the HALog files for this service.
//         */
//        final File[] files;
//        {
//
////            // most recent snapshot commit counter or -1L if no snapshots exist.
////            final long snapshotCommitCounter = snapshotRootBlock == null ? -1L
////                    : snapshotRootBlock.getCommitCounter();
//            
//            final File currentLogFile = journal.getHALogWriter().getFile();
//
//            final String currentLogFileName = currentLogFile == null ? null
//                    : currentLogFile.getName();
//
//            final File logDir = journal.getHALogNexus().getHALogDir();
//
//            files = logDir.listFiles(new FilenameFilter() {
//
//                /**
//                 * Return <code>true</code> iff the file is an HALog file
//                 * associated with a commit counter GTE the commit counter of
//                 * the most recent snaphot.
//                 * 
//                 * @param name
//                 *            The name of that HALog file (encodes the
//                 *            commitCounter).
//                 */
//                @Override
//                public boolean accept(final File dir, final String name) {
//
//                    if (!name.endsWith(IHALogReader.HA_LOG_EXT)) {
//                        // Not an HALog file.
//                        return false;
//                    }
//
//                    if (currentLogFile != null
//                            && name.equals(currentLogFileName)) {
//                        // filter out the current log file
//                        return false;
//                    }
//
//                    // Strip off the filename extension.
//                    final String logFileBaseName = name.substring(0,
//                            name.length() - IHALogReader.HA_LOG_EXT.length());
//
//                    // Closing commitCounter for HALog file.
//                    final long logCommitCounter = Long
//                            .parseLong(logFileBaseName);
//
//                    if (logCommitCounter > sinceCommitCounter) {
//                        /*
//                         * HALog is more recent than the current snapshot
//                         * 
//                         * Note: We do not include the HALog file if it was for
//                         * the commit point of the snapshot. We are only
//                         * counting HALog file bytes that have NOT yet been
//                         * incorporated into a snapshot.
//                         */
//                        return true;
//                    }
//
//                    return false;
//
//                }
//            });
//            
//        }

        if (haLog.isInfoEnabled())
            haLog.info("sinceCommitCounter=" + sinceCommitCounter + ", files="
                    + nfiles + ", bytesOnDisk=" + nbytes);

        return nbytes;
        
    }
    
    /**
     * Protects logs from removal while a digest is being computed
     * @param earliestDigest
     */
    void addAccessor() {
    	if (logAccessors.incrementAndGet() == 1) {
    		if (log.isInfoEnabled())
    			log.info("Access protection added");
    	}
    }
    
    /**
     * Releases current protection against log removal
     */
    void releaseAccessor() {
    	if (logAccessors.decrementAndGet() == 0) {
    		if (log.isInfoEnabled())
    			log.info("Access protection removed");
    	}
    }
    
    /**
     * Delete HALogs that are no longer required.
     * 
     * @param earliestRetainedSnapshotCommitCounter
     *            The commit counter on the current root block of the earliest
     *            retained snapshot. We need to retain any HALogs that are GTE
     *            this commit counter since they will be applied to that
     *            snapshot.
     */
    void deleteHALogs(final long token,
            final long earliestRetainedSnapshotCommitCounter) {

        final long nfiles = haLogIndex.getEntryCount();
        
        long ndeleted = 0L, totalBytes = 0L;

        final Iterator<IHALogRecord> itr = getHALogs();
        
        while(itr.hasNext() && logAccessors.get() == 0) {
            
            final IHALogRecord r = itr.next();

            final long closingCommitCounter = r.getCommitCounter();
            
            final boolean deleteFile = closingCommitCounter < earliestRetainedSnapshotCommitCounter;

            if (!deleteFile) {

                // No more files to delete.
                break;

            }

            if (!journal.getQuorum().isQuorumFullyMet(token)) {
                /*
                 * Halt operation.
                 * 
                 * Note: This is not an error, but we can not remove
                 * snapshots or HALogs if this invariant is violated.
                 */
                break;
            }

            // The HALog file to be removed.
            final File logFile = getHALogFile(closingCommitCounter);

            // Remove that HALog file from the file system and our index.
            removeHALog(logFile);

            ndeleted++;

            totalBytes += r.sizeOnDisk();

        }

        if (haLog.isInfoEnabled())
            haLog.info("PURGED LOGS: nfound=" + nfiles + ", ndeleted="
                    + ndeleted + ", totalBytes=" + totalBytes
                    + ", earliestRetainedSnapshotCommitCounter="
                    + earliestRetainedSnapshotCommitCounter);

    }
    
    /**
     * Delete all HALog files (except the current one). The {@link #haLogIndex}
     * is updated as each HALog file is removed.
     * 
     * @throws IOException
     */
    void deleteAllHALogsExceptCurrent() throws IOException {

        logLock.lock();

        try {

            CommitCounterUtility.recursiveDelete(true/* errorIfDeleteFails */,
                    haLogDir, HALOG_FILTER_EXCLUDES_CURRENT);

            haLogIndex.removeAll();

            ensureHALogDirExists();
            
        } finally {

            logLock.unlock();
            
        }
        
    }

    /**
     * Return the HALog file associated with the specified commit counter.
     * 
     * @param commitCounter
     *            The closing commit counter (the HALog file is named for the
     *            commit counter that will be associated with the closing root
     *            block).
     * 
     * @return The HALog {@link File}.
     */
    public File getHALogFile(final long closingCommitCounter) {

        return HALogWriter
                .getHALogFileName(getHALogDir(), closingCommitCounter);

    }

    /*
     * IHALogWriter implementation.
     * 
     * Note: This could be used to migrate to the altha.HALogManager as a
     * replacement for the HALogWriter.
     */
    
    /**
     * Return the {@link IHALogReader} for the specified commit counter. If the
     * request identifies the HALog that is currently being written, then an
     * {@link IHALogReader} will be returned that will "see" newly written
     * entries on the HALog. If the request identifies a historical HALog that
     * has been closed and which exists, then a reader will be returned for that
     * HALog file. Otherwise, an exception is thrown.
     * 
     * @param commitCounter
     *            The commit counter associated with the commit point at the
     *            close of the write set (the commit counter that is in the file
     *            name).
     * 
     * @return The {@link IHALogReader}.
     * 
     * @throws IOException
     *             if the commitCounter identifies an HALog file that does not
     *             exist or can not be read.
     */
    public IHALogReader getReader(final long commitCounter)
            throws FileNotFoundException, IOException {
        
        return haLogWriter.getReader(commitCounter);
        
    }

    /**
     * Return the {@link IHALogReader} for the specified HALog file. If the
     * request identifies the HALog that is currently being written, then an
     * {@link IHALogReader} will be returned that will "see" newly written
     * entries on the HALog. If the request identifies a historical HALog that
     * has been closed and which exists, then a reader will be returned for that
     * HALog file. Otherwise, an exception is thrown.
     * 
     * @param logFile
     *            The HALog file.
     * 
     * @return The {@link IHALogReader}.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IOException
     *             if the HALog file does not exist or can not be read.
     */
    public IHALogReader getReader(final File logFile) throws IOException {

        if (logFile == null)
            throw new IllegalArgumentException();
        
        logLock.lock();

        try {

            if (haLogWriter.getFile().equals(logFile)) {

                /*
                 * This is the live HALog file.
                 */

                // The closing commit counter.
                final long cc = haLogWriter.getCommitCounter() + 1;

                return haLogWriter.getReader(cc);

            }

            /*
             * This is an historical HALog file.
             */

            return new HALogReader(logFile);

        } finally {

            logLock.unlock();

        }

    }

    /**
     * Open an HALog file for the write set starting with the given root block.
     * 
     * @param rootBlock
     *            The root block.
     */
    public void createHALog(final IRootBlockView rootBlock)
            throws FileNotFoundException, IOException {
        
        logLock.lock();

        try {

            haLogWriter.createLog(rootBlock);
            
        } finally {
            
            logLock.unlock();
            
        }
        
    }

    /**
     * Conditionally create the HALog.
     * 
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void conditionalCreateHALog() throws FileNotFoundException,
            IOException {

        logLock.lock();

        try {

            if (!isHALogOpen()) {

                /*
                 * Open the HALogWriter for our current root blocks.
                 * 
                 * Note: We always use the current root block when receiving an
                 * HALog file, even for historical writes. This is because the
                 * historical log writes occur when we ask the leader to send us
                 * a prior commit point in RESYNC.
                 */

                createHALog(journal.getRootBlockView());

            }

        } finally {

            logLock.unlock();

        }

    }
    
    @Override
    public boolean isHALogOpen() {
        
        logLock.lock();

        try {

            return haLogWriter.isHALogOpen();
            
        } finally {
            
            logLock.unlock();
            
        }

    }

    @Override
    public void closeHALog(final IRootBlockView rootBlock)
            throws IOException {

        logLock.lock();

        try {

            final long closingCommitCounter = rootBlock.getCommitCounter();

            final File file = getHALogFile(closingCommitCounter);

            haLogWriter.closeHALog(rootBlock);

            /*
             * Note: WE MUST enter HALog files into this index once they are
             * closed. This is true even if the thread is interrupted due to a
             * quorum break or service leave since otherwise we would not have
             * visibility into that HALog file while it would still be in the
             * file system.
             */
            boolean interrupted = false;
            while (true) {
                try {
                    addHALog(file);
                    break;
                } catch (Throwable t) {
                    if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                        interrupted = true;
                        continue;
                    }
                    throw new RuntimeException(t);
                }
            }
            if (interrupted) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
            }

        } finally {

            logLock.unlock();

        }

    }

    @Override
    public void disableHALog() throws IOException {

        logLock.lock();

        try {

            haLogWriter.disableHALog();

        } finally {

            logLock.unlock();

        }

   }

    @Override
    public void writeOnHALog(final IHAWriteMessage msg, final ByteBuffer data)
            throws IOException, IllegalStateException {

        logLock.lock();

        try {

            haLogWriter.writeOnHALog(msg, data);

        } finally {

            logLock.unlock();

        }

    }

    @Override
    public long getCommitCounter() {
        
        return haLogWriter.getCommitCounter();
        
    }

    @Override
    public long getSequence() {

        return haLogWriter.getSequence();
        
    }
    
}
