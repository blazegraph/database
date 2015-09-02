/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.ha.halog.HALogReader;
import com.bigdata.ha.halog.HALogWriter;
import com.bigdata.ha.halog.IHALogReader;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.journal.CommitCounterUtility;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

/**
 * Utility class may be used to apply HALog files to a {@link Journal}, rolling
 * it forward to a specific commit point. This class can decompress a snapshot
 * file for processing. It can also identify the most recent snapshot in the
 * snapshot directory, and then decompress that snapshot for processing. When
 * starting with a snapshot, the target journal file may be specified on the
 * command line.
 * 
 * @see #main(String[])
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HARestore {

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /** The journal to be rolled forward. */
    private final Journal journal;
    /**
     * The directory containing the HALog files to be applied to that journal.
     */
    private final File haLogDir;

    /**
     * 
     * @param journal
     *            The journal to be rolled forward.
     * @param haLogDir
     *            The directory containing the HALog files to be applied to that
     *            journal.
     */
    public HARestore(final Journal journal, final File haLogDir) {

        if (journal == null)
            throw new IllegalArgumentException();

        if (haLogDir == null)
            throw new IllegalArgumentException();

        this.journal = journal;

        this.haLogDir = haLogDir;

    }

    /**
     * Apply HALog files, rolling the {@link Journal} forward one commit point
     * at a time.
     * 
     * @param listCommitPoints
     *            When <code>true</code>, the HALog files are visited and their
     *            root blocks are validated and logged, but nothing is applied
     *            to the {@link Journal}.
     * @param haltingCommitCounter
     *            The last commit counter that will be applied (halting point
     *            for the restore).
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void restore(final boolean listCommitPoints,
            final long haltingCommitCounter) throws IOException,
            InterruptedException {

        // The initial root block on the journal
        final IRootBlockView initialRootBlock;

        /*
         * Dump out the journal's root block.
         */

        initialRootBlock = journal.getRootBlockView();

        if (haLog.isInfoEnabled())
            haLog.info("JOURNAL: file=" + journal.getFile()
                    + ", commitCounter=" + initialRootBlock.getCommitCounter()
                    + ", initialRootBlock=" + initialRootBlock);

        /*
         * Now roll through the HALog files for the commit points starting with
         * the first commit point not found on the Journal.
         */
        int nfound = 0;
        long totalBytes = 0L;
        IRootBlockView lastRootBlock = null;
        for (long cc = initialRootBlock.getCommitCounter();; cc++) {

            if (haltingCommitCounter != Long.MAX_VALUE
                    && cc > haltingCommitCounter) {

                /*
                 * Done. We have applied all desired HALog files.
                 */
                break;

            }

            final File logFile = HALogWriter.getHALogFileName(haLogDir, cc);

            if (!logFile.exists()) {

                /*
                 * We have run out of HALog files. It is possible that there are
                 * HALog files for future commit points, but there is no HALog
                 * file for this commit point.
                 */
                break;

            }

            final IHALogReader logReader = new HALogReader(logFile);

            try {

                if (logReader.isEmpty()) {

                    haLog.warn("Empty HALog: commitCounter=" + cc);

                    /*
                     * We can not continue once we hit an empty HALog file.
                     * 
                     * Note: An HALog file is empty until the closing root block
                     * is written onto the file. Thus, an empty HALog file can
                     * exist when the server was shutdown abruptly. Since the
                     * commit point was never applied, the HALog file is empty
                     * and we can not roll the database any further forward.
                     */
                    break;

                }

                // dump closing root block.
                final IRootBlockView lrb = logReader.getClosingRootBlock();

                if (haLog.isInfoEnabled())
                    haLog.info("HALog: commitCounter=" + lrb.getCommitCounter()
                            + ", closingRootBlock=" + lrb);

                // Verify HALog root block consistent with journal.
                assertRootBlocksConsistent(journal, lrb);

                nfound++;

                totalBytes += logFile.length();

                if (!listCommitPoints) {

                    /*
                     * Apply HALog and go through a local commit.
                     * 
                     * See HAJournalServer.RestoreTask() which already does
                     * this.
                     */
                    applyHALog(logReader);

                    journal.doLocalCommit(logReader.getClosingRootBlock());

                    lastRootBlock = logReader.getClosingRootBlock();
                    
                }

            } finally {

                logReader.close();

            }

        } // next commit point

        if (lastRootBlock != null) {

            /*
             * 
             * FIXME For some reason, we need to close and reopen the journal
             * before it can be used. This problem is documented in the test
             * suite. It does not effect use through main() but it is a problem
             * for progammatic reuse. 
             * 
             * The commented out code immediately below is NOT sufficient.
             */
            
//            (((IHABufferStrategy) journal.getBufferStrategy()))
//                    .resetFromHARootBlock(lastRootBlock);
//
//            journal.abort();
            
        }
        
        if (haLog.isInfoEnabled())
            haLog.info("HALogDir: nfound="
                    + nfound
                    + ", totalBytes="
                    + totalBytes
                    + (haltingCommitCounter == Long.MAX_VALUE ? ""
                            : ", haltingCommitCounter=" + haltingCommitCounter));

        if (!listCommitPoints) {

            final IRootBlockView finalRootBlock = journal.getRootBlockView();

            if (haLog.isInfoEnabled())
                haLog.info("JOURNAL: file=" + journal.getFile()
                        + ", commitCounter="
                        + finalRootBlock.getCommitCounter()
                        + ", finalRootBlock=" + finalRootBlock);

        }

    }

    /**
     * Apply the write set to the local journal.
     * 
     * @param r
     *            The {@link IHALogReader} for the HALog file containing
     *            the write set.
     *            
     * @throws IOException
     * @throws InterruptedException
     */
    private void applyHALog(final IHALogReader r) throws IOException,
            InterruptedException {

        final IBufferAccess buf = DirectBufferPool.INSTANCE.acquire();

        try {

            while (r.hasMoreBuffers()) {

                // get message and fill write cache buffer (unless WORM).
                final IHAWriteMessage msg = r.processNextBuffer(buf
                        .buffer());

                writeWriteCacheBlock(msg, buf.buffer());
                
            }

            haLog.warn("Applied HALog: closingCommitCounter="
                    + r.getClosingRootBlock().getCommitCounter());

        } finally {

            buf.release();

        }
    }

    /**
     * Write the raw {@link WriteCache} block onto the backing store.
     */
    private void writeWriteCacheBlock(final IHAWriteMessage msg,
            final ByteBuffer data) throws IOException, InterruptedException {

        setExtent(msg);
        
        /*
         * Note: the ByteBuffer is owned by the HAReceiveService. This just
         * wraps up the reference to the ByteBuffer with an interface that
         * is also used by the WriteCache to control access to ByteBuffers
         * allocated from the DirectBufferPool. However, release() is a NOP
         * on this implementation since the ByteBuffer is owner by the
         * HAReceiveService.
         */
        
        final IBufferAccess b = new IBufferAccess() {

            @Override
            public void release(long timeout, TimeUnit unit)
                    throws InterruptedException {
                // NOP
            }

            @Override
            public void release() throws InterruptedException {
                // NOP
            }

            @Override
            public ByteBuffer buffer() {
                return data;
            }
        };

        ((IHABufferStrategy) journal.getBufferStrategy())
                .writeRawBuffer(msg, b);
        
    }
    
    /**
     * Adjust the size on the disk of the local store to that given in the
     * message.
     * <p>
     * Note: When historical messages are being replayed, the caller needs
     * to decide whether the message should applied to the local store. If
     * so, then the extent needs to be updated. If not, then the message
     * should be ignored (it will already have been replicated to the next
     * follower).
     */
    private void setExtent(final IHAWriteMessage msg) throws IOException {

        try {

            ((IHABufferStrategy) journal.getBufferStrategy())
                    .setExtentForLocalStore(msg.getFileExtent());

        } catch (InterruptedException e) {

            throw new RuntimeException(e);

        } catch (RuntimeException t) {

            // Wrap with the HA message.
            throw new RuntimeException("msg=" + msg + ": " + t, t);
            
        }

    }

    /**
     * Apply HALog file(s) to a journal or snapshot file. If the file specified
     * is a snapshot, then it is uncompressed into the current working directory
     * to obtain a journal file and the HALogs are applied to that journal. If
     * the file specified is a journal, then the HALog files are simply rolled
     * forward against that journal. If the file is a directory, it is assumed
     * to be the snapshot directory. In this case, the most recent snapshot file
     * is located, decompressed to obtain a journal file, and then rolled
     * forward by applying any more recent HALog files.
     * <p>
     * Each HALog file represents a single native transaction on the database
     * and will advance the journal by one commit point. The journal will go
     * through a local commit protocol as each HALog is applied. HALogs will be
     * applied starting with the first commit point GT the current commit point
     * on the journal. You may optionally specify a stopping criteria, e.g., the
     * last commit point that you wish to restore. If no stopping criteria is
     * specified, then all HALog files in the specified directory will be
     * applied and the journal will be rolled forward to the most recent
     * transaction. The HALog files are not removed, making this process safe.
     * 
     * @param args
     *            <code>[options] journalOrSnapshotFileOrSnapshotDir haLogDir</code>
     * <br>
     *            where <code>journalOrSnapshotFileOrSnapshotDir</code> is the
     *            name of the journal file (ending in <code>.jnl</code>), the
     *            name of a specific snapshot file (ending in
     *            <code>.jnl.gz</code>), or the name of the snapshot directory
     *            (this is generally a directory named <code>snapshot</code>
     *            that is a child of the service directory) <br>
     *            where <code>haLogDir</code> is the name of a directory
     *            containing zero or more HALog files (this is generally a
     *            directory name <code>HALog</code> that is a child of the
     *            service directory)<br>
     *            where <code>options</code> are any of:
     *            <dl>
     *            <dt>-l</dt>
     *            <dd>List available commit points, but do not apply
     *            them. This option provides information about the current
     *            commit point on the journal and the commit points available in
     *            the HALog files.</dd>
     *            <dt>-h commitCounter</dt>
     *            <dd>The last commit counter that will be applied (halting
     *            point for restore).</dd>
     *            <dt>-o journalFile</dt>
     *            <dd>When restoring from a snapshot, this parameter specifies
     *            the name of the journal file to be created.  It is an error
     *            if the file exists (this utility will not overwrite an existing
     *            journal file).</dd>
     *            </dl>
     * 
     * @return <code>0</code> iff the operation was fully successful.
     * 
     * @throws IOException
     *             if an error occcur when reading an HALog or writing on the
     *             journal.
     * @throws NoSnapshotException
     *             if you specify a snapshot directory to be searched, but no
     *             snapshot files are found. This can happend you specify the
     *             wrong directory. It can also happen if you are using the
     *             {@link NoSnapshotPolicy} and never took a snapshot!
     * @throws RuntimeException
     *             if the {@link UUID}s or other critical metadata of the
     *             journal and the HALogs differ.
     */
    public static void main(final String[] args) throws IOException {

        if (args.length == 0) {

            usage(args);

            System.exit(1);

        }

        int i = 0;

        boolean listCommitPoints = false;
        
        String decompressTargetFile = null;

        // Defaults to Long.MAX_VALUE.
        long haltingCommitCounter = Long.MAX_VALUE;

        for (; i < args.length; i++) {

            String arg = args[i];

            if (!arg.startsWith("-")) {

                // End of options.
                break;

            }

            if (arg.equals("-l")) {

                listCommitPoints = true;

            }

            else if (arg.equals("-h")) {

                haltingCommitCounter = Long.parseLong(args[++i]);

            }

            else if (arg.equals("-o")) {

                decompressTargetFile = args[++i];

            }
            
            else
                throw new RuntimeException("Unknown argument: " + arg);

        }

        if (i != args.length - 2) {

            usage(args);

            System.exit(1);

        }

        // Journal file.
        File journalFile = new File(args[i++]);

        // HALogDir.
        final File haLogDir = new File(args[i++]);

        if(journalFile.isDirectory()) {

            /*
             * File is a directory.
             * 
             * We assume that it is the snapshot directory.
             * 
             * Locate the most recent snapshot in that directory structure.
             */
        
            final File tmp = CommitCounterUtility.findGreatestCommitCounter(
                    journalFile, SnapshotManager.SNAPSHOT_FILTER);

            if (tmp == null) {

                /*
                 * There are no snapshot files.
                 * 
                 * Note: This can happen if you specify the wrong directory. It
                 * can also happen if you are using the NoSnapshotPolicy and
                 * never took a snapshot!
                 */

                throw new NoSnapshotException("No snapshot file(s): "
                        + journalFile);

            }

            System.out.println("Most recent snapshot: " + tmp);

            journalFile = tmp;
            
        }
        
        if (journalFile.getName().endsWith(SnapshotManager.SNAPSHOT_EXT)) {

            /*
             * File is a snapshot.
             * 
             * Decompress the snapshot onto either a temporary file or the file
             * specified by the caller (in which case the file must not exist).
             */

            // source is the snapshot.
            final File in = journalFile;

            final long commitCounter = SnapshotManager
                    .parseCommitCounterFile(journalFile.getName());

            final File out;
            if (decompressTargetFile == null) {
                /*
                 * Temporary file in the current working directory
                 */
                out = File.createTempFile("restored-from-snapshot" + "-"
                        + commitCounter + "-", Journal.Options.JNL, journalFile
                        .getAbsoluteFile().getParentFile());
            } else {
                /*
                 * Decompress onto a file specified by the caller.
                 */
                out = new File(decompressTargetFile);
                if (out.exists()) {
                    // Do not decompress onto an existing file.
                    throw new IOException("File exists: " + out);
                }
            }

            System.out.println("Decompressing " + in + " to " + out);
            
            // Decompress the snapshot.
            SnapshotManager.decompress(in, out);

            journalFile = out;
            
        }

        /*
         * Log some metadata about the journal file.
         */
        {

            System.out.println("Journal File: " + journalFile);

            if (!journalFile.exists()) {

                System.err.println("No such file: " + journalFile);

                System.exit(1);

            }

            if (!journalFile.isFile()) {

                System.err.println("Not a regular file: " + journalFile);

                System.exit(1);

            }

            System.out.println("Length: " + journalFile.length());

            System.out.println("Last Modified: "
                    + new Date(journalFile.lastModified()));

        }

        /*
         * Open the journal.
         */
        try {

            final Properties properties = new Properties();

            {

                properties.setProperty(Options.FILE, journalFile.toString());

                if (listCommitPoints)
                    properties.setProperty(Options.READ_ONLY, "" + true);

                // properties.setProperty(Options.BUFFER_MODE,
                // BufferMode.Disk.toString());

            }

            final Journal journal = new Journal(properties);

            try {

                /*
                 * Apply zero or more HALog files to roll forward the journal.
                 */
                final HARestore util = new HARestore(journal, haLogDir);

                util.restore(listCommitPoints, haltingCommitCounter);

            } finally {

                journal.close();

            }

        } catch (Throwable t) {

            t.printStackTrace();

            System.exit(1);

        }

    }

    private static void usage(final String[] args) {

        System.err.println("usage: (-l|-h haltingCommitPoint|-o outputJournalFile) <journalFile|snapshotFile|snapshotDir> haLogDir");

    }

    /**
     * Verify that the HALog root block is consistent with the Journal's root
     * block.
     * 
     * @param jnl
     *            The journal.
     * @param lrb
     *            The HALog's root block.
     */
    private static void assertRootBlocksConsistent(final Journal jnl,
            final IRootBlockView lrb) {

        if (jnl == null)
            throw new IllegalArgumentException();

        if (lrb == null)
            throw new IllegalArgumentException();

        // Current root block.
        final IRootBlockView jrb = jnl.getRootBlockView();

        if (!jrb.getUUID().equals(lrb.getUUID())) {

            throw new RuntimeException("UUID differs: journal=" + jrb
                    + ", log=" + lrb);

        }

        if (!jrb.getStoreType().equals(lrb.getStoreType())) {

            throw new RuntimeException("StoreType differs: journal=" + jrb
                    + ", log=" + lrb);

        }

    }

}
