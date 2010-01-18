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
package com.bigdata.journal;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IOverflowHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.journal.Journal.Options;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.Name2Addr.EntrySerializer;
import com.bigdata.resources.OverflowManager;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.ShutdownHelper;

/**
 * Task compacts the journal state onto a caller specified file. This may be
 * used to compact a journal, to create backups, or to convert an in-memory
 * journal into a disk-based journal. The task reads the state of each named
 * index as of the selected commit point, writing the index entries in index
 * order onto the output journal. This process will typically both reduce the
 * the space on disk required by the (new) backing store and improve locality in
 * the (new) backing store.
 * <p>
 * Note: The new {@link Journal} WILL NOT include any historical commit points
 * other than the one selected by the caller specified <i>commitTime</i>.
 * <p>
 * Note: If any indices use references to raw records then they MUST define an
 * {@link IOverflowHandler} in order for the raw records to be copied to the new
 * store and those references updated in the index to point to the records in
 * the new store. For example, the {@link BigdataFileSystem} uses such
 * references and defines an {@link IOverflowHandler} so that the raw file
 * blocks will not be lost on overflow.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see Journal#compact(File)
 * 
 * @todo it would be easy enough to change the branching factor during this
 *       task.
 * 
 * @todo add listener api for start/end notice (and perhaps error).
 * 
 * @todo add export task that builds index segments from the indices on a
 *       journal and work this into {@link CompactJournalUtility} or a similar
 *       utility class.
 */
public class CompactTask implements Callable<Journal> {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(CompactTask.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    /** The source {@link Journal}. */
    final protected Journal oldJournal;

    /** The output {@link File}. */
    final protected File outFile;

    /** The caller specified commit time. */
    final protected long commitTime;

    // cause from first task to error.
    final protected AtomicReference<Throwable> firstCause = new AtomicReference<Throwable>();
    
    // #of tasks started.
    final protected AtomicInteger startCount = new AtomicInteger(0);
    
    // #of tasks completed successfully.
    final protected AtomicInteger doneCount = new AtomicInteger(0);
    
    /**
     * The task reads the state of each named index as of the given
     * commitTime and writes the index data in order on the output journal.
     * <p>
     * Note: Unlike the {@link IndexSegmentBuilder}, this does not produce
     * a perfect read-optimized index. However, in many cases this task does
     * significantly improve the locality of reference for the {@link BTree}s
     * and will discard any deleted data or data which has been
     * overrwritten.
     * 
     * @param src
     *            The source journal.
     * @param outFile
     *            The output file.
     * @param commitTime
     *            The commit time whose state will be compacted onto the
     *            output file (the first commit point whose commit time is
     *            LTE to the given commit time will be used).
     */
    public CompactTask(final Journal src, final File outFile,
            final long commitTime) {

        if (src == null)
            throw new IllegalArgumentException();

        if (outFile == null)
            throw new IllegalArgumentException();

        if (commitTime <= 0) {
            // invalid commit time.
            throw new IllegalArgumentException();
        }

        if (commitTime > src.getLastCommitTime())
            // time beyond the most recent commit time.
            throw new IllegalArgumentException();

        this.oldJournal = src;

        this.outFile = outFile;
        
        this.commitTime = commitTime;

    }

    /**
     * Compact the {@link #oldJournal} journal onto the {@link #outFile}
     * file.
     * 
     * @return The already open {@link Journal} iff this task succeeds. If
     *         the task fails, then the {@link Journal} (if created) will
     *         have been closed. If you are backing up data, then be sure to
     *         shutdown the returned {@link Journal} so that it can release
     *         its resources.
     */
    public Journal call() throws Exception {

        final Journal newJournal = createJournal();

        try {

            // copy all named indices.
            copyIndices(newJournal);

            // write a commit point (!!!)
            newJournal.commit();

            return newJournal;

        } catch (Throwable t) {

            try {

                // make sure that the output journal is closed.
                newJournal.close();

            } catch (Throwable t2) {

                log.warn("Could not close the new journal", t2);

                // ignore.

            }

            // rethrow the exception.
            throw new RuntimeException(t);

        }

    }

    /**
     * Create the output journal.
     * 
     * @return The output journal.
     */
    protected Journal createJournal() {

        // default properties from the source journal.
        final Properties p = oldJournal.getProperties();

        // set the file for the new journal.
        p.setProperty(Options.FILE, outFile.getAbsolutePath());

        if (p.getProperty(Options.CREATE_TEMP_FILE) != null) {

            // make sure that this property is turned off.
            p.setProperty(Options.CREATE_TEMP_FILE, "false");

        }

        if (p.getProperty(Options.BUFFER_MODE) != null) {

            BufferMode bufferMode = BufferMode.valueOf(p
                    .getProperty(Options.BUFFER_MODE));

            if (!bufferMode.isStable()) {

                /*
                 * Force the disk-only mode if the source journal was not
                 * stable.
                 */
                p.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

            }

        }

        return new Journal(p);

    }

    /**
     * Copy all named indices from the {@link #oldJournal} to the new
     * journal.
     * <p>
     * Note: This code is similar to that found in the
     * {@link OverflowManager}.
     * 
     * @param newJournal
     *            The new journal.
     */
    protected void copyIndices(final Journal newJournal) {

        final long begin = System.currentTimeMillis();

        // using read-committed view of Name2Addr
        final int nindices = (int) oldJournal.getName2Addr(commitTime)
                .rangeCount(null, null);

        // using read-committed view of Name2Addr
        final ITupleIterator itr = oldJournal.getName2Addr(commitTime)
                .rangeIterator(null, null);

        /*
         * This service will limit the #of indices that we process in parallel.
         * 
         * Note: Based on some (limited) experimentation, the store file is
         * reduced by the same amount regardless of parallel vs serial
         * processing of the index files.
         * 
         * Note: Too much parallelism here appears to slow things down.
         * 
         * FIXME The ParallelismLimitedExecutorService is broken (11/10/08).
         * This can be demonstrated if it is enabled for the pipeline join.
         * Therefore it has been taken out of service until it can be fixed.
         */
//        final ParallelismLimitedExecutorService service = new ParallelismLimitedExecutorService(
//                oldJournal.getExecutorService(), 3/* maxParallel */, 20/* queueCapacity */);
        
        final ThreadPoolExecutor service = (ThreadPoolExecutor)Executors.newFixedThreadPool(
                3/* maxParallel */, DaemonThreadFactory.defaultThreadFactory());
        
        while (itr.hasNext()) {

            final ITuple tuple = itr.next();

            final Entry entry = EntrySerializer.INSTANCE
                    .deserialize(new DataInputBuffer(tuple.getValue()));

            // Submit task to copy the index to the new journal.
            service.submit(new CopyIndexTask(newJournal, entry));

        }

        try {

            // shutdown the service and await termination.
            new ShutdownHelper(service, 60L/* logTimeout */, TimeUnit.SECONDS) {

                protected void logTimeout() {

                    if(INFO)
                    log.info("Waiting on task(s)" + ": elapsed="
                            + TimeUnit.NANOSECONDS.toMillis(elapsed())
                            + "ms, #active=" + service.getActiveCount());
                    
                }

            };

        } catch (InterruptedException e) {

            /*
             * Interrupted awaiting task completion. shutdown the service
             * immediately and rethrow the exception.
             */

            service.shutdownNow();
            
            throw new RuntimeException(e);

        }
        
        final long elapsed = System.currentTimeMillis() - begin;

        if (INFO)
            log.info("Copied " + nindices + " in " + elapsed + "ms");

    } // copyIndices

    /**
     * Copy an index to the new journal.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class CopyIndexTask implements Callable<Void> {
        
        /** The new journal. */
        protected final Journal newJournal;

        /**
         * An {@link Entry} from the {@link Name2Addr} index for an index
         * defined on the {@link #oldJournal}.
         */
        protected final Entry entry;

        /**
         * @param newJournal
         *            The new journal.
         * @param entry
         *            An {@link Entry} from the {@link Name2Addr} index for an
         *            index defined on the {@link #oldJournal}.
         */
        public CopyIndexTask(final Journal newJournal, final Entry entry) {

            if (newJournal == null)
                throw new IllegalArgumentException();
            if (entry == null)
                throw new IllegalArgumentException();

            this.newJournal = newJournal;

            this.entry = entry;

        }

        /**
         * Creates and index on the {@link #newJournal}, copies the data from
         * the index on the old journal, and then registers the new index on the
         * {@link #newJournal}.
         */
        public Void call() throws Exception {

            try {

                startCount.incrementAndGet();

                if (INFO)
                    log.info("Start: name=" + entry.name);
                
                // source index.
                final BTree oldBTree = oldJournal
                        .getIndex(entry.checkpointAddr);

                // #of index entries on the old index.
                final long entryCount = oldBTree.rangeCount();

                // clone index metadata.
                final IndexMetadata indexMetadata = oldBTree.getIndexMetadata()
                        .clone();

                /*
                 * Create and register the index on the new journal.
                 * 
                 * Note: This is essentially a variant of BTree#create() where
                 * we need to propagate the counter from the old BTree to the
                 * new BTree.
                 */

                /*
                 * Write metadata record on store. The address of that record is
                 * set as a side-effect on the metadata object.
                 */
                indexMetadata.write(newJournal);

                // note the current counter value.
                final long oldCounter = oldBTree.getCounter().get();

                if (INFO)
                    log.info("name=" + entry.name //
                            + ", entryCount=" + entryCount//
                            + ", checkpoint=" + oldBTree.getCheckpoint()//
                    );

                // Create checkpoint for the new B+Tree.
                final Checkpoint overflowCheckpoint = indexMetadata
                        .overflowCheckpoint(oldBTree.getCheckpoint());

                /*
                 * Write the checkpoint record on the store. The address of the
                 * checkpoint record is set on the object as a side effect.
                 */
                overflowCheckpoint.write(newJournal);

                /*
                 * Load the B+Tree from the store using that checkpoint record.
                 */
                final BTree newBTree = BTree
                        .load(newJournal, overflowCheckpoint
                                .getCheckpointAddr(), false/* readOnly */);

                // Note the counter value on the new BTree.
                final long newCounter = newBTree.getCounter().get();

                // Verify the counter was propagated to the new BTree.
                assert newCounter == oldCounter : "expected oldCounter="
                        + oldCounter + ", but found newCounter=" + newCounter;

                /*
                 * Copy the data from the B+Tree on the old journal into the
                 * B+Tree on the new journal.
                 * 
                 * Note: [overflow := true] since we are copying from the old
                 * journal onto the new journal.
                 */

                if (DEBUG)
                    log.debug("Copying data to new journal: name=" + entry.name
                            + ", entryCount=" + entryCount);

                newBTree.rangeCopy(oldBTree, null, null, true/* overflow */);

                /*
                 * Register the new B+Tree on the new journal.
                 */
                newJournal.registerIndex(entry.name, newBTree);

                if (DEBUG)
                    log.debug("Done with index: name=" + entry.name);

                doneCount.incrementAndGet();

            } catch (Throwable t) {

                firstCause.compareAndSet(null/* expect */, t);

            }

            return null;

        }

    }

}
