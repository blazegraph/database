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

import java.text.NumberFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.counters.CounterSet;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.Bytes;

/**
 * Class encapsulates reporting API for resource (index and store files) events.
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
 * <p>
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
 * FIXME Revisit and refactor - this is incomplete as it stands. This is event
 * focused, which is fine. We also have the {@link CounterSet}s which allow us
 * to report statistics regarding classes of events.
 * 
 * @todo use {@link MDC} to put metadata into the logging context {thread, host,
 *       dataService, global index name, local index name (includes the index
 *       partition), etc}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResourceEvents {

    /**
     * Logger.
     * 
     * @todo change the logger configuration to write on a JMS queue or JINI
     *       discovered service in order to aggregate results from multiple
     *       hosts in a scale-out solution.
     */
    protected static final Logger log = Logger.getLogger(ResourceEvents.class);

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
    
    /**
     * Leading zeros without commas used to format the partition identifiers
     * into index segment file names.
     */
    static NumberFormat leadingZeros;

    static {

        cf = NumberFormat.getNumberInstance();

        cf.setGroupingUsed(true);

        fpf = NumberFormat.getNumberInstance();

        fpf.setGroupingUsed(false);

        fpf.setMaximumFractionDigits(2);

        leadingZeros = NumberFormat.getIntegerInstance();
        
        leadingZeros.setMinimumIntegerDigits(5);
        
        leadingZeros.setGroupingUsed(false);
        
    }

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
     * @param builder
     *            The object responsible for building the {@link IndexSegment}.
     */
    static public void notifyIndexSegmentBuildEvent(IndexSegmentBuilder builder) {

        if (INFO) {

            String name = builder.metadata.getName();
            String filename = builder.outFile.toString();
            int nentries = builder.plan.nentries;
            long elapsed = builder.elapsed;
            long commitTime = builder.getCheckpoint().commitTime;
            long nbytes = builder.getCheckpoint().length;
            
            // data rate in MB/sec.
            float mbPerSec = builder.mbPerSec;

            log.info("name=" + name + ", filename=" + filename + ", nentries="
                    + nentries + ", commitTime=" + commitTime + ", elapsed="
                    + elapsed + ", "
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
    static public void openTx(long startTime) {

        if (INFO)
            log.info(TimestampUtility.toString(startTime));

    }

    /**
     * Report completion of a transaction.
     * 
     * @param tx
     *            The transaction identifier.
     * @param revisionTime
     *            The timestamp assigned to the revisions written by the
     *            transactions when it commits (non-zero iff this was a writable
     *            transaction that committed successfully and zero otherwise).
     * @param aborted
     *            True iff the transaction aborted vs completing successfully.
     */
    static public void closeTx(long tx, long revisionTime, boolean aborted) {

        if (INFO)
            log.info("tx=" + tx + ", revisionTime=" + revisionTime
                    + ", aborted=" + aborted + ", elapsed="
                    + (revisionTime - tx));

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

//    /**
//     * Report an overflow event.
//     * 
//     * @param journal
//     */
//    public void notifyJournalOverflowEvent(AbstractJournal journal) {
//
//        if (INFO) {
//
//            log.info("filename=" + journal.getFile() + ", nextOffset="
//                    + journal.getBufferStrategy().getNextOffset() + ", extent="
//                    + journal.getBufferStrategy().getExtent() + ", maxExtent="
//                    + journal.getMaximumExtent());
//
//        }
//        
//    }

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

}
