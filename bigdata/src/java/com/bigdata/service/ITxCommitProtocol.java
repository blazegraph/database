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
/*
 * Created on Mar 22, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.rmi.Remote;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.ITuple;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ValidationError;

/**
 * Remote interface by which the {@link ITransactionService} manages the state
 * of transactions on the distributed {@link IDataService}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITxCommitProtocol extends Remote {

    /**
     * Notify a data service that it MAY release data required to support views
     * for up to the specified <i>releaseTime </i>. This is the mechanism by
     * which read locks are released. In effect, a read lock is a requirement
     * that the releaseTime not be advanced as far as the start time of the
     * transaction holding that read lock. Periodically and as transactions
     * complete, the transaction manager will advance the releaseTime, thereby
     * releasing read locks.
     * 
     * @param releaseTime
     *            The new release time (strictly advanced by the transaction
     *            manager).
     * 
     * @throws IllegalStateException
     *             if the read lock is set to a time earlier than its current
     *             value.
     * @throws IOException
     *             if there is an RMI problem.
     */
    public void setReleaseTime(long releaseTime) throws IOException;

    /**
     * Request abort of the transaction by the data service. This message is
     * sent in response to {@link ITransactionService#abort(long)} to each
     * {@link IDataService} on which the transaction has written. It is NOT sent
     * for read-only transactions since they have no local state on the
     * {@link IDataService}s.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @throws IllegalArgumentException
     *             if the transaction has not been started on this data service.
     * @throws IOException
     *             if there is an RMI problem.
     */
    public void abort(long tx) throws IOException;

    /**
     * Request commit of the transaction by the data service. In the case where
     * the transaction is entirely contained on the data service this method may
     * be used to both prepare (validate) and commit the transaction (a single
     * phase commit). Otherwise a 2-/3- phase commit is required and a separate
     * {@link #prepare(long)} message MUST be used.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The commit time assigned to that transaction.
     * 
     * @throws IllegalArgumentException
     *             if the transaction is read-only.
     * @throws IllegalStateException
     *             if the transaction is not known to the data service.
     * @throws InterruptedException
     *             if interrupted.
     * @throws ExecutionException
     *             This will wrap a {@link ValidationError} if validation fails.
     * @throws IOException
     *             if there is an RMI problem.
     */
    public long singlePhaseCommit(long tx) throws InterruptedException,
            ExecutionException, IOException;

//    /**
//     * Request that {@link IDataService} prepare for a 2-phase commit.
//     * 
//     * @param tx
//     *            The transaction identifier.
//     * @param revisionTime
//     *            The timestamp that will be written into the {@link ITuple}s
//     *            when the write set of the validated transaction is merged down
//     *            onto the unisolated indices.
//     * 
//     * @return The {@link Future} of the task running the commit protocol on the
//     *         {@link IDataService} (this may be used to cancel validation while
//     *         it is already in progress).
//     * 
//     * @throws IllegalArgumentException
//     *             if the transaction is read-only.
//     * @throws IllegalStateException
//     *             if the transaction is not known to the data service.
//     * @throws InterruptedException
//     *             if interrupted.
//     * @throws ExecutionException
//     *             This will wrap a {@link ValidationError} if validation fails.
//     * @throws IOException
//     *             if there is an RMI problem.
//     */
//    public Future<Void> twoPhasePrepare(long tx, long revisionTime)
//            throws InterruptedException, ExecutionException, IOException;

    /**
     * Request that the {@link IDataService} participate in a 3-phase commit.
     * <p>
     * When the {@link IDataService} is sent the {@link #prepare(long, long)}
     * message it executes a task which will handle commit processing for the
     * transaction. That task MUST hold exclusive locks for the unisolated
     * indices to which the transaction write sets will be applied. While
     * holding those locks, the task must first validate the transaction's write
     * set and then merge down the write set onto the corresponding unisolated
     * indices using the specified <i>revisionTime</i> and checkpoint the
     * indices in order to reduce all possible sources of latency. Note that
     * each {@link IDataService} is able to independently prepare exactly those
     * parts of the transaction's write set which are mapped onto index
     * partitions hosted by a given {@link IDataService}.
     * <p>
     * Once validation is complete and all possible steps have been taken to
     * reduce sources of latency (e.g., checkpoint the indices and pre-extending
     * the store if necessary), the task notifies the
     * {@link ITransactionService} that it has prepared using
     * {@link ITransactionService#prepared(long)}. The
     * {@link ITransactionService} will wait until all tasks have prepared. If a
     * task CAN NOT prepare the transaction, then it MUST throw an exception out
     * of its {@link #prepare(long, long)} method.
     * <p>
     * Once all tasks have send an {@link ITransactionService#prepared(long)}
     * message to the {@link ITransactionService}, it will assign a commitTime
     * to the transaction and permit those methods to return that commitTime to
     * the {@link IDataService}s. Once the task receives the assigned commit
     * time, it must obtain an exclusive write lock for the live journal (this
     * is a higher requirement than just an exclusive lock on the necessary
     * indices and will lock out all other write requests for the journal),
     * register the checkpointed indices on the commit list and then request a
     * commit of the journal using the specified commitTime. The task then
     * notifies the transaction service that it has completed its commit using
     * {@link ITransactionService#committed(long)} and awaits a response. If the
     * {@link ITransactionService} indicates that the commit was not successful,
     * the task rolls back the live journal to the prior commit point and throws
     * an exception out of {@link #prepare(long, long)}.
     * <p>
     * A sample flow for successful a distributed transaction commit is shown
     * below. This example shows two {@link IDataService}s on which the client
     * has written. (If the client only writes on a single data service then we
     * use a single-phase commit protocol).
     * 
     * <pre>
     * client -------+----txService----+--dataService1--+--dataService2--+...           
     *   | [1]                    
     *   | commit(tx) -------- + [2]
     *   |                     | prepare(tx,rev) +
     *   |                     | [3]             |
     *   |                     | prepare(tx,rev) ------------------+
     *   |                     |                 |                 |
     *   |                     | &lt;--prepared(tx) +                 |
     *   |                     |                                   |
     *   |                     | &lt;------------------- prepared(tx) +
     *   |                     |  
     *   |                  barrier
     *   |                     | [4]
     *   |                     | -- (commitTime) +  
     *   |                     | -------------------- (commitTime) +
     *   |                     | [5]             |                 |
     *   |                     | &lt;--committed(tx)------------------+  
     *   |                     | [6]             |
     *   |                     | &lt;--committed(tx)+
     *   |                     | 
     *   |                  barrier [7]
     *   |                     | [8]
     *   |                     | ------ (success)+  
     *   |                     | [9]             |
     *   |                     | (void)----------+
     *   |                     |                 halt
     *   |                     | [10]             
     *   |                     | ------------------------ (success)+
     *   |                     | [11]                              |
     *   |                     | (void)----------------------------+
     *   |                [12] |                                   halt
     *   | (commitTime)--------+  
     *   |                   
     * </pre>
     * 
     * <ul>
     * <li> [1] The client issues an {@link ITransactionService#commit(long)}
     * request, in which it specifies the transaction identifer (tx). </li>
     * <li> [2,3] The transaction service issues concurrent
     * {@link #prepare(long, long)} requests to the participating
     * {@link IDataService}s, specifying the transaction identifier (tx) and
     * the revision timestamp (rev) to be used and then waits at a barrier until
     * it receives {@link ITransactionService#prepared(long)} messages from
     * those {@link IDataService}s.</li>
     * <li> [4] When all participants have prepared, the barrier breaks and the
     * {@link ITransactionService} assigns a <i>commitTime</i> and returns that
     * commitTime as the return value for the prepared messages.</li>
     * <li> [5,6] Once the {@link IDataService} obtains that commitTime, it
     * proceeds with its atomic commit using the specified commitTime and then
     * sends an {@link ITransactionService#committed(long)} message to the
     * {@link ITransactionService}.</li>
     * <li> [7] The {@link ITransactionService} waits at another barrier.</li>
     * <li> [8,10] Once it has received an
     * {@link ITransactionService#committed(long)} message from each
     * participating {@link IDataService} the transaction has been successfully
     * committed and the barrier breaks. The {@link ITransactionService} now
     * lets the {@link ITransactionService#committed(long)} messages return
     * <code>true</code>, indicating success.</li>
     * <li> [9,11] The {@link IDataService}s return (void) from their
     * {@link #prepare(long, long)} message and the threads running their side
     * of the commit protocol halt.</li>
     * <li> [12] The {@link ITransactionService} returns the commit time which
     * it assigned and which was used by each participating {@link IDataService}
     * to the client.
     * </ul>
     * There are many points in the protocol where commit processing can fail.
     * However, there are two primary failure classifications that are of
     * interest for error handling. Up until the first barrier is satisified,
     * there is no side-effect on the persistent state so error handling need
     * only halt processing on the {@link IDataService}s and discard any local
     * state associated with the transaction and throw an exception out of
     * {@link #prepare(long, long)}. Once the first barrier has been
     * satisified, persistent side-effects MAY occur. Error handling in this
     * case must rollback the state of the live journal for each of the
     * participating {@link IDataService}s. If error handling was performed in
     * response to a local error, then the {@link IDataService} must throw that
     * error out of {@link #prepare(long, long)}. However, if error handling
     * was initiated because {@link ITransactionService#committed(long)}
     * returned <code>false</code> then it should return normally (after
     * rolling back the journal).
     * 
     * @param tx
     *            The transaction identifier.
     * @param revisionTime
     *            The timestamp that will be written into the {@link ITuple}s
     *            when the write set of the validated transaction is merged down
     *            onto the unisolated indices.
     * 
     * @throws Throwable
     *             if there is a problem during the execution of the commit
     *             protocol by the {@link IDataService}.
     * @throws IOException
     *             if there is an RMI problem.
     * 
     * @todo it may be possible to set the desired commit time on the abstract
     *       task (or a subclass specific to the distributed commit protocol)
     *       and then use that timestamp rather than requesting one from the
     *       {@link ITransactionService} in the group commit. This would allow
     *       us to use the normal commit processing.
     *       <p>
     *       If each distributed transaction gets its own commit time then we
     *       can not allow more than one distributed transaction into a given
     *       commit group. Therefore it seems that the
     *       {@link ITransactionService} would have to be able to assign the
     *       same commitTime to a set of distributed transactions that it knew
     *       were prepared together and would commit together. I can't quite see
     *       how that would work.
     *       <p>
     *       Failing that, we will need to exclude other tasks (or at least
     *       other distributed commit processing tasks) from the commit group.
     */
    public void prepare(long tx, long revisionTime) throws Throwable, IOException;

//    /**
//     * Cancel a 2-phase commit.
//     * 
//     * @param tx
//     *            The transaction identifier.
//     *            
//     * @throws IllegalArgumentException
//     *             if the transaction has not been started on this data service.
//     * @throws IllegalStateException
//     *             if the transaction is not participating in a 2-phase commit.
//     * @throws IOException
//     *             if there is an RMI problem.
//     */
//    public void twoPhaseCancel(long tx) throws IOException;
    
}
