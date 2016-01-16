/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;

import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ITxCommitProtocol;

/**
 * Extended interface for distributed 2-phase transactions for an
 * {@link IBigdataFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IDistributedTransactionService extends ITransactionService {

    /**
     * An {@link IDataService} MUST invoke this method before permitting an
     * operation isolated by a read-write transaction to execute with access to
     * the named resources (this applies only to distributed databases). The
     * declared resources are used in the commit phase of the read-write tx to
     * impose a partial order on commits. That partial order guarantees that
     * commits do not deadlock in contention for the same resources.
     * 
     * @param tx
     *            The transaction identifier.
     * @param dataService
     *            The {@link UUID} an {@link IDataService} on which the
     *            transaction will write.
     * @param resource
     *            An array of the named resources which the transaction will use
     *            on that {@link IDataService} (this may be different for each
     *            operation submitted by that transaction to the
     *            {@link IDataService}).
     * 
     * @return {@link IllegalStateException} if the transaction is not an active
     *         read-write transaction.
     */
    public void declareResources(long tx, UUID dataService, String[] resource)
            throws IOException;

    /**
     * Callback by an {@link IDataService} participating in a two phase commit
     * for a distributed transaction. The {@link ITransactionService} will wait
     * until all {@link IDataService}s have prepared. It will then choose a
     * <i>commitTime</i> for the transaction and return that value to each
     * {@link IDataService}.
     * <p>
     * Note: If this method throws ANY exception then the task MUST cancel the
     * commit, discard the local write set of the transaction, and note that the
     * transaction is aborted in its local state.
     * 
     * @param tx
     *            The transaction identifier.
     * @param dataService
     *            The {@link UUID} of the {@link IDataService} which sent the
     *            message.
     * 
     * @return The assigned commit time.
     * 
     * @throws InterruptedException
     * @throws BrokenBarrierException
     * @throws IOException
     *             if there is an RMI problem.
     */
    public long prepared(long tx, UUID dataService) throws IOException,
            InterruptedException, BrokenBarrierException;

    /**
     * Sent by a task participating in a distributed commit of a transaction
     * when the task has successfully committed the write set of the transaction
     * on the live journal of the local {@link IDataService}. If this method
     * returns <code>false</code> then the distributed commit has failed and
     * the task MUST rollback the live journal to the previous commit point. If
     * the return is <code>true</code> then the distributed commit was
     * successful and the task should halt permitting the {@link IDataService}
     * to return from the {@link ITxCommitProtocol#prepare(long, long)} method.
     * 
     * @param tx
     *            The transaction identifier.
     * @param dataService
     *            The {@link UUID} of the {@link IDataService} which sent the
     *            message.
     * 
     * @return <code>true</code> if the distributed commit was successfull and
     *         <code>false</code> if there was a problem.
     * 
     * @throws IOException
     */
    public boolean committed(long tx, UUID dataService) throws IOException,
            InterruptedException, BrokenBarrierException;

}
