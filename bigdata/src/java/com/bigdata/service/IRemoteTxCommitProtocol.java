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

import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ValidationError;

/**
 * Remote interface by which the centralized {@link ITransactionService} manages
 * the state of transactions on the distributed {@link IDataService}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRemoteTxCommitProtocol extends Remote {

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
     */
    public void setReleaseTime(long releaseTime) throws IOException;

    /**
     * Request preparation of a read-write transaction for a 2-phase commit.
     * 
     * @param tx
     *            The transaction identifier.
     *            
     * @throws IllegalArgumentException
     *             if the transaction has not been started on this data service.
     * @throws ValidationError
     *             if validation fails.
     * @throws IOException
     */
    public void prepare(long tx) throws ValidationError, IOException;

    /**
     * Request commit of the transaction by the data service. In the case where
     * the transaction is entirely contained on the data service this method may
     * be used to both prepare (validate) and commit the transaction (a single
     * phase commit). Otherwise a 2-/3- phase commit is required and a separate
     * {@link #prepare(long)} message MUST be used.
     * 
     * @param tx
     *            The transaction identifier.
     * @param commitTime
     *            The commit time assigned to that transaction.
     * 
     * @throws IllegalArgumentException
     *             if the transaction has not been started on this data service.
     * @throws IllegalArgumentException
     *             if the <i>commitTime</i> is LTE the last commit time
     *             performed on this data service.
     * @throws IllegalStateException
     *             if the transaction is a read-write transaction and it has not
     *             been {@link #prepare(long) prepared}
     * @throws IllegalStateException
     *             if the transaction is a read-write transaction and a timeout
     *             has invalidated {@link #prepare(long) prepared} commit.
     * @throws ValidationError
     *             if validation fails (single-phase commit only).
     * @throws IOException
     */
    public void commit(long tx, long commitTime) throws ValidationError,
            IOException;

    /**
     * Request abort of the transaction by the data service.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @throws IllegalArgumentException
     *             if the transaction has not been started on this data service.
     * @throws IOException
     */
    public void abort(long tx) throws IOException;

}
