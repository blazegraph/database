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
 * Created on Feb 19, 2008
 */

package com.bigdata.journal;

import com.bigdata.service.IServiceShutdown;

/**
 * Interface for managing local transaction state (the client side of the
 * {@link ITransactionService}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo we don't really need an interface for this as there is only one impl.
 */
public interface ILocalTransactionManager extends /*ITransactionManager,*/ IServiceShutdown {

    /**
     * The server side of the transaction manager (possibly remote, in which
     * case this may require the service to be discovered).
     */
    public ITransactionService getTransactionService();
    
    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     * 
     * @todo test for transactions that have already been completed? that would
     *       represent a protocol error in the transaction manager service.
     */
    public void activateTx(ITx tx) throws IllegalStateException;
    
    /**
     * Notify the journal that a transaction has prepared (and hence is no
     * longer active).
     * 
     * @param tx
     *            The transaction
     * 
     * @throws IllegalStateException
     */
    public void preparedTx(ITx tx) throws IllegalStateException;

    /**
     * Notify the journal that a transaction is completed (either aborted or
     * committed).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     */
    public void completedTx(ITx tx) throws IllegalStateException;

    /**
     * Lookup an active or prepared transaction (exact match).
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The transaction -or- <code>null</code> if there is no such
     *         transaction.
     */
    public ITx getTx(long tx);
    
    /**
     * Return the next timestamp from the {@link ITransactionService}.
     * <p>
     * Note: This method is "robust" and will "retry"
     * {@link ITransactionService#nextTimestamp()} several times before giving
     * up.
     * 
     * @return The next timestamp assigned by the {@link ITransactionService}.
     * 
     * @throws RuntimeException
     *             if the service can not be resolved or the timestamp can not
     *             be obtained.
     * 
     * @see ITimestampService#nextTimestamp()
     */
    public long nextTimestamp();

    /**
     * Notify the global transaction manager that a commit has been performed
     * with the given timestamp (which it assigned) and that it should update
     * its lastCommitTime iff the given commitTime is GT its current
     * lastCommitTime.
     * 
     * @param commitTime
     *            The commit time.
     */
    public void notifyCommit(long commitTime);

}
