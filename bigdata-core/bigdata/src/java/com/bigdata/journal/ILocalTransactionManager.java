/*

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
/*
 * Created on Feb 19, 2008
 */

package com.bigdata.journal;

import java.util.Set;

import com.bigdata.service.IServiceShutdown;
import com.bigdata.service.ITxState;

/**
 * Interface for managing local transaction state (the client side of the
 * {@link ITransactionService}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ILocalTransactionManager extends
        /* ITransactionManager, */IServiceShutdown {

    /**
     * The server side of the transaction manager (possibly remote, in which
     * case this may require the service to be discovered).
     */
    ITransactionService getTransactionService();
    
    /**
     * Return the local state for a transaction.
     * 
     * @param tx
     *            The transaction identifier.
     * 
     * @return The local state for the identified transaction -or-
     *         <code>null</code> if the start time is not mapped to either an
     *         active or prepared transaction.
     */
    ITx getTx(final long tx);
    
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
    long nextTimestamp();

    /**
     * Notify the global transaction manager that a commit has been performed
     * with the given timestamp (which it assigned) and that it should update
     * its lastCommitTime iff the given commitTime is GT its current
     * lastCommitTime.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @see ITransactionService#notifyCommit(long)
     */
    void notifyCommit(long commitTime);

    /**
     * Return a snapshot of the active transactions. 
     */
    ITxState[] getActiveTx();
    
}
