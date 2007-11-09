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
 * Created on Mar 15, 2007
 */

package com.bigdata.journal;

import com.bigdata.service.IDataService;

/**
 * An interface implemented by an {@link IDataService} for the commit / abort of
 * the local write set for a transaction as directed by a centralized
 * {@link ITransactionManager} in response to client requests.
 * <p>
 * Clients DO NOT make direct calls against this API. Instead, they MUST locate
 * the {@link ITransactionManager} service and direct messages to that service.
 * <p>
 * Note: These methods should be invoked iff the transaction manager knows that
 * the {@link IDataService} is buffering writes for the transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Since the overall concurrency control algorithm is MVCC, the
 *       {@link ITransactionManager} becomes aware of the required locks during
 *       the active {@link ITx#isActive()} phase of the transaction. By the time
 *       the transaction is done executing and a COMMIT is requested by the
 *       client the {@link ITransactionManager} knows the set of resources
 *       (named indices) on which the transaction has written. As its first step
 *       in the commit protocol the {@link ITransactionManager} acquires the
 *       necessary exclusive locks on those resources. The locks are eventually
 *       released when the transaction either commits or aborts.
 * 
 * FIXME in order to support 2-/3-phase commit, the [commitTime] from the
 * transaction manager service must be passed through to the journal rather than
 * being returned from {@link #commit(long)}. There also needs to be a distinct
 * "prepare" message that validates the write set of the transaction and makes
 * it restart safe. finally, i have to coordinate the serialization of the wait
 * for the "commit" message. (The write set of the transaction also needs to be
 * restart safe when it indicates that it has "prepared" so that a commit will
 * eventually succeed.)
 */
public interface ITxCommitProtocol {

    /**
     * Request commit of the transaction write set (synchronous).
     * 
     * @param tx
     *            The transaction identifier.
     */
    public long commit(long tx) throws ValidationError;

    /**
     * Request abort of the transaction write set.
     * 
     * @param tx
     *            The transaction identifier.
     */
    public void abort(long tx);

//    /**
//     * Obtain a lock on a named index (synchronous).
//     * <p>
//     * Note: Clients DO NOT use this method. Locks are NOT required during the
//     * active phrase of a transaction. Locks are only required during the commit
//     * phase of a transaction where they are used to coordinate execution with
//     * concurrent unisolated operations. The transaction manager automatically
//     * acquires the necessary locks during the commit and will cause those locks
//     * to be released before the transaction is complete.
//     * 
//     * @param tx
//     *            The transaction identifier.
//     * @param name
//     *            The resource name(s).
//     */
//    public void lock(long tx,String resource[]);
//
//    /**
//     * Release all locks held by the transaction.
//     * 
//     * @param tx
//     *            The transaction identifier.
//     */
//    public void releaseLocks(long tx);
    
}
