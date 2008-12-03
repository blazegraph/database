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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.io.IOException;
import java.rmi.Remote;

/**
 * A service for unique timestamps.
 * <p>
 * An instance of this service provides timestamps for a variety of purposes,
 * including:
 * <ul>
 * <li>transaction start times</li>
 * <li>transaction commit times</li>
 * <li>...</li>
 * </ul>
 * Transaction commit times are stored in {@link IRootBlockView}s of the
 * journals that participate in a given database and reported via
 * {@link IRootBlockView#getFirstCommitTime()} and
 * {@link IRootBlockView#getLastCommitTime()}).
 * <p>
 * While the transaction "start" and commit "times" these do not strictly
 * speaking have to be "times" they do have to be assigned using the same
 * measure, so this implies either a coordinated time server or a strictly
 * increasing counter. Regardless, we need to know "when" a transaction commits
 * as well as "when" it starts. Also note that we need to assign "commit times"
 * even when the operation is unisolated. This means that we have to coordinate
 * an unisolated commit on a store that is part of a distributed database with
 * the centralized transaction manager. This should be done as part of the group
 * commit since we are waiting at that point anyway to optimize IO by minimizing
 * syncs to disk.
 * <p>
 * A transaction identifier serves to uniquely distinguish transactions. The
 * practice is to use strictly increasing non-ZERO long integer values for
 * transaction identifiers. The strictly increasing requirement is introduced by
 * the MVCC protocol. The non-ZERO requirement is introduced by the APIs since
 * they interpret a 0L transaction identifier as an
 * <code>unisolated operation</code> - that is, not as a transaction at all -
 * see {@link ITx#UNISOLATED}.
 * <p>
 * It is much simpler to use a unique long integer counter for transaction
 * identifiers than to rely on timestamps. Counters are simpler because there is
 * no reliance on system clocks, which can be reset or adjusted by a variety of
 * mechanisms and which have to be continually synchronized across failover
 * transaction identifier services. In contract, a transaction identifier based
 * solely on a counter is much simpler but is decoupled from the notion of wall
 * clock time. In either case, a store SHOULD refuse a commit where the
 * transaction "commit time" appears to go backward.
 * <p>
 * If a counter is used to assign transaction start and commit "times" then the
 * service SHOULD keep the time at which that transaction identifier was
 * generated persistently on hand so that we are able to roll back the system to
 * the state corresponding to some moment in time (assuming that all relevant
 * history is on hand). When using a counter, there can be multiple transactions
 * that start or end within the resolution of the "moment". E.g., within the
 * same millisecond given a transaction identifier service associated with a
 * clock having millisecond precision (especially if it has less than
 * millisecond accuracy).
 * 
 * FIXME My recommendation based on the above is to go with a centralized time
 * service and to use a heartbeat to drive distributed commits of both isolated
 * and unisolated transactions. the group commit protocol can wait for a
 * suitable heartbeat to make the data restart safe.
 * 
 * @todo The transaction server should make sure that time does not go backwards
 *       when it starts up (with respect to the last time that it issued).
 * 
 * @todo Consider decoupling from the {@link ITimestampService} or folding it
 *       into this class since {@link ITimestampService} may get used to readily
 *       and the timestamp mechanisms of the {@link ITransactionManager} are
 *       quite specialized.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITimestampService extends Remote {

    /**
     * Return the next unique timestamp. Timestamps must be strictly increasing.
     * <p>
     * Note: This method MUST return strictly increasing values, even when it is
     * invoked by concurrent threads. While other implementations are possible
     * and may be more efficient, one way to insure thread safety is to
     * synchronize on some object such that the implementaiton exhibits a FIFO
     * behavior.
     * 
     * @throws IOException
     *             if there is an RMI problem.
     * 
     * @throws NullPointerException
     *             if the {@link ITimestampService} has not been discovered yet.
     * 
     * @see TimestampServiceUtil#nextTimestamp(ITimestampService)
     */
    public long nextTimestamp() throws IOException;

    /**
     * Notify the global transaction manager that a commit has been performed
     * with the given timestamp (which it assigned) and that it should update
     * its lastCommitTime iff the given commitTime is GT its current
     * lastCommitTime.
     * 
     * @param commitTime
     *            The commit time.
     * 
     * @throws IOException
     */
    public void notifyCommit(long commitTime) throws IOException;

    /**
     * Return the last commit time reported to the {@link ITimestampService}.
     * 
     * @return The last known commit time.
     * 
     * @throws IOException
     */
    public long lastCommitTime() throws IOException;
    
}
