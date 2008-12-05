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

import com.bigdata.resources.ResourceManager;

/**
 * A service for unique timestamps.
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
     * 
     * @todo the NPE here should be removed from the javadoc. it wound up here
     *       because of the screwy delegation patterns.
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

    /**
     * Advance the release time, thereby releasing any read locks on views older
     * than the specified timestamp. The caller is responsible for ensuring that
     * the release time is advanced once there are no longer any readers for
     * timestamps earlier than the new release time.
     * <p>
     * Normally this is invoked by the transaction manager. Whenever a read
     * historical transaction completes it consults its internal state and
     * decides if there are existing readers still reading from an earlier
     * timestamp. If not, then it can choose to advance the release time. Data
     * services MAY release data for views whose timestamp is less than or equal
     * to the specified release time IFF that action would be in keeping with
     * their local history retention policy (minReleaseAge) AND if the data is
     * not required for the most current committed state (data for the most
     * current committed state is not releasable regardless of the release time
     * or the minReleaseAge).
     * 
     * @param releaseTime
     *            The timestamp.
     * 
     * @throws IOException
     * 
     * @see ResourceManager#getMinReleaseAge()
     */
    public void setReleaseTime(long releaseTime) throws IOException;
    
}
