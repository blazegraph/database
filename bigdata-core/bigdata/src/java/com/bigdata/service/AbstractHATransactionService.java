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
 * Created on Dec 18, 2008
 */
package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HATXSGlue;
import com.bigdata.ha.msg.IHAGatherReleaseTimeRequest;
import com.bigdata.ha.msg.IHANotifyReleaseTimeResponse;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ITransactionService;

/**
 * Adds local methods to support an HA aware {@link ITransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractHATransactionService extends
        AbstractTransactionService implements HATXSGlue {

    public AbstractHATransactionService(final Properties properties) {

        super(properties);
        
    }
    
    /**
     * Factory for the Gather task that will be executed by the follower.
     * 
     * @param leader
     *            The proxy for the quorum leader (the service that made this
     *            request). This is used to RMI back to the leader and therefore
     *            MUST be non- <code>null</code>.
     * @param serviceId
     *            The {@link UUID} of this service. This is required as part of
     *            the RMI back to the leader (so the leader knows which services
     *            responded) and therefore MUST be non-<code>null</code>.
     * @param req
     *            The request.
     * 
     * @return The task to run on the follower.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/673" >
     *      Native thread leak in HAJournalServer process </a>
     */
    abstract public Callable<IHANotifyReleaseTimeResponse> newGatherMinimumVisibleCommitTimeTask(
            final HAGlue leader, final UUID serviceId,
            final IHAGatherReleaseTimeRequest req);

    /**
     * Coordinate the update of the <i>releaseTime</i> on each service that is
     * joined with the met quorum.
     * 
     * @param newCommitCounter
     *            The commit counter that will be assigned to the new commit
     *            point.
     * @param newCommitTime
     *            The commit time that will be assigned to the new commit point.
     * @param joinedServiceIds
     *            The services that are joined with the met quorum as of an
     *            atomic decision point in {@link AbstractJournal#commitNow()}.
     * @param timeout
     *            The timeout for the release time consensus protocol.
     * @param units
     *            The units for that timeout.
     */
    abstract public IHANotifyReleaseTimeResponse updateReleaseTimeConsensus(
            final long newCommitCounter,
            final long newCommitTime,
            final UUID[] joinedServiceIds, final long timeout,
            final TimeUnit units) throws IOException, TimeoutException,
            InterruptedException, Exception;

    /**
     * Used to make a serviceJoin() MUTEX with the consensus protocol.
     */
    abstract public void runWithBarrierLock(Runnable r);

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to expose this method so it may be invoked when a follower
     * joins an existing quorum to set the consensus release time of the quorum
     * leader of the newly joined follower. This prevents the follower from
     * permitting new transaction starts against a commit point which has been
     * recycled by the quorum leader.
     */
    @Override
    public void setReleaseTime(final long newReleaseTime) {

        super.setReleaseTime(newReleaseTime);

    }

}
