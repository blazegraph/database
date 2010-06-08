/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Jun 2, 2010
 */

package com.bigdata.quorum;

import java.util.UUID;

/**
 * An interface for informational quorum events. These events are intended for
 * clients interested in quorum state changes. Services that are HA aware use a
 * more intimate API to handle the state changes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumEvent {

    /**
     * The type of event and never <code>null</code>.
     */
    QuorumEventEnum getEventType();

    /**
     * The current quorum token at the time the event was generated.
     */
    long token();

    /**
     * The service identifier associated with the event, if any. For example,
     * when a service joins or leaves the quorum, this will be the {@link UUID}
     * of that service.
     */
    UUID getServiceId();

    /**
     * The lastCommitTime for which a vote was cast.
     * 
     * @return The lastCommitTime for which the vote was cast.
     * 
     * @throws UnsupportedOperationException
     *             unless {@link #getEventType()} returns
     *             {@link QuorumEventEnum#VOTE_CAST}
     */
    long lastCommitTime();

}
