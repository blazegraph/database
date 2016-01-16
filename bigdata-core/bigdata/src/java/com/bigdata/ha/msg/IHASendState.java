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
package com.bigdata.ha.msg;

import java.io.Serializable;
import java.util.UUID;

/**
 * Interface for the state of the sender of an {@link IHAMessage}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHASendState extends Serializable {

    /**
     * A unique (one-up) message sequence identifier for the messages from the
     * sender. This identifier may be used to verify that the bytes available
     * from the replication stream are associated with the designed payload.
     */
    long getMessageId();

    /**
     * The {@link UUID} of the originating service. This may be used to verify
     * that a message was sourced the expected quorum leader.
     */
    UUID getOriginalSenderId();

    /**
     * The {@link UUID} of the sending service. This may be used to verify that
     * a message was sourced the expected upstream service.
     */
    UUID getSenderId();

    /**
     * The current quorum token on the sender.
     */
    long getQuorumToken();

    /**
     * The current replication factor on the sender.
     */
    int getReplicationFactor();

    /**
     * A byte[] marker that must prefix the message payload, needed to skip
     * stale data from failed read tasks.
     */
    byte[] getMarker();

}
