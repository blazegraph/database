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
 * Created on Jun 13, 2010
 */

package com.bigdata.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import com.bigdata.ha.pipeline.HAReceiveService;
import com.bigdata.ha.pipeline.HASendService;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.quorum.Quorum;

/**
 * A non-remote interface for a member service in a {@link Quorum} defining
 * methods to replicating writes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumPipeline<S extends HAPipelineGlue> {

    /**
     * Return a {@link Future} for a task which will replicate an NIO buffer
     * along the write pipeline. This method is only invoked by the quorum
     * leader. The payload is replicated to the first follower in the write
     * pipeline. That follower will accept the payload (and replicate it if
     * necessary) using {@link #receiveAndReplicate(HAWriteMessage)}.
     * 
     * @param msg
     *            The RMI metadata about the payload.
     * @param b
     *            The payload.
     */
    Future<Void> replicate(HAWriteMessage msg, ByteBuffer b) throws IOException;

    /**
     * Return a {@link Future} for a task which will replicate an NIO buffer
     * along the write pipeline. This method is invoked for any node except the
     * master, including the last node in the failover chain.
     * 
     * @param msg
     *            The RMI metadata about the payload.
     */
    Future<Void> receiveAndReplicate(HAWriteMessage msg) throws IOException;

    /**
     * The service used by the leader to transmit NIO buffers to the next node
     * in the write pipeline. The returned object is valid as long as this node
     * remains the quorum leader.
     * 
     * @throws UnsupportedOperationException
     *             if the quorum is not highly available.
     */
    HASendService getHASendService();

    /**
     * The service used by the followers to accept and relay NIO buffers along
     * the write pipeline. The returned object is valid until the service
     * becomes the leader or disconnects from the {@link Quorum}.
     * 
     * @throws UnsupportedOperationException
     *             if the quorum is not highly available.
     */
    HAReceiveService<HAWriteMessage> getHAReceiveService();

}
