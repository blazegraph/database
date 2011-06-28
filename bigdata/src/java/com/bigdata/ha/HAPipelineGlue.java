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
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.util.concurrent.Future;

import com.bigdata.journal.WriteExecutorService;
import com.bigdata.journal.ha.HAWriteMessage;

/**
 * A {@link Remote} interface supporting the write replication pipeline. The
 * quorum leader accepts writes from the application layer. The writes are
 * formatted onto low-level cache blocks. Those cache blocks are replicated from
 * the quorum leader to the quorum followers along the pipeline.
 * 
 * @see QuorumPipeline
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface HAPipelineGlue extends Remote {

    /**
     * Return the address at which this service will listen for write pipeline
     * messages sent from the upstream service.
     * 
     * @todo This should be handled as a smart proxy so this method does not
     *       actually perform RMI.
     */ 
    InetSocketAddress getWritePipelineAddr();

    /**
     * Instruct the service to move to the end of the write pipeline. The leader
     * MUST be the first service in the write pipeline since it is the service
     * to which the application directs all write requests. This message is used
     * when a leader will be elected and it needs to force other services before
     * it in the write pipeline to leave/add themselves from/to the write
     * pipeline such that the leader becomes the first service in the write
     * pipeline.
     * 
     * @todo It is possible for the leader to issue these messages in an
     *       sequence which is designed to arrange the write pipeline such that
     *       it has a good network topology. The leader must first analyze the
     *       network topology for the services in the pipeline, decide on a good
     *       order, and then issue the requests to the services in that order.
     *       Each service will move to the end of the pipeline in turn. Once all
     *       services have been moved to the end of the pipeline, the pipeline
     *       will reflect the desired total order. However, note that transient
     *       network partitioning, zookeeper session timeouts, etc. can all
     *       cause this order to be perturbed.
     * 
     * @todo A pipeline leave currently causes a service leave. Reconsider this
     *       because it would mean that reorganizing the pipeline would actually
     *       cause service leaves, which would temporarily break the quorum and
     *       certainly distribute the leader election. If we can do a pipeline
     *       leave without a service leave, if services which are joined always
     *       rejoin the write pipeline when the observe a pipeline leave, and if
     *       we can force a service leave (by instructing the service to
     *       {@link #bounceZookeeperConnection()}) if the service fails to
     *       rejoin the pipeline, then it would be easier to reorganize the
     *       pipeline. [This would still make it possible for services to add
     *       themselves to the pipeline without being joined with the quorum but
     *       the quorum watcher would now automatically add the service to the
     *       quorum before a join and add it back if it is removed from the
     *       pipeline while it was joined _if_ the quorum is not yet met (once
     *       the quorum is met we can not rejoin the pipeline if writes have
     *       propagated along the pipeline, and the pipeline is not clean at
     *       each commit - only when we have an exclusive lock on the
     *       {@link WriteExecutorService}).]
     * 
     * @todo The leader could also invoke this at any commit point where a
     *       service joins the quorum since we need to obtain an exclusive lock
     *       on the {@link WriteExecutorService} at that point (use as we do
     *       when we do a synchronous overflow).
     *       <p>
     *       Periodic re-optimizations should take into account that services
     *       which are bouncing should stay at the end of the pipeline even
     *       though they have higher network costs because loosing the service
     *       at the end of the pipeline causes minimal retransmission.
     */
    Future<Void> moveToEndOfPipeline() throws IOException;
    
    /**
     * Accept metadata describing an NIO buffer transfer along the write
     * pipeline. This method is never invoked on the master. It is only invoked
     * on the failover nodes, including the last node in the failover chain.
     * 
     * @param msg
     *            The metadata.
     * 
     * @return The {@link Future} which will become available once the buffer
     *         transfer is complete.
     */
    Future<Void> receiveAndReplicate(HAWriteMessage msg) throws IOException;

}
