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
 * Created on Jun 30, 2010
 */

package com.bigdata.quorum.zk;

import java.rmi.Remote;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.ha.QuorumPipeline;
import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumClient;
import com.bigdata.quorum.QuorumMember;

/**
 * Extended interface includes zookeeper specific definitions. The root of the
 * quorum state within zookeeper for a given highly available service is the
 * {@value #QUORUM} znode, which is a direct child of the
 * <em>logicalServiceZPath</em> corresponding to the logical service. For
 * example, see {@link BigdataZooDefs#LOGICAL_SERVICE_PREFIX}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ZKQuorum<S extends Remote, C extends QuorumClient<S>> extends
        Quorum<S, C> {

    /**
     * The name of the znode dominating the quorum state for a given logical
     * service. The data of this znode are a {@link QuorumTokenState} object
     * whose attributes are:
     * <dl>
     * <dt>lastValidToken</dt>
     * <dd>The last token assigned by an elected leader and initially -1 (MINUS
     * ONE).</dd>
     * <dt>currentToken</dt>
     * <dd>The current token. This is initially -1 (MINUS ONE) and is cleared
     * that value each time the quorum breaks.</dd>
     * </dl>
     * The atomic signal that the quorum has met or has broken is the set/clear
     * of the <i>currentToken</i>. All {@link QuorumClient}s must watch that
     * data field.
     * <p>
     * The direct children of this znode are:
     * <dl>
     * 
     * <dt>{@value #QUORUM_MEMBER}</dt>
     * <dd>The service instances currently registered with the quorum using
     * {@link QuorumActor#memberAdd()}</dd>
     * 
     * <dt>{@value #QUORUM_VOTE_PREFIX}</dt>
     * <dd>The znode dominating the <i>lastCommitTime</i>s for which quorum
     * members have cast a vote. The direct children are the lastCommitTime
     * values. The 2nd level children are the votes for a given lastCommitTime.
     * See {@link QuorumActor#castVote(long)}.</dd>
     * 
     * <dt>{@value #QUORUM_PIPELINE}</dt>
     * <dd>The service instances currently registered to participate in the
     * write pipeline using {@link QuorumActor#pipelineAdd()}</dd>
     * 
     * <dt>{@value #QUORUM_JOINED}</dt>
     * <dd>The service instances currently joined with the quorum using
     * {@link QuorumActor#serviceJoin()}</dd>
     * 
     * </dl>
     */
    String QUORUM = "quorum";

    /**
     * The name of the znode whose direct children are represent the instances
     * of the logical service which have registered themselves with the quorum
     * state using {@link QuorumActor#memberAdd()}.
     */
    String QUORUM_MEMBER = "member";

    /**
     * The prefix used by the ephemeral children of the {@link #QUORUM_MEMBER}
     * znode.
     */
    String QUORUM_MEMBER_PREFIX = "member";

    /**
     * The name of the znode dominating the votes cast by {@link QuorumMember}s
     * for specific <i>lastCommitTime</i>s. The direct children of this znode
     * are <i>lastCommitTime</i> values for the current root block associated
     * with a {@link QuorumMember} as of the last time which it cast a vote.
     * Each {@link QuorumMember} gets only a single vote. The children of the
     * <i>lastCommitTime</i> znodes are {@link CreateMode#EPHEMERAL_SEQUENTIAL}
     * znodes corresponding to the {@link ZooKeeper} connection for each
     * {@link QuorumMember} who has cast a vote. The vote order determines the
     * service join order. The quorum leader is the first service in the service
     * join order.
     * <p>
     * The {@link QuorumMember} must withdraw its current vote (if any) before
     * casting a new vote. If there are no more votes for a given
     * <i>lastCommitTime</i> znode, then that znode should be destroyed
     * (ignoring any errors if there is a concurrent create of a child due to a
     * service casting its vote for that <i>lastCommitTime</i>). Services will
     * <i>join</i> the quorum in their <i>vote order</i> once
     * <code>(k+1)/2</code> {@link QuorumMember}s have cast their vote for the
     * same <i>lastCommitTime</i>.
     */
    String QUORUM_VOTES = "votes";

    /**
     * The prefix used by the ephemeral children appearing under each
     * <i>lastCommitTime</i> for the {@link #QUORUM_VOTES} znode.
     */
    String QUORUM_VOTE_PREFIX = "vote";

    /**
     * The name of the znode whose direct children are
     * {@link CreateMode#EPHEMERAL_SEQUENTIAL} znodes corresponding to the
     * {@link ZooKeeper} connection for the {@link QuorumMember}s joined with
     * the quorum. The SEQUENTIAL flag is used to maintain information about the
     * order in which the services join the quorum. Note that services must join
     * in vote order. That constraint is enforced by {@link AbstractQuorum}.
     * <p>
     * Once <code>k+1/2</code> services are joined, the first service in the
     * quorum order will be elected the quorum leader. It will update the
     * <i>lastValidToken</i> on the {@link #QUORUM} and then set the
     * <i>currentToken</i> on the {@link #QUORUM_JOINED}.
     */
    String QUORUM_JOINED = "joined";

    /**
     * The prefix used by the ephemeral children of the {@link #QUORUM_JOINED}
     * znode.
     */
    String QUORUM_JOINED_PREFIX = "joined";

    /**
     * The name of the znode whose direct children are registered as
     * participating in the write pipeline ({@link QuorumPipeline}). The direct
     * children are {@link CreateMode#EPHEMERAL_SEQUENTIAL} znodes corresponding
     * to the {@link ZooKeeper} connection for each {@link QuorumMember} which
     * has added itself to the write pipeline.
     * <p>
     * The write pipeline provides an efficient replication of low-level cache
     * blocks from the quorum leader to each service joined with the quorum. In
     * addition to the joined services, services synchronizing with the quorum
     * may also be present in the write pipeline.
     * <p>
     * When the quorum leader is elected, it MAY reorganize the write pipeline
     * to (a) ensure that it is the first service in the write pipeline order;
     * and (b) optionally optimize the write pipeline based on the network
     * topology.
     * <p>
     * The data associated with the leaf ephemeral znodes are
     * {@link QuorumPipelineState} objects, whose state includes the Internet
     * address and port at which the service will listen for replicated writes
     * send along the write pipeline.
     */
    String QUORUM_PIPELINE = "pipeline";

    /**
     * The prefix used by the ephemeral children of the {@link #QUORUM_PIPELINE}
     * znode.
     */
    String QUORUM_PIPELINE_PREFIX = "pipeline";

}
