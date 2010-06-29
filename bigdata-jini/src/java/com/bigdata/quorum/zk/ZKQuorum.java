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
 * Created on Jun 29, 2010
 */

package com.bigdata.quorum.zk;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.io.SerializerUtil;
import com.bigdata.jini.start.BigdataZooDefs;
import com.bigdata.jini.start.ManageLogicalServiceTask;
import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumClient;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumWatcher;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.ZooKeeperAccessor;

/**
 * Implementation of the {@link Quorum} using zookeeper to maintain the
 * distributed quorum state. This implementation includes a zookeeper aware
 * {@link QuorumWatcher} which registers patterns of watchers in order to be
 * notified of changes in the distributed quorum state. It also includes a
 * {@link QuorumActor} which knows how to make the various changes in the
 * distributed quorum state within zookeeper.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo adapt the existing unit tests to run against a zookeeper instance.
 */
public class ZKQuorum<S extends Remote, C extends QuorumClient<S>> extends
        AbstractQuorum<S, C> implements BigdataZooDefs {

    /**
     * 
     */
    private final JiniFederation<?> fed;

    /**
     * Return a live {@link ZooKeeper} connection.
     * 
     * @throws InterruptedException
     */
    protected ZooKeeper getZookeeper() throws InterruptedException {
        
        return fed.getZookeeperAccessor().getZookeeper();
        
    }

    /**
     * The ACLs used by the quorum actor and watcher.
     */
    protected List<ACL> getZookeeperACL() {
        
        return fed.getZooConfig().acl;
        
    }
    
    /**
     * @param k
     * @param fed
     *            The federation, which provides access to the {@link ZooKeeper}
     *            connection, a means to obtain new {@link ZooKeeper}
     *            connections if the current connection is invalidated, ACLs for
     *            zookeeper, a means to resolve {@link UUID} to RMI proxy
     *            objects, etc.
     * 
     * @todo In order to be used with a Journal we need to separate out the
     *       {@link ZooKeeperAccessor} and the zookeeper ACLs from the
     *       {@link JiniFederation}.  A derived class could be federation aware
     */
    public ZKQuorum(final int k, final JiniFederation<?> fed) {
        
        super(k);
        
        if(fed == null)
            throw new IllegalArgumentException();
        
        if(!fed.isOpen())
            throw new IllegalStateException();

        this.fed = fed;
        
    }

    @Override
    protected QuorumActorBase newActor(final String logicalServiceId,
            final UUID serviceId) {

        return new ZkQuorumActor(logicalServiceId, serviceId);

    }

    @Override
    protected QuorumWatcherBase newWatcher(final String logicalServiceId) {

        return new ZkQuorumWatcher(logicalServiceId);

    }

    /**
     * 
     * @todo decide who has responsibility for creating the znodes for the
     *       quorum. Right now it is {@link ManageLogicalServiceTask}.
     */
    protected class ZkQuorumActor extends QuorumActorBase {

        /**
         * The value of the service's {@link UUID} as a {@link String} (we use
         * this a lot).
         */
        final private String serviceIdStr;
        
        protected ZkQuorumActor(final String logicalServiceZPath,
                final UUID serviceId) {

            super(logicalServiceZPath, serviceId);

            serviceIdStr = serviceId.toString();
            
        }

        @Override
        protected void doMemberAdd() {
            try {
                getZookeeper().create(
                        logicalServiceId + ZSLASH + QUORUM_MEMBER + ZSLASH
                                + QUORUM_MEMBER_PREFIX + serviceIdStr,
                        new byte[0]/* empty */, getZookeeperACL(),
                        CreateMode.EPHEMERAL);
            } catch (NodeExistsException e) {
                // ignore.
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }

        @Override
        protected void doMemberRemove() {
            try {
                getZookeeper()
                        .delete(
                                logicalServiceId + ZSLASH + QUORUM_MEMBER
                                        + ZSLASH + QUORUM_MEMBER_PREFIX
                                        + serviceIdStr, -1/* anyVersion */);
            } catch (NoNodeException e) {
                // ignore.
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }

        @Override
        protected void doPipelineAdd() {
            // resolve the local service implementation object.
            final HAPipelineGlue tmp = (HAPipelineGlue) getMember()
                    .getService();
            // ask it for the write pipeline's address and port.
            final QuorumPipelineState pipelineState = new QuorumPipelineState(
                    tmp.getWritePipelineAddr());
            try {
                getZookeeper().create(
                        logicalServiceId + ZSLASH + QUORUM_PIPELINE + ZSLASH
                                + QUORUM_PIPELINE_PREFIX + serviceIdStr,
                        SerializerUtil.serialize(pipelineState),
                        getZookeeperACL(), CreateMode.EPHEMERAL);
            } catch (NodeExistsException e) {
                // ignore.
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }

        @Override
        protected void doPipelineRemove() {
            try {
                getZookeeper()
                        .delete(
                                logicalServiceId + ZSLASH + QUORUM_PIPELINE
                                        + ZSLASH + QUORUM_PIPELINE_PREFIX
                                        + serviceIdStr, -1/* anyVersion */);
            } catch (NoNodeException e) {
                // ignore.
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }

        /**
         * Directs each service before this service in the write pipeline to
         * move itself to the end of the write pipeline.
         * 
         * @todo optimize the write pipeline for the network topology.
         */
        @Override
        protected void reorganizePipeline() {
            final UUID[] pipeline = getPipeline();
            for (int i = 0; i < pipeline.length; i++) {
                final UUID t = pipeline[i];
                if (serviceId.equals(t)) {
                    // Done.
                    return;
                }
                // some other service in the pipeline ahead of the leader.
                final S otherService = getQuorumMember().getService(serviceId);
                if (otherService == null) {
                    throw new QuorumException(
                            "Could not discover service: serviceId="
                                    + serviceId);
                }
                try {
                    // ask it to move itself to the end of the pipeline.
                    ((HAPipelineGlue) otherService).moveToEndOfPipeline();
                } catch (IOException ex) {
                    throw new QuorumException(
                            "Could not move service to end of the pipeline: serviceId="
                                    + serviceId);
                }
            }
        }

        @Override
        protected void doCastVote(final long lastCommitTime) {
            // zpath for the lastCommitTime.
            final String zpath = logicalServiceId + ZSLASH + QUORUM_VOTES
                    + ZSLASH + lastCommitTime;
            while (true) {
                // create znode for the lastCommitTime.
                try {
                    getZookeeper().create(zpath, new byte[0]/* empty */,
                            getZookeeperACL(), CreateMode.PERSISTENT);
                } catch (NodeExistsException e) {
                    // ignore.
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    // propagate the interrupt.
                    Thread.currentThread().interrupt();
                    return;
                }
                // create znode for this service's vote for that lastCommitTime.
                try {
                    getZookeeper().create(zpath + ZSLASH + serviceIdStr,
                            new byte[0]/* empty */, getZookeeperACL(),
                            CreateMode.EPHEMERAL);
                } catch (NoNodeException e) {
                    /*
                     * The parent znode was concurrently deleted, e.g., by
                     * doWithdrawVote(). This is handled by a simple retry loop
                     * when casting a vote. The logic to delete lastCommitTime
                     * znodes without children DOES NOT retry. Since a delete
                     * only occurs when there was only a single child for that
                     * lastCommitTime, there should be at most one retry (more
                     * than one retries would imply that a different service had
                     * managed to vote and then withdraw its vote before this
                     * service was able to finish casting its vote).
                     */
                    log.warn("Concurrent delete (retrying): zpath=" + zpath);
                } catch (NodeExistsException e) {
                    // ignore (this service has already cast this vote).
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    // propagate the interrupt.
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /**
         * Searches for a vote cast by this service. If found, it is removed. If
         * there are no remaining votes for the lastCommitTime from which it was
         * removed, then the lastCommitTime znode is deleted.
         * <p>
         * When we delete lastCommitTime znode with no children, we ignore any
         * errors since there could be a concurrent create in response to a vote
         * cast by another service. For its part, {@link #castVote(long)}
         * handles a concurrent delete by a simple retry loop.
         */
        @Override
        protected void doWithdrawVote() {
            // zpath for votes.
            final String zpath = logicalServiceId + ZSLASH + QUORUM_VOTES;
            // find all lastCommitTimes for which votes have been cast.
            final List<String> lastCommitTimes;
            try {
                lastCommitTimes = getZookeeper()
                        .getChildren(zpath, false/* watch */);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
            // search for a vote for this service.
            for (String lastCommitTime : lastCommitTimes) {
                // zpath for votes for that lastCommitTime.
                final String lzpath = zpath + ZSLASH + lastCommitTime;
                // get the votes for that lastCommitTime.
                final List<String> votes;
                try {
                    votes = getZookeeper()
                            .getChildren(lzpath, false/* watch */);
                } catch (NoNodeException ex) {
                    // ignore (concurrent delete, so there are no children).
                    continue;
                } catch (KeeperException ex) {
                    throw new RuntimeException(ex);
                } catch (InterruptedException e) {
                    // propagate the interrupt.
                    Thread.currentThread().interrupt();
                    return;
                }
                if(votes.isEmpty()) {
                    /*
                     * There are no votes for this lastCommitTime, so try ONCE
                     * to remove the znode for that lastCommitTime.
                     */
                    try {
                        // delete the service znode for this lastCommitTime.
                        getZookeeper().delete(lzpath, -1/* anyVersion */);
                    } catch (NoNodeException e) {
                        // ignore (concurrent delete).
                        continue;
                    } catch (NotEmptyException e) {
                        // ignore (concurrent create of child (cast vote)).
                        continue;
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        // propagate the interrupt.
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                // try each vote for this lastCommitTime.
                for (String vote : votes) {
                    if (serviceIdStr.equals(vote)) {
                        try {
                            // delete vote cast by service for lastCommitTime.
                            getZookeeper().delete(lzpath + ZSLASH + serviceId,
                                    -1/* anyVersion */);
                            /*
                             * If successful, then the service had a vote cast
                             * for that last commit time.
                             */
                            if (votes.size() == 1) {
                                /*
                                 * Since there were no other votes for that
                                 * lastCommitTime, try ONCE to remove the znode
                                 * for that lastCommitTime.
                                 */
                                try {
                                    // delete the service znode for this lastCommitTime.
                                    getZookeeper().delete(lzpath, -1/* anyVersion */);
                                } catch (NoNodeException e) {
                                    // ignore (concurrent delete).
                                    continue;
                                } catch (NotEmptyException e) {
                                    // ignore (concurrent create of child (cast vote)).
                                    continue;
                                } catch (KeeperException e) {
                                    throw new RuntimeException(e);
                                } catch (InterruptedException e) {
                                    // propagate the interrupt.
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                            // done.
                            if (log.isInfoEnabled())
                                log.info("withdrawn: serviceId=" + serviceIdStr
                                        + ", lastCommitTime=" + lastCommitTime);
                            return;
                        } catch (NoNodeException e) {
                            /*
                             * ignore (the service does not have a vote cast for
                             * that lastCommitTime).
                             */
                            continue;
                        } catch (KeeperException e) {
                            throw new RuntimeException(e);
                        } catch (InterruptedException e) {
                            // propagate the interrupt.
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }

       }

        @Override
        protected void doServiceJoin() {
            try {
                getZookeeper().create(
                        logicalServiceId + ZSLASH + QUORUM_JOINED + ZSLASH
                                + QUORUM_JOINED_PREFIX + serviceId,
                        new byte[0]/* empty */, getZookeeperACL(),
                        CreateMode.EPHEMERAL);
            } catch (NodeExistsException e) {
                // ignore.
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }

        @Override
        protected void doServiceLeave() {
            try {
                getZookeeper()
                        .delete(
                                logicalServiceId + ZSLASH + QUORUM_JOINED
                                        + ZSLASH + QUORUM_JOINED_PREFIX
                                        + serviceId, -1/* anyVersion */);
            } catch (NoNodeException e) {
                // ignore.
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }

        /**
         * @todo Make sure the quorum breaks when the leader dies as well as
         *       when it leaves nicely. To do this, any quorum member which
         *       observes that the #of joined services falls beneath (k+1/2)
         *       should attempt to clear the token.
         */
        @Override
        protected void doClearToken() {
            /*
             * Try in a loop until we can read the data and update it without
             * having a concurrent update. If the token has been cleared, then
             * we are done (it does not matter which service observing a quorum
             * break actually clears the token, only that it is quickly
             * cleared).
             */
            while (true) {
                /*
                 * Read the current quorum state.
                 */
                final Stat stat = new Stat();
                final QuorumTokenState oldState;
                try {
                    oldState = (QuorumTokenState) SerializerUtil
                            .deserialize(getZookeeper().getData(
                                    logicalServiceId, false/* watch */, stat));
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    // propagate the interrupt.
                    Thread.currentThread().interrupt();
                    return;
                }
                if (oldState.token() == Quorum.NO_QUORUM) {
                    // done (the token is not set).
                    return;
                }
                // new local state object w/ the current token cleared.
                final QuorumTokenState newState = new QuorumTokenState(oldState
                        .lastValidToken(), Quorum.NO_QUORUM);
                try {
                    getZookeeper().setData(logicalServiceId,
                            SerializerUtil.serialize(newState), stat.getVersion());
                    // done.
                    if (log.isInfoEnabled())
                        log.info("Cleared: serviceId=" + serviceId);
                    return;
                } catch (BadVersionException e) {
                    /*
                     * If we get a version conflict, then just retry. Either the
                     * token was cleared by someone else or we will try to clear
                     * it ourselves.
                     */
                    log.warn("Concurrent update (retry): serviceId="
                            + serviceIdStr);
                    continue;
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    // propagate the interrupt.
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        @Override
        protected void doSetLastValidToken(final long newToken) {
            /*
             * Note: Unlike clearToken(), which can be done by anyone, only the
             * quorum leader should be updating the lastValidToken. For that
             * reason, there should be only one thread in the entire distributed
             * system taking this action and we should not have to retry.
             */
            /*
             * Read the current quorum state.
             */
            final Stat stat = new Stat();
            final QuorumTokenState oldState;
            try {
                oldState = (QuorumTokenState) SerializerUtil
                        .deserialize(getZookeeper().getData(logicalServiceId,
                                false/* watch */, stat));
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
            /*
             * Check some preconditions.
             */
            if (oldState.lastValidToken() >= newToken) {
                // the lastValidToken must advance.
                throw new QuorumException(
                        "New value must be GT old value: oldValue="
                                + oldState.lastValidToken() + ", but newValue="
                                + newToken);
            }
            if (oldState.token() != Quorum.NO_QUORUM) {
                /*
                 * A new value for lastValidToken should not be assigned unless
                 * the quorum has broken. The quorum token should have been
                 * cleared when the quorum broken.
                 */
                throw new QuorumException(
                        "The quorum token has not been cleared");
            }
            try {
                // new local state object w/ the current token cleared.
                final QuorumTokenState newState = new QuorumTokenState(
                        newToken, oldState.token());
                // update data (verifying the version!)
                getZookeeper().setData(logicalServiceId,
                        SerializerUtil.serialize(newState), stat.getVersion());
                // done.
                if (log.isInfoEnabled())
                    log.info("Set: lastValidToken=" + newToken);
                return;
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }

        @Override
        protected void doSetToken() {
            /*
             * Note: Unlike clearToken(), which can be done by anyone, only the
             * quorum leader should be updating the currentToken. For that
             * reason, there should be only one thread in the entire distributed
             * system taking this action and we should not have to retry.
             */
            /*
             * Read the current quorum state.
             */
            final Stat stat = new Stat();
            final QuorumTokenState oldState;
            try {
                oldState = (QuorumTokenState) SerializerUtil
                        .deserialize(getZookeeper().getData(logicalServiceId,
                                false/* watch */, stat));
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
            /*
             * Check some preconditions.
             */
            if (oldState.token() != Quorum.NO_QUORUM) {
                /*
                 * A new value for lastValidToken should not be assigned unless
                 * the quorum has broken. The quorum token should have been
                 * cleared when the quorum broken.
                 */
                throw new QuorumException(
                        "The quorum token has not been cleared");
            }
            try {
                /*
                 * Take the new quorum token from the lastValidToken field and
                 * create a new local state object w/ the current token set.
                 */
                final long newToken = oldState.lastValidToken();
                final QuorumTokenState newState = new QuorumTokenState(oldState
                        .lastValidToken(), newToken);
                // update data (verifying the version!)
                getZookeeper().setData(logicalServiceId,
                        SerializerUtil.serialize(newState), stat.getVersion());
                // done.
                if (log.isInfoEnabled())
                    log.info("Set: token=" + newToken);
                return;
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // propagate the interrupt.
                Thread.currentThread().interrupt();
                return;
            }
        }

    }

    /**
     * @todo establish the appropriate watcher patterns.
     */
    protected class ZkQuorumWatcher extends QuorumWatcherBase {

        /**
         * @param logicalServiceUUID
         */
        protected ZkQuorumWatcher(String logicalServiceZPath) {
          
            super(logicalServiceZPath);
            
        }

        @Override
        protected void setupDiscovery() {
            // TODO Auto-generated method stub
            
        }
        
    }

}
