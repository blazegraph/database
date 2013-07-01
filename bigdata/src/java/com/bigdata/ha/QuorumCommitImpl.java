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
package com.bigdata.ha;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.HA2PhaseAbortMessage;
import com.bigdata.ha.msg.HA2PhaseCommitMessage;
import com.bigdata.ha.msg.HA2PhasePrepareMessage;
import com.bigdata.ha.msg.IHA2PhaseAbortMessage;
import com.bigdata.ha.msg.IHA2PhaseCommitMessage;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.quorum.QuorumStateChangeListener;
import com.bigdata.quorum.QuorumStateChangeListenerBase;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * {@link QuorumCommit} implementation.
 */
public class QuorumCommitImpl<S extends HACommitGlue> extends
        QuorumStateChangeListenerBase implements QuorumCommit<S>,
        QuorumStateChangeListener {

    static private transient final Logger log = Logger
            .getLogger(QuorumCommitImpl.class);

    protected final QuorumMember<S> member;
    
    /**
     * The downstream service in the write pipeline.
     */
    protected volatile UUID downStreamId = null;

    public QuorumCommitImpl(final QuorumMember<S> member) {
        
        this.member = member;
        
    }

    protected Quorum<?, ?> getQuorum() {
        
        return member.getQuorum();
        
    }
    
    protected HACommitGlue getService(final UUID serviceId) {

        return member.getService(serviceId);
        
    }

    /**
     * Cancel the requests on the remote services (RMI). This is a best effort
     * implementation. Any RMI related errors are trapped and ignored in order
     * to be robust to failures in RMI when we try to cancel the futures.
     */
    protected <F extends Future<T>, T> void cancelRemoteFutures(
            final List<F> remoteFutures) {

        if (log.isInfoEnabled())
            log.info("");

        for (F rf : remoteFutures) {

            try {

                rf.cancel(true/* mayInterruptIfRunning */);

            } catch (Throwable t) {

                // ignored (to be robust).

            }

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation runs the operation on the leader in the caller's
     * thread to avoid deadlock. The other services run the operation
     * asynchronously on their side while the leader awaits their future's using
     * get().
     * <p>
     * Note: The {@link IHA2PhasePrepareMessage} is sent to all services in
     * write pipeline. This ensures that services that are not yet joined with
     * the met quorum will still observe the root blocks. This is necessary in
     * order for them to catch up with the met quorum. The 2-phase commit
     * protocol is "aware" that not all messages are being sent to services
     * whose votes count. Only services that are actually in the met quorum get
     * a vote.
     * <p>
     * Note: This method is responsible for the atomic decision regarding
     * whether a service will be considered to be "joined" with the met quorum
     * for the 2-phase commit. A service that is synchronizing will either have
     * already voted the appropriate lastCommitTime and entered the met quorum
     * or it will not. That decision point is captured atomically when we obtain
     * a snapshot of the joined services from the quorum state. The decision is
     * propagated through the {@link IHA2PhasePrepareMessage} and that
     * information is retained by the service together with the new root block
     * from the prepare message. This metadata is used to decide how the service
     * will handle the prepare, commit, and abort messages.
     */
    public PrepareResponse prepare2Phase(final PrepareRequest req)
            throws InterruptedException, IOException {

        if (log.isInfoEnabled())
            log.info("req=" + req);

        final IRootBlockView rootBlock = req.getRootBlock();

        final UUID[] joinedServiceIds = req.getPrepareAndNonJoinedServices()
                .getJoinedServiceIds();
        
//        final Set<UUID> nonJoinedPipelineServiceIds = req
//                .getNonJoinedPipelineServiceIds();
        
        final long timeout = req.getTimeout();
        
        final TimeUnit unit = req.getUnit();
        
        /*
         * The token of the quorum for which the leader issued this prepare
         * message.
         */
        final long token = rootBlock.getQuorumToken();

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to prepare concurrently with the leader's IO.
         */

        final long begin = System.nanoTime();
        final long nanos = unit.toNanos(timeout);
        long remaining = nanos;
        
        /*
         * The leader is a local service. The followers and other service in the
         * pipeline (but not yet joined) are remote services.
         */

        // #of remote followers (joined services, excluding the leader).
        final int nfollowers = (joinedServiceIds.length - 1);

//        // #of non-joined services in the pipeline.
//        final int nNonJoinedPipelineServices = nonJoinedPipelineServiceIds
//                .size();

        // #of remote services (followers plus others in the pipeline).
        final int remoteServiceCount = nfollowers;// + nNonJoinedPipelineServices;

        // Random access list of futures.
        final ArrayList<Future<Boolean>> remoteFutures = new ArrayList<Future<Boolean>>(
                remoteServiceCount);

        for (int i = 0; i <= remoteServiceCount; i++) {

            // Pre-size to ensure sufficient room for set(i,foo).
            remoteFutures.add(null);
        
        }
        
        try {

            // Verify the quorum is valid.
            member.assertLeader(token);

            {

                // First, message the joined services (met with the quorum).
                int i = 1;
                {

                    for (; i < joinedServiceIds.length; i++) {

                        final UUID serviceId = joinedServiceIds[i];

                        /*
                         * Figure out if this service participated in the
                         * GATHER.
                         */
                        final boolean isGatherService;
                        {
                            boolean found = false;
                            for (UUID x : req
                                    .getGatherJoinedAndNonJoinedServices()
                                    .getJoinedServiceIds()) {
                                if (serviceId.equals(x)) {
                                    found = true;
                                    break;
                                }
                            }
                            isGatherService = found;
                        }

                        // The message used for the services that are joined.
                        final IHA2PhasePrepareMessage msgForJoinedService = new HA2PhasePrepareMessage(
                                req.getConsensusReleaseTime(),//
                                isGatherService,//
                                true, // isJoinedService
                                rootBlock, timeout, unit);

                        /*
                         * Runnable which will execute this message on the
                         * remote service.
                         * 
                         * FIXME Because async futures cause DGC native thread
                         * leaks this is no longer running the prepare
                         * asynchronously on the followers. Change the code
                         * here, and in commit2Phase and abort2Phase to use
                         * multiple threads to run the tasks on the followers.
                         */
                        
                        final HACommitGlue service = getService(serviceId);
                        
                        Future<Boolean> rf = null;
                        try {
                            // RMI.
                            rf = service.prepare2Phase(msgForJoinedService);
                        } catch (final Throwable t) {
                            // If anything goes wrong, wrap up exception as Future.
                            final FutureTask<Boolean> ft = new FutureTask<Boolean>(new Runnable() {
                                public void run() {
                                    throw new RuntimeException(t);
                                }
                            }, Boolean.FALSE);
                            rf = ft;
                            ft.run(); // evaluate future. 
                        }

                        // add to list of futures we will check.
                        remoteFutures.set(i, rf);

                    }

                }

//                // Next, message the pipeline services NOT met with the quorum.
//                {
//
//                    // message for non-joined services.
//                    final IHA2PhasePrepareMessage msg = new HA2PhasePrepareMessage(
//                            false/* isJoinedService */, rootBlock, timeout, unit);
//
//                    for (UUID serviceId : nonJoinedPipelineServiceIds) {
//
//                        /*
//                         * Runnable which will execute this message on the
//                         * remote service.
//                         */
//                        final Future<Boolean> rf = getService(serviceId)
//                                .prepare2Phase(msg);
//
//                        // add to list of futures we will check.
//                        remoteFutures.set(i, rf);
//
//                        i++;
//
//                    }
//                }

                /*
                 * Finally, run the operation on the leader using local method
                 * call (non-RMI) in the caller's thread to avoid deadlock.
                 * 
                 * Note: Because we are running this in the caller's thread on
                 * the leader the timeout will be ignored for the leader.
                 * 
                 * Note: The leader will do this operation synchronously in this
                 * thread. This is why we add its future into the collection of
                 * futures after we have issued the RMI requests to the other
                 * services.
                 */
                {

                    final S leader = member.getService();
                    
                    // The message used for the leader.
                    final IHA2PhasePrepareMessage msgForJoinedService = new HA2PhasePrepareMessage(
                            req.getConsensusReleaseTime(),//
                            true, // isGatherService (always true for leader)
                            true, // isJoinedService (always true for leader)
                            rootBlock, timeout, unit);

                    final Future<Boolean> f = leader
                            .prepare2Phase(msgForJoinedService);
                    
                    remoteFutures.set(0/* index */, f);
                    
                }
                
            }

            /*
             * Check futures for all services that were messaged.
             */
            int nyes = 0;
            assert remoteFutures.size() == remoteServiceCount + 1;
            final boolean[] votes = new boolean[remoteServiceCount + 1];
            for (int i = 0; i <= remoteServiceCount; i++) {
                final Future<Boolean> rf = remoteFutures.get(i);
                if (rf == null)
                    throw new AssertionError("null @ index=" + i);
                boolean done = false;
                try {
                    remaining = nanos - (System.nanoTime() - begin);
                    final boolean vote = rf
                            .get(remaining, TimeUnit.NANOSECONDS);
                    votes[i] = vote;
                    if (i < joinedServiceIds.length) {
                        // Only the leader and the followers get a vote.
                        nyes += vote ? 1 : 0;
                    } else {
                        // non-joined pipeline service. vote does not count.
                        if (!vote) {
                            log.warn("Non-joined pipeline service will not prepare");
                        }
                    }
                    done = true;
                } catch (CancellationException ex) {
                    // This Future was cancelled.
                    log.error(ex, ex);
                    done = true; // CancellationException indicates isDone().
                } catch (TimeoutException ex) {
                    // Timeout on this Future.
                    log.error(ex, ex);
                    done = false;
                } catch (ExecutionException ex) {
                    /*
                     * Note: prepare2Phase() is throwing exceptions if
                     * preconditions are violated. These thrown exceptions are
                     * interpreted as a "NO" vote. An exception can also appear
                     * here if there is an RMI failure or even a failure on this
                     * service when attempting to perform the RMI.
                     */
                    log.error(ex, ex);
                    done = true; // ExecutionException indicates isDone().
                } catch (RuntimeException ex) {
                    /*
                     * Note: ClientFuture.get() can throw a RuntimeException if
                     * there is a problem with the RMI call. In this case we do
                     * not know whether the Future is done.
                     */
                    log.error(ex, ex);
                } finally {
                    if (!done) {
                        // Cancel the request on the remote service (RMI).
                        try {
                            rf.cancel(true/* mayInterruptIfRunning */);
                        } catch (Throwable t) {
                            // ignored.
                        }
                    }
                }
            }

            // The quorum replication factor.
            final int k = getQuorum().replicationFactor();

            /*
             * Note: The leader MUST vote YES in order for the commit to
             * continue. In addition, we need a majority of the joined services.
             * In practice, for an HA3 configuration, this means that up to one
             * follower could fail and the commit would still go through.
             * However, if the leader fails then the commit will fail as well.
             */
            final boolean willCommit = votes[0] && getQuorum().isQuorum(nyes);

            if (!willCommit) {

                log.error("prepare rejected: leader=" + votes[0] + ", nyes="
                        + nyes + " out of " + k);

            }

            return new PrepareResponse(k, nyes, willCommit, votes);

        } finally {
            /*
             * Ensure that all futures are cancelled.
             */
            for (Future<Boolean> rf : remoteFutures) {
                if (rf == null) // ignore empty slots.
                    continue;
                if (!rf.isDone()) {
                    // Cancel the request on the remote service (RMI).
                    try {
                        rf.cancel(true/* mayInterruptIfRunning */);
                    } catch (Throwable t) {
                        // ignored.
                    }
                }
            }
        }

    }

    public void commit2Phase(final CommitRequest req) throws IOException,
            InterruptedException {

        if (log.isInfoEnabled())
            log.info("req=" + req);

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to commit concurrently with the leader's IO.
         * 
         * Note: Only services that voted "YES" will get a commit2Phase message.
         * 
         * Note: Do NOT message the services that voted NO. [At one point the
         * code was modified to message each joined and non-joined service. That
         * change was introduced to support services that join during the
         * 2-phase commit. However, we have since resolved the service join by
         * relying on the service blocking the pipeline writes in
         * handleReplicatedWrite(). Since we can reliably know that there will
         * not be a concurrent commit, we can atomically join an existing quorum
         * and we do not need to make the 2-phase commit protocol visible to the
         * non-joined services. Thus we do not need to push the 2-phase commit
         * protocol to a service that is not joined with the met quorum at the
         * atomic decision point concerning such things in commitNow().]
         */
        
        final PrepareRequest preq = req.getPrepareRequest();

        final UUID[] joinedServiceIds = preq.getPrepareAndNonJoinedServices()
                .getJoinedServiceIds();

//        final Set<UUID> nonJoinedPipelineServiceIds = preq
//                .getNonJoinedPipelineServiceIds();

        final long token = preq.getRootBlock().getQuorumToken();
        
        final long commitTime = preq.getRootBlock().getLastCommitTime();

        final PrepareResponse presp = req.getPrepareResponse();

        // true iff we have a full complement of services that vote YES for this
        // commit.
        final boolean didAllServicesPrepare = presp.getYesCount() == presp
                .replicationFactor();
        
        member.assertLeader(token);

        final List<Future<Void>> remoteFutures = new LinkedList<Future<Void>>();
        
        try {

            final IHA2PhaseCommitMessage msgJoinedService = new HA2PhaseCommitMessage(
                    true/* isJoinedService */, commitTime, didAllServicesPrepare);

            for (int i = 1; i < joinedServiceIds.length; i++) {

                final UUID serviceId = joinedServiceIds[i];

                if (!presp.getVote(i)) {

                    // Skip services that did not vote YES in PREPARE.
                    continue;
                    
                }
                
                /*
                 * Runnable which will execute this message on the remote
                 * service.
                 */
                final Future<Void> rf = getService(serviceId).commit2Phase(
                        msgJoinedService);

                // add to list of futures we will check.
                remoteFutures.add(rf);

            }

//            if (!nonJoinedPipelineServiceIds.isEmpty()) {
//
//                final IHA2PhaseCommitMessage msgNonJoinedService = new HA2PhaseCommitMessage(
//                        false/* isJoinedService */, commitTime);
//
//                for (UUID serviceId : nonJoinedPipelineServiceIds) {
//
//                    /*
//                     * Runnable which will execute this message on the remote
//                     * service.
//                     */
//                    final Future<Void> rf = getService(serviceId).commit2Phase(
//                            msgNonJoinedService);
//
//                    // add to list of futures we will check.
//                    remoteFutures.add(rf);
//
//                }
//
//            }

            {
                /*
                 * Run the operation on the leader using local method call in
                 * the caller's thread to avoid deadlock.
                 */

                final S leader = member.getService();

                final Future<Void> f = leader.commit2Phase(msgJoinedService);

                remoteFutures.add(f);

            }

            /*
             * Check the futures for the other services in the quorum.
             */
            final List<Throwable> causes = new LinkedList<Throwable>();
            for (Future<Void> rf : remoteFutures) {
                boolean done = false;
                try {
                    rf.get(); // TODO Timeout to await followers in commit2Phase().
                    done = true;
//                } catch (TimeoutException ex) {
//                    // Timeout on this Future.
//                    log.error(ex, ex);
//                    causes.add(ex);
//                    done = false;
                } catch (CancellationException ex) {
                    // Future was cancelled.
                    log.error(ex, ex);
                    causes.add(ex);
                    done = true; // Future is done since cancelled.
                } catch (ExecutionException ex) {
                    log.error(ex, ex);
                    causes.add(ex);
                    done = true; // Note: ExecutionException indicates isDone().
                } catch (RuntimeException ex) {
                    /*
                     * Note: ClientFuture.get() can throw a RuntimeException
                     * if there is a problem with the RMI call. In this case
                     * we do not know whether the Future is done.
                     */
                    log.error(ex, ex);
                    causes.add(ex);
                } finally {
                    if (!done) {
                        // Cancel the request on the remote service (RMI).
                        try {
                            rf.cancel(true/* mayInterruptIfRunning */);
                        } catch (Throwable t) {
                            // ignored.
                        }
                    }
                }
            }

            /*
             * If there were any errors, then throw an exception listing them.
             */
            if (!causes.isEmpty()) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Throw exception back to the leader.
                if (causes.size() == 1)
                    throw new RuntimeException(causes.get(0));
                throw new RuntimeException("remote errors: nfailures="
                        + causes.size(), new ExecutionExceptions(causes));
            }

        } finally {
            /*
             * Ensure that all futures are cancelled.
             */
            for (Future<Void> rf : remoteFutures) {
                if (!rf.isDone()) {
                    // Cancel the request on the remote service (RMI).
                    try {
                        rf.cancel(true/* mayInterruptIfRunning */);
                    } catch (Throwable t) {
                        // ignored.
                    }
                }
            }
        }

    }

    /**
     * FIXME Only issue abort to services that voted YES in prepare? [We have
     * that information in commitNow(), but we do not have the atomic set of
     * joined services in AbstractJournal.abort())].
     */
    public void abort2Phase(final long token) throws IOException,
            InterruptedException {

        if (log.isInfoEnabled())
            log.info("token=" + token);

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to commit concurrently with the leader's IO.
         */

        final List<Future<Void>> remoteFutures = new LinkedList<Future<Void>>();

        /*
         * For services (other than the leader) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        final UUID[] joinedServiceIds = getQuorum().getJoined();

        member.assertLeader(token);
        
        final IHA2PhaseAbortMessage msg = new HA2PhaseAbortMessage(token);

        try {

            for (int i = 1; i < joinedServiceIds.length; i++) {

                final UUID serviceId = joinedServiceIds[i];

                /*
                 * Runnable which will execute this message on the remote
                 * service.
                 */
                final Future<Void> rf = getService(serviceId).abort2Phase(msg);

                // add to list of futures we will check.
                remoteFutures.add(rf);

            }

            {
                /*
                 * Run the operation on the leader using a local method call
                 * (non-RMI) in the caller's thread to avoid deadlock.
                 */
                member.assertLeader(token);
                final S leader = member.getService();
                final Future<Void> f = leader.abort2Phase(msg);
                remoteFutures.add(f);
            }

            /*
             * Check the futures for the other services in the quorum.
             */
            final List<Throwable> causes = new LinkedList<Throwable>();
            for (Future<Void> rf : remoteFutures) {
                boolean done = false;
                try {
                    rf.get();
                    done = true;
                } catch (InterruptedException ex) {
                    log.error(ex, ex);
                    causes.add(ex);
                } catch (ExecutionException ex) {
                    log.error(ex, ex);
                    causes.add(ex);
                    done = true; // Note: ExecutionException indicates isDone().
                } catch (RuntimeException ex) {
                    /*
                     * Note: ClientFuture.get() can throw a RuntimeException
                     * if there is a problem with the RMI call. In this case
                     * we do not know whether the Future is done.
                     */
                    log.error(ex, ex);
                    causes.add(ex);
                } finally {
                    if (!done) {
                        // Cancel the request on the remote service (RMI).
                        try {
                            rf.cancel(true/* mayInterruptIfRunning */);
                        } catch (Throwable t) {
                            // ignored.
                        }
                    }
                }
            }

            /*
             * If there were any errors, then throw an exception listing them.
             * 
             * TODO But only throw an exception for the joined services.
             * Non-joined services, we just long an error.
             */
            if (!causes.isEmpty()) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Throw exception back to the leader.
                if (causes.size() == 1)
                    throw new RuntimeException(causes.get(0));
                throw new RuntimeException("remote errors: nfailures="
                        + causes.size(), new ExecutionExceptions(causes));
            }

        } finally {
            /*
             * Ensure that all futures are cancelled.
             */
            for (Future<Void> rf : remoteFutures) {
                if (!rf.isDone()) {
                    // Cancel the request on the remote service (RMI).
                    try {
                        rf.cancel(true/* mayInterruptIfRunning */);
                    } catch (Throwable t) {
                        // ignored.
                    }
                }
            }

        }

    }

}
