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
package com.bigdata.ha;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
import com.bigdata.quorum.ServiceLookup;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * {@link QuorumCommit} implementation.
 */
public class QuorumCommitImpl<S extends HACommitGlue> extends
        QuorumStateChangeListenerBase implements QuorumCommit<S>,
        QuorumStateChangeListener, ServiceLookup<HACommitGlue> {

    static transient final Logger log = Logger
            .getLogger(QuorumCommitImpl.class);

    private final QuorumMember<S> member;
    private final ExecutorService executorService;

    public QuorumCommitImpl(final QuorumMember<S> member) {

        if (member == null)
            throw new IllegalArgumentException();
        
        this.member = member;
        
        this.executorService = member.getExecutor();
        
    }

    private Quorum<?, ?> getQuorum() {
        
        return member.getQuorum();
        
    }

    public HACommitGlue getService(final UUID serviceId) {

        return member.getService(serviceId);
        
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
    @Override
    public PrepareResponse prepare2Phase(final PrepareRequest req)
            throws InterruptedException, IOException {

        if (log.isInfoEnabled())
            log.info("req=" + req);

        final IRootBlockView rootBlock = req.getRootBlock();

        final UUID[] joinedServiceIds = req.getPrepareAndNonJoinedServices()
                .getJoinedServiceIds();
        
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
         * Random access list of futures.
         * 
         * Note: These are *local* Futures. Except for the leader, the Future is
         * for a task that submits an RMI request. This allows us to run the
         * PREPARE in parallel for less total latency. On the leader, the task
         * is run in the caller's thread (on the leader, the caller holds a lock
         * that we need so we have to execute the behavior in the caller's
         * thread).
         * 
         * Note: If a follower was joined as of the atomic decision point, but
         * did not *participate* in the GATHER protocol, then we still send it
         * the PREPARE message. 
         */
        final ArrayList<Future<Boolean>> localFutures = new ArrayList<Future<Boolean>>(
                joinedServiceIds.length);

        try {

            // #of remote followers (joined services, excluding the leader).
            final int nfollowers = joinedServiceIds.length - 1;

            for (int i = 0; i <= nfollowers; i++) {

                // Pre-size to ensure sufficient room for set(i,foo).
                localFutures.add(null);

            }
            
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
                         * Submit task which will execute this message on the
                         * remote service.  We will await this task below.
                         */
                        final Future<Boolean> rf = executorService
                                .submit(new PrepareMessageTask(this, serviceId,
                                        msgForJoinedService));

                        // add to list of futures we will check.
                        localFutures.set(i, rf);

                    }

                }

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
                    
                    localFutures.set(0/* index */, f);
                    
                }
                
            }

            /*
             * Check futures for all services that were messaged.
             */
            int nyes = 0;
            final boolean[] votes = new boolean[1 + nfollowers];
            for (int i = 0; i <= nfollowers; i++) {
                final Future<Boolean> ft = localFutures.get(i);
                if (ft == null)
                    throw new AssertionError("null @ index=" + i);
                try {
                    remaining = nanos - (System.nanoTime() - begin);
                    final boolean vote = ft
                            .get(remaining, TimeUnit.NANOSECONDS);
                    votes[i] = vote;
                    // Only the leader and the followers get a vote.
                    nyes += vote ? 1 : 0;
                } catch (CancellationException ex) {
                    // This Future was cancelled.
                    log.error(ex, ex);
                } catch (TimeoutException ex) {
                    // Timeout on this Future.
                    log.error(ex, ex);
                } catch (ExecutionException ex) {
                    /*
                     * Note: prepare2Phase() is throwing exceptions if
                     * preconditions are violated. These thrown exceptions are
                     * interpreted as a "NO" vote. An exception can also appear
                     * here if there is an RMI failure or even a failure on this
                     * service when attempting to perform the RMI.
                     */
                    log.error(ex, ex);
                } catch (RuntimeException ex) {
                    /*
                     * Note: ClientFuture.get() can throw a RuntimeException if
                     * there is a problem with the RMI call. In this case we do
                     * not know whether the Future is done.
                     */
                    log.error(ex, ex);
                } finally {
                    // Note: cancelling a *local* Future wrapping an RMI.
                    ft.cancel(true/*mayInterruptIfRunning*/);
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
            
            QuorumServiceBase.cancelFutures(localFutures);
            
        }

    }

    @Override
    public CommitResponse commit2Phase(final CommitRequest commitRequest)
            throws IOException, InterruptedException {

        if (log.isInfoEnabled())
            log.info("req=" + commitRequest);

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
        
        final PrepareRequest prepareRequest = commitRequest.getPrepareRequest();

        final UUID[] joinedServiceIds = prepareRequest.getPrepareAndNonJoinedServices()
                .getJoinedServiceIds();

        final long token = prepareRequest.getRootBlock().getQuorumToken();
        
        final long commitTime = prepareRequest.getRootBlock().getLastCommitTime();

        final PrepareResponse prepareResponse = commitRequest.getPrepareResponse();

        // true iff we have a full complement of services that vote YES for this
        // commit.
        final boolean didAllServicesPrepare = prepareResponse.getYesCount() == prepareResponse
                .replicationFactor();
        
        /*
         * Note: These entries are in service join order. The first entry is
         * always the leader. If a services did not vote YES for the PREPARE
         * then it will have a null entry in this list.
         */
        final ArrayList<Future<Void>> localFutures = new ArrayList<Future<Void>>(
                joinedServiceIds.length);

        final ArrayList<Throwable> causes = new ArrayList<Throwable>();

        try {

            for (int i = 0; i < joinedServiceIds.length; i++) {

                // Pre-size to ensure sufficient room for set(i,foo).
                localFutures.add(null);
                causes.add(null);

            }
            
            member.assertLeader(token);

            final IHA2PhaseCommitMessage msgJoinedService = new HA2PhaseCommitMessage(
                    true/* isJoinedService */, commitTime, didAllServicesPrepare);

            for (int i = 1; i < joinedServiceIds.length; i++) {

                if (!prepareResponse.getVote(i)) {

                    // Skip services that did not vote YES in PREPARE.
                    continue;
                    
                }
                
                final UUID serviceId = joinedServiceIds[i];

                /*
                 * Submit task on local executor. The task will do an RMI to the
                 * remote service.
                 */
                final Future<Void> rf = executorService
                        .submit(new CommitMessageTask(this, serviceId,
                                msgJoinedService));

                // add to list of futures we will check.
                localFutures.set(i, rf);

            }

            {
                /*
                 * Run the operation on the leader using local method call in
                 * the caller's thread to avoid deadlock.
                 */

                final S leader = member.getService();

                final Future<Void> f = leader.commit2Phase(msgJoinedService);

                localFutures.set(0/* leader */, f);

            }

            /*
             * Check the futures for the other services in the quorum.
             */
            for (int i = 0; i < joinedServiceIds.length; i++) {
                final Future<Void> ft = localFutures.get(i);
                if (ft == null)
                    continue;
                try {
                    ft.get(); // FIXME Timeout to await followers in commit2Phase().
//                } catch (TimeoutException ex) {
//                    // Timeout on this Future.
//                    log.error(ex, ex);
//                    causes.set(i, ex);
                } catch (CancellationException ex) {
                    // Future was cancelled.
                    log.error(ex, ex);
                    causes.set(i, ex);
                } catch (ExecutionException ex) {
                    log.error(ex, ex);
                    causes.set(i, ex);
                } catch (RuntimeException ex) {
                    /*
                     * Note: ClientFuture.get() can throw a RuntimeException if
                     * there is a problem with the RMI call. In this case we do
                     * not know whether the Future is done.
                     */
                    log.error(ex, ex);
                    causes.set(i, ex);
                } finally {
                    // Note: cancelling a *local* Future wrapping an RMI.
                    ft.cancel(true/* mayInterruptIfRunning */);
                }
            }

            /*
             * If there were any errors, then throw an exception listing them.
             */
            return new CommitResponse(commitRequest, causes);

        } finally {

            // Ensure that all futures are cancelled.
            QuorumServiceBase.cancelFutures(localFutures);
            
        }

    }

    /**
     * {@inheritDoc}
     * 
     * FIXME Only issue abort to services that voted YES in prepare? [We have
     * that information in commitNow(), but we do not have the atomic set of
     * joined services in AbstractJournal.abort())].
     */
    @Override
    public void abort2Phase(final long token) throws IOException,
            InterruptedException {

        if (log.isInfoEnabled())
            log.info("token=" + token);

        /*
         * For services (other than the leader) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        final UUID[] joinedServiceIds = getQuorum().getJoined();

        member.assertLeader(token);
        
        final IHA2PhaseAbortMessage msg = new HA2PhaseAbortMessage(token);

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. 
         */
        final List<Future<Void>> localFutures = new LinkedList<Future<Void>>();

        try {

            for (int i = 1; i < joinedServiceIds.length; i++) {

                final UUID serviceId = joinedServiceIds[i];

                /*
                 * Submit task on local executor. The task will do an RMI to the
                 * remote service.
                 */
                final Future<Void> rf = executorService
                        .submit(new AbortMessageTask(this, serviceId, msg));

                // add to list of futures we will check.
                localFutures.add(rf);

            }

            {
                /*
                 * Run the operation on the leader using a local method call
                 * (non-RMI) in the caller's thread to avoid deadlock.
                 */
                member.assertLeader(token);
                final S leader = member.getService();
                final Future<Void> f = leader.abort2Phase(msg);
                localFutures.add(f);
            }

            /*
             * Check the futures for the other services in the quorum.
             */
            final List<Throwable> causes = new LinkedList<Throwable>();
            for (Future<Void> ft : localFutures) {
                try {
                    ft.get(); // TODO Timeout for abort?
                } catch (InterruptedException ex) {
                    log.error(ex, ex);
                    causes.add(ex);
                } catch (ExecutionException ex) {
                    log.error(ex, ex);
                    causes.add(ex);
                } catch (RuntimeException ex) {
                    /*
                     * Note: ClientFuture.get() can throw a RuntimeException
                     * if there is a problem with the RMI call. In this case
                     * we do not know whether the Future is done.
                     */
                    log.error(ex, ex);
                    causes.add(ex);
                } finally {
                    // Note: cancelling a *local* Future wrapping an RMI.
                    ft.cancel(true/* mayInterruptIfRunning */);
                }
            }

            /*
             * If there were any errors, then throw an exception listing them.
             * 
             * TODO But only throw an exception for the joined services.
             * Non-joined services, we just long an error (or simply do not tell
             * them to do an abort()).
             */
            if (!causes.isEmpty()) {
                // Throw exception back to the leader.
                if (causes.size() == 1)
                    throw new RuntimeException(causes.get(0));
                throw new RuntimeException("remote errors: nfailures="
                        + causes.size(), new ExecutionExceptions(causes));
            }

        } finally {

            // Ensure that all futures are cancelled.
            QuorumServiceBase.cancelFutures(localFutures);

        }

    }

    static private class PrepareMessageTask extends
            AbstractMessageTask<HACommitGlue, Boolean, IHA2PhasePrepareMessage> {

        public PrepareMessageTask(
                final ServiceLookup<HACommitGlue> serviceLookup,
                final UUID serviceId, final IHA2PhasePrepareMessage msg) {

            super(serviceLookup, serviceId, msg);

        }

        @Override
        protected Future<Boolean> doRMI(final HACommitGlue service)
                throws IOException {

            return service.prepare2Phase(msg);

        }

    }

    static private class CommitMessageTask extends
            AbstractMessageTask<HACommitGlue, Void, IHA2PhaseCommitMessage> {

        public CommitMessageTask(
                final ServiceLookup<HACommitGlue> serviceLookup,
                final UUID serviceId, final IHA2PhaseCommitMessage msg) {

            super(serviceLookup, serviceId, msg);

        }

        @Override
        protected Future<Void> doRMI(final HACommitGlue service)
                throws IOException {

            return service.commit2Phase(msg);
        }

    }

    static private class AbortMessageTask extends
            AbstractMessageTask<HACommitGlue, Void, IHA2PhaseAbortMessage> {

        public AbortMessageTask(
                final ServiceLookup<HACommitGlue> serviceLookup,
                final UUID serviceId, final IHA2PhaseAbortMessage msg) {

            super(serviceLookup, serviceId, msg);

        }

        @Override
        protected Future<Void> doRMI(final HACommitGlue service)
                throws IOException {

            return service.abort2Phase(msg);
        }

    }

}
