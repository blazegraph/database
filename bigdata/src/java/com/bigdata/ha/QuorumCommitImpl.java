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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
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
    public int prepare2Phase(//
            final UUID[] joinedServiceIds, //
            final Set<UUID> nonJoinedPipelineServiceIds,//
            final IRootBlockView rootBlock,//
            final long timeout, final TimeUnit unit//
            )
            throws InterruptedException, TimeoutException, IOException {

        if (rootBlock == null)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();
        
        final boolean isRootBlock0 = rootBlock.isRootBlock0();

        if (log.isInfoEnabled())
            log.info("isRootBlock0=" + isRootBlock0 + "rootBlock=" + rootBlock
                    + ", timeout=" + timeout + ", unit=" + unit);

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

        // #of non-joined services in the pipeline.
        final int nNonJoinedPipelineServices = nonJoinedPipelineServiceIds
                .size();

        // #of remote services (followers plus others in the pipeline).
        final int remoteServiceCount = nfollowers + nNonJoinedPipelineServices;

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

                // The message used for the services that are joined.
                final IHA2PhasePrepareMessage msgForJoinedService = new HA2PhasePrepareMessage(
                        true/* isJoinedService */, rootBlock, timeout, unit);

                // First, message the joined services (met with the quorum).
                int i = 1;
                {

                    for (; i < joinedServiceIds.length; i++) {

                        final UUID serviceId = joinedServiceIds[i];

                        /*
                         * Runnable which will execute this message on the
                         * remote service.
                         */
                        final Future<Boolean> rf = getService(serviceId)
                                .prepare2Phase(msgForJoinedService);

                        // add to list of futures we will check.
                        remoteFutures.set(i, rf);

                    }

                }

                // Next, message the pipeline services NOT met with the quorum.
                {

                    // message for non-joined services.
                    final IHA2PhasePrepareMessage msg = new HA2PhasePrepareMessage(
                            false/* isJoinedService */, rootBlock, timeout, unit);

                    for (UUID serviceId : nonJoinedPipelineServiceIds) {

                        /*
                         * Runnable which will execute this message on the
                         * remote service.
                         */
                        final Future<Boolean> rf = getService(serviceId)
                                .prepare2Phase(msg);

                        // add to list of futures we will check.
                        remoteFutures.set(i, rf);

                        i++;

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
            for (int i = 0; i <= remoteServiceCount; i++) {
                final Future<Boolean> rf = remoteFutures.get(i);
                if (rf == null)
                    throw new AssertionError("null @ index=" + i);
                boolean done = false;
                try {
                    remaining = nanos - (begin - System.nanoTime());
                    final boolean vote = rf
                            .get(remaining, TimeUnit.NANOSECONDS);
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
                } catch (ExecutionException ex) {
                    /*
                     * TODO prepare2Phase() is throwing exceptions if
                     * preconditions are violated. Unless if is a joined
                     * service, it probably should just vote "no" instead. We do
                     * not need to log @ ERROR when a precondition for a
                     * non-joined service has been violated.
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

            final int k = getQuorum().replicationFactor();

            if (!getQuorum().isQuorum(nyes)) {

                log.error("prepare rejected: nyes=" + nyes + " out of " + k);

            }

            return nyes;

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

    public void commit2Phase(final UUID[] joinedServiceIds, //
            final Set<UUID> nonJoinedPipelineServiceIds,//
            final long token, final long commitTime) throws IOException,
            InterruptedException {

        if (log.isInfoEnabled())
            log.info("token=" + token + ", commitTime=" + commitTime);

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to commit concurrently with the leader's IO.
         */

        member.assertLeader(token);

        final List<Future<Void>> remoteFutures = new LinkedList<Future<Void>>();
        
        try {

            final IHA2PhaseCommitMessage msgJoinedService = new HA2PhaseCommitMessage(
                    true/* isJoinedService */, commitTime);

            for (int i = 1; i < joinedServiceIds.length; i++) {

                final UUID serviceId = joinedServiceIds[i];

                /*
                 * Runnable which will execute this message on the remote
                 * service.
                 */
                final Future<Void> rf = getService(serviceId).commit2Phase(
                        msgJoinedService);

                // add to list of futures we will check.
                remoteFutures.add(rf);

            }

            if (!nonJoinedPipelineServiceIds.isEmpty()) {

                final IHA2PhaseCommitMessage msgNonJoinedService = new HA2PhaseCommitMessage(
                        false/* isJoinedService */, commitTime);

                for (UUID serviceId : nonJoinedPipelineServiceIds) {

                    /*
                     * Runnable which will execute this message on the remote
                     * service.
                     */
                    final Future<Void> rf = getService(serviceId).commit2Phase(
                            msgNonJoinedService);

                    // add to list of futures we will check.
                    remoteFutures.add(rf);

                }

            }

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
                    rf.get();
                    done = true;
                } catch (InterruptedException ex) {
                    log.error(ex, ex);
                    causes.add(ex);
                } catch (ExecutionException ex) {
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
             * TODO But only throw the exception if the errors were for a joined
             * service. Otherwise just log.
             */
            if (!causes.isEmpty()) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Throw exception back to the leader.
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
