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
package com.bigdata.ha;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.HAWriteMessageBase;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHAMessage;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.pipeline.HAReceiveService;
import com.bigdata.ha.pipeline.HAReceiveService.IHAReceiveCallback;
import com.bigdata.ha.pipeline.HASendService;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.quorum.QuorumStateChangeListener;
import com.bigdata.quorum.QuorumStateChangeListenerBase;
import com.bigdata.util.InnerCause;

/**
 * {@link QuorumPipeline} implementation.
 * <p>
 * The {@link QuorumMember} must pass along the "pipeline" messages, including:
 * <ul>
 * <li>{@link QuorumMember#pipelineAdd()}</li>
 * <li>{@link QuorumMember#pipelineRemove()}</li>
 * <li>{@link QuorumMember#pipelineChange(UUID, UUID)}</li>
 * </ul>
 * When a quorum is met, the <i>leader</i> is always first in the write pipeline
 * since it is the node which receives writes from clients. When a service joins
 * the write pipeline, it always does so at the end of the chain. Services may
 * enter the write pipeline before joining a quorum in order to synchronize with
 * the quorum. If a service in the middle of the chain leaves the pipeline, then
 * the upstream node will reconfigure and retransmit the current cache block to
 * its new downstream node. This prevent nodes which are "bouncing" during
 * synchronization from causing write sets to be discarded. However, if the
 * leader leaves the write pipeline, then the quorum is broken and the write set
 * will be discarded.
 * <p>
 * Since the write pipeline is used to synchronize services trying to join the
 * quorum as well as the replicate writes for services joined with the quorum,
 * {@link HAReceiveService} may be live for a met quorum even though the
 * {@link QuorumMember} on whose behalf this class is acting is not joined with
 * the met quorum.
 * 
 * <h3>Pipeline maintenance</h3>
 * 
 * There are three broad categories which have to be handled: (1) leader leaves;
 * (2) pipeline leader election; and (3) follower leaves. A leader leave causes
 * the quorum to break, which will cause service leaves and pipeline leaves for
 * all joined services. However, services must add themselves to the pipeline
 * before they join the quorum and the pipeline will be reorganized if necessary
 * when the quorum leader is elected. This will result in a
 * {@link #pipelineElectedLeader()} event. A follower leave only causes the
 * follower to leave the pipeline and results in a
 * {@link #pipelineChange(UUID, UUID)} event.
 * <p>
 * There are two cases for a follower leave: (A) when the follower did not did
 * not have a downstream node; and (B) when there is downstream node. For (B),
 * the upstream node from the left follower should reconfigure for the new
 * downstream node and retransmit the current cache block and the event should
 * be otherwise unnoticed.
 * <p>
 * Handling a follower join requires us to synchronize the follower first which
 * requires some more infrastructure and should be done as part of the HA
 * synchronization test suite.
 * <p>
 * What follows is an example of how events will arrive for a quorum of three
 * services: A, B, and C.
 * 
 * <pre>
 * A.getActor().pipelineAdd() => A.pipelineAdd()
 * B.getActor().pipelineAdd() => B.pipelineAdd(); A.pipelineChange(null,B); 
 * C.getActor().pipelineAdd() => C.pipelineAdd(); B.pipelineChange(null,C);
 * </pre>
 * 
 * At this point the pipeline order is <code>[A,B,C]</code>. Notice that the
 * {@link HASendService} for A is not established until the
 * <code>A.pipelineChange(null,B)</code> sets B as the new downstream service
 * for A. Likewise, B will not relay to C until it handles the
 * <code>B.pipelineChange(null,C)</code> event.
 * 
 * <p>
 * 
 * Given the pipeline order <code>[A,B,C]</code>, if B were to leave, then the
 * events would be:
 * 
 * <pre>
 * B.getActor().pipelineRemove() => B.pipelineRemove(); A.pipelineChange(B,C);
 * </pre>
 * 
 * and when this class handles the <code>A.pipelineChange(B,C)</code> event, it
 * must update the {@link HAReceiveService} such that it now relays data to C.
 * 
 * <p>
 * 
 * On the other hand, given the pipeline order <code>[A,B,C]</code>, if C were
 * to leave the events would be:
 * 
 * <pre>
 * C.getActor().pipelineRemove() => C.pipelineRemove(); B.pipelineChange(C,null);
 * </pre>
 * 
 * and when this class handles the <code>B.pipelineChange(C,null)</code> event,
 * it must update the C's {@link HAReceiveService} such that it continues to
 * receive data, but no longer relays data to a downstream service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S>
 */
abstract public class QuorumPipelineImpl<S extends HAPipelineGlue> extends
        QuorumStateChangeListenerBase implements QuorumPipeline<S>,
        QuorumStateChangeListener {

    static private transient final Logger log = Logger
            .getLogger(QuorumPipelineImpl.class);

    /**
     * The {@link QuorumMember}.
     */
    protected final QuorumMember<S> member;

    /**
     * The service {@link UUID} for the {@link QuorumMember}.
     */
    protected final UUID serviceId;
    
    /**
     * Lock managing the various mutable aspects of the pipeline state.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /** send service (iff this is the leader). */
    private HASendService sendService;

    /**
     * The receive service (iff this is a follower in a met quorum).
     */
    private HAReceiveService<HAMessageWrapper> receiveService;

    /**
     * The buffer used to relay the data. This is only allocated for a
     * follower. 
     */
    private IBufferAccess receiveBuffer;
    
    /**
     * Cached metadata about the downstream service.
     */
    private final AtomicReference<PipelineState<S>> pipelineStateRef = new AtomicReference<PipelineState<S>>();

    public QuorumPipelineImpl(final QuorumMember<S> member) {

        if (member == null)
            throw new IllegalArgumentException();

        this.member = member;

        this.serviceId = member.getServiceId();

    }

    /**
     * Extended to invoke {@link #tearDown()} in order to guarantee the eventual
     * release of the {@link #receiveBuffer} and the shutdown of the
     * {@link #sendService} or {@link #receiveService}.
     */
    @Override
    protected void finalize() throws Throwable {

        tearDown();

        super.finalize();

    }

    /**
     * Return the index at which the given serviceId appears in the array of
     * serviceIds.
     * 
     * @param serviceId
     *            The {@link UUID} of some quorum member.
     * @param a
     *            An array of service {@link UUID}s.
     * 
     * @return The index of the service in the array -or- <code>-1</code> if the
     *         service does not appear in the array.
     */
    private int getIndex(final UUID serviceId, final UUID[] a) {

        if (serviceId == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < a.length; i++) {

            if (serviceId.equals(a[i])) {

                return i;

            }
        }

        return -1;

    }

    /**
     * Return the NIO buffer used to receive payloads written on the HA write
     * pipeline.
     * 
     * @return The buffer -or- <code>null</code> if the pipeline has been torn
     *         down or if this is the leader.
     */
    private ByteBuffer getReceiveBuffer() {

        if (!lock.isHeldByCurrentThread()) {

            // The caller MUST be holding the lock.
            throw new IllegalMonitorStateException();

        }

        // trinary pattern is safe while thread has lock.
        return receiveBuffer == null ? null : receiveBuffer.buffer();

    }

    /**
     * Return the {@link HAReceiveService} used to receive payloads written on
     * the HA write pipeline.
     * 
     * @return The buffer -or- <code>null</code> if the pipeline has been torn
     *         down or if this is the leader.
     */
    private HAReceiveService<HAMessageWrapper> getHAReceiveService() {

        if (!lock.isHeldByCurrentThread()) {

            // The caller MUST be holding the lock.
            throw new IllegalMonitorStateException();

        }

        return receiveService;
        
    }

    /**
     * Return the {@link HASendService} used to write payloads on the HA write
     * pipeline.
     * 
     * @return The {@link HASendService} -or- <code>null</code> if the pipeline
     *         has been torn down.
     */
    private HASendService getHASendService() {
        
        if (!lock.isHeldByCurrentThread()) {

            // The caller MUST be holding the lock.
            throw new IllegalMonitorStateException();

        }

        return sendService;
        
    }

    /*
     * QuorumStateChangeListener 
     */
    
//    /**
//     * Extended to setup this service as a leader ({@link #setUpLeader()}),
//     * or a relay ({@link #setUpReceiveAndRelay()}. 
//     */
//    @Override
//    public void quorumMeet(final long token, final UUID leaderId) {
//        super.quorumMeet(token, leaderId);
//        lock.lock();
//        try {
//            this.token = token;
//            if(leaderId.equals(serviceId)) {
//                setUpLeader();
//            } else if(member.isPipelineMember()) {
//                setUpReceiveAndRelay();
//            }
//        } finally {
//            lock.unlock();
//        }        
//    }

//    @Override
//    public void quorumBreak() {
//        super.quorumBreak();
//        lock.lock();
//        try {
//            tearDown();
//        } finally {
//            lock.unlock();
//        }
//    }

    /**
     * Sets up the {@link HASendService} or the {@link HAReceiveService} as
     * appropriate depending on whether or not this service is the first in the
     * pipeline order.
     */
    public void pipelineAdd() {
        if (log.isInfoEnabled())
            log.info("");
        super.pipelineAdd();
        lock.lock();
        try {
            // The current pipeline order.
            final UUID[] pipelineOrder = member.getQuorum().getPipeline();
            // The index of this service in the pipeline order.
            final int index = getIndex(serviceId, pipelineOrder);
            if (index == 0) {
                setUpSendService();
            } else 
            if (index > 0) {
                setUpReceiveService();
            }
        } finally {
            lock.unlock();
        }
    }

    public void pipelineElectedLeader() {
        if (log.isInfoEnabled())
            log.info("");
        super.pipelineElectedLeader();
        lock.lock();
        try {
            tearDown();
            setUpSendService();
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Tears down the {@link HASendService} or {@link HAReceiveService}
     * associated with this service.
     */
    @Override
    public void pipelineRemove() {
        if (log.isInfoEnabled())
            log.info("");
        super.pipelineRemove();
        lock.lock();
        try {
            tearDown();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Changes the target of the {@link HASendService} for the leader (or the
     * {@link HAReceiveService} for a follower) to send (or relay) write cache
     * blocks to the specified service.
     */
    public void pipelineChange(final UUID oldDownStreamId,
            final UUID newDownStreamId) {
        super.pipelineChange(oldDownStreamId, newDownStreamId);
        lock.lock();
        try {
            // The address of the next service in the pipeline.
            final InetSocketAddress addrNext = newDownStreamId == null ? null
                    : getAddrNext(newDownStreamId);
            if (log.isInfoEnabled())
                log.info("oldDownStreamId=" + oldDownStreamId
                        + ",newDownStreamId=" + newDownStreamId + ", addrNext="
                        + addrNext + ", sendService=" + sendService
                        + ", receiveService=" + receiveService);
            if (sendService != null) {
                /*
                 * Terminate the existing connection (we were the first service
                 * in the pipeline).
                 */
                sendService.terminate();
                if (addrNext != null) {
                    if (log.isDebugEnabled())
                        log.debug("sendService.start(): addrNext=" + addrNext);
                    sendService.start(addrNext);
                }
            } else if (receiveService != null) {
                /*
                 * Reconfigure the receive service to change how it is relaying
                 * (we were relaying, so the receiveService was running but not
                 * the sendService).
                 */
                if (log.isDebugEnabled())
                    log.debug("receiveService.changeDownStream(): addrNext="
                            + addrNext);
                receiveService.changeDownStream(addrNext);
            }
            // populate and/or clear the cache.
            cachePipelineState(newDownStreamId);
            if (log.isDebugEnabled())
                log.debug("pipelineChange - done.");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void pipelineUpstreamChange() {
        super.pipelineUpstreamChange();
        lock.lock();
        try {
            if (receiveService != null) {
                /*
                 * Make sure that the receiveService closes out its client
                 * connection with the old upstream service.
                 */
                if (log.isInfoEnabled())
                    log.info("receiveService=" + receiveService);
                receiveService.changeUpStream();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Request the {@link InetSocketAddress} of the write pipeline for a service
     * (RMI).
     * 
     * @param downStreamId
     *            The service.
     *            
     * @return It's {@link InetSocketAddress}
     */
    private InetSocketAddress getAddrNext(final UUID downStreamId) {

        if (downStreamId == null)
            return null;

        final S service = member.getService(downStreamId);

        try {

            final InetSocketAddress addrNext = service.getWritePipelineAddr();

            return addrNext;
            
        } catch (IOException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * Tear down any state associated with the {@link QuorumPipelineImpl}. This
     * implementation tears down the send/receive service and releases the
     * receive buffer.
     */
    private void tearDown() {
        if (log.isInfoEnabled())
            log.info("");
        lock.lock();
        try {
            /*
             * Leader tear down.
             */
            {
                if (sendService != null) {
                    sendService.terminate();
                    sendService = null;
                }
            }
            /*
             * Follower tear down.
             */
            {
                if (receiveService != null) {
                    receiveService.terminate();
                    try {
                        receiveService.awaitShutdown();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        receiveService = null;
                    }
                }
                if (receiveBuffer != null) {
                    try {
                        /*
                         * Release the buffer back to the pool.
                         */
                        receiveBuffer.release();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        receiveBuffer = null;
                    }
                }
            }
            // clear cache.
            pipelineStateRef.set(null);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Populate or clear the {@link #pipelineState} cache.
     * <p>
     * Note: The only times we need to populate the {@link #pipelineState} are
     * in response to a {@link #pipelineChange(UUID, UUID)} event or in response
     * to message a {@link #pipelineElectedLeader()} event.
     * 
     * @param downStreamId
     *            The downstream service {@link UUID}.
     */
    private void cachePipelineState(final UUID downStreamId) {
        
        if (downStreamId == null) {
        
            pipelineStateRef.set(null);
            
            return;
        
        }

        final S nextService = member.getService(downStreamId);
        
        final PipelineState<S> pipelineState = new PipelineState<S>();
        
        try {

            pipelineState.addr = nextService.getWritePipelineAddr();
            
        } catch (IOException e) {
            
            throw new RuntimeException(e);
            
        }
        
        pipelineState.service = nextService;
        
        this.pipelineStateRef.set(pipelineState);
        
    }
    
    /**
     * Setup the send service.
     */
    private void setUpSendService() {
        if (log.isInfoEnabled())
            log.info("");
        lock.lock();
        try {
            // Allocate the send service.
            sendService = new HASendService();
            /*
             * The service downstream from this service.
             * 
             * Note: The downstream service in the pipeline is not available
             * when the first service adds itself to the pipeline. In those
             * cases the pipelineChange() event is used to update the
             * HASendService to send to the downstream service.
             * 
             * Note: When we handle a pipelineLeaderElected() message the
             * downstream service MAY already be available, which is why we
             * handle downstreamId != null conditionally.
             */
            final UUID downstreamId = member.getDownstreamServiceId();
            if (downstreamId != null) {
                // The address of the next service in the pipeline.
                final InetSocketAddress addrNext = member.getService(
                        downstreamId).getWritePipelineAddr();
                // Start the send service.
                sendService.start(addrNext);
            }
            // populate and/or clear the cache.
            cachePipelineState(downstreamId);
        } catch (Throwable t) {
            try {
                tearDown();
            } catch (Throwable t2) {
                log.error(t2, t2);
            }
            throw new RuntimeException(t);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Glue class wraps the {@link IHAWriteMessage} and the
     * {@link IHALogRequest} message and exposes the requires {@link IHAMessage}
     * interface to the {@link HAReceiveService}. This class is never persisted.
     * It just let's us handshake with the {@link HAReceiveService} and get back
     * out the original {@link IHAWriteMessage} as well as the optional
     * {@link IHALogRequest} message.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class HAMessageWrapper extends HAWriteMessageBase {

        private static final long serialVersionUID = 1L;

        final IHASyncRequest req;
        final IHAWriteMessage msg;
        
        public HAMessageWrapper(final IHASyncRequest req,
                final IHAWriteMessage msg) {

            // Use size and checksum from real IHAWriteMessage.
            super(msg.getSize(),msg.getChk());
            
            this.req = req; // MAY be null;
            this.msg = msg;
            
        }

    }
    
    /**
     * Setup the service to receive pipeline writes and to relay them (if there
     * is a downstream service).
     */
    private void setUpReceiveService() {
        lock.lock();
        try {
            // The downstream service UUID.
            final UUID downstreamId = member.getDownstreamServiceId();
            // Acquire buffer from the pool to receive data.
            try {
                receiveBuffer = DirectBufferPool.INSTANCE.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // The address of this service.
            final InetSocketAddress addrSelf = member.getService()
                    .getWritePipelineAddr();
            // Address of the downstream service (if any).
            final InetSocketAddress addrNext = downstreamId == null ? null
                    : member.getService(downstreamId).getWritePipelineAddr();
            // Setup the receive service.
            receiveService = new HAReceiveService<HAMessageWrapper>(addrSelf,
                    addrNext, new IHAReceiveCallback<HAMessageWrapper>() {
                        public void callback(final HAMessageWrapper msg,
                                final ByteBuffer data) throws Exception {
                            // delegate handling of write cache blocks.
                            handleReplicatedWrite(msg.req, msg.msg, data);
                        }
                    });
            // Start the receive service - will not return until service is
            // running
            receiveService.start();
        } catch (Throwable t) {
            /*
             * Always tear down if there was a setup problem to avoid leaking
             * threads or a native ByteBuffer.
             */
            try {
                tearDown();
            } catch (Throwable t2) {
                log.error(t2, t2);
            } finally {
                log.error(t, t);
            }
            throw new RuntimeException(t);
        } finally {
            lock.unlock();
        }
    }

    /*
     * This is the leader, so send() the buffer.
     */
    @Override
    public Future<Void> replicate(final IHASyncRequest req,
            final IHAWriteMessage msg, final ByteBuffer b) throws IOException {

        final RunnableFuture<Void> ft;

        lock.lock();
        try {

            ft = new FutureTask<Void>(new RobustReplicateTask(req, msg, b));

        } finally {

            lock.unlock();

        }

        // Submit Future for execution (outside of the lock).
        member.getExecutor().execute(ft);

        // Return Future. Caller must wait on the Future.
        return ft;

    }

    /**
     * Task robustly replicates an {@link IHAWriteMessage} and the associated
     * payload. 
     */
    private class RobustReplicateTask implements Callable<Void> {

        /**
         * An historical message is indicated when the {@link IHASyncRequest} is
         * non-<code>null</code>.
         */
        private final IHASyncRequest req;
        
        /**
         * The {@link IHAWriteMessage}.
         */
        private final IHAWriteMessage msg;
        
        /**
         * The associated payload.
         */
        private final ByteBuffer b;

        /**
         * The token for the leader.
         */
        private final long quorumToken;

        /**
         * The #of times the leader in a highly available quorum will attempt to
         * retransmit the current write cache block if there is an error when
         * sending that write cache block to the downstream node.
         */
        static protected final int RETRY_COUNT = 3;

        /**
         * The timeout for a sleep before the next retry. This timeout is designed
         * to allow some asynchronous processes to reconnect the
         * {@link HASendService} and the {@link HAReceiveService}s in write pipeline
         * such that a retransmit can succeed after a service has left the pipeline.
         */
        static protected final int RETRY_SLEEP = 50;

        public RobustReplicateTask(final IHASyncRequest req,
                final IHAWriteMessage msg, final ByteBuffer b) {
            
            // Note: [req] MAY be null.

            if (msg == null)
                throw new IllegalArgumentException();
            
            if (b == null)
                throw new IllegalArgumentException();

            this.req = req;

            this.msg = msg;
            
            this.b = b;

            if (b.remaining() == 0) {
             
                // Empty buffer.
                
                throw new IllegalStateException("Empty buffer: req=" + req
                        + ", msg=" + msg + ", buffer=" + b);
                
            }

            if (req == null) {
            
                /*
                 * Live message.
                 * 
                 * Use the quorum token on the message. It was put there by the
                 * WriteCacheService. This allows us to ensure that the qourum
                 * token remains valid for all messages replicated by the
                 * leader.
                 */

                quorumToken = msg.getQuorumToken();

            } else {
                
                /*
                 * Historical message.
                 */

                // Use the current quorum token.
                quorumToken = member.getQuorum().token();
                
            }
            
        }
        
        public Void call() throws Exception {

            try {

                innerReplicate(0/* retryCount */);
                
            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, InterruptedException.class)) {
                 
                    throw (InterruptedException) t;
                    
                }
                
                // Log initial error.
                log.error(t, t);

                if (!retrySend()) {

                    // Rethrow the original exception.
                    throw new RuntimeException(
                            "Giving up. Could not send after " + RETRY_COUNT
                                    + " attempts : " + t, t);

                }
                
            }

            return null;
            
        }

        /**
         * Replicate from the leader to the first follower. Each non-final
         * follower will receiveAndReplicate the write cache buffer. The last
         * follower will receive the buffer.
         * 
         * @param retryCount
         *            The #of attempts and ZERO (0) if this is the first
         *            attempt.
         *            
         * @throws Exception
         */
        private void innerReplicate(final int retryCount) throws Exception {

            lock.lockInterruptibly();

            try {

                if (log.isTraceEnabled())
                    log.trace("Leader will send: " + b.remaining()
                            + " bytes, retryCount=" + retryCount + ", req="
                            + req + ", msg=" + msg);

                /*
                 * Note: disable assert if we allow non-leaders to replicate
                 * HALog messages (or just verify service joined with the
                 * quorum).
                 */
                
                if (req == null) {

                    // // Note: Do not test quorum token for historical writes.
                    // member.assertLeader(msg.getQuorumToken());

                    /*
                     * This service must be the leader.
                     * 
                     * Note: The [quorumToken] is from the message IFF this is a
                     * live message and is otherwise the current quorum token.
                     */
                    member.assertLeader(quorumToken);

                }

                final PipelineState<S> downstream = pipelineStateRef.get();

                final HASendService sendService = getHASendService();

                final ByteBuffer b = this.b.duplicate();

                new SendBufferTask<S>(req, msg, b, downstream, sendService,
                        sendLock).call();

                return;

            } finally {

                lock.unlock();

            }

        } // call()

        /**
         * Robust retransmit of the current cache block. This method is designed to
         * handle several kinds of recoverable errors, including:
         * <ul>
         * <li>downstream service leaves the pipeline</li>
         * <li>intermittent failure sending the RMI message</li>
         * <li>intermittent failure sending the payload</li>
         * </ul>
         * The basic pattern is that it will retry the operation a few times to see
         * if there is a repeatable error. Each time it attempts the operation it
         * will discover the current downstream serviceId and verify that the quorum
         * is still valid. Each error (including the first) is logged. If the
         * operation fails, the original error is rethrown. If the operation
         * succeeds, then the cache block was successfully transmitted to the
         * current downstream service and this method returns without error.
         * 
         * @throws InterruptedException 
         */
        private boolean retrySend() throws InterruptedException {

            // we already tried once.
            int tryCount = 1;

            // now try some more times.
            for (; tryCount < RETRY_COUNT; tryCount++) {

                // Sleep before each retry (including the first).
                Thread.sleep(RETRY_SLEEP/* ms */);

                try {

                    // send to 1st follower.
                    innerReplicate(tryCount);

                    // Success.
                    return true;
                    
                } catch (Exception ex) {
                    
                    // log and retry.
                    log.error("retry=" + tryCount + " : " + ex, ex);
                    
                    continue;
                    
                }

            }

            // Send was not successful.
            return false;
            
        } // retrySend()

    } // class RobustReplicateTask
    
    /**
     * The logic needs to support the asynchronous termination of the
     * {@link Future} that is responsible for replicating the {@link WriteCache}
     * block, which is why the API exposes the means to inform the caller about
     * that {@link Future}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface IRetrySendCallback {
        /**
         * 
         * @param remoteFuture
         */
        void notifyRemoteFuture(final Future<Void> remoteFuture);
    }
    
    /**
     * Task to send() a buffer to the follower.
     */
    static private class SendBufferTask<S extends HAPipelineGlue> implements
            Callable<Void> {

        private final IHASyncRequest req;
        private final IHAWriteMessage msg;
        private final ByteBuffer b;
        private final PipelineState<S> downstream;
        private final HASendService sendService;
        private final Lock sendLock;

        public SendBufferTask(final IHASyncRequest req,
                final IHAWriteMessage msg, final ByteBuffer b,
                final PipelineState<S> downstream,
                final HASendService sendService, final Lock sendLock) {

            this.req = req; // Note: MAY be null.
            this.msg = msg;
            this.b = b;
            this.downstream = downstream;
            this.sendService = sendService;
            this.sendLock = sendLock;

        }

        public Void call() throws Exception {

            /*
             * Lock ensures that we do not have more than one request on the
             * write pipeline at a time.
             */

            sendLock.lock();
            try {

                doRunWithLock();
                
                return null;
                
            } finally {
                
                sendLock.unlock();
                
            }

        }
        
        private void doRunWithLock() throws InterruptedException,
                ExecutionException, IOException {

            // Get Future for send() outcome on local service.
            final Future<Void> futSnd = sendService.send(b);

            try {

                // Get Future for receive outcome on the remote service (RMI).
                final Future<Void> futRec = downstream.service
                        .receiveAndReplicate(req, msg);

                try {

                    /*
                     * Await the Futures, but spend more time waiting on the
                     * local Future and only check the remote Future every
                     * second. Timeouts are ignored during this loop.
                     */
                    while (!futSnd.isDone() && !futRec.isDone()) {
                        try {
                            futSnd.get(1L, TimeUnit.SECONDS);
                        } catch (TimeoutException ignore) {
                        }
                        try {
                            futRec.get(10L, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException ignore) {
                        }
                    }
                    futSnd.get();
                    futRec.get();

                } finally {
                    if (!futRec.isDone()) {
                        // cancel remote Future unless done.
                        futRec.cancel(true/* mayInterruptIfRunning */);
                    }
                }

            } finally {
                // cancel the local Future.
                futSnd.cancel(true/* mayInterruptIfRunning */);
            }

        }
        
    }
    
    /**
     * Lock used to ensure that at most one message is being sent along the
     * write pipeline at a time.
     */
    private final Lock sendLock = new ReentrantLock();

    @Override
    public Future<Void> receiveAndReplicate(final IHASyncRequest req,
            final IHAWriteMessage msg) throws IOException {

        final RunnableFuture<Void> ft;

        lock.lock();

        try {

            if (receiveBuffer == null) {
               
                /*
                 * The quorum broke and the receive buffer was cleared or
                 * possibly we have become a leader.
                 * 
                 * TODO We should probably pass in the Quorum and then just
                 * assert that the msg.getQuorumToken() is valid for the quorum
                 * (but we can't do that for historical messages).
                 */

                throw new QuorumException();
            
            }
        
            final PipelineState<S> downstream = pipelineStateRef.get();

            if (log.isTraceEnabled())
                log.trace("Will receive "
                        + ((downstream != null) ? " and replicate" : "") + ": msg="
                        + msg);
            
            final ByteBuffer b = getReceiveBuffer();
            
            final HAReceiveService<HAMessageWrapper> receiveService = getHAReceiveService();

         	if (downstream == null) {

                /*
                 * This is the last service in the write pipeline, so just receive
                 * the buffer.
                 * 
                 * Note: The receive service is executing this Future locally on
                 * this host. We do not submit it for execution ourselves.
                 */

                try {

                    // wrap the messages together.
                    final HAMessageWrapper wrappedMsg = new HAMessageWrapper(
                            req, msg);
                    
                    // receive.
                    return receiveService.receiveData(wrappedMsg, b);

                } catch (InterruptedException e) {

                    throw new RuntimeException(e);

                }

            }
            
            /*
             * A service in the middle of the write pipeline (not the first and
             * not the last).
             */

            ft = new FutureTask<Void>(new ReceiveAndReplicateTask<S>(req, msg,
                    b, downstream, receiveService));

        } finally {
            
            lock.unlock();
            
        }

        // execute the FutureTask.
        member.getExecutor().execute(ft);

        return ft;

    }
    
    /**
     * A service in the middle of the write pipeline (not the first and not the
     * last).
     */
    private static class ReceiveAndReplicateTask<S extends HAPipelineGlue>
            implements Callable<Void> {
        
        private final IHASyncRequest req;
        private final IHAWriteMessage msg;
        private final ByteBuffer b;
        private final PipelineState<S> downstream;
        private final HAReceiveService<HAMessageWrapper> receiveService;

        public ReceiveAndReplicateTask(final IHASyncRequest req,
                final IHAWriteMessage msg, final ByteBuffer b,
                final PipelineState<S> downstream,
                final HAReceiveService<HAMessageWrapper> receiveService) {

            this.req = req; // Note: MAY be null.
            this.msg = msg;
            this.b = b;
            this.downstream = downstream;
            this.receiveService = receiveService;
        }

        public Void call() throws Exception {

            // wrap the messages together.
            final HAMessageWrapper wrappedMsg = new HAMessageWrapper(
                    req, msg);

            // Get Future for send() outcome on local service.
            final Future<Void> futSnd = receiveService.receiveData(wrappedMsg,
                    b);

            try {

                // Get future for receive outcome on the remote
                // service.
                final Future<Void> futRec = downstream.service
                        .receiveAndReplicate(req, msg);

                try {

                    /*
                     * Await the Futures, but spend more time
                     * waiting on the local Future and only check
                     * the remote Future every second. Timeouts are
                     * ignored during this loop.
                     */
                    while (!futSnd.isDone() && !futRec.isDone()) {
                        try {
                            futSnd.get(1L, TimeUnit.SECONDS);
                        } catch (TimeoutException ignore) {
                        }
                        try {
                            futRec.get(10L, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException ignore) {
                        }
                    }
                    futSnd.get();
                    futRec.get();

                } finally {
                    if (!futRec.isDone()) {
                        // cancel remote Future unless done.
                        futRec
                                .cancel(true/* mayInterruptIfRunning */);
                    }
                }

            } finally {
                // cancel the local Future.
                futSnd.cancel(true/* mayInterruptIfRunning */);
            }

            // done
            return null;
        }

    }

    /**
     * Core implementation handles the message and payload when received on a
     * service.
     * 
     * @param req
     *            The synchronization request (optional). When non-
     *            <code>null</code> the msg and payload are historical data.
     *            When <code>null</code> they are live data.
     * @param msg
     *            Metadata about a buffer containing data replicated to this
     *            node.
     * @param data
     *            The buffer containing the data.
     * 
     * @throws Exception
     */
    abstract protected void handleReplicatedWrite(final IHASyncRequest req,
            final IHAWriteMessage msg, final ByteBuffer data) throws Exception;

    /**
     * A utility class that bundles together the Internet address and port at which
     * the downstream service will accept and relay cache blocks for the write
     * pipeline and the remote interface which is used to communicate with that
     * service using RMI.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private
    static
    class PipelineState<S extends HAPipelineGlue> implements Externalizable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * The Internet address and port at which the downstream service will accept
         * and relay cache blocks for the write pipeline.
         */
        public InetSocketAddress addr;

        /**
         * The remote interface for the downstream service which will accept and
         * relay cache blocks from this service.
         * <p>
         * Note: In order for an instance of this class to be serializable, an
         * exported proxy for the {@link HAGlue} object must be used here rather
         * than the local object reference.
         */
        public S service;

        public PipelineState() {

        }

        @SuppressWarnings("unchecked")
        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            addr = (InetSocketAddress) in.readObject();

            service = (S) in.readObject();

        }

        public void writeExternal(final ObjectOutput out) throws IOException {

            out.writeObject(addr);

            out.writeObject(service);

        }

    }
    
}
