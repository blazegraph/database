package com.bigdata.ha;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.ha.pipeline.HAReceiveService;
import com.bigdata.ha.pipeline.HASendService;
import com.bigdata.ha.pipeline.HAReceiveService.IHAReceiveCallback;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.quorum.QuorumStateChangeListener;
import com.bigdata.quorum.QuorumStateChangeListenerBase;

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
 * {@link #pipelineElectedLeader()} event. A follower leave only causes follower
 * to leave the pipeline and results in a {@link #pipelineChange(UUID, UUID)}
 * event.
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

    static protected transient final Logger log = Logger
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

    /** send service for the leader. */
    private HASendService sendService;

    /**
     * The receive services for the k-1 followers.
     * <p>
     * Note: you index directly into this array (the first element is
     * <code>null</code>).
     */
    private HAReceiveService<HAWriteMessage> receiveService;

    /**
     * The buffer used to relay the data. This is only allocated for a
     * follower. The indices are the quorum index values. Quorum members
     * that do not relay will have a <code>null</code> at their
     * corresponding index.
     */
    private ByteBuffer receiveBuffer;

    /**
     * Cached metadata about the downstream service.
     */
    private AtomicReference<PipelineState<S>> pipelineStateRef = new AtomicReference<PipelineState<S>>();
    
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
    protected int getIndex(final UUID serviceId, final UUID[] a) {
        if (serviceId == null)
            throw new IllegalArgumentException();
        for (int i = 0; i < a.length; i++) {
            if (serviceId.equals(a[i])) {
                return i;
            }
        }
        return -1;
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
                    : member.getService(newDownStreamId).getWritePipelineAddr();
            if (sendService != null) {
                // Terminate the existing connection.
                sendService.terminate();
                if (addrNext != null) {
                    sendService.start(addrNext);
                }
            } else if (receiveService != null) {
                /*
                 * Reconfigure the receive service to change how it is relaying.
                 */
                receiveService.changeDownStream(addrNext);
            }
            // populate and/or clear the cache.
            cachePipelineState(newDownStreamId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Tear down any state associated with the {@link QuorumPipelineImpl}. This
     * implementation tears down the send/receive service and releases the
     * receive buffer.
     */
    protected void tearDown() {
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
                        DirectBufferPool.INSTANCE.release(receiveBuffer);
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
        
        pipelineState.addr = nextService.getWritePipelineAddr();
        
        pipelineState.service = nextService;
        
        this.pipelineStateRef.set(pipelineState);
        
    }
    
    /**
     * Setup the send service.
     */
    protected void setUpSendService() {
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
            receiveService = new HAReceiveService<HAWriteMessage>(addrSelf,
                    addrNext, new IHAReceiveCallback<HAWriteMessage>() {
                        public void callback(HAWriteMessage msg, ByteBuffer data)
                                throws Exception {
                            // delegate handling of write cache blocks.
                            handleReplicatedWrite(msg, data);
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
    
    /**
     * Return the NIO buffer used to receive payloads written on the HA write
     * pipeline.
     * 
     * @throws IllegalStateException
     *             if this is the leader.
     */
    protected ByteBuffer getReceiveBuffer() {
        return receiveBuffer;
    }

    public HASendService getHASendService() {
        return sendService;
    }

    public HAReceiveService<HAWriteMessage> getHAReceiveService() {
        return receiveService;
    }

    public Future<Void> replicate(final HAWriteMessage msg, final ByteBuffer b)
            throws IOException {

        member.assertLeader(msg.getQuorumToken());

        final PipelineState<S> downstream = pipelineStateRef.get();

        final RunnableFuture<Void> ft;

        /*
         * This is the leader, so send() the buffer.
         */

        if (log.isTraceEnabled())
            log.trace("Leader will send: " + b.remaining() + " bytes");

        ft = new FutureTask<Void>(new Callable<Void>() {

            public Void call() throws Exception {

                // Get Future for send() outcome on local service.
                final Future<Void> futSnd = getHASendService().send(b);

                try {

                    // Get Future for receive outcome on the remote service.
                    final Future<Void> futRec = downstream.service
                            .receiveAndReplicate(msg);

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

                // done
                return null;
            }

        });

        // execute the FutureTask.
        member.getExecutor().execute(ft);

        return ft;

    }

    public Future<Void> receiveAndReplicate(final HAWriteMessage msg)
            throws IOException {

        final PipelineState<S> downstream = pipelineStateRef.get();

        if (log.isTraceEnabled())
            log.trace("Will receive "
                    + ((downstream != null) ? " and replicate" : "") + ": msg="
                    + msg);

        final ByteBuffer b = getReceiveBuffer();

        if (downstream == null) {

            /*
             * This is the last service in the write pipeline, so just receive
             * the buffer.
             * 
             * Note: The receive service is executing this Future locally on
             * this host. We do not submit it for execution ourselves.
             */

            try {

                return getHAReceiveService().receiveData(msg, b);

            } catch (InterruptedException e) {

                throw new RuntimeException(e);

            }

        }

        /*
         * A service in the middle of the write pipeline (not the first and not
         * the last).
         */
        final RunnableFuture<Void> ft = new FutureTask<Void>(
                new Callable<Void>() {

                    public Void call() throws Exception {

                        // Get Future for send() outcome on local service.
                        final Future<Void> futSnd = getHAReceiveService()
                                .receiveData(msg, b);

                        try {

                            // Get future for receive outcome on the remote
                            // service.
                            final Future<Void> futRec = downstream.service
                                    .receiveAndReplicate(msg);

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

                });

        // execute the FutureTask.
        member.getExecutor().execute(ft);

        return ft;

    }

    /**
     * Core implementation handles the message and payload when received on a
     * service.
     * 
     * @param msg
     *            Metadata about a buffer containing data replicated to this
     *            node.
     * @param data
     *            The buffer containing the data.
     *            
     * @throws Exception
     */
    abstract protected void handleReplicatedWrite(final HAWriteMessage msg,
            final ByteBuffer data) throws Exception;

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
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            addr = (InetSocketAddress) in.readObject();

            service = (S) in.readObject();

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeObject(addr);

            out.writeObject(service);

        }

    }

    
}
