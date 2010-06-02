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
 * Created on Jun 1, 2010
 */

package com.bigdata.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Environment;
import com.bigdata.journal.IResourceManager;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ha.HAReceiveService;
import com.bigdata.journal.ha.HASendService;
import com.bigdata.journal.ha.HAWriteMessage;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Abstract implementation provides the logic for distributing messages for the
 * quorum 2-phase commit protocol, failover reads, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Change the generic type of <L> to {@link IResourceManager} or
 *       {@link Environment}.
 */
abstract public class AbstractHAService<S extends HAGlue, L extends AbstractJournal>
        extends AbstractQuorumMember<S> implements HAService<S> {

    static protected final Logger log = Logger
            .getLogger(AbstractHAService.class);

    private final S service;

    private final L localService;

    /**
     * @param quorum
     *            The object which tracks the quorum state.
     * @param service
     *            The interface for the local service that is exposed to remote
     *            clients (typically as a smart proxy).
     * @param localService
     *            The local service implementation.
     * 
     *            FIXME generics on the quorum argument.
     */ 
    protected AbstractHAService(final Quorum quorum, final S service,
            final L localService) {

        super(quorum, service.getServiceId());

        if (localService == null)
            throw new IllegalArgumentException();

        this.service = service;
        
        this.localService = localService;
        
    }
    
    /**
     * Return the local service implementation object (NOT the RMI proxy).
     */
    public S getService() {
        
        return service;
        
    }
    
    protected L getLocalService() {
        
        return localService;
        
    }

    public Executor getExecutor() {
        return getLocalService().getExecutorService();
    }

    /*
     * FIXME These methods should probably be resolved against the localService.
     * They were historically on the Quorum.
     */
    
    // @todo fast code path for self.
//    public S getService(UUID serviceId) {
//        return null;
//    }

//    public HASendService getHASendService() {
//        // TODO Auto-generated method stub
//        return null;
//    }

//  @todo getHAReceiveService()
//    public HAReceiveService<HAWriteMessage> getHAReceiveService() {
//        //return getLocalService().getHADelegate()...;
//    }

    /**
     * Cancel the requests on the remote services (RMI). This is a best effort
     * implementation. Any RMI related errors are trapped and ignored in order
     * to be robust to failures in RMI when we try to cancel the futures.
     */
    protected <F extends Future<T>, T> void cancelRemoteFutures(
            final List<F> remoteFutures) {

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
     */
    public int prepare2Phase(final IRootBlockView rootBlock,
            final long timeout, final TimeUnit unit)
            throws InterruptedException, TimeoutException, IOException {

        if (rootBlock == null)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();
        
        /*
         * The token of the quorum for which the leader issued this prepare
         * message.
         */
        final long token = rootBlock.getQuorumToken();

        // Verify the quorum is valid.
        assertLeader(token);
        
        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to prepare concurrently with the leader's IO.
         */

        final long begin = System.nanoTime();
        final long nanos = unit.toNanos(timeout);
        long remaining = nanos;

        int nyes = 0;

        // // Copy the root block into a byte[].
        // final byte[] data;
        // {
        // final ByteBuffer rb = rootBlock.asReadOnlyBuffer();
        // data = new byte[rb.limit()];
        // rb.get(data);
        // }

        final List<RunnableFuture<Boolean>> remoteFutures = new LinkedList<RunnableFuture<Boolean>>();

        /*
         * For services (other than the leader) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        final UUID[] joinedServiceIds = getQuorum().getJoinedMembers();
        
        for (int i = 1; i < joinedServiceIds.length; i++) {
            
            final UUID serviceId = joinedServiceIds[i];

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Boolean> rf = getService(serviceId)
                    .prepare2Phase(rootBlock);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the leader's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            getExecutor().execute(rf);

        }

        {
            /*
             * Run the operation on the leader using local method call in the
             * caller's thread to avoid deadlock.
             * 
             * Note: Because we are running this in the caller's thread on the
             * leader the timeout will be ignored for the leader.
             */
            final S leader = getService();
            final RunnableFuture<Boolean> f = leader
                    .prepare2Phase(rootBlock);
            /*
             * Note: This runs synchronously in the caller's thread (it ignores
             * timeout).
             */
            f.run();
            try {
                remaining = nanos - (begin - System.nanoTime());
                nyes += f.get(remaining, TimeUnit.NANOSECONDS) ? 1 : 0;
            } catch (ExecutionException e) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Error on the leader.
                throw new RuntimeException(e);
            } finally {
                f.cancel(true/* mayInterruptIfRunning */);
            }
        }

        /*
         * Check the futures for the other services in the quorum.
         */
        for (Future<Boolean> rf : remoteFutures) {
            boolean done = false;
            try {
                remaining = nanos - (begin - System.nanoTime());
                nyes += rf.get(remaining, TimeUnit.NANOSECONDS) ? 1 : 0;
                done = true;
            } catch (ExecutionException ex) {
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

        if (nyes < (k + 1) / 2) {

            log.error("prepare rejected: nyes=" + nyes + " out of " + k);

        }

        return nyes;

    }

    public void commit2Phase(final long token, final long commitTime)
            throws IOException, InterruptedException {

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to commit concurrently with the leader's IO.
         */

        assertLeader(token);

        final List<RunnableFuture<Void>> remoteFutures = new LinkedList<RunnableFuture<Void>>();

        /*
         * For services (other than the leader) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        final UUID[] joinedServiceIds = getQuorum().getJoinedMembers();
        
        for (int i = 1; i < joinedServiceIds.length; i++) {
            
            final UUID serviceId = joinedServiceIds[i];

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Void> rf = getService(serviceId).commit2Phase(
                    commitTime);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the leader's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            getExecutor().execute(rf);

        }

        {
            /*
             * Run the operation on the leader using local method call in the
             * caller's thread to avoid deadlock.
             */
            final S leader = getService();
            final RunnableFuture<Void> f = leader.commit2Phase(commitTime);
            // Note: This runs synchronously (ignores timeout).
            f.run();
            try {
                f.get();
            } catch (ExecutionException e) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Error on the leader.
                throw new RuntimeException(e);
            } finally {
                f.cancel(true/* mayInterruptIfRunning */);
            }
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
         */
        if (!causes.isEmpty()) {
            // Cancel remote futures.
            cancelRemoteFutures(remoteFutures);
            // Throw exception back to the leader.
            throw new RuntimeException("remote errors: nfailures="
                    + causes.size(), new ExecutionExceptions(causes));
        }

    }

    public void abort2Phase(final long token) throws IOException,
            InterruptedException {

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to commit concurrently with the leader's IO.
         */

        assertLeader(token);

        final List<RunnableFuture<Void>> remoteFutures = new LinkedList<RunnableFuture<Void>>();

        /*
         * For services (other than the leader) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        final UUID[] joinedServiceIds = getQuorum().getJoinedMembers();

        for (int i = 1; i < joinedServiceIds.length; i++) {

            final UUID serviceId = joinedServiceIds[i];

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Void> rf = getService(serviceId).abort2Phase(
                    token);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the leader's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            getExecutor().execute(rf);

        }

        {
            /*
             * Run the operation on the leader using local method call in the
             * caller's thread to avoid deadlock.
             */
            final RunnableFuture<Void> f = getLeaderService(token).abort2Phase(
                    token);
            // Note: This runs synchronously (ignores timeout).
            f.run();
            try {
                f.get();
            } catch (ExecutionException e) {
                // Cancel remote futures.
                cancelRemoteFutures(remoteFutures);
                // Error on the leader.
                throw new RuntimeException(e);
            } finally {
                f.cancel(true/* mayInterruptIfRunning */);
            }
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
         */
        if (!causes.isEmpty()) {
            // Cancel remote futures.
            cancelRemoteFutures(remoteFutures);
            // Throw exception back to the leader.
            throw new RuntimeException("remote errors: nfailures="
                    + causes.size(), new ExecutionExceptions(causes));
        }

    }

    /**
     * Return the {@link UUID} of the joined service to which this service will
     * direct a failover read. The default implementation uses a round-robin
     * policy.
     * 
     * @param joinedServiceIds
     *            The {@link UUID}s of the services currently joined with the
     *            quorum.
     * 
     * @return The {@link UUID} of the service to which the failover read will
     *         be directed.
     */
    protected UUID getNextBadReadServiceId(final UUID[] joinedServiceIds) {

        // This service.
        final UUID serviceId = getServiceId();

        /*
         * Note: This loop will always terminate if the quorum is joined since
         * there will be (k+1)/2 services on which we can read, which is a
         * minimum of one service which is not _this_ service even when k := 3.
         */

        while (true) {

            final int i = nextIndex.incrementAndGet() % joinedServiceIds.length;

            if (serviceId.equals(joinedServiceIds[i])) {

                // Do not issue the call to ourselves.
                continue;

            }

            // Issue the call to this service.
            return joinedServiceIds[i];

        }

    }

    /**
     * Used to implement a round-robin policy for
     * {@link #getNextBadReadServiceId(UUID[])}.
     */
    private final AtomicInteger nextIndex = new AtomicInteger();
    
    /**
     * {@inheritDoc}
     * 
     * @todo If this blocks awaiting a quorum, then make sure that it is not
     *       invoked in a context where it is holding a lock on the local
     *       low-level store!
     */
    public ByteBuffer readFromQuorum(final UUID storeId, final long addr)
            throws InterruptedException, IOException {

        if (storeId == null)
            throw new IllegalArgumentException();
        
        if (addr == IRawStore.NULL)
            throw new IllegalArgumentException();

        if (!getQuorum().isHighlyAvailable()) {

            // This service is not configured for high availability.
            throw new IllegalStateException();

        }

        // Block if necessary awaiting a met quorum.
        /*final long token =*/ getQuorum().awaitQuorum();

        final UUID[] joinedServiceIds = getQuorum().getJoinedMembers();

        // Figure out which other joined service we will read on.
        final UUID otherId = getNextBadReadServiceId(joinedServiceIds);
        
        // The RMI interface for the service on which we will read.
        final S otherService = getService(otherId);

        /*
         * Read from that service. The request runs in the caller's thread.
         */
        try {

            final RunnableFuture<ByteBuffer> rf = otherService.readFromDisk(
                    storeId, addr);

            rf.run();

            return rf.get();

        } catch (ExecutionException e) {

            throw new RuntimeException(e);

        }

    }

    public Future<Void> replicate(final HAWriteMessage msg, final ByteBuffer b)
            throws IOException {

        assertLeader(msg.getQuorumToken());

        final PipelineState<S> downstream = getDownPipelineState(msg
                .getQuorumToken());

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
        getExecutor().execute(ft);

        return ft;

    }

    public Future<Void> receiveAndReplicate(final HAWriteMessage msg)
            throws IOException {

        final PipelineState<S> downstream = getDownPipelineState(msg
                .getQuorumToken());

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
        getExecutor().execute(ft);

        return ft;

    }

    /**
     * Return the NIO buffer used to receive payloads written on the HA write
     * pipeline. Note that the quorum DOES NOT need to be met to use the write
     * pipeline.
     * 
     * @throws IllegalStateException
     *             if this is the leader.
     * 
     * @todo why not always have this buffer allocated regardless of whether or
     *       not we are the leader?
     */
    abstract protected ByteBuffer getReceiveBuffer();

    /**
     * Return the metadata about the downstream service.
     * 
     * FIXME The downstream {@link PipelineState} MUST be maintained based on
     * events pumped out of the {@link Quorum}. When the downstream service
     * changes, the {@link HASendService} and/or {@link HAReceiveService} need
     * to be dynamically reconfigured.
     */
    protected PipelineState<S> getDownPipelineState(final long token) {

        final UUID nextId = super.getDownstreamService(token);

        if (nextId == null) {

            return null;

        }

        final S nextService = getService(nextId);

        final PipelineState<S> tmp = new PipelineState<S>();

        tmp.addr = nextService.getWritePipelineAddr();

        tmp.service = nextService;

        return tmp;

    }

}
