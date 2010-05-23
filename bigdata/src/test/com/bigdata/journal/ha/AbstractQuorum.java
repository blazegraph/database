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
 * Created on May 23, 2010
 */

package com.bigdata.journal.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.journal.IRootBlockView;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Abstract base class handles much of the logic for the distribution of RMI
 * calls from the leader to the follower and for the HA write pipeline.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractQuorum implements Quorum {

    static protected final Logger log = Logger.getLogger(AbstractQuorum.class);

    /**
     * Constructor.
     */
    protected AbstractQuorum() {
    }

    /**
     * Initialization to be invoked after the constructor.
     */
    public void init() {

    }

    protected void finalize() throws Throwable {

        invalidate();

        super.finalize();

    }

    public boolean isLeader() {
        return getIndex() > 0;
    }

    public boolean isFollower() {
        return getIndex() > 0;
    }

    protected void assertLeader() {
        if (!isLeader())
            throw new IllegalStateException();
    }

    protected void assertFollower() {
        if (!isFollower())
            throw new IllegalStateException();
    }

    /**
     * Cancel the requests on the remote services (RMI). Any RMI related errors
     * are trapped.
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

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to prepare concurrently with the leader's IO.
         */

        final long begin = System.nanoTime();
        final long nanos = unit.toNanos(timeout);
        long remaining = nanos;
        assertLeader();

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
        final int n = size();
        for (int i = 1; i < n; i++) {

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Boolean> rf = getHAGlue(i).prepare2Phase(
                    rootBlock);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the leader's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            getExecutorService().submit(rf);

        }

        {
            /*
             * Run the operation on the leader using local method call in the
             * caller's thread to avoid deadlock.
             * 
             * Note: Because we are running this in the caller's thread on the
             * leader the timeout will be ignored for the leader.
             */
            final RunnableFuture<Boolean> f = getHAGlue(0/* leader */)
                    .prepare2Phase(rootBlock);
            // Note: This runs synchronously (ignores timeout).
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

        if (nyes < (replicationFactor() + 1) >> 1) {

            log.error("prepare rejected: nyes=" + nyes + " out of "
                    + replicationFactor());

        }

        return nyes;

    }

    public void commit2Phase(final long commitTime) throws IOException,
            InterruptedException {

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to commit concurrently with the leader's IO.
         */

        assertLeader();

        final List<RunnableFuture<Void>> remoteFutures = new LinkedList<RunnableFuture<Void>>();

        /*
         * For services (other than the leader) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        final int n = size();
        for (int i = 1; i < n; i++) {

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Void> rf = getHAGlue(i).commit2Phase(
                    commitTime);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the leader's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            getExecutorService().submit(rf);

        }

        {
            /*
             * Run the operation on the leader using local method call in the
             * caller's thread to avoid deadlock.
             */
            final RunnableFuture<Void> f = getHAGlue(0/* leader */)
                    .commit2Phase(commitTime);
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

    public void abort2Phase() throws IOException, InterruptedException {

        /*
         * To minimize latency, we first submit the futures for the other
         * services and then do f.run() on the leader. This will allow the other
         * services to commit concurrently with the leader's IO.
         */

        assertLeader();

        final long token = token();

        final List<RunnableFuture<Void>> remoteFutures = new LinkedList<RunnableFuture<Void>>();

        /*
         * For services (other than the leader) in the quorum, submit the
         * RunnableFutures to an Executor.
         */
        final int n = size();
        for (int i = 1; i < n; i++) {

            /*
             * Runnable which will execute this message on the remote service.
             */
            final RunnableFuture<Void> rf = getHAGlue(i).abort2Phase(token);

            // add to list of futures we will check.
            remoteFutures.add(rf);

            /*
             * Submit the runnable for execution by the leader's
             * ExecutorService. When the runnable runs it will execute the
             * message on the remote service using RMI.
             */
            getExecutorService().submit(rf);

        }

        {
            /*
             * Run the operation on the leader using local method call in the
             * caller's thread to avoid deadlock.
             */
            final RunnableFuture<Void> f = getHAGlue(0/* leader */)
                    .abort2Phase(token);
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
     * Handle a bad read from the local disk as identified by a checksum error
     * on the data in the record by reading on another member of the
     * {@link Quorum}.
     */
    public ByteBuffer readFromQuorum(final long addr)
            throws InterruptedException, IOException {

        if (replicationFactor() > 1) {

            // This service is not configured for high availability.
            throw new IllegalStateException();

        }

        // The quorum must be met, in which case there will be at least 1 other
        // node.
        if (!isQuorumMet()) {
            throw new IllegalStateException();
        }
        /*
         * Prefer to read on the previous service in the quorum order since it
         * will always have anything which has been written onto this service.
         * Otherwise, read on the downstream service in the quorum order. The
         * downstream node should also always have the record. Recent small
         * records will still be in cache (both on this node and the downstream
         * node) and thus should never have resulted in a read through to the
         * disk and a ChecksumError. Large records are written directly through
         * to the disk, but they are written through to the disk synchronously
         * so the downstream node will always have the large record on the disk
         * as well.
         */
        // The index of this service in the quorum.
        final int indexSelf = getIndex();

        final int indexOther = indexSelf > 0 ? indexSelf - 1 : indexSelf + 1;

        // The RMI interface to the node on which we will read.
        final HAGlue haGlue = getHAGlue(indexOther);

        /*
         * Read from that node. The request runs in the caller's thread.
         */
        try {

            final RunnableFuture<ByteBuffer> rf = haGlue.readFromDisk(token(),
                    addr);

            rf.run();

            return rf.get();

        } catch (ExecutionException e) {

            throw new RuntimeException(e);

        }

    }

    public Future<Void> replicate(final HAWriteMessage msg, final ByteBuffer b)
            throws IOException {

        final int indexSelf = getIndex();

        final Future<Void> ft;

        assertLeader();

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
                    final Future<Void> futRec = getHAGlue(indexSelf + 1)
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
        getExecutorService().submit((FutureTask<Void>) ft);

        return ft;

    }

    /**
     * Return the NIO buffer used to receive payloads written on the HA write
     * pipeline.
     * 
     * @throws IllegalStateException
     *             if this is the leader.
     */
    abstract protected ByteBuffer getReceiveBuffer();

    public Future<Void> receiveAndReplicate(final HAWriteMessage msg)
            throws IOException {

        assertFollower();

        if (log.isTraceEnabled())
            log.trace("Follower[" + getIndex() + "] : " + msg);

        final ByteBuffer b = getReceiveBuffer();

        final Future<Void> ft;

        if (isLastInChain()) {

            /*
             * This is the last node in the write pipeline, so just receive the
             * buffer.
             * 
             * Note: The receive service is executing this Future locally on
             * this host.
             */

            try {
                ft = getHAReceiveService().receiveData(msg, b);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        } else {

            /*
             * A node in the middle of the write pipeline.
             */

            ft = new FutureTask<Void>(new Callable<Void>() {

                public Void call() throws Exception {

                    final int indexSelf = getIndex();

                    // Get Future for send() outcome on local service.
                    final Future<Void> futSnd = getHAReceiveService()
                            .receiveData(msg, b);

                    try {

                        // Get future for receive outcome on the remote service.
                        final Future<Void> futRec = getHAGlue(indexSelf + 1)
                                .receiveAndReplicate(msg);

                        try {

                            /*
                             * Await the Futures, but spend more time waiting on
                             * the local Future and only check the remote Future
                             * every second. Timeouts are ignored during this
                             * loop.
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
            getExecutorService().submit((FutureTask<Void>) ft);

        }

        return ft;

    }

}
