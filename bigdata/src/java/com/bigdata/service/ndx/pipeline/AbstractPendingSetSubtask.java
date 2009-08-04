/*

 Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jul 13, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.jini.master.ClientLocator;
import com.bigdata.service.jini.master.IAsynchronousClientTask;

/**
 * Extended to assign chunks of work items to a remote
 * {@link IAsynchronousClientTask}, to track the set of outstanding
 * asynchronous operations for a specific client task (the "pending set"), and
 * to close the client task when the sink not assign any more work to that
 * client.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractPendingSetSubtask<//
HS extends AbstractSubtaskStats, //
M extends AbstractPendingSetMasterTask<? extends AbstractPendingSetMasterStats<L, HS>, E, ? extends AbstractPendingSetSubtask, L>, //
E, //
L> //
        extends AbstractSubtask<HS, M, E, L> {

    final protected IAsynchronousClientTask<?, E> clientTask;

    /**
     * Lock used ONLY for the {@link #pendingSet}.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition signalled when the {@link #pendingSet} is empty.
     */
    private final Condition pendingSetEmpty = lock.newCondition();

    /**
     * {@inheritDoc}
     * 
     * @param clientTask
     *            The proxy for the remote client task.
     */
    public AbstractPendingSetSubtask(final M master, final L locator,
            final IAsynchronousClientTask<?, E> clientTask,
            final BlockingBuffer<E[]> buffer) {

        super(master, locator, buffer);

        if (clientTask == null)
            throw new IllegalArgumentException();

        this.clientTask = clientTask;

    }

    /**
     * Return the pending set.
     */
    abstract protected Set<E> getPendingSet();
    
    public int getPendingSetSize() {
        lock.lock();
        try {
            return getPendingSet().size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected final void cancelRemoteTask(final boolean mayInterruptIfRunning)
            throws InterruptedException {
        
        try {
 
            // cancel the remote task.
            clientTask.getFuture().cancel(mayInterruptIfRunning);
        
        } catch (RemoteException e) {
            
            throw new RuntimeException(e);
            
        }
        
    }

    @Override
    protected final void awaitPending() throws InterruptedException {
        /*
         * Instruct the client task that we will not assign it any new work.
         */
        try {
            clientTask.close();
        } catch (RemoteException ex) {
            throw new RuntimeException(toString(), ex);
        }
        /*
         * Wait for the pending set to empty.
         */
        lock.lockInterruptibly();
        try {
            if (!getPendingSet().isEmpty()) {
                pendingSetEmpty.await();
            }
        } finally {
            lock.unlock();
        }
    }

    final protected boolean addPending(final E e) {
        lock.lock();
        try {
            return getPendingSet().add(e);
        } finally {
            lock.unlock();
        }
    }

    final protected boolean removePending(final E e) {
        lock.lock();
        try {
            final boolean ret = getPendingSet().remove(e);
            if (getPendingSet().isEmpty()) {
                pendingSetEmpty.signal();
            }
            return ret;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Submits the chunk of resources for processing by the remote client task.
     * Clients should accept resources for asynchronous processing, notifying
     * the sink as resources succeed or fail.
     * 
     * @param chunk
     *            A chunk of resources to be processed.
     * 
     * @return <code>true</code> iff the client will not accept additional
     *         chunks.
     * 
     * @throws IOException
     *             RMI error.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected boolean handleChunk(final E[] chunk) throws ExecutionException,
            InterruptedException, IOException {

        assert chunk != null;
        assert chunk.length > 0;

        final int chunkSize = chunk.length;

        /*
         * Instantiate the procedure using the data from the chunk and submit it
         * to be executed on the DataService using an RMI call.
         */
        final long beginNanos = System.nanoTime();
        try {

            // /*
            // * @todo isolate this as a retry policy, but note that we need to
            // be
            // * able to indicate when the error is fatal, when the error was
            // * handled by a redirect and hence the sink should close, and when
            // * the error was handled by a successful retry.
            // */
            // boolean done = false;
            // final int maxtries = 3;
            // final long retryDelayNanos = TimeUnit.MILLISECONDS.toNanos(1000);
            // for (int ntries = 0; ntries < maxtries; ntries++) {
            //
            // try {

            /*
             * Submit (blocks until chunk is queued by the client task).
             */
            clientTask.accept(chunk);
            // done = true;
            // break;
            //
            // } catch (ExecutionException ex) {
            //
            // if (ntries + 1 < maxtries) {
            //
            // log.error("Will retry (" + ntries + " of " + maxtries
            // + "): " + this, ex);
            //
            // continue;
            //
            // }
            //
            // log.fatal(this, ex);
            //
            // throw ex;
            //
            // }
            // }
            // if (!done) {
            // // should not reach this point.
            // throw new AssertionError();
            // }

            if (log.isDebugEnabled())
                log.debug(stats);

            // keep reading.
            return false;

        } finally {

            final long elapsedNanos = System.nanoTime() - beginNanos;

            // update the local statistics.
            synchronized (stats) {
                stats.chunksOut.incrementAndGet();
                stats.elementsOut.addAndGet(chunkSize);
                stats.elapsedChunkWritingNanos += elapsedNanos;
            }

            // update the master's statistics.
            synchronized (master.stats) {
                master.stats.chunksOut.incrementAndGet();
                master.stats.elementsOut.addAndGet(chunkSize);
                // note: duplicate elimination is not being performed.
                // master.stats.duplicateCount.addAndGet(duplicateCount);
                master.stats.elapsedSinkChunkWritingNanos += elapsedNanos;
            }

        }

    }

    /**
     * @todo This should handle the redirect of the pendingSet if the remote
     *       client task dies. To be robust, we need to notice client death even
     *       if it occurs when we are not invoking client#accept(chunk). Doing
     *       that will require some changes to the logic in this class, perhaps
     *       only in {@link #awaitPending()} which might have to poll the future
     *       of the client to make sure that it is still alive (or check w/ zk
     *       but zk can disconnect clients overly eagerly).
     * 
     * @todo This class ALSO needs to handle the resubmit of the resources
     *       associated with the current submit. That will occur only when the
     *       remote client task dies while invoking client#accept(chunk). The
     *       {@link AbstractPendingSetMasterTask} needs to start a new client
     *       task for the given {@link ClientLocator}, ideally on a different
     *       {@link IRemoteExecutor} service. If there is no such available
     *       service then it could multiplex multiple client#s onto the same
     *       client, essentially doubling the load for some client. Or we could
     *       hash partition based on the #of remaining clients, which would
     *       distribute the load evenly over the remaining clients.
     */
    @Override
    protected void notifyClientOfRedirect(L locator, Throwable cause) {

        throw new UnsupportedOperationException();

    }

}
