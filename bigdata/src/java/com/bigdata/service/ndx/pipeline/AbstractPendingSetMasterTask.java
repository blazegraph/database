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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.btree.BigdataMap;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.INotifyOutcome;

/**
 * Extends the master task to track outstanding asynchronous operations on work
 * items.
 * <p>
 * The clients notify the {@link AbstractPendingSetSubtask} as each operation
 * completes. The subtask notifies the master, which then clears the entry from
 * its {@link #pendingMap} and also clears the entry from any other subtask that
 * had been tasked with the same work item (this permits subtasks to terminate
 * as soon as their work is complete regardless of which subtask actually
 * performed the work). The master will not terminate until all outstanding
 * asynchronous operations (the pending set) are complete.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractPendingSetMasterTask<//
H extends AbstractPendingSetMasterStats<L, ? extends AbstractSubtaskStats>, //
E, //
S extends AbstractPendingSetSubtask, //
L> //
        extends AbstractMasterTask<H, E, S, L> //
implements INotifyOutcome<E, L>
{

    /**
     * Lock used to serialize operations on the {@link #pendingMap}.
     */
    private final ReentrantLock lock = new ReentrantLock();

    private final JiniFederation<?> fed;

    public JiniFederation<?> getFederation() {

        return fed;

    }

    /**
     * A proxy for this class which is used by the client task to send
     * asynchronous notifications.
     */
    protected final INotifyOutcome<E,L> masterProxy;

    /**
     * Return the pending map. The pending map reflects the resources which are
     * in process. Resources are added to this collection when they are posted
     * to a client for processing and are removed when the client asynchronously
     * reports success or failure for the resource.
     */
    abstract protected Map<E, Collection<L>> getPendingMap();
    
    /**
     * @param stats
     * @param buffer
     * @param sinkIdleTimeoutNanos
     * @param sinkPollTimeoutNanos
     */
    public AbstractPendingSetMasterTask(final JiniFederation<?> fed,
            final H stats, final BlockingBuffer<E[]> buffer,
            final long sinkIdleTimeoutNanos, final long sinkPollTimeoutNanos) {

        super(stats, buffer, sinkIdleTimeoutNanos, sinkPollTimeoutNanos);

        if (fed == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.masterProxy = (INotifyOutcome<E, L>) fed
                .getProxy(this, true/* enableDGC */);
        
    }

    final protected boolean nothingPending() {
        lock.lock();
        try {
            return getPendingMap().isEmpty();
        } finally {
            lock.unlock();
        }
    }

    final public int getPendingSetSize() {
        lock.lock();
        try {
            return getPendingMap().size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Add a work item to the pending set.
     * <p>
     * Note: This method is written such that a {@link BigdataMap} could be used
     * as the implementation object. (The tuple is always updated by an insert
     * when its value's state is changed.)
     * 
     * @param e
     *            The work item.
     * @param locator
     *            The locator of the subtask/client that will process that work
     *            item.
     * 
     * @return <code>true</code> iff the pending set did not contain an entry
     *         for that work item. Since entries are cleared from the map when
     *         work is successfully complete or if all pending operations fail
     *         for a work item, a <code>true</code> return does not
     *         conclusively indicate a new work item.
     */
    protected boolean addPending(final E e, final L locator) {
        if (e == null)
            throw new IllegalArgumentException();
        if (locator == null)
            throw new IllegalArgumentException();
        lock.lock();
        try {
            Collection<L> locators = getPendingMap().remove(e);
            if (locators == null) {
                locators = new LinkedHashSet<L>();
                locators.add(locator);
                getPendingMap().put(e, locators);
                // added to the map.
                return true;
            } else {
                // already in the map.
                locators.add(locator);
                getPendingMap().put(e, locators);
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove a work item from the pending set.
     * <p>
     * Note: This method is written such that a {@link BigdataMap} could be used
     * as the implementation object. (The tuple is always updated by an insert
     * when its value's state is changed.)
     * 
     * @param e
     *            The work item.
     * @param locator
     *            The subtask / client locator.
     * @param cause
     *            <code>null</code> unless an error is being reported.
     * 
     * @return <code>true</code> iff the work item was cleared from the
     *         pending set (present on entry but cleared on exit).
     * 
     * @todo unit tests for the add/remove pending methods since they are a bit
     *       complex internally.
     */
     protected boolean removePending(final E e, final L locator,
            final Throwable cause) {
        if (e == null)
            throw new IllegalArgumentException();
        if (locator == null)
            throw new IllegalArgumentException();
        if (cause == null)
            throw new IllegalArgumentException();
        boolean notify = false;
        lock.lock();
        try {
            if (cause == null) {
                /*
                 * Successful completion.
                 */
                final Collection<L> locators = getPendingMap().remove(e);
                if (locators == null) {
                    /*
                     * Presume already successful since not in the map. Return
                     * false since map was not modified.
                     */
                    return false;
                }
                // for each locator tasked with that item.
                for (L t : locators) {
                    // clear item from the sink's pending set.
                    final S sink;
                    try {
                        sink = super.getSink(t, false/* reopen */);
                    } catch (InterruptedException ex) {
                        halt(ex);
                        throw new RuntimeException(ex);
                    }
                    sink.removePending(e);
                }
                // notify (success).
                notify = locators != null;
                // no more requests remain for that work item.
                return true;
            }
            /*
             * Error reported.
             */
            final Collection<L> locators = getPendingMap().get(e);
            if (locators == null) {
                /*
                 * Presume already successful since not in the map. Return false
                 * since map was not modified.
                 */
                return false;
            }
            // remove the entry for the locator which reported the error.
            locators.remove(locator);
            {
                // clear item from the sink's pending set.
                final S sink;
                try {
                    sink = super.getSink(locator, false/* reopen */);
                } catch (InterruptedException ex) {
                    halt(ex);
                    throw new RuntimeException(ex);
                }
                sink.removePending(e);
            }
            if (locators.isEmpty()) {
                // no outstanding requests remain, so will notify error.
                getPendingMap().remove(e);
                // will notify.
                notify = true;
                // no more rquests for that work item.
                return true;
            } else {
                // otherwise outstanding requests remain, so update map.
                getPendingMap().put(e, locators);
                // requests remain for that work item.
                return false;
            }
        } finally {
            lock.unlock();
            // notify once we have released the lock.
            if (notify) {
                if (cause == null) {
                    didSucceed(e);
                } else {
                    didFail(e, cause);
                }
            }
        }
    }

    /**
     * Return a new pending map instance. The size of this collection places a
     * machine limit on the #of resources which may be processed concurrently. A
     * {@link BigdataMap} may be used if sufficient RAM is not available.
     */
    abstract protected Map<E, Collection<L>> newPendingMap();

    /**
     * The resource is removed from the {@link #pendingMap} and the pending set
     * for each sink for which there is an outstanding request for that
     * resource.
     */
    final public void success(final E e, final L locator) {

        removePending(e, locator, null/* cause */);

    }

    /**
     * The resource is removed from the pending set for the sink associated with
     * that locator. If there are no more outstanding requests for that resource
     * in the {@link #pendingMap} then the resource is removed from the pending
     * map as well.
     */
    final public void error(final E resource, final L locator, final Throwable cause) {

        if (removePending(resource, locator, null/* cause */)) {

            // all pending operations have failed for this resource.
            log.error(resource, cause);

        }

    }
    
    /**
     * Hook provides notification the first time work for the resource has been
     * successfully completed for any set of concurrent outstanding work
     * requests and may be <em>extended</em> if necessary. The final outcome
     * for each resource is not retained. Therefore if the same resource is
     * resubmitted after its successful completion or its failure, then this
     * method may be invoked again for that resource. The default implementation
     * logs the event @ INFO.
     */
    protected void didSucceed(final E e) {

        if (log.isInfoEnabled()) {

            // an asynchronous operation has succeeded for this resource.
            log.info("resource=" + e);

        }

    }

    /**
     * Hook provides notification if all outstanding work requests for the
     * resource have failed. There may be more than one request to perform the
     * same work. This method is not invoked until all such requests have failed
     * and is not invoked if any of those requests succeed. Note that work
     * requests MUST be idempotent. The default implementation logs the event @ ERROR.
     * 
     * @param resource
     *            The resource.
     * @param cause
     *            The exception.
     */
    protected void didFail(final E resource, final Throwable cause) {

        // all pending operations have failed for this resource.
        log.error(resource, cause);

    }

}
