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
 * Created on Jan 30, 2009
 */

package com.bigdata.zookeeper;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * Interface exposing a {@link ZooKeeper} client that may be transparently
 * replaced by a new instance in response to a {@link SessionExpiredException}.
 * <p>
 * The {@link SessionExpiredException} is a severe error. A {@link ZooKeeper}
 * instance which throws this exception WILL NOT reconnect. This is by design
 * since in general the application needs to be aware of expired sessions and
 * take correcting actions.
 * <p>
 * A {@link SessionExpiredException} means that all ephemeral znodes for the
 * {@link ZooKeeper} have been discarded by the ensemble. An application MUST be
 * extremely careful in using this interface that they do not violate
 * assumptions concerning their state within the zookeeper ensemble.
 * <p>
 * To ensure this, the application SHOULD use the following pattern.
 * 
 * <ol>
 * 
 * <li>
 * <p>
 * Obtain ZooKeeper client from this interface.
 * </p>
 * <p>
 * Note that the client MIGHT already have an expired session which has not yet
 * been reported to the watcher behind this interface. The implementation will
 * allocate a new {@link ZooKeeper} client if {@link ZooKeeper#getState()} is
 * {@link ZooKeeper.States#CLOSED}. Therefore the first event observed by (2)
 * MIGHT be {@link ZooKeeper.States#CLOSED} (or equivalently, the first request
 * made using the client MIGHT throw a {@link SessionExpiredException}), in
 * which case the application MUST request a new {@link ZooKeeper} client from
 * this interface.
 * </p>
 * </li>
 * 
 * <li>When {@link SessionExpiredException} is thrown or the state for a
 * {@link WatchedEvent} is {@link ZooKeeper.States#CLOSED}:
 * <ul>
 * <li>Handle the expired session in any manner demanded by the watcher's local
 * semantics. All ephemeral znodes for the old {@link ZooKeeper} client will
 * have been deleted so this is your chance to clean things up. You may also
 * have to halt processing based on the assumption that you are holding a
 * {@link ZLock}, etc.</li>
 * 
 * <li>If the event was survivable, then obtain a new {@link ZooKeeper} client
 * using this interface (e.g., goto 1).</li>
 * </ul>
 * </li>
 * </ol>
 * 
 * @see http://wiki.apache.org/hadoop/ZooKeeper/FAQ, which has a state
 *      transition diagram for the {@link ZooKeeper} client.
 * 
 * FIXME Check all use of {@link SessionExpiredException}, of
 * {@link ZooKeeper.States#CLOSED} or {@link ZooKeeper.States#isAlive()}, and
 * of {@link KeeperState#Expired}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo this does not make any systematic attempt to facilitate handling the
 *       other absorbing state which is an authentication failure.
 */
public class ZooKeeperAccessor {

    protected static final Logger log = Logger.getLogger(ZooKeeperAccessor.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    private volatile ZooKeeper zookeeper;

    private final String host;

    private final int sessionTimeout;
    
    public ZooKeeperAccessor(final String hosts, final int sessionTimeout)
            throws InterruptedException {

        if (hosts == null)
            throw new IllegalArgumentException();

        this.host = hosts;

        this.sessionTimeout = sessionTimeout;

        getZookeeper();

    }
    
    /**
     * Return a {@link ZooKeeper} instance that is not "dead" as reported by
     * {@link ZooKeeper.States#isAlive()}. This method will block and retry if
     * there is an {@link IOException} connecting to the ensemble and will log
     * errors during such retries.
     * 
     * @throws InterruptedException
     *             if interrupted while attempting to obtain a {@link ZooKeeper}
     *             client.
     * @throws IllegalStateException
     *             if this class is closed.
     *             
     * @todo variant with timeout, perhaps running on an executor service.
     */
    public synchronized ZooKeeper getZookeeper() throws InterruptedException {

        final long begin = System.nanoTime();
        
        int ntries = 1;

        while (true) {

            if (!open)
                throw new IllegalStateException("Closed");

            lock.lockInterruptibly();

            try {

                if (zookeeper != null && zookeeper.getState().isAlive()) {

                    if (INFO)
                        log.info("success: ntries="
                                + ntries
                                + ", elapsed="
                                + TimeUnit.NANOSECONDS.toMillis((System
                                        .nanoTime() - begin)));
                       
                    // success
                    return zookeeper;

                }

                try {

                    log.warn("Creating new client");

                    zookeeper = new ZooKeeper(host, sessionTimeout,
                            new ZooAliveWatcher());

                    // new client counts as an "event".
                    event.signalAll();
                    
                } catch (IOException ex) {

                    log.error("Could not connect: ntries=" + ntries, ex);

                    ntries++;

                }

            } finally {

                lock.unlock();

            }

            try {

                Thread.sleep(1000/* ms */);

            } catch (InterruptedException e) {

                throw e;

            }

            // try again.
            
        } // while

    }

    /**
     * 
     * @throws InterruptedException
     *             if interrupted before {@link ZooKeeper#close()} was finished.
     *             If this case this object will continue to report that it
     *             {@link #isOpen()} and should be {@link #close()}d again.
     */
    synchronized public void close() throws InterruptedException {
        
        if(!open) return;
        
        lock.lockInterruptibly();
        try {

            zookeeper.close();

            zookeeper = null;

            open = false;
            
            // close of this class counts as an event.
            event.signalAll();
            
            log.warn("Closed.");
            
        } finally {

            lock.unlock();

        }
        
    }
    private volatile boolean open = true;
    
    public boolean isOpen() {
        
        return open;
        
    }
    
    /**
     * Watcher for expired sessions.
     * <p>
     * Note: A new instance of this watcher MUST be used for each new
     * {@link ZooKeeper} client so that a watcher instance will never see events
     * from more than one client (e.g., one client was expired but another is
     * alive so we don't want to confuse the expired event as coming from the
     * one that is still alive).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ZooAliveWatcher implements Watcher {

        private boolean connected = false;
        
        public void process(final WatchedEvent e) {

            if(!open) return;

            if (INFO)
                log.info(e.toString());

            lock.lock();

            if(!open) return;
            
            try {
            
                switch (e.getState()) {
                
                case Unknown:
                    // @todo what to do with these events?
                    log.warn(e);
                    break;
                
                case Disconnected:
                    
                    if (connected) {
                        // transition to disconnected counts as an event.
                        connected = false;
                        event.signalAll();
                    }
                    break;

                case NoSyncConnected:
                case SyncConnected:
                    
                    if (!connected) {
                        // transition to connected counts as an event.
                        connected = true;
                        event.signalAll();
                    }
                    break;
                
                case Expired:
                    
                    // expired session counts as an event.
                    zookeeper = null;
                    event.signalAll();
                
                }
                
            } finally {
                
                lock.unlock();
                
            }

            for (Watcher w : watchers) {

                // send event to any registered watchers.
                w.process(e);

            }
            
        }

    }

    /**
     * Await {@link ZooKeeper} to be {@link ZooKeeper.States#CONNECTED}, but
     * not more than the specified timeout. Note that even a <code>true</code>
     * return DOES NOT mean that the {@link Zookeeper} instance returned by
     * {@link #getZookeeper()} will be connected by the time you attempt to use
     * it.
     * 
     * @param timeout
     * @param unit
     * 
     * @return <code>true</code> if we noticed the {@link ZooKeeper} client
     *         entering the connected state before the timeout.
     * 
     * @throws InterruptedException
     *             if interrupted awaiting the {@link ZooKeeper} client to be
     *             connected.
     * @throws IllegalStateException
     *             if this class is closed.
     */
    public boolean awaitZookeeperConnected(final long timeout,
            final TimeUnit unit) throws InterruptedException {

        final long begin = System.nanoTime();
        
        // nanoseconds remaining.
        long nanos = unit.toNanos(timeout);

        ZooKeeper.States state = null;

        while ((nanos -= (System.nanoTime() - begin)) > 0) {

            switch (state = getZookeeper().getState()) {

            case CONNECTED:
                if (INFO)
                    log.info("connected: elapsed="
                            + TimeUnit.NANOSECONDS
                                    .toMillis((System.nanoTime() - begin)));
                return true;
            
            case AUTH_FAILED:
                log.error("Zookeeper authorization failure.");
                break;
            
            default:
                // wait a bit, but not more than the time remaining.
                lock.lockInterruptibly();
                try {
                    event.awaitNanos(nanos);
                } finally {
                    lock.unlock();
                }
            }

        }

        final long elapsed = System.nanoTime() - begin;

        log.warn("Zookeeper: not connected: state=" + state + ", elapsed="
                + TimeUnit.NANOSECONDS.toMillis(elapsed));

        // not observably connected, auth failure, etc.
        return false;

    }

    /**
     * Lock controlling access to the {@link #event} {@link Condition}.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition signaled any time there is a {@link WatchedEvent} delivered to
     * our {@link #process(WatchedEvent)}.
     */
    private final Condition event = lock.newCondition();
    
    /**
     * Adds a {@link Watcher} which will receive {@link WatchedEvent}s until it
     * is removed.
     * 
     * @param w
     *            The watcher.
     */
    public void addWatcher(final Watcher w) {

        if (w == null)
            throw new IllegalArgumentException();

        if(INFO)
            log.info("watcher="+w);
        
        watchers.add(w);

    }

    /**
     * Remove a {@link Watcher}.
     * 
     * @param w
     *            The watcher.
     */
    public void removeWatcher(final Watcher w) {

        if (w == null)
            throw new IllegalArgumentException();

        if(INFO)
            log.info("watcher="+w);

        watchers.remove(w);

    }

    private final CopyOnWriteArrayList<Watcher> watchers = new CopyOnWriteArrayList<Watcher>();

}
