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
 * Created on Jan 4, 2009
 */

package com.bigdata.zookeeper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;

/**
 * Basic queue pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractZooQueue<E extends Serializable> extends
        AbstractZooPrimitive {

    protected static final Logger log = Logger.getLogger(AbstractZooQueue.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    protected final String zroot;

    private final List<ACL> acl;
    
    private int capacity;
    
    /**
     * Return the prefix that is used for the children of the {@link #zroot}.
     */
    protected abstract String getChildPrefix();
    
    /**
     * The mode used to create elements in the queue.
     * <p>
     * Note: election differs by the use of the
     * {@link CreateMode#EPHEMERAL_SEQUENTIAL} flag in the ctor.
     */
    protected abstract CreateMode getCreateMode();

    /**
     * @param zookeeper
     * @param zroot
     *            The znode for the queue pattern.
     * @param acl
     *            The ACL for the zroot (used when it is created).
     * @param capacity
     *            The capacity of the queue. This is stored in the queue znode.
     *            A watcher is set so that a changed value will be noticed. Use
     *            {@link Integer#MAX_VALUE} for no limit, in which case the #of
     *            children in the queue will not be tested by
     *            {@link #add(Serializable)}.
     * 
     * @throws InterruptedException
     */
    public AbstractZooQueue(final ZooKeeper zookeeper, final String zroot,
            final List<ACL> acl, final int capacity) throws KeeperException,
            InterruptedException {

        super(zookeeper);

        if (zroot == null)
            throw new IllegalArgumentException();

        if (acl == null)
            throw new IllegalArgumentException();
        
        if (capacity < 0)
            throw new IllegalArgumentException();

        this.zroot = zroot;

        this.acl = acl;
        
        this.capacity = capacity;
        
        try {

            zookeeper.create(zroot, SerializerUtil.serialize(Integer
                    .valueOf(capacity)), acl, CreateMode.PERSISTENT);

            if (INFO)
                log.info("New queue: " + zroot);

        } catch (KeeperException.NodeExistsException ex) {

            if (INFO)
                log.info("Existing queue: " + zroot);
            
        }

        /*
         * Set watcher on the capacity so that we will be notified if it is
         * changed.
         */

        this.capacity = (Integer) SerializerUtil.deserialize(zookeeper
                .getData(zroot, new CapacityWatcher(), new Stat()));

    }

    private class CapacityWatcher implements Watcher {

        public void process(WatchedEvent event) {
            
        }

    }

    /**
     * Adds an element to the tail of the queue.
     */
    public void add(final E e) throws KeeperException, InterruptedException {

        try {

            add(e, Long.MAX_VALUE, TimeUnit.SECONDS);
            
        } catch (TimeoutException e1) {

            // should not be thrown.
            throw new AssertionError(e1);
            
        }
        
    }
    
    public void add(final E e, final long timeout, final TimeUnit unit)
            throws KeeperException, TimeoutException, InterruptedException {

        if (e == null)
            throw new IllegalArgumentException();

        if (capacity != Integer.MAX_VALUE) {

            if (!new BlockedWatcher(zookeeper, zroot).awaitCondition(timeout, unit)) {
                
                throw new TimeoutException();
                
            }
            
        }
        
        final String znode = zookeeper.create(zroot + "/" + getChildPrefix(),
                SerializerUtil.serialize(e), acl, getCreateMode());

        if (INFO)
            log.info("zroot=" + zroot + ", e=" + e + ", znode=" + znode);

    }

    /**
     * Watches until the queue is no longer at capacity.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class BlockedWatcher implements Watcher {

        private volatile boolean cancelled = false;
        
        private final ZooKeeper zookeeper;
        private final String zpath;
        
        public BlockedWatcher(final ZooKeeper zookeeper, final String zpath) {

            if (zookeeper == null)
                throw new IllegalArgumentException();

            if (zpath == null)
                throw new IllegalArgumentException();
            
            this.zookeeper = zookeeper;
            
            this.zpath = zpath;
            
        }
        
        public void process(final WatchedEvent e) {

            if (cancelled)
                return;

            synchronized(this) {
                
                this.notify();
                
            }

        }

        public void awaitCondition() throws InterruptedException,
                KeeperException {

            awaitCondition(Long.MAX_VALUE, TimeUnit.SECONDS);

        }

        /**
         * @throws KeeperException
         *             if we are unable to read the children of the znode the on
         *             entry.
         * @throws InterruptedException
         *             if the thread is interrupted while waiting for an event.
         * @throws InterruptedException
         *             if the znode we are watching is deleted.
         *             
         * @todo this will not notice a change in the queue capacity while
         *       blocked unless it sets its own watcher on the data for the
         *       znode.
         */
        public boolean awaitCondition(final long timeout, final TimeUnit unit)
                throws InterruptedException, KeeperException {

            final long begin = System.currentTimeMillis();
            
            long remaining = unit.toMillis(timeout);

            try {

                synchronized (this) {

                    /*
                     * If we can't read the znode the first time through then
                     * throw an exception. Otherwise we have set a watcher and
                     * will just hang around until it gets notified or we get
                     * interrupted.
                     */
                    int n = zookeeper.getChildren(zpath, this).size();

                    try {

                        while (n >= capacity) {

                            this.wait(remaining);

                            remaining -= System.currentTimeMillis() - begin;

                            n = zookeeper.getChildren(zpath, this).size();

                        }

                    } catch (NoNodeException e) {

                        // deleting the queue znode qualifies as an interrupt.
                        throw new InterruptedException();

                    } catch (KeeperException e) {

                        // log error but keep waiting for events.
                        log.error("zpath=" + zpath, e);
                        
                    }

                    return n < capacity;

                }

            } finally {

                cancelled = true;

            }

        }
        
    }
    
    /**
     * The approximate #of elements in the queue.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    public int size() throws KeeperException, InterruptedException {

//        zookeeper.sync(zroot, new AsyncCallback.VoidCallback() {
//
//            public void processResult(int rc, String path, Object ctx) {
//                log.info("callback: rc="+rc+", path="+path+", ctx="+ctx);
//            }
//        }, new Object()/*ctx*/);
        
        final List<String> list = zookeeper.getChildren(zroot, false/* watch */);

        final int n = list.size();

        if (INFO)
            log.info("zroot=" + zroot + ", size=" + n+", children="+list);

        return n;
        
    }
    
    /**
     * Removes the element at the head of the queue (blocking).
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     * 
     * @todo timeout variant.
     * @todo non-blocking variant.
     */
    public E remove() throws KeeperException, InterruptedException {

        if (INFO)
            log.info("zroot=" + zroot);

        while (true) {

            final List<String> list = zookeeper
                    .getChildren(zroot, this/* watcher */);

            if (list.size() == 0) {

                if (INFO)
                    log.info("zroot=" + zroot + " : blocked.");

                synchronized(lock) {
                    
                    lock.wait();
                    
                }

                continue;

            }
            
            // put into sorted order.
            final String[] a = list.toArray(new String[list.size()]);
            
            Arrays.sort(a);
            
            // first element (lex order is also sorted for sequential ids).
            final String selected = zroot + "/" + a[0];
            
            final E e = (E) SerializerUtil.deserialize(zookeeper.getData(
                    selected, false/* watch */, new Stat()));

            // note: uses -1 to ignore the version match.
            zookeeper.delete(selected, -1/* version */);

            if (INFO)
                log.info("zroot=" + zroot + ", e=" + e);

            return e;

        }

    }

}
