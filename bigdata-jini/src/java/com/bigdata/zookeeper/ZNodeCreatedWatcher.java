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
 * Created on Jan 6, 2009
 */

package com.bigdata.zookeeper;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * An instance of this class may be used to watch for a "create" event for a
 * single znode. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZNodeCreatedWatcher extends AbstractZNodeConditionWatcher {

//    final static protected Logger log = Logger.getLogger(ZNodeCreatedWatcher.class);
//
//    final static protected boolean INFO = log.isInfoEnabled();
//
//    final static protected boolean DEBUG = log.isDebugEnabled();
//
//    protected final ZooKeeper zookeeper;
//    
//    /**
//     * 
//     * @param zookeeper
//     * @param zpath
//     *            The path that is being watched.
//     */
//    protected ZNodeCreatedWatcher(final ZooKeeper zookeeper, final String zpath) {
//
//        if (zookeeper == null)
//            throw new IllegalArgumentException();
//
//        if (zpath == null)
//            throw new IllegalArgumentException();
//
//        this.zookeeper = zookeeper;
//
//        this.zpath = zpath;
//
//    }
//    
//    private volatile boolean disconnected = false;
//    
//    private volatile boolean created = false;
//   
//    /**
//     * The zpath that is being watched.
//     */
//    protected final String zpath;
//    
//    /*
//     * Note: Impl MUST be safe and non-blocking.
//     */
//    public String toString() {
//        
//        return getClass().getSimpleName() + "{zpath=" + zpath + ", created="
//                + created + ", disconnected=" + disconnected + "}";
//        
//    }
//    
//    /**
//     * Clear the watch. This is necessary for the {@link Watcher} to stop
//     * getting notices of changes after it has noticed the change that it was
//     * looking for.
//     */
//    final protected void clearWatch() {
//        
//        try {
//
//            if(INFO)
//                log.info("Clearing watch: zpath="+zpath);
//        
//            // clears the watch.
//            zookeeper.exists(zpath, false);
//            
//        } catch (KeeperException ex) {
//            
//            // ignore
//            log.warn(ex);
//            
//        } catch (InterruptedException ex) {
//            
//            // ignore.
//            log.warn(ex);
//            
//        }
//
//    }
//
//    /**
//     * The watcher notifies a {@link Thread} synchronized on itself when the
//     * znode that it is watching is created. The caller sets an instance of this
//     * watcher on a <strong>single</strong> znode if it wishes to be notified
//     * when that znode is created and then {@link Object#wait()} on the watcher.
//     * The {@link Thread} MUST test {@link #created} while holding the lock and
//     * before waiting (in case the event has already occurred), and again each
//     * time {@link Object#wait()} returns (since wait and friends MAY return
//     * spuriously). The watcher will re-establish the watch if the
//     * {@link Watcher.Event.EventType} was not
//     * {@link Watcher.Event.EventType#NodeCreated}, but it WILL NOT continue to
//     * watch the znode once it has been created.
//     * 
//     * FIXME This pattern is not robust in the face of a {@link KeeperState}
//     * disconnect. on reconnect it would have to verify that the node either did
//     * or did not exist and then reset or clear its watch as necessary. The
//     * watcher will also need to hold the zpath itself. For
//     * {@link ZNodeDeletedWatcher} as well.
//     * 
//     * FIXME Encapsulate this pattern into a base class and then use it for the
//     * {@link ServiceConfigurationWatcher}.
//     */
//    public void process(final WatchedEvent event) {
//
//        if(INFO)
//            log.info(event.toString());
//        
//        synchronized (this) {
//
//            switch(event.getState()) {
//            case Disconnected:
//                // nothing to do until we are reconnected.
//                disconnected = true;
//                return;
//            default:
//                if (disconnected) {
//                    resumeWatch();
//                }
//                // fall through
//                break;
//            }
//            
//            if (event.getType().equals(Watcher.Event.EventType.NodeCreated)) {
//
//                success("created");
//                
//                return;
//
//            } else {
//                
//                resumeWatch();
//                
//            }
//
//        }
//
//    }
//
//    /**
//     * Resumes watching the zpath. However, if the condition is satisified then
//     * we report {@link #success(String)} and clear the watch.
//     */
//    protected void resumeWatch() {
//
//        try {
//
//            if (INFO)
//                log.info("will reset watch");
//
//            // reset the watch.
//            if (zookeeper.exists(zpath, this) != null) {
//
//                // in case we were disconnected.
//                disconnected = false;
//
//                // node already exists.
//                success("already exists");
//
//            }
//
//            // in case we were disconnected.
//            disconnected = false;
//
//            if (INFO)
//                log.info("did reset watch");
//            
//        } catch (Throwable t) {
//
//            log.warn("Could not reset the watch: " + this, t);
//
//        }
//
//    }
//
//    /**
//     * Caller must be synchronized on <i>this</i>.
//     */
//    protected void success(final String msg) {
//
//        if(INFO)
//            log.info(msg + " : " + this);
//
//        // node was created.
//        created = true;
//
//        this.notify();
//        
//        // clear watch or we will keep getting notices.
//        clearWatch();
//
//    }
//    
//    /**
//     * Wait up to timeout units for the watched znode to be created.
//     * <p>
//     * Note: the resolution is millseconds at most.
//     * 
//     * @param timeout
//     *            The timeout.
//     * @param unit
//     *            The units.
//     * 
//     * @return <code>false</code> if the waiting time detectably elapsed
//     *         before return from the method, else <code>true</code>.
//     * 
//     * @throws TimeoutException
//     * @throws InterruptedException
//     */
//    protected boolean awaitCreate(final long timeout, final TimeUnit unit)
//            throws InterruptedException {
//
//        synchronized (this) {
//
//            final long begin = System.currentTimeMillis();
//
//            long millis = unit.toMillis(timeout);
//
//            while (millis > 0 && !created) {
//
//                this.wait(millis);
//
//                millis -= (System.currentTimeMillis() - begin);
//
//                if (INFO)
//                    log.info("woke up: created=" + created + ", remaining="
//                            + millis + "ms");
//                
//            }
//
//            return millis > 0;
//            
//        }
//        
//    }

//    /**
//     * If the znode identified by the path does not exist, then wait up to the
//     * timeout for the znode to be created.
//     * 
//     * @param zookeeper
//     * @param zpath
//     * @param timeout
//     * @param unit
//     * @return <code>false</code> if the waiting time detectably elapsed
//     *         before return from the method, else <code>true</code>.
//     * 
//     * @throws KeeperException
//     * @throws InterruptedException
//     * 
//     * @todo does not handle the case where the client is disconnected when the
//     *       request is made. it throws an exception, but it should wait for a
//     *       reconnect.
//     */
//    static public boolean awaitCreate(ZooKeeper zookeeper, String zpath,
//            final long timeout, final TimeUnit unit) throws KeeperException,
//            InterruptedException {
//
//        if (zookeeper == null)
//            throw new IllegalArgumentException();
//
//        if (zpath == null)
//            throw new IllegalArgumentException();
//        
//        final ZNodeCreatedWatcher watcher = new ZNodeCreatedWatcher(zookeeper,
//                zpath);
//
//        if (zookeeper.exists(zpath, watcher) != null) {
//
//            if (INFO)
//                log.info("znode exists: zpath=" + zpath);
//
//            watcher.clearWatch();
//
//            return true;
//            
//        } else {
//            
//            if (INFO)
//                log.info("Waiting on znode create: zpath=" + zpath);
//
//            final boolean ret = watcher.awaitCreate(timeout, unit);
//
//            if (INFO)
//                log.info("timeout=" + !ret + ", zpath=" + zpath);
//            
//            return ret;
//
//        }
//        
//    }

    /**
     * If the znode identified by the path does not exist, then wait up to the
     * timeout for the znode to be created.
     * 
     * @param zookeeper
     * @param zpath
     * @param timeout
     * @param unit
     * @return <code>false</code> if the waiting time detectably elapsed
     *         before return from the method, else <code>true</code>.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    static public boolean awaitCreate(final ZooKeeper zookeeper,
            final String zpath, final long timeout, final TimeUnit unit)
            throws InterruptedException {

        return new ZNodeCreatedWatcher(zookeeper, zpath).awaitCondition(
                timeout, unit);
      
    };

    protected ZNodeCreatedWatcher(final ZooKeeper zookeeper,
            final String zpath) {
        
        super(zookeeper,zpath);
        
    }
    
    protected boolean isConditionSatisified(WatchedEvent event)
            throws KeeperException, InterruptedException {

        return event.getType().equals(
                Watcher.Event.EventType.NodeCreated);

    }

    protected boolean isConditionSatisified() throws KeeperException,
            InterruptedException {
        
        return zookeeper.exists(zpath, this) != null;

    }

    protected void clearWatch() throws KeeperException, InterruptedException {

        // clears the watch.
        zookeeper.exists(zpath, false);

    }

}
