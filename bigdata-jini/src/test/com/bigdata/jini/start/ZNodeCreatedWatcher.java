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

package com.bigdata.jini.start;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
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
public class ZNodeCreatedWatcher implements Watcher {

    final static protected Logger log = Logger.getLogger(ZNodeCreatedWatcher.class);

    final static protected boolean INFO = log.isInfoEnabled();

    final static protected boolean DEBUG = log.isDebugEnabled();

    protected final ZooKeeper zookeeper;
    
    public ZNodeCreatedWatcher(final ZooKeeper zookeeper) {

        if (zookeeper == null)
            throw new IllegalArgumentException();

        this.zookeeper = zookeeper;
        
    }
    
    volatile boolean created = false;
   
    /**
     * Clear the watch. This is necessary for the {@link Watcher} to stop
     * getting notices of changes after it has noticed the change that it was
     * looking for.
     */
    private void clearWatch(final String zpath) {
        try {
            zookeeper.exists(zpath, false);
        } catch (KeeperException ex) {
            // ignore
            log.warn(ex);
        } catch (InterruptedException ex) {
            // ignore.
            log.warn(ex);
        }

    }

    /**
     * The watcher notifies a {@link Thread} synchronized on itself when the
     * znode that it is watching is created. The caller sets an instance of this
     * watcher on a <strong>single</strong> znode if it wishes to be notified
     * when that znode is created and then {@link Object#wait()} on the watcher.
     * The {@link Thread} MUST test {@link #created} while holding the lock and
     * before waiting (in case the event has already occurred), and again each
     * time {@link Object#wait()} returns (since wait and friends MAY return
     * spuriously). The watcher will re-establish the watch if the
     * {@link Watcher.Event.EventType} was not
     * {@link Watcher.Event.EventType#NodeCreated}, but it WILL NOT continue to
     * watch the znode once it has been created.
     */
    public void process(final WatchedEvent event) {

        if(INFO)
            log.info(event.toString());
        
        synchronized (this) {
            
            if (event.getType().equals(Watcher.Event.EventType.NodeCreated)) {

                // node was created.
                created = true;

                this.notify();
                
                // clear watch or we will keep getting notices.
                clearWatch(event.getPath());

            } else {
                
                try {
        
                    if(INFO)
                        log.info("will reset watch");
                    
                    // reset the watch.
                    if (zookeeper.exists(event.getPath(), this) != null) {

                        // node already exists.
                        
                        if(INFO)
                            log.info("node already exists");

                        created = true;
                        
                        this.notify();

                        // clear watch or we will keep getting notices.
                        clearWatch(event.getPath());

                    }
                    
                    if(INFO)
                        log.info("did reset watch");
                                        
                } catch (Exception e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }

        }

    }

    /**
     * Wait up to timeout units for the watched znode to be created.
     * <p>
     * Note: the resolution is millseconds at most.
     * 
     * @param timeout
     *            The timeout.
     * @param unit
     *            The units.
     * 
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void awaitInsert(final long timeout, final TimeUnit unit)
            throws TimeoutException, InterruptedException {

        synchronized (this) {

            final long begin = System.currentTimeMillis();

            long millis = unit.toMillis(timeout);

            while (millis > 0 && !created) {

                this.wait(millis);

                millis -= (System.currentTimeMillis() - begin);

                if (INFO)
                    log.info("woke up: created=" + created + ", remaining="
                            + millis + "ms");
                
            }

            if (!created)
                throw new TimeoutException();
            
        }
        
    }
    
}
