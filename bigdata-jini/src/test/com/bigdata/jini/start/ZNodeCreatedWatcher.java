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

import javax.swing.event.DocumentEvent.EventType;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.bigdata.jini.start.AbstractJiniServiceConfiguration.JiniServiceStarter;

/**
 * Watcher notifies a Thread synchronized on itself when the znode that it is
 * watching is created. The caller should set an instance of this watcher on a
 * <strong>single</strong> znode if it wishes to be notified when that znode is
 * created and then {@link Object#wait()} on the watcher. The {@link Thread}
 * MUST test {@link #deleted} while holding the lock and before waiting (in case
 * the event has already occurred), and again each time {@link Object#wait()}
 * returns (since wait and friends MAY return spuriously). The watcher will
 * re-establish the watch if the {@link EventType} was not
 * {@link EventType#REMOVE}, but it WILL NOT continue to watch the znode once
 * it has been deleted.
 * 
 * FIXME update javadoc here and on {@link ZNodeCreatedWatcher} and then modify
 * the {@link JiniServiceStarter} to also watch for the ephemeral znode for the
 * service to be created before concluding that the service has started
 * successfully. [we could go further and query for a normal service status, but
 * the minimum criteria must be discovered by jini and znode in zookeeper.] Then
 * try the {@link TestServiceStarter} again and see if it will complete normally
 * rather than failing (it complains that the physical znode does not exist).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZNodeCreatedWatcher implements Watcher {

    protected final ZooKeeper zookeeper;
    
    public ZNodeCreatedWatcher(final ZooKeeper zookeeper) {

        if (zookeeper == null)
            throw new IllegalArgumentException();

        this.zookeeper = zookeeper;
        
    }
    
    volatile boolean deleted = false;
    
    public void process(final WatchedEvent event) {

        synchronized (this) {

            if (event.getType().equals(EventType.REMOVE)) {

                deleted = true;

                this.notify();

            } else {
                
                try {
                    
                    // reset the watch.
                    zookeeper.exists(event.getPath(), this);
                    
                } catch (Exception e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
            }

        }

    }

    /**
     * Wait up to timeout units for the watched znode to be deleted.
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
    public void awaitRemove(final long timeout, final TimeUnit unit)
            throws TimeoutException, InterruptedException {

        synchronized (this) {

            final long begin = System.currentTimeMillis();

            long millis = unit.toMillis(timeout);

            while (millis > 0 && !deleted) {

                this.wait(millis);

                millis -= (System.currentTimeMillis() - begin);

            }

            throw new TimeoutException();
            
        }
        
    }
    
}
