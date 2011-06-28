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
    
    protected boolean isConditionSatisfied(final WatchedEvent event)
            throws KeeperException, InterruptedException {

        return event.getType().equals(
                Watcher.Event.EventType.NodeCreated);

    }

    protected boolean isConditionSatisfied() throws KeeperException,
            InterruptedException {
        
        return zookeeper.exists(zpath, this) != null;

    }

    protected void clearWatch() throws KeeperException, InterruptedException {

        // clears the watch.
        zookeeper.exists(zpath, false);

    }

}
