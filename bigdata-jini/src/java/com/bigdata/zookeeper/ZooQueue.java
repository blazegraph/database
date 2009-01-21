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
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * Producer-consumer queue.
 * 
 * <h3>Queue pattern</h3>
 * 
 * This is a summary of the queue pattern. This pattern is encapsulated by the
 * API, but it is here for your reference anyway.
 * <p>
 * The children in the queue are created using the SEQUENTIAL flag. The data for
 * each child is the value specified to {@link #add(Serializable)}.
 * <p>
 * (A) The consumer read the children, setting a watch. If there are no more
 * children in the queue, then the consumer will be notified when the watch is
 * triggered. When the watch is triggered, the children are again read from the
 * queue.
 * <p>
 * (B) If there are children, the consumer sorts them into lexical order, and
 * proceeds in the sorted order (FIFO queue). As children are processed
 * successfully, they are deleted from the queue. Goto (A).
 * <p>
 * If the producers wishes to block when the queue is full it must read the
 * capacity of the queue (stored as an {@link Integer} in the znode of the queue
 * itself). It reads the children, setting a watch as a side effect. If the #of
 * children exceeds the capacity of the queue, then the producer blocks until
 * the watch is triggered and, on inspection, there are fewer children in the
 * queue than its capacity.
 * <p>
 * If there is a single producer, then it can destroy queue whenever it likes
 * simply by deleting any children remaining in the queue and then deleting the
 * queue. If there are multiple producers, then a sibling of the queue znode
 * must be create which marks the queue as invalid and producers must look for
 * that mark (just like {@link ZLock}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZooQueue<E extends Serializable> extends AbstractZooQueue<E> {

    /**
     * @param zookeeper
     * @param zroot
     * @param acl
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public ZooQueue(ZooKeeper zookeeper, String zroot, List<ACL> acl,
            int capacity) throws KeeperException, InterruptedException {

        super(zookeeper, zroot, acl, capacity);

    }

    @Override
    protected CreateMode getCreateMode() {
        
        return CreateMode.PERSISTENT_SEQUENTIAL;
        
    }

    private final static transient String PREFIX = "element";

    protected String getChildPrefix() {
        
        return PREFIX;
        
    }
    
}
