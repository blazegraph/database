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

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
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

    /**
     * @todo should there be a more restricted ACL for the children?
     */
    private final List<ACL> childACL = Ids.OPEN_ACL_UNSAFE;
    
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
     * @param rootACL
     *            The ACL for the zroot (used when it is created).
     * 
     * @throws InterruptedException
     */
    public AbstractZooQueue(final ZooKeeper zookeeper, final String zroot,
            final List<ACL> rootACL) throws KeeperException,
            InterruptedException {

        super(zookeeper);

        this.zroot = zroot;

        try {

            zookeeper.create(zroot, new byte[0], rootACL, CreateMode.PERSISTENT);

            if (INFO)
                log.info("New queue: " + zroot);

        } catch (KeeperException.NodeExistsException ex) {

            if (INFO)
                log.info("Existing queue: " + zroot);
            
        }
        
    }

    /**
     * Adds an element to the tail of the queue.
     */
    public void add(final E e) throws KeeperException, InterruptedException {

        if (e == null)
            throw new IllegalArgumentException();

        final String znode = zookeeper.create(zroot + "/" + getChildPrefix(),
                SerializerUtil.serialize(e), childACL, getCreateMode());

        if (INFO)
            log.info("zroot=" + zroot + ", e=" + e + ", znode=" + znode);

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

            if(true) // FIXME Must sort the elements!
                throw new UnsupportedOperationException();
            
            // first element (lex order is also sorted for sequential ids).
            final String selected = zroot + "/" + list.get(0);
            
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
