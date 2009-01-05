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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * {@link ZooQueue} test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZooQueue extends AbstractZooTestCase {

    /**
     * 
     */
    public TestZooQueue() {
        
    }

    /**
     * @param name
     */
    public TestZooQueue(String name) {
        
        super(name);
        
    }

    /**
     * Simple test populates a queue and then waits until it has been drained.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void test_queue() throws KeeperException, InterruptedException {
        
        final String zroot = "/test/" + getName() + UUID.randomUUID();
        
        final ZooQueue<String> queue = new ZooQueue<String>(zookeeper, zroot,
                Ids.OPEN_ACL_UNSAFE);

        final ReentrantLock lock = new ReentrantLock();

        final Condition consumerDone = lock.newCondition();

        assertEquals(0, queue.size());

        queue.add("1");

        assertEquals(1, queue.size());

        queue.add("2");

        assertEquals(2, queue.size());

        new ClientThread(Thread.currentThread(), lock) {

            public void run2() throws Exception {

                assertEquals(2, queue.size());

                assertEquals("1", queue.remove());

                assertEquals(1, queue.size());

                assertEquals("2", queue.remove());

                assertEquals(0, queue.size());

                lock.lock();
                try {
                    consumerDone.signal();
                } finally {
                    lock.unlock();
                }

            }

        }.start();

        lock.lock();
        try {
            consumerDone.await(500, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }

        assertEquals(0, queue.size());

    }

}
