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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;


/**
 * {@link ZooBarrier} test suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestZooBarrier extends AbstractZooTestCase {

    /**
     * 
     */
    public TestZooBarrier() {
    }

    /**
     * @param arg0
     */
    public TestZooBarrier(String arg0) {
        super(arg0);
    }
    
    public void test_barrier() throws KeeperException, InterruptedException {

        final String zroot = "/test/" + getName() + UUID.randomUUID();
        
        final ZooBarrier barrier = new ZooBarrier(zookeeper, zroot,
                Ids.OPEN_ACL_UNSAFE, 2/* size */);

        final ReentrantLock lock = new ReentrantLock();

        final AtomicInteger counter = new AtomicInteger(0);
        
        final Condition broken = lock.newCondition();
        
        new ClientThread(Thread.currentThread(),lock) {
          
            public void run2() throws Exception {
                
                final String id = "1";
                
                counter.incrementAndGet();
                
                barrier.enter(id);
                
                assertEquals(2,counter.get());
                
                barrier.leave(id);
                
                lock.lock();
                try {
                    broken.signal();
                } finally {
                    lock.unlock();
                }
                
            }
            
        }.start();
        
        new ClientThread(Thread.currentThread(), lock) {
          
            public void run2() throws Exception {
                
                final String id = "2";
                
                counter.incrementAndGet();
                
                barrier.enter(id);
                
                assertEquals(2,counter.get());
                
                barrier.leave(id);
                
                lock.lock();
                try {
                    broken.signal();
                } finally {
                    lock.unlock();
                }
                
            }
            
        }.start();

        lock.lock();
        try {
            broken.await(500, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }

        assertEquals(2, counter.get());

    }

}
