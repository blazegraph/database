/*

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    
    public void test_barrier() throws KeeperException, InterruptedException, ExecutionException, TimeoutException {

        final String zroot = "/test/" + getName() + UUID.randomUUID();
        
        final ZooBarrier barrier = new ZooBarrier(zookeeper, zroot,
                Ids.OPEN_ACL_UNSAFE, 2/* size */);

        final ReentrantLock lock = new ReentrantLock();

        final AtomicInteger counter = new AtomicInteger(0);
        
        final Condition broken = lock.newCondition();
        
        final FutureTask<Void> ft1 = new FutureTask<Void>(
                new Callable<Void>() {

                    public Void call() throws Exception {

                        try {

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
                            
                            return null;

                        } catch (Throwable t) {

                            log.error(t, t);

                            throw new RuntimeException(t);

                        }
                    }
                });

        final FutureTask<Void> ft2 = new FutureTask<Void>(
                new Callable<Void>() {

                    public Void call() throws Exception {

                        try {

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
                            
                            return null;

                        } catch (Throwable t) {

                            log.error(t, t);

                            throw new RuntimeException(t);

                        }
                    }
                });

        /*
         * Run both tasks. They should block until both are present at the
         * barrier.
         */ 
        service.execute(ft1);
        service.execute(ft2);

        // Verify that the barrier is broken.
        lock.lock();
        try {
            broken.await(500, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }

        assertEquals(2, counter.get());

        // Check their futures.
        ft1.get(sessionTimeout,TimeUnit.MILLISECONDS);
        ft2.get(sessionTimeout,TimeUnit.MILLISECONDS);
        
    }

}
