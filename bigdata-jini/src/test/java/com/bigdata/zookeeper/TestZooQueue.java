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

import org.apache.zookeeper.KeeperException;

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
     * @throws TimeoutException 
     * @throws ExecutionException 
     */
    public void test_queue() throws KeeperException, InterruptedException, ExecutionException, TimeoutException {
        
        final String zroot = "/test/" + getName() + UUID.randomUUID();
        
        final ZooQueue<String> queue = new ZooQueue<String>(zookeeper, zroot,
                acl, Integer.MAX_VALUE/*capacity*/);

//        final ReentrantLock lock = new ReentrantLock();
//
//        final Condition consumerDone = lock.newCondition();

        assertEquals(0, queue.size());

        queue.add("1");

        assertEquals(1, queue.size());

        queue.add("2");

        assertEquals(2, queue.size());

        final FutureTask<Void> ft1 = new FutureTask<Void>(new Callable<Void>() {
            
            public Void call() throws Exception {
            
                try {
                    assertEquals(2, queue.size());

                    assertEquals("1", queue.remove());

                    assertEquals(1, queue.size());

                    assertEquals("2", queue.remove());

                    assertEquals(0, queue.size());

//                    lock.lock();
//                    try {
//                        consumerDone.signal();
//                    } finally {
//                        lock.unlock();
//                    }

                    return null;
                } catch (Throwable t) {
                    log.error(t, t);
                    throw new RuntimeException(t);
                }
            }
        });

        // Run task
        service.execute(ft1);

        // Wait for future/error.
        ft1.get(1000, TimeUnit.MILLISECONDS);
        
//        // The condition should have been signaled.
//        lock.lock();
//        try {
//            consumerDone.await(500, TimeUnit.MILLISECONDS);
//        } finally {
//            lock.unlock();
//        }

        assertEquals(0, queue.size());

    }

    public void test_queueBlocks() throws KeeperException, InterruptedException, ExecutionException, TimeoutException {
        
        final String zroot = "/test/" + getName() + UUID.randomUUID();
        
        final int capacity = 2;
        
        final ZooQueue<String> queue = new ZooQueue<String>(zookeeper, zroot,
                acl, capacity);

//        final ReentrantLock unusedLock = new ReentrantLock();
  
        // task will fill the queue to its capacity and then block.
        final FutureTask<Void> ftProducer = new FutureTask<Void>(new Callable<Void>() {
  
            public Void call() throws Exception {

                assertEquals(2, queue.capacity());

                assertEquals(0, queue.size());
                
                queue.add("A");
                
                assertEquals(1, queue.size());
                
                queue.add("B");
                
                assertEquals(2, queue.size());

                log.info("Should block.");

                queue.add("C"); // should block.
                
                log.info("Producer done.");
                
                return null;
                
            }
        });
        
        // start task.
        service.execute(ftProducer);
        
        int size;
        while ((size = queue.size()) < capacity) {

            if(log.isInfoEnabled())
                log.info("size=" + size);
            
            Thread.sleep(10/* ms */);
            
            if(ftProducer.isDone()) {
                // The task SHOULD NOT be done.
                ftProducer.get();
                throw new AssertionError();
            }
            
        }

        if(log.isInfoEnabled())
            log.info("Queue is at capacity: size=" + queue.size());

        /* Make sure that the producer is blocked.  If it is not blocked
         * then the producer will add another element and the queue will
         * be over capacity.
         */
        Thread.sleep(500/* ms */);

        // queue is still at capacity.
        assertEquals(capacity, queue.size());
        
        if(ftProducer.isDone()) {
            // The task SHOULD NOT be done.
            ftProducer.get();
            throw new AssertionError();
        }

        // take an item from the queue.
        assertEquals("A",queue.remove());
        
        // producer should now complete.
        ftProducer.get(1000, TimeUnit.MILLISECONDS);

        // queue is back at capacity.
        assertEquals(2, queue.size());

        /*
         * Now verify that we can detect when the queue becomes empty.
         */

        final FutureTask<Void> ftConsumer = new FutureTask<Void>(
                new Callable<Void>() {

                    public Void call() throws Exception {

                        try {

                            assertEquals(2, queue.capacity());

                            assertEquals(2, queue.size());

                            assertEquals("B", queue.remove());

                            assertEquals(1, queue.size());

                            // Wait to give awaitEmpty() a chance in the main
                            // thread.
                            Thread.sleep(500/* ms */);

                            assertEquals("C", queue.remove());

                            assertEquals(0, queue.size());

                            log.info("Consumer done.");

                            return null;

                        } catch (Throwable t) {

                            log.error(t, t);

                            throw new RuntimeException(t);

                        }
                    }
                });

        service.execute(ftConsumer);
        
        log.info("Will wait for queue to become empty");
        
        // not empty yet.
        assertNotSame(0, queue.size());

        queue.awaitEmpty();

        assertEquals(0, queue.size());
        
        // check the future.
        ftConsumer.get(1000,TimeUnit.MILLISECONDS);

    }

}
