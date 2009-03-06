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
 * Created on Jan 12, 2009
 */

package com.bigdata.zookeeper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event;

/**
 * Test suite for {@link HierarchicalZNodeWatcher}.
 * <p>
 * Note: Zookeeper has other events that could appear during these unit tests,
 * such as the connection status change events. However the unit test are not
 * expecting such events during testing. If they appear, those events could
 * cause test failures when we examine the queue. Basically, the tests are not
 * robust if your zookeeper client is flakey.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHierarchicalZNodeWatcher extends AbstractZooTestCase implements
        HierarchicalZNodeWatcherFlags {

    /**
     * 
     */
    public TestHierarchicalZNodeWatcher() {
        
    }

    /**
     * @param name
     */
    public TestHierarchicalZNodeWatcher(String name) {

        super(name);

    }

    protected String zroot;
    
    /**
     * Sets up a unique {@link #zroot}.
     */
    public void setUp() throws Exception {
        
        super.setUp();
        
        zroot = zookeeper.create("/test", new byte[0], acl,
                CreateMode.PERSISTENT_SEQUENTIAL);
        
    }

    /**
     * Destroys the {@link #zroot} and its children.
     */
    public void tearDown() throws Exception {

        if (zroot != null) {

            destroyZNodes(zookeeperAccessor.getZookeeper(), zroot);
            
        }
        
        super.tearDown();

    }
    
    /**
     * Test when the node at the root of the hierarchy does not exist when we
     * setup the watcher, then create the znode and verify that we see the event
     * in the queue.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void test_noticeCreate() throws KeeperException,
            InterruptedException {

        WatchedEvent e;
        
        final String zroot = this.zroot + "/" + "a";
        
        final HierarchicalZNodeWatcher watcher = new HierarchicalZNodeWatcher(
                zookeeper, zroot, EXISTS) {

            @Override
            protected int watch(String path, String child) {

                return NONE;

            }

        };

        assertTrue(watcher.queue.isEmpty());
        
        assertTrue(watcher.isWatched(zroot));

        /*
         * Create the zroot and verify the event is placed into the queue.
         */
        
        zookeeper.create(zroot, new byte[0], acl, CreateMode.PERSISTENT);

        // look for the create event.
        e= watcher.queue.poll(1000, TimeUnit.MILLISECONDS);

        assertNotNull(e);

        assertEquals(zroot,e.getPath());
        
        assertEquals(Event.EventType.NodeCreated,e.getType());

        assertTrue(watcher.queue.isEmpty());
        
        /*
         * Delete the znode and verify the event is placed into the queue.
         */
 
        zookeeper.delete(zroot, -1/*version*/);
        
        // look for the delete event.
        e = watcher.queue.poll(1000, TimeUnit.MILLISECONDS);

        assertNotNull(e);

        assertEquals(zroot,e.getPath());
        
        assertEquals(Event.EventType.NodeDeleted, e.getType());

        assertTrue(watcher.queue.isEmpty());

        /*
         * Re-create the zroot and verify the event is placed into the queue
         * (this makes sure that we are keeping the watch in place).
         */

        zookeeper.create(zroot, new byte[0], acl, CreateMode.PERSISTENT);

        // look for the create event.
        e = watcher.queue.poll(1000, TimeUnit.MILLISECONDS);

        assertNotNull(e);

        assertEquals(zroot, e.getPath());

        assertEquals(Event.EventType.NodeCreated, e.getType());

        assertTrue(watcher.queue.isEmpty());

        /*
         * cancel the watcher and verify that it does not notice a delete of the
         * zroot after it was cancelled.
         */
        watcher.cancel();

        /*
         * Delete the znode - no event should appear.
         */

        zookeeper.delete(zroot, -1/*version*/);

        // look for the delete event.
        e = watcher.queue.poll(1000, TimeUnit.MILLISECONDS);

        assertNull(e);

    }

    /**
     * Unit test verifies that we notice specific children as they are created
     * and destroyed. "red" znodes are ignored. if the znode is "blue" then we
     * extend the watch over its children as well.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     * 
     * @todo test queue when data is changed.
     */
    public void test_noticeChildren() throws InterruptedException,
            KeeperException {

        WatchedEvent e;

        HierarchicalZNodeWatcher watcher = new HierarchicalZNodeWatcher(
                zookeeper, zroot, EXISTS | CHILDREN) {

            @Override
            protected int watch(String path, String child) {

                if (child.equals("red"))
                    return NONE;

                if (child.equals("blue"))
                    return EXISTS | CHILDREN;

                if (child.equals("green"))
                    return DATA;

                throw new AssertionFailedError("Not expecting: path=" + path
                        + ", child=" + child);

            }

        };

        zookeeper.create(zroot + "/" + "red", new byte[0], acl,
                CreateMode.PERSISTENT);

        e = watcher.queue.poll(1000,TimeUnit.MILLISECONDS);
        assertNotNull(e);
        assertEquals(zroot,e.getPath());
        assertEquals(Event.EventType.NodeChildrenChanged,e.getType());
        
        zookeeper.create(zroot + "/" + "blue", new byte[0], acl,
                CreateMode.PERSISTENT);

//        e = watcher.queue.poll(1000,TimeUnit.MILLISECONDS);
//        assertNotNull(e);
//        assertEquals(zroot+"/"+"red",e.getPath());
//        assertEquals(Event.EventType.NodeCreated,e.getType());

        zookeeper.create(zroot + "/" + "blue" + "/" + "green", new byte[0],
                acl, CreateMode.PERSISTENT);

        assertEquals(NONE, watcher.getFlags(zroot + "/" + "red"));

        assertEquals(EXISTS | CHILDREN, watcher.getFlags(zroot + "/" + "blue"));

        assertEquals(DATA, watcher.getFlags(zroot + "/" + "blue" + "/"
                + "green"));

        // clear any events in the queue.
        watcher.queue.clear();
        
        // update the data.
        zookeeper.setData(zroot + "/" + "blue" + "/" + "green",
                new byte[] { 1 }, -1/* version */);
        
        // verify event.
        e = watcher.queue.poll(1000,TimeUnit.MILLISECONDS);
        assertNotNull(e);
        assertEquals(zroot + "/" + "blue" + "/" + "green",e.getPath());
        assertEquals(Event.EventType.NodeDataChanged,e.getType());

        // won't been seen since a "red" path.
        zookeeper.create(zroot + "/" + "red" + "/" + "blue", new byte[0], acl,
                CreateMode.PERSISTENT);

        assertEquals(NONE, watcher.getFlags(zroot + "/" + "red" + "/" + "blue"));

        /*
         * There should be three watched znodes: zroot; zroot/blue; and
         * zroot/blue/green
         */
        assertEquals(3,watcher.getWatchedSize());
        
        watcher.cancel();

        assertEquals(0,watcher.getWatchedSize());
        assertFalse(watcher.isWatched(zroot));
        assertFalse(watcher.isWatched(zroot+"/"+"blue"));
        assertFalse(watcher.isWatched(zroot+"/"+"blue"+"/"+"green"));
        
        assertTrue(watcher.queue.isEmpty());
        
        /*
         * Setup a new watcher that wathes all paths but the red ones. The
         * znodes already exist. Now verify that we receive various notices when
         * the watcher is created.
         */
        watcher = new HierarchicalZNodeWatcher(zookeeper, zroot, ALL, true/* pumpMockEventsDuringStartup */) {
            
            @Override
            protected int watch(String path, String child) {
                
                return ALL;
                
            }

//            @Override
//            protected void addedWatch(String path, int flags) {
//            
//                placeMockEventInQueue(path, flags);
//                
//            }
            
        };

        /*
         * We created 4 znodes plus the pre-existing zroot, so there should be
         * five nodes picked up by the new watcher.
         */
        final String[] nodes = new String[] {
                zroot,
                zroot + "/" + "red",
                zroot + "/" + "red" + "/" + "blue",
                zroot + "/" + "blue",
                zroot + "/" + "green" + "/" + "green",
        };
        
        // put into a set.
        final HashSet<String> set = new HashSet<String>(Arrays.asList(nodes));
        
        // verify new watched size.
        assertEquals(nodes.length, watcher.getWatchedSize());
        
        /*
         * Verify mock events were pumped into the queue. Since we specified
         * ALL, there should be three events for each znode.
         */
        assertEquals(3 * 5, watcher.queue.size());

        while ((e = watcher.queue.poll()) != null) {
            
            System.err.println("mockEvent: "+e);
            
        }
        
//        for (int i = 0; i < nodes.length; i++) {
//
//            log.info("mockEvent: "+e);
//            
//            e = watcher.queue.take();
//
//            final String path = e.getPath();
//
//            assertTrue(set.contains(path));
//            
//            e = watcher.queue.take();
//            
//            assertEquals(path, e.getPath());
//            
//            e = watcher.queue.take();
//            
//            assertEquals(path, e.getPath());
//            
//            set.remove(path);
//            
//        }
        
        watcher.cancel();
        
    }

}
