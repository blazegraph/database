/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jan 31, 2012
 */

package com.bigdata.cache;

import java.util.Stack;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.cache.SynchronizedHardReferenceQueueWithTimeout.IRef;

/**
 * Test suite for {@link SynchronizedHardReferenceQueueWithTimeout}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Write a unit test for the timeout mechanism. Note that it is
 *          invoked with a minimum granularity of 5 seconds by the JVM wide
 *          cleaner thread.
 */
public class TestSynchronizedHardReferenceQueueWithTimeout extends TestCase2 {

    /**
     * 
     */
    public TestSynchronizedHardReferenceQueueWithTimeout() {
    }

    /**
     * @param name
     */
    public TestSynchronizedHardReferenceQueueWithTimeout(String name) {
        super(name);
    }

    /**
     * Test constructor and its post-conditions.
     */
    public void test_ctor() {

        final SynchronizedHardReferenceQueueWithTimeout<String> cache = new SynchronizedHardReferenceQueueWithTimeout<String>(
                100/* capacity */, 20/* nscan */, 10000L/* timeout */);

//        assertEquals("listener", listener, cache.getListener());
        assertEquals("timeout", 10000L, cache.timeout());
        assertEquals("capacity", 100, cache.capacity());
        assertEquals("size", 0, cache.size());
        assertEquals("nscan", 20, cache.nscan());
        assertEquals("isEmpty", true, cache.isEmpty());
        assertEquals("isFull", false, cache.isFull());

    }
    
    /**
     * Correct rejection tests for the constructor.
     */
    public void test_ctor_correct_rejection() {
        
//        try {
//            new HardReferenceQueue<String>(null, 100);
//            fail("Expecting: " + IllegalArgumentException.class);
//        } catch (IllegalArgumentException ex) {
//            System.err.println("Ignoring expectedRefs exception: " + ex);
//        }

        // nscan MAY be ZERO (0)
        new SynchronizedHardReferenceQueueWithTimeout<String>(
                10/* capacity */, 0/* nscan */, 10000L/* timeout */);

        // capacity MAY NOT be ZERO (0)
        try {
            new SynchronizedHardReferenceQueueWithTimeout<String>(
                    0/* capacity */, 0/* nscan */, 10000L/* timeout */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // nscan MAY be EQ capacity
        new SynchronizedHardReferenceQueueWithTimeout<String>(
                10/* capacity */, 10/* nscan */, 10000L/* timeout */);

        // nscan MAY NOT be GT capacity
        try {
            new SynchronizedHardReferenceQueueWithTimeout<String>(
                    10/* capacity */, 11/* nscan */, 10000L/* timeout */);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // timeout MAY be ZERO (0) (implies NO timeout).
        new SynchronizedHardReferenceQueueWithTimeout<String>(
                10/* capacity */, 10/* nscan */, 0L/* timeout */);

    }

    /**
     * Correct rejection test for appending a null reference to the cache.
     */
    public void test_append_null() {

        final SynchronizedHardReferenceQueueWithTimeout<String> cache = new SynchronizedHardReferenceQueueWithTimeout<String>(
                100/* capacity */, 2/* nscan */, 0L/* timeout */);

        try {
            cache.add(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
        
    }
    
    /**
     * Verify that the indirected references on the innerQueue are the same as
     * the given references.
     * 
     * @param expected
     *            The expected references in the expected order.
     * @param innerQueue
     *            The innerQueue whose order is to be verified.
     */
    private void assertSameOrder(final String[] expected,
            final HardReferenceQueue<IRef<String>> innerQueue) {

        final IRef<String>[] actual = innerQueue.toArray(new IRef[0]);
        
        final String[] actual2 = new String[actual.length];
        
        for (int i = 0; i < actual.length; i++) {
 
            // Indirection.
            actual2[i] = actual[i] == null ? null : actual[i].get();

        }

        assertEquals("order", expected, actual2);

    }

    /**
     * <p>
     * Test verifies that we can add distinct references until the cache is full
     * and that a subsequent add causes an eviction notice. While the cache is
     * full, we then explicitly evict the LRU reference and verify that the
     * cache state correctly reflects the eviction. Finally, we test
     * evictAll(false) (does not clear the references from the cache) and
     * evictAll(true) (clears the references from the cache).
     * </p>
     * <p>
     * Note that scanning of the last N references added is disabled for this
     * test.
     * </p>
     */
    public void test_add_evict() {

        final int capacity = 5;
        final int nscan = 0;
        final long timeoutNanos = 0L;

        final MyListener<String, IRef<String>> listener = new MyListener<String, IRef<String>>();

        final SynchronizedHardReferenceQueueWithTimeout<String> cache = new SynchronizedHardReferenceQueueWithTimeout<String>(
                listener, capacity, nscan, timeoutNanos);

        final HardReferenceQueue<IRef<String>> innerQueue = cache.getQueue();

        final String ref0 = "0";
        final String ref1 = "1";
        final String ref2 = "2";
        final String ref3 = "3";
        final String ref4 = "4";
        final String ref5 = "5";
        
        assertEquals("size",0,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",0,innerQueue.getHeadIndex());
        assertTrue("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{}, innerQueue);
//        assertEquals("order",new String[]{},innerQueue.toArray(new String[0]));
        
        assertTrue(cache.add(ref0));
        assertEquals("size",1,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",1,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref0}, innerQueue);
//        assertEquals("order",new String[]{ref0},innerQueue.toArray(new String[0]));
        
        assertTrue(cache.add(ref1));
        assertEquals("size",2,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",2,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref0,ref1}, innerQueue);
//        assertEquals("order",new String[]{ref0,ref1},innerQueue.toArray(new String[0]));
        
        assertTrue(cache.add(ref2));
        assertEquals("size",3,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",3,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref0,ref1,ref2}, innerQueue);
//        assertEquals("order",new String[]{ref0,ref1,ref2},innerQueue.toArray(new String[0]));
        
        assertTrue(cache.add(ref3));
        assertEquals("size",4,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",4,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref0,ref1,ref2,ref3}, innerQueue);
//        assertEquals("order",new String[]{ref0,ref1,ref2,ref3},innerQueue.toArray(new String[0]));
        
        assertTrue(cache.add(ref4));
        assertEquals("size",5,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",0,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertSameOrder(new String[]{ref0,ref1,ref2,ref3,ref4}, innerQueue);
//        assertEquals("order",new String[]{ref0,ref1,ref2,ref3,ref4},innerQueue.toArray(new String[0]));

        listener.setExpectedRef(ref0);
        assertTrue(cache.add(ref5));
        listener.assertEvicted();
        assertEquals("size",5,cache.size());
        assertEquals("tail",1,innerQueue.getTailIndex());
        assertEquals("head",1,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertSameOrder(new String[]{ref1,ref2,ref3,ref4,ref5}, innerQueue);
//        assertEquals("order",new String[]{ref1,ref2,ref3,ref4,ref5},innerQueue.toArray(new String[0]));

        /*
         * Evict the LRU reference and verify that the cache size goes down by
         * one.
         */
        listener.setExpectedRef(ref1);
        assertTrue(cache.evict());
        listener.assertEvicted();
        assertEquals("size",4,cache.size());
        assertEquals("tail",2,innerQueue.getTailIndex());
        assertEquals("head",1,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref2,ref3,ref4,ref5}, innerQueue);
//        assertEquals("order",new String[]{ref2,ref3,ref4,ref5},innerQueue.toArray(new String[0]));

        /*
         * add a reference - no eviction since the cache was not at capacity. As
         * a post-condition, the cache is once again at capacity.
         */
        assertTrue(cache.add(ref4));
        assertEquals("size",5,cache.size());
        assertEquals("tail",2,innerQueue.getTailIndex());
        assertEquals("head",2,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertSameOrder(new String[]{ref2,ref3,ref4,ref5,ref4}, innerQueue);
//        assertEquals("order",new String[]{ref2,ref3,ref4,ref5,ref4},innerQueue.toArray(new String[0]));

        /*
         * Add another reference and verify that an eviction occurs.
         */
        listener.setExpectedRef(ref2);
        assertTrue(cache.add(ref2));
        listener.assertEvicted();
        assertEquals("size",5,cache.size());
        assertEquals("tail",3,innerQueue.getTailIndex());
        assertEquals("head",3,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertSameOrder(new String[]{ref3,ref4,ref5,ref4,ref2}, innerQueue);
//        assertEquals("order",new String[]{ref3,ref4,ref5,ref4,ref2},innerQueue.toArray(new String[0]));

        /*
         * Test evictAll(false) (does not change the cache state).
         */
        int nevicted = listener.getEvictionCount();
        listener.setExpectedRefs(new String[]{ref3,ref4,ref5,ref4,ref2});
        cache.evictAll(false);
        listener.assertEvictionCount(nevicted+5);
        assertEquals("size",5,cache.size());
        assertEquals("tail",3,innerQueue.getTailIndex());
        assertEquals("head",3,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertSameOrder(new String[]{ref3,ref4,ref5,ref4,ref2}, innerQueue);
//        assertEquals("order",new String[]{ref3,ref4,ref5,ref4,ref2},innerQueue.toArray(new String[0]));

        /*
         * Test evictAll(false) (actually evicts the references from the cache).
         */
        nevicted = listener.getEvictionCount();
        listener.setExpectedRefs(new String[]{ref3,ref4,ref5,ref4,ref2});
        cache.evictAll(true);
        listener.assertEvictionCount(nevicted+5);
        assertEquals("size",0,cache.size());
        assertEquals("tail",3,innerQueue.getTailIndex());
        assertEquals("head",3,innerQueue.getHeadIndex());
        assertTrue("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{}, innerQueue);
//        assertEquals("order",new String[]{},innerQueue.toArray(new String[0]));
        
    }
    
    /**
     * Test verifies scan of the last N references when adding a reference to
     * the cache. When the test starts the tail is at index 0, but eventually we
     * wrap the cache around and continue testing to make sure that scans
     * function correctly with a head index of 0 (this requires continuing the
     * scan from the array capacity).
     * <p>
     * Note: This is more complex than for the simple {@link HardReferenceQueue}
     * due to the indirection through the {@link IRef} object, which breaks
     * reference testing for equality. The scanHead() method in the inner queue
     * was modified in order for this test to pass.
     * 
     * @see <a
     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/465#comment:2">
     *      Too many GRS reads</a>
     */
    public void test_add_scan() {

        final int capacity = 5;
        final int nscan = 2;
        final long timeoutNanos = 0L;

        final MyListener<String, IRef<String>> listener = new MyListener<String, IRef<String>>();

        final SynchronizedHardReferenceQueueWithTimeout<String> cache = new SynchronizedHardReferenceQueueWithTimeout<String>(
                listener, capacity, nscan, timeoutNanos);

        final HardReferenceQueue<IRef<String>> innerQueue = cache.getQueue();

//        final MyListener<String> listener = new MyListener<String>();

//        final HardReferenceQueue<String> cache = new HardReferenceQueue<String>(
//                listener, 5, 2 );

        final String ref0 = "0";
        final String ref1 = "1";
        final String ref2 = "2";

        // initial conditions.
        assertEquals("size",0,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",0,innerQueue.getHeadIndex());
        assertTrue("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{}, innerQueue);
//        assertEquals("order",new String[]{},cache.toArray(new String[0]));

        // append and check post-conditions.
        assertTrue(cache.add(ref0));
        assertEquals("size",1,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",1,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref0}, innerQueue);
//        assertEquals("order",new String[]{ref0},cache.toArray(new String[0]));

        // verify scan finds ref.
        assertFalse(cache.add(ref0));

        // append and check post-conditions.
        assertTrue(cache.add(ref1));
        assertEquals("size",2,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",2,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref0,ref1}, innerQueue);
//        assertEquals("order",new String[]{ref0,ref1},cache.toArray(new String[0]));

        // verify scan finds ref.
        assertFalse(cache.add(ref1));

        // verify scan finds ref.
        assertFalse(cache.add(ref0));
        
        // append and check post-conditions.
        assertTrue(cache.add(ref2));
        assertEquals("size",3,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",3,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref0,ref1,ref2}, innerQueue);
//        assertEquals("order",new String[]{ref0,ref1,ref2},cache.toArray(new String[0]));

        // verify scan finds ref.
        assertFalse(cache.add(ref2));

        // verify scan finds ref.
        assertFalse(cache.add(ref1));
        
        /*
         * Verify scan does NOT find ref. The reference is still in the cache
         * but the scan does not reach back that far.
         */
        assertTrue(cache.add(ref0));
        assertEquals("size",4,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",4,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertSameOrder(new String[]{ref0,ref1,ref2,ref0}, innerQueue);
//        assertEquals("order",new String[]{ref0,ref1,ref2,ref0},cache.toArray(new String[0]));

        // verify scan finds ref.
        assertFalse(cache.add(ref0));

        // verify scan finds ref.
        assertFalse(cache.add(ref2));
        
        /*
         * Verify scan does NOT find ref. The reference is still in the cache
         * but the scan does not reach back that far.  At this point the cache
         * is at capacity.
         */
        assertTrue(cache.add(ref1));
        assertEquals("size",5,cache.size());
        assertEquals("tail",0,innerQueue.getTailIndex());
        assertEquals("head",0,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertSameOrder(new String[]{ref0,ref1,ref2,ref0,ref1}, innerQueue);
//        assertEquals("order",new String[]{ref0,ref1,ref2,ref0,ref1},cache.toArray(new String[0]));
        
        // verify scan finds ref.
        assertFalse(cache.add(ref1));

        // verify scan finds ref.
        assertFalse(cache.add(ref0));
        
        /*
         * Verify scan does NOT find ref. The reference is still in the cache
         * but the scan does not reach back that far.  At this point the cache
         * overflows and evicts the LRU reference.
         */
        listener.setExpectedRef(ref0);
        assertTrue(cache.add(ref2));
        listener.assertEvicted();
        assertEquals("size",5,cache.size());
        assertEquals("tail",1,innerQueue.getTailIndex());
        assertEquals("head",1,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertSameOrder(new String[]{ref1,ref2,ref0,ref1,ref2}, innerQueue);
//        assertEquals("order",new String[]{ref1,ref2,ref0,ref1,ref2},cache.toArray(new String[0]));

        // verify scan finds ref.
        assertFalse(cache.add(ref2));

        // verify scan finds ref.
        assertFalse(cache.add(ref1));

        // verify scan does NOT find ref.
        listener.setExpectedRef(ref1);
        assertTrue(cache.add(ref0));
        listener.assertEvicted();
        assertEquals("size",5,cache.size());
        assertEquals("tail",2,innerQueue.getTailIndex());
        assertEquals("head",2,innerQueue.getHeadIndex());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertSameOrder(new String[]{ref2,ref0,ref1,ref2,ref0}, innerQueue);
//        assertEquals("order",new String[]{ref2,ref0,ref1,ref2,ref0},cache.toArray(new String[0]));

    }
    
    /**
     * Helper class for testing correct behavior of the cache and the listener
     * interface.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @param <T>
     */
    private class MyListener<G,T extends IRef<G>> implements
            HardReferenceQueueEvictionListener<T> {

        /**
         * Constructor.
         */
        public MyListener() {
        }

        /**
         * Set the next N expected references for eviction notices.  You can
         * only do this when nothing is currently expected.
         * 
         * @param refs
         *            The expected references.
         * 
         * @exception IllegalStateExecption
         *                unless there is no current expected reference.
         */
        public void setExpectedRefs(final G[] refs) {

            if( expectedRef != null ) {

                throw new IllegalStateException();
            
            }

            assert refs != null;

            assert refs.length > 0;

            for (int i = refs.length - 1; i >= 0; i--) {

                final G ref = refs[i];

                assert ref != null;
                
                expectedRefs.push(ref);
                
            }

            setExpectedRef( expectedRefs.pop() );
            
        }
        private Stack<G> expectedRefs = new Stack<G>();
        
        /**
         * Set the expected reference for the next eviction notice. The listener
         * will thrown an exception if there is a cache eviction unless you
         * first invoke this method.
         * 
         * @param ref
         *            The expected reference or null to cause the listener to
         *            throw an exception if a reference is evicted.
         */
        public void setExpectedRef(final G ref) {

            this.expectedRef = ref;

            this.evicted = false;
            
        }
        private G expectedRef = null;

        /**
         * Test for an eviction event.
         * 
         * @exception AssertionFailedError
         *                if nothing was evicted since the last time an expected
         *                eviction reference was set.
         */
        public void assertEvicted() {
            
            if(!evicted) {
                
                TestHardReferenceQueue.fail("Expected "+expectedRef+" to have been evicted.");
                
            }
            
        }
        private boolean evicted = false;
        
        /**
         * Test for the expected #of eviction notices to date.
         * 
         * @param expected
         */
        public void assertEvictionCount(int expected) {

            assertEquals("evictionCount", expected, nevicted);

        }

        /**
         * The #of eviction notices to date.
         */
        public int getEvictionCount() {
        
            return nevicted;
            
        }

        private int nevicted = 0;
        
        /**
         * @throws AssertionFailedError
         *             if the evicted reference is not the next expected
         *             eviction reference or if no eviction is expected.
         */
        public void evicted(final IHardReferenceQueue<T> cache, final T ref) {

            assertNotNull("cache", cache);
            assertNotNull("ref", ref);

            if( expectedRef == null && expectedRefs.size() > 0 ) {
                
                /*
                 * There is no current expectation, but there is one on the
                 * stack, so we pop it off the stack and continue.
                 * 
                 * Note: We pop the expectation off of the stack lazily so that
                 * the unit tests have the opportunity to verify that an
                 * expected reference was evicted.
                 */
                setExpectedRef( expectedRefs.pop() );

            }
            
            if( expectedRef == null ) {
                
                fail("Not expecting a cache eviction: ref="+ref);
                
            }

            /*
             * Note: This is a reference test, but it is against the indirected
             * reference!
             */

            assertEquals("ref", expectedRef, ref.get());
//                assertTrue("ref", expectedRef == ref);

            // Reset the expectated ref to null.
            expectedRef = null;

            // Note that the eviction occurred.
            evicted = true;

            nevicted ++;
            
        }

    }
    
}
