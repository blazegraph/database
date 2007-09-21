/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 8, 2006
 */

package com.bigdata.cache;

import java.util.Stack;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import com.bigdata.cache.HardReferenceQueue.HardReferenceQueueEvictionListener;

/**
 * Unit tests for {@link HardReferenceQueue}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHardReferenceCache extends TestCase2 {

    public TestHardReferenceCache() {
    }
    
    public TestHardReferenceCache(String name) {
        
        super(name);
        
    }

    /**
     * Test constructor and its post-conditions.
     */
    public void test_ctor() {
        
        HardReferenceQueueEvictionListener<String> listener = new MyListener<String>();
        
        HardReferenceQueue<String> cache = new HardReferenceQueue<String>(
                listener, 100, 20);

        assertEquals("listener", listener, cache.getListener());
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
        
        try {
            new HardReferenceQueue<String>(null, 100);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expectedRefs exception: " + ex);
        }

        try {
            new HardReferenceQueue<String>(new MyListener<String>(), 0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expectedRefs exception: " + ex);
        }

    }

    /**
     * Correct rejection test for appending a null reference to the cache.
     */
    public void test_append_null() {

        HardReferenceQueueEvictionListener<String> listener = new MyListener<String>();

        HardReferenceQueue<String> cache = new HardReferenceQueue<String>(
                listener, 100, 2 );

        try {
            cache.append(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expectedRefs exception: " + ex);
        }
        
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

        MyListener<String> listener = new MyListener<String>();

        HardReferenceQueue<String> cache = new HardReferenceQueue<String>(
                listener, 5, 0 );

        final String ref0 = "0";
        final String ref1 = "1";
        final String ref2 = "2";
        final String ref3 = "3";
        final String ref4 = "4";
        final String ref5 = "5";
        
        assertEquals("size",0,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",0,cache.head());
        assertTrue("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{},cache.toArray());
        
        assertTrue(cache.append(ref0));
        assertEquals("size",1,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",1,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref0},cache.toArray());
        
        assertTrue(cache.append(ref1));
        assertEquals("size",2,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",2,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref0,ref1},cache.toArray());
        
        assertTrue(cache.append(ref2));
        assertEquals("size",3,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",3,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref0,ref1,ref2},cache.toArray());
        
        assertTrue(cache.append(ref3));
        assertEquals("size",4,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",4,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref0,ref1,ref2,ref3},cache.toArray());
        
        assertTrue(cache.append(ref4));
        assertEquals("size",5,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",0,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertEquals("order",new String[]{ref0,ref1,ref2,ref3,ref4},cache.toArray());

        listener.setExpectedRef(ref0);
        assertTrue(cache.append(ref5));
        listener.assertEvicted();
        assertEquals("size",5,cache.size());
        assertEquals("tail",1,cache.tail());
        assertEquals("head",1,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertEquals("order",new String[]{ref1,ref2,ref3,ref4,ref5},cache.toArray());

        /*
         * Evict the LRU reference and verify that the cache size goes down by
         * one.
         */
        listener.setExpectedRef(ref1);
        assertTrue(cache.evict());
        listener.assertEvicted();
        assertEquals("size",4,cache.size());
        assertEquals("tail",2,cache.tail());
        assertEquals("head",1,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref2,ref3,ref4,ref5},cache.toArray());

        /*
         * add a reference - no eviction since the cache was not at capacity. As
         * a post-condition, the cache is once again at capacity.
         */
        assertTrue(cache.append(ref4));
        assertEquals("size",5,cache.size());
        assertEquals("tail",2,cache.tail());
        assertEquals("head",2,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertEquals("order",new String[]{ref2,ref3,ref4,ref5,ref4},cache.toArray());

        /*
         * Add another reference and verify that an eviction occurs.
         */
        listener.setExpectedRef(ref2);
        assertTrue(cache.append(ref2));
        listener.assertEvicted();
        assertEquals("size",5,cache.size());
        assertEquals("tail",3,cache.tail());
        assertEquals("head",3,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertEquals("order",new String[]{ref3,ref4,ref5,ref4,ref2},cache.toArray());

        /*
         * Test evictAll(false) (does not change the cache state).
         */
        int nevicted = listener.getEvictionCount();
        listener.setExpectedRefs(new String[]{ref3,ref4,ref5,ref4,ref2});
        cache.evictAll(false);
        listener.assertEvictionCount(nevicted+5);
        assertEquals("size",5,cache.size());
        assertEquals("tail",3,cache.tail());
        assertEquals("head",3,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertEquals("order",new String[]{ref3,ref4,ref5,ref4,ref2},cache.toArray());

        /*
         * Test evictAll(false) (actually evicts the references from the cache).
         */
        nevicted = listener.getEvictionCount();
        listener.setExpectedRefs(new String[]{ref3,ref4,ref5,ref4,ref2});
        cache.evictAll(true);
        listener.assertEvictionCount(nevicted+5);
        assertEquals("size",0,cache.size());
        assertEquals("tail",3,cache.tail());
        assertEquals("head",3,cache.head());
        assertTrue("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{},cache.toArray());
        
    }
    
    /**
     * Test verifies scan of the last N references when adding a reference to
     * the cache. When the test starts the tail is at index 0, but eventually we
     * wrap the cache around and continue testing to make sure that scans
     * function correctly with a head index of 0 (this requires continuing the
     * scan from the array capacity).
     */
    public void test_add_scan() {

        MyListener<String> listener = new MyListener<String>();

        HardReferenceQueue<String> cache = new HardReferenceQueue<String>(
                listener, 5, 2 );

        final String ref0 = "0";
        final String ref1 = "1";
        final String ref2 = "2";

        // initial conditions.
        assertEquals("size",0,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",0,cache.head());
        assertTrue("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{},cache.toArray());

        // append and check post-conditions.
        assertTrue(cache.append(ref0));
        assertEquals("size",1,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",1,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref0},cache.toArray());

        // verify scan finds ref.
        assertFalse(cache.append(ref0));

        // append and check post-conditions.
        assertTrue(cache.append(ref1));
        assertEquals("size",2,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",2,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref0,ref1},cache.toArray());

        // verify scan finds ref.
        assertFalse(cache.append(ref1));

        // verify scan finds ref.
        assertFalse(cache.append(ref0));
        
        // append and check post-conditions.
        assertTrue(cache.append(ref2));
        assertEquals("size",3,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",3,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref0,ref1,ref2},cache.toArray());

        // verify scan finds ref.
        assertFalse(cache.append(ref2));

        // verify scan finds ref.
        assertFalse(cache.append(ref1));
        
        /*
         * Verify scan does NOT find ref. The reference is still in the cache
         * but the scan does not reach back that far.
         */
        assertTrue(cache.append(ref0));
        assertEquals("size",4,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",4,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertFalse("full",cache.isFull());
        assertEquals("order",new String[]{ref0,ref1,ref2,ref0},cache.toArray());

        // verify scan finds ref.
        assertFalse(cache.append(ref0));

        // verify scan finds ref.
        assertFalse(cache.append(ref2));
        
        /*
         * Verify scan does NOT find ref. The reference is still in the cache
         * but the scan does not reach back that far.  At this point the cache
         * is at capacity.
         */
        assertTrue(cache.append(ref1));
        assertEquals("size",5,cache.size());
        assertEquals("tail",0,cache.tail());
        assertEquals("head",0,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertEquals("order",new String[]{ref0,ref1,ref2,ref0,ref1},cache.toArray());
        
        // verify scan finds ref.
        assertFalse(cache.append(ref1));

        // verify scan finds ref.
        assertFalse(cache.append(ref0));
        
        /*
         * Verify scan does NOT find ref. The reference is still in the cache
         * but the scan does not reach back that far.  At this point the cache
         * overflows and evicts the LRU reference.
         */
        listener.setExpectedRef(ref0);
        assertTrue(cache.append(ref2));
        listener.assertEvicted();
        assertEquals("size",5,cache.size());
        assertEquals("tail",1,cache.tail());
        assertEquals("head",1,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertEquals("order",new String[]{ref1,ref2,ref0,ref1,ref2},cache.toArray());

        // verify scan finds ref.
        assertFalse(cache.append(ref2));

        // verify scan finds ref.
        assertFalse(cache.append(ref1));

        // verify scan does NOT find ref.
        listener.setExpectedRef(ref1);
        assertTrue(cache.append(ref0));
        listener.assertEvicted();
        assertEquals("size",5,cache.size());
        assertEquals("tail",2,cache.tail());
        assertEquals("head",2,cache.head());
        assertFalse("empty",cache.isEmpty());
        assertTrue("full",cache.isFull());
        assertEquals("order",new String[]{ref2,ref0,ref1,ref2,ref0},cache.toArray());

    }

    /**
     * Helper class for testing correct behavior of the cache and the listener
     * interface.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     */
    public static class MyListener<T> implements
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
        public void setExpectedRefs(T[] refs) {

            if( expectedRef != null ) {

                throw new IllegalStateException();
            
            }

            assert refs != null;
            
            assert refs.length > 0;
            
            for( int i=refs.length-1; i>=0; i-- ) {
                
                T ref = refs[i];
                
                assert ref != null;
                
                expectedRefs.push(ref);
                
            }

            setExpectedRef( expectedRefs.pop() );
            
        }
        Stack<T> expectedRefs = new Stack<T>();
        
        /**
         * Set the expected reference for the next eviction notice. The listener
         * will thrown an exception if there is a cache eviction unless you
         * first invoke this method.
         * 
         * @param ref
         *            The expected reference or null to cause the listener to
         *            throw an exception if a reference is evicted.
         */
        public void setExpectedRef(T ref) {

            this.expectedRef = ref;

            this.evicted = false;
            
        }
        private T expectedRef = null;

        /**
         * Test for an eviction event.
         * 
         * @exception AssertionFailedError
         *                if nothing was evicted since the last time an expected
         *                eviction reference was set.
         */
        public void assertEvicted() {
            
            if(!evicted) {
                
                fail("Expected "+expectedRef+" to have been evicted.");
                
            }
            
        }
        private boolean evicted = false;
        
        /**
         * Test for the expected #of eviction notices to date.
         * 
         * @param expected
         */
        public void assertEvictionCount(int expected) {
            
            assertEquals("evictionCount",expected,nevicted);
            
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
        public void evicted(HardReferenceQueue<T> cache, T ref) {

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

            assertEquals("ref",expectedRef,ref); // Note: This is a reference test.
//            assertTrue("ref", expectedRef == ref);

            // Reset the expectated ref to null.
            expectedRef = null;

            // Note that the eviction occurred.
            evicted = true;

            nevicted ++;
            
        }

    }

}
