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

package com.bigdata.objndx;

import java.util.Vector;

import com.bigdata.objndx.HardReferenceCache.HardReferenceCacheEvictionListener;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

/**
 * Unit tests for {@link HardReferenceCache}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHardReferenceCache extends TestCase {

    public TestHardReferenceCache() {
    }
    
    public TestHardReferenceCache(String name) {
        
        super(name);
        
    }

    /**
     * Test constructor and its post-conditions.
     */
    public void test_ctor() {
        
        HardReferenceCacheEvictionListener<String> listener = new MyListener<String>(
                new Vector<String>());
        
        HardReferenceCache<String> cache = new HardReferenceCache<String>(
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
            new HardReferenceCache<String>(null, 100);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expectedRefs exception: " + ex);
        }

        try {
            new HardReferenceCache<String>(new MyListener<String>(
                    new Vector<String>()), 0);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expectedRefs exception: " + ex);
        }

    }

    /**
     * Correct rejection test for appending a null reference to the cache.
     */
    public void test_append_null() {

        Vector<String> expectedRefs = new Vector<String>();

        HardReferenceCacheEvictionListener<String> listener = new MyListener<String>(
                expectedRefs);

        HardReferenceCache<String> cache = new HardReferenceCache<String>(
                listener, 100, 2 );

        try {
            cache.append(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            System.err.println("Ignoring expectedRefs exception: " + ex);
        }
        
    }
    
    /**
     * Test the behavior of the hard reference cache.
     * 
     * @todo test scan of last N refs.
     * @todo test eviction order.
     * @todo test with wrap.
     * @todo test evictAll(false) and evictAll(true)
     */
    public void test_semantics() {

        Vector<String> expectedRefs = new Vector<String>();

        HardReferenceCacheEvictionListener<String> listener = new MyListener<String>(
                expectedRefs);

        HardReferenceCache<String> cache = new HardReferenceCache<String>(
                listener, 5, 2 );

        final String ref0 = "0";
        final String ref1 = "1";
        final String ref2 = "2";
        
        assertTrue(cache.append(ref0));
        assertFalse(cache.append(ref0));

        assertTrue(cache.append(ref1));
        assertFalse(cache.append(ref1));
        assertFalse(cache.append(ref0));
        
        assertTrue(cache.append(ref2));
        assertFalse(cache.append(ref1));
        assertFalse(cache.append(ref1));
        assertTrue(cache.append(ref0));

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
            HardReferenceCacheEvictionListener<T> {

        /**
         * Vector of the object references in the cache in order from MRU to
         * LRU.  Evictions are reported for the _last_ object in this vector.
         */
        private Vector<T> expectedRefs = new Vector<T>();

        /**
         * 
         * @param expectedRefs
         *            Vector of the object references in the cache in order from
         *            MRU to LRU. Evictions are reported for the _last_ object
         *            in this vector.
         */
        public MyListener(Vector<T> expectedRefs) {

            assert expectedRefs != null;

            this.expectedRefs = expectedRefs;

        }

        /**
         * Set the expected references for eviction notices.
         * 
         * @param expectedRefs
         *            Vector of the object references in the cache in order from
         *            MRU to LRU. Evictions are reported for the _last_ object
         *            in this vector.
         */
        public void setExpectedRefs(Vector<T> expectedRefs) {

            assert expectedRefs != null;

            this.expectedRefs = expectedRefs;

        }

        /**
         * @throws AssertionFailedError
         *             if the evicted reference is not the next expected
         *             eviction reference or if no eviction is expected.
         */
        public void evicted(HardReferenceCache<T> cache, T ref) {

            assertNotNull("cache", cache);
            assertNotNull("ref", ref);

            final int index = expectedRefs.size();

            if (index == 0) {

                fail("Not expecting any evictions: ref=" + ref);

            }

            // Remove the last reference from the array.
            final T expectedRef = expectedRefs.remove(index);

            // verify that this is the same reference.
            assertTrue("ref", expectedRef == ref);

        }

    }

}
