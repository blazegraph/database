/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 27, 2010
 */

package cutthecrap.utils.striterators;

import java.util.Iterator;

import cutthecrap.utils.striterators.FilterBase;

import junit.framework.TestCase;

/**
 * Test suite for {@link FilterBase}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFilterBase extends TestCase {

    /**
     * 
     */
    public TestFilterBase() {
    }

    /**
     * @param name
     */
    public TestFilterBase(String name) {
        super(name);
    }

    public void test_filterBase_ctor() {
        
        final FilterBase fixture = new MockFilterBase();
        
        assertEquals("annotations", null, fixture.annotations);
        assertEquals("filterChain", null, fixture.filterChain);
        
    }

    public void test_filterBase_annotations() {
        
        final FilterBase fixture = new MockFilterBase();
        
        assertEquals("annotations", null, fixture.annotations);

        final String name = "name";
        final Object value = Integer.valueOf(0);
        final Object value2 = Integer.valueOf(1);

        try {
            fixture.getRequiredProperty(name);
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {

        }

        assertNull(fixture.getProperty(name));
        assertNull(fixture.setProperty(name, value));
        assertNotNull("annotations", fixture.annotations);
        assertEquals("annotations", 1, fixture.annotations.size());
        assertEquals(value, fixture.getProperty(name));
        assertEquals(value, fixture.getRequiredProperty(name));
        assertEquals(value, fixture.setProperty(name, value2));
        assertEquals(value2, fixture.getProperty(name));
        assertEquals(value2, fixture.getRequiredProperty(name));

    }

    public void test_filterBase_filterChain() {

        final Object s1 = "s1";
        final Object s2 = "s2";
        final Object s3 = "s3";

        final FilterBase f1, f2, f3;

        final FilterBase fixture = f1 = new MockFilterBase();

        assertNull("filterChain", fixture.filterChain);

        fixture.addFilter(f2 = new MockFilterBase());

        assertNotNull("filterChain", fixture.filterChain);

        fixture.addFilter(f3 = new MockFilterBase());

        final IFilter[] expected = new IFilter[] { f2, f3 };

        final IFilter[] actual = f1.filterChain.toArray(new IFilter[] {});

        assertEquals("#filters", expected.length, actual.length);

        for (int i = 0; i < expected.length; i++) {

            assertEquals("filter[" + i + "]", expected[i], actual[i]);
            
        }
        
    }

    /**
     * Test creation of iterator without filter chain. make sure that the
     * context is passed through.
     */
    public void test_filter() {
        
        final FilterBase fixture = new MockFilterBase();

        final Object context = new Object();

        final Iterator src = EmptyIterator.DEFAULT;

        final MockIterator actual = (MockIterator) fixture.filter(src, context);

        assertNotNull(actual);
        assertTrue("src", actual.src == src);
        assertTrue("context", actual.context == context);
        assertTrue("filter", actual.filter == fixture);

    }

    /**
     * Test creation of iterator with filter chain. Make sure that the create
     * order is correct and that the context is passed through to each iterator.
     * The iterators are assembled in FIFO order, so the iterator stack winds up
     * being LIFO.
     */
    public void test_filter2() {

        // create filters w/ state objects (helps visual inspection in debugger).
    	// or would do if we hadn't removed teh state variable...
    	
        final Object s1 = "s1";
        final Object s2 = "s2";
        final Object s3 = "s3";
        final FilterBase fixture1 = new MockFilterBase();
        final FilterBase fixture2 = new MockFilterBase();
        final FilterBase fixture3 = new MockFilterBase();

        // chain 2 filters to the first.
        fixture1.addFilter(fixture2);
        fixture1.addFilter(fixture3);
        
        // verify the filter chain.
        assertNotNull(fixture1.filterChain);
        assertEquals(2, fixture1.filterChain.size());
        assertTrue(fixture2 == fixture1.filterChain.get(0));
        assertTrue(fixture3 == fixture1.filterChain.get(1));

        // verify other filter chains are empty.
        assertNull(fixture2.filterChain);
        assertNull(fixture3.filterChain);
        
        final Object context = "context";

        final Iterator src = EmptyIterator.DEFAULT;

        /*
         * Create and verify the iterator stack.
         * 
         * Note: The iterator are created in the order in filter chain order,
         * but each iterator wraps the previous iterator. This has the effect of
         * building an iterator stack which is the reverse of the filter chain.
         * 
         * logical filter chain: filter1, filter2, filter3
         * 
         * logical iterator stack: itr1(filter3), itr2(filter2), itr3(filter1).
         */
        final MockIterator actual1 = (MockIterator) fixture1.filter(src, context);
        final MockIterator actual2 = (MockIterator) actual1.src;
        final MockIterator actual3 = (MockIterator) actual2.src;
        // itr3 (bottom of the stack)
        assertTrue("src", actual3.src == src);
        assertTrue("context", actual3.context == context);
        assertTrue("filter", actual3.filter == fixture1);
        // itr2
        assertTrue("src", actual2.src == actual3);
        assertTrue("context", actual2.context == context);
        assertTrue("filter", actual2.filter == fixture2);
        // itr1 (top of the stack)
        assertTrue("src", actual1.src == actual2);
        assertTrue("context", actual1.context == context);
        assertTrue("filter", actual1.filter == fixture3);
    }

    /**
     * Mock object.
     */
    private static class MockFilterBase extends FilterBase {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        public MockFilterBase() {
            super();
        }
                
        @Override
        protected Iterator filterOnce(Iterator src, Object context) {
            
            return new MockIterator(src, context, this);
            
        }
        
    }

    private static class MockIterator<E> implements Iterator<E> {

        final Iterator src;

        final Object context;

        final MockFilterBase filter;
        
        public MockIterator(Iterator src, Object context, MockFilterBase filter) {

            this.src = src;

            this.context = context;
            
            this.filter = filter;
            
        }
        
        public boolean hasNext() {
            return false;
        }

        public E next() {
            return null;
        }

        public void remove() {
        }
        
    }
    
}
